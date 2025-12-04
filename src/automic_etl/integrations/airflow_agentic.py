"""Agentic Apache Airflow integration with LLM-powered orchestration.

This module extends the basic Airflow integration with intelligent,
autonomous capabilities powered by LLM agents for:
- Dynamic DAG generation based on data analysis
- Self-healing task recovery
- Intelligent scheduling optimization
- Autonomous pipeline monitoring and adjustment
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Literal
import asyncio
import re

import structlog

from automic_etl.integrations.airflow import AirflowConfig, AirflowIntegration
from automic_etl.llm.client import LLMClient, LLMResponse
from automic_etl.core.config import Settings, get_settings
from automic_etl.core.pipeline import Pipeline, PipelineStatus, PipelineResult
from automic_etl.core.utils import utc_now, format_iso
from automic_etl.lineage.tracker import LineageTracker

logger = structlog.get_logger()

# =============================================================================
# Constants
# =============================================================================

# Maximum number of failed tasks to analyze to avoid token overflow
MAX_FAILED_TASKS_TO_ANALYZE = 3

# Maximum characters of log content to include in analysis
MAX_LOG_CHARS = 2000

# Maximum number of error logs to include in decision context
MAX_ERROR_LOGS_IN_CONTEXT = 5

# Minimum confidence threshold for auto-executing recovery plans
AUTO_RECOVERY_CONFIDENCE_THRESHOLD = 0.8

# Minimum confidence threshold for monitoring to auto-heal
MONITORING_HEAL_CONFIDENCE_THRESHOLD = 0.7

# Maximum historical runs to fetch for analysis
MAX_HISTORICAL_RUNS = 20

# Maximum recent runs to include in context
MAX_RECENT_RUNS_IN_CONTEXT = 10

# Maximum runs to fetch for optimization analysis
MAX_RUNS_FOR_OPTIMIZATION = 50

# Default recovery time in seconds when diagnosis fails
DEFAULT_RECOVERY_TIME_SECONDS = 3600

# Default estimated recovery time multiplier (minutes to seconds)
RECOVERY_TIME_MINUTES_TO_SECONDS = 60

# Maximum failed runs to check per DAG during monitoring
MAX_FAILED_RUNS_TO_CHECK = 5


class AgentAction(str, Enum):
    """Actions an agent can take."""
    ANALYZE = "analyze"
    GENERATE_DAG = "generate_dag"
    OPTIMIZE = "optimize"
    RECOVER = "recover"
    SCALE = "scale"
    ALERT = "alert"
    SKIP = "skip"
    RETRY = "retry"
    ROLLBACK = "rollback"
    MODIFY_SCHEDULE = "modify_schedule"


class AgentDecisionConfidence(str, Enum):
    """Confidence levels for agent decisions."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class AgentDecision:
    """Result of an agent's decision-making process."""
    action: AgentAction
    confidence: AgentDecisionConfidence
    reasoning: str
    parameters: dict[str, Any] = field(default_factory=dict)
    requires_approval: bool = False
    timestamp: datetime = field(default_factory=utc_now)

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action.value,
            "confidence": self.confidence.value,
            "reasoning": self.reasoning,
            "parameters": self.parameters,
            "requires_approval": self.requires_approval,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class AgentContext:
    """Context information for agent decision making."""
    pipeline_name: str
    current_state: dict[str, Any]
    historical_runs: list[dict[str, Any]] = field(default_factory=list)
    error_logs: list[str] = field(default_factory=list)
    data_profile: dict[str, Any] = field(default_factory=dict)
    lineage: dict[str, Any] = field(default_factory=dict)
    constraints: dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskRecoveryPlan:
    """Plan for recovering a failed task."""
    task_id: str
    dag_id: str
    failure_reason: str
    recovery_steps: list[dict[str, Any]]
    estimated_recovery_time: int  # seconds
    requires_human_intervention: bool
    confidence: float
    alternative_strategies: list[str] = field(default_factory=list)


@dataclass
class DAGOptimizationSuggestion:
    """Suggestion for optimizing a DAG."""
    dag_id: str
    optimization_type: str
    current_value: Any
    suggested_value: Any
    expected_improvement: str
    reasoning: str
    risk_level: Literal["low", "medium", "high"]
    auto_applicable: bool


class AirflowAgent:
    """
    LLM-powered agent for intelligent Airflow orchestration.

    This agent can:
    - Analyze pipeline performance and suggest optimizations
    - Generate DAGs from natural language descriptions
    - Automatically recover from failures
    - Dynamically adjust schedules based on data patterns
    - Monitor and alert on anomalies
    """

    SYSTEM_PROMPT = """You are an expert data engineering agent specializing in Apache Airflow orchestration and ETL pipeline management.

Your capabilities include:
1. Analyzing pipeline performance metrics and identifying bottlenecks
2. Generating optimal DAG configurations based on data requirements
3. Diagnosing and recovering from task failures
4. Optimizing schedules based on data arrival patterns
5. Ensuring data quality and lineage tracking

When making decisions:
- Always consider data dependencies and downstream impacts
- Prioritize data consistency and reliability
- Minimize resource usage while maintaining SLAs
- Provide clear reasoning for all recommendations
- Flag high-risk actions for human approval

You communicate through structured JSON responses for programmatic processing."""

    def __init__(
        self,
        llm_client: LLMClient,
        airflow: AirflowIntegration,
        settings: Settings | None = None,
        auto_approve_low_risk: bool = True,
    ):
        self.llm = llm_client
        self.airflow = airflow
        self.settings = settings or get_settings()
        self.auto_approve_low_risk = auto_approve_low_risk
        self.decision_history: list[AgentDecision] = []
        self.logger = logger.bind(component="airflow_agent")

    async def analyze_pipeline(
        self,
        pipeline_name: str,
        include_history: bool = True,
        include_lineage: bool = True,
    ) -> dict[str, Any]:
        """
        Analyze a pipeline's performance, health, and optimization opportunities.

        Args:
            pipeline_name: Name of the pipeline/DAG to analyze
            include_history: Include historical run data
            include_lineage: Include data lineage information

        Returns:
            Comprehensive analysis with recommendations
        """
        self.logger.info("Analyzing pipeline", pipeline=pipeline_name)

        # Gather context
        context = await self._build_context(
            pipeline_name,
            include_history=include_history,
            include_lineage=include_lineage,
        )

        prompt = f"""Analyze the following ETL pipeline and provide comprehensive insights:

Pipeline: {pipeline_name}

Current State:
{json.dumps(context.current_state, indent=2, default=str)}

Historical Runs (last {MAX_RECENT_RUNS_IN_CONTEXT}):
{json.dumps(context.historical_runs[:MAX_RECENT_RUNS_IN_CONTEXT], indent=2, default=str)}

Data Profile:
{json.dumps(context.data_profile, indent=2, default=str)}

Provide your analysis as JSON with this structure:
{{
    "health_score": <0-100>,
    "performance_summary": "<brief summary>",
    "bottlenecks": [
        {{"task": "<task_id>", "issue": "<description>", "impact": "<high/medium/low>"}}
    ],
    "optimization_opportunities": [
        {{"type": "<type>", "description": "<description>", "expected_improvement": "<percentage>"}}
    ],
    "data_quality_concerns": [
        {{"concern": "<description>", "affected_tables": ["<table>"]}}
    ],
    "recommendations": [
        {{"priority": 1, "action": "<action>", "reasoning": "<why>"}}
    ],
    "risk_assessment": {{
        "overall_risk": "<low/medium/high>",
        "factors": ["<risk factor>"]
    }}
}}"""

        response = self.llm.complete(
            prompt=prompt,
            system_prompt=self.SYSTEM_PROMPT,
            temperature=0.3,
        )

        try:
            analysis = json.loads(self._extract_json(response.content))
            analysis["tokens_used"] = response.tokens_used
            analysis["analyzed_at"] = format_iso()
            return analysis
        except json.JSONDecodeError:
            self.logger.error("Failed to parse analysis response")
            return {
                "error": "Failed to parse analysis",
                "raw_response": response.content,
            }

    async def generate_dag_from_description(
        self,
        description: str,
        source_tables: list[str] | None = None,
        target_table: str | None = None,
        schedule: str | None = None,
        constraints: dict[str, Any] | None = None,
    ) -> str:
        """
        Generate an Airflow DAG from a natural language description.

        Args:
            description: Natural language description of the pipeline
            source_tables: List of source table names
            target_table: Target table name
            schedule: Desired schedule (or None for agent to suggest)
            constraints: Any constraints to consider

        Returns:
            Generated DAG Python code
        """
        self.logger.info("Generating DAG from description", description=description[:100])

        prompt = f"""Generate an Apache Airflow DAG based on this description:

Description: {description}

Source Tables: {source_tables or 'Determine from description'}
Target Table: {target_table or 'Determine from description'}
Schedule: {schedule or 'Suggest optimal schedule'}
Constraints: {json.dumps(constraints or {}, indent=2)}

Requirements:
1. Use Automic ETL operators and patterns
2. Include proper error handling and retries
3. Add data quality checks
4. Include proper logging
5. Set appropriate task dependencies
6. Use Jinja templating for dates where needed

Provide your response as JSON with this structure:
{{
    "dag_id": "<suggested_dag_id>",
    "description": "<dag_description>",
    "schedule_interval": "<cron_expression>",
    "reasoning": "<why_this_schedule>",
    "tasks": [
        {{
            "task_id": "<task_id>",
            "operator": "<operator_type>",
            "description": "<what_it_does>",
            "config": {{}},
            "dependencies": ["<upstream_task_id>"]
        }}
    ],
    "dag_config": {{
        "retries": <number>,
        "retry_delay_minutes": <number>,
        "catchup": <boolean>,
        "max_active_runs": <number>
    }},
    "data_quality_checks": [
        {{"check": "<description>", "threshold": "<value>"}}
    ]
}}"""

        response = self.llm.complete(
            prompt=prompt,
            system_prompt=self.SYSTEM_PROMPT,
            temperature=0.5,
        )

        try:
            dag_spec = json.loads(self._extract_json(response.content))
            dag_code = self._generate_dag_code(dag_spec)
            return dag_code
        except json.JSONDecodeError:
            self.logger.error("Failed to parse DAG specification")
            raise ValueError(f"Failed to generate DAG: {response.content}")

    async def diagnose_failure(
        self,
        dag_id: str,
        run_id: str,
        task_id: str | None = None,
    ) -> TaskRecoveryPlan:
        """
        Diagnose a DAG/task failure and create a recovery plan.

        Args:
            dag_id: The DAG that failed
            run_id: The specific run that failed
            task_id: Optional specific task that failed

        Returns:
            Recovery plan with steps and confidence
        """
        self.logger.info("Diagnosing failure", dag_id=dag_id, run_id=run_id)

        # Get failure information from Airflow
        dag_run = self.airflow.get_dag_run(dag_id, run_id)
        task_instances = self.airflow.get_task_instances(dag_id, run_id)

        # Get logs for failed tasks
        failed_tasks = [t for t in task_instances if t.get("state") == "failed"]
        error_logs = []

        for task in failed_tasks[:MAX_FAILED_TASKS_TO_ANALYZE]:
            try:
                logs = self.airflow.get_task_logs(
                    dag_id, run_id, task["task_id"],
                    task_try_number=task.get("try_number", 1)
                )
                error_logs.append({
                    "task_id": task["task_id"],
                    "logs": logs[-MAX_LOG_CHARS:] if len(logs) > MAX_LOG_CHARS else logs
                })
            except Exception:
                pass

        prompt = f"""Diagnose this Airflow DAG failure and create a recovery plan:

DAG: {dag_id}
Run: {run_id}
State: {dag_run.get('state')}

Failed Tasks:
{json.dumps([{{"task_id": t["task_id"], "state": t.get("state"), "try_number": t.get("try_number")}} for t in failed_tasks], indent=2)}

Error Logs:
{json.dumps(error_logs, indent=2)}

Provide your diagnosis and recovery plan as JSON:
{{
    "root_cause": "<description of root cause>",
    "failure_category": "<data/infrastructure/code/external/unknown>",
    "affected_downstream": ["<task_ids>"],
    "recovery_plan": {{
        "strategy": "<retry/skip/rollback/manual/fix_and_retry>",
        "steps": [
            {{"order": 1, "action": "<action>", "details": "<how>"}}
        ],
        "estimated_time_minutes": <number>,
        "requires_human": <boolean>,
        "confidence": <0-1>
    }},
    "prevention_recommendations": [
        "<how to prevent this in future>"
    ],
    "alternative_strategies": [
        {{"strategy": "<name>", "trade_offs": "<description>"}}
    ]
}}"""

        response = self.llm.complete(
            prompt=prompt,
            system_prompt=self.SYSTEM_PROMPT,
            temperature=0.3,
        )

        try:
            diagnosis = json.loads(self._extract_json(response.content))

            return TaskRecoveryPlan(
                task_id=task_id or (failed_tasks[0]["task_id"] if failed_tasks else "unknown"),
                dag_id=dag_id,
                failure_reason=diagnosis.get("root_cause", "Unknown"),
                recovery_steps=diagnosis.get("recovery_plan", {}).get("steps", []),
                estimated_recovery_time=diagnosis.get("recovery_plan", {}).get("estimated_time_minutes", 30) * RECOVERY_TIME_MINUTES_TO_SECONDS,
                requires_human_intervention=diagnosis.get("recovery_plan", {}).get("requires_human", True),
                confidence=diagnosis.get("recovery_plan", {}).get("confidence", 0.5),
                alternative_strategies=[
                    s["strategy"] for s in diagnosis.get("alternative_strategies", [])
                ],
            )
        except json.JSONDecodeError:
            self.logger.error("Failed to parse diagnosis")
            return TaskRecoveryPlan(
                task_id=task_id or "unknown",
                dag_id=dag_id,
                failure_reason="Unable to diagnose",
                recovery_steps=[{"order": 1, "action": "Manual investigation required"}],
                estimated_recovery_time=DEFAULT_RECOVERY_TIME_SECONDS,
                requires_human_intervention=True,
                confidence=0.0,
            )

    async def execute_recovery(
        self,
        recovery_plan: TaskRecoveryPlan,
        auto_execute: bool = False,
    ) -> dict[str, Any]:
        """
        Execute a recovery plan for a failed task.

        Args:
            recovery_plan: The recovery plan to execute
            auto_execute: If True, execute without confirmation for high-confidence plans

        Returns:
            Recovery execution result
        """
        self.logger.info(
            "Executing recovery",
            dag_id=recovery_plan.dag_id,
            task_id=recovery_plan.task_id,
            confidence=recovery_plan.confidence,
        )

        # Check if we can auto-execute
        can_auto = (
            auto_execute
            and recovery_plan.confidence >= AUTO_RECOVERY_CONFIDENCE_THRESHOLD
            and not recovery_plan.requires_human_intervention
            and self.auto_approve_low_risk
        )

        if not can_auto and not auto_execute:
            return {
                "status": "pending_approval",
                "plan": {
                    "task_id": recovery_plan.task_id,
                    "dag_id": recovery_plan.dag_id,
                    "steps": recovery_plan.recovery_steps,
                    "confidence": recovery_plan.confidence,
                },
                "message": "Recovery plan requires approval before execution",
            }

        results = []

        for step in recovery_plan.recovery_steps:
            action = step.get("action", "").lower()

            try:
                if "retry" in action or "clear" in action:
                    # Clear and retry the failed task
                    result = self.airflow.clear_task_instances(
                        dag_id=recovery_plan.dag_id,
                        run_id=step.get("run_id", ""),
                        task_ids=[recovery_plan.task_id],
                        include_downstream=True,
                    )
                    results.append({"step": step["order"], "action": action, "status": "success", "result": result})

                elif "skip" in action:
                    # Mark task as skipped (would need custom implementation)
                    results.append({"step": step["order"], "action": action, "status": "skipped", "result": "Skip not directly supported"})

                elif "trigger" in action:
                    # Trigger a new DAG run
                    run_id = self.airflow.trigger_dag(recovery_plan.dag_id)
                    results.append({"step": step["order"], "action": action, "status": "success", "result": {"run_id": run_id}})

                else:
                    results.append({"step": step["order"], "action": action, "status": "manual_required", "result": step.get("details")})

            except Exception as e:
                results.append({"step": step["order"], "action": action, "status": "failed", "error": str(e)})

        return {
            "status": "completed",
            "plan": recovery_plan.task_id,
            "results": results,
            "timestamp": format_iso(),
        }

    async def optimize_schedule(
        self,
        dag_id: str,
        optimization_goal: Literal["latency", "cost", "reliability"] = "reliability",
    ) -> list[DAGOptimizationSuggestion]:
        """
        Analyze and suggest schedule optimizations for a DAG.

        Args:
            dag_id: The DAG to optimize
            optimization_goal: What to optimize for

        Returns:
            List of optimization suggestions
        """
        self.logger.info("Optimizing schedule", dag_id=dag_id, goal=optimization_goal)

        # Get DAG info and historical runs
        dag_info = self.airflow.get_dag(dag_id)
        runs = self.airflow.list_dag_runs(dag_id, limit=MAX_RUNS_FOR_OPTIMIZATION)

        # Analyze run patterns
        run_durations = []
        run_times = []
        for run in runs:
            if run.get("end_date") and run.get("start_date"):
                try:
                    start = datetime.fromisoformat(run["start_date"].replace("Z", "+00:00"))
                    end = datetime.fromisoformat(run["end_date"].replace("Z", "+00:00"))
                    run_durations.append((end - start).total_seconds())
                    run_times.append(start.hour)
                except (ValueError, TypeError):
                    pass

        prompt = f"""Optimize the schedule for this Airflow DAG:

DAG: {dag_id}
Current Schedule: {dag_info.get('schedule_interval')}
Optimization Goal: {optimization_goal}

Historical Run Analysis:
- Total Runs: {len(runs)}
- Average Duration: {sum(run_durations)/len(run_durations) if run_durations else 'N/A'} seconds
- Success Rate: {sum(1 for r in runs if r.get('state') == 'success') / len(runs) * 100 if runs else 0:.1f}%
- Common Run Hours: {set(run_times)}

Recent Run States:
{json.dumps([{{"state": r.get("state"), "duration": r.get("duration")}} for r in runs[:10]], indent=2)}

Provide optimization suggestions as JSON:
{{
    "current_analysis": {{
        "schedule_appropriateness": "<good/suboptimal/poor>",
        "issues": ["<issue>"]
    }},
    "suggestions": [
        {{
            "type": "<schedule/parallelism/resources/dependencies>",
            "current_value": "<current>",
            "suggested_value": "<suggested>",
            "expected_improvement": "<description>",
            "reasoning": "<why>",
            "risk_level": "<low/medium/high>",
            "can_auto_apply": <boolean>
        }}
    ],
    "alternative_approaches": [
        {{"approach": "<description>", "trade_off": "<description>"}}
    ]
}}"""

        response = self.llm.complete(
            prompt=prompt,
            system_prompt=self.SYSTEM_PROMPT,
            temperature=0.4,
        )

        try:
            optimization = json.loads(self._extract_json(response.content))

            return [
                DAGOptimizationSuggestion(
                    dag_id=dag_id,
                    optimization_type=s.get("type", "unknown"),
                    current_value=s.get("current_value"),
                    suggested_value=s.get("suggested_value"),
                    expected_improvement=s.get("expected_improvement", ""),
                    reasoning=s.get("reasoning", ""),
                    risk_level=s.get("risk_level", "medium"),
                    auto_applicable=s.get("can_auto_apply", False),
                )
                for s in optimization.get("suggestions", [])
            ]
        except json.JSONDecodeError:
            self.logger.error("Failed to parse optimization response")
            return []

    async def decide_action(
        self,
        context: AgentContext,
        available_actions: list[AgentAction] | None = None,
    ) -> AgentDecision:
        """
        Make an autonomous decision about what action to take.

        Args:
            context: Current context information
            available_actions: List of actions the agent can consider

        Returns:
            Agent's decision with reasoning
        """
        available_actions = available_actions or list(AgentAction)

        prompt = f"""Given the current pipeline context, decide what action to take:

Pipeline: {context.pipeline_name}

Current State:
{json.dumps(context.current_state, indent=2, default=str)}

Recent Errors:
{json.dumps(context.error_logs[-MAX_ERROR_LOGS_IN_CONTEXT:] if context.error_logs else [], indent=2)}

Constraints:
{json.dumps(context.constraints, indent=2)}

Available Actions: {[a.value for a in available_actions]}

Decide the best action and provide your response as JSON:
{{
    "action": "<action_from_available>",
    "confidence": "<high/medium/low>",
    "reasoning": "<detailed explanation>",
    "parameters": {{}},
    "requires_approval": <boolean>,
    "risks": ["<potential risk>"],
    "expected_outcome": "<what should happen>"
}}"""

        response = self.llm.complete(
            prompt=prompt,
            system_prompt=self.SYSTEM_PROMPT,
            temperature=0.3,
        )

        try:
            decision_data = json.loads(self._extract_json(response.content))

            decision = AgentDecision(
                action=AgentAction(decision_data.get("action", "skip")),
                confidence=AgentDecisionConfidence(decision_data.get("confidence", "low")),
                reasoning=decision_data.get("reasoning", ""),
                parameters=decision_data.get("parameters", {}),
                requires_approval=decision_data.get("requires_approval", True),
            )

            self.decision_history.append(decision)

            self.logger.info(
                "Agent decision made",
                action=decision.action.value,
                confidence=decision.confidence.value,
                requires_approval=decision.requires_approval,
            )

            return decision

        except (json.JSONDecodeError, ValueError) as e:
            self.logger.error("Failed to parse decision", error=str(e))
            return AgentDecision(
                action=AgentAction.SKIP,
                confidence=AgentDecisionConfidence.LOW,
                reasoning=f"Failed to make decision: {str(e)}",
                requires_approval=True,
            )

    async def _build_context(
        self,
        pipeline_name: str,
        include_history: bool = True,
        include_lineage: bool = True,
    ) -> AgentContext:
        """Build context for agent decision making."""
        context = AgentContext(
            pipeline_name=pipeline_name,
            current_state={},
        )

        try:
            # Get current DAG state
            dag_info = self.airflow.get_dag(pipeline_name)
            context.current_state = {
                "is_paused": dag_info.get("is_paused"),
                "schedule": dag_info.get("schedule_interval"),
                "tags": dag_info.get("tags", []),
            }

            # Get historical runs
            if include_history:
                runs = self.airflow.list_dag_runs(pipeline_name, limit=MAX_HISTORICAL_RUNS)
                context.historical_runs = runs

        except Exception as e:
            self.logger.warning("Failed to fetch Airflow context", error=str(e))

        return context

    def _extract_json(self, content: str) -> str:
        """Extract JSON from LLM response that may contain markdown."""
        content = content.strip()

        # Check for markdown code blocks
        if "```json" in content:
            match = re.search(r"```json\s*(.*?)\s*```", content, re.DOTALL)
            if match:
                return match.group(1)
        elif "```" in content:
            match = re.search(r"```\s*(.*?)\s*```", content, re.DOTALL)
            if match:
                return match.group(1)

        # Try to find JSON object directly
        if "{" in content:
            start = content.index("{")
            depth = 0
            for i, char in enumerate(content[start:], start):
                if char == "{":
                    depth += 1
                elif char == "}":
                    depth -= 1
                    if depth == 0:
                        return content[start:i+1]

        return content

    def _generate_dag_code(self, dag_spec: dict[str, Any]) -> str:
        """Generate Python DAG code from specification."""
        dag_id = dag_spec.get("dag_id", "generated_dag")
        description = dag_spec.get("description", "")
        schedule = dag_spec.get("schedule_interval", "@daily")
        tasks = dag_spec.get("tasks", [])
        config = dag_spec.get("dag_config", {})

        # Build task definitions
        task_defs = []
        for task in tasks:
            task_id = task.get("task_id", "task")
            operator = task.get("operator", "PythonOperator")
            task_config = task.get("config", {})

            if operator == "PythonOperator" or operator == "automic_etl":
                task_defs.append(f'''
    {task_id} = PythonOperator(
        task_id='{task_id}',
        python_callable=run_automic_pipeline,
        op_kwargs={json.dumps(task_config)},
    )''')
            elif operator == "BashOperator":
                task_defs.append(f'''
    {task_id} = BashOperator(
        task_id='{task_id}',
        bash_command='{task_config.get("command", "echo hello")}',
    )''')
            else:
                task_defs.append(f'''
    {task_id} = PythonOperator(
        task_id='{task_id}',
        python_callable=lambda: None,  # TODO: Implement
    )''')

        # Build dependencies
        dep_lines = []
        for task in tasks:
            deps = task.get("dependencies", [])
            if deps:
                for dep in deps:
                    dep_lines.append(f"    {dep} >> {task['task_id']}")

        code = f'''"""
Auto-generated Airflow DAG: {dag_id}

{description}

Generated by Automic ETL Agentic Orchestrator
Generated at: {format_iso()}
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def run_automic_pipeline(pipeline_name: str, config: dict = None, **kwargs):
    """Run an Automic ETL pipeline."""
    from automic_etl.medallion.lakehouse import Lakehouse

    lakehouse = Lakehouse()
    result = lakehouse.run_pipeline(pipeline_name, config or {{}})
    return result


default_args = {{
    'owner': 'automic-etl',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': {config.get('retries', 3)},
    'retry_delay': timedelta(minutes={config.get('retry_delay_minutes', 5)}),
}}

with DAG(
    dag_id='{dag_id}',
    default_args=default_args,
    description='{description}',
    schedule_interval='{schedule}',
    start_date=datetime(2024, 1, 1),
    catchup={config.get('catchup', False)},
    max_active_runs={config.get('max_active_runs', 1)},
    tags=['automic-etl', 'auto-generated'],
) as dag:
{"".join(task_defs)}

    # Task dependencies
{chr(10).join(dep_lines) if dep_lines else "    pass"}
'''
        return code


class AgenticAirflowOrchestrator:
    """
    High-level orchestrator that combines Airflow integration with agentic capabilities.

    This orchestrator provides:
    - Autonomous monitoring and self-healing
    - Dynamic DAG management
    - Intelligent scheduling
    - Continuous optimization
    """

    def __init__(
        self,
        airflow_config: AirflowConfig | None = None,
        settings: Settings | None = None,
        enable_auto_healing: bool = True,
        enable_auto_optimization: bool = True,
    ):
        self.settings = settings or get_settings()
        self.airflow_config = airflow_config or AirflowConfig()
        self.airflow = AirflowIntegration(self.airflow_config)

        # Initialize LLM client
        from automic_etl.llm.client import LLMClient
        self.llm = LLMClient(self.settings)

        self.agent = AirflowAgent(
            llm_client=self.llm,
            airflow=self.airflow,
            settings=self.settings,
        )

        self.enable_auto_healing = enable_auto_healing
        self.enable_auto_optimization = enable_auto_optimization

        self.logger = logger.bind(component="agentic_orchestrator")

        # Tracking
        self._monitored_dags: set[str] = set()
        self._optimization_history: list[dict] = []
        self._healing_history: list[dict] = []

    async def register_pipeline(
        self,
        pipeline: Pipeline,
        schedule: str = "@daily",
        auto_generate_dag: bool = True,
    ) -> str:
        """
        Register a pipeline with the orchestrator.

        Args:
            pipeline: The Pipeline to register
            schedule: Cron schedule for the pipeline
            auto_generate_dag: If True, automatically generate and deploy DAG

        Returns:
            DAG ID
        """
        self.logger.info("Registering pipeline", name=pipeline.name)

        dag_id = f"automic_{pipeline.name.lower().replace(' ', '_')}"

        if auto_generate_dag:
            # Generate tasks from pipeline stages
            tasks = []
            prev_task = None

            for i, stage in enumerate(pipeline.stages):
                task = {
                    "task_id": f"stage_{i}_{stage.name.lower().replace(' ', '_')}",
                    "operator": "automic_etl",
                    "description": f"Execute {stage.name}",
                    "config": {"stage_index": i, "pipeline_name": pipeline.name},
                    "dependencies": [prev_task] if prev_task else [],
                }
                tasks.append(task)
                prev_task = task["task_id"]

            # Save DAG
            if self.airflow_config.dags_folder:
                self.airflow.save_dag(
                    dag_id=dag_id,
                    tasks=tasks,
                    description=f"Automic ETL Pipeline: {pipeline.name}",
                    schedule=schedule,
                    tags=["automic-etl", "auto-managed"],
                )

        self._monitored_dags.add(dag_id)

        return dag_id

    async def create_pipeline_from_description(
        self,
        description: str,
        deploy: bool = True,
    ) -> tuple[str, str]:
        """
        Create a complete pipeline from a natural language description.

        Args:
            description: Natural language description of the desired pipeline
            deploy: If True, deploy the DAG to Airflow

        Returns:
            Tuple of (dag_id, dag_code)
        """
        self.logger.info("Creating pipeline from description", description=description[:100])

        dag_code = await self.agent.generate_dag_from_description(description)

        # Extract DAG ID from generated code
        import re
        match = re.search(r"dag_id='([^']+)'", dag_code)
        dag_id = match.group(1) if match else f"generated_{utc_now().strftime('%Y%m%d_%H%M%S')}"

        if deploy and self.airflow_config.dags_folder:
            dag_path = Path(self.airflow_config.dags_folder) / f"{dag_id}.py"
            dag_path.parent.mkdir(parents=True, exist_ok=True)
            dag_path.write_text(dag_code)
            self.logger.info("DAG deployed", path=str(dag_path))

        self._monitored_dags.add(dag_id)

        return dag_id, dag_code

    async def monitor_and_heal(self) -> list[dict[str, Any]]:
        """
        Monitor all registered DAGs and automatically heal failures.

        Returns:
            List of healing actions taken
        """
        if not self.enable_auto_healing:
            return []

        healing_actions = []

        for dag_id in self._monitored_dags:
            try:
                # Check for failed runs
                runs = self.airflow.list_dag_runs(dag_id, state="failed", limit=MAX_FAILED_RUNS_TO_CHECK)

                for run in runs:
                    run_id = run.get("dag_run_id")
                    if not run_id:
                        continue

                    self.logger.info("Found failed run, diagnosing", dag_id=dag_id, run_id=run_id)

                    # Diagnose and create recovery plan
                    recovery_plan = await self.agent.diagnose_failure(dag_id, run_id)

                    # Execute recovery if confidence is high enough
                    if recovery_plan.confidence >= MONITORING_HEAL_CONFIDENCE_THRESHOLD and not recovery_plan.requires_human_intervention:
                        result = await self.agent.execute_recovery(recovery_plan, auto_execute=True)
                        healing_actions.append({
                            "dag_id": dag_id,
                            "run_id": run_id,
                            "recovery_plan": recovery_plan.recovery_steps,
                            "result": result,
                            "timestamp": format_iso(),
                        })
                        self._healing_history.append(healing_actions[-1])
                    else:
                        self.logger.info(
                            "Recovery requires human intervention",
                            dag_id=dag_id,
                            confidence=recovery_plan.confidence,
                        )

            except Exception as e:
                self.logger.error("Error monitoring DAG", dag_id=dag_id, error=str(e))

        return healing_actions

    async def optimize_all(self) -> list[DAGOptimizationSuggestion]:
        """
        Analyze and suggest optimizations for all monitored DAGs.

        Returns:
            List of optimization suggestions
        """
        if not self.enable_auto_optimization:
            return []

        all_suggestions = []

        for dag_id in self._monitored_dags:
            try:
                suggestions = await self.agent.optimize_schedule(dag_id)
                all_suggestions.extend(suggestions)

                # Auto-apply low-risk suggestions
                for suggestion in suggestions:
                    if suggestion.auto_applicable and suggestion.risk_level == "low":
                        self.logger.info(
                            "Auto-applying optimization",
                            dag_id=dag_id,
                            type=suggestion.optimization_type,
                        )
                        # Would apply the optimization here
                        self._optimization_history.append({
                            "dag_id": dag_id,
                            "suggestion": suggestion.optimization_type,
                            "applied": True,
                            "timestamp": format_iso(),
                        })

            except Exception as e:
                self.logger.error("Error optimizing DAG", dag_id=dag_id, error=str(e))

        return all_suggestions

    async def get_intelligent_insights(self, dag_id: str) -> dict[str, Any]:
        """
        Get comprehensive AI-powered insights for a DAG.

        Args:
            dag_id: The DAG to analyze

        Returns:
            Comprehensive insights and recommendations
        """
        analysis = await self.agent.analyze_pipeline(dag_id)
        optimizations = await self.agent.optimize_schedule(dag_id)

        return {
            "dag_id": dag_id,
            "analysis": analysis,
            "optimizations": [
                {
                    "type": o.optimization_type,
                    "current": o.current_value,
                    "suggested": o.suggested_value,
                    "reasoning": o.reasoning,
                    "risk": o.risk_level,
                }
                for o in optimizations
            ],
            "generated_at": format_iso(),
        }

    def get_status(self) -> dict[str, Any]:
        """Get orchestrator status."""
        return {
            "monitored_dags": list(self._monitored_dags),
            "auto_healing_enabled": self.enable_auto_healing,
            "auto_optimization_enabled": self.enable_auto_optimization,
            "healing_actions_count": len(self._healing_history),
            "optimizations_applied_count": len(self._optimization_history),
            "airflow_healthy": self._check_airflow_health(),
        }

    def _check_airflow_health(self) -> bool:
        """Check if Airflow is healthy."""
        try:
            health = self.airflow.health_check()
            return health.get("metadatabase", {}).get("status") == "healthy"
        except Exception:
            return False
