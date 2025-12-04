"""Agentic Pipeline Optimizer for Airflow.

This module provides AI-powered optimization capabilities for Airflow pipelines:
- Performance analysis and bottleneck detection
- Resource allocation optimization
- Schedule optimization based on data patterns
- Dependency graph optimization
- Cost optimization strategies
"""

from __future__ import annotations

import json
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Literal

import structlog

from automic_etl.llm.client import LLMClient
from automic_etl.core.config import Settings, get_settings
from automic_etl.core.utils import utc_now, format_iso
from automic_etl.integrations.airflow import AirflowIntegration, AirflowConfig

logger = structlog.get_logger()


class OptimizationType(str, Enum):
    """Types of optimizations."""
    SCHEDULE = "schedule"
    PARALLELISM = "parallelism"
    RESOURCES = "resources"
    DEPENDENCIES = "dependencies"
    RETRY_POLICY = "retry_policy"
    TIMEOUT = "timeout"
    POOL = "pool"
    PRIORITY = "priority"
    BATCH_SIZE = "batch_size"
    CACHING = "caching"


class RiskLevel(str, Enum):
    """Risk levels for optimizations."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


@dataclass
class PerformanceMetrics:
    """Pipeline performance metrics."""
    dag_id: str
    total_runs: int
    success_rate: float
    avg_duration_seconds: float
    p95_duration_seconds: float
    max_duration_seconds: float
    min_duration_seconds: float
    avg_task_count: int
    failed_tasks_rate: float
    retry_rate: float
    data_collected_at: datetime = field(default_factory=utc_now)

    @classmethod
    def from_runs(cls, dag_id: str, runs: list[dict]) -> "PerformanceMetrics":
        """Create metrics from run data."""
        if not runs:
            return cls(
                dag_id=dag_id,
                total_runs=0,
                success_rate=0.0,
                avg_duration_seconds=0.0,
                p95_duration_seconds=0.0,
                max_duration_seconds=0.0,
                min_duration_seconds=0.0,
                avg_task_count=0,
                failed_tasks_rate=0.0,
                retry_rate=0.0,
            )

        durations = []
        successes = 0
        failures = 0

        for run in runs:
            if run.get("state") == "success":
                successes += 1
            elif run.get("state") == "failed":
                failures += 1

            if run.get("start_date") and run.get("end_date"):
                try:
                    start = datetime.fromisoformat(str(run["start_date"]).replace("Z", "+00:00"))
                    end = datetime.fromisoformat(str(run["end_date"]).replace("Z", "+00:00"))
                    durations.append((end - start).total_seconds())
                except (ValueError, TypeError):
                    pass

        if durations:
            sorted_durations = sorted(durations)
            p95_index = int(len(sorted_durations) * 0.95)
            p95_duration = sorted_durations[p95_index] if p95_index < len(sorted_durations) else sorted_durations[-1]
        else:
            p95_duration = 0.0

        return cls(
            dag_id=dag_id,
            total_runs=len(runs),
            success_rate=successes / len(runs) if runs else 0.0,
            avg_duration_seconds=statistics.mean(durations) if durations else 0.0,
            p95_duration_seconds=p95_duration,
            max_duration_seconds=max(durations) if durations else 0.0,
            min_duration_seconds=min(durations) if durations else 0.0,
            avg_task_count=0,  # Would need task data
            failed_tasks_rate=failures / len(runs) if runs else 0.0,
            retry_rate=0.0,  # Would need retry data
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "dag_id": self.dag_id,
            "total_runs": self.total_runs,
            "success_rate": f"{self.success_rate * 100:.1f}%",
            "avg_duration_seconds": round(self.avg_duration_seconds, 2),
            "p95_duration_seconds": round(self.p95_duration_seconds, 2),
            "max_duration_seconds": round(self.max_duration_seconds, 2),
            "min_duration_seconds": round(self.min_duration_seconds, 2),
            "failed_tasks_rate": f"{self.failed_tasks_rate * 100:.1f}%",
        }


@dataclass
class OptimizationRecommendation:
    """A single optimization recommendation."""
    optimization_type: OptimizationType
    title: str
    description: str
    current_value: Any
    recommended_value: Any
    expected_improvement: str
    reasoning: str
    risk_level: RiskLevel
    implementation_steps: list[str]
    auto_applicable: bool
    estimated_impact: dict[str, Any] = field(default_factory=dict)
    prerequisites: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "type": self.optimization_type.value,
            "title": self.title,
            "description": self.description,
            "current_value": self.current_value,
            "recommended_value": self.recommended_value,
            "expected_improvement": self.expected_improvement,
            "reasoning": self.reasoning,
            "risk_level": self.risk_level.value,
            "implementation_steps": self.implementation_steps,
            "auto_applicable": self.auto_applicable,
            "estimated_impact": self.estimated_impact,
        }


@dataclass
class OptimizationPlan:
    """Complete optimization plan for a DAG."""
    dag_id: str
    metrics: PerformanceMetrics
    recommendations: list[OptimizationRecommendation]
    overall_health_score: int
    priority_actions: list[str]
    estimated_total_improvement: str
    generated_at: datetime = field(default_factory=utc_now)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "dag_id": self.dag_id,
            "metrics": self.metrics.to_dict(),
            "recommendations": [r.to_dict() for r in self.recommendations],
            "overall_health_score": self.overall_health_score,
            "priority_actions": self.priority_actions,
            "estimated_total_improvement": self.estimated_total_improvement,
            "generated_at": self.generated_at.isoformat(),
        }


class PipelineOptimizer:
    """
    AI-powered pipeline optimizer for Airflow DAGs.

    Features:
    - Performance analysis based on historical runs
    - Bottleneck detection and resolution suggestions
    - Schedule optimization
    - Resource allocation recommendations
    - Dependency graph analysis
    """

    SYSTEM_PROMPT = """You are an expert data pipeline optimization specialist with deep knowledge of Apache Airflow, distributed systems, and ETL best practices.

Your role is to analyze pipeline performance metrics and provide actionable optimization recommendations.

When analyzing pipelines:
1. Consider both performance and reliability
2. Identify bottlenecks using statistical analysis
3. Suggest optimizations with clear implementation steps
4. Assess risks honestly and provide mitigation strategies
5. Prioritize high-impact, low-risk improvements
6. Consider resource constraints and cost implications

Always provide structured, actionable recommendations with measurable expected outcomes."""

    def __init__(
        self,
        settings: Settings | None = None,
        airflow_config: AirflowConfig | None = None,
    ):
        self.settings = settings or get_settings()
        self.airflow_config = airflow_config or AirflowConfig()
        self.airflow = AirflowIntegration(self.airflow_config)
        self.llm = LLMClient(self.settings)
        self.logger = logger.bind(component="pipeline_optimizer")

    async def analyze_and_optimize(
        self,
        dag_id: str,
        optimization_focus: Literal["performance", "reliability", "cost", "all"] = "all",
        lookback_days: int = 30,
    ) -> OptimizationPlan:
        """
        Perform comprehensive analysis and generate optimization plan.

        Args:
            dag_id: DAG to optimize
            optimization_focus: What to optimize for
            lookback_days: Days of history to analyze

        Returns:
            Complete optimization plan
        """
        self.logger.info("Starting optimization analysis", dag_id=dag_id, focus=optimization_focus)

        # Gather metrics
        metrics = await self._collect_metrics(dag_id, lookback_days)

        # Get DAG configuration
        dag_config = await self._get_dag_config(dag_id)

        # Generate recommendations using LLM
        recommendations = await self._generate_recommendations(
            dag_id, metrics, dag_config, optimization_focus
        )

        # Calculate health score
        health_score = self._calculate_health_score(metrics)

        # Prioritize actions
        priority_actions = self._prioritize_recommendations(recommendations)

        # Estimate total improvement
        total_improvement = self._estimate_total_improvement(recommendations)

        plan = OptimizationPlan(
            dag_id=dag_id,
            metrics=metrics,
            recommendations=recommendations,
            overall_health_score=health_score,
            priority_actions=priority_actions,
            estimated_total_improvement=total_improvement,
        )

        self.logger.info(
            "Optimization analysis complete",
            dag_id=dag_id,
            health_score=health_score,
            recommendations_count=len(recommendations),
        )

        return plan

    async def quick_optimize(
        self,
        dag_id: str,
        target: OptimizationType,
    ) -> OptimizationRecommendation | None:
        """
        Get a quick optimization for a specific aspect.

        Args:
            dag_id: DAG to optimize
            target: Specific optimization type

        Returns:
            Single recommendation or None
        """
        self.logger.info("Quick optimization", dag_id=dag_id, target=target.value)

        metrics = await self._collect_metrics(dag_id, lookback_days=7)
        dag_config = await self._get_dag_config(dag_id)

        prompt = f"""Provide a single optimization recommendation for:

DAG: {dag_id}
Optimization Target: {target.value}

Current Metrics:
{json.dumps(metrics.to_dict(), indent=2)}

DAG Configuration:
{json.dumps(dag_config, indent=2)}

Provide your recommendation as JSON:
{{
    "title": "<short title>",
    "description": "<detailed description>",
    "current_value": "<current setting>",
    "recommended_value": "<recommended setting>",
    "expected_improvement": "<expected improvement>",
    "reasoning": "<why this change helps>",
    "risk_level": "<low/medium/high>",
    "implementation_steps": ["<step 1>", "<step 2>"],
    "auto_applicable": <boolean>
}}"""

        response = self.llm.complete(
            prompt=prompt,
            system_prompt=self.SYSTEM_PROMPT,
            temperature=0.3,
        )

        try:
            rec_data = json.loads(self._extract_json(response.content))

            return OptimizationRecommendation(
                optimization_type=target,
                title=rec_data.get("title", ""),
                description=rec_data.get("description", ""),
                current_value=rec_data.get("current_value"),
                recommended_value=rec_data.get("recommended_value"),
                expected_improvement=rec_data.get("expected_improvement", ""),
                reasoning=rec_data.get("reasoning", ""),
                risk_level=RiskLevel(rec_data.get("risk_level", "medium")),
                implementation_steps=rec_data.get("implementation_steps", []),
                auto_applicable=rec_data.get("auto_applicable", False),
            )

        except (json.JSONDecodeError, ValueError) as e:
            self.logger.error("Failed to parse recommendation", error=str(e))
            return None

    async def apply_optimization(
        self,
        recommendation: OptimizationRecommendation,
        dag_id: str,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        """
        Apply an optimization recommendation.

        Args:
            recommendation: The recommendation to apply
            dag_id: DAG to apply to
            dry_run: If True, only simulate the change

        Returns:
            Result of the application
        """
        self.logger.info(
            "Applying optimization",
            dag_id=dag_id,
            type=recommendation.optimization_type.value,
            dry_run=dry_run,
        )

        if dry_run:
            return {
                "status": "dry_run",
                "dag_id": dag_id,
                "optimization": recommendation.optimization_type.value,
                "would_change": {
                    "from": recommendation.current_value,
                    "to": recommendation.recommended_value,
                },
                "steps": recommendation.implementation_steps,
            }

        # Apply based on optimization type
        result = {"status": "not_implemented"}

        if recommendation.optimization_type == OptimizationType.SCHEDULE:
            # Update DAG schedule through Airflow API or DAG file modification
            result = await self._apply_schedule_change(dag_id, recommendation)

        elif recommendation.optimization_type == OptimizationType.RETRY_POLICY:
            result = await self._apply_retry_policy_change(dag_id, recommendation)

        elif recommendation.optimization_type == OptimizationType.PARALLELISM:
            result = await self._apply_parallelism_change(dag_id, recommendation)

        return result

    async def compare_configurations(
        self,
        dag_id: str,
        config_a: dict[str, Any],
        config_b: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Compare two DAG configurations and recommend the better one.

        Args:
            dag_id: DAG ID for context
            config_a: First configuration
            config_b: Second configuration

        Returns:
            Comparison analysis with recommendation
        """
        prompt = f"""Compare these two DAG configurations and recommend the better option:

DAG: {dag_id}

Configuration A:
{json.dumps(config_a, indent=2)}

Configuration B:
{json.dumps(config_b, indent=2)}

Analyze both configurations and provide:
{{
    "recommended": "<A or B>",
    "confidence": <0-1>,
    "comparison": {{
        "performance": {{"winner": "<A or B>", "reason": "<why>"}},
        "reliability": {{"winner": "<A or B>", "reason": "<why>"}},
        "cost": {{"winner": "<A or B>", "reason": "<why>"}},
        "maintainability": {{"winner": "<A or B>", "reason": "<why>"}}
    }},
    "trade_offs": ["<trade off 1>", "<trade off 2>"],
    "summary": "<overall recommendation summary>"
}}"""

        response = self.llm.complete(
            prompt=prompt,
            system_prompt=self.SYSTEM_PROMPT,
            temperature=0.3,
        )

        try:
            return json.loads(self._extract_json(response.content))
        except json.JSONDecodeError:
            return {"error": "Failed to parse comparison", "raw": response.content}

    async def predict_impact(
        self,
        dag_id: str,
        proposed_change: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Predict the impact of a proposed change.

        Args:
            dag_id: DAG to analyze
            proposed_change: The change being proposed

        Returns:
            Impact prediction with confidence
        """
        metrics = await self._collect_metrics(dag_id, lookback_days=14)

        prompt = f"""Predict the impact of this proposed change:

DAG: {dag_id}

Current Metrics:
{json.dumps(metrics.to_dict(), indent=2)}

Proposed Change:
{json.dumps(proposed_change, indent=2)}

Predict the impact:
{{
    "performance_impact": {{
        "duration_change_percent": <number>,
        "throughput_change_percent": <number>,
        "confidence": <0-1>
    }},
    "reliability_impact": {{
        "success_rate_change_percent": <number>,
        "failure_risk_change": "<increase/decrease/neutral>",
        "confidence": <0-1>
    }},
    "resource_impact": {{
        "cpu_change_percent": <number>,
        "memory_change_percent": <number>,
        "cost_change_percent": <number>
    }},
    "risks": ["<potential risk>"],
    "rollback_complexity": "<low/medium/high>",
    "recommendation": "<proceed/caution/avoid>",
    "reasoning": "<explanation>"
}}"""

        response = self.llm.complete(
            prompt=prompt,
            system_prompt=self.SYSTEM_PROMPT,
            temperature=0.3,
        )

        try:
            return json.loads(self._extract_json(response.content))
        except json.JSONDecodeError:
            return {"error": "Failed to predict impact"}

    async def _collect_metrics(
        self,
        dag_id: str,
        lookback_days: int,
    ) -> PerformanceMetrics:
        """Collect performance metrics for a DAG."""
        try:
            runs = self.airflow.list_dag_runs(dag_id, limit=100)
            return PerformanceMetrics.from_runs(dag_id, runs)
        except Exception as e:
            self.logger.warning("Failed to collect metrics", error=str(e))
            return PerformanceMetrics.from_runs(dag_id, [])

    async def _get_dag_config(self, dag_id: str) -> dict[str, Any]:
        """Get DAG configuration."""
        try:
            dag_info = self.airflow.get_dag(dag_id)
            return {
                "schedule_interval": dag_info.get("schedule_interval"),
                "is_paused": dag_info.get("is_paused"),
                "tags": dag_info.get("tags", []),
                "max_active_runs": dag_info.get("max_active_runs", 16),
                "catchup": dag_info.get("catchup", True),
            }
        except Exception as e:
            self.logger.warning("Failed to get DAG config", error=str(e))
            return {}

    async def _generate_recommendations(
        self,
        dag_id: str,
        metrics: PerformanceMetrics,
        dag_config: dict[str, Any],
        focus: str,
    ) -> list[OptimizationRecommendation]:
        """Generate optimization recommendations using LLM."""
        prompt = f"""Analyze this DAG and provide optimization recommendations:

DAG: {dag_id}
Optimization Focus: {focus}

Performance Metrics:
{json.dumps(metrics.to_dict(), indent=2)}

DAG Configuration:
{json.dumps(dag_config, indent=2)}

Provide 3-5 prioritized recommendations as JSON:
{{
    "recommendations": [
        {{
            "type": "<schedule/parallelism/resources/dependencies/retry_policy/timeout/pool/priority/batch_size/caching>",
            "title": "<short title>",
            "description": "<detailed description>",
            "current_value": "<current>",
            "recommended_value": "<recommended>",
            "expected_improvement": "<e.g., 20% faster execution>",
            "reasoning": "<why this helps>",
            "risk_level": "<low/medium/high>",
            "implementation_steps": ["<step>"],
            "auto_applicable": <boolean>,
            "estimated_impact": {{
                "duration_reduction_percent": <number>,
                "reliability_improvement_percent": <number>
            }}
        }}
    ]
}}"""

        response = self.llm.complete(
            prompt=prompt,
            system_prompt=self.SYSTEM_PROMPT,
            temperature=0.4,
        )

        try:
            data = json.loads(self._extract_json(response.content))
            recommendations = []

            for rec in data.get("recommendations", []):
                try:
                    recommendations.append(OptimizationRecommendation(
                        optimization_type=OptimizationType(rec.get("type", "schedule")),
                        title=rec.get("title", ""),
                        description=rec.get("description", ""),
                        current_value=rec.get("current_value"),
                        recommended_value=rec.get("recommended_value"),
                        expected_improvement=rec.get("expected_improvement", ""),
                        reasoning=rec.get("reasoning", ""),
                        risk_level=RiskLevel(rec.get("risk_level", "medium")),
                        implementation_steps=rec.get("implementation_steps", []),
                        auto_applicable=rec.get("auto_applicable", False),
                        estimated_impact=rec.get("estimated_impact", {}),
                    ))
                except (ValueError, KeyError) as e:
                    self.logger.warning("Failed to parse recommendation", error=str(e))
                    continue

            return recommendations

        except json.JSONDecodeError:
            self.logger.error("Failed to parse recommendations")
            return []

    def _calculate_health_score(self, metrics: PerformanceMetrics) -> int:
        """Calculate overall health score (0-100)."""
        score = 100

        # Deduct for low success rate
        if metrics.success_rate < 0.99:
            score -= int((1 - metrics.success_rate) * 50)

        # Deduct for high variability
        if metrics.max_duration_seconds > 0 and metrics.min_duration_seconds > 0:
            variability = metrics.max_duration_seconds / metrics.min_duration_seconds
            if variability > 2:
                score -= min(20, int((variability - 2) * 5))

        # Deduct for high failure rate
        if metrics.failed_tasks_rate > 0.01:
            score -= min(30, int(metrics.failed_tasks_rate * 100))

        return max(0, min(100, score))

    def _prioritize_recommendations(
        self,
        recommendations: list[OptimizationRecommendation],
    ) -> list[str]:
        """Prioritize recommendations and return top actions."""
        # Sort by risk (low first) and auto_applicable (true first)
        sorted_recs = sorted(
            recommendations,
            key=lambda r: (
                {"low": 0, "medium": 1, "high": 2}[r.risk_level.value],
                0 if r.auto_applicable else 1,
            ),
        )

        return [r.title for r in sorted_recs[:3]]

    def _estimate_total_improvement(
        self,
        recommendations: list[OptimizationRecommendation],
    ) -> str:
        """Estimate total improvement from all recommendations."""
        total_duration_reduction = 0
        total_reliability_improvement = 0

        for rec in recommendations:
            impact = rec.estimated_impact
            total_duration_reduction += impact.get("duration_reduction_percent", 0)
            total_reliability_improvement += impact.get("reliability_improvement_percent", 0)

        parts = []
        if total_duration_reduction > 0:
            parts.append(f"{total_duration_reduction:.0f}% faster execution")
        if total_reliability_improvement > 0:
            parts.append(f"{total_reliability_improvement:.0f}% more reliable")

        return ", ".join(parts) if parts else "Improvements vary based on implementation"

    async def _apply_schedule_change(
        self,
        dag_id: str,
        recommendation: OptimizationRecommendation,
    ) -> dict[str, Any]:
        """Apply a schedule change."""
        # This would typically modify the DAG file or use Airflow API
        # For now, return a placeholder
        return {
            "status": "requires_dag_file_modification",
            "dag_id": dag_id,
            "change": {
                "type": "schedule",
                "from": recommendation.current_value,
                "to": recommendation.recommended_value,
            },
            "instructions": recommendation.implementation_steps,
        }

    async def _apply_retry_policy_change(
        self,
        dag_id: str,
        recommendation: OptimizationRecommendation,
    ) -> dict[str, Any]:
        """Apply a retry policy change."""
        return {
            "status": "requires_dag_file_modification",
            "dag_id": dag_id,
            "change": {
                "type": "retry_policy",
                "from": recommendation.current_value,
                "to": recommendation.recommended_value,
            },
            "instructions": recommendation.implementation_steps,
        }

    async def _apply_parallelism_change(
        self,
        dag_id: str,
        recommendation: OptimizationRecommendation,
    ) -> dict[str, Any]:
        """Apply a parallelism change."""
        return {
            "status": "requires_dag_file_modification",
            "dag_id": dag_id,
            "change": {
                "type": "parallelism",
                "from": recommendation.current_value,
                "to": recommendation.recommended_value,
            },
            "instructions": recommendation.implementation_steps,
        }

    def _extract_json(self, content: str) -> str:
        """Extract JSON from response."""
        import re

        content = content.strip()

        if "```json" in content:
            match = re.search(r"```json\s*(.*?)\s*```", content, re.DOTALL)
            if match:
                return match.group(1)
        elif "```" in content:
            match = re.search(r"```\s*(.*?)\s*```", content, re.DOTALL)
            if match:
                return match.group(1)

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


class ContinuousOptimizer:
    """
    Continuous optimization service that monitors and optimizes DAGs automatically.

    Runs in the background and:
    - Monitors DAG performance
    - Detects degradation
    - Applies safe optimizations automatically
    - Alerts on issues requiring human intervention
    """

    def __init__(
        self,
        settings: Settings | None = None,
        check_interval_seconds: int = 300,
        auto_apply_low_risk: bool = True,
    ):
        self.settings = settings or get_settings()
        self.optimizer = PipelineOptimizer(settings)
        self.check_interval = check_interval_seconds
        self.auto_apply_low_risk = auto_apply_low_risk
        self._running = False
        self._monitored_dags: set[str] = set()
        self._optimization_history: list[dict] = []
        self.logger = logger.bind(component="continuous_optimizer")

    def add_dag(self, dag_id: str):
        """Add a DAG to continuous monitoring."""
        self._monitored_dags.add(dag_id)
        self.logger.info("Added DAG to monitoring", dag_id=dag_id)

    def remove_dag(self, dag_id: str):
        """Remove a DAG from monitoring."""
        self._monitored_dags.discard(dag_id)
        self.logger.info("Removed DAG from monitoring", dag_id=dag_id)

    async def run_optimization_cycle(self) -> list[dict]:
        """Run a single optimization cycle for all monitored DAGs."""
        results = []

        for dag_id in self._monitored_dags:
            try:
                plan = await self.optimizer.analyze_and_optimize(dag_id)

                # Apply low-risk, auto-applicable optimizations
                if self.auto_apply_low_risk:
                    for rec in plan.recommendations:
                        if rec.risk_level == RiskLevel.LOW and rec.auto_applicable:
                            result = await self.optimizer.apply_optimization(
                                rec, dag_id, dry_run=False
                            )
                            self._optimization_history.append({
                                "timestamp": format_iso(),
                                "dag_id": dag_id,
                                "optimization": rec.title,
                                "result": result,
                            })

                results.append({
                    "dag_id": dag_id,
                    "health_score": plan.overall_health_score,
                    "recommendations_count": len(plan.recommendations),
                    "applied_count": sum(
                        1 for r in plan.recommendations
                        if r.risk_level == RiskLevel.LOW and r.auto_applicable
                    ),
                })

            except Exception as e:
                self.logger.error("Optimization cycle failed", dag_id=dag_id, error=str(e))
                results.append({"dag_id": dag_id, "error": str(e)})

        return results

    def get_optimization_history(
        self,
        dag_id: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """Get optimization history."""
        history = self._optimization_history[-limit:]

        if dag_id:
            history = [h for h in history if h.get("dag_id") == dag_id]

        return history

    def get_status(self) -> dict[str, Any]:
        """Get optimizer status."""
        return {
            "running": self._running,
            "monitored_dags": list(self._monitored_dags),
            "auto_apply_enabled": self.auto_apply_low_risk,
            "check_interval_seconds": self.check_interval,
            "optimizations_applied": len(self._optimization_history),
        }
