"""Airflow API routes for Automic ETL.

This module provides REST API endpoints for:
- DAG management and orchestration
- Agentic pipeline operations
- Dynamic DAG generation
- Pipeline monitoring and recovery
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Query
from pydantic import BaseModel, Field

import structlog

logger = structlog.get_logger()

router = APIRouter(prefix="/airflow", tags=["airflow"])


# =============================================================================
# Request/Response Models
# =============================================================================

class AirflowConnectionConfig(BaseModel):
    """Airflow connection configuration."""
    base_url: str = Field(default="http://localhost:8080")
    username: str = Field(default="admin")
    password: str = Field(default="admin")
    api_version: str = Field(default="v1")


class DAGTriggerRequest(BaseModel):
    """Request to trigger a DAG."""
    dag_id: str
    conf: dict[str, Any] | None = None
    execution_date: datetime | None = None


class DAGGenerateRequest(BaseModel):
    """Request to generate a DAG."""
    name: str
    description: str | None = None
    source_type: str | None = None
    source_config: dict[str, Any] | None = None
    tables: list[str] | None = None
    schedule: str = "@daily"
    pattern: str | None = None  # medallion, cdc, streaming, etc.
    deploy: bool = False


class DAGFromDescriptionRequest(BaseModel):
    """Request to generate DAG from natural language."""
    description: str
    context: dict[str, Any] | None = None
    deploy: bool = False


class PipelineAnalysisRequest(BaseModel):
    """Request for pipeline analysis."""
    pipeline_name: str
    include_history: bool = True
    include_lineage: bool = True


class RecoveryRequest(BaseModel):
    """Request to recover a failed DAG run."""
    dag_id: str
    run_id: str
    task_id: str | None = None
    auto_execute: bool = False


class OptimizationRequest(BaseModel):
    """Request for DAG optimization."""
    dag_id: str
    optimization_goal: Literal["latency", "cost", "reliability"] = "reliability"


class AgentDecisionRequest(BaseModel):
    """Request for agentic decision making."""
    pipeline_name: str
    context: dict[str, Any] | None = None
    available_actions: list[str] | None = None


# Response models
class DAGInfo(BaseModel):
    """DAG information."""
    dag_id: str
    description: str | None = None
    schedule: str | None = None
    is_paused: bool = False
    tags: list[str] = []
    last_run_state: str | None = None


class DAGRunInfo(BaseModel):
    """DAG run information."""
    dag_id: str
    run_id: str
    state: str
    execution_date: datetime | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None


class TaskInfo(BaseModel):
    """Task instance information."""
    task_id: str
    state: str
    start_date: datetime | None = None
    end_date: datetime | None = None
    try_number: int = 1


class AnalysisResult(BaseModel):
    """Pipeline analysis result."""
    health_score: int
    performance_summary: str
    bottlenecks: list[dict[str, Any]]
    recommendations: list[dict[str, Any]]


class RecoveryPlan(BaseModel):
    """Recovery plan for failed task."""
    task_id: str
    dag_id: str
    failure_reason: str
    recovery_steps: list[dict[str, Any]]
    confidence: float
    requires_human: bool


class OptimizationSuggestion(BaseModel):
    """Optimization suggestion."""
    optimization_type: str
    current_value: Any
    suggested_value: Any
    reasoning: str
    risk_level: str


class GeneratedDAG(BaseModel):
    """Generated DAG response."""
    dag_id: str
    code: str
    deployed: bool = False
    pattern: str


class OrchestratorStatus(BaseModel):
    """Orchestrator status."""
    monitored_dags: list[str]
    auto_healing_enabled: bool
    auto_optimization_enabled: bool
    healing_actions_count: int
    airflow_healthy: bool


# =============================================================================
# Dependencies
# =============================================================================

def get_airflow_integration():
    """Get Airflow integration instance."""
    from automic_etl.integrations.airflow import AirflowIntegration, AirflowConfig
    return AirflowIntegration(AirflowConfig())


def get_agentic_orchestrator():
    """Get agentic orchestrator instance."""
    from automic_etl.integrations.airflow_agentic import AgenticAirflowOrchestrator
    return AgenticAirflowOrchestrator()


def get_dag_factory():
    """Get DAG factory instance."""
    from automic_etl.integrations.airflow_dag_factory import DAGFactory
    return DAGFactory()


# =============================================================================
# DAG Management Endpoints
# =============================================================================

@router.get("/dags", response_model=list[DAGInfo])
async def list_dags(
    limit: int = Query(default=100, ge=1, le=1000),
    tags: list[str] | None = Query(default=None),
    only_active: bool = True,
):
    """List all DAGs."""
    try:
        airflow = get_airflow_integration()
        dags = airflow.list_dags(limit=limit, tags=tags, only_active=only_active)

        return [
            DAGInfo(
                dag_id=dag["dag_id"],
                description=dag.get("description"),
                schedule=dag.get("schedule_interval"),
                is_paused=dag.get("is_paused", False),
                tags=dag.get("tags", []),
            )
            for dag in dags
        ]
    except Exception as e:
        logger.error("Failed to list DAGs", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dags/{dag_id}", response_model=DAGInfo)
async def get_dag(dag_id: str):
    """Get DAG details."""
    try:
        airflow = get_airflow_integration()
        dag = airflow.get_dag(dag_id)

        return DAGInfo(
            dag_id=dag["dag_id"],
            description=dag.get("description"),
            schedule=dag.get("schedule_interval"),
            is_paused=dag.get("is_paused", False),
            tags=dag.get("tags", []),
        )
    except Exception as e:
        logger.error("Failed to get DAG", dag_id=dag_id, error=str(e))
        raise HTTPException(status_code=404, detail=f"DAG not found: {dag_id}")


@router.post("/dags/{dag_id}/trigger")
async def trigger_dag(dag_id: str, request: DAGTriggerRequest | None = None):
    """Trigger a DAG run."""
    try:
        airflow = get_airflow_integration()

        run_id = airflow.trigger_dag(
            dag_id=dag_id,
            conf=request.conf if request else None,
            execution_date=request.execution_date if request else None,
        )

        return {
            "dag_id": dag_id,
            "run_id": run_id,
            "status": "triggered",
            "message": f"DAG {dag_id} triggered successfully",
        }
    except Exception as e:
        logger.error("Failed to trigger DAG", dag_id=dag_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/dags/{dag_id}/pause")
async def pause_dag(dag_id: str):
    """Pause a DAG."""
    try:
        airflow = get_airflow_integration()
        airflow.pause_dag(dag_id)
        return {"dag_id": dag_id, "is_paused": True, "status": "paused"}
    except ConnectionError as e:
        logger.error("Connection error pausing DAG", dag_id=dag_id, error=str(e))
        raise HTTPException(status_code=503, detail=f"Airflow connection error: {e}")
    except TimeoutError as e:
        logger.error("Timeout pausing DAG", dag_id=dag_id, error=str(e))
        raise HTTPException(status_code=504, detail=f"Request timeout: {e}")
    except Exception as e:
        logger.error("Failed to pause DAG", dag_id=dag_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/dags/{dag_id}/unpause")
async def unpause_dag(dag_id: str):
    """Unpause a DAG."""
    try:
        airflow = get_airflow_integration()
        airflow.unpause_dag(dag_id)
        return {"dag_id": dag_id, "is_paused": False, "status": "unpaused"}
    except ConnectionError as e:
        logger.error("Connection error unpausing DAG", dag_id=dag_id, error=str(e))
        raise HTTPException(status_code=503, detail=f"Airflow connection error: {e}")
    except TimeoutError as e:
        logger.error("Timeout unpausing DAG", dag_id=dag_id, error=str(e))
        raise HTTPException(status_code=504, detail=f"Request timeout: {e}")
    except Exception as e:
        logger.error("Failed to unpause DAG", dag_id=dag_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# DAG Run Endpoints
# =============================================================================

@router.get("/dags/{dag_id}/runs", response_model=list[DAGRunInfo])
async def list_dag_runs(
    dag_id: str,
    limit: int = Query(default=25, ge=1, le=100),
    state: str | None = None,
):
    """List DAG runs."""
    try:
        airflow = get_airflow_integration()
        runs = airflow.list_dag_runs(dag_id, limit=limit, state=state)

        return [
            DAGRunInfo(
                dag_id=dag_id,
                run_id=run["dag_run_id"],
                state=run.get("state", "unknown"),
                execution_date=run.get("execution_date"),
                start_date=run.get("start_date"),
                end_date=run.get("end_date"),
            )
            for run in runs
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dags/{dag_id}/runs/{run_id}")
async def get_dag_run(dag_id: str, run_id: str):
    """Get DAG run details."""
    try:
        airflow = get_airflow_integration()
        run = airflow.get_dag_run(dag_id, run_id)
        return run
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")


@router.get("/dags/{dag_id}/runs/{run_id}/tasks", response_model=list[TaskInfo])
async def get_task_instances(dag_id: str, run_id: str):
    """Get task instances for a DAG run."""
    try:
        airflow = get_airflow_integration()
        tasks = airflow.get_task_instances(dag_id, run_id)

        return [
            TaskInfo(
                task_id=task["task_id"],
                state=task.get("state", "unknown"),
                start_date=task.get("start_date"),
                end_date=task.get("end_date"),
                try_number=task.get("try_number", 1),
            )
            for task in tasks
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dags/{dag_id}/runs/{run_id}/tasks/{task_id}/logs")
async def get_task_logs(dag_id: str, run_id: str, task_id: str, try_number: int = 1):
    """Get task logs."""
    try:
        airflow = get_airflow_integration()
        logs = airflow.get_task_logs(dag_id, run_id, task_id, try_number)
        return {"logs": logs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# DAG Generation Endpoints
# =============================================================================

@router.post("/generate", response_model=GeneratedDAG)
async def generate_dag(request: DAGGenerateRequest):
    """Generate a new DAG."""
    try:
        factory = get_dag_factory()

        if request.pattern == "medallion" and request.source_type and request.tables:
            dag_def, code = factory.create_medallion_dag(
                name=request.name,
                source_type=request.source_type,
                source_config=request.source_config or {},
                tables=request.tables,
                schedule=request.schedule,
                deploy=request.deploy,
            )
        else:
            from automic_etl.integrations.airflow_dag_factory import (
                DAGGenerationRequest as FactoryRequest,
                DataSource,
            )

            sources = []
            if request.source_type:
                sources.append(DataSource(
                    name=request.name,
                    source_type=request.source_type,
                    connection_config=request.source_config or {},
                    tables=request.tables,
                ))

            factory_request = FactoryRequest(
                name=request.name,
                description=request.description,
                sources=sources,
                schedule=request.schedule,
            )

            dag_def, code = factory.create_from_request(factory_request, deploy=request.deploy)

        return GeneratedDAG(
            dag_id=dag_def.dag_id,
            code=code,
            deployed=request.deploy,
            pattern=dag_def.pattern.value,
        )

    except Exception as e:
        logger.error("Failed to generate DAG", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/generate/from-description", response_model=GeneratedDAG)
async def generate_dag_from_description(request: DAGFromDescriptionRequest):
    """Generate a DAG from natural language description."""
    try:
        factory = get_dag_factory()

        dag_def, code = factory.create_from_natural_language(
            description=request.description,
            context=request.context,
            deploy=request.deploy,
        )

        return GeneratedDAG(
            dag_id=dag_def.dag_id,
            code=code,
            deployed=request.deploy,
            pattern=dag_def.pattern.value,
        )

    except Exception as e:
        logger.error("Failed to generate DAG from description", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Agentic Endpoints
# =============================================================================

@router.post("/analyze", response_model=AnalysisResult)
async def analyze_pipeline(request: PipelineAnalysisRequest):
    """Analyze a pipeline with AI-powered insights."""
    try:
        orchestrator = get_agentic_orchestrator()

        analysis = await orchestrator.agent.analyze_pipeline(
            pipeline_name=request.pipeline_name,
            include_history=request.include_history,
            include_lineage=request.include_lineage,
        )

        return AnalysisResult(
            health_score=analysis.get("health_score", 0),
            performance_summary=analysis.get("performance_summary", ""),
            bottlenecks=analysis.get("bottlenecks", []),
            recommendations=analysis.get("recommendations", []),
        )

    except Exception as e:
        logger.error("Failed to analyze pipeline", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/diagnose", response_model=RecoveryPlan)
async def diagnose_failure(request: RecoveryRequest):
    """Diagnose a DAG/task failure."""
    try:
        orchestrator = get_agentic_orchestrator()

        recovery_plan = await orchestrator.agent.diagnose_failure(
            dag_id=request.dag_id,
            run_id=request.run_id,
            task_id=request.task_id,
        )

        return RecoveryPlan(
            task_id=recovery_plan.task_id,
            dag_id=recovery_plan.dag_id,
            failure_reason=recovery_plan.failure_reason,
            recovery_steps=recovery_plan.recovery_steps,
            confidence=recovery_plan.confidence,
            requires_human=recovery_plan.requires_human_intervention,
        )

    except Exception as e:
        logger.error("Failed to diagnose failure", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/recover")
async def execute_recovery(request: RecoveryRequest, background_tasks: BackgroundTasks):
    """Execute a recovery plan for a failed task."""
    try:
        orchestrator = get_agentic_orchestrator()

        # First diagnose
        recovery_plan = await orchestrator.agent.diagnose_failure(
            dag_id=request.dag_id,
            run_id=request.run_id,
            task_id=request.task_id,
        )

        # Execute recovery
        result = await orchestrator.agent.execute_recovery(
            recovery_plan=recovery_plan,
            auto_execute=request.auto_execute,
        )

        return result

    except Exception as e:
        logger.error("Failed to execute recovery", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/optimize", response_model=list[OptimizationSuggestion])
async def get_optimization_suggestions(request: OptimizationRequest):
    """Get optimization suggestions for a DAG."""
    try:
        orchestrator = get_agentic_orchestrator()

        suggestions = await orchestrator.agent.optimize_schedule(
            dag_id=request.dag_id,
            optimization_goal=request.optimization_goal,
        )

        return [
            OptimizationSuggestion(
                optimization_type=s.optimization_type,
                current_value=s.current_value,
                suggested_value=s.suggested_value,
                reasoning=s.reasoning,
                risk_level=s.risk_level,
            )
            for s in suggestions
        ]

    except Exception as e:
        logger.error("Failed to get optimizations", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/decide")
async def agent_decision(request: AgentDecisionRequest):
    """Get an autonomous agent decision."""
    try:
        orchestrator = get_agentic_orchestrator()

        from automic_etl.integrations.airflow_agentic import AgentContext, AgentAction

        context = AgentContext(
            pipeline_name=request.pipeline_name,
            current_state=request.context or {},
        )

        available_actions = None
        if request.available_actions:
            available_actions = [AgentAction(a) for a in request.available_actions]

        decision = await orchestrator.agent.decide_action(
            context=context,
            available_actions=available_actions,
        )

        return decision.to_dict()

    except Exception as e:
        logger.error("Failed to get agent decision", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/insights/{dag_id}")
async def get_intelligent_insights(dag_id: str):
    """Get comprehensive AI-powered insights for a DAG."""
    try:
        orchestrator = get_agentic_orchestrator()
        insights = await orchestrator.get_intelligent_insights(dag_id)
        return insights
    except Exception as e:
        logger.error("Failed to get insights", dag_id=dag_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Orchestrator Management
# =============================================================================

@router.get("/orchestrator/status", response_model=OrchestratorStatus)
async def get_orchestrator_status():
    """Get orchestrator status."""
    try:
        orchestrator = get_agentic_orchestrator()
        status = orchestrator.get_status()

        return OrchestratorStatus(
            monitored_dags=status["monitored_dags"],
            auto_healing_enabled=status["auto_healing_enabled"],
            auto_optimization_enabled=status["auto_optimization_enabled"],
            healing_actions_count=status["healing_actions_count"],
            airflow_healthy=status["airflow_healthy"],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Global monitoring task reference for cancellation
_monitoring_task: asyncio.Task | None = None


@router.post("/orchestrator/monitor")
async def start_monitoring(background_tasks: BackgroundTasks):
    """Start monitoring and auto-healing."""
    global _monitoring_task

    try:
        # Check if monitoring is already running
        if _monitoring_task is not None and not _monitoring_task.done():
            return {"status": "already_running", "message": "Monitoring is already active"}

        orchestrator = get_agentic_orchestrator()

        async def monitor_task():
            try:
                while True:
                    await orchestrator.monitor_and_heal()
                    await asyncio.sleep(60)  # Check every minute
            except asyncio.CancelledError:
                logger.info("Monitoring task cancelled")
                raise

        _monitoring_task = asyncio.create_task(monitor_task())

        return {"status": "monitoring_started"}
    except Exception as e:
        logger.error("Failed to start monitoring", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/orchestrator/monitor/stop")
async def stop_monitoring():
    """Stop the monitoring background task."""
    global _monitoring_task

    if _monitoring_task is None or _monitoring_task.done():
        return {"status": "not_running", "message": "Monitoring is not active"}

    _monitoring_task.cancel()
    try:
        await _monitoring_task
    except asyncio.CancelledError:
        pass

    _monitoring_task = None
    return {"status": "stopped", "message": "Monitoring has been stopped"}


@router.post("/orchestrator/optimize-all")
async def optimize_all_dags():
    """Optimize all monitored DAGs."""
    try:
        orchestrator = get_agentic_orchestrator()
        suggestions = await orchestrator.optimize_all()

        return {
            "status": "completed",
            "suggestions_count": len(suggestions),
            "suggestions": [
                {
                    "dag_id": s.dag_id,
                    "type": s.optimization_type,
                    "suggested": s.suggested_value,
                }
                for s in suggestions
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Health Check
# =============================================================================

@router.get("/health")
async def health_check():
    """Check Airflow connectivity."""
    try:
        airflow = get_airflow_integration()
        health = airflow.health_check()
        return {
            "status": "healthy" if health.get("metadatabase", {}).get("status") == "healthy" else "unhealthy",
            "details": health,
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
        }
