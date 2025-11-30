"""Pipeline management endpoints."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks, Depends

from automic_etl.api.models import (
    PipelineCreate,
    PipelineUpdate,
    PipelineResponse,
    PipelineRunRequest,
    PipelineRunResponse,
    PipelineStatus,
    PaginatedResponse,
    BaseResponse,
)
from automic_etl.api.middleware import (
    get_security_context,
    require_permission,
    require_resource_access,
    filter_by_company,
    check_resource_access,
)
from automic_etl.auth.models import PermissionType
from automic_etl.auth.security import (
    SecurityContext,
    ResourceType,
    AccessLevel,
)

router = APIRouter()

# In-memory storage for demo (would be database in production)
_pipelines: dict[str, dict] = {}
_pipeline_runs: dict[str, dict] = {}


@router.get("", response_model=PaginatedResponse)
async def list_pipelines(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: PipelineStatus | None = None,
    tag: str | None = None,
    search: str | None = None,
    ctx: SecurityContext = Depends(require_permission(PermissionType.PIPELINE_READ)),
):
    """
    List all pipelines with pagination and filtering.

    Args:
        page: Page number
        page_size: Items per page
        status: Filter by last run status
        tag: Filter by tag
        search: Search in name and description
    """
    # Filter by company (multi-tenant isolation)
    pipelines = filter_by_company(ctx, list(_pipelines.values()))

    # Apply filters
    if status:
        pipelines = [p for p in pipelines if p.get("last_status") == status]
    if tag:
        pipelines = [p for p in pipelines if tag in p.get("tags", [])]
    if search:
        search_lower = search.lower()
        pipelines = [
            p for p in pipelines
            if search_lower in p["name"].lower() or search_lower in p.get("description", "").lower()
        ]

    # Paginate
    total = len(pipelines)
    start = (page - 1) * page_size
    end = start + page_size
    items = pipelines[start:end]

    return PaginatedResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        pages=(total + page_size - 1) // page_size,
    )


@router.post("", response_model=PipelineResponse, status_code=201)
async def create_pipeline(
    pipeline: PipelineCreate,
    ctx: SecurityContext = Depends(require_permission(PermissionType.PIPELINE_CREATE)),
):
    """
    Create a new pipeline.

    Args:
        pipeline: Pipeline configuration
    """
    # Check for duplicate name within company
    company_pipelines = filter_by_company(ctx, list(_pipelines.values()))
    if any(p["name"] == pipeline.name for p in company_pipelines):
        raise HTTPException(status_code=400, detail=f"Pipeline '{pipeline.name}' already exists")

    pipeline_id = str(uuid.uuid4())
    now = datetime.utcnow()

    pipeline_data = {
        "id": pipeline_id,
        "company_id": ctx.tenant.company_id,  # Multi-tenant: associate with company
        "created_by": ctx.user.user_id,
        "name": pipeline.name,
        "description": pipeline.description,
        "stages": [s.model_dump() for s in pipeline.stages],
        "schedule": pipeline.schedule,
        "enabled": pipeline.enabled,
        "tags": pipeline.tags,
        "config": pipeline.config,
        "created_at": now,
        "updated_at": now,
        "last_run": None,
        "last_status": None,
    }

    _pipelines[pipeline_id] = pipeline_data
    return PipelineResponse(**pipeline_data)


@router.get("/{pipeline_id}", response_model=PipelineResponse)
async def get_pipeline(
    pipeline_id: str,
    ctx: SecurityContext = Depends(require_permission(PermissionType.PIPELINE_READ)),
):
    """
    Get a pipeline by ID.

    Args:
        pipeline_id: Pipeline ID
    """
    if pipeline_id not in _pipelines:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    pipeline = _pipelines[pipeline_id]

    # Check tenant access
    check_resource_access(
        ctx, ResourceType.PIPELINE, pipeline_id,
        pipeline.get("company_id", ""), AccessLevel.READ
    )

    return PipelineResponse(**pipeline)


@router.put("/{pipeline_id}", response_model=PipelineResponse)
async def update_pipeline(
    pipeline_id: str,
    update: PipelineUpdate,
    ctx: SecurityContext = Depends(require_permission(PermissionType.PIPELINE_UPDATE)),
):
    """
    Update a pipeline.

    Args:
        pipeline_id: Pipeline ID
        update: Fields to update
    """
    if pipeline_id not in _pipelines:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    pipeline = _pipelines[pipeline_id]

    # Check tenant access with WRITE level
    check_resource_access(
        ctx, ResourceType.PIPELINE, pipeline_id,
        pipeline.get("company_id", ""), AccessLevel.WRITE
    )

    # Apply updates
    if update.description is not None:
        pipeline["description"] = update.description
    if update.stages is not None:
        pipeline["stages"] = [s.model_dump() for s in update.stages]
    if update.schedule is not None:
        pipeline["schedule"] = update.schedule
    if update.enabled is not None:
        pipeline["enabled"] = update.enabled
    if update.tags is not None:
        pipeline["tags"] = update.tags
    if update.config is not None:
        pipeline["config"] = update.config

    pipeline["updated_at"] = datetime.utcnow()

    return PipelineResponse(**pipeline)


@router.delete("/{pipeline_id}", response_model=BaseResponse)
async def delete_pipeline(
    pipeline_id: str,
    ctx: SecurityContext = Depends(require_permission(PermissionType.PIPELINE_DELETE)),
):
    """
    Delete a pipeline.

    Args:
        pipeline_id: Pipeline ID
    """
    if pipeline_id not in _pipelines:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    pipeline = _pipelines[pipeline_id]

    # Check tenant access with ADMIN level for delete
    check_resource_access(
        ctx, ResourceType.PIPELINE, pipeline_id,
        pipeline.get("company_id", ""), AccessLevel.ADMIN
    )

    del _pipelines[pipeline_id]

    return BaseResponse(success=True, message="Pipeline deleted successfully")


@router.post("/{pipeline_id}/run", response_model=PipelineRunResponse)
async def run_pipeline(
    pipeline_id: str,
    request: PipelineRunRequest,
    background_tasks: BackgroundTasks,
    ctx: SecurityContext = Depends(require_permission(PermissionType.PIPELINE_EXECUTE)),
):
    """
    Trigger a pipeline run.

    Args:
        pipeline_id: Pipeline ID
        request: Run configuration
    """
    if pipeline_id not in _pipelines:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    pipeline = _pipelines[pipeline_id]

    # Check tenant access with EXECUTE level
    check_resource_access(
        ctx, ResourceType.PIPELINE, pipeline_id,
        pipeline.get("company_id", ""), AccessLevel.EXECUTE
    )

    if not pipeline["enabled"]:
        raise HTTPException(status_code=400, detail="Pipeline is disabled")

    run_id = str(uuid.uuid4())
    now = datetime.utcnow()

    run_data = {
        "run_id": run_id,
        "pipeline_id": pipeline_id,
        "pipeline_name": pipeline["name"],
        "status": PipelineStatus.PENDING if request.async_execution else PipelineStatus.RUNNING,
        "started_at": now,
        "completed_at": None,
        "metrics": {},
        "error": None,
    }

    _pipeline_runs[run_id] = run_data

    # Update pipeline last run
    pipeline["last_run"] = now
    pipeline["last_status"] = run_data["status"]

    if request.async_execution:
        # In production, this would queue the job
        background_tasks.add_task(_execute_pipeline, run_id, pipeline, request.config_overrides)

    return PipelineRunResponse(**run_data)


async def _execute_pipeline(run_id: str, pipeline: dict, config_overrides: dict):
    """Execute pipeline in background."""
    import asyncio

    run = _pipeline_runs[run_id]
    run["status"] = PipelineStatus.RUNNING

    try:
        # Simulate pipeline execution
        await asyncio.sleep(2)

        run["status"] = PipelineStatus.COMPLETED
        run["completed_at"] = datetime.utcnow()
        run["metrics"] = {
            "rows_processed": 1000,
            "execution_time_ms": 2000,
        }

        # Update pipeline status
        if pipeline["id"] in _pipelines:
            _pipelines[pipeline["id"]]["last_status"] = PipelineStatus.COMPLETED

    except Exception as e:
        run["status"] = PipelineStatus.FAILED
        run["error"] = str(e)
        run["completed_at"] = datetime.utcnow()

        if pipeline["id"] in _pipelines:
            _pipelines[pipeline["id"]]["last_status"] = PipelineStatus.FAILED


@router.get("/{pipeline_id}/runs", response_model=PaginatedResponse)
async def list_pipeline_runs(
    pipeline_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: PipelineStatus | None = None,
    ctx: SecurityContext = Depends(require_permission(PermissionType.PIPELINE_READ)),
):
    """
    List runs for a pipeline.

    Args:
        pipeline_id: Pipeline ID
        page: Page number
        page_size: Items per page
        status: Filter by status
    """
    if pipeline_id not in _pipelines:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    pipeline = _pipelines[pipeline_id]

    # Check tenant access
    check_resource_access(
        ctx, ResourceType.PIPELINE, pipeline_id,
        pipeline.get("company_id", ""), AccessLevel.READ
    )

    runs = [r for r in _pipeline_runs.values() if r["pipeline_id"] == pipeline_id]

    if status:
        runs = [r for r in runs if r["status"] == status]

    # Sort by started_at descending
    runs.sort(key=lambda x: x["started_at"], reverse=True)

    total = len(runs)
    start = (page - 1) * page_size
    end = start + page_size

    return PaginatedResponse(
        items=runs[start:end],
        total=total,
        page=page,
        page_size=page_size,
        pages=(total + page_size - 1) // page_size,
    )


@router.get("/{pipeline_id}/runs/{run_id}", response_model=PipelineRunResponse)
async def get_pipeline_run(
    pipeline_id: str,
    run_id: str,
    ctx: SecurityContext = Depends(require_permission(PermissionType.PIPELINE_READ)),
):
    """
    Get details of a specific pipeline run.

    Args:
        pipeline_id: Pipeline ID
        run_id: Run ID
    """
    if pipeline_id not in _pipelines:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    pipeline = _pipelines[pipeline_id]

    # Check tenant access
    check_resource_access(
        ctx, ResourceType.PIPELINE, pipeline_id,
        pipeline.get("company_id", ""), AccessLevel.READ
    )

    if run_id not in _pipeline_runs:
        raise HTTPException(status_code=404, detail="Pipeline run not found")

    run = _pipeline_runs[run_id]
    if run["pipeline_id"] != pipeline_id:
        raise HTTPException(status_code=404, detail="Pipeline run not found")

    return PipelineRunResponse(**run)


@router.post("/{pipeline_id}/runs/{run_id}/cancel", response_model=BaseResponse)
async def cancel_pipeline_run(
    pipeline_id: str,
    run_id: str,
    ctx: SecurityContext = Depends(require_permission(PermissionType.PIPELINE_EXECUTE)),
):
    """
    Cancel a running pipeline.

    Args:
        pipeline_id: Pipeline ID
        run_id: Run ID
    """
    if pipeline_id not in _pipelines:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    pipeline = _pipelines[pipeline_id]

    # Check tenant access with EXECUTE level (same as running)
    check_resource_access(
        ctx, ResourceType.PIPELINE, pipeline_id,
        pipeline.get("company_id", ""), AccessLevel.EXECUTE
    )

    if run_id not in _pipeline_runs:
        raise HTTPException(status_code=404, detail="Pipeline run not found")

    run = _pipeline_runs[run_id]
    if run["pipeline_id"] != pipeline_id:
        raise HTTPException(status_code=404, detail="Pipeline run not found")

    if run["status"] not in (PipelineStatus.PENDING, PipelineStatus.RUNNING):
        raise HTTPException(status_code=400, detail="Pipeline run is not active")

    run["status"] = PipelineStatus.CANCELLED
    run["completed_at"] = datetime.utcnow()

    return BaseResponse(success=True, message="Pipeline run cancelled")
