"""Pipeline management endpoints."""

from __future__ import annotations

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
from automic_etl.db.pipeline_service import get_pipeline_service
from automic_etl.db.models import PipelineModel, PipelineRunModel

router = APIRouter()


def _pipeline_to_dict(pipeline: PipelineModel) -> dict:
    """Convert database model to API response format."""
    return {
        "id": pipeline.id,
        "company_id": pipeline.owner_id,  # Use owner_id for company tracking
        "created_by": pipeline.owner_id,
        "name": pipeline.name,
        "description": pipeline.description or "",
        "stages": pipeline.transformations or [],
        "schedule": pipeline.schedule,
        "enabled": pipeline.status not in ("disabled", "draft"),
        "tags": pipeline.metadata_.get("tags", []) if pipeline.metadata_ else [],
        "config": pipeline.source_config or {},
        "created_at": pipeline.created_at,
        "updated_at": pipeline.updated_at,
        "last_run": pipeline.last_run_at,
        "last_status": pipeline.status,
    }


def _run_to_dict(run: PipelineRunModel, pipeline_name: str = "") -> dict:
    """Convert database run model to API response format."""
    return {
        "run_id": run.id,
        "pipeline_id": run.pipeline_id,
        "pipeline_name": pipeline_name,
        "status": run.status,
        "started_at": run.started_at,
        "completed_at": run.completed_at,
        "metrics": {
            "rows_processed": run.records_processed or 0,
            "execution_time_ms": (run.duration_seconds or 0) * 1000,
        },
        "error": run.error_message,
    }


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
    service = get_pipeline_service()

    # Get pipelines from database
    db_pipelines = service.list_pipelines(
        status=status.value if status else None,
    )

    # Convert to response format
    pipelines = [_pipeline_to_dict(p) for p in db_pipelines]

    # Filter by company (multi-tenant isolation)
    pipelines = filter_by_company(ctx, pipelines)

    # Apply filters
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
    service = get_pipeline_service()

    # Check for duplicate name within company
    existing = service.list_pipelines(owner_id=ctx.user.user_id)
    if any(p.name == pipeline.name for p in existing):
        raise HTTPException(status_code=400, detail=f"Pipeline '{pipeline.name}' already exists")

    # Prepare source config with additional settings
    source_config = pipeline.config or {}
    metadata = {"tags": pipeline.tags or []}

    db_pipeline = service.create_pipeline(
        name=pipeline.name,
        owner_id=ctx.user.user_id,
        description=pipeline.description or "",
        schedule=pipeline.schedule,
        transformations=[s.model_dump() for s in pipeline.stages] if pipeline.stages else [],
        source_config=source_config,
    )

    # Update metadata with tags
    if pipeline.tags:
        service.update_pipeline(db_pipeline.id, metadata_=metadata)

    return PipelineResponse(**_pipeline_to_dict(db_pipeline))


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
    service = get_pipeline_service()
    pipeline = service.get_pipeline(pipeline_id)

    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    pipeline_dict = _pipeline_to_dict(pipeline)

    # Check tenant access
    check_resource_access(
        ctx, ResourceType.PIPELINE, pipeline_id,
        pipeline_dict.get("company_id", ""), AccessLevel.READ
    )

    return PipelineResponse(**pipeline_dict)


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
    service = get_pipeline_service()
    pipeline = service.get_pipeline(pipeline_id)

    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    pipeline_dict = _pipeline_to_dict(pipeline)

    # Check tenant access with WRITE level
    check_resource_access(
        ctx, ResourceType.PIPELINE, pipeline_id,
        pipeline_dict.get("company_id", ""), AccessLevel.WRITE
    )

    # Build update kwargs
    update_kwargs = {}
    if update.description is not None:
        update_kwargs["description"] = update.description
    if update.stages is not None:
        update_kwargs["transformations"] = [s.model_dump() for s in update.stages]
    if update.schedule is not None:
        update_kwargs["schedule"] = update.schedule
    if update.enabled is not None:
        update_kwargs["status"] = "active" if update.enabled else "disabled"
    if update.config is not None:
        update_kwargs["source_config"] = update.config

    updated = service.update_pipeline(pipeline_id, **update_kwargs)

    return PipelineResponse(**_pipeline_to_dict(updated))


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
    service = get_pipeline_service()
    pipeline = service.get_pipeline(pipeline_id)

    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    pipeline_dict = _pipeline_to_dict(pipeline)

    # Check tenant access with ADMIN level for delete
    check_resource_access(
        ctx, ResourceType.PIPELINE, pipeline_id,
        pipeline_dict.get("company_id", ""), AccessLevel.ADMIN
    )

    service.delete_pipeline(pipeline_id)

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
    service = get_pipeline_service()
    pipeline = service.get_pipeline(pipeline_id)

    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    pipeline_dict = _pipeline_to_dict(pipeline)

    # Check tenant access with EXECUTE level
    check_resource_access(
        ctx, ResourceType.PIPELINE, pipeline_id,
        pipeline_dict.get("company_id", ""), AccessLevel.EXECUTE
    )

    if pipeline.status == "disabled":
        raise HTTPException(status_code=400, detail="Pipeline is disabled")

    # Create a run in the database
    run = service.run_pipeline(pipeline_id)

    if not run:
        raise HTTPException(status_code=500, detail="Failed to create pipeline run")

    if request.async_execution:
        # Execute in background
        background_tasks.add_task(_execute_pipeline, run.id, pipeline_id, request.config_overrides)

    return PipelineRunResponse(**_run_to_dict(run, pipeline.name))


async def _execute_pipeline(run_id: str, pipeline_id: str, config_overrides: dict):
    """Execute pipeline in background."""
    import asyncio
    import time

    from automic_etl.notifications import get_notification_event_service

    service = get_pipeline_service()
    notification_service = get_notification_event_service()

    # Get pipeline info
    pipeline = service.get_pipeline(pipeline_id)
    pipeline_name = pipeline.name if pipeline else f"Pipeline-{pipeline_id[:8]}"

    # Emit start notification
    notification_service.pipeline_started(pipeline_name, pipeline_id)

    start_time = time.time()

    try:
        # Simulate pipeline execution
        await asyncio.sleep(2)

        duration = time.time() - start_time
        rows_processed = 1000

        # Complete the run successfully
        service.complete_run(
            run_id=run_id,
            status="completed",
            records_processed=rows_processed,
        )

        # Emit completion notification
        notification_service.pipeline_completed(
            pipeline_name, pipeline_id, duration, rows_processed
        )

    except Exception as e:
        duration = time.time() - start_time

        # Complete the run with error
        service.complete_run(
            run_id=run_id,
            status="failed",
            error_message=str(e),
        )

        # Emit failure notification
        notification_service.pipeline_failed(pipeline_name, pipeline_id, str(e))


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
    service = get_pipeline_service()
    pipeline = service.get_pipeline(pipeline_id)

    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    pipeline_dict = _pipeline_to_dict(pipeline)

    # Check tenant access
    check_resource_access(
        ctx, ResourceType.PIPELINE, pipeline_id,
        pipeline_dict.get("company_id", ""), AccessLevel.READ
    )

    db_runs = service.get_pipeline_runs(pipeline_id, limit=100)
    runs = [_run_to_dict(r, pipeline.name) for r in db_runs]

    if status:
        runs = [r for r in runs if r["status"] == status.value]

    # Sort by started_at descending (already sorted from service)
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
    service = get_pipeline_service()
    pipeline = service.get_pipeline(pipeline_id)

    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    pipeline_dict = _pipeline_to_dict(pipeline)

    # Check tenant access
    check_resource_access(
        ctx, ResourceType.PIPELINE, pipeline_id,
        pipeline_dict.get("company_id", ""), AccessLevel.READ
    )

    runs = service.get_pipeline_runs(pipeline_id, limit=100)
    for run in runs:
        if run.id == run_id:
            return PipelineRunResponse(**_run_to_dict(run, pipeline.name))

    raise HTTPException(status_code=404, detail="Pipeline run not found")


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
    service = get_pipeline_service()
    pipeline = service.get_pipeline(pipeline_id)

    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    pipeline_dict = _pipeline_to_dict(pipeline)

    # Check tenant access with EXECUTE level (same as running)
    check_resource_access(
        ctx, ResourceType.PIPELINE, pipeline_id,
        pipeline_dict.get("company_id", ""), AccessLevel.EXECUTE
    )

    runs = service.get_pipeline_runs(pipeline_id, limit=100)
    for run in runs:
        if run.id == run_id:
            if run.status not in ("pending", "running"):
                raise HTTPException(status_code=400, detail="Pipeline run is not active")

            service.complete_run(
                run_id=run_id,
                status="cancelled",
                error_message="Cancelled by user",
            )

            return BaseResponse(success=True, message="Pipeline run cancelled")

    raise HTTPException(status_code=404, detail="Pipeline run not found")
