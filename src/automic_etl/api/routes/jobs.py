"""Job orchestration endpoints."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from automic_etl.api.models import (
    JobCreate,
    JobResponse,
    JobRunResponse,
    JobStatus,
    PaginatedResponse,
    BaseResponse,
)
from automic_etl.db.job_service import get_job_service

router = APIRouter()


def _schedule_to_db(schedule: JobScheduleModel) -> dict:
    """Convert database model to API response format."""
    return {
        "id": schedule.id,
        "name": schedule.name,
        "pipeline_id": schedule.target_id,
        "pipeline_name": f"Pipeline-{schedule.target_id[:8]}" if schedule.target_id else None,
        "schedule": schedule.schedule_value,
        "enabled": schedule.enabled,
        "config": schedule.config or {},
        "notifications": schedule.config.get("notifications", {}) if schedule.config else {},
        "status": JobStatus.SCHEDULED if schedule.enabled else JobStatus.PAUSED,
        "last_run": schedule.last_run_at,
        "next_run": schedule.next_run_at,
        "run_count": schedule.run_count or 0,
        "success_count": schedule.config.get("success_count", 0) if schedule.config else 0,
        "failure_count": schedule.config.get("failure_count", 0) if schedule.config else 0,
        "created_at": schedule.created_at,
    }


def _run_to_dict(run: JobRunModel, job_name: str = "") -> dict:
    """Convert database run model to API response format."""
    return {
        "run_id": run.id,
        "job_id": run.schedule_id,
        "job_name": job_name,
        "status": run.status,
        "started_at": run.started_at,
        "completed_at": run.completed_at,
        "duration_seconds": run.duration_seconds,
        "metrics": run.result or {},
        "logs": run.logs or [],
        "error": run.error_message,
    }


# Import models for type hints
from automic_etl.db.models import JobScheduleModel, JobRunModel


@router.get("", response_model=PaginatedResponse)
async def list_jobs(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: JobStatus | None = None,
    pipeline_id: str | None = None,
    enabled: bool | None = None,
):
    """
    List all scheduled jobs.

    Args:
        page: Page number
        page_size: Items per page
        status: Filter by status
        pipeline_id: Filter by pipeline
        enabled: Filter by enabled status
    """
    service = get_job_service()
    schedules = service.list_schedules(
        job_type="pipeline",
        enabled=enabled,
    )

    # Convert to response format
    jobs = [_schedule_to_db(s) for s in schedules]

    # Apply additional filters
    if status:
        jobs = [j for j in jobs if j["status"] == status]
    if pipeline_id:
        jobs = [j for j in jobs if j["pipeline_id"] == pipeline_id]

    # Paginate
    total = len(jobs)
    start = (page - 1) * page_size
    end = start + page_size

    return PaginatedResponse(
        items=jobs[start:end],
        total=total,
        page=page,
        page_size=page_size,
        pages=(total + page_size - 1) // page_size,
    )


@router.post("", response_model=JobResponse, status_code=201)
async def create_job(job: JobCreate):
    """
    Create a new scheduled job.

    Args:
        job: Job configuration
    """
    service = get_job_service()

    # Check for duplicate name
    existing = service.list_schedules(job_type="pipeline")
    if any(s.name == job.name for s in existing):
        raise HTTPException(status_code=400, detail=f"Job '{job.name}' already exists")

    # Prepare config with notifications
    config = job.config or {}
    config["notifications"] = job.notifications or {}
    config["success_count"] = 0
    config["failure_count"] = 0

    schedule = service.create_schedule(
        name=job.name,
        job_type="pipeline",
        target_id=job.pipeline_id,
        schedule_type="cron",
        schedule_value=job.schedule,
        enabled=job.enabled,
        config=config,
    )

    return JobResponse(**_schedule_to_db(schedule))


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: str):
    """
    Get a job by ID.

    Args:
        job_id: Job ID
    """
    service = get_job_service()
    schedule = service.get_schedule(job_id)

    if not schedule:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobResponse(**_schedule_to_db(schedule))


@router.put("/{job_id}", response_model=JobResponse)
async def update_job(job_id: str, job: JobCreate):
    """
    Update a job.

    Args:
        job_id: Job ID
        job: New configuration
    """
    service = get_job_service()
    existing = service.get_schedule(job_id)

    if not existing:
        raise HTTPException(status_code=404, detail="Job not found")

    # Prepare config with notifications
    config = job.config or {}
    config["notifications"] = job.notifications or {}
    # Preserve counts
    old_config = existing.config or {}
    config["success_count"] = old_config.get("success_count", 0)
    config["failure_count"] = old_config.get("failure_count", 0)

    schedule = service.update_schedule(
        schedule_id=job_id,
        name=job.name,
        target_id=job.pipeline_id,
        schedule_value=job.schedule,
        enabled=job.enabled,
        config=config,
    )

    return JobResponse(**_schedule_to_db(schedule))


@router.delete("/{job_id}", response_model=BaseResponse)
async def delete_job(job_id: str):
    """
    Delete a job.

    Args:
        job_id: Job ID
    """
    service = get_job_service()
    schedule = service.get_schedule(job_id)

    if not schedule:
        raise HTTPException(status_code=404, detail="Job not found")

    name = schedule.name
    service.delete_schedule(job_id)

    return BaseResponse(success=True, message=f"Job '{name}' deleted")


@router.post("/{job_id}/trigger", response_model=JobRunResponse)
async def trigger_job(job_id: str):
    """
    Manually trigger a job run.

    Args:
        job_id: Job ID
    """
    from automic_etl.notifications import get_notification_event_service

    service = get_job_service()
    notification_service = get_notification_event_service()

    schedule = service.get_schedule(job_id)

    if not schedule:
        raise HTTPException(status_code=404, detail="Job not found")

    now = datetime.utcnow()

    # Create a new run
    run = service.create_run(
        schedule_id=job_id,
        triggered_by="manual",
    )

    # Add initial logs
    logs = [
        f"[{now.isoformat()}] Job triggered manually",
        f"[{now.isoformat()}] Starting pipeline execution...",
    ]
    service.update_run(run.id, logs=logs)

    # Emit job started notification
    notification_service.job_started(schedule.name, job_id)

    return JobRunResponse(**_run_to_dict(run, schedule.name))


@router.post("/{job_id}/pause", response_model=JobResponse)
async def pause_job(job_id: str):
    """
    Pause a scheduled job.

    Args:
        job_id: Job ID
    """
    service = get_job_service()
    schedule = service.get_schedule(job_id)

    if not schedule:
        raise HTTPException(status_code=404, detail="Job not found")

    schedule = service.update_schedule(schedule_id=job_id, enabled=False)

    return JobResponse(**_schedule_to_db(schedule))


@router.post("/{job_id}/resume", response_model=JobResponse)
async def resume_job(job_id: str):
    """
    Resume a paused job.

    Args:
        job_id: Job ID
    """
    service = get_job_service()
    schedule = service.get_schedule(job_id)

    if not schedule:
        raise HTTPException(status_code=404, detail="Job not found")

    schedule = service.update_schedule(schedule_id=job_id, enabled=True)

    return JobResponse(**_schedule_to_db(schedule))


@router.get("/{job_id}/runs", response_model=PaginatedResponse)
async def list_job_runs(
    job_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: JobStatus | None = None,
):
    """
    List runs for a job.

    Args:
        job_id: Job ID
        page: Page number
        page_size: Items per page
        status: Filter by status
    """
    service = get_job_service()
    schedule = service.get_schedule(job_id)

    if not schedule:
        raise HTTPException(status_code=404, detail="Job not found")

    db_runs = service.get_runs(schedule_id=job_id, status=status.value if status else None)
    runs = [_run_to_dict(r, schedule.name) for r in db_runs]

    # Paginate
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


@router.get("/{job_id}/runs/{run_id}", response_model=JobRunResponse)
async def get_job_run(job_id: str, run_id: str):
    """
    Get details of a specific job run.

    Args:
        job_id: Job ID
        run_id: Run ID
    """
    service = get_job_service()
    schedule = service.get_schedule(job_id)

    if not schedule:
        raise HTTPException(status_code=404, detail="Job not found")

    runs = service.get_runs(schedule_id=job_id)
    for run in runs:
        if run.id == run_id:
            return JobRunResponse(**_run_to_dict(run, schedule.name))

    raise HTTPException(status_code=404, detail="Job run not found")


@router.get("/{job_id}/runs/{run_id}/logs")
async def get_job_run_logs(
    job_id: str,
    run_id: str,
    tail: int = Query(100, ge=1, le=1000),
):
    """
    Get logs for a job run.

    Args:
        job_id: Job ID
        run_id: Run ID
        tail: Number of log lines to return
    """
    service = get_job_service()
    schedule = service.get_schedule(job_id)

    if not schedule:
        raise HTTPException(status_code=404, detail="Job not found")

    runs = service.get_runs(schedule_id=job_id)
    for run in runs:
        if run.id == run_id:
            logs = run.logs or []
            return {
                "run_id": run_id,
                "logs": logs[-tail:],
                "total_lines": len(logs),
            }

    raise HTTPException(status_code=404, detail="Job run not found")


@router.post("/{job_id}/runs/{run_id}/cancel", response_model=BaseResponse)
async def cancel_job_run(job_id: str, run_id: str):
    """
    Cancel a running job.

    Args:
        job_id: Job ID
        run_id: Run ID
    """
    service = get_job_service()
    schedule = service.get_schedule(job_id)

    if not schedule:
        raise HTTPException(status_code=404, detail="Job not found")

    runs = service.get_runs(schedule_id=job_id)
    for run in runs:
        if run.id == run_id:
            if run.status != "running":
                raise HTTPException(status_code=400, detail="Job run is not running")

            now = datetime.utcnow()
            logs = run.logs or []
            logs.append(f"[{now.isoformat()}] Job cancelled by user")

            service.update_run(
                run_id=run_id,
                status="failed",
                error_message="Cancelled by user",
                logs=logs,
            )

            # Update failure count in config
            config = schedule.config or {}
            config["failure_count"] = config.get("failure_count", 0) + 1
            service.update_schedule(schedule_id=job_id, config=config)

            return BaseResponse(success=True, message="Job run cancelled")

    raise HTTPException(status_code=404, detail="Job run not found")


@router.get("/stats/summary")
async def get_job_stats():
    """Get summary statistics for all jobs."""
    service = get_job_service()
    schedules = service.list_schedules(job_type="pipeline")

    total_jobs = len(schedules)
    enabled_jobs = sum(1 for s in schedules if s.enabled)

    # Get all runs for stats
    total_runs = 0
    successful_runs = 0
    failed_runs = 0
    running_jobs = 0

    for schedule in schedules:
        runs = service.get_runs(schedule_id=schedule.id)
        total_runs += len(runs)

        for run in runs:
            if run.status == "completed":
                successful_runs += 1
            elif run.status == "failed":
                failed_runs += 1
            elif run.status == "running":
                running_jobs += 1

    return {
        "total_jobs": total_jobs,
        "enabled_jobs": enabled_jobs,
        "running_jobs": running_jobs,
        "paused_jobs": total_jobs - enabled_jobs,
        "total_runs": total_runs,
        "successful_runs": successful_runs,
        "failed_runs": failed_runs,
        "success_rate": successful_runs / total_runs * 100 if total_runs > 0 else 0,
    }
