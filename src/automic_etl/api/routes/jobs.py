"""Job orchestration endpoints."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
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

router = APIRouter()

# In-memory storage
_jobs: dict[str, dict] = {}
_job_runs: dict[str, list[dict]] = {}


def _parse_cron(cron_expr: str) -> datetime:
    """Parse cron expression and return next run time."""
    # Simplified - in production use croniter
    now = datetime.utcnow()
    return now + timedelta(hours=1)


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
    jobs = list(_jobs.values())

    # Apply filters
    if status:
        jobs = [j for j in jobs if j["status"] == status]
    if pipeline_id:
        jobs = [j for j in jobs if j["pipeline_id"] == pipeline_id]
    if enabled is not None:
        jobs = [j for j in jobs if j["enabled"] == enabled]

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
    # Check for duplicate name
    if any(j["name"] == job.name for j in _jobs.values()):
        raise HTTPException(status_code=400, detail=f"Job '{job.name}' already exists")

    job_id = str(uuid.uuid4())
    now = datetime.utcnow()

    job_data = {
        "id": job_id,
        "name": job.name,
        "pipeline_id": job.pipeline_id,
        "pipeline_name": f"Pipeline-{job.pipeline_id[:8]}",  # In production, lookup actual name
        "schedule": job.schedule,
        "enabled": job.enabled,
        "config": job.config,
        "notifications": job.notifications,
        "status": JobStatus.SCHEDULED if job.enabled else JobStatus.PAUSED,
        "last_run": None,
        "next_run": _parse_cron(job.schedule) if job.enabled else None,
        "run_count": 0,
        "success_count": 0,
        "failure_count": 0,
        "created_at": now,
    }

    _jobs[job_id] = job_data
    _job_runs[job_id] = []

    return JobResponse(**job_data)


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: str):
    """
    Get a job by ID.

    Args:
        job_id: Job ID
    """
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobResponse(**_jobs[job_id])


@router.put("/{job_id}", response_model=JobResponse)
async def update_job(job_id: str, job: JobCreate):
    """
    Update a job.

    Args:
        job_id: Job ID
        job: New configuration
    """
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    existing = _jobs[job_id]

    existing["name"] = job.name
    existing["pipeline_id"] = job.pipeline_id
    existing["schedule"] = job.schedule
    existing["enabled"] = job.enabled
    existing["config"] = job.config
    existing["notifications"] = job.notifications

    if job.enabled:
        existing["status"] = JobStatus.SCHEDULED
        existing["next_run"] = _parse_cron(job.schedule)
    else:
        existing["status"] = JobStatus.PAUSED
        existing["next_run"] = None

    return JobResponse(**existing)


@router.delete("/{job_id}", response_model=BaseResponse)
async def delete_job(job_id: str):
    """
    Delete a job.

    Args:
        job_id: Job ID
    """
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    name = _jobs[job_id]["name"]
    del _jobs[job_id]
    if job_id in _job_runs:
        del _job_runs[job_id]

    return BaseResponse(success=True, message=f"Job '{name}' deleted")


@router.post("/{job_id}/trigger", response_model=JobRunResponse)
async def trigger_job(job_id: str):
    """
    Manually trigger a job run.

    Args:
        job_id: Job ID
    """
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = _jobs[job_id]
    now = datetime.utcnow()

    run_id = str(uuid.uuid4())
    run_data = {
        "run_id": run_id,
        "job_id": job_id,
        "job_name": job["name"],
        "status": JobStatus.RUNNING,
        "started_at": now,
        "completed_at": None,
        "duration_seconds": None,
        "metrics": {},
        "logs": [
            f"[{now.isoformat()}] Job triggered manually",
            f"[{now.isoformat()}] Starting pipeline execution...",
        ],
        "error": None,
    }

    _job_runs[job_id].insert(0, run_data)
    job["status"] = JobStatus.RUNNING
    job["run_count"] += 1

    return JobRunResponse(**run_data)


@router.post("/{job_id}/pause", response_model=JobResponse)
async def pause_job(job_id: str):
    """
    Pause a scheduled job.

    Args:
        job_id: Job ID
    """
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = _jobs[job_id]
    job["enabled"] = False
    job["status"] = JobStatus.PAUSED
    job["next_run"] = None

    return JobResponse(**job)


@router.post("/{job_id}/resume", response_model=JobResponse)
async def resume_job(job_id: str):
    """
    Resume a paused job.

    Args:
        job_id: Job ID
    """
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = _jobs[job_id]
    job["enabled"] = True
    job["status"] = JobStatus.SCHEDULED
    job["next_run"] = _parse_cron(job["schedule"])

    return JobResponse(**job)


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
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    runs = _job_runs.get(job_id, [])

    if status:
        runs = [r for r in runs if r["status"] == status]

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
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    runs = _job_runs.get(job_id, [])
    for run in runs:
        if run["run_id"] == run_id:
            return JobRunResponse(**run)

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
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    runs = _job_runs.get(job_id, [])
    for run in runs:
        if run["run_id"] == run_id:
            logs = run.get("logs", [])
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
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    runs = _job_runs.get(job_id, [])
    for run in runs:
        if run["run_id"] == run_id:
            if run["status"] != JobStatus.RUNNING:
                raise HTTPException(status_code=400, detail="Job run is not running")

            run["status"] = JobStatus.FAILED
            run["completed_at"] = datetime.utcnow()
            run["error"] = "Cancelled by user"
            run["logs"].append(f"[{datetime.utcnow().isoformat()}] Job cancelled by user")

            # Update job stats
            _jobs[job_id]["status"] = JobStatus.SCHEDULED
            _jobs[job_id]["failure_count"] += 1

            return BaseResponse(success=True, message="Job run cancelled")

    raise HTTPException(status_code=404, detail="Job run not found")


@router.get("/stats/summary")
async def get_job_stats():
    """Get summary statistics for all jobs."""
    total_jobs = len(_jobs)
    enabled_jobs = sum(1 for j in _jobs.values() if j["enabled"])
    running_jobs = sum(1 for j in _jobs.values() if j["status"] == JobStatus.RUNNING)

    total_runs = sum(len(runs) for runs in _job_runs.values())
    successful_runs = sum(j["success_count"] for j in _jobs.values())
    failed_runs = sum(j["failure_count"] for j in _jobs.values())

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
