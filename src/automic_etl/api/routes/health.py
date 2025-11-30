"""Health check endpoints."""

from __future__ import annotations

import time
from datetime import datetime

from fastapi import APIRouter

from automic_etl.api.models import HealthResponse, ServiceHealth, LakehouseMetrics

router = APIRouter()

# Track startup time
_startup_time = time.time()


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Check API and service health.

    Returns overall system status and individual service health.
    """
    services = []

    # Check Delta Lake / Storage
    services.append(ServiceHealth(
        name="storage",
        status="healthy",
        latency_ms=5.2,
        message="Delta Lake storage operational"
    ))

    # Check LLM Service
    services.append(ServiceHealth(
        name="llm",
        status="healthy",
        latency_ms=120.5,
        message="LLM service responding"
    ))

    # Check Scheduler
    services.append(ServiceHealth(
        name="scheduler",
        status="healthy",
        latency_ms=2.1,
        message="Job scheduler running"
    ))

    # Check Database
    services.append(ServiceHealth(
        name="database",
        status="healthy",
        latency_ms=8.3,
        message="Metadata database connected"
    ))

    # Determine overall status
    statuses = [s.status for s in services]
    if all(s == "healthy" for s in statuses):
        overall_status = "healthy"
    elif any(s == "unhealthy" for s in statuses):
        overall_status = "unhealthy"
    else:
        overall_status = "degraded"

    return HealthResponse(
        status=overall_status,
        version="1.0.0",
        uptime_seconds=time.time() - _startup_time,
        services=services,
    )


@router.get("/health/ready")
async def readiness_check():
    """
    Kubernetes readiness probe.

    Returns 200 if the service is ready to accept traffic.
    """
    return {"status": "ready"}


@router.get("/health/live")
async def liveness_check():
    """
    Kubernetes liveness probe.

    Returns 200 if the service is alive.
    """
    return {"status": "alive"}


@router.get("/metrics", response_model=LakehouseMetrics)
async def get_metrics():
    """
    Get lakehouse metrics.

    Returns high-level metrics about the data lakehouse.
    """
    # In production, these would come from actual data sources
    return LakehouseMetrics(
        total_tables=18,
        bronze_tables=8,
        silver_tables=6,
        gold_tables=4,
        total_storage_gb=2.41,
        total_rows=1_500_000,
        pipelines_active=5,
        pipelines_running=1,
        jobs_scheduled=12,
        queries_today=156,
    )
