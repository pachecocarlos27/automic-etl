"""FastAPI application for Automic ETL REST API."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

from automic_etl.api.middleware import (
    TenantMiddleware,
    AuditMiddleware,
    RateLimitMiddleware,
    MaintenanceModeMiddleware,
)
from automic_etl.auth.security import AccessDeniedError, TenantMismatchError

logger = logging.getLogger(__name__)

# Global app instance
app: FastAPI | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Startup
    logger.info("Starting Automic ETL API")
    yield
    # Shutdown
    logger.info("Shutting down Automic ETL API")


def create_app(
    title: str = "Automic ETL API",
    description: str = "REST API for Automic ETL - AI-Augmented Data Lakehouse Platform",
    version: str = "1.0.0",
    cors_origins: list[str] | None = None,
    debug: bool = False,
) -> FastAPI:
    """
    Create and configure the FastAPI application.

    Args:
        title: API title
        description: API description
        version: API version
        cors_origins: Allowed CORS origins
        debug: Enable debug mode

    Returns:
        Configured FastAPI application
    """
    global app

    app = FastAPI(
        title=title,
        description=description,
        version=version,
        lifespan=lifespan,
        debug=debug,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        openapi_tags=[
            {"name": "health", "description": "Health check endpoints"},
            {"name": "pipelines", "description": "Pipeline management"},
            {"name": "tables", "description": "Table and schema operations"},
            {"name": "queries", "description": "Query execution"},
            {"name": "connectors", "description": "Data source connectors"},
            {"name": "lineage", "description": "Data lineage"},
            {"name": "jobs", "description": "Job orchestration"},
            {"name": "companies", "description": "Company/organization management"},
            {"name": "admin", "description": "Superadmin controls"},
            {"name": "airflow", "description": "Apache Airflow integration with agentic capabilities"},
        ],
    )

    # Add CORS middleware
    if cors_origins is None:
        cors_origins = ["*"]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Add security middleware (order matters - last added runs first)
    # 1. Maintenance mode check (runs first)
    app.add_middleware(MaintenanceModeMiddleware)
    # 2. Rate limiting
    app.add_middleware(RateLimitMiddleware, requests_per_minute=100)
    # 3. Audit logging
    app.add_middleware(AuditMiddleware)
    # 4. Tenant context injection (runs last, closest to route)
    app.add_middleware(TenantMiddleware)

    # Add exception handlers
    @app.exception_handler(AccessDeniedError)
    async def access_denied_handler(request: Request, exc: AccessDeniedError):
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={
                "error": "Access Denied",
                "detail": str(exc),
            },
        )

    @app.exception_handler(TenantMismatchError)
    async def tenant_mismatch_handler(request: Request, exc: TenantMismatchError):
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={
                "error": "Tenant Mismatch",
                "detail": str(exc),
            },
        )

    # Add validation exception handlers
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={
                "error": "Validation Error",
                "detail": exc.errors(),
            },
        )

    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        logger.exception(f"Unhandled exception: {exc}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "error": "Internal Server Error",
                "detail": str(exc) if debug else "An unexpected error occurred",
            },
        )

    # Include routers
    from automic_etl.api.routes import (
        health,
        pipelines,
        tables,
        queries,
        connectors,
        lineage,
        jobs,
        companies,
        admin,
        airflow,
    )

    app.include_router(health.router, prefix="/api/v1", tags=["health"])
    app.include_router(pipelines.router, prefix="/api/v1/pipelines", tags=["pipelines"])
    app.include_router(tables.router, prefix="/api/v1/tables", tags=["tables"])
    app.include_router(queries.router, prefix="/api/v1/queries", tags=["queries"])
    app.include_router(connectors.router, prefix="/api/v1/connectors", tags=["connectors"])
    app.include_router(lineage.router, prefix="/api/v1/lineage", tags=["lineage"])
    app.include_router(jobs.router, prefix="/api/v1/jobs", tags=["jobs"])
    app.include_router(companies.router, prefix="/api/v1/companies", tags=["companies"])
    app.include_router(admin.router, prefix="/api/v1/admin", tags=["admin"])
    app.include_router(airflow.router, prefix="/api/v1", tags=["airflow"])

    return app


def get_app() -> FastAPI:
    """Get the FastAPI application instance."""
    global app
    if app is None:
        app = create_app()
    return app
