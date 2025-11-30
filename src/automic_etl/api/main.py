"""FastAPI application for Automic ETL REST API."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

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

    # Add exception handlers
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
    from automic_etl.api.routes import health, pipelines, tables, queries, connectors, lineage, jobs

    app.include_router(health.router, prefix="/api/v1", tags=["health"])
    app.include_router(pipelines.router, prefix="/api/v1/pipelines", tags=["pipelines"])
    app.include_router(tables.router, prefix="/api/v1/tables", tags=["tables"])
    app.include_router(queries.router, prefix="/api/v1/queries", tags=["queries"])
    app.include_router(connectors.router, prefix="/api/v1/connectors", tags=["connectors"])
    app.include_router(lineage.router, prefix="/api/v1/lineage", tags=["lineage"])
    app.include_router(jobs.router, prefix="/api/v1/jobs", tags=["jobs"])

    return app


def get_app() -> FastAPI:
    """Get the FastAPI application instance."""
    global app
    if app is None:
        app = create_app()
    return app
