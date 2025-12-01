"""REST API for Automic ETL."""

from automic_etl.api.main import create_app, get_app, app
from automic_etl.api.routes import (
    pipelines,
    tables,
    queries,
    connectors,
    lineage,
    health,
    jobs,
    companies,
    admin,
)

__all__ = [
    "create_app",
    "get_app",
    "app",
    "pipelines",
    "tables",
    "queries",
    "connectors",
    "lineage",
    "health",
    "jobs",
    "companies",
    "admin",
]
