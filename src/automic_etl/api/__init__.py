"""REST API for Automic ETL."""

from automic_etl.api.main import create_app, app
from automic_etl.api.routes import pipelines, tables, queries, connectors, lineage, health

__all__ = [
    "create_app",
    "app",
    "pipelines",
    "tables",
    "queries",
    "connectors",
    "lineage",
    "health",
]
