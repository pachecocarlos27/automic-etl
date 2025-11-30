"""API route modules."""

from automic_etl.api.routes import health
from automic_etl.api.routes import pipelines
from automic_etl.api.routes import tables
from automic_etl.api.routes import queries
from automic_etl.api.routes import connectors
from automic_etl.api.routes import lineage
from automic_etl.api.routes import jobs

__all__ = [
    "health",
    "pipelines",
    "tables",
    "queries",
    "connectors",
    "lineage",
    "jobs",
]
