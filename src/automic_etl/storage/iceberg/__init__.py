"""Apache Iceberg integration for Automic ETL."""

from automic_etl.storage.iceberg.catalog import IcebergCatalog
from automic_etl.storage.iceberg.tables import IcebergTableManager
from automic_etl.storage.iceberg.schemas import SchemaBuilder, schema_from_polars

__all__ = [
    "IcebergCatalog",
    "IcebergTableManager",
    "SchemaBuilder",
    "schema_from_polars",
]
