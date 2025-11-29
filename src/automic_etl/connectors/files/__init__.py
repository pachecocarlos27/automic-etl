"""File connectors."""

from automic_etl.connectors.files.csv_connector import CSVConnector
from automic_etl.connectors.files.json_connector import JSONConnector
from automic_etl.connectors.files.parquet_connector import ParquetConnector

__all__ = [
    "CSVConnector",
    "JSONConnector",
    "ParquetConnector",
]
