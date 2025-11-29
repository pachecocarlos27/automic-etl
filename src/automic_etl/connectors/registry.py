"""Connector registry for dynamic connector creation."""

from __future__ import annotations

from typing import Any, Type

from automic_etl.connectors.base import BaseConnector, ConnectorConfig, ConnectorType


class ConnectorRegistry:
    """Registry for managing connector types."""

    _connectors: dict[str, Type[BaseConnector]] = {}

    @classmethod
    def register(cls, name: str, connector_class: Type[BaseConnector]) -> None:
        """Register a connector class."""
        cls._connectors[name.lower()] = connector_class

    @classmethod
    def get(cls, name: str) -> Type[BaseConnector] | None:
        """Get a connector class by name."""
        return cls._connectors.get(name.lower())

    @classmethod
    def list_connectors(cls) -> list[str]:
        """List all registered connector names."""
        return list(cls._connectors.keys())

    @classmethod
    def create(cls, name: str, config: ConnectorConfig) -> BaseConnector:
        """Create a connector instance by name."""
        connector_class = cls.get(name)
        if connector_class is None:
            raise ValueError(f"Unknown connector: {name}")
        return connector_class(config)


# Register built-in connectors
def _register_builtin_connectors() -> None:
    """Register all built-in connectors."""
    # Database connectors
    from automic_etl.connectors.databases.postgresql import PostgreSQLConnector
    from automic_etl.connectors.databases.mysql import MySQLConnector
    from automic_etl.connectors.databases.mongodb import MongoDBConnector

    ConnectorRegistry.register("postgresql", PostgreSQLConnector)
    ConnectorRegistry.register("postgres", PostgreSQLConnector)
    ConnectorRegistry.register("mysql", MySQLConnector)
    ConnectorRegistry.register("mongodb", MongoDBConnector)
    ConnectorRegistry.register("mongo", MongoDBConnector)

    # File connectors
    from automic_etl.connectors.files.csv_connector import CSVConnector
    from automic_etl.connectors.files.json_connector import JSONConnector
    from automic_etl.connectors.files.parquet_connector import ParquetConnector

    ConnectorRegistry.register("csv", CSVConnector)
    ConnectorRegistry.register("json", JSONConnector)
    ConnectorRegistry.register("parquet", ParquetConnector)

    # Unstructured connectors
    from automic_etl.connectors.unstructured.pdf import PDFConnector
    from automic_etl.connectors.unstructured.documents import DocumentConnector

    ConnectorRegistry.register("pdf", PDFConnector)
    ConnectorRegistry.register("document", DocumentConnector)
    ConnectorRegistry.register("docx", DocumentConnector)
    ConnectorRegistry.register("word", DocumentConnector)


# Register connectors on import
_register_builtin_connectors()


def get_connector(
    connector_type: str,
    **config_kwargs: Any,
) -> BaseConnector:
    """
    Convenience function to create a connector.

    Args:
        connector_type: Type of connector (e.g., 'postgresql', 'csv', 'pdf')
        **config_kwargs: Configuration parameters for the connector

    Returns:
        Configured connector instance

    Example:
        >>> conn = get_connector('postgresql', host='localhost', database='mydb')
        >>> conn.connect()
        >>> result = conn.extract(table='users')
    """
    connector_class = ConnectorRegistry.get(connector_type)
    if connector_class is None:
        raise ValueError(
            f"Unknown connector type: {connector_type}. "
            f"Available: {ConnectorRegistry.list_connectors()}"
        )

    # Determine the config class based on connector type
    config_mapping = {
        "postgresql": "automic_etl.connectors.databases.postgresql.PostgreSQLConfig",
        "postgres": "automic_etl.connectors.databases.postgresql.PostgreSQLConfig",
        "mysql": "automic_etl.connectors.databases.mysql.MySQLConfig",
        "mongodb": "automic_etl.connectors.databases.mongodb.MongoDBConfig",
        "mongo": "automic_etl.connectors.databases.mongodb.MongoDBConfig",
        "csv": "automic_etl.connectors.files.csv_connector.CSVConfig",
        "json": "automic_etl.connectors.files.json_connector.JSONConfig",
        "parquet": "automic_etl.connectors.files.parquet_connector.ParquetConfig",
        "pdf": "automic_etl.connectors.unstructured.pdf.PDFConfig",
        "document": "automic_etl.connectors.unstructured.documents.DocumentConfig",
        "docx": "automic_etl.connectors.unstructured.documents.DocumentConfig",
        "word": "automic_etl.connectors.unstructured.documents.DocumentConfig",
    }

    config_path = config_mapping.get(connector_type.lower())
    if config_path:
        module_path, class_name = config_path.rsplit(".", 1)
        import importlib
        module = importlib.import_module(module_path)
        config_class = getattr(module, class_name)
        config = config_class(name=connector_type, **config_kwargs)
    else:
        config = ConnectorConfig(
            name=connector_type,
            connector_type=ConnectorType.DATABASE,
            **config_kwargs,
        )

    return connector_class(config)
