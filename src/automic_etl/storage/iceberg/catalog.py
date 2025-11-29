"""Iceberg catalog management."""

from __future__ import annotations

from typing import Any

import structlog
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
)
from pyiceberg.table import Table

from automic_etl.core.config import CatalogType, Settings
from automic_etl.core.exceptions import IcebergError

logger = structlog.get_logger()


class IcebergCatalog:
    """Wrapper for Iceberg catalog operations."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._catalog: Catalog | None = None
        self.logger = logger.bind(component="iceberg_catalog")

    @property
    def catalog(self) -> Catalog:
        """Get or create the Iceberg catalog."""
        if self._catalog is None:
            self._catalog = self._create_catalog()
        return self._catalog

    def _create_catalog(self) -> Catalog:
        """Create the Iceberg catalog based on settings."""
        catalog_config = self.settings.iceberg.catalog
        warehouse = self.settings.get_warehouse_path()

        self.logger.info(
            "Initializing Iceberg catalog",
            catalog_type=catalog_config.type.value,
            catalog_name=catalog_config.name,
            warehouse=warehouse,
        )

        try:
            if catalog_config.type == CatalogType.SQL:
                return SqlCatalog(
                    name=catalog_config.name,
                    **{
                        "uri": catalog_config.uri,
                        "warehouse": warehouse,
                    },
                )
            elif catalog_config.type == CatalogType.GLUE:
                return load_catalog(
                    catalog_config.name,
                    **{
                        "type": "glue",
                        "warehouse": warehouse,
                    },
                )
            elif catalog_config.type == CatalogType.REST:
                return load_catalog(
                    catalog_config.name,
                    **{
                        "type": "rest",
                        "uri": catalog_config.uri,
                        "warehouse": warehouse,
                    },
                )
            elif catalog_config.type == CatalogType.HIVE:
                return load_catalog(
                    catalog_config.name,
                    **{
                        "type": "hive",
                        "uri": catalog_config.uri,
                        "warehouse": warehouse,
                    },
                )
            elif catalog_config.type == CatalogType.DYNAMODB:
                return load_catalog(
                    catalog_config.name,
                    **{
                        "type": "dynamodb",
                        "warehouse": warehouse,
                    },
                )
            else:
                raise IcebergError(
                    f"Unsupported catalog type: {catalog_config.type}",
                    operation="create_catalog",
                )
        except Exception as e:
            raise IcebergError(
                f"Failed to create catalog: {str(e)}",
                operation="create_catalog",
                details={"catalog_type": catalog_config.type.value},
            )

    # =========================================================================
    # Namespace Operations
    # =========================================================================

    def create_namespace(
        self,
        namespace: str,
        properties: dict[str, str] | None = None,
    ) -> None:
        """Create a namespace if it doesn't exist."""
        try:
            self.catalog.create_namespace(namespace, properties or {})
            self.logger.info("Created namespace", namespace=namespace)
        except NamespaceAlreadyExistsError:
            self.logger.debug("Namespace already exists", namespace=namespace)
        except Exception as e:
            raise IcebergError(
                f"Failed to create namespace: {str(e)}",
                operation="create_namespace",
                details={"namespace": namespace},
            )

    def namespace_exists(self, namespace: str) -> bool:
        """Check if a namespace exists."""
        try:
            self.catalog.load_namespace_properties(namespace)
            return True
        except NoSuchNamespaceError:
            return False

    def list_namespaces(self) -> list[str]:
        """List all namespaces."""
        try:
            namespaces = self.catalog.list_namespaces()
            return [".".join(ns) for ns in namespaces]
        except Exception as e:
            raise IcebergError(
                f"Failed to list namespaces: {str(e)}",
                operation="list_namespaces",
            )

    def drop_namespace(self, namespace: str) -> None:
        """Drop a namespace."""
        try:
            self.catalog.drop_namespace(namespace)
            self.logger.info("Dropped namespace", namespace=namespace)
        except NoSuchNamespaceError:
            self.logger.debug("Namespace does not exist", namespace=namespace)
        except Exception as e:
            raise IcebergError(
                f"Failed to drop namespace: {str(e)}",
                operation="drop_namespace",
                details={"namespace": namespace},
            )

    # =========================================================================
    # Table Operations
    # =========================================================================

    def table_exists(self, namespace: str, table_name: str) -> bool:
        """Check if a table exists."""
        try:
            self.catalog.load_table(f"{namespace}.{table_name}")
            return True
        except NoSuchTableError:
            return False

    def load_table(self, namespace: str, table_name: str) -> Table:
        """Load an existing table."""
        try:
            return self.catalog.load_table(f"{namespace}.{table_name}")
        except NoSuchTableError:
            raise IcebergError(
                f"Table not found: {namespace}.{table_name}",
                table=f"{namespace}.{table_name}",
                operation="load_table",
            )
        except Exception as e:
            raise IcebergError(
                f"Failed to load table: {str(e)}",
                table=f"{namespace}.{table_name}",
                operation="load_table",
            )

    def list_tables(self, namespace: str) -> list[str]:
        """List all tables in a namespace."""
        try:
            tables = self.catalog.list_tables(namespace)
            return [".".join(t) for t in tables]
        except NoSuchNamespaceError:
            return []
        except Exception as e:
            raise IcebergError(
                f"Failed to list tables: {str(e)}",
                operation="list_tables",
                details={"namespace": namespace},
            )

    def drop_table(self, namespace: str, table_name: str, purge: bool = False) -> None:
        """Drop a table."""
        identifier = f"{namespace}.{table_name}"
        try:
            self.catalog.drop_table(identifier, purge=purge)
            self.logger.info("Dropped table", table=identifier, purge=purge)
        except NoSuchTableError:
            self.logger.debug("Table does not exist", table=identifier)
        except Exception as e:
            raise IcebergError(
                f"Failed to drop table: {str(e)}",
                table=identifier,
                operation="drop_table",
            )

    def rename_table(
        self,
        namespace: str,
        old_name: str,
        new_name: str,
    ) -> None:
        """Rename a table."""
        old_identifier = f"{namespace}.{old_name}"
        new_identifier = f"{namespace}.{new_name}"
        try:
            self.catalog.rename_table(old_identifier, new_identifier)
            self.logger.info(
                "Renamed table",
                old_name=old_identifier,
                new_name=new_identifier,
            )
        except Exception as e:
            raise IcebergError(
                f"Failed to rename table: {str(e)}",
                table=old_identifier,
                operation="rename_table",
            )

    # =========================================================================
    # Convenience Methods
    # =========================================================================

    def ensure_namespace(self, namespace: str) -> None:
        """Ensure a namespace exists, creating it if necessary."""
        if not self.namespace_exists(namespace):
            self.create_namespace(namespace)

    def get_table_location(self, namespace: str, table_name: str) -> str:
        """Get the storage location of a table."""
        table = self.load_table(namespace, table_name)
        return table.location()

    def get_table_schema(self, namespace: str, table_name: str) -> Any:
        """Get the schema of a table."""
        table = self.load_table(namespace, table_name)
        return table.schema()

    def get_table_snapshots(self, namespace: str, table_name: str) -> list[Any]:
        """Get all snapshots of a table."""
        table = self.load_table(namespace, table_name)
        return list(table.history())
