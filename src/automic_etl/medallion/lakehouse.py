"""Lakehouse orchestration combining all medallion layers."""

from __future__ import annotations

from typing import Any, Callable

import polars as pl
import structlog

from automic_etl.core.config import Settings, get_settings
from automic_etl.core.exceptions import LoadError
from automic_etl.core.utils import utc_now
from automic_etl.core.validation import (
    validate_in_choices,
    validate_non_empty_string,
    validate_table_name,
)
from automic_etl.medallion.bronze import BronzeLayer
from automic_etl.medallion.silver import SilverLayer
from automic_etl.medallion.gold import GoldLayer, AggregationType, MetricDefinition

logger = structlog.get_logger()


class Lakehouse:
    """
    Unified interface for the Lakehouse.

    Orchestrates data flow through the medallion architecture:
    Bronze (raw) -> Silver (cleaned) -> Gold (aggregated)

    Provides a high-level API for common lakehouse operations.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.bronze = BronzeLayer(self.settings)
        self.silver = SilverLayer(self.settings)
        self.gold = GoldLayer(self.settings)
        self.logger = logger.bind(component="lakehouse")

    # =========================================================================
    # High-Level Ingestion
    # =========================================================================

    def ingest(
        self,
        table_name: str,
        data: pl.DataFrame | dict | list | bytes | str,
        source: str,
        data_type: str = "structured",
        **kwargs: Any,
    ) -> int:
        """
        Ingest data into the bronze layer.

        Automatically handles structured, semi-structured, and unstructured data.

        Args:
            table_name: Target table name
            data: Data to ingest (DataFrame, dict, list, bytes, or str)
            source: Data source identifier
            data_type: Type of data ('structured', 'semi_structured', 'unstructured')
            **kwargs: Additional arguments passed to the ingest method

        Returns:
            Number of rows ingested

        Raises:
            LoadError: If ingestion fails or invalid parameters provided
        """
        try:
            validate_table_name(table_name)
            validate_non_empty_string(source, "source")
            validate_in_choices(
                data_type,
                {"structured", "semi_structured", "unstructured"},
                "data_type",
            )

            self.logger.info(
                "Ingesting data",
                table=table_name,
                source=source,
                data_type=data_type,
            )

            if data_type == "structured":
                if isinstance(data, pl.DataFrame):
                    return self.bronze.ingest(table_name, data, source, **kwargs)
                elif isinstance(data, (dict, list)):
                    df = pl.DataFrame(data if isinstance(data, list) else [data])
                    return self.bronze.ingest(table_name, df, source, **kwargs)

            elif data_type == "semi_structured":
                return self.bronze.ingest_semi_structured(
                    table_name=table_name,
                    data=data,
                    source=source,
                    **kwargs,
                )

            elif data_type == "unstructured":
                content_type = kwargs.pop("content_type", "application/octet-stream")
                return self.bronze.ingest_unstructured(
                    table_name=table_name,
                    content=data,
                    source=source,
                    content_type=content_type,
                    **kwargs,
                )

        except Exception as e:
            self.logger.error(
                "Ingestion failed",
                table=table_name,
                source=source,
                error=str(e),
            )
            raise LoadError(
                f"Failed to ingest data to {table_name}: {str(e)}",
                target=table_name,
                details={"source": source, "data_type": data_type},
            ) from e

        raise LoadError(
            f"Unknown data_type: {data_type}",
            target=table_name,
        )

    # =========================================================================
    # End-to-End Pipeline
    # =========================================================================

    def process_to_silver(
        self,
        bronze_table: str,
        silver_table: str | None = None,
        transformations: list[Callable[[pl.DataFrame], pl.DataFrame]] | None = None,
        dedup_columns: list[str] | None = None,
        incremental: bool = True,
    ) -> int:
        """
        Process data from bronze to silver layer.

        Args:
            bronze_table: Source bronze table
            silver_table: Target silver table (defaults to bronze_table name)
            transformations: Custom transformations to apply
            dedup_columns: Columns for deduplication
            incremental: Process incrementally

        Returns:
            Number of rows processed
        """
        silver_table = silver_table or bronze_table

        self.logger.info(
            "Processing to silver",
            bronze_table=bronze_table,
            silver_table=silver_table,
        )

        return self.silver.process(
            source_table=bronze_table,
            target_table=silver_table,
            transformations=transformations,
            dedup_columns=dedup_columns,
            incremental=incremental,
        )

    def aggregate_to_gold(
        self,
        silver_table: str,
        gold_table: str,
        group_by: list[str],
        aggregations: dict[str, list[tuple[str, AggregationType]]],
        filter_expr: str | None = None,
    ) -> int:
        """
        Aggregate data from silver to gold layer.

        Args:
            silver_table: Source silver table
            gold_table: Target gold table
            group_by: Columns to group by
            aggregations: Aggregation definitions
            filter_expr: Optional filter

        Returns:
            Number of rows written
        """
        self.logger.info(
            "Aggregating to gold",
            silver_table=silver_table,
            gold_table=gold_table,
        )

        return self.gold.aggregate(
            source_table=silver_table,
            target_table=gold_table,
            group_by=group_by,
            aggregations=aggregations,
            filter_expr=filter_expr,
        )

    def full_pipeline(
        self,
        table_name: str,
        data: pl.DataFrame,
        source: str,
        transformations: list[Callable[[pl.DataFrame], pl.DataFrame]] | None = None,
        dedup_columns: list[str] | None = None,
        gold_aggregations: dict[str, Any] | None = None,
    ) -> dict[str, int]:
        """
        Run a full pipeline from ingestion to gold.

        Args:
            table_name: Table name (used for all layers)
            data: Data to ingest
            source: Data source
            transformations: Transformations for silver
            dedup_columns: Dedup columns for silver
            gold_aggregations: If provided, creates gold aggregations

        Returns:
            Dict with row counts for each layer
        """
        results = {}

        # Bronze
        results["bronze"] = self.ingest(table_name, data, source)

        # Silver
        results["silver"] = self.process_to_silver(
            bronze_table=table_name,
            silver_table=table_name,
            transformations=transformations,
            dedup_columns=dedup_columns,
            incremental=False,
        )

        # Gold (optional)
        if gold_aggregations:
            results["gold"] = self.aggregate_to_gold(
                silver_table=table_name,
                gold_table=f"{table_name}_aggregated",
                **gold_aggregations,
            )

        self.logger.info("Full pipeline completed", results=results)
        return results

    # =========================================================================
    # Query Interface
    # =========================================================================

    def query(
        self,
        table: str,
        layer: str = "silver",
        columns: list[str] | None = None,
        filter_expr: str | None = None,
        limit: int | None = None,
    ) -> pl.DataFrame:
        """
        Query data from any layer.

        Args:
            table: Table name
            layer: Layer to query ('bronze', 'silver', 'gold')
            columns: Columns to select
            filter_expr: Filter expression
            limit: Row limit

        Returns:
            Query results as DataFrame
        """
        layer_map = {
            "bronze": self.bronze,
            "silver": self.silver,
            "gold": self.gold,
        }

        if layer not in layer_map:
            raise ValueError(f"Unknown layer: {layer}")

        return layer_map[layer].read(
            table_name=table,
            columns=columns,
            filter_expr=filter_expr,
            limit=limit,
        )

    def sql(self, query: str) -> pl.DataFrame:
        """
        Execute a SQL query across lakehouse tables.

        Uses Polars SQL context to query data.

        Args:
            query: SQL query string

        Returns:
            Query results as DataFrame
        """
        # Register tables in SQL context
        ctx = pl.SQLContext()

        # Register bronze tables
        for table in self.bronze.list_tables():
            df = self.bronze.read(table)
            ctx.register(f"bronze_{table}", df)

        # Register silver tables
        for table in self.silver.list_tables():
            df = self.silver.read(table)
            ctx.register(f"silver_{table}", df)

        # Register gold tables
        for table in self.gold.list_tables():
            df = self.gold.read(table)
            ctx.register(f"gold_{table}", df)

        return ctx.execute(query).collect()

    # =========================================================================
    # Management
    # =========================================================================

    def list_tables(self, layer: str | None = None) -> dict[str, list[str]]:
        """
        List all tables in the lakehouse.

        Args:
            layer: Optional layer filter

        Returns:
            Dict of layer -> table list
        """
        if layer:
            layer_map = {
                "bronze": self.bronze,
                "silver": self.silver,
                "gold": self.gold,
            }
            if layer not in layer_map:
                raise ValueError(f"Unknown layer: {layer}")
            return {layer: layer_map[layer].list_tables()}

        return {
            "bronze": self.bronze.list_tables(),
            "silver": self.silver.list_tables(),
            "gold": self.gold.list_tables(),
        }

    def get_table_info(self, table: str, layer: str) -> dict[str, Any]:
        """Get information about a table."""
        from automic_etl.storage.iceberg import IcebergTableManager

        table_manager = IcebergTableManager(self.settings)
        namespace = layer

        if not table_manager.catalog.table_exists(namespace, table):
            raise ValueError(f"Table {namespace}.{table} does not exist")

        iceberg_table = table_manager.catalog.load_table(namespace, table)
        schema = iceberg_table.schema()

        return {
            "name": table,
            "layer": layer,
            "location": iceberg_table.location(),
            "columns": [
                {
                    "name": field.name,
                    "type": str(field.field_type),
                    "required": field.required,
                }
                for field in schema.fields
            ],
            "snapshots": len(list(iceberg_table.history())),
        }

    def initialize(self) -> None:
        """Initialize the lakehouse namespaces."""
        from automic_etl.storage.iceberg import IcebergCatalog

        catalog = IcebergCatalog(self.settings)

        for namespace in ["bronze", "silver", "gold"]:
            catalog.ensure_namespace(namespace)

        self.logger.info("Lakehouse initialized")

    def cleanup(
        self,
        bronze_retention_days: int | None = None,
        expire_snapshots: bool = True,
    ) -> dict[str, Any]:
        """
        Clean up old data and snapshots.

        Args:
            bronze_retention_days: Days to retain bronze data (uses config default if None)
            expire_snapshots: Whether to expire old snapshots

        Returns:
            Cleanup statistics
        """
        from datetime import datetime, timedelta

        retention = bronze_retention_days or self.settings.medallion.bronze.retention_days
        if retention is None:
            retention = 90

        cutoff = utc_now() - timedelta(days=retention)
        stats = {"expired_snapshots": 0, "tables_cleaned": 0}

        from automic_etl.storage.iceberg import IcebergTableManager

        table_manager = IcebergTableManager(self.settings)

        # Expire snapshots for all tables
        if expire_snapshots:
            for layer in ["bronze", "silver", "gold"]:
                for table in self.list_tables(layer).get(layer, []):
                    try:
                        table_manager.expire_snapshots(layer, table, cutoff)
                        stats["expired_snapshots"] += 1
                    except Exception as e:
                        self.logger.warning(
                            "Failed to expire snapshots",
                            table=f"{layer}.{table}",
                            error=str(e),
                        )

        self.logger.info("Cleanup completed", stats=stats)
        return stats
