"""Silver layer implementation - Cleaned and validated data."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

import polars as pl
import structlog

from automic_etl.core.config import Settings
from automic_etl.core.exceptions import TransformationError
from automic_etl.storage.iceberg import IcebergTableManager
from automic_etl.core.utils import utc_now

logger = structlog.get_logger()


class SilverLayer:
    """
    Silver Layer - Cleaned and Validated Data.

    The silver layer contains cleaned, validated, and standardized data.
    It applies data quality rules and transformations to bronze data.

    Features:
    - Data cleaning and normalization
    - Schema validation and enforcement
    - Deduplication
    - Data type standardization
    - Null handling
    - Data quality checks
    """

    NAMESPACE = "silver"

    # Metadata columns for silver layer
    METADATA_COLUMNS = [
        "_processing_time",
        "_bronze_table",
        "_bronze_batch_id",
        "_processing_date",
        "_quality_score",
    ]

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.table_manager = IcebergTableManager(settings)
        self.logger = logger.bind(layer="silver")

    def process(
        self,
        source_table: str,
        target_table: str,
        transformations: list[Callable[[pl.DataFrame], pl.DataFrame]] | None = None,
        dedup_columns: list[str] | None = None,
        schema_mapping: dict[str, str] | None = None,
        quality_checks: list[Callable[[pl.DataFrame], pl.DataFrame]] | None = None,
        incremental: bool = True,
        watermark_column: str = "_ingestion_time",
    ) -> int:
        """
        Process data from bronze to silver layer.

        Args:
            source_table: Bronze table name
            target_table: Silver table name
            transformations: List of transformation functions
            dedup_columns: Columns for deduplication
            schema_mapping: Column rename mapping
            quality_checks: Data quality check functions
            incremental: Whether to process incrementally
            watermark_column: Column to use for incremental processing

        Returns:
            Number of rows processed
        """
        from automic_etl.medallion.bronze import BronzeLayer

        bronze = BronzeLayer(self.settings)

        # Get data from bronze
        if incremental and self.table_exists(target_table):
            # Get last processing time
            last_processed = self._get_last_watermark(target_table)
            if last_processed:
                df = bronze.read_new_since(source_table, last_processed)
            else:
                df = bronze.read(source_table)
        else:
            df = bronze.read(source_table)

        if df.is_empty():
            self.logger.info(
                "No new data to process",
                source=source_table,
                target=target_table,
            )
            return 0

        # Apply processing pipeline
        df = self._apply_pipeline(
            df=df,
            transformations=transformations,
            dedup_columns=dedup_columns,
            schema_mapping=schema_mapping,
            quality_checks=quality_checks,
        )

        # Add silver metadata
        processing_time = utc_now()
        df = self._add_metadata(
            df=df,
            bronze_table=source_table,
            processing_time=processing_time,
        )

        # Write to silver layer
        return self._write(target_table, df, processing_time)

    def _apply_pipeline(
        self,
        df: pl.DataFrame,
        transformations: list[Callable[[pl.DataFrame], pl.DataFrame]] | None = None,
        dedup_columns: list[str] | None = None,
        schema_mapping: dict[str, str] | None = None,
        quality_checks: list[Callable[[pl.DataFrame], pl.DataFrame]] | None = None,
    ) -> pl.DataFrame:
        """Apply the full processing pipeline."""
        # 1. Apply schema mapping (rename columns)
        if schema_mapping:
            df = self._apply_schema_mapping(df, schema_mapping)

        # 2. Apply standard cleaning
        df = self._standard_cleaning(df)

        # 3. Apply custom transformations
        if transformations:
            for transform in transformations:
                try:
                    df = transform(df)
                except Exception as e:
                    raise TransformationError(
                        f"Transformation failed: {str(e)}",
                        transformation=transform.__name__,
                    )

        # 4. Deduplicate
        if dedup_columns:
            df = self._deduplicate(df, dedup_columns)

        # 5. Apply quality checks
        if quality_checks:
            for check in quality_checks:
                df = check(df)

        return df

    def _standard_cleaning(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply standard cleaning transformations."""
        settings = self.settings.transformation

        # Trim whitespace from string columns
        if settings.trim_whitespace:
            string_cols = [
                col for col, dtype in df.schema.items()
                if dtype in (pl.Utf8, pl.String)
            ]
            for col in string_cols:
                df = df.with_columns(pl.col(col).str.strip_chars())

        # Handle null string values
        null_values = self.settings.transformation.null_string_values
        if null_values:
            string_cols = [
                col for col, dtype in df.schema.items()
                if dtype in (pl.Utf8, pl.String)
            ]
            for col in string_cols:
                df = df.with_columns(
                    pl.when(pl.col(col).is_in(null_values))
                    .then(None)
                    .otherwise(pl.col(col))
                    .alias(col)
                )

        return df

    def _apply_schema_mapping(
        self,
        df: pl.DataFrame,
        mapping: dict[str, str],
    ) -> pl.DataFrame:
        """Apply column renames from schema mapping."""
        for old_name, new_name in mapping.items():
            if old_name in df.columns:
                df = df.rename({old_name: new_name})
        return df

    def _deduplicate(
        self,
        df: pl.DataFrame,
        dedup_columns: list[str],
        keep: str = "last",
    ) -> pl.DataFrame:
        """Deduplicate based on specified columns."""
        original_count = len(df)

        # Sort by processing time if available, then deduplicate
        if "_ingestion_time" in df.columns:
            df = df.sort("_ingestion_time", descending=(keep == "last"))

        df = df.unique(subset=dedup_columns, keep=keep)

        dedup_count = original_count - len(df)
        if dedup_count > 0:
            self.logger.info(
                "Deduplicated rows",
                removed=dedup_count,
                columns=dedup_columns,
            )

        return df

    def _add_metadata(
        self,
        df: pl.DataFrame,
        bronze_table: str,
        processing_time: datetime,
    ) -> pl.DataFrame:
        """Add silver layer metadata columns."""
        # Get bronze batch ID if available
        bronze_batch_id = None
        if "_batch_id" in df.columns:
            bronze_batch_id = df.select(pl.col("_batch_id").first()).item()

        return df.with_columns([
            pl.lit(processing_time).alias("_processing_time"),
            pl.lit(bronze_table).alias("_bronze_table"),
            pl.lit(bronze_batch_id).alias("_bronze_batch_id"),
            pl.lit(processing_time.date()).alias("_processing_date"),
        ])

    def _write(
        self,
        table_name: str,
        df: pl.DataFrame,
        processing_time: datetime,
    ) -> int:
        """Write data to silver layer."""
        try:
            if self.table_exists(table_name):
                rows = self.table_manager.append(self.NAMESPACE, table_name, df)
            else:
                partition_columns = self.settings.medallion.silver.partition_by
                self.table_manager.create_table_from_dataframe(
                    namespace=self.NAMESPACE,
                    table_name=table_name,
                    df=df,
                    partition_columns=partition_columns,
                    properties={
                        "automic.layer": "silver",
                        "automic.created": processing_time.isoformat(),
                    },
                )
                rows = self.table_manager.append(self.NAMESPACE, table_name, df)

            self.logger.info(
                "Processed data to silver",
                table=table_name,
                rows=rows,
            )
            return rows

        except Exception as e:
            raise TransformationError(
                f"Failed to write to silver layer: {str(e)}",
                transformation="write_silver",
                details={"table": table_name},
            )

    def read(
        self,
        table_name: str,
        columns: list[str] | None = None,
        filter_expr: str | None = None,
        limit: int | None = None,
    ) -> pl.DataFrame:
        """Read data from the silver layer."""
        return self.table_manager.read(
            namespace=self.NAMESPACE,
            table_name=table_name,
            columns=columns,
            filter_expr=filter_expr,
            limit=limit,
        )

    def list_tables(self) -> list[str]:
        """List all tables in the silver layer."""
        return self.table_manager.catalog.list_tables(self.NAMESPACE)

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        return self.table_manager.catalog.table_exists(self.NAMESPACE, table_name)

    def _get_last_watermark(self, table_name: str) -> datetime | None:
        """Get the last processing time for incremental processing."""
        try:
            df = self.read(table_name, columns=["_processing_time"])
            if df.is_empty():
                return None
            return df.select(pl.col("_processing_time").max()).item()
        except Exception:
            return None


# ============================================================================
# Common Transformations
# ============================================================================

def normalize_column_names(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize column names to snake_case."""
    import re

    def to_snake_case(name: str) -> str:
        name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
        name = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", name)
        name = re.sub(r"[-\s]+", "_", name)
        return name.lower()

    rename_map = {col: to_snake_case(col) for col in df.columns}
    return df.rename(rename_map)


def cast_timestamps(
    df: pl.DataFrame,
    columns: list[str],
    format: str = "%Y-%m-%d %H:%M:%S",
) -> pl.DataFrame:
    """Cast string columns to timestamps."""
    for col in columns:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).str.to_datetime(format).alias(col)
            )
    return df


def fill_nulls(
    df: pl.DataFrame,
    fills: dict[str, Any],
) -> pl.DataFrame:
    """Fill null values in specified columns."""
    for col, value in fills.items():
        if col in df.columns:
            df = df.with_columns(pl.col(col).fill_null(value))
    return df


def filter_invalid_rows(
    df: pl.DataFrame,
    required_columns: list[str],
) -> pl.DataFrame:
    """Remove rows where required columns are null."""
    for col in required_columns:
        if col in df.columns:
            df = df.filter(pl.col(col).is_not_null())
    return df
