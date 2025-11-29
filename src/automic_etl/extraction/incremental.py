"""Incremental extraction functionality."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable

import polars as pl
import structlog

from automic_etl.connectors.base import BaseConnector
from automic_etl.core.config import Settings
from automic_etl.extraction.watermark import WatermarkManager
from automic_etl.utils.helpers import parse_duration

logger = structlog.get_logger()


@dataclass
class IncrementalResult:
    """Result of incremental extraction."""

    data: pl.DataFrame
    new_rows: int
    previous_watermark: Any | None
    new_watermark: Any | None
    is_initial_load: bool
    extraction_time: datetime


class IncrementalExtractor:
    """
    Extract data incrementally using watermarks.

    Features:
    - Timestamp-based watermarks
    - ID-based watermarks
    - Lookback windows for late-arriving data
    - Automatic watermark management
    """

    def __init__(
        self,
        settings: Settings,
        watermark_manager: WatermarkManager | None = None,
    ) -> None:
        self.settings = settings
        self.watermark_manager = watermark_manager or WatermarkManager(settings)
        self.logger = logger.bind(component="incremental_extractor")

        # Get default settings
        inc_config = settings.extraction.incremental
        self.default_watermark_column = inc_config.watermark_column
        self.lookback_window = parse_duration(inc_config.lookback_window)

    def extract(
        self,
        connector: BaseConnector,
        source_name: str,
        watermark_column: str | None = None,
        table: str | None = None,
        query_template: str | None = None,
        lookback: timedelta | None = None,
        transform: Callable[[pl.DataFrame], pl.DataFrame] | None = None,
    ) -> IncrementalResult:
        """
        Extract data incrementally.

        Args:
            connector: Data connector
            source_name: Unique source identifier for watermark tracking
            watermark_column: Column to use for watermarking
            table: Table name (if using table extraction)
            query_template: Query template with {watermark} placeholder
            lookback: Override lookback window
            transform: Optional transformation

        Returns:
            IncrementalResult with new data
        """
        watermark_column = watermark_column or self.default_watermark_column
        lookback = lookback or self.lookback_window

        # Get previous watermark
        previous_watermark = self.watermark_manager.get_value(source_name)
        is_initial = previous_watermark is None

        # Apply lookback to handle late-arriving data
        effective_watermark = previous_watermark
        if previous_watermark is not None and lookback:
            if isinstance(previous_watermark, datetime):
                effective_watermark = previous_watermark - lookback

        self.logger.info(
            "Starting incremental extraction",
            source=source_name,
            watermark_column=watermark_column,
            previous_watermark=str(previous_watermark),
            effective_watermark=str(effective_watermark),
            is_initial=is_initial,
        )

        # Extract data
        if query_template:
            # Use query template
            if effective_watermark is not None:
                query = query_template.format(watermark=effective_watermark)
            else:
                # Initial load - remove WHERE clause or use full query
                query = self._remove_watermark_filter(query_template)

            result = connector.extract(query=query)
        else:
            # Use connector's incremental extraction
            result = connector.extract_incremental(
                watermark_column=watermark_column,
                last_watermark=effective_watermark,
                table=table,
            )

        df = result.data

        # Apply transformation
        if transform and not df.is_empty():
            df = transform(df)

        # Update watermark
        new_watermark = self.watermark_manager.update_from_dataframe(
            source_name=source_name,
            df=df,
            watermark_column=watermark_column,
        )

        self.logger.info(
            "Incremental extraction completed",
            source=source_name,
            new_rows=len(df),
            new_watermark=str(new_watermark),
        )

        return IncrementalResult(
            data=df,
            new_rows=len(df),
            previous_watermark=previous_watermark,
            new_watermark=new_watermark,
            is_initial_load=is_initial,
            extraction_time=datetime.utcnow(),
        )

    def _remove_watermark_filter(self, query_template: str) -> str:
        """Remove watermark filter from query for initial load."""
        # Simple approach - look for common patterns
        import re

        # Remove WHERE {watermark} patterns
        patterns = [
            r"\s+WHERE\s+\w+\s*[><=]+\s*'\{watermark\}'",
            r"\s+AND\s+\w+\s*[><=]+\s*'\{watermark\}'",
            r"\s+WHERE\s+\w+\s*[><=]+\s*\{watermark\}",
            r"\s+AND\s+\w+\s*[><=]+\s*\{watermark\}",
        ]

        result = query_template
        for pattern in patterns:
            result = re.sub(pattern, "", result, flags=re.IGNORECASE)

        return result

    def extract_to_lakehouse(
        self,
        connector: BaseConnector,
        source_name: str,
        table_name: str,
        watermark_column: str | None = None,
        source_table: str | None = None,
    ) -> int:
        """
        Extract incrementally directly to lakehouse.

        Args:
            connector: Data connector
            source_name: Source identifier
            table_name: Target lakehouse table
            watermark_column: Watermark column
            source_table: Source table name

        Returns:
            Number of rows ingested
        """
        from automic_etl.medallion import Lakehouse

        result = self.extract(
            connector=connector,
            source_name=source_name,
            watermark_column=watermark_column,
            table=source_table,
        )

        if result.data.is_empty():
            return 0

        lakehouse = Lakehouse(self.settings)
        return lakehouse.ingest(
            table_name=table_name,
            data=result.data,
            source=source_name,
        )

    def reset_watermark(self, source_name: str) -> bool:
        """
        Reset watermark for a source (forces full reload).

        Args:
            source_name: Source identifier

        Returns:
            True if reset, False if not found
        """
        return self.watermark_manager.delete(source_name)

    def get_extraction_status(
        self,
        source_name: str,
    ) -> dict[str, Any]:
        """
        Get extraction status for a source.

        Args:
            source_name: Source identifier

        Returns:
            Status information
        """
        watermark = self.watermark_manager.get(source_name)

        if watermark is None:
            return {
                "source": source_name,
                "status": "not_started",
                "watermark": None,
                "last_updated": None,
            }

        return {
            "source": source_name,
            "status": "active",
            "watermark_column": watermark.column,
            "watermark_value": watermark.value,
            "last_updated": watermark.updated_at.isoformat(),
            "metadata": watermark.metadata,
        }

    def list_sources(self) -> list[dict[str, Any]]:
        """List all tracked sources with their status."""
        return [
            self.get_extraction_status(source)
            for source in self.watermark_manager.list_sources()
        ]


class CDCExtractor(IncrementalExtractor):
    """
    Change Data Capture (CDC) extractor.

    Supports:
    - Log-based CDC (database-specific)
    - Timestamp-based CDC
    - Soft deletes
    """

    def __init__(
        self,
        settings: Settings,
        watermark_manager: WatermarkManager | None = None,
    ) -> None:
        super().__init__(settings, watermark_manager)
        self.logger = logger.bind(component="cdc_extractor")

    def extract_changes(
        self,
        connector: BaseConnector,
        source_name: str,
        table: str,
        watermark_column: str = "updated_at",
        deleted_column: str | None = "deleted_at",
        primary_key: str | list[str] = "id",
    ) -> IncrementalResult:
        """
        Extract changed records including deletes.

        Args:
            connector: Data connector
            source_name: Source identifier
            table: Table name
            watermark_column: Column tracking modifications
            deleted_column: Column tracking soft deletes
            primary_key: Primary key column(s)

        Returns:
            IncrementalResult with changes
        """
        result = self.extract(
            connector=connector,
            source_name=source_name,
            watermark_column=watermark_column,
            table=table,
        )

        if result.data.is_empty():
            return result

        # Add change type column
        df = result.data

        if deleted_column and deleted_column in df.columns:
            df = df.with_columns(
                pl.when(pl.col(deleted_column).is_not_null())
                .then(pl.lit("DELETE"))
                .otherwise(
                    pl.when(result.is_initial_load)
                    .then(pl.lit("INSERT"))
                    .otherwise(pl.lit("UPSERT"))
                )
                .alias("_change_type")
            )
        else:
            df = df.with_columns(
                pl.lit("UPSERT" if not result.is_initial_load else "INSERT")
                .alias("_change_type")
            )

        result.data = df
        return result

    def apply_changes(
        self,
        target_df: pl.DataFrame,
        changes_df: pl.DataFrame,
        primary_key: str | list[str],
    ) -> pl.DataFrame:
        """
        Apply CDC changes to a target DataFrame.

        Args:
            target_df: Current target data
            changes_df: Changes to apply
            primary_key: Primary key column(s)

        Returns:
            Updated DataFrame
        """
        if isinstance(primary_key, str):
            primary_key = [primary_key]

        if "_change_type" not in changes_df.columns:
            # Assume all updates
            changes_df = changes_df.with_columns(pl.lit("UPSERT").alias("_change_type"))

        # Separate by change type
        deletes = changes_df.filter(pl.col("_change_type") == "DELETE")
        upserts = changes_df.filter(pl.col("_change_type") != "DELETE")

        # Remove deleted records
        if not deletes.is_empty():
            target_df = target_df.join(
                deletes.select(primary_key),
                on=primary_key,
                how="anti",
            )

        # Remove records that will be updated
        if not upserts.is_empty():
            target_df = target_df.join(
                upserts.select(primary_key),
                on=primary_key,
                how="anti",
            )

            # Add updated/new records (without _change_type)
            upserts = upserts.drop("_change_type")
            target_df = pl.concat([target_df, upserts])

        return target_df
