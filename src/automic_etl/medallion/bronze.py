"""Bronze layer implementation - Raw data ingestion."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import polars as pl
import structlog

from automic_etl.core.config import Settings
from automic_etl.core.exceptions import LoadError
from automic_etl.storage.iceberg import IcebergTableManager, schema_from_polars

logger = structlog.get_logger()


class BronzeLayer:
    """
    Bronze Layer - Raw Data Ingestion.

    The bronze layer stores raw data exactly as received from sources.
    It preserves the original format and adds metadata for tracking.

    Features:
    - Preserves original data without transformation
    - Adds ingestion metadata (_ingestion_time, _source, _batch_id)
    - Supports structured, semi-structured, and unstructured data
    - Partitioned by ingestion date for efficient querying
    """

    NAMESPACE = "bronze"

    # Metadata columns added to all bronze tables
    METADATA_COLUMNS = [
        "_ingestion_time",
        "_source",
        "_source_file",
        "_batch_id",
        "_ingestion_date",
    ]

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.table_manager = IcebergTableManager(settings)
        self.logger = logger.bind(layer="bronze")

    def ingest(
        self,
        table_name: str,
        df: pl.DataFrame,
        source: str,
        source_file: str | None = None,
        batch_id: str | None = None,
        additional_metadata: dict[str, Any] | None = None,
    ) -> int:
        """
        Ingest raw data into the bronze layer.

        Args:
            table_name: Target table name
            df: Data to ingest
            source: Data source identifier
            source_file: Optional source file name
            batch_id: Optional batch identifier
            additional_metadata: Optional additional metadata

        Returns:
            Number of rows ingested
        """
        if df.is_empty():
            self.logger.warning("Empty dataframe, skipping ingestion", table=table_name)
            return 0

        # Add metadata columns
        ingestion_time = datetime.utcnow()
        df = self._add_metadata(
            df=df,
            source=source,
            source_file=source_file,
            batch_id=batch_id,
            ingestion_time=ingestion_time,
        )

        # Add any additional metadata
        if additional_metadata:
            for key, value in additional_metadata.items():
                df = df.with_columns(pl.lit(value).alias(f"_meta_{key}"))

        try:
            # Check if table exists
            if self.table_manager.catalog.table_exists(self.NAMESPACE, table_name):
                # Append to existing table
                rows = self.table_manager.append(self.NAMESPACE, table_name, df)
            else:
                # Create new table with partitioning
                partition_columns = self.settings.medallion.bronze.partition_by
                self.table_manager.create_table_from_dataframe(
                    namespace=self.NAMESPACE,
                    table_name=table_name,
                    df=df,
                    partition_columns=partition_columns,
                    properties={
                        "automic.layer": "bronze",
                        "automic.source": source,
                    },
                )
                rows = self.table_manager.append(self.NAMESPACE, table_name, df)

            self.logger.info(
                "Ingested data to bronze",
                table=table_name,
                rows=rows,
                source=source,
            )
            return rows

        except Exception as e:
            raise LoadError(
                f"Failed to ingest to bronze layer: {str(e)}",
                target=f"{self.NAMESPACE}.{table_name}",
                details={"source": source},
            )

    def ingest_unstructured(
        self,
        table_name: str,
        content: bytes | str,
        source: str,
        content_type: str,
        source_file: str | None = None,
        metadata: dict[str, Any] | None = None,
        batch_id: str | None = None,
    ) -> int:
        """
        Ingest unstructured data (documents, images, etc.).

        Stores the raw content along with metadata for later processing.

        Args:
            table_name: Target table name
            content: Raw content (bytes or text)
            source: Data source identifier
            content_type: MIME type of the content
            source_file: Original file name
            metadata: Additional metadata
            batch_id: Optional batch identifier

        Returns:
            Number of rows ingested (always 1 for unstructured)
        """
        # Convert content to appropriate format
        if isinstance(content, str):
            content_bytes = content.encode("utf-8")
            text_content = content
        else:
            content_bytes = content
            try:
                text_content = content.decode("utf-8")
            except UnicodeDecodeError:
                text_content = None

        # Create dataframe for unstructured content
        data = {
            "_content_bytes": [content_bytes],
            "_content_text": [text_content],
            "_content_type": [content_type],
            "_content_size": [len(content_bytes)],
        }

        # Add extracted metadata
        if metadata:
            for key, value in metadata.items():
                data[f"_extracted_{key}"] = [value]

        df = pl.DataFrame(data)

        return self.ingest(
            table_name=table_name,
            df=df,
            source=source,
            source_file=source_file,
            batch_id=batch_id,
        )

    def ingest_semi_structured(
        self,
        table_name: str,
        data: dict[str, Any] | list[dict[str, Any]],
        source: str,
        source_file: str | None = None,
        batch_id: str | None = None,
    ) -> int:
        """
        Ingest semi-structured data (JSON, XML converted to dict).

        Stores the data with schema inference and raw JSON preservation.

        Args:
            table_name: Target table name
            data: Dictionary or list of dictionaries
            source: Data source identifier
            source_file: Original file name
            batch_id: Optional batch identifier

        Returns:
            Number of rows ingested
        """
        import json

        # Ensure list format
        if isinstance(data, dict):
            data = [data]

        # Create dataframe from data
        df = pl.DataFrame(data)

        # Add raw JSON column for preservation
        raw_json = [json.dumps(item) for item in data]
        df = df.with_columns(pl.Series("_raw_json", raw_json))

        return self.ingest(
            table_name=table_name,
            df=df,
            source=source,
            source_file=source_file,
            batch_id=batch_id,
        )

    def read(
        self,
        table_name: str,
        columns: list[str] | None = None,
        filter_expr: str | None = None,
        limit: int | None = None,
    ) -> pl.DataFrame:
        """
        Read data from the bronze layer.

        Args:
            table_name: Table to read
            columns: Columns to select
            filter_expr: Filter expression
            limit: Maximum rows

        Returns:
            Polars DataFrame
        """
        return self.table_manager.read(
            namespace=self.NAMESPACE,
            table_name=table_name,
            columns=columns,
            filter_expr=filter_expr,
            limit=limit,
        )

    def read_new_since(
        self,
        table_name: str,
        since: datetime,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """
        Read data ingested since a specific time.

        Useful for incremental processing to silver layer.
        """
        df = self.read(table_name, columns)
        return df.filter(pl.col("_ingestion_time") > since)

    def get_latest_ingestion_time(self, table_name: str) -> datetime | None:
        """Get the latest ingestion time for a table."""
        try:
            df = self.read(table_name, columns=["_ingestion_time"], limit=1)
            if df.is_empty():
                return None

            # Get max ingestion time
            max_time = (
                self.read(table_name, columns=["_ingestion_time"])
                .select(pl.col("_ingestion_time").max())
                .item()
            )
            return max_time
        except Exception:
            return None

    def list_tables(self) -> list[str]:
        """List all tables in the bronze layer."""
        return self.table_manager.catalog.list_tables(self.NAMESPACE)

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        return self.table_manager.catalog.table_exists(self.NAMESPACE, table_name)

    def _add_metadata(
        self,
        df: pl.DataFrame,
        source: str,
        source_file: str | None,
        batch_id: str | None,
        ingestion_time: datetime,
    ) -> pl.DataFrame:
        """Add metadata columns to the dataframe."""
        return df.with_columns([
            pl.lit(ingestion_time).alias("_ingestion_time"),
            pl.lit(source).alias("_source"),
            pl.lit(source_file).alias("_source_file"),
            pl.lit(batch_id).alias("_batch_id"),
            pl.lit(ingestion_time.date()).alias("_ingestion_date"),
        ])
