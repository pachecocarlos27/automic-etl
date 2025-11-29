"""Iceberg table management."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import polars as pl
import pyarrow as pa
import structlog
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.sorting import SortOrder
from pyiceberg.transforms import (
    DayTransform,
    HourTransform,
    MonthTransform,
    YearTransform,
    IdentityTransform,
    BucketTransform,
    TruncateTransform,
)

from automic_etl.core.config import Settings
from automic_etl.core.exceptions import IcebergError
from automic_etl.storage.iceberg.catalog import IcebergCatalog
from automic_etl.storage.iceberg.schemas import schema_from_polars

logger = structlog.get_logger()


class IcebergTableManager:
    """Manager for Iceberg table operations."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.catalog = IcebergCatalog(settings)
        self.logger = logger.bind(component="iceberg_tables")

    # =========================================================================
    # Table Creation
    # =========================================================================

    def create_table(
        self,
        namespace: str,
        table_name: str,
        schema: Schema,
        partition_spec: PartitionSpec | None = None,
        sort_order: SortOrder | None = None,
        location: str | None = None,
        properties: dict[str, str] | None = None,
    ) -> Table:
        """
        Create a new Iceberg table.

        Args:
            namespace: Table namespace
            table_name: Table name
            schema: Iceberg schema
            partition_spec: Optional partition specification
            sort_order: Optional sort order
            location: Optional custom location
            properties: Optional table properties

        Returns:
            The created Table
        """
        identifier = f"{namespace}.{table_name}"

        # Ensure namespace exists
        self.catalog.ensure_namespace(namespace)

        # Set default properties
        default_properties = {
            "format-version": str(self.settings.iceberg.table_defaults.format_version),
            "write.parquet.compression-codec": self.settings.iceberg.table_defaults.compression,
        }

        if properties:
            default_properties.update(properties)

        try:
            table = self.catalog.catalog.create_table(
                identifier=identifier,
                schema=schema,
                partition_spec=partition_spec,
                sort_order=sort_order,
                location=location,
                properties=default_properties,
            )
            self.logger.info(
                "Created table",
                table=identifier,
                columns=len(schema.fields),
            )
            return table
        except Exception as e:
            raise IcebergError(
                f"Failed to create table: {str(e)}",
                table=identifier,
                operation="create_table",
            )

    def create_table_from_dataframe(
        self,
        namespace: str,
        table_name: str,
        df: pl.DataFrame,
        partition_columns: list[str] | None = None,
        sort_columns: list[str] | None = None,
        properties: dict[str, str] | None = None,
    ) -> Table:
        """
        Create a table from a Polars DataFrame schema.

        Args:
            namespace: Table namespace
            table_name: Table name
            df: Polars DataFrame (used for schema inference)
            partition_columns: Columns to partition by
            sort_columns: Columns to sort by
            properties: Optional table properties

        Returns:
            The created Table
        """
        schema = schema_from_polars(df)

        partition_spec = None
        if partition_columns:
            partition_spec = self._build_partition_spec(schema, partition_columns)

        sort_order = None
        if sort_columns:
            sort_order = self._build_sort_order(schema, sort_columns)

        return self.create_table(
            namespace=namespace,
            table_name=table_name,
            schema=schema,
            partition_spec=partition_spec,
            sort_order=sort_order,
            properties=properties,
        )

    def create_or_replace_table(
        self,
        namespace: str,
        table_name: str,
        schema: Schema,
        partition_spec: PartitionSpec | None = None,
        properties: dict[str, str] | None = None,
    ) -> Table:
        """Create a table, dropping existing if present."""
        if self.catalog.table_exists(namespace, table_name):
            self.catalog.drop_table(namespace, table_name, purge=True)

        return self.create_table(
            namespace=namespace,
            table_name=table_name,
            schema=schema,
            partition_spec=partition_spec,
            properties=properties,
        )

    # =========================================================================
    # Data Operations
    # =========================================================================

    def append(
        self,
        namespace: str,
        table_name: str,
        df: pl.DataFrame,
    ) -> int:
        """
        Append data to an existing table.

        Args:
            namespace: Table namespace
            table_name: Table name
            df: Data to append

        Returns:
            Number of rows appended
        """
        table = self.catalog.load_table(namespace, table_name)

        # Convert Polars to PyArrow
        arrow_table = df.to_arrow()

        try:
            table.append(arrow_table)
            self.logger.info(
                "Appended data",
                table=f"{namespace}.{table_name}",
                rows=len(df),
            )
            return len(df)
        except Exception as e:
            raise IcebergError(
                f"Failed to append data: {str(e)}",
                table=f"{namespace}.{table_name}",
                operation="append",
            )

    def overwrite(
        self,
        namespace: str,
        table_name: str,
        df: pl.DataFrame,
    ) -> int:
        """
        Overwrite table data.

        Args:
            namespace: Table namespace
            table_name: Table name
            df: Data to write

        Returns:
            Number of rows written
        """
        table = self.catalog.load_table(namespace, table_name)

        # Convert Polars to PyArrow
        arrow_table = df.to_arrow()

        try:
            table.overwrite(arrow_table)
            self.logger.info(
                "Overwrote table",
                table=f"{namespace}.{table_name}",
                rows=len(df),
            )
            return len(df)
        except Exception as e:
            raise IcebergError(
                f"Failed to overwrite data: {str(e)}",
                table=f"{namespace}.{table_name}",
                operation="overwrite",
            )

    def upsert(
        self,
        namespace: str,
        table_name: str,
        df: pl.DataFrame,
        key_columns: list[str],
    ) -> tuple[int, int]:
        """
        Upsert data (insert or update based on key columns).

        Args:
            namespace: Table namespace
            table_name: Table name
            df: Data to upsert
            key_columns: Columns to use as keys

        Returns:
            Tuple of (rows_inserted, rows_updated)
        """
        # Read existing data
        existing_df = self.read(namespace, table_name)

        if existing_df.is_empty():
            # No existing data, just append
            self.append(namespace, table_name, df)
            return len(df), 0

        # Identify new and existing rows
        new_rows = df.join(
            existing_df.select(key_columns),
            on=key_columns,
            how="anti",
        )

        updated_rows = df.join(
            existing_df.select(key_columns),
            on=key_columns,
            how="inner",
        )

        # Get rows from existing that aren't being updated
        unchanged_rows = existing_df.join(
            df.select(key_columns),
            on=key_columns,
            how="anti",
        )

        # Combine unchanged + updated + new
        result_df = pl.concat([unchanged_rows, updated_rows, new_rows])

        # Overwrite table
        self.overwrite(namespace, table_name, result_df)

        return len(new_rows), len(updated_rows)

    def delete(
        self,
        namespace: str,
        table_name: str,
        condition: str,
    ) -> int:
        """
        Delete rows matching a condition.

        Args:
            namespace: Table namespace
            table_name: Table name
            condition: Filter expression

        Returns:
            Number of rows deleted
        """
        table = self.catalog.load_table(namespace, table_name)

        try:
            # Read current data
            df = self.read(namespace, table_name)
            original_count = len(df)

            # Filter out rows to delete
            remaining_df = df.filter(~pl.sql_expr(condition))

            # Overwrite with remaining data
            self.overwrite(namespace, table_name, remaining_df)

            deleted_count = original_count - len(remaining_df)
            self.logger.info(
                "Deleted rows",
                table=f"{namespace}.{table_name}",
                deleted=deleted_count,
            )
            return deleted_count
        except Exception as e:
            raise IcebergError(
                f"Failed to delete data: {str(e)}",
                table=f"{namespace}.{table_name}",
                operation="delete",
            )

    # =========================================================================
    # Read Operations
    # =========================================================================

    def read(
        self,
        namespace: str,
        table_name: str,
        columns: list[str] | None = None,
        filter_expr: str | None = None,
        limit: int | None = None,
    ) -> pl.DataFrame:
        """
        Read data from a table.

        Args:
            namespace: Table namespace
            table_name: Table name
            columns: Columns to select
            filter_expr: Filter expression
            limit: Maximum rows to return

        Returns:
            Polars DataFrame
        """
        table = self.catalog.load_table(namespace, table_name)

        try:
            scan = table.scan(
                selected_fields=tuple(columns) if columns else (),
                limit=limit,
            )

            arrow_table = scan.to_arrow()
            df = pl.from_arrow(arrow_table)

            if filter_expr:
                df = df.filter(pl.sql_expr(filter_expr))

            return df
        except Exception as e:
            raise IcebergError(
                f"Failed to read table: {str(e)}",
                table=f"{namespace}.{table_name}",
                operation="read",
            )

    def read_at_snapshot(
        self,
        namespace: str,
        table_name: str,
        snapshot_id: int,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """Read data at a specific snapshot (time travel)."""
        table = self.catalog.load_table(namespace, table_name)

        try:
            scan = table.scan(
                selected_fields=tuple(columns) if columns else (),
                snapshot_id=snapshot_id,
            )
            arrow_table = scan.to_arrow()
            return pl.from_arrow(arrow_table)
        except Exception as e:
            raise IcebergError(
                f"Failed to read at snapshot: {str(e)}",
                table=f"{namespace}.{table_name}",
                operation="read_at_snapshot",
            )

    def read_at_timestamp(
        self,
        namespace: str,
        table_name: str,
        timestamp: datetime,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """Read data at a specific timestamp (time travel)."""
        table = self.catalog.load_table(namespace, table_name)

        # Find snapshot at timestamp
        history = list(table.history())
        target_snapshot = None

        for entry in history:
            if entry.timestamp_ms <= int(timestamp.timestamp() * 1000):
                target_snapshot = entry.snapshot_id

        if target_snapshot is None:
            raise IcebergError(
                f"No snapshot found at or before {timestamp}",
                table=f"{namespace}.{table_name}",
                operation="read_at_timestamp",
            )

        return self.read_at_snapshot(namespace, table_name, target_snapshot, columns)

    # =========================================================================
    # Schema Evolution
    # =========================================================================

    def add_column(
        self,
        namespace: str,
        table_name: str,
        column_name: str,
        column_type: Any,
        required: bool = False,
        doc: str | None = None,
    ) -> None:
        """Add a new column to a table."""
        table = self.catalog.load_table(namespace, table_name)

        try:
            with table.update_schema() as update:
                update.add_column(column_name, column_type, required=required, doc=doc)

            self.logger.info(
                "Added column",
                table=f"{namespace}.{table_name}",
                column=column_name,
            )
        except Exception as e:
            raise IcebergError(
                f"Failed to add column: {str(e)}",
                table=f"{namespace}.{table_name}",
                operation="add_column",
            )

    def drop_column(
        self,
        namespace: str,
        table_name: str,
        column_name: str,
    ) -> None:
        """Drop a column from a table."""
        table = self.catalog.load_table(namespace, table_name)

        try:
            with table.update_schema() as update:
                update.delete_column(column_name)

            self.logger.info(
                "Dropped column",
                table=f"{namespace}.{table_name}",
                column=column_name,
            )
        except Exception as e:
            raise IcebergError(
                f"Failed to drop column: {str(e)}",
                table=f"{namespace}.{table_name}",
                operation="drop_column",
            )

    def rename_column(
        self,
        namespace: str,
        table_name: str,
        old_name: str,
        new_name: str,
    ) -> None:
        """Rename a column."""
        table = self.catalog.load_table(namespace, table_name)

        try:
            with table.update_schema() as update:
                update.rename_column(old_name, new_name)

            self.logger.info(
                "Renamed column",
                table=f"{namespace}.{table_name}",
                old_name=old_name,
                new_name=new_name,
            )
        except Exception as e:
            raise IcebergError(
                f"Failed to rename column: {str(e)}",
                table=f"{namespace}.{table_name}",
                operation="rename_column",
            )

    # =========================================================================
    # Maintenance Operations
    # =========================================================================

    def compact(self, namespace: str, table_name: str) -> None:
        """Compact small files in a table."""
        table = self.catalog.load_table(namespace, table_name)

        try:
            # Rewrite data files
            table.refresh()
            self.logger.info(
                "Compacted table",
                table=f"{namespace}.{table_name}",
            )
        except Exception as e:
            raise IcebergError(
                f"Failed to compact table: {str(e)}",
                table=f"{namespace}.{table_name}",
                operation="compact",
            )

    def expire_snapshots(
        self,
        namespace: str,
        table_name: str,
        older_than: datetime,
        retain_last: int = 1,
    ) -> int:
        """Expire old snapshots."""
        table = self.catalog.load_table(namespace, table_name)

        try:
            timestamp_ms = int(older_than.timestamp() * 1000)
            expired = table.expire_snapshots().expire_older_than(
                timestamp_ms
            ).retain_last(retain_last).commit()

            self.logger.info(
                "Expired snapshots",
                table=f"{namespace}.{table_name}",
            )
            return 0  # PyIceberg doesn't return count
        except Exception as e:
            raise IcebergError(
                f"Failed to expire snapshots: {str(e)}",
                table=f"{namespace}.{table_name}",
                operation="expire_snapshots",
            )

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _build_partition_spec(
        self,
        schema: Schema,
        partition_columns: list[str],
    ) -> PartitionSpec:
        """Build a partition spec from column names."""
        fields = []

        for i, col_name in enumerate(partition_columns):
            # Find the field in schema
            field = schema.find_field(col_name)
            if field is None:
                raise IcebergError(
                    f"Partition column not found in schema: {col_name}",
                    operation="build_partition_spec",
                )

            # Determine transform based on column name pattern
            transform = IdentityTransform()

            if "_date" in col_name.lower() or col_name.lower().endswith("_dt"):
                transform = DayTransform()
            elif "_month" in col_name.lower():
                transform = MonthTransform()
            elif "_year" in col_name.lower():
                transform = YearTransform()
            elif "_hour" in col_name.lower():
                transform = HourTransform()

            fields.append(
                PartitionField(
                    source_id=field.field_id,
                    field_id=1000 + i,
                    transform=transform,
                    name=f"{col_name}_partition",
                )
            )

        return PartitionSpec(*fields)

    def _build_sort_order(
        self,
        schema: Schema,
        sort_columns: list[str],
    ) -> SortOrder:
        """Build a sort order from column names."""
        from pyiceberg.table.sorting import SortField, NullOrder, SortDirection

        fields = []
        for col_name in sort_columns:
            field = schema.find_field(col_name)
            if field is None:
                raise IcebergError(
                    f"Sort column not found in schema: {col_name}",
                    operation="build_sort_order",
                )

            fields.append(
                SortField(
                    source_id=field.field_id,
                    transform=IdentityTransform(),
                    direction=SortDirection.ASC,
                    null_order=NullOrder.NULLS_LAST,
                )
            )

        return SortOrder(*fields)
