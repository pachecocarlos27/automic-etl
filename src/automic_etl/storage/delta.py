"""Delta Lake integration for Automic ETL."""

from __future__ import annotations

from typing import Any, Iterator
from datetime import datetime
from pathlib import Path

import polars as pl
import structlog

from automic_etl.core.config import Settings

logger = structlog.get_logger()


class DeltaTableManager:
    """
    Delta Lake table manager supporting ACID transactions.

    Features:
    - Create and manage Delta tables
    - Time travel queries
    - MERGE (upsert) operations
    - Schema evolution
    - Table optimization (VACUUM, OPTIMIZE)
    - Change Data Feed for CDC
    """

    def __init__(self, settings: Settings) -> None:
        """Initialize Delta Lake manager."""
        self.settings = settings
        self.logger = logger.bind(component="delta_manager")

    def create_table(
        self,
        path: str,
        df: pl.DataFrame,
        partition_by: list[str] | None = None,
        mode: str = "error",
        name: str | None = None,
        description: str | None = None,
    ) -> None:
        """
        Create a new Delta table.

        Args:
            path: Table storage path
            df: Initial data
            partition_by: Partition columns
            mode: 'error', 'overwrite', 'append', 'ignore'
            name: Table name for metadata
            description: Table description
        """
        from deltalake import write_deltalake

        write_deltalake(
            path,
            df.to_arrow(),
            partition_by=partition_by,
            mode=mode,
            name=name,
            description=description,
        )

        self.logger.info(
            "Delta table created",
            path=path,
            rows=len(df),
            partitions=partition_by,
        )

    def read(
        self,
        path: str,
        columns: list[str] | None = None,
        filter_expr: str | None = None,
        version: int | None = None,
        timestamp: datetime | str | None = None,
    ) -> pl.DataFrame:
        """
        Read from a Delta table.

        Args:
            path: Table path
            columns: Columns to read
            filter_expr: Filter expression (Polars syntax)
            version: Specific version to read
            timestamp: Read as of timestamp

        Returns:
            Polars DataFrame
        """
        from deltalake import DeltaTable

        if version is not None:
            dt = DeltaTable(path, version=version)
        elif timestamp is not None:
            if isinstance(timestamp, datetime):
                timestamp = timestamp.isoformat()
            dt = DeltaTable(path)
            dt.load_as_version(timestamp)
        else:
            dt = DeltaTable(path)

        # Read to Arrow then convert
        arrow_table = dt.to_pyarrow_table(columns=columns)
        df = pl.from_arrow(arrow_table)

        if filter_expr:
            df = df.filter(pl.sql_expr(filter_expr))

        return df

    def append(self, path: str, df: pl.DataFrame) -> None:
        """Append data to existing Delta table."""
        from deltalake import write_deltalake

        write_deltalake(path, df.to_arrow(), mode="append")
        self.logger.info("Data appended", path=path, rows=len(df))

    def overwrite(
        self,
        path: str,
        df: pl.DataFrame,
        partition_filters: list[tuple] | None = None,
    ) -> None:
        """
        Overwrite Delta table or partitions.

        Args:
            path: Table path
            df: Data to write
            partition_filters: Partition predicates for selective overwrite
        """
        from deltalake import write_deltalake

        write_deltalake(
            path,
            df.to_arrow(),
            mode="overwrite",
            partition_filters=partition_filters,
        )
        self.logger.info("Data overwritten", path=path, rows=len(df))

    def merge(
        self,
        path: str,
        source_df: pl.DataFrame,
        predicate: str,
        source_alias: str = "s",
        target_alias: str = "t",
        when_matched_update: dict[str, str] | None = None,
        when_matched_delete: str | None = None,
        when_not_matched_insert: dict[str, str] | None = None,
        when_not_matched_by_source_delete: str | None = None,
    ) -> dict[str, int]:
        """
        Perform MERGE (upsert) operation.

        Args:
            path: Table path
            source_df: Source data for merge
            predicate: Join predicate (e.g., 's.id = t.id')
            source_alias: Alias for source data
            target_alias: Alias for target table
            when_matched_update: Update expressions when matched
            when_matched_delete: Delete condition when matched
            when_not_matched_insert: Insert expressions for new records
            when_not_matched_by_source_delete: Delete condition for orphaned target records

        Returns:
            Merge statistics
        """
        from deltalake import DeltaTable

        dt = DeltaTable(path)

        merge_builder = (
            dt.merge(
                source=source_df.to_arrow(),
                predicate=predicate,
                source_alias=source_alias,
                target_alias=target_alias,
            )
        )

        if when_matched_update:
            if when_matched_delete:
                merge_builder = merge_builder.when_matched_update(
                    updates=when_matched_update,
                    predicate=f"NOT ({when_matched_delete})",
                ).when_matched_delete(predicate=when_matched_delete)
            else:
                merge_builder = merge_builder.when_matched_update_all()

        if when_not_matched_insert:
            merge_builder = merge_builder.when_not_matched_insert_all()

        if when_not_matched_by_source_delete:
            merge_builder = merge_builder.when_not_matched_by_source_delete(
                predicate=when_not_matched_by_source_delete
            )

        result = merge_builder.execute()

        stats = {
            "rows_inserted": result.get("num_target_rows_inserted", 0),
            "rows_updated": result.get("num_target_rows_updated", 0),
            "rows_deleted": result.get("num_target_rows_deleted", 0),
        }

        self.logger.info("Merge completed", path=path, **stats)
        return stats

    def upsert(
        self,
        path: str,
        df: pl.DataFrame,
        merge_keys: list[str],
    ) -> dict[str, int]:
        """
        Simple upsert based on merge keys.

        Args:
            path: Table path
            df: Data to upsert
            merge_keys: Columns for matching

        Returns:
            Merge statistics
        """
        predicate = " AND ".join([f"s.{k} = t.{k}" for k in merge_keys])

        return self.merge(
            path=path,
            source_df=df,
            predicate=predicate,
            when_matched_update={},  # Update all columns
            when_not_matched_insert={},  # Insert all columns
        )

    def delete(
        self,
        path: str,
        predicate: str,
    ) -> int:
        """
        Delete rows matching predicate.

        Args:
            path: Table path
            predicate: Delete condition

        Returns:
            Number of rows deleted
        """
        from deltalake import DeltaTable

        dt = DeltaTable(path)
        result = dt.delete(predicate)
        rows_deleted = result.get("num_deleted_rows", 0)

        self.logger.info("Rows deleted", path=path, rows=rows_deleted)
        return rows_deleted

    def update(
        self,
        path: str,
        updates: dict[str, str],
        predicate: str | None = None,
    ) -> int:
        """
        Update rows in table.

        Args:
            path: Table path
            updates: Column -> expression mapping
            predicate: Optional condition

        Returns:
            Number of rows updated
        """
        from deltalake import DeltaTable

        dt = DeltaTable(path)
        result = dt.update(updates, predicate)
        rows_updated = result.get("num_updated_rows", 0)

        self.logger.info("Rows updated", path=path, rows=rows_updated)
        return rows_updated

    def get_history(self, path: str, limit: int | None = None) -> pl.DataFrame:
        """
        Get table version history.

        Args:
            path: Table path
            limit: Maximum history entries

        Returns:
            DataFrame with version history
        """
        from deltalake import DeltaTable

        dt = DeltaTable(path)
        history = dt.history(limit)

        return pl.DataFrame(history)

    def get_version(self, path: str) -> int:
        """Get current table version."""
        from deltalake import DeltaTable
        return DeltaTable(path).version()

    def restore(self, path: str, version: int | None = None, timestamp: str | None = None) -> None:
        """
        Restore table to a previous version.

        Args:
            path: Table path
            version: Version number to restore
            timestamp: Timestamp to restore to
        """
        from deltalake import DeltaTable

        dt = DeltaTable(path)

        if version is not None:
            dt.restore(version)
        elif timestamp:
            dt.restore(timestamp)

        self.logger.info("Table restored", path=path, version=version, timestamp=timestamp)

    def vacuum(
        self,
        path: str,
        retention_hours: int = 168,  # 7 days
        dry_run: bool = False,
    ) -> list[str]:
        """
        Remove old files not in current table state.

        Args:
            path: Table path
            retention_hours: File retention period
            dry_run: If True, only list files to delete

        Returns:
            List of deleted files
        """
        from deltalake import DeltaTable

        dt = DeltaTable(path)
        deleted = dt.vacuum(
            retention_hours=retention_hours,
            dry_run=dry_run,
            enforce_retention_duration=False,
        )

        self.logger.info(
            "Vacuum completed",
            path=path,
            files_deleted=len(deleted),
            dry_run=dry_run,
        )
        return deleted

    def optimize(
        self,
        path: str,
        partition_filters: list[tuple] | None = None,
        target_size: int | None = None,
    ) -> dict[str, Any]:
        """
        Optimize table by compacting small files.

        Args:
            path: Table path
            partition_filters: Only optimize matching partitions
            target_size: Target file size in bytes

        Returns:
            Optimization statistics
        """
        from deltalake import DeltaTable

        dt = DeltaTable(path)
        result = dt.optimize.compact(
            partition_filters=partition_filters,
            target_size=target_size,
        )

        stats = {
            "files_added": result.get("numFilesAdded", 0),
            "files_removed": result.get("numFilesRemoved", 0),
            "bytes_added": result.get("numBytesAdded", 0),
            "bytes_removed": result.get("numBytesRemoved", 0),
        }

        self.logger.info("Optimization completed", path=path, **stats)
        return stats

    def z_order(
        self,
        path: str,
        columns: list[str],
        partition_filters: list[tuple] | None = None,
    ) -> dict[str, Any]:
        """
        Apply Z-ordering for improved query performance.

        Args:
            path: Table path
            columns: Columns to Z-order by
            partition_filters: Only optimize matching partitions

        Returns:
            Optimization statistics
        """
        from deltalake import DeltaTable

        dt = DeltaTable(path)
        result = dt.optimize.z_order(
            columns=columns,
            partition_filters=partition_filters,
        )

        self.logger.info("Z-ordering completed", path=path, columns=columns)
        return result

    def get_schema(self, path: str) -> dict[str, Any]:
        """Get table schema."""
        from deltalake import DeltaTable

        dt = DeltaTable(path)
        schema = dt.schema()

        return {
            "fields": [
                {
                    "name": f.name,
                    "type": str(f.type),
                    "nullable": f.nullable,
                    "metadata": f.metadata,
                }
                for f in schema.fields
            ]
        }

    def add_columns(
        self,
        path: str,
        columns: list[dict[str, Any]],
    ) -> None:
        """
        Add new columns to table schema.

        Args:
            path: Table path
            columns: List of column definitions
        """
        from deltalake import DeltaTable
        import pyarrow as pa

        dt = DeltaTable(path)

        new_fields = []
        for col in columns:
            field = pa.field(
                col["name"],
                getattr(pa, col["type"])(),
                nullable=col.get("nullable", True),
            )
            new_fields.append(field)

        dt.alter.add_columns(new_fields)
        self.logger.info("Columns added", path=path, columns=[c["name"] for c in columns])

    def get_metadata(self, path: str) -> dict[str, Any]:
        """Get table metadata."""
        from deltalake import DeltaTable

        dt = DeltaTable(path)
        metadata = dt.metadata()

        return {
            "id": metadata.id,
            "name": metadata.name,
            "description": metadata.description,
            "partition_columns": metadata.partition_columns,
            "configuration": metadata.configuration,
            "created_time": metadata.created_time,
        }

    def get_stats(self, path: str) -> dict[str, Any]:
        """Get table statistics."""
        from deltalake import DeltaTable

        dt = DeltaTable(path)
        files = dt.files()

        return {
            "version": dt.version(),
            "num_files": len(files),
            "total_size_bytes": sum(f.size for f in dt.get_add_actions()),
        }

    def enable_change_data_feed(self, path: str) -> None:
        """Enable Change Data Feed for CDC."""
        from deltalake import DeltaTable

        dt = DeltaTable(path)
        dt.alter.set_table_properties({"delta.enableChangeDataFeed": "true"})

        self.logger.info("Change Data Feed enabled", path=path)

    def get_changes(
        self,
        path: str,
        starting_version: int,
        ending_version: int | None = None,
    ) -> pl.DataFrame:
        """
        Get changes between versions (requires Change Data Feed).

        Args:
            path: Table path
            starting_version: Start version (exclusive)
            ending_version: End version (inclusive, None for latest)

        Returns:
            DataFrame with change data
        """
        from deltalake import DeltaTable

        dt = DeltaTable(path)

        cdf = dt.load_cdf(
            starting_version=starting_version,
            ending_version=ending_version,
        )

        return pl.from_arrow(cdf.to_pyarrow_table())
