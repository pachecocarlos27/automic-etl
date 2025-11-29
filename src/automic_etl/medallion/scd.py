"""Slowly Changing Dimension (SCD) Type 2 implementation."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import polars as pl
import structlog

from automic_etl.core.config import Settings
from automic_etl.core.exceptions import TransformationError
from automic_etl.storage.iceberg import IcebergTableManager

logger = structlog.get_logger()


class SCDType2Manager:
    """
    Manage Slowly Changing Dimension Type 2 tables.

    SCD Type 2 tracks historical changes by:
    - Adding new rows for changes (preserving history)
    - Using effective_from/effective_to date ranges
    - Maintaining is_current flag for easy querying

    Standard SCD2 columns added:
    - _scd_effective_from: When this version became effective
    - _scd_effective_to: When this version expired (null if current)
    - _scd_is_current: Boolean flag for current version
    - _scd_version: Version number for the record
    - _scd_hash: Hash of tracked columns for change detection
    """

    # Default SCD2 metadata columns
    SCD_COLUMNS = [
        "_scd_effective_from",
        "_scd_effective_to",
        "_scd_is_current",
        "_scd_version",
        "_scd_hash",
    ]

    # Default end date for current records
    END_OF_TIME = datetime(9999, 12, 31, 23, 59, 59)

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.table_manager = IcebergTableManager(settings)
        self.logger = logger.bind(component="scd2_manager")

    def apply_scd2(
        self,
        source_df: pl.DataFrame,
        table_name: str,
        business_keys: list[str],
        tracked_columns: list[str] | None = None,
        namespace: str = "silver",
        effective_date: datetime | None = None,
    ) -> dict[str, int]:
        """
        Apply SCD Type 2 logic to a table.

        Args:
            source_df: New/updated records
            table_name: Target SCD2 table name
            business_keys: Columns that identify a unique business entity
            tracked_columns: Columns to track for changes (all non-key if None)
            namespace: Target namespace (default: silver)
            effective_date: Effective date for changes (default: now)

        Returns:
            Dict with counts: inserted, updated, unchanged
        """
        effective_date = effective_date or datetime.utcnow()

        # Determine tracked columns (exclude business keys and SCD columns)
        if tracked_columns is None:
            tracked_columns = [
                col for col in source_df.columns
                if col not in business_keys and col not in self.SCD_COLUMNS
            ]

        self.logger.info(
            "Applying SCD2",
            table=table_name,
            business_keys=business_keys,
            tracked_columns=tracked_columns[:5],
        )

        # Add hash column for change detection
        source_df = self._add_hash_column(source_df, tracked_columns)

        # Check if table exists
        if not self.table_manager.catalog.table_exists(namespace, table_name):
            # Initial load - all records are new
            return self._initial_load(
                source_df=source_df,
                table_name=table_name,
                namespace=namespace,
                business_keys=business_keys,
                effective_date=effective_date,
            )

        # Load existing current records
        existing_df = self.table_manager.read(
            namespace=namespace,
            table_name=table_name,
            filter_expr="_scd_is_current = true",
        )

        # Identify changes
        changes = self._identify_changes(
            source_df=source_df,
            existing_df=existing_df,
            business_keys=business_keys,
        )

        # Apply changes
        return self._apply_changes(
            changes=changes,
            existing_df=existing_df,
            table_name=table_name,
            namespace=namespace,
            business_keys=business_keys,
            effective_date=effective_date,
        )

    def _add_hash_column(
        self,
        df: pl.DataFrame,
        tracked_columns: list[str],
    ) -> pl.DataFrame:
        """Add a hash column for change detection."""
        # Concatenate tracked columns and hash
        hash_expr = pl.concat_str(
            [pl.col(c).cast(pl.String).fill_null("") for c in tracked_columns],
            separator="|",
        ).hash()

        return df.with_columns(hash_expr.alias("_scd_hash"))

    def _initial_load(
        self,
        source_df: pl.DataFrame,
        table_name: str,
        namespace: str,
        business_keys: list[str],
        effective_date: datetime,
    ) -> dict[str, int]:
        """Handle initial load (table doesn't exist)."""
        # Add SCD2 columns
        df = source_df.with_columns([
            pl.lit(effective_date).alias("_scd_effective_from"),
            pl.lit(None).cast(pl.Datetime).alias("_scd_effective_to"),
            pl.lit(True).alias("_scd_is_current"),
            pl.lit(1).alias("_scd_version"),
        ])

        # Create table and insert
        self.table_manager.create_table_from_dataframe(
            namespace=namespace,
            table_name=table_name,
            df=df,
            partition_columns=["_scd_is_current"],
            properties={"automic.scd_type": "2"},
        )
        self.table_manager.append(namespace, table_name, df)

        self.logger.info(
            "SCD2 initial load completed",
            table=table_name,
            rows=len(df),
        )

        return {"inserted": len(df), "updated": 0, "unchanged": 0}

    def _identify_changes(
        self,
        source_df: pl.DataFrame,
        existing_df: pl.DataFrame,
        business_keys: list[str],
    ) -> dict[str, pl.DataFrame]:
        """
        Identify inserts, updates, and unchanged records.

        Returns:
            Dict with 'inserts', 'updates', 'unchanged' DataFrames
        """
        # New records (in source but not in existing)
        inserts = source_df.join(
            existing_df.select(business_keys),
            on=business_keys,
            how="anti",
        )

        # Existing records that appear in source
        matched = source_df.join(
            existing_df.select(business_keys + ["_scd_hash"]).rename({"_scd_hash": "_existing_hash"}),
            on=business_keys,
            how="inner",
        )

        # Changed records (hash different)
        updates = matched.filter(pl.col("_scd_hash") != pl.col("_existing_hash"))
        updates = updates.drop("_existing_hash")

        # Unchanged records (hash same)
        unchanged = matched.filter(pl.col("_scd_hash") == pl.col("_existing_hash"))
        unchanged = unchanged.drop("_existing_hash")

        return {
            "inserts": inserts,
            "updates": updates,
            "unchanged": unchanged,
        }

    def _apply_changes(
        self,
        changes: dict[str, pl.DataFrame],
        existing_df: pl.DataFrame,
        table_name: str,
        namespace: str,
        business_keys: list[str],
        effective_date: datetime,
    ) -> dict[str, int]:
        """Apply identified changes to the SCD2 table."""
        inserts = changes["inserts"]
        updates = changes["updates"]
        unchanged = changes["unchanged"]

        counts = {
            "inserted": len(inserts),
            "updated": len(updates),
            "unchanged": len(unchanged),
        }

        if inserts.is_empty() and updates.is_empty():
            self.logger.info("No changes detected", table=table_name)
            return counts

        # Prepare new rows to insert
        rows_to_insert = []

        # 1. New inserts (brand new business entities)
        if not inserts.is_empty():
            new_inserts = inserts.with_columns([
                pl.lit(effective_date).alias("_scd_effective_from"),
                pl.lit(None).cast(pl.Datetime).alias("_scd_effective_to"),
                pl.lit(True).alias("_scd_is_current"),
                pl.lit(1).alias("_scd_version"),
            ])
            rows_to_insert.append(new_inserts)

        # 2. Updates - need to close old records and insert new versions
        if not updates.is_empty():
            # Get current versions for updated records
            update_keys = updates.select(business_keys)

            # Find existing versions to determine new version number
            old_versions = existing_df.join(
                update_keys,
                on=business_keys,
                how="inner",
            )

            # Create version map
            version_map = old_versions.select(
                business_keys + ["_scd_version"]
            ).rename({"_scd_version": "_old_version"})

            # Add new version info to updates
            new_updates = updates.join(
                version_map,
                on=business_keys,
                how="left",
            ).with_columns([
                pl.lit(effective_date).alias("_scd_effective_from"),
                pl.lit(None).cast(pl.Datetime).alias("_scd_effective_to"),
                pl.lit(True).alias("_scd_is_current"),
                (pl.col("_old_version").fill_null(0) + 1).alias("_scd_version"),
            ]).drop("_old_version")

            rows_to_insert.append(new_updates)

            # Close old records (will be handled via merge)
            closed_records = old_versions.with_columns([
                pl.lit(effective_date).alias("_scd_effective_to"),
                pl.lit(False).alias("_scd_is_current"),
            ])

            # Get records that aren't being updated
            unchanged_existing = existing_df.join(
                update_keys,
                on=business_keys,
                how="anti",
            )

            # Rebuild table with: unchanged + closed + new
            all_records = pl.concat([
                unchanged_existing,
                closed_records,
            ] + rows_to_insert)

            # Overwrite table
            self.table_manager.overwrite(namespace, table_name, all_records)

        elif not inserts.is_empty():
            # Only inserts, just append
            self.table_manager.append(namespace, table_name, rows_to_insert[0])

        self.logger.info(
            "SCD2 changes applied",
            table=table_name,
            **counts,
        )

        return counts

    def get_current_records(
        self,
        table_name: str,
        namespace: str = "silver",
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """Get only current records from an SCD2 table."""
        return self.table_manager.read(
            namespace=namespace,
            table_name=table_name,
            columns=columns,
            filter_expr="_scd_is_current = true",
        )

    def get_record_at_time(
        self,
        table_name: str,
        business_key_values: dict[str, Any],
        as_of: datetime,
        namespace: str = "silver",
    ) -> pl.DataFrame | None:
        """
        Get the record version that was effective at a specific time.

        Args:
            table_name: SCD2 table name
            business_key_values: Dict of business key column -> value
            as_of: Point in time to query
            namespace: Table namespace

        Returns:
            DataFrame with the record version, or None if not found
        """
        df = self.table_manager.read(namespace, table_name)

        # Filter by business keys
        for col, value in business_key_values.items():
            df = df.filter(pl.col(col) == value)

        # Filter by time range
        df = df.filter(
            (pl.col("_scd_effective_from") <= as_of) &
            (
                pl.col("_scd_effective_to").is_null() |
                (pl.col("_scd_effective_to") > as_of)
            )
        )

        return df if not df.is_empty() else None

    def get_history(
        self,
        table_name: str,
        business_key_values: dict[str, Any],
        namespace: str = "silver",
    ) -> pl.DataFrame:
        """
        Get the complete history of a record.

        Args:
            table_name: SCD2 table name
            business_key_values: Dict of business key column -> value
            namespace: Table namespace

        Returns:
            DataFrame with all versions ordered by effective date
        """
        df = self.table_manager.read(namespace, table_name)

        # Filter by business keys
        for col, value in business_key_values.items():
            df = df.filter(pl.col(col) == value)

        # Order by version
        return df.sort("_scd_version")

    def merge_scd2(
        self,
        source_df: pl.DataFrame,
        table_name: str,
        business_keys: list[str],
        namespace: str = "silver",
        delete_indicator: str | None = None,
        effective_date: datetime | None = None,
    ) -> dict[str, int]:
        """
        Merge with support for deletes (soft delete in SCD2).

        Args:
            source_df: Source DataFrame
            table_name: Target table
            business_keys: Business key columns
            namespace: Table namespace
            delete_indicator: Column name indicating deleted records
            effective_date: Effective date for changes

        Returns:
            Counts of inserted, updated, deleted, unchanged
        """
        effective_date = effective_date or datetime.utcnow()

        # Separate deletes from regular records
        if delete_indicator and delete_indicator in source_df.columns:
            deletes_df = source_df.filter(pl.col(delete_indicator) == True)
            source_df = source_df.filter(pl.col(delete_indicator) != True)
        else:
            deletes_df = pl.DataFrame()

        # Apply regular SCD2 logic
        counts = self.apply_scd2(
            source_df=source_df,
            table_name=table_name,
            business_keys=business_keys,
            namespace=namespace,
            effective_date=effective_date,
        )

        # Handle deletes (close records without replacement)
        if not deletes_df.is_empty():
            deleted_count = self._apply_deletes(
                deletes_df=deletes_df,
                table_name=table_name,
                business_keys=business_keys,
                namespace=namespace,
                effective_date=effective_date,
            )
            counts["deleted"] = deleted_count
        else:
            counts["deleted"] = 0

        return counts

    def _apply_deletes(
        self,
        deletes_df: pl.DataFrame,
        table_name: str,
        business_keys: list[str],
        namespace: str,
        effective_date: datetime,
    ) -> int:
        """Close records for deleted entities."""
        existing_df = self.table_manager.read(
            namespace=namespace,
            table_name=table_name,
        )

        delete_keys = deletes_df.select(business_keys)

        # Find current records to close
        to_close = existing_df.join(
            delete_keys,
            on=business_keys,
            how="inner",
        ).filter(pl.col("_scd_is_current") == True)

        if to_close.is_empty():
            return 0

        # Close the records
        closed = to_close.with_columns([
            pl.lit(effective_date).alias("_scd_effective_to"),
            pl.lit(False).alias("_scd_is_current"),
        ])

        # Get unchanged records
        unchanged = existing_df.join(
            delete_keys,
            on=business_keys,
            how="anti",
        )

        # Also keep already-closed versions of deleted records
        already_closed = existing_df.join(
            delete_keys,
            on=business_keys,
            how="inner",
        ).filter(pl.col("_scd_is_current") == False)

        # Rebuild table
        all_records = pl.concat([unchanged, already_closed, closed])
        self.table_manager.overwrite(namespace, table_name, all_records)

        return len(to_close)
