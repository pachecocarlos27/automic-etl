"""Batch extraction functionality."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Iterator

import polars as pl
import structlog

from automic_etl.connectors.base import BaseConnector, ExtractionResult
from automic_etl.core.config import Settings
from automic_etl.core.exceptions import ExtractionError
from automic_etl.core.utils import utc_now

logger = structlog.get_logger()


@dataclass
class BatchResult:
    """Result of batch extraction."""

    data: pl.DataFrame
    total_rows: int
    batches_processed: int
    start_time: datetime
    end_time: datetime
    errors: list[Exception]

    @property
    def duration_seconds(self) -> float:
        """Get extraction duration in seconds."""
        return (self.end_time - self.start_time).total_seconds()

    @property
    def rows_per_second(self) -> float:
        """Get extraction rate."""
        if self.duration_seconds > 0:
            return self.total_rows / self.duration_seconds
        return 0


class BatchExtractor:
    """
    Extract data in batches.

    Features:
    - Configurable batch sizes
    - Parallel extraction
    - Progress tracking
    - Error handling per batch
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.batch_size = settings.extraction.batch.size
        self.parallel_workers = settings.extraction.batch.parallel_workers
        self.logger = logger.bind(component="batch_extractor")

    def extract(
        self,
        connector: BaseConnector,
        query: str | None = None,
        table: str | None = None,
        batch_size: int | None = None,
        transform: Callable[[pl.DataFrame], pl.DataFrame] | None = None,
        on_batch: Callable[[pl.DataFrame, int], None] | None = None,
    ) -> BatchResult:
        """
        Extract data in batches.

        Args:
            connector: Data connector to use
            query: Optional query string
            table: Optional table name
            batch_size: Override batch size
            transform: Optional transformation per batch
            on_batch: Callback for each batch

        Returns:
            BatchResult with combined data
        """
        batch_size = batch_size or self.batch_size
        start_time = utc_now()
        errors: list[Exception] = []
        batches: list[pl.DataFrame] = []
        batch_num = 0

        self.logger.info(
            "Starting batch extraction",
            batch_size=batch_size,
            query=query[:100] if query else None,
            table=table,
        )

        try:
            for batch_result in connector.extract_batch(
                query=query,
                batch_size=batch_size,
            ):
                batch_num += 1

                try:
                    df = batch_result.data

                    # Apply transformation if provided
                    if transform:
                        df = transform(df)

                    batches.append(df)

                    # Call batch callback
                    if on_batch:
                        on_batch(df, batch_num)

                    self.logger.debug(
                        "Processed batch",
                        batch=batch_num,
                        rows=len(df),
                    )

                except Exception as e:
                    self.logger.error(f"Batch {batch_num} failed: {e}")
                    errors.append(e)

        except Exception as e:
            self.logger.error(f"Extraction failed: {e}")
            errors.append(e)

        # Combine batches
        combined = pl.concat(batches) if batches else pl.DataFrame()

        end_time = utc_now()

        result = BatchResult(
            data=combined,
            total_rows=len(combined),
            batches_processed=batch_num,
            start_time=start_time,
            end_time=end_time,
            errors=errors,
        )

        self.logger.info(
            "Batch extraction completed",
            total_rows=result.total_rows,
            batches=batch_num,
            duration_seconds=result.duration_seconds,
            rows_per_second=result.rows_per_second,
            errors=len(errors),
        )

        return result

    def extract_parallel(
        self,
        connector: BaseConnector,
        queries: list[str],
        max_workers: int | None = None,
        transform: Callable[[pl.DataFrame], pl.DataFrame] | None = None,
    ) -> BatchResult:
        """
        Extract multiple queries in parallel.

        Args:
            connector: Data connector to use
            queries: List of queries to execute
            max_workers: Maximum parallel workers
            transform: Optional transformation per result

        Returns:
            BatchResult with combined data
        """
        max_workers = max_workers or self.parallel_workers
        start_time = utc_now()
        errors: list[Exception] = []
        results: list[pl.DataFrame] = []

        self.logger.info(
            "Starting parallel extraction",
            queries=len(queries),
            max_workers=max_workers,
        )

        def extract_query(query: str) -> pl.DataFrame:
            result = connector.extract(query=query)
            df = result.data
            if transform:
                df = transform(df)
            return df

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_query = {
                executor.submit(extract_query, q): q for q in queries
            }

            for future in as_completed(future_to_query):
                query = future_to_query[future]
                try:
                    df = future.result()
                    results.append(df)
                except Exception as e:
                    self.logger.error(f"Query failed: {query[:50]}: {e}")
                    errors.append(e)

        combined = pl.concat(results) if results else pl.DataFrame()
        end_time = utc_now()

        return BatchResult(
            data=combined,
            total_rows=len(combined),
            batches_processed=len(queries),
            start_time=start_time,
            end_time=end_time,
            errors=errors,
        )

    def extract_tables(
        self,
        connector: BaseConnector,
        tables: list[str],
        parallel: bool = True,
    ) -> dict[str, BatchResult]:
        """
        Extract multiple tables.

        Args:
            connector: Data connector to use
            tables: List of table names
            parallel: Whether to extract in parallel

        Returns:
            Dict of table name -> BatchResult
        """
        results = {}

        if parallel:
            with ThreadPoolExecutor(max_workers=self.parallel_workers) as executor:
                future_to_table = {
                    executor.submit(
                        self.extract,
                        connector,
                        table=table,
                    ): table
                    for table in tables
                }

                for future in as_completed(future_to_table):
                    table = future_to_table[future]
                    try:
                        results[table] = future.result()
                    except Exception as e:
                        self.logger.error(f"Table {table} failed: {e}")
        else:
            for table in tables:
                try:
                    results[table] = self.extract(connector, table=table)
                except Exception as e:
                    self.logger.error(f"Table {table} failed: {e}")

        return results

    def extract_to_lakehouse(
        self,
        connector: BaseConnector,
        table_name: str,
        source: str,
        query: str | None = None,
        batch_size: int | None = None,
    ) -> int:
        """
        Extract directly to lakehouse bronze layer.

        Args:
            connector: Data connector
            table_name: Target table name
            source: Source identifier
            query: Optional query
            batch_size: Override batch size

        Returns:
            Total rows ingested
        """
        from automic_etl.medallion import Lakehouse

        lakehouse = Lakehouse(self.settings)
        total_rows = 0

        for batch_result in connector.extract_batch(
            query=query,
            batch_size=batch_size or self.batch_size,
        ):
            rows = lakehouse.ingest(
                table_name=table_name,
                data=batch_result.data,
                source=source,
            )
            total_rows += rows

        return total_rows
