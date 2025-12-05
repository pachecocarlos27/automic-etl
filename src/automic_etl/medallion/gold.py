"""Gold layer implementation - Business-level aggregations."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Callable

import polars as pl
import structlog

from automic_etl.core.config import Settings
from automic_etl.core.exceptions import TransformationError
from automic_etl.storage.iceberg import IcebergTableManager
from automic_etl.core.utils import utc_now

logger = structlog.get_logger()


class AggregationType(str, Enum):
    """Types of aggregations."""

    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    COLLECT = "collect"


class GoldLayer:
    """
    Gold Layer - Business-Level Aggregations.

    The gold layer contains refined, business-ready datasets optimized
    for analytics, reporting, and machine learning.

    Features:
    - Business-level aggregations and metrics
    - Denormalized views for performance
    - Feature engineering for ML
    - Pre-computed KPIs and metrics
    - Optimized for query performance
    """

    NAMESPACE = "gold"

    # Metadata columns for gold layer
    METADATA_COLUMNS = [
        "_computed_time",
        "_source_tables",
        "_aggregation_period",
    ]

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.table_manager = IcebergTableManager(settings)
        self.logger = logger.bind(layer="gold")

    def aggregate(
        self,
        source_table: str,
        target_table: str,
        group_by: list[str],
        aggregations: dict[str, list[tuple[str, AggregationType]]],
        filter_expr: str | None = None,
        having_expr: str | None = None,
        mode: str = "overwrite",
    ) -> int:
        """
        Create an aggregated gold table.

        Args:
            source_table: Silver table name
            target_table: Gold table name
            group_by: Columns to group by
            aggregations: Dict of {output_col: [(source_col, agg_type), ...]}
            filter_expr: Optional filter before aggregation
            having_expr: Optional having clause after aggregation
            mode: 'overwrite' or 'append'

        Returns:
            Number of rows written
        """
        from automic_etl.medallion.silver import SilverLayer

        silver = SilverLayer(self.settings)

        # Read from silver
        df = silver.read(source_table, filter_expr=filter_expr)

        if df.is_empty():
            self.logger.warning("No data to aggregate", source=source_table)
            return 0

        # Build aggregation expressions
        agg_exprs = self._build_aggregation_exprs(aggregations)

        # Apply aggregation
        df = df.group_by(group_by).agg(agg_exprs)

        # Apply having clause
        if having_expr:
            df = df.filter(pl.sql_expr(having_expr))

        # Add gold metadata
        computed_time = utc_now()
        df = self._add_metadata(df, [source_table], computed_time)

        # Write to gold layer
        return self._write(target_table, df, mode)

    def create_feature_table(
        self,
        source_tables: list[str],
        target_table: str,
        entity_column: str,
        features: list[FeatureDefinition],
        time_column: str | None = None,
        mode: str = "overwrite",
    ) -> int:
        """
        Create a feature table for ML.

        Args:
            source_tables: Silver tables to join
            target_table: Gold feature table name
            entity_column: Entity identifier column
            features: List of feature definitions
            time_column: Optional time column for point-in-time features
            mode: 'overwrite' or 'append'

        Returns:
            Number of rows written
        """
        from automic_etl.medallion.silver import SilverLayer

        silver = SilverLayer(self.settings)

        # Read and join source tables
        dfs = [silver.read(table) for table in source_tables]

        if len(dfs) == 1:
            df = dfs[0]
        else:
            # Join tables on entity column
            df = dfs[0]
            for other_df in dfs[1:]:
                if entity_column in other_df.columns:
                    df = df.join(
                        other_df,
                        on=entity_column,
                        how="outer",
                        suffix="_right",
                    )

        # Generate features
        for feature in features:
            df = feature.compute(df)

        # Select only entity and feature columns
        feature_cols = [entity_column] + [f.name for f in features]
        if time_column and time_column in df.columns:
            feature_cols.append(time_column)

        df = df.select([c for c in feature_cols if c in df.columns])

        # Add metadata
        computed_time = utc_now()
        df = self._add_metadata(df, source_tables, computed_time)

        return self._write(target_table, df, mode)

    def create_denormalized_view(
        self,
        source_tables: list[str],
        target_table: str,
        joins: list[JoinDefinition],
        select_columns: list[str] | None = None,
        mode: str = "overwrite",
    ) -> int:
        """
        Create a denormalized view by joining multiple tables.

        Args:
            source_tables: Silver tables to join
            target_table: Gold table name
            joins: List of join definitions
            select_columns: Columns to include in output
            mode: 'overwrite' or 'append'

        Returns:
            Number of rows written
        """
        from automic_etl.medallion.silver import SilverLayer

        silver = SilverLayer(self.settings)

        # Read base table
        df = silver.read(source_tables[0])

        # Apply joins
        for join_def in joins:
            right_df = silver.read(join_def.table)
            df = df.join(
                right_df,
                left_on=join_def.left_on,
                right_on=join_def.right_on,
                how=join_def.how,
                suffix=f"_{join_def.table}",
            )

        # Select columns
        if select_columns:
            df = df.select([c for c in select_columns if c in df.columns])

        # Add metadata
        computed_time = utc_now()
        df = self._add_metadata(df, source_tables, computed_time)

        return self._write(target_table, df, mode)

    def compute_metrics(
        self,
        source_table: str,
        target_table: str,
        metrics: list[MetricDefinition],
        dimensions: list[str] | None = None,
        time_column: str | None = None,
        time_granularity: str = "day",
        mode: str = "overwrite",
    ) -> int:
        """
        Compute business metrics.

        Args:
            source_table: Silver table name
            target_table: Gold metrics table
            metrics: List of metric definitions
            dimensions: Dimension columns
            time_column: Time column for time-series metrics
            time_granularity: day, week, month, year
            mode: 'overwrite' or 'append'

        Returns:
            Number of rows written
        """
        from automic_etl.medallion.silver import SilverLayer

        silver = SilverLayer(self.settings)
        df = silver.read(source_table)

        # Build group by columns
        group_by = dimensions or []

        # Add time dimension if specified
        if time_column and time_column in df.columns:
            time_col = self._get_time_truncation(time_column, time_granularity)
            df = df.with_columns(time_col.alias("_time_period"))
            group_by.append("_time_period")

        # Compute metrics
        metric_exprs = []
        for metric in metrics:
            expr = metric.compute_expr()
            metric_exprs.append(expr.alias(metric.name))

        if group_by:
            df = df.group_by(group_by).agg(metric_exprs)
        else:
            df = df.select(metric_exprs)

        # Add metadata
        computed_time = utc_now()
        df = self._add_metadata(df, [source_table], computed_time)

        return self._write(target_table, df, mode)

    def _build_aggregation_exprs(
        self,
        aggregations: dict[str, list[tuple[str, AggregationType]]],
    ) -> list[pl.Expr]:
        """Build Polars aggregation expressions."""
        exprs = []

        for output_col, aggs in aggregations.items():
            for source_col, agg_type in aggs:
                col_expr = pl.col(source_col)

                if agg_type == AggregationType.SUM:
                    expr = col_expr.sum()
                elif agg_type == AggregationType.COUNT:
                    expr = col_expr.count()
                elif agg_type == AggregationType.AVG:
                    expr = col_expr.mean()
                elif agg_type == AggregationType.MIN:
                    expr = col_expr.min()
                elif agg_type == AggregationType.MAX:
                    expr = col_expr.max()
                elif agg_type == AggregationType.FIRST:
                    expr = col_expr.first()
                elif agg_type == AggregationType.LAST:
                    expr = col_expr.last()
                elif agg_type == AggregationType.COLLECT:
                    expr = col_expr.implode()
                else:
                    raise ValueError(f"Unknown aggregation type: {agg_type}")

                exprs.append(expr.alias(output_col))

        return exprs

    def _get_time_truncation(self, column: str, granularity: str) -> pl.Expr:
        """Get time truncation expression."""
        col = pl.col(column)

        if granularity == "hour":
            return col.dt.truncate("1h")
        elif granularity == "day":
            return col.dt.truncate("1d")
        elif granularity == "week":
            return col.dt.truncate("1w")
        elif granularity == "month":
            return col.dt.truncate("1mo")
        elif granularity == "year":
            return col.dt.truncate("1y")
        else:
            return col.dt.truncate("1d")

    def _add_metadata(
        self,
        df: pl.DataFrame,
        source_tables: list[str],
        computed_time: datetime,
    ) -> pl.DataFrame:
        """Add gold layer metadata."""
        return df.with_columns([
            pl.lit(computed_time).alias("_computed_time"),
            pl.lit(",".join(source_tables)).alias("_source_tables"),
        ])

    def _write(
        self,
        table_name: str,
        df: pl.DataFrame,
        mode: str,
    ) -> int:
        """Write data to gold layer."""
        try:
            if mode == "overwrite" or not self.table_exists(table_name):
                if self.table_exists(table_name):
                    self.table_manager.overwrite(self.NAMESPACE, table_name, df)
                else:
                    partition_columns = self.settings.medallion.gold.partition_by
                    self.table_manager.create_table_from_dataframe(
                        namespace=self.NAMESPACE,
                        table_name=table_name,
                        df=df,
                        partition_columns=partition_columns if partition_columns else None,
                        properties={"automic.layer": "gold"},
                    )
                    self.table_manager.append(self.NAMESPACE, table_name, df)
                rows = len(df)
            else:
                rows = self.table_manager.append(self.NAMESPACE, table_name, df)

            self.logger.info(
                "Wrote data to gold",
                table=table_name,
                rows=rows,
                mode=mode,
            )
            return rows

        except Exception as e:
            raise TransformationError(
                f"Failed to write to gold layer: {str(e)}",
                transformation="write_gold",
                details={"table": table_name},
            )

    def read(
        self,
        table_name: str,
        columns: list[str] | None = None,
        filter_expr: str | None = None,
        limit: int | None = None,
    ) -> pl.DataFrame:
        """Read data from the gold layer."""
        return self.table_manager.read(
            namespace=self.NAMESPACE,
            table_name=table_name,
            columns=columns,
            filter_expr=filter_expr,
            limit=limit,
        )

    def list_tables(self) -> list[str]:
        """List all tables in the gold layer."""
        return self.table_manager.catalog.list_tables(self.NAMESPACE)

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        return self.table_manager.catalog.table_exists(self.NAMESPACE, table_name)


# ============================================================================
# Supporting Classes
# ============================================================================

class FeatureDefinition:
    """Definition for a feature computation."""

    def __init__(
        self,
        name: str,
        source_column: str,
        transform: Callable[[pl.Expr], pl.Expr] | None = None,
        expression: pl.Expr | None = None,
    ) -> None:
        self.name = name
        self.source_column = source_column
        self.transform = transform
        self.expression = expression

    def compute(self, df: pl.DataFrame) -> pl.DataFrame:
        """Compute the feature and add to dataframe."""
        if self.expression is not None:
            return df.with_columns(self.expression.alias(self.name))
        elif self.transform is not None:
            expr = self.transform(pl.col(self.source_column))
            return df.with_columns(expr.alias(self.name))
        else:
            return df.with_columns(pl.col(self.source_column).alias(self.name))


class JoinDefinition:
    """Definition for a table join."""

    def __init__(
        self,
        table: str,
        left_on: str | list[str],
        right_on: str | list[str],
        how: str = "left",
    ) -> None:
        self.table = table
        self.left_on = left_on
        self.right_on = right_on
        self.how = how


class MetricDefinition:
    """Definition for a business metric."""

    def __init__(
        self,
        name: str,
        expression: pl.Expr | None = None,
        column: str | None = None,
        aggregation: AggregationType = AggregationType.SUM,
    ) -> None:
        self.name = name
        self.expression = expression
        self.column = column
        self.aggregation = aggregation

    def compute_expr(self) -> pl.Expr:
        """Get the Polars expression for this metric."""
        if self.expression is not None:
            return self.expression

        if self.column is None:
            raise ValueError(f"Metric {self.name} requires either expression or column")

        col = pl.col(self.column)

        if self.aggregation == AggregationType.SUM:
            return col.sum()
        elif self.aggregation == AggregationType.COUNT:
            return col.count()
        elif self.aggregation == AggregationType.AVG:
            return col.mean()
        elif self.aggregation == AggregationType.MIN:
            return col.min()
        elif self.aggregation == AggregationType.MAX:
            return col.max()
        else:
            return col.sum()
