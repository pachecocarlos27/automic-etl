"""Data quality checker and profiler."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from datetime import datetime

import polars as pl
import structlog

logger = structlog.get_logger()


@dataclass
class ColumnProfile:
    """Profile for a single column."""
    name: str
    dtype: str
    total_count: int
    null_count: int
    unique_count: int
    null_percentage: float
    unique_percentage: float

    # Numeric stats
    mean: float | None = None
    std: float | None = None
    min_value: Any = None
    max_value: Any = None
    percentiles: dict[str, float] = field(default_factory=dict)

    # String stats
    min_length: int | None = None
    max_length: int | None = None
    avg_length: float | None = None

    # Value distribution
    top_values: list[tuple[Any, int]] = field(default_factory=list)

    # Quality indicators
    is_constant: bool = False
    has_outliers: bool = False
    outlier_count: int = 0


@dataclass
class QualityReport:
    """Complete data quality report."""
    dataset_name: str
    profile_time: datetime
    row_count: int
    column_count: int
    memory_bytes: int
    column_profiles: dict[str, ColumnProfile]

    # Dataset-level metrics
    duplicate_rows: int = 0
    complete_rows: int = 0
    completeness_score: float = 0.0
    quality_score: float = 0.0

    # Issues found
    issues: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "dataset_name": self.dataset_name,
            "profile_time": self.profile_time.isoformat(),
            "summary": {
                "row_count": self.row_count,
                "column_count": self.column_count,
                "memory_mb": self.memory_bytes / (1024 * 1024),
                "duplicate_rows": self.duplicate_rows,
                "complete_rows": self.complete_rows,
                "completeness_score": self.completeness_score,
                "quality_score": self.quality_score,
            },
            "columns": {
                name: {
                    "dtype": p.dtype,
                    "null_percentage": p.null_percentage,
                    "unique_percentage": p.unique_percentage,
                    "is_constant": p.is_constant,
                }
                for name, p in self.column_profiles.items()
            },
            "issues": self.issues,
        }

    def to_dataframe(self) -> pl.DataFrame:
        """Convert column profiles to DataFrame."""
        rows = []
        for name, p in self.column_profiles.items():
            rows.append({
                "column": name,
                "dtype": p.dtype,
                "null_count": p.null_count,
                "null_pct": p.null_percentage,
                "unique_count": p.unique_count,
                "unique_pct": p.unique_percentage,
                "mean": p.mean,
                "std": p.std,
                "min": str(p.min_value) if p.min_value is not None else None,
                "max": str(p.max_value) if p.max_value is not None else None,
                "is_constant": p.is_constant,
                "has_outliers": p.has_outliers,
            })
        return pl.DataFrame(rows)


class DataQualityChecker:
    """
    Comprehensive data quality checker and profiler.

    Features:
    - Column-level profiling
    - Statistical analysis
    - Anomaly detection
    - Quality scoring
    - Issue identification
    """

    def __init__(
        self,
        sample_size: int | None = None,
        detect_outliers: bool = True,
        outlier_std: float = 3.0,
    ) -> None:
        """
        Initialize quality checker.

        Args:
            sample_size: If set, profile a sample instead of full data
            detect_outliers: Whether to detect outliers
            outlier_std: Standard deviations for outlier detection
        """
        self.sample_size = sample_size
        self.detect_outliers = detect_outliers
        self.outlier_std = outlier_std
        self.logger = logger.bind(component="quality_checker")

    def profile(
        self,
        df: pl.DataFrame,
        dataset_name: str = "unnamed",
    ) -> QualityReport:
        """
        Generate a complete quality profile.

        Args:
            df: DataFrame to profile
            dataset_name: Name for the dataset

        Returns:
            QualityReport with detailed analysis
        """
        self.logger.info("Starting quality profile", dataset=dataset_name, rows=len(df))

        # Sample if needed
        if self.sample_size and len(df) > self.sample_size:
            df = df.sample(n=self.sample_size)

        column_profiles = {}
        issues = []

        for col in df.columns:
            profile = self._profile_column(df, col)
            column_profiles[col] = profile

            # Collect issues
            if profile.null_percentage > 50:
                issues.append({
                    "type": "high_null_rate",
                    "column": col,
                    "value": profile.null_percentage,
                    "message": f"Column {col} has {profile.null_percentage:.1f}% null values",
                })

            if profile.is_constant:
                issues.append({
                    "type": "constant_column",
                    "column": col,
                    "message": f"Column {col} has only one unique value",
                })

            if profile.has_outliers:
                issues.append({
                    "type": "outliers_detected",
                    "column": col,
                    "value": profile.outlier_count,
                    "message": f"Column {col} has {profile.outlier_count} outliers",
                })

        # Calculate dataset-level metrics
        duplicate_rows = len(df) - len(df.unique())
        complete_rows = len(df.drop_nulls())

        # Completeness score (percentage of non-null values)
        total_cells = len(df) * len(df.columns)
        null_cells = sum(p.null_count for p in column_profiles.values())
        completeness_score = ((total_cells - null_cells) / total_cells * 100) if total_cells > 0 else 100

        # Quality score (composite)
        quality_score = self._calculate_quality_score(column_profiles, duplicate_rows, len(df))

        if duplicate_rows > 0:
            issues.append({
                "type": "duplicate_rows",
                "value": duplicate_rows,
                "message": f"Dataset has {duplicate_rows} duplicate rows",
            })

        report = QualityReport(
            dataset_name=dataset_name,
            profile_time=datetime.utcnow(),
            row_count=len(df),
            column_count=len(df.columns),
            memory_bytes=df.estimated_size(),
            column_profiles=column_profiles,
            duplicate_rows=duplicate_rows,
            complete_rows=complete_rows,
            completeness_score=completeness_score,
            quality_score=quality_score,
            issues=issues,
        )

        self.logger.info(
            "Quality profile complete",
            dataset=dataset_name,
            quality_score=quality_score,
            issues=len(issues),
        )

        return report

    def _profile_column(self, df: pl.DataFrame, col: str) -> ColumnProfile:
        """Profile a single column."""
        series = df[col]
        dtype = str(series.dtype)
        total = len(series)
        null_count = series.null_count()
        unique_count = series.n_unique()

        profile = ColumnProfile(
            name=col,
            dtype=dtype,
            total_count=total,
            null_count=null_count,
            unique_count=unique_count,
            null_percentage=(null_count / total * 100) if total > 0 else 0,
            unique_percentage=(unique_count / total * 100) if total > 0 else 0,
            is_constant=unique_count <= 1,
        )

        # Numeric stats
        if series.dtype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                           pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
                           pl.Float32, pl.Float64]:
            non_null = series.drop_nulls()
            if len(non_null) > 0:
                profile.mean = non_null.mean()
                profile.std = non_null.std()
                profile.min_value = non_null.min()
                profile.max_value = non_null.max()

                # Percentiles
                try:
                    profile.percentiles = {
                        "25%": non_null.quantile(0.25),
                        "50%": non_null.quantile(0.50),
                        "75%": non_null.quantile(0.75),
                    }
                except Exception:
                    pass

                # Outlier detection
                if self.detect_outliers and profile.std and profile.std > 0:
                    lower = profile.mean - (self.outlier_std * profile.std)
                    upper = profile.mean + (self.outlier_std * profile.std)
                    outliers = ((non_null < lower) | (non_null > upper)).sum()
                    profile.has_outliers = outliers > 0
                    profile.outlier_count = outliers

        # String stats
        elif series.dtype == pl.Utf8:
            non_null = series.drop_nulls()
            if len(non_null) > 0:
                lengths = non_null.str.len_chars()
                profile.min_length = lengths.min()
                profile.max_length = lengths.max()
                profile.avg_length = lengths.mean()

        # Top values
        try:
            value_counts = df.group_by(col).agg(pl.count().alias("count")).sort("count", descending=True)
            profile.top_values = [
                (row[col], row["count"])
                for row in value_counts.head(5).to_dicts()
            ]
        except Exception:
            pass

        return profile

    def _calculate_quality_score(
        self,
        profiles: dict[str, ColumnProfile],
        duplicates: int,
        total_rows: int,
    ) -> float:
        """Calculate composite quality score (0-100)."""
        if not profiles:
            return 100.0

        scores = []

        # Completeness component (40%)
        avg_completeness = sum(100 - p.null_percentage for p in profiles.values()) / len(profiles)
        scores.append(avg_completeness * 0.4)

        # Uniqueness component (20%)
        duplicate_penalty = (duplicates / total_rows * 100) if total_rows > 0 else 0
        scores.append((100 - duplicate_penalty) * 0.2)

        # Consistency component (20%) - fewer constant columns is better
        constant_cols = sum(1 for p in profiles.values() if p.is_constant)
        constant_penalty = (constant_cols / len(profiles) * 100)
        scores.append((100 - constant_penalty) * 0.2)

        # Validity component (20%) - fewer outliers is better
        outlier_cols = sum(1 for p in profiles.values() if p.has_outliers)
        outlier_penalty = (outlier_cols / len(profiles) * 50)  # Max 50% penalty
        scores.append((100 - outlier_penalty) * 0.2)

        return sum(scores)

    def compare_profiles(
        self,
        profile1: QualityReport,
        profile2: QualityReport,
    ) -> dict[str, Any]:
        """
        Compare two quality profiles.

        Useful for comparing before/after transformation
        or detecting data drift.
        """
        comparison = {
            "row_count_diff": profile2.row_count - profile1.row_count,
            "column_count_diff": profile2.column_count - profile1.column_count,
            "quality_score_diff": profile2.quality_score - profile1.quality_score,
            "completeness_diff": profile2.completeness_score - profile1.completeness_score,
            "column_changes": [],
        }

        # Compare columns
        all_cols = set(profile1.column_profiles.keys()) | set(profile2.column_profiles.keys())

        for col in all_cols:
            p1 = profile1.column_profiles.get(col)
            p2 = profile2.column_profiles.get(col)

            if p1 is None:
                comparison["column_changes"].append({
                    "column": col,
                    "change": "added",
                })
            elif p2 is None:
                comparison["column_changes"].append({
                    "column": col,
                    "change": "removed",
                })
            else:
                # Compare stats
                if p1.dtype != p2.dtype:
                    comparison["column_changes"].append({
                        "column": col,
                        "change": "type_changed",
                        "from": p1.dtype,
                        "to": p2.dtype,
                    })

                null_diff = p2.null_percentage - p1.null_percentage
                if abs(null_diff) > 10:  # More than 10% change
                    comparison["column_changes"].append({
                        "column": col,
                        "change": "null_rate_changed",
                        "diff": null_diff,
                    })

        return comparison

    def detect_anomalies(
        self,
        df: pl.DataFrame,
        reference_profile: QualityReport | None = None,
    ) -> list[dict[str, Any]]:
        """
        Detect anomalies in the data.

        Args:
            df: DataFrame to check
            reference_profile: Optional reference profile for drift detection

        Returns:
            List of detected anomalies
        """
        anomalies = []
        current_profile = self.profile(df, "anomaly_check")

        # Basic anomalies from current profile
        anomalies.extend(current_profile.issues)

        # Drift detection if reference provided
        if reference_profile:
            comparison = self.compare_profiles(reference_profile, current_profile)

            if abs(comparison["row_count_diff"]) > reference_profile.row_count * 0.5:
                anomalies.append({
                    "type": "row_count_drift",
                    "expected": reference_profile.row_count,
                    "actual": current_profile.row_count,
                    "message": "Significant change in row count detected",
                })

            if comparison["quality_score_diff"] < -20:
                anomalies.append({
                    "type": "quality_degradation",
                    "expected": reference_profile.quality_score,
                    "actual": current_profile.quality_score,
                    "message": "Quality score dropped significantly",
                })

        return anomalies
