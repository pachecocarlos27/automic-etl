"""QA and verification tools for data transformations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from enum import Enum
import json

import polars as pl
import structlog

from automic_etl.core.utils import utc_now

logger = structlog.get_logger()


class QAStatus(str, Enum):
    """Status of a QA check."""

    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"


@dataclass
class QACheck:
    """Individual QA check result."""

    name: str
    status: QAStatus
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    severity: str = "info"


@dataclass
class ComparisonReport:
    """Report comparing before and after states."""

    before_stats: dict[str, Any]
    after_stats: dict[str, Any]
    differences: list[dict[str, Any]]
    summary: str
    passed: bool
    checks: list[QACheck] = field(default_factory=list)
    timestamp: datetime = field(default_factory=utc_now)

    def to_dict(self) -> dict[str, Any]:
        return {
            "before_stats": self.before_stats,
            "after_stats": self.after_stats,
            "differences": self.differences,
            "summary": self.summary,
            "passed": self.passed,
            "checks": [
                {
                    "name": c.name,
                    "status": c.status.value,
                    "message": c.message,
                    "details": c.details,
                }
                for c in self.checks
            ],
            "timestamp": self.timestamp.isoformat(),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, default=str)


@dataclass
class QAResult:
    """Overall QA result."""

    passed: bool
    checks: list[QACheck]
    summary: str
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=utc_now)

    @property
    def passed_count(self) -> int:
        return sum(1 for c in self.checks if c.status == QAStatus.PASSED)

    @property
    def failed_count(self) -> int:
        return sum(1 for c in self.checks if c.status == QAStatus.FAILED)

    @property
    def warning_count(self) -> int:
        return sum(1 for c in self.checks if c.status == QAStatus.WARNING)

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "summary": self.summary,
            "passed_count": self.passed_count,
            "failed_count": self.failed_count,
            "warning_count": self.warning_count,
            "checks": [
                {
                    "name": c.name,
                    "status": c.status.value,
                    "message": c.message,
                    "details": c.details,
                    "severity": c.severity,
                }
                for c in self.checks
            ],
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class QAConfig:
    """Configuration for QA checks."""

    check_row_count: bool = True
    check_null_counts: bool = True
    check_duplicates: bool = True
    check_schema: bool = True
    check_value_ranges: bool = False
    sample_size: int = 10
    tolerance_percent: float = 5.0
    fail_on_warnings: bool = False


class TransformationQA:
    """
    Quality assurance tools for verifying data transformations.

    Supports:
    - Before/after comparison
    - Entity count verification
    - Sample-based spot checking
    - Statistical summaries
    - Redaction verification
    """

    def __init__(self, config: QAConfig | None = None) -> None:
        self.config = config or QAConfig()
        self.logger = logger.bind(component="transformation_qa")

    def compare_dataframes(
        self,
        before: pl.DataFrame,
        after: pl.DataFrame,
        key_columns: list[str] | None = None,
    ) -> ComparisonReport:
        """
        Compare two DataFrames and generate a report.

        Args:
            before: Original DataFrame
            after: Transformed DataFrame
            key_columns: Columns to use for row matching

        Returns:
            ComparisonReport with detailed comparison
        """
        checks = []
        differences = []

        before_stats = self._compute_stats(before, "before")
        after_stats = self._compute_stats(after, "after")

        if self.config.check_row_count:
            check = self._check_row_count(before, after)
            checks.append(check)
            if check.status == QAStatus.FAILED:
                differences.append({
                    "type": "row_count",
                    "before": len(before),
                    "after": len(after),
                    "change": len(after) - len(before),
                })

        if self.config.check_schema:
            check = self._check_schema(before, after)
            checks.append(check)
            if check.status != QAStatus.PASSED:
                differences.append({
                    "type": "schema",
                    "details": check.details,
                })

        if self.config.check_null_counts:
            check = self._check_null_counts(before, after)
            checks.append(check)

        if self.config.check_duplicates:
            check = self._check_duplicates(after, key_columns)
            checks.append(check)

        passed = all(
            c.status == QAStatus.PASSED or
            (c.status == QAStatus.WARNING and not self.config.fail_on_warnings)
            for c in checks
        )

        summary = self._generate_summary(checks, differences)

        return ComparisonReport(
            before_stats=before_stats,
            after_stats=after_stats,
            differences=differences,
            summary=summary,
            passed=passed,
            checks=checks,
        )

    def verify_redaction(
        self,
        before_text: str,
        after_text: str,
        expected_tags: list[str] | None = None,
    ) -> QAResult:
        """
        Verify that redaction was applied correctly.

        Args:
            before_text: Original text
            after_text: Redacted text
            expected_tags: Tags that should appear in redacted text

        Returns:
            QAResult with verification details
        """
        checks = []

        if len(after_text) > len(before_text):
            checks.append(QACheck(
                name="text_length",
                status=QAStatus.WARNING,
                message="Redacted text is longer than original",
                details={
                    "before_length": len(before_text),
                    "after_length": len(after_text),
                },
            ))
        else:
            checks.append(QACheck(
                name="text_length",
                status=QAStatus.PASSED,
                message="Text length is acceptable",
            ))

        if expected_tags:
            import re
            found_tags = set(re.findall(r'\[[A-Z_]+\]', after_text))
            expected_set = set(expected_tags)
            missing = expected_set - found_tags

            if missing:
                checks.append(QACheck(
                    name="expected_tags",
                    status=QAStatus.WARNING,
                    message=f"Some expected tags not found: {missing}",
                    details={"missing": list(missing), "found": list(found_tags)},
                ))
            else:
                checks.append(QACheck(
                    name="expected_tags",
                    status=QAStatus.PASSED,
                    message="All expected tags found",
                    details={"found": list(found_tags)},
                ))

        import re as re_module
        tag_count = len(re_module.findall(r'\[[A-Z_]+\]', after_text))
        checks.append(QACheck(
            name="redaction_count",
            status=QAStatus.PASSED,
            message=f"Found {tag_count} redaction tags",
            details={"tag_count": tag_count},
        ))

        passed = all(c.status != QAStatus.FAILED for c in checks)

        return QAResult(
            passed=passed,
            checks=checks,
            summary=f"Redaction verification: {'PASSED' if passed else 'FAILED'}",
            metadata={
                "before_length": len(before_text),
                "after_length": len(after_text),
            },
        )

    def verify_entity_counts(
        self,
        before_counts: dict[str, int],
        after_counts: dict[str, int],
        expect_zero_after: bool = True,
    ) -> QAResult:
        """
        Verify entity counts before and after redaction.

        Args:
            before_counts: Entity counts in original data
            after_counts: Entity counts after redaction
            expect_zero_after: Whether all entities should be zero after

        Returns:
            QAResult with verification details
        """
        checks = []

        total_before = sum(before_counts.values())
        total_after = sum(after_counts.values())

        if expect_zero_after:
            if total_after == 0:
                checks.append(QACheck(
                    name="all_entities_redacted",
                    status=QAStatus.PASSED,
                    message=f"All {total_before} entities were redacted",
                    details={
                        "before": before_counts,
                        "after": after_counts,
                    },
                ))
            else:
                remaining = {k: v for k, v in after_counts.items() if v > 0}
                checks.append(QACheck(
                    name="all_entities_redacted",
                    status=QAStatus.FAILED,
                    message=f"{total_after} entities remain after redaction",
                    details={
                        "remaining": remaining,
                        "before": before_counts,
                    },
                    severity="error",
                ))
        else:
            reduction = total_before - total_after
            reduction_pct = (reduction / total_before * 100) if total_before > 0 else 0

            checks.append(QACheck(
                name="entity_reduction",
                status=QAStatus.PASSED,
                message=f"Reduced entities by {reduction_pct:.1f}%",
                details={
                    "before_total": total_before,
                    "after_total": total_after,
                    "reduction": reduction,
                    "reduction_percent": reduction_pct,
                },
            ))

        passed = all(c.status != QAStatus.FAILED for c in checks)

        return QAResult(
            passed=passed,
            checks=checks,
            summary=f"Entity count verification: {'PASSED' if passed else 'FAILED'}",
            metadata={
                "before_counts": before_counts,
                "after_counts": after_counts,
            },
        )

    def generate_spot_check_samples(
        self,
        before_df: pl.DataFrame,
        after_df: pl.DataFrame,
        text_column: str,
        n_samples: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Generate samples for manual spot checking.

        Args:
            before_df: Original DataFrame
            after_df: Transformed DataFrame
            text_column: Column containing text to compare
            n_samples: Number of samples to generate

        Returns:
            List of sample comparisons
        """
        n_samples = n_samples or self.config.sample_size

        def to_str(value: Any) -> str:
            """Convert any value to string safely."""
            if value is None:
                return ""
            if isinstance(value, str):
                return value
            try:
                return str(value)
            except Exception:
                return repr(value)

        def make_sample(idx: int, before: Any, after: Any, note: str = "") -> dict[str, Any]:
            """Create a consistent sample dict structure."""
            before_str = to_str(before)
            after_str = to_str(after)
            return {
                "index": idx,
                "before": before_str,
                "after": after_str,
                "changed": before_str != after_str,
                "note": note,
            }

        if len(before_df) == 0 or len(after_df) == 0:
            return [make_sample(-1, "", "", "empty_dataframe")]

        available = min(len(before_df), len(after_df))
        n_samples = min(n_samples, available)
        if n_samples == 0:
            return [make_sample(-1, "", "", "no_samples")]

        samples = []
        step = max(1, available // n_samples) if n_samples > 0 else 1
        indices = list(range(0, available, step))[:n_samples]

        for idx in indices:
            before_text = before_df[text_column][idx]
            after_text = None
            note = ""

            if text_column in after_df.columns:
                after_text = after_df[text_column][idx]
            else:
                redacted_col = f"{text_column}_redacted"
                if redacted_col in after_df.columns:
                    after_text = after_df[redacted_col][idx]
                else:
                    note = "missing_after_column"

            samples.append(make_sample(idx, before_text, after_text, note))

        return samples

    def generate_summary_report(
        self,
        results: list[QAResult],
        title: str = "QA Summary Report",
    ) -> dict[str, Any]:
        """Generate a summary report from multiple QA results."""
        total_checks = sum(len(r.checks) for r in results)
        total_passed = sum(r.passed_count for r in results)
        total_failed = sum(r.failed_count for r in results)
        total_warnings = sum(r.warning_count for r in results)

        overall_passed = all(r.passed for r in results)

        return {
            "title": title,
            "overall_status": "PASSED" if overall_passed else "FAILED",
            "timestamp": utc_now().isoformat(),
            "summary": {
                "total_checks": total_checks,
                "passed": total_passed,
                "failed": total_failed,
                "warnings": total_warnings,
                "pass_rate": (total_passed / total_checks * 100) if total_checks > 0 else 0,
            },
            "results": [r.to_dict() for r in results],
        }

    def _compute_stats(self, df: pl.DataFrame, label: str) -> dict[str, Any]:
        """Compute basic statistics for a DataFrame."""
        stats = {
            "label": label,
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns),
            "null_counts": {},
            "dtypes": {},
        }

        for col in df.columns:
            stats["null_counts"][col] = df[col].null_count()
            stats["dtypes"][col] = str(df[col].dtype)

        return stats

    def _check_row_count(self, before: pl.DataFrame, after: pl.DataFrame) -> QACheck:
        """Check if row counts match."""
        if len(before) == len(after):
            return QACheck(
                name="row_count",
                status=QAStatus.PASSED,
                message=f"Row count unchanged: {len(before)}",
            )
        else:
            diff_pct = abs(len(after) - len(before)) / len(before) * 100 if len(before) > 0 else 100
            status = QAStatus.WARNING if diff_pct <= self.config.tolerance_percent else QAStatus.FAILED
            return QACheck(
                name="row_count",
                status=status,
                message=f"Row count changed: {len(before)} -> {len(after)} ({diff_pct:.1f}% change)",
                details={
                    "before": len(before),
                    "after": len(after),
                    "change_percent": diff_pct,
                },
            )

    def _check_schema(self, before: pl.DataFrame, after: pl.DataFrame) -> QACheck:
        """Check if schemas are compatible."""
        before_cols = set(before.columns)
        after_cols = set(after.columns)

        removed = before_cols - after_cols
        added = after_cols - before_cols

        if not removed and not added:
            return QACheck(
                name="schema",
                status=QAStatus.PASSED,
                message="Schema unchanged",
            )

        return QACheck(
            name="schema",
            status=QAStatus.WARNING,
            message=f"Schema changed: +{len(added)} columns, -{len(removed)} columns",
            details={
                "added": list(added),
                "removed": list(removed),
            },
        )

    def _check_null_counts(self, before: pl.DataFrame, after: pl.DataFrame) -> QACheck:
        """Check null count changes."""
        common_cols = set(before.columns) & set(after.columns)
        significant_changes = []

        for col in common_cols:
            before_nulls = before[col].null_count()
            after_nulls = after[col].null_count()

            if after_nulls > before_nulls:
                significant_changes.append({
                    "column": col,
                    "before": before_nulls,
                    "after": after_nulls,
                    "increase": after_nulls - before_nulls,
                })

        if not significant_changes:
            return QACheck(
                name="null_counts",
                status=QAStatus.PASSED,
                message="No increase in null values",
            )

        return QACheck(
            name="null_counts",
            status=QAStatus.WARNING,
            message=f"Null values increased in {len(significant_changes)} columns",
            details={"changes": significant_changes},
        )

    def _check_duplicates(
        self,
        df: pl.DataFrame,
        key_columns: list[str] | None,
    ) -> QACheck:
        """Check for duplicate rows."""
        if key_columns:
            cols_to_check = [c for c in key_columns if c in df.columns]
            if not cols_to_check:
                return QACheck(
                    name="duplicates",
                    status=QAStatus.SKIPPED,
                    message="Key columns not found in DataFrame",
                )
            dup_count = len(df) - df.unique(subset=cols_to_check).height
        else:
            dup_count = len(df) - df.unique().height

        if dup_count == 0:
            return QACheck(
                name="duplicates",
                status=QAStatus.PASSED,
                message="No duplicate rows found",
            )

        return QACheck(
            name="duplicates",
            status=QAStatus.WARNING,
            message=f"Found {dup_count} duplicate rows",
            details={"duplicate_count": dup_count},
        )

    def _generate_summary(
        self,
        checks: list[QACheck],
        differences: list[dict[str, Any]],
    ) -> str:
        """Generate a human-readable summary."""
        passed = sum(1 for c in checks if c.status == QAStatus.PASSED)
        failed = sum(1 for c in checks if c.status == QAStatus.FAILED)
        warnings = sum(1 for c in checks if c.status == QAStatus.WARNING)

        parts = [
            f"QA Summary: {passed} passed, {failed} failed, {warnings} warnings.",
        ]

        if differences:
            parts.append(f"Found {len(differences)} differences.")

        return " ".join(parts)
