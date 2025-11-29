"""Validation rules for data quality checks."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable
from enum import Enum
import re

import polars as pl


class Severity(Enum):
    """Validation rule severity levels."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationResult:
    """Result of a validation check."""
    rule_name: str
    passed: bool
    severity: Severity
    message: str
    failing_rows: int = 0
    total_rows: int = 0
    failing_values: list[Any] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def pass_rate(self) -> float:
        """Calculate pass rate percentage."""
        if self.total_rows == 0:
            return 100.0
        return ((self.total_rows - self.failing_rows) / self.total_rows) * 100


class ValidationRule(ABC):
    """Base class for validation rules."""

    def __init__(
        self,
        name: str,
        severity: Severity = Severity.ERROR,
        description: str = "",
    ) -> None:
        self.name = name
        self.severity = severity
        self.description = description

    @abstractmethod
    def validate(self, df: pl.DataFrame) -> ValidationResult:
        """Validate the DataFrame against this rule."""
        pass


class NotNullRule(ValidationRule):
    """Validate that column(s) contain no null values."""

    def __init__(
        self,
        columns: list[str] | str,
        name: str | None = None,
        severity: Severity = Severity.ERROR,
        allow_empty: bool = False,
    ) -> None:
        self.columns = [columns] if isinstance(columns, str) else columns
        self.allow_empty = allow_empty
        super().__init__(
            name=name or f"not_null_{'-'.join(self.columns)}",
            severity=severity,
            description=f"Columns {self.columns} must not be null",
        )

    def validate(self, df: pl.DataFrame) -> ValidationResult:
        total_rows = len(df)
        failing_rows = 0
        failing_details = {}

        for col in self.columns:
            if col not in df.columns:
                continue

            null_mask = df[col].is_null()
            if not self.allow_empty and df[col].dtype == pl.Utf8:
                null_mask = null_mask | (df[col] == "")

            col_failing = null_mask.sum()
            if col_failing > 0:
                failing_rows += col_failing
                failing_details[col] = col_failing

        return ValidationResult(
            rule_name=self.name,
            passed=failing_rows == 0,
            severity=self.severity,
            message=f"Found {failing_rows} null values" if failing_rows > 0 else "No null values found",
            failing_rows=failing_rows,
            total_rows=total_rows * len(self.columns),
            details=failing_details,
        )


class UniqueRule(ValidationRule):
    """Validate that column(s) contain unique values."""

    def __init__(
        self,
        columns: list[str] | str,
        name: str | None = None,
        severity: Severity = Severity.ERROR,
    ) -> None:
        self.columns = [columns] if isinstance(columns, str) else columns
        super().__init__(
            name=name or f"unique_{'-'.join(self.columns)}",
            severity=severity,
            description=f"Columns {self.columns} must be unique",
        )

    def validate(self, df: pl.DataFrame) -> ValidationResult:
        total_rows = len(df)

        # Count duplicates
        dupe_count = df.group_by(self.columns).agg(
            pl.count().alias("_count")
        ).filter(pl.col("_count") > 1)

        duplicate_rows = dupe_count["_count"].sum() - len(dupe_count) if len(dupe_count) > 0 else 0

        # Get sample of duplicate values
        failing_values = []
        if len(dupe_count) > 0:
            failing_values = dupe_count.head(10).select(self.columns).to_dicts()

        return ValidationResult(
            rule_name=self.name,
            passed=duplicate_rows == 0,
            severity=self.severity,
            message=f"Found {duplicate_rows} duplicate rows" if duplicate_rows > 0 else "All values are unique",
            failing_rows=duplicate_rows,
            total_rows=total_rows,
            failing_values=failing_values,
        )


class RangeRule(ValidationRule):
    """Validate that numeric values fall within a range."""

    def __init__(
        self,
        column: str,
        min_value: float | int | None = None,
        max_value: float | int | None = None,
        name: str | None = None,
        severity: Severity = Severity.ERROR,
        inclusive: bool = True,
    ) -> None:
        self.column = column
        self.min_value = min_value
        self.max_value = max_value
        self.inclusive = inclusive
        super().__init__(
            name=name or f"range_{column}",
            severity=severity,
            description=f"Column {column} must be between {min_value} and {max_value}",
        )

    def validate(self, df: pl.DataFrame) -> ValidationResult:
        total_rows = len(df)
        col = df[self.column]

        # Build condition
        conditions = []
        if self.min_value is not None:
            if self.inclusive:
                conditions.append(col < self.min_value)
            else:
                conditions.append(col <= self.min_value)

        if self.max_value is not None:
            if self.inclusive:
                conditions.append(col > self.max_value)
            else:
                conditions.append(col >= self.max_value)

        if conditions:
            failing_mask = conditions[0]
            for cond in conditions[1:]:
                failing_mask = failing_mask | cond
            failing_rows = failing_mask.sum()
        else:
            failing_rows = 0

        return ValidationResult(
            rule_name=self.name,
            passed=failing_rows == 0,
            severity=self.severity,
            message=f"Found {failing_rows} values outside range" if failing_rows > 0 else "All values in range",
            failing_rows=failing_rows,
            total_rows=total_rows,
            details={
                "min_value": self.min_value,
                "max_value": self.max_value,
                "actual_min": col.min(),
                "actual_max": col.max(),
            },
        )


class RegexRule(ValidationRule):
    """Validate that string values match a regex pattern."""

    def __init__(
        self,
        column: str,
        pattern: str,
        name: str | None = None,
        severity: Severity = Severity.ERROR,
        negate: bool = False,
    ) -> None:
        self.column = column
        self.pattern = pattern
        self.negate = negate
        super().__init__(
            name=name or f"regex_{column}",
            severity=severity,
            description=f"Column {column} must {'not ' if negate else ''}match pattern {pattern}",
        )

    def validate(self, df: pl.DataFrame) -> ValidationResult:
        total_rows = len(df)

        # Apply regex match
        matches = df[self.column].str.contains(self.pattern)

        if self.negate:
            failing_rows = matches.sum()
        else:
            failing_rows = (~matches).sum()

        # Get sample failing values
        if self.negate:
            failing_values = df.filter(matches)[self.column].head(10).to_list()
        else:
            failing_values = df.filter(~matches)[self.column].head(10).to_list()

        return ValidationResult(
            rule_name=self.name,
            passed=failing_rows == 0,
            severity=self.severity,
            message=f"Found {failing_rows} values not matching pattern" if failing_rows > 0 else "All values match pattern",
            failing_rows=failing_rows,
            total_rows=total_rows,
            failing_values=failing_values,
        )


class InSetRule(ValidationRule):
    """Validate that values are in a specified set."""

    def __init__(
        self,
        column: str,
        allowed_values: list[Any],
        name: str | None = None,
        severity: Severity = Severity.ERROR,
        case_sensitive: bool = True,
    ) -> None:
        self.column = column
        self.allowed_values = allowed_values
        self.case_sensitive = case_sensitive
        super().__init__(
            name=name or f"in_set_{column}",
            severity=severity,
            description=f"Column {column} must be in {allowed_values[:5]}...",
        )

    def validate(self, df: pl.DataFrame) -> ValidationResult:
        total_rows = len(df)
        col = df[self.column]

        if not self.case_sensitive and col.dtype == pl.Utf8:
            col = col.str.to_lowercase()
            allowed = [v.lower() if isinstance(v, str) else v for v in self.allowed_values]
        else:
            allowed = self.allowed_values

        failing_mask = ~col.is_in(allowed)
        failing_rows = failing_mask.sum()

        # Get unique failing values
        failing_values = df.filter(failing_mask)[self.column].unique().head(10).to_list()

        return ValidationResult(
            rule_name=self.name,
            passed=failing_rows == 0,
            severity=self.severity,
            message=f"Found {failing_rows} values not in allowed set" if failing_rows > 0 else "All values in allowed set",
            failing_rows=failing_rows,
            total_rows=total_rows,
            failing_values=failing_values,
        )


class ForeignKeyRule(ValidationRule):
    """Validate foreign key relationships."""

    def __init__(
        self,
        column: str,
        reference_df: pl.DataFrame,
        reference_column: str,
        name: str | None = None,
        severity: Severity = Severity.ERROR,
    ) -> None:
        self.column = column
        self.reference_df = reference_df
        self.reference_column = reference_column
        super().__init__(
            name=name or f"fk_{column}",
            severity=severity,
            description=f"Column {column} must reference {reference_column}",
        )

    def validate(self, df: pl.DataFrame) -> ValidationResult:
        total_rows = len(df)

        # Get reference values
        ref_values = self.reference_df[self.reference_column].unique()

        # Check which values don't exist
        failing_mask = ~df[self.column].is_in(ref_values)
        failing_rows = failing_mask.sum()

        failing_values = df.filter(failing_mask)[self.column].unique().head(10).to_list()

        return ValidationResult(
            rule_name=self.name,
            passed=failing_rows == 0,
            severity=self.severity,
            message=f"Found {failing_rows} orphan records" if failing_rows > 0 else "All foreign keys valid",
            failing_rows=failing_rows,
            total_rows=total_rows,
            failing_values=failing_values,
        )


class CustomSQLRule(ValidationRule):
    """Validate using a custom SQL expression."""

    def __init__(
        self,
        expression: str,
        name: str,
        severity: Severity = Severity.ERROR,
        description: str = "",
    ) -> None:
        self.expression = expression
        super().__init__(
            name=name,
            severity=severity,
            description=description or f"Custom validation: {expression}",
        )

    def validate(self, df: pl.DataFrame) -> ValidationResult:
        total_rows = len(df)

        # Use SQL context to evaluate expression
        try:
            # The expression should return rows that FAIL the validation
            failing_df = df.filter(pl.sql_expr(self.expression))
            failing_rows = len(failing_df)
        except Exception as e:
            return ValidationResult(
                rule_name=self.name,
                passed=False,
                severity=self.severity,
                message=f"Expression error: {str(e)}",
                failing_rows=0,
                total_rows=total_rows,
            )

        return ValidationResult(
            rule_name=self.name,
            passed=failing_rows == 0,
            severity=self.severity,
            message=f"Found {failing_rows} failing rows" if failing_rows > 0 else "Validation passed",
            failing_rows=failing_rows,
            total_rows=total_rows,
        )


class SchemaRule(ValidationRule):
    """Validate DataFrame schema matches expected schema."""

    def __init__(
        self,
        expected_columns: dict[str, str | type],
        name: str = "schema_validation",
        severity: Severity = Severity.ERROR,
        allow_extra_columns: bool = True,
    ) -> None:
        self.expected_columns = expected_columns
        self.allow_extra_columns = allow_extra_columns
        super().__init__(
            name=name,
            severity=severity,
            description="Validate schema matches expected structure",
        )

    def validate(self, df: pl.DataFrame) -> ValidationResult:
        issues = []
        actual_schema = {col: str(df[col].dtype) for col in df.columns}

        # Check for missing columns
        for col, expected_type in self.expected_columns.items():
            if col not in actual_schema:
                issues.append(f"Missing column: {col}")
            else:
                actual_type = actual_schema[col]
                if isinstance(expected_type, str):
                    if expected_type.lower() not in actual_type.lower():
                        issues.append(f"Type mismatch for {col}: expected {expected_type}, got {actual_type}")

        # Check for extra columns
        if not self.allow_extra_columns:
            extra = set(actual_schema.keys()) - set(self.expected_columns.keys())
            if extra:
                issues.append(f"Unexpected columns: {extra}")

        return ValidationResult(
            rule_name=self.name,
            passed=len(issues) == 0,
            severity=self.severity,
            message="; ".join(issues) if issues else "Schema validation passed",
            failing_rows=0,
            total_rows=len(df),
            details={
                "expected_columns": list(self.expected_columns.keys()),
                "actual_columns": list(actual_schema.keys()),
                "issues": issues,
            },
        )


class CustomFunctionRule(ValidationRule):
    """Validate using a custom Python function."""

    def __init__(
        self,
        func: Callable[[pl.DataFrame], tuple[bool, str, int]],
        name: str,
        severity: Severity = Severity.ERROR,
        description: str = "",
    ) -> None:
        """
        Args:
            func: Function that takes DataFrame and returns (passed, message, failing_rows)
        """
        self.func = func
        super().__init__(
            name=name,
            severity=severity,
            description=description or "Custom function validation",
        )

    def validate(self, df: pl.DataFrame) -> ValidationResult:
        total_rows = len(df)

        try:
            passed, message, failing_rows = self.func(df)
        except Exception as e:
            return ValidationResult(
                rule_name=self.name,
                passed=False,
                severity=self.severity,
                message=f"Function error: {str(e)}",
                failing_rows=0,
                total_rows=total_rows,
            )

        return ValidationResult(
            rule_name=self.name,
            passed=passed,
            severity=self.severity,
            message=message,
            failing_rows=failing_rows,
            total_rows=total_rows,
        )
