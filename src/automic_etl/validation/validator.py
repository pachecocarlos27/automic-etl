"""Data validator for executing validation rules."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from datetime import datetime

import polars as pl
import structlog

from automic_etl.validation.rules import ValidationRule, ValidationResult, Severity

logger = structlog.get_logger()


@dataclass
class ValidationReport:
    """Complete validation report for a dataset."""
    dataset_name: str
    validation_time: datetime
    total_rules: int
    passed_rules: int
    failed_rules: int
    warning_rules: int
    total_rows: int
    results: list[ValidationResult]
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_valid(self) -> bool:
        """Check if all error-level rules passed."""
        return all(
            r.passed for r in self.results
            if r.severity == Severity.ERROR
        )

    @property
    def pass_rate(self) -> float:
        """Overall pass rate percentage."""
        if self.total_rules == 0:
            return 100.0
        return (self.passed_rules / self.total_rules) * 100

    def to_dict(self) -> dict[str, Any]:
        """Convert report to dictionary."""
        return {
            "dataset_name": self.dataset_name,
            "validation_time": self.validation_time.isoformat(),
            "summary": {
                "total_rules": self.total_rules,
                "passed": self.passed_rules,
                "failed": self.failed_rules,
                "warnings": self.warning_rules,
                "pass_rate": self.pass_rate,
                "is_valid": self.is_valid,
            },
            "total_rows": self.total_rows,
            "results": [
                {
                    "rule": r.rule_name,
                    "passed": r.passed,
                    "severity": r.severity.value,
                    "message": r.message,
                    "failing_rows": r.failing_rows,
                    "pass_rate": r.pass_rate,
                }
                for r in self.results
            ],
            "metadata": self.metadata,
        }

    def to_dataframe(self) -> pl.DataFrame:
        """Convert results to DataFrame."""
        return pl.DataFrame([
            {
                "rule_name": r.rule_name,
                "passed": r.passed,
                "severity": r.severity.value,
                "message": r.message,
                "failing_rows": r.failing_rows,
                "total_rows": r.total_rows,
                "pass_rate": r.pass_rate,
            }
            for r in self.results
        ])


class DataValidator:
    """
    Data validation engine.

    Features:
    - Register and execute validation rules
    - Generate validation reports
    - Support fail-fast or complete validation
    - Configurable thresholds
    """

    def __init__(
        self,
        name: str = "default_validator",
        fail_fast: bool = False,
        error_threshold: float | None = None,
    ) -> None:
        """
        Initialize validator.

        Args:
            name: Validator name
            fail_fast: Stop on first failure
            error_threshold: Maximum allowed error percentage (0-100)
        """
        self.name = name
        self.fail_fast = fail_fast
        self.error_threshold = error_threshold
        self.rules: list[ValidationRule] = []
        self.logger = logger.bind(validator=name)

    def add_rule(self, rule: ValidationRule) -> "DataValidator":
        """Add a validation rule."""
        self.rules.append(rule)
        return self

    def add_rules(self, rules: list[ValidationRule]) -> "DataValidator":
        """Add multiple validation rules."""
        self.rules.extend(rules)
        return self

    def clear_rules(self) -> "DataValidator":
        """Remove all rules."""
        self.rules = []
        return self

    def validate(
        self,
        df: pl.DataFrame,
        dataset_name: str = "unnamed",
    ) -> ValidationReport:
        """
        Validate a DataFrame against all rules.

        Args:
            df: DataFrame to validate
            dataset_name: Name for the dataset

        Returns:
            ValidationReport with all results
        """
        self.logger.info(
            "Starting validation",
            dataset=dataset_name,
            rules=len(self.rules),
            rows=len(df),
        )

        results = []
        passed = 0
        failed = 0
        warnings = 0

        for rule in self.rules:
            try:
                result = rule.validate(df)
                results.append(result)

                if result.passed:
                    passed += 1
                elif result.severity == Severity.WARNING:
                    warnings += 1
                else:
                    failed += 1

                self.logger.debug(
                    "Rule executed",
                    rule=rule.name,
                    passed=result.passed,
                    failing_rows=result.failing_rows,
                )

                if self.fail_fast and not result.passed and result.severity == Severity.ERROR:
                    self.logger.warning("Validation stopped (fail-fast)", rule=rule.name)
                    break

            except Exception as e:
                self.logger.error("Rule execution failed", rule=rule.name, error=str(e))
                results.append(ValidationResult(
                    rule_name=rule.name,
                    passed=False,
                    severity=Severity.ERROR,
                    message=f"Rule execution error: {str(e)}",
                    total_rows=len(df),
                ))
                failed += 1

        report = ValidationReport(
            dataset_name=dataset_name,
            validation_time=datetime.utcnow(),
            total_rules=len(self.rules),
            passed_rules=passed,
            failed_rules=failed,
            warning_rules=warnings,
            total_rows=len(df),
            results=results,
        )

        # Check threshold
        if self.error_threshold is not None:
            error_rate = (failed / len(self.rules)) * 100 if self.rules else 0
            if error_rate > self.error_threshold:
                self.logger.error(
                    "Error threshold exceeded",
                    error_rate=error_rate,
                    threshold=self.error_threshold,
                )

        self.logger.info(
            "Validation complete",
            dataset=dataset_name,
            passed=passed,
            failed=failed,
            warnings=warnings,
            is_valid=report.is_valid,
        )

        return report

    def validate_and_filter(
        self,
        df: pl.DataFrame,
        dataset_name: str = "unnamed",
    ) -> tuple[pl.DataFrame, pl.DataFrame, ValidationReport]:
        """
        Validate and separate valid/invalid rows.

        Args:
            df: DataFrame to validate
            dataset_name: Name for the dataset

        Returns:
            Tuple of (valid_df, invalid_df, report)
        """
        report = self.validate(df, dataset_name)

        # Build combined filter for invalid rows
        invalid_mask = pl.lit(False)

        for result in report.results:
            if not result.passed and result.severity == Severity.ERROR:
                # Try to reconstruct the failing rows filter
                # This requires rules to provide enough info
                pass

        # For now, return all rows in valid (this needs rule-specific logic)
        return df, pl.DataFrame(), report

    @classmethod
    def common_rules(
        cls,
        df: pl.DataFrame,
        not_null_columns: list[str] | None = None,
        unique_columns: list[str] | None = None,
    ) -> "DataValidator":
        """
        Create validator with common rules inferred from DataFrame.

        Args:
            df: DataFrame to analyze
            not_null_columns: Columns that should not have nulls
            unique_columns: Columns that should be unique
        """
        from automic_etl.validation.rules import NotNullRule, UniqueRule

        validator = cls("auto_validator")

        if not_null_columns:
            for col in not_null_columns:
                if col in df.columns:
                    validator.add_rule(NotNullRule(col))

        if unique_columns:
            for col in unique_columns:
                if col in df.columns:
                    validator.add_rule(UniqueRule(col))

        return validator


class ValidationPipeline:
    """Pipeline for chaining multiple validators."""

    def __init__(self, name: str = "validation_pipeline") -> None:
        self.name = name
        self.validators: list[tuple[str, DataValidator, bool]] = []

    def add_stage(
        self,
        name: str,
        validator: DataValidator,
        required: bool = True,
    ) -> "ValidationPipeline":
        """
        Add a validation stage.

        Args:
            name: Stage name
            validator: Validator to run
            required: If True, pipeline fails if this stage fails
        """
        self.validators.append((name, validator, required))
        return self

    def run(
        self,
        df: pl.DataFrame,
        dataset_name: str = "unnamed",
    ) -> dict[str, ValidationReport]:
        """
        Run all validation stages.

        Args:
            df: DataFrame to validate
            dataset_name: Name for the dataset

        Returns:
            Dictionary mapping stage names to reports
        """
        reports = {}

        for stage_name, validator, required in self.validators:
            report = validator.validate(df, f"{dataset_name}_{stage_name}")
            reports[stage_name] = report

            if required and not report.is_valid:
                break

        return reports
