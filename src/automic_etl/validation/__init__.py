"""Data validation framework for Automic ETL."""

from automic_etl.validation.rules import (
    ValidationRule,
    NotNullRule,
    UniqueRule,
    RangeRule,
    RegexRule,
    InSetRule,
    ForeignKeyRule,
    CustomSQLRule,
    SchemaRule,
)
from automic_etl.validation.validator import DataValidator, ValidationResult
from automic_etl.validation.quality import DataQualityChecker, QualityReport

__all__ = [
    "ValidationRule",
    "NotNullRule",
    "UniqueRule",
    "RangeRule",
    "RegexRule",
    "InSetRule",
    "ForeignKeyRule",
    "CustomSQLRule",
    "SchemaRule",
    "DataValidator",
    "ValidationResult",
    "DataQualityChecker",
    "QualityReport",
]
