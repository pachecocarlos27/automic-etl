"""Custom exceptions for Automic ETL."""

from typing import Any


class AutomicETLError(Exception):
    """Base exception for all Automic ETL errors."""

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        self.message = message
        self.details = details or {}
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message


class ConfigurationError(AutomicETLError):
    """Raised when there's a configuration error."""

    pass


class ConnectionError(AutomicETLError):
    """Raised when a connection fails."""

    def __init__(
        self,
        message: str,
        connector_type: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.connector_type = connector_type
        super().__init__(message, details)


class ExtractionError(AutomicETLError):
    """Raised when data extraction fails."""

    def __init__(
        self,
        message: str,
        source: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.source = source
        super().__init__(message, details)


class TransformationError(AutomicETLError):
    """Raised when data transformation fails."""

    def __init__(
        self,
        message: str,
        transformation: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.transformation = transformation
        super().__init__(message, details)


class LoadError(AutomicETLError):
    """Raised when data loading fails."""

    def __init__(
        self,
        message: str,
        target: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.target = target
        super().__init__(message, details)


class StorageError(AutomicETLError):
    """Raised when storage operations fail."""

    def __init__(
        self,
        message: str,
        provider: str | None = None,
        operation: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.provider = provider
        self.operation = operation
        super().__init__(message, details)


class IcebergError(AutomicETLError):
    """Raised when Iceberg operations fail."""

    def __init__(
        self,
        message: str,
        table: str | None = None,
        operation: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.table = table
        self.operation = operation
        super().__init__(message, details)


class LLMError(AutomicETLError):
    """Raised when LLM operations fail."""

    def __init__(
        self,
        message: str,
        provider: str | None = None,
        model: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.provider = provider
        self.model = model
        super().__init__(message, details)


class DataQualityError(AutomicETLError):
    """Raised when data quality checks fail."""

    def __init__(
        self,
        message: str,
        check_name: str | None = None,
        failed_records: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.check_name = check_name
        self.failed_records = failed_records
        super().__init__(message, details)


class SchemaError(AutomicETLError):
    """Raised when schema validation fails."""

    def __init__(
        self,
        message: str,
        expected_schema: dict[str, Any] | None = None,
        actual_schema: dict[str, Any] | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.expected_schema = expected_schema
        self.actual_schema = actual_schema
        super().__init__(message, details)


class WatermarkError(AutomicETLError):
    """Raised when watermark operations fail."""

    def __init__(
        self,
        message: str,
        watermark_column: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.watermark_column = watermark_column
        super().__init__(message, details)


class RetryExhaustedError(AutomicETLError):
    """Raised when all retry attempts are exhausted."""

    def __init__(
        self,
        message: str,
        attempts: int | None = None,
        last_error: Exception | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(message, details)
