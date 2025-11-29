"""Validation utilities for input parameters and configuration."""

from __future__ import annotations

import re
from typing import Any, Callable, TypeVar

import structlog

from automic_etl.core.exceptions import ConfigurationError

logger = structlog.get_logger()

T = TypeVar("T")


class ValidationError(ConfigurationError):
    """Raised when validation fails."""

    pass


def validate_table_name(table_name: str) -> None:
    """
    Validate table name format.

    Args:
        table_name: Table name to validate

    Raises:
        ValidationError: If table name is invalid
    """
    if not table_name or not isinstance(table_name, str):
        raise ValidationError("Table name must be a non-empty string")

    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table_name):
        raise ValidationError(
            f"Invalid table name '{table_name}'. "
            "Must start with letter or underscore, contain only alphanumeric and underscores."
        )

    if len(table_name) > 255:
        raise ValidationError(f"Table name too long (max 255 characters): {table_name}")


def validate_column_name(column_name: str) -> None:
    """
    Validate column name format.

    Args:
        column_name: Column name to validate

    Raises:
        ValidationError: If column name is invalid
    """
    if not column_name or not isinstance(column_name, str):
        raise ValidationError("Column name must be a non-empty string")

    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", column_name):
        raise ValidationError(
            f"Invalid column name '{column_name}'. "
            "Must start with letter or underscore, contain only alphanumeric and underscores."
        )

    if len(column_name) > 255:
        raise ValidationError(f"Column name too long (max 255 characters): {column_name}")


def validate_non_empty_string(value: Any, field_name: str) -> str:
    """
    Validate that value is a non-empty string.

    Args:
        value: Value to validate
        field_name: Name of field for error messages

    Returns:
        The validated string

    Raises:
        ValidationError: If value is not a non-empty string
    """
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{field_name} must be a non-empty string, got: {value!r}")

    return value.strip()


def validate_positive_int(value: Any, field_name: str, allow_zero: bool = False) -> int:
    """
    Validate that value is a positive integer.

    Args:
        value: Value to validate
        field_name: Name of field for error messages
        allow_zero: Whether zero is allowed

    Returns:
        The validated integer

    Raises:
        ValidationError: If value is not a positive integer
    """
    if not isinstance(value, int) or value < 0:
        raise ValidationError(f"{field_name} must be a positive integer, got: {value!r}")

    if not allow_zero and value == 0:
        raise ValidationError(f"{field_name} cannot be zero, got: {value}")

    return value


def validate_dict_keys(
    data: Any,
    required_keys: set[str] | None = None,
    allowed_keys: set[str] | None = None,
) -> dict[str, Any]:
    """
    Validate dictionary structure.

    Args:
        data: Dictionary to validate
        required_keys: Keys that must be present
        allowed_keys: Keys that are allowed (if specified, only these are allowed)

    Returns:
        The validated dictionary

    Raises:
        ValidationError: If validation fails
    """
    if not isinstance(data, dict):
        raise ValidationError(f"Expected dict, got {type(data).__name__}")

    required_keys = required_keys or set()
    missing = required_keys - set(data.keys())
    if missing:
        raise ValidationError(f"Missing required keys: {', '.join(sorted(missing))}")

    if allowed_keys is not None:
        extra = set(data.keys()) - allowed_keys
        if extra:
            raise ValidationError(f"Unexpected keys: {', '.join(sorted(extra))}")

    return data


def validate_in_choices(
    value: Any,
    choices: set[str] | list[str],
    field_name: str,
) -> str:
    """
    Validate that value is in allowed choices.

    Args:
        value: Value to validate
        choices: Allowed choices
        field_name: Name of field for error messages

    Returns:
        The validated value

    Raises:
        ValidationError: If value is not in choices
    """
    if value not in choices:
        raise ValidationError(
            f"Invalid {field_name}: {value!r}. Must be one of: {', '.join(str(c) for c in choices)}"
        )

    return value


def validate_batch_size(batch_size: int) -> int:
    """
    Validate batch size.

    Args:
        batch_size: Batch size to validate

    Returns:
        The validated batch size

    Raises:
        ValidationError: If batch size is invalid
    """
    if not isinstance(batch_size, int):
        raise ValidationError(f"Batch size must be an integer, got: {type(batch_size).__name__}")

    if batch_size < 1:
        raise ValidationError(f"Batch size must be >= 1, got: {batch_size}")

    if batch_size > 1_000_000:
        raise ValidationError(f"Batch size too large (max 1,000,000), got: {batch_size}")

    return batch_size


def validate_list_items(
    items: Any,
    item_validator: Callable[[Any], None],
    field_name: str,
    allow_empty: bool = True,
) -> list[Any]:
    """
    Validate items in a list.

    Args:
        items: List to validate
        item_validator: Function to validate each item
        field_name: Name of field for error messages
        allow_empty: Whether empty list is allowed

    Returns:
        The validated list

    Raises:
        ValidationError: If validation fails
    """
    if not isinstance(items, list):
        raise ValidationError(
            f"{field_name} must be a list, got: {type(items).__name__}"
        )

    if not allow_empty and len(items) == 0:
        raise ValidationError(f"{field_name} cannot be empty")

    for idx, item in enumerate(items):
        try:
            item_validator(item)
        except ValidationError as e:
            raise ValidationError(f"{field_name}[{idx}]: {str(e)}")

    return items
