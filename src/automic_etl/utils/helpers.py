"""Helper utilities for Automic ETL."""

from __future__ import annotations

import re
import uuid
from datetime import timedelta
from typing import Any, Generator, Iterable, TypeVar

T = TypeVar("T")


def generate_id(prefix: str = "") -> str:
    """Generate a unique identifier."""
    uid = str(uuid.uuid4())[:8]
    if prefix:
        return f"{prefix}_{uid}"
    return uid


def parse_size(size_str: str) -> int:
    """
    Parse a size string to bytes.

    Examples:
        parse_size("1GB") -> 1073741824
        parse_size("512MB") -> 536870912
        parse_size("1024KB") -> 1048576
    """
    units = {
        "B": 1,
        "KB": 1024,
        "MB": 1024**2,
        "GB": 1024**3,
        "TB": 1024**4,
    }

    pattern = r"^(\d+(?:\.\d+)?)\s*([A-Z]{1,2})$"
    match = re.match(pattern, size_str.upper().strip())

    if not match:
        raise ValueError(f"Invalid size format: {size_str}")

    value = float(match.group(1))
    unit = match.group(2)

    if unit not in units:
        raise ValueError(f"Unknown unit: {unit}")

    return int(value * units[unit])


def format_size(bytes_size: int) -> str:
    """
    Format bytes to human-readable size.

    Examples:
        format_size(1073741824) -> "1.00 GB"
        format_size(536870912) -> "512.00 MB"
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(bytes_size) < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"


def parse_duration(duration_str: str) -> timedelta:
    """
    Parse a duration string to timedelta.

    Examples:
        parse_duration("1 hour") -> timedelta(hours=1)
        parse_duration("30 minutes") -> timedelta(minutes=30)
        parse_duration("2 days") -> timedelta(days=2)
    """
    pattern = r"^(\d+)\s*(second|minute|hour|day|week)s?$"
    match = re.match(pattern, duration_str.lower().strip())

    if not match:
        raise ValueError(f"Invalid duration format: {duration_str}")

    value = int(match.group(1))
    unit = match.group(2)

    unit_mapping = {
        "second": "seconds",
        "minute": "minutes",
        "hour": "hours",
        "day": "days",
        "week": "weeks",
    }

    return timedelta(**{unit_mapping[unit]: value})


def chunk_iterable(
    iterable: Iterable[T],
    chunk_size: int,
) -> Generator[list[T], None, None]:
    """
    Split an iterable into chunks of a specified size.

    Examples:
        list(chunk_iterable([1,2,3,4,5], 2)) -> [[1,2], [3,4], [5]]
    """
    chunk: list[T] = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def flatten_dict(
    d: dict[str, Any],
    parent_key: str = "",
    sep: str = ".",
) -> dict[str, Any]:
    """
    Flatten a nested dictionary.

    Examples:
        flatten_dict({"a": {"b": 1}}) -> {"a.b": 1}
    """
    items: list[tuple[str, Any]] = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def unflatten_dict(d: dict[str, Any], sep: str = ".") -> dict[str, Any]:
    """
    Unflatten a dictionary with dotted keys.

    Examples:
        unflatten_dict({"a.b": 1}) -> {"a": {"b": 1}}
    """
    result: dict[str, Any] = {}
    for key, value in d.items():
        parts = key.split(sep)
        current = result
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value
    return result


def safe_get(d: dict[str, Any], *keys: str, default: Any = None) -> Any:
    """
    Safely get a nested value from a dictionary.

    Examples:
        safe_get({"a": {"b": 1}}, "a", "b") -> 1
        safe_get({"a": {}}, "a", "b", default=0) -> 0
    """
    current = d
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current


def merge_dicts(*dicts: dict[str, Any]) -> dict[str, Any]:
    """
    Deep merge multiple dictionaries.

    Later dictionaries take precedence over earlier ones.
    """
    result: dict[str, Any] = {}
    for d in dicts:
        for key, value in d.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = merge_dicts(result[key], value)
            else:
                result[key] = value
    return result


def sanitize_column_name(name: str) -> str:
    """
    Sanitize a column name for use in databases/Iceberg.

    Converts to lowercase, replaces spaces and special chars with underscores.
    """
    # Remove leading/trailing whitespace
    name = name.strip()
    # Replace spaces and special characters with underscores
    name = re.sub(r"[^\w]", "_", name)
    # Remove consecutive underscores
    name = re.sub(r"_+", "_", name)
    # Remove leading/trailing underscores
    name = name.strip("_")
    # Ensure starts with letter or underscore
    if name and name[0].isdigit():
        name = f"col_{name}"
    return name.lower()


def infer_mime_type(file_path: str) -> str:
    """Infer MIME type from file extension."""
    extension_mapping = {
        ".csv": "text/csv",
        ".json": "application/json",
        ".jsonl": "application/jsonlines",
        ".parquet": "application/parquet",
        ".avro": "application/avro",
        ".orc": "application/orc",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".xls": "application/vnd.ms-excel",
        ".pdf": "application/pdf",
        ".txt": "text/plain",
        ".xml": "application/xml",
        ".html": "text/html",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".doc": "application/msword",
        ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        ".mp3": "audio/mpeg",
        ".mp4": "video/mp4",
        ".wav": "audio/wav",
    }

    for ext, mime_type in extension_mapping.items():
        if file_path.lower().endswith(ext):
            return mime_type

    return "application/octet-stream"
