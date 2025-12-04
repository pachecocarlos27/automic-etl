"""Core utilities for Automic ETL."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional


def utc_now() -> datetime:
    """
    Get current UTC time as timezone-aware datetime.

    This is the recommended replacement for datetime.utcnow() which is deprecated
    in Python 3.12+. Returns a timezone-aware datetime object.

    Returns:
        datetime: Current UTC time with timezone info
    """
    return datetime.now(timezone.utc)


def ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    """
    Ensure a datetime is timezone-aware in UTC.

    Args:
        dt: A datetime object (may be naive or timezone-aware)

    Returns:
        Timezone-aware datetime in UTC, or None if input is None
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        # Assume naive datetime is UTC
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_iso(dt: Optional[datetime] = None) -> str:
    """
    Format a datetime as ISO 8601 string.

    Args:
        dt: Datetime to format (uses current UTC time if None)

    Returns:
        ISO 8601 formatted string
    """
    if dt is None:
        dt = utc_now()
    return dt.isoformat()


def parse_iso(iso_string: str) -> datetime:
    """
    Parse an ISO 8601 datetime string.

    Args:
        iso_string: ISO 8601 formatted datetime string

    Returns:
        Timezone-aware datetime object
    """
    dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
    return ensure_utc(dt)


__all__ = [
    "utc_now",
    "ensure_utc",
    "format_iso",
    "parse_iso",
]
