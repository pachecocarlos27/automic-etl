"""Watermark management for incremental extraction."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl
import structlog

from automic_etl.core.config import Settings
from automic_etl.core.utils import utc_now

logger = structlog.get_logger()


@dataclass
class Watermark:
    """Represents a watermark for a data source."""

    source_name: str
    column: str
    value: Any
    updated_at: datetime
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "source_name": self.source_name,
            "column": self.column,
            "value": self._serialize_value(self.value),
            "value_type": type(self.value).__name__,
            "updated_at": self.updated_at.isoformat(),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Watermark":
        """Create from dictionary."""
        value = data["value"]
        value_type = data.get("value_type", "str")

        # Deserialize value based on type
        if value_type == "datetime":
            value = datetime.fromisoformat(value)
        elif value_type == "int":
            value = int(value)
        elif value_type == "float":
            value = float(value)

        return cls(
            source_name=data["source_name"],
            column=data["column"],
            value=value,
            updated_at=datetime.fromisoformat(data["updated_at"]),
            metadata=data.get("metadata", {}),
        )

    def _serialize_value(self, value: Any) -> str:
        """Serialize value for storage."""
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)


class WatermarkManager:
    """
    Manage watermarks for incremental extraction.

    Supports:
    - Local file storage
    - Iceberg table storage
    - In-memory storage for testing
    """

    def __init__(
        self,
        settings: Settings,
        storage_type: str = "file",
        storage_path: str | None = None,
    ) -> None:
        self.settings = settings
        self.storage_type = storage_type
        self.storage_path = storage_path or ".automic/watermarks.json"
        self.logger = logger.bind(component="watermark_manager")
        self._watermarks: dict[str, Watermark] = {}
        self._load()

    def _load(self) -> None:
        """Load watermarks from storage."""
        if self.storage_type == "file":
            self._load_from_file()
        elif self.storage_type == "memory":
            pass  # In-memory, nothing to load
        # Iceberg storage can be added

    def _load_from_file(self) -> None:
        """Load watermarks from JSON file."""
        path = Path(self.storage_path)
        if path.exists():
            try:
                with open(path) as f:
                    data = json.load(f)
                for key, wm_data in data.items():
                    self._watermarks[key] = Watermark.from_dict(wm_data)
                self.logger.debug(
                    "Loaded watermarks",
                    count=len(self._watermarks),
                )
            except Exception as e:
                self.logger.warning(f"Failed to load watermarks: {e}")

    def _save(self) -> None:
        """Save watermarks to storage."""
        if self.storage_type == "file":
            self._save_to_file()

    def _save_to_file(self) -> None:
        """Save watermarks to JSON file."""
        path = Path(self.storage_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        data = {key: wm.to_dict() for key, wm in self._watermarks.items()}

        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    def get(self, source_name: str) -> Watermark | None:
        """
        Get watermark for a source.

        Args:
            source_name: Data source identifier

        Returns:
            Watermark if exists, None otherwise
        """
        return self._watermarks.get(source_name)

    def get_value(self, source_name: str) -> Any | None:
        """
        Get just the watermark value.

        Args:
            source_name: Data source identifier

        Returns:
            Watermark value if exists
        """
        wm = self.get(source_name)
        return wm.value if wm else None

    def set(
        self,
        source_name: str,
        column: str,
        value: Any,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Set watermark for a source.

        Args:
            source_name: Data source identifier
            column: Watermark column name
            value: Watermark value
            metadata: Optional metadata
        """
        self._watermarks[source_name] = Watermark(
            source_name=source_name,
            column=column,
            value=value,
            updated_at=utc_now(),
            metadata=metadata or {},
        )
        self._save()

        self.logger.info(
            "Updated watermark",
            source=source_name,
            column=column,
            value=str(value)[:50],
        )

    def update_from_dataframe(
        self,
        source_name: str,
        df: pl.DataFrame,
        watermark_column: str,
    ) -> Any | None:
        """
        Update watermark from DataFrame's max value.

        Args:
            source_name: Data source identifier
            df: DataFrame to get watermark from
            watermark_column: Column containing watermark values

        Returns:
            New watermark value
        """
        if df.is_empty() or watermark_column not in df.columns:
            return None

        new_value = df.select(pl.col(watermark_column).max()).item()

        if new_value is not None:
            current = self.get_value(source_name)
            if current is None or new_value > current:
                self.set(source_name, watermark_column, new_value)
                return new_value

        return self.get_value(source_name)

    def delete(self, source_name: str) -> bool:
        """
        Delete watermark for a source.

        Args:
            source_name: Data source identifier

        Returns:
            True if deleted, False if not found
        """
        if source_name in self._watermarks:
            del self._watermarks[source_name]
            self._save()
            return True
        return False

    def list_sources(self) -> list[str]:
        """List all sources with watermarks."""
        return list(self._watermarks.keys())

    def list_all(self) -> list[Watermark]:
        """List all watermarks."""
        return list(self._watermarks.values())

    def clear(self) -> None:
        """Clear all watermarks."""
        self._watermarks.clear()
        self._save()

    def export(self) -> dict[str, Any]:
        """Export all watermarks as dictionary."""
        return {key: wm.to_dict() for key, wm in self._watermarks.items()}

    def import_watermarks(self, data: dict[str, Any]) -> None:
        """Import watermarks from dictionary."""
        for key, wm_data in data.items():
            self._watermarks[key] = Watermark.from_dict(wm_data)
        self._save()
