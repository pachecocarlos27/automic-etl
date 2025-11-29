"""Base storage interface for cloud providers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO, Iterator

import polars as pl


@dataclass
class StorageObject:
    """Represents an object in cloud storage."""

    key: str
    size: int
    last_modified: datetime
    etag: str | None = None
    content_type: str | None = None
    metadata: dict[str, str] | None = None

    @property
    def name(self) -> str:
        """Get the object name (filename)."""
        return Path(self.key).name

    @property
    def extension(self) -> str:
        """Get the file extension."""
        return Path(self.key).suffix.lower()

    @property
    def parent(self) -> str:
        """Get the parent path."""
        return str(Path(self.key).parent)


class StorageBackend(ABC):
    """Abstract base class for cloud storage backends."""

    def __init__(self, bucket: str, prefix: str = "") -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")

    def _full_path(self, path: str) -> str:
        """Get the full path including prefix."""
        path = path.strip("/")
        if self.prefix:
            return f"{self.prefix}/{path}"
        return path

    # =========================================================================
    # Object Operations
    # =========================================================================

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if an object exists."""
        pass

    @abstractmethod
    def get(self, path: str) -> bytes:
        """Get object contents as bytes."""
        pass

    @abstractmethod
    def put(
        self,
        path: str,
        data: bytes | BinaryIO,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> str:
        """Put an object and return its URI."""
        pass

    @abstractmethod
    def delete(self, path: str) -> None:
        """Delete an object."""
        pass

    @abstractmethod
    def copy(self, source_path: str, dest_path: str) -> str:
        """Copy an object within the same bucket."""
        pass

    @abstractmethod
    def move(self, source_path: str, dest_path: str) -> str:
        """Move an object within the same bucket."""
        pass

    # =========================================================================
    # Listing Operations
    # =========================================================================

    @abstractmethod
    def list_objects(
        self,
        prefix: str = "",
        recursive: bool = True,
        max_results: int | None = None,
    ) -> Iterator[StorageObject]:
        """List objects with optional prefix filter."""
        pass

    @abstractmethod
    def list_prefixes(self, prefix: str = "") -> list[str]:
        """List 'directories' at the given prefix level."""
        pass

    # =========================================================================
    # Metadata Operations
    # =========================================================================

    @abstractmethod
    def get_metadata(self, path: str) -> StorageObject:
        """Get object metadata without downloading content."""
        pass

    @abstractmethod
    def set_metadata(self, path: str, metadata: dict[str, str]) -> None:
        """Set/update object metadata."""
        pass

    # =========================================================================
    # URL Operations
    # =========================================================================

    @abstractmethod
    def get_uri(self, path: str) -> str:
        """Get the full URI for an object (e.g., s3://bucket/key)."""
        pass

    @abstractmethod
    def generate_presigned_url(
        self,
        path: str,
        expiration: int = 3600,
        operation: str = "get",
    ) -> str:
        """Generate a presigned URL for temporary access."""
        pass

    # =========================================================================
    # Polars Integration
    # =========================================================================

    def read_parquet(
        self,
        path: str,
        columns: list[str] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame:
        """Read a Parquet file into a Polars DataFrame."""
        uri = self.get_uri(path)
        return pl.read_parquet(uri, columns=columns, **kwargs)

    def write_parquet(
        self,
        df: pl.DataFrame,
        path: str,
        compression: str = "zstd",
        **kwargs: Any,
    ) -> str:
        """Write a Polars DataFrame to Parquet."""
        uri = self.get_uri(path)
        df.write_parquet(uri, compression=compression, **kwargs)
        return uri

    def read_csv(
        self,
        path: str,
        **kwargs: Any,
    ) -> pl.DataFrame:
        """Read a CSV file into a Polars DataFrame."""
        data = self.get(path)
        return pl.read_csv(data, **kwargs)

    def write_csv(
        self,
        df: pl.DataFrame,
        path: str,
        **kwargs: Any,
    ) -> str:
        """Write a Polars DataFrame to CSV."""
        csv_bytes = df.write_csv().encode("utf-8")
        return self.put(path, csv_bytes, content_type="text/csv")

    def read_json(
        self,
        path: str,
        **kwargs: Any,
    ) -> pl.DataFrame:
        """Read a JSON file into a Polars DataFrame."""
        data = self.get(path)
        return pl.read_json(data, **kwargs)

    def write_json(
        self,
        df: pl.DataFrame,
        path: str,
        **kwargs: Any,
    ) -> str:
        """Write a Polars DataFrame to JSON."""
        json_bytes = df.write_json().encode("utf-8")
        return self.put(path, json_bytes, content_type="application/json")

    # =========================================================================
    # Batch Operations
    # =========================================================================

    def delete_prefix(self, prefix: str) -> int:
        """Delete all objects with the given prefix. Returns count of deleted objects."""
        count = 0
        for obj in self.list_objects(prefix):
            self.delete(obj.key)
            count += 1
        return count

    def copy_prefix(self, source_prefix: str, dest_prefix: str) -> int:
        """Copy all objects from one prefix to another. Returns count of copied objects."""
        count = 0
        for obj in self.list_objects(source_prefix):
            relative_path = obj.key[len(self._full_path(source_prefix)) :].lstrip("/")
            dest_path = f"{dest_prefix}/{relative_path}"
            self.copy(obj.key, dest_path)
            count += 1
        return count

    # =========================================================================
    # Context Manager
    # =========================================================================

    def __enter__(self) -> "StorageBackend":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass
