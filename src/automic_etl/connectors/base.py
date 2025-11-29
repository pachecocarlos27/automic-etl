"""Base connector interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Iterator

import polars as pl
import structlog

from automic_etl.core.exceptions import ConnectionError, ExtractionError

logger = structlog.get_logger()


class ConnectorType(str, Enum):
    """Types of connectors."""

    DATABASE = "database"
    FILE = "file"
    API = "api"
    STREAMING = "streaming"
    UNSTRUCTURED = "unstructured"


@dataclass
class ConnectorConfig:
    """Base configuration for connectors."""

    name: str
    connector_type: ConnectorType
    batch_size: int = 10000
    timeout: int = 30
    retry_attempts: int = 3
    retry_delay: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExtractionResult:
    """Result of an extraction operation."""

    data: pl.DataFrame
    row_count: int
    watermark: Any | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    extraction_time: datetime = field(default_factory=datetime.utcnow)


class BaseConnector(ABC):
    """Abstract base class for all data connectors."""

    connector_type: ConnectorType = ConnectorType.DATABASE

    def __init__(self, config: ConnectorConfig) -> None:
        self.config = config
        self.logger = logger.bind(
            connector=config.name,
            connector_type=self.connector_type.value,
        )
        self._connected = False

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the data source."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close the connection."""
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Test if the connection is valid."""
        pass

    @abstractmethod
    def extract(
        self,
        query: str | None = None,
        **kwargs: Any,
    ) -> ExtractionResult:
        """Extract data from the source."""
        pass

    def extract_batch(
        self,
        query: str | None = None,
        batch_size: int | None = None,
        **kwargs: Any,
    ) -> Iterator[ExtractionResult]:
        """Extract data in batches."""
        batch_size = batch_size or self.config.batch_size
        offset = 0

        while True:
            result = self.extract(
                query=query,
                limit=batch_size,
                offset=offset,
                **kwargs,
            )

            if result.row_count == 0:
                break

            yield result
            offset += batch_size

            if result.row_count < batch_size:
                break

    def extract_incremental(
        self,
        watermark_column: str,
        last_watermark: Any | None = None,
        **kwargs: Any,
    ) -> ExtractionResult:
        """Extract data incrementally based on watermark."""
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support incremental extraction"
        )

    @property
    def is_connected(self) -> bool:
        """Check if connector is connected."""
        return self._connected

    def __enter__(self) -> "BaseConnector":
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.disconnect()

    def _validate_connection(self) -> None:
        """Ensure connector is connected."""
        if not self._connected:
            raise ConnectionError(
                "Connector is not connected",
                connector_type=self.connector_type.value,
            )


class DatabaseConnector(BaseConnector):
    """Base class for database connectors."""

    connector_type = ConnectorType.DATABASE

    @abstractmethod
    def get_tables(self) -> list[str]:
        """List available tables."""
        pass

    @abstractmethod
    def get_table_schema(self, table: str) -> dict[str, str]:
        """Get schema for a table."""
        pass

    @abstractmethod
    def get_row_count(self, table: str) -> int:
        """Get row count for a table."""
        pass

    def extract_table(
        self,
        table: str,
        columns: list[str] | None = None,
        filter_expr: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> ExtractionResult:
        """Extract data from a specific table."""
        # Build query
        cols = ", ".join(columns) if columns else "*"
        query = f"SELECT {cols} FROM {table}"

        if filter_expr:
            query += f" WHERE {filter_expr}"

        if limit:
            query += f" LIMIT {limit}"

        if offset:
            query += f" OFFSET {offset}"

        return self.extract(query=query)


class FileConnector(BaseConnector):
    """Base class for file-based connectors."""

    connector_type = ConnectorType.FILE

    @abstractmethod
    def list_files(self, pattern: str | None = None) -> list[str]:
        """List available files."""
        pass

    @abstractmethod
    def read_file(self, path: str) -> ExtractionResult:
        """Read a single file."""
        pass

    def read_files(
        self,
        paths: list[str],
        parallel: bool = True,
    ) -> ExtractionResult:
        """Read multiple files and combine."""
        dfs = []
        total_rows = 0

        for path in paths:
            result = self.read_file(path)
            dfs.append(result.data)
            total_rows += result.row_count

        combined = pl.concat(dfs) if dfs else pl.DataFrame()

        return ExtractionResult(
            data=combined,
            row_count=total_rows,
            metadata={"files": paths, "file_count": len(paths)},
        )


class APIConnector(BaseConnector):
    """Base class for API connectors."""

    connector_type = ConnectorType.API

    @abstractmethod
    def get_endpoints(self) -> list[str]:
        """List available API endpoints."""
        pass

    @abstractmethod
    def fetch(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> ExtractionResult:
        """Fetch data from an endpoint."""
        pass

    def fetch_paginated(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        page_size: int = 100,
        max_pages: int | None = None,
    ) -> Iterator[ExtractionResult]:
        """Fetch data with pagination."""
        params = params or {}
        page = 1

        while max_pages is None or page <= max_pages:
            params["page"] = page
            params["per_page"] = page_size

            result = self.fetch(endpoint, params)

            if result.row_count == 0:
                break

            yield result
            page += 1

            if result.row_count < page_size:
                break


class UnstructuredConnector(BaseConnector):
    """Base class for unstructured data connectors."""

    connector_type = ConnectorType.UNSTRUCTURED

    @abstractmethod
    def extract_content(self, source: str | bytes) -> dict[str, Any]:
        """Extract content from unstructured source."""
        pass

    @abstractmethod
    def extract_metadata(self, source: str | bytes) -> dict[str, Any]:
        """Extract metadata from unstructured source."""
        pass

    def process(
        self,
        source: str | bytes,
        extract_text: bool = True,
        extract_tables: bool = True,
        extract_images: bool = False,
    ) -> ExtractionResult:
        """Process unstructured data."""
        content = self.extract_content(source)
        metadata = self.extract_metadata(source)

        data = {
            "content": [content.get("text", "")],
            "content_type": [content.get("type", "unknown")],
        }

        if extract_tables and "tables" in content:
            data["tables"] = [content["tables"]]

        # Convert to DataFrame
        df = pl.DataFrame(data)

        return ExtractionResult(
            data=df,
            row_count=1,
            metadata=metadata,
        )
