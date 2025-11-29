"""JSON file connector."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from automic_etl.connectors.base import (
    ConnectorConfig,
    ConnectorType,
    ExtractionResult,
    FileConnector,
)
from automic_etl.core.exceptions import ExtractionError


@dataclass
class JSONConfig(ConnectorConfig):
    """JSON connector configuration."""

    path: str = ""
    json_lines: bool = False  # True for JSONL format
    encoding: str = "utf-8"

    def __post_init__(self) -> None:
        self.connector_type = ConnectorType.FILE


class JSONConnector(FileConnector):
    """JSON/JSONL file connector."""

    def __init__(self, config: JSONConfig) -> None:
        super().__init__(config)
        self.json_config = config

    def connect(self) -> None:
        """Verify path exists."""
        self._connected = True
        self.logger.info("JSON connector ready", path=self.json_config.path)

    def disconnect(self) -> None:
        """No persistent connection to close."""
        self._connected = False

    def test_connection(self) -> bool:
        """Test if path is accessible."""
        path = Path(self.json_config.path)
        return path.exists() or path.parent.exists()

    def extract(
        self,
        query: str | None = None,
        path: str | None = None,
        json_path: str | None = None,
        **kwargs: Any,
    ) -> ExtractionResult:
        """Extract data from JSON file(s)."""
        file_path = path or self.json_config.path

        try:
            if self.json_config.json_lines or file_path.endswith(".jsonl"):
                df = pl.read_ndjson(file_path)
            else:
                # Read JSON file
                with open(file_path, "r", encoding=self.json_config.encoding) as f:
                    data = json.load(f)

                # Handle different JSON structures
                if isinstance(data, list):
                    df = pl.DataFrame(data)
                elif isinstance(data, dict):
                    # Check if there's a specific path to extract
                    if json_path:
                        for key in json_path.split("."):
                            data = data[key]
                    if isinstance(data, list):
                        df = pl.DataFrame(data)
                    else:
                        df = pl.DataFrame([data])
                else:
                    df = pl.DataFrame({"value": [data]})

            return ExtractionResult(
                data=df,
                row_count=len(df),
                metadata={"file": file_path, "columns": list(df.columns)},
            )
        except Exception as e:
            raise ExtractionError(
                f"Failed to read JSON: {str(e)}",
                source=file_path,
            )

    def list_files(self, pattern: str | None = None) -> list[str]:
        """List JSON files matching pattern."""
        base_path = Path(self.json_config.path)

        if base_path.is_file():
            return [str(base_path)]

        if base_path.is_dir():
            pattern = pattern or "*.json"
            files = list(base_path.glob(pattern))
            files.extend(base_path.glob("*.jsonl"))
            return [str(f) for f in files]

        return []

    def read_file(self, path: str) -> ExtractionResult:
        """Read a single JSON file."""
        return self.extract(path=path)

    def read_nested(
        self,
        path: str | None = None,
        json_path: str | None = None,
        unnest: bool = False,
    ) -> ExtractionResult:
        """Read JSON with nested structure handling."""
        file_path = path or self.json_config.path

        with open(file_path, "r", encoding=self.json_config.encoding) as f:
            data = json.load(f)

        # Navigate to specific path
        if json_path:
            for key in json_path.split("."):
                if isinstance(data, dict):
                    data = data.get(key, {})
                elif isinstance(data, list) and key.isdigit():
                    data = data[int(key)]

        # Convert to DataFrame
        if isinstance(data, list):
            df = pl.DataFrame(data)
        else:
            df = pl.DataFrame([data])

        # Optionally unnest struct columns
        if unnest:
            for col in df.columns:
                if df[col].dtype == pl.Struct:
                    df = df.unnest(col)

        return ExtractionResult(
            data=df,
            row_count=len(df),
            metadata={"file": file_path, "json_path": json_path},
        )

    def scan(self, path: str | None = None) -> pl.LazyFrame:
        """Create a lazy scan for large JSONL files."""
        file_path = path or self.json_config.path

        if not (self.json_config.json_lines or file_path.endswith(".jsonl")):
            # For regular JSON, read and convert to LazyFrame
            result = self.extract(path=file_path)
            return result.data.lazy()

        return pl.scan_ndjson(file_path)
