"""CSV file connector."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
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
class CSVConfig(ConnectorConfig):
    """CSV connector configuration."""

    path: str = ""
    has_header: bool = True
    delimiter: str = ","
    quote_char: str = '"'
    encoding: str = "utf-8"
    skip_rows: int = 0
    null_values: list[str] = field(default_factory=lambda: ["", "NULL", "null", "NA", "N/A"])
    infer_schema_length: int = 1000

    def __post_init__(self) -> None:
        self.connector_type = ConnectorType.FILE


class CSVConnector(FileConnector):
    """CSV file connector."""

    def __init__(self, config: CSVConfig) -> None:
        super().__init__(config)
        self.csv_config = config

    def connect(self) -> None:
        """Verify path exists."""
        path = Path(self.csv_config.path)
        if path.is_file() or path.is_dir():
            self._connected = True
            self.logger.info("CSV connector ready", path=str(path))
        else:
            self._connected = True  # Allow for glob patterns
            self.logger.info("CSV connector ready (pattern mode)", path=str(path))

    def disconnect(self) -> None:
        """No persistent connection to close."""
        self._connected = False

    def test_connection(self) -> bool:
        """Test if path is accessible."""
        try:
            path = Path(self.csv_config.path)
            if path.is_file():
                return path.exists()
            elif path.is_dir():
                return path.exists()
            else:
                # Check parent directory for glob patterns
                return path.parent.exists() if path.parent else True
        except Exception:
            return False

    def extract(
        self,
        query: str | None = None,
        path: str | None = None,
        columns: list[str] | None = None,
        n_rows: int | None = None,
        **kwargs: Any,
    ) -> ExtractionResult:
        """Extract data from CSV file(s)."""
        file_path = path or self.csv_config.path

        try:
            df = pl.read_csv(
                file_path,
                has_header=self.csv_config.has_header,
                separator=self.csv_config.delimiter,
                quote_char=self.csv_config.quote_char,
                encoding=self.csv_config.encoding,
                skip_rows=self.csv_config.skip_rows,
                null_values=self.csv_config.null_values,
                infer_schema_length=self.csv_config.infer_schema_length,
                columns=columns,
                n_rows=n_rows,
            )

            return ExtractionResult(
                data=df,
                row_count=len(df),
                metadata={"file": file_path, "columns": list(df.columns)},
            )
        except Exception as e:
            raise ExtractionError(
                f"Failed to read CSV: {str(e)}",
                source=file_path,
            )

    def list_files(self, pattern: str | None = None) -> list[str]:
        """List CSV files matching pattern."""
        base_path = Path(self.csv_config.path)

        if base_path.is_file():
            return [str(base_path)]

        if base_path.is_dir():
            pattern = pattern or "*.csv"
            return [str(f) for f in base_path.glob(pattern)]

        # Treat as glob pattern
        parent = base_path.parent
        pattern = base_path.name
        return [str(f) for f in parent.glob(pattern)]

    def read_file(self, path: str) -> ExtractionResult:
        """Read a single CSV file."""
        return self.extract(path=path)

    def scan(
        self,
        path: str | None = None,
        columns: list[str] | None = None,
    ) -> pl.LazyFrame:
        """Create a lazy scan for large files."""
        file_path = path or self.csv_config.path

        return pl.scan_csv(
            file_path,
            has_header=self.csv_config.has_header,
            separator=self.csv_config.delimiter,
            quote_char=self.csv_config.quote_char,
            encoding=self.csv_config.encoding,
            skip_rows=self.csv_config.skip_rows,
            null_values=self.csv_config.null_values,
            infer_schema_length=self.csv_config.infer_schema_length,
        )
