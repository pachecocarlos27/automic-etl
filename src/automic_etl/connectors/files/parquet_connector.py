"""Parquet file connector."""

from __future__ import annotations

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
class ParquetConfig(ConnectorConfig):
    """Parquet connector configuration."""

    path: str = ""
    use_statistics: bool = True
    rechunk: bool = True

    def __post_init__(self) -> None:
        self.connector_type = ConnectorType.FILE


class ParquetConnector(FileConnector):
    """Parquet file connector with optimized reading."""

    def __init__(self, config: ParquetConfig) -> None:
        super().__init__(config)
        self.parquet_config = config

    def connect(self) -> None:
        """Verify path exists."""
        self._connected = True
        self.logger.info("Parquet connector ready", path=self.parquet_config.path)

    def disconnect(self) -> None:
        """No persistent connection to close."""
        self._connected = False

    def test_connection(self) -> bool:
        """Test if path is accessible."""
        path = Path(self.parquet_config.path)
        return path.exists() or path.parent.exists()

    def extract(
        self,
        query: str | None = None,
        path: str | None = None,
        columns: list[str] | None = None,
        n_rows: int | None = None,
        row_index_name: str | None = None,
        **kwargs: Any,
    ) -> ExtractionResult:
        """Extract data from Parquet file(s)."""
        file_path = path or self.parquet_config.path

        try:
            df = pl.read_parquet(
                file_path,
                columns=columns,
                n_rows=n_rows,
                use_statistics=self.parquet_config.use_statistics,
                rechunk=self.parquet_config.rechunk,
                row_index_name=row_index_name,
            )

            return ExtractionResult(
                data=df,
                row_count=len(df),
                metadata={
                    "file": file_path,
                    "columns": list(df.columns),
                    "schema": {col: str(dtype) for col, dtype in df.schema.items()},
                },
            )
        except Exception as e:
            raise ExtractionError(
                f"Failed to read Parquet: {str(e)}",
                source=file_path,
            )

    def list_files(self, pattern: str | None = None) -> list[str]:
        """List Parquet files matching pattern."""
        base_path = Path(self.parquet_config.path)

        if base_path.is_file():
            return [str(base_path)]

        if base_path.is_dir():
            pattern = pattern or "*.parquet"
            return [str(f) for f in base_path.glob(pattern)]

        return []

    def read_file(self, path: str) -> ExtractionResult:
        """Read a single Parquet file."""
        return self.extract(path=path)

    def scan(
        self,
        path: str | None = None,
        columns: list[str] | None = None,
    ) -> pl.LazyFrame:
        """Create a lazy scan for large Parquet files."""
        file_path = path or self.parquet_config.path

        return pl.scan_parquet(
            file_path,
            rechunk=self.parquet_config.rechunk,
        )

    def get_schema(self, path: str | None = None) -> dict[str, str]:
        """Get the schema of a Parquet file without reading data."""
        file_path = path or self.parquet_config.path

        lf = pl.scan_parquet(file_path)
        return {col: str(dtype) for col, dtype in lf.schema.items()}

    def get_metadata(self, path: str | None = None) -> dict[str, Any]:
        """Get Parquet file metadata."""
        import pyarrow.parquet as pq

        file_path = path or self.parquet_config.path

        parquet_file = pq.ParquetFile(file_path)
        metadata = parquet_file.metadata

        return {
            "num_rows": metadata.num_rows,
            "num_columns": metadata.num_columns,
            "num_row_groups": metadata.num_row_groups,
            "created_by": metadata.created_by,
            "format_version": metadata.format_version,
        }

    def read_partitioned(
        self,
        path: str | None = None,
        hive_partitioning: bool = True,
    ) -> ExtractionResult:
        """Read partitioned Parquet dataset."""
        file_path = path or self.parquet_config.path

        try:
            df = pl.read_parquet(
                f"{file_path}/**/*.parquet",
                hive_partitioning=hive_partitioning,
            )

            return ExtractionResult(
                data=df,
                row_count=len(df),
                metadata={
                    "path": file_path,
                    "partitioned": True,
                },
            )
        except Exception as e:
            raise ExtractionError(
                f"Failed to read partitioned Parquet: {str(e)}",
                source=file_path,
            )
