"""Hugging Face Datasets connector for loading datasets from the HF Hub."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

import polars as pl

from automic_etl.connectors.base import (
    BaseConnector,
    ConnectorConfig,
    ConnectorType,
    ExtractionResult,
)
from automic_etl.core.exceptions import ConnectionError, ExtractionError


@dataclass
class HuggingFaceConfig(ConnectorConfig):
    """Configuration for Hugging Face dataset connector."""

    dataset_name: str = ""
    subset: str | None = None
    split: str = "train"
    streaming: bool = False
    sample_size: int | None = None
    cache_dir: str | None = None
    trust_remote_code: bool = False
    revision: str | None = None
    token: str | None = None
    columns: list[str] | None = None
    download_audio: bool = True
    audio_output_dir: str | None = None

    def __post_init__(self) -> None:
        self.connector_type = ConnectorType.API
        if not self.name:
            self.name = f"huggingface_{self.dataset_name.replace('/', '_')}"


class HuggingFaceConnector(BaseConnector):
    """
    Connector for loading datasets from Hugging Face Hub.

    Supports:
    - Loading any public or private dataset
    - Streaming for large datasets
    - Configurable sampling
    - Schema auto-detection
    - Audio/media file handling
    """

    connector_type = ConnectorType.API

    def __init__(self, config: HuggingFaceConfig) -> None:
        super().__init__(config)
        self.hf_config = config
        self._dataset = None
        self._info = None

    def connect(self) -> None:
        """Load the dataset from Hugging Face Hub."""
        try:
            from datasets import load_dataset, load_dataset_builder

            builder = load_dataset_builder(
                self.hf_config.dataset_name,
                name=self.hf_config.subset,
                trust_remote_code=self.hf_config.trust_remote_code,
                revision=self.hf_config.revision,
                token=self.hf_config.token,
            )
            self._info = builder.info

            self._dataset = load_dataset(
                self.hf_config.dataset_name,
                name=self.hf_config.subset,
                split=self.hf_config.split,
                streaming=self.hf_config.streaming,
                cache_dir=self.hf_config.cache_dir,
                trust_remote_code=self.hf_config.trust_remote_code,
                revision=self.hf_config.revision,
                token=self.hf_config.token,
            )

            self._connected = True
            self.logger.info(
                "Connected to Hugging Face dataset",
                dataset=self.hf_config.dataset_name,
                split=self.hf_config.split,
            )

        except ImportError:
            raise ConnectionError(
                "datasets library not installed. Install with: pip install datasets",
                connector_type="huggingface",
            )
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to Hugging Face dataset: {str(e)}",
                connector_type="huggingface",
            )

    def disconnect(self) -> None:
        """Close the dataset connection."""
        self._dataset = None
        self._info = None
        self._connected = False
        self.logger.info("Disconnected from Hugging Face dataset")

    def test_connection(self) -> bool:
        """Test if the dataset is accessible."""
        try:
            from datasets import load_dataset_builder

            builder = load_dataset_builder(
                self.hf_config.dataset_name,
                name=self.hf_config.subset,
                trust_remote_code=self.hf_config.trust_remote_code,
                token=self.hf_config.token,
            )
            return builder is not None
        except Exception:
            return False

    def get_info(self) -> dict[str, Any]:
        """Get dataset information and metadata."""
        if self._info is None:
            return {}

        return {
            "description": self._info.description,
            "citation": self._info.citation,
            "homepage": self._info.homepage,
            "license": self._info.license,
            "features": str(self._info.features) if self._info.features else None,
            "splits": list(self._info.splits.keys()) if self._info.splits else [],
            "download_size": self._info.download_size,
            "dataset_size": self._info.dataset_size,
        }

    def get_schema(self) -> dict[str, str]:
        """Get the dataset schema (column names and types)."""
        if self._info is None or self._info.features is None:
            return {}

        schema = {}
        for name, feature in self._info.features.items():
            schema[name] = str(type(feature).__name__)
        return schema

    def extract(
        self,
        query: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        columns: list[str] | None = None,
        **kwargs: Any,
    ) -> ExtractionResult:
        """
        Extract data from the dataset.

        Args:
            query: Not used (for API compatibility)
            limit: Maximum number of rows to extract
            offset: Number of rows to skip
            columns: Specific columns to extract
            **kwargs: Additional arguments

        Returns:
            ExtractionResult with the extracted data
        """
        self._validate_connection()

        try:
            limit = limit or self.hf_config.sample_size
            columns = columns or self.hf_config.columns

            if self.hf_config.streaming:
                return self._extract_streaming(limit, columns)
            else:
                return self._extract_batch(limit, offset, columns)

        except Exception as e:
            raise ExtractionError(
                f"Failed to extract data from dataset: {str(e)}",
                source=self.hf_config.dataset_name,
            )

    def _extract_batch(
        self,
        limit: int | None,
        offset: int | None,
        columns: list[str] | None,
    ) -> ExtractionResult:
        """Extract data from non-streaming dataset."""
        if self._dataset is None:
            raise ExtractionError(
                "Dataset not loaded. Call connect() first.",
                source=self.hf_config.dataset_name,
            )

        dataset = self._dataset
        dataset_len = len(dataset)

        if offset and offset < dataset_len:
            dataset = dataset.select(range(offset, dataset_len))
            dataset_len = len(dataset)

        if limit and limit < dataset_len:
            dataset = dataset.select(range(min(limit, dataset_len)))

        if columns:
            available_cols = [c for c in columns if c in dataset.column_names]
            if available_cols:
                dataset = dataset.select_columns(available_cols)

        audio_files = []
        if self.hf_config.download_audio and self.hf_config.audio_output_dir:
            audio_files = self._save_audio_from_dataset(dataset)

        data = dataset.to_pandas()
        df = pl.from_pandas(data)

        return ExtractionResult(
            data=df,
            row_count=len(df),
            metadata={
                "dataset_name": self.hf_config.dataset_name,
                "split": self.hf_config.split,
                "columns": list(df.columns),
                "audio_files": audio_files,
                **self.get_info(),
            },
        )

    def _extract_streaming(
        self,
        limit: int | None,
        columns: list[str] | None,
    ) -> ExtractionResult:
        """Extract data from streaming dataset."""
        if self._dataset is None:
            raise ExtractionError(
                "Dataset not loaded. Call connect() first.",
                source=self.hf_config.dataset_name,
            )

        records = []
        raw_items = []
        count = 0

        for item in self._dataset:
            if self.hf_config.download_audio and self.hf_config.audio_output_dir:
                raw_items.append(dict(item))

            if columns:
                item = {k: v for k, v in item.items() if k in columns}

            processed = self._process_item(item)
            records.append(processed)
            count += 1

            if limit and count >= limit:
                break

        audio_files = []
        if raw_items and self.hf_config.download_audio and self.hf_config.audio_output_dir:
            audio_files = self._save_audio_from_items(raw_items)

        if not records:
            df = pl.DataFrame()
        else:
            df = pl.DataFrame(records)

        return ExtractionResult(
            data=df,
            row_count=len(df),
            metadata={
                "dataset_name": self.hf_config.dataset_name,
                "split": self.hf_config.split,
                "streaming": True,
                "columns": list(df.columns) if len(df) > 0 else [],
                "audio_files": audio_files,
            },
        )

    def _process_item(self, item: dict[str, Any]) -> dict[str, Any]:
        """Process a single item, handling special types like Audio."""
        processed = {}

        for key, value in item.items():
            if hasattr(value, "keys") and "array" in value and "sampling_rate" in value:
                processed[f"{key}_sampling_rate"] = value.get("sampling_rate")
                processed[f"{key}_path"] = value.get("path", "")
            elif hasattr(value, "keys") and "bytes" in value:
                processed[f"{key}_bytes_len"] = len(value.get("bytes", b""))
            elif isinstance(value, (str, int, float, bool)) or value is None:
                processed[key] = value
            elif isinstance(value, list):
                processed[key] = str(value) if len(value) > 100 else value
            elif isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    if isinstance(sub_value, (str, int, float, bool)) or sub_value is None:
                        processed[f"{key}_{sub_key}"] = sub_value
            else:
                processed[key] = str(value)

        return processed

    def _save_audio_from_dataset(self, dataset: Any, max_files: int = 1000) -> list[str]:
        """Save audio files from HuggingFace dataset to disk (batch mode).

        Args:
            dataset: HuggingFace dataset (map-style)
            max_files: Maximum files to save

        Returns:
            List of saved file paths
        """
        if not self.hf_config.audio_output_dir:
            return []

        items = []
        try:
            dataset_len = len(dataset) if hasattr(dataset, '__len__') else max_files
            count = min(dataset_len, max_files)
            for i in range(count):
                items.append(dataset[i])
        except Exception as e:
            self.logger.warning(f"Failed to access dataset items: {e}")
            return []

        return self._save_audio_from_items(items)

    def _save_audio_from_items(self, items: list[dict[str, Any]], max_files: int = 1000) -> list[str]:
        """Save audio files from raw HuggingFace item dicts.

        Args:
            items: List of raw HF dataset items containing audio feature dicts
            max_files: Maximum files to save

        Returns:
            List of saved file paths
        """
        if not self.hf_config.audio_output_dir:
            return []

        output_dir = Path(self.hf_config.audio_output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        try:
            import soundfile as sf
            import numpy as np
        except ImportError:
            self.logger.warning("soundfile/numpy not available for audio saving")
            return []

        saved_files = []

        for idx, item in enumerate(items[:max_files]):
            for key, value in item.items():
                if not isinstance(value, dict):
                    continue
                if "array" not in value or "sampling_rate" not in value:
                    continue

                try:
                    audio_array = np.array(value["array"])
                    sample_rate = value["sampling_rate"]
                    file_path = output_dir / f"{idx:05d}_{key}.wav"

                    sf.write(str(file_path), audio_array, sample_rate)
                    saved_files.append(str(file_path))
                    self.logger.debug(f"Saved audio file: {file_path}")
                except Exception as e:
                    self.logger.warning(f"Failed to save audio {idx}/{key}: {e}")
                    continue

        return saved_files

    def extract_batch(
        self,
        query: str | None = None,
        batch_size: int | None = None,
        **kwargs: Any,
    ) -> Iterator[ExtractionResult]:
        """Extract data in batches for large datasets."""
        self._validate_connection()

        if self._dataset is None:
            return

        batch_size = batch_size or self.config.batch_size

        if self.hf_config.streaming:
            batch = []
            for item in self._dataset:
                processed = self._process_item(item)
                batch.append(processed)

                if len(batch) >= batch_size:
                    df = pl.DataFrame(batch)
                    yield ExtractionResult(
                        data=df,
                        row_count=len(df),
                        metadata={
                            "dataset_name": self.hf_config.dataset_name,
                            "batch_size": batch_size,
                        },
                    )
                    batch = []

            if batch:
                df = pl.DataFrame(batch)
                yield ExtractionResult(
                    data=df,
                    row_count=len(df),
                    metadata={
                        "dataset_name": self.hf_config.dataset_name,
                        "batch_size": len(batch),
                    },
                )
        else:
            total = len(self._dataset)
            for offset in range(0, total, batch_size):
                yield self.extract(limit=batch_size, offset=offset)

    def get_sample(self, n: int = 5) -> pl.DataFrame:
        """Get a sample of the dataset."""
        result = self.extract(limit=n)
        return result.data

    def list_splits(self) -> list[str]:
        """List available splits for the dataset."""
        if self._info and self._info.splits:
            return list(self._info.splits.keys())
        return []

    def get_column_names(self) -> list[str]:
        """Get column names from the dataset."""
        if self._info and self._info.features:
            return list(self._info.features.keys())
        return []
