"""Dataset curation service for preparing data for distribution."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl
import structlog

logger = structlog.get_logger()


@dataclass
class CurationConfig:
    """Configuration for dataset curation."""

    output_dir: str = "curated_dataset"
    audio_subdir: str = "audio"
    transcripts_raw_subdir: str = "transcripts_raw"
    transcripts_deid_subdir: str = "transcripts_deid"
    metadata_subdir: str = "metadata"
    qa_subdir: str = "qa"
    splits: list[str] = field(default_factory=lambda: ["train", "test", "val"])
    split_ratios: dict[str, float] = field(
        default_factory=lambda: {"train": 0.8, "test": 0.1, "val": 0.1}
    )
    audio_format: str = "flac"
    metadata_format: str = "parquet"
    transcript_format: str = "json"
    deid_version: str | None = None
    include_qa_report: bool = True
    compress_output: bool = False


@dataclass
class DatasetMetadata:
    """Metadata for a curated dataset."""

    name: str
    description: str
    version: str
    created_at: datetime
    row_count: int
    splits: dict[str, int]
    schema: dict[str, str]
    statistics: dict[str, Any]
    processing_info: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "created_at": self.created_at.isoformat(),
            "row_count": self.row_count,
            "splits": self.splits,
            "schema": self.schema,
            "statistics": self.statistics,
            "processing_info": self.processing_info,
        }


class DatasetCurator:
    """
    Service for curating and packaging datasets for distribution.

    Supports:
    - Configurable output layouts
    - Train/test/val splitting
    - Metadata schema generation (Parquet, JSON)
    - QA report inclusion
    - Multiple file formats
    """

    def __init__(self, config: CurationConfig) -> None:
        self.config = config
        self.logger = logger.bind(component="dataset_curator")

        if self.config.deid_version is None:
            self.config.deid_version = datetime.utcnow().strftime("%Y-%m-%d_v1")

    def curate(
        self,
        data: pl.DataFrame,
        name: str,
        description: str = "",
        audio_files: list[str] | None = None,
        qa_report: dict[str, Any] | None = None,
    ) -> DatasetMetadata:
        """
        Curate a dataset and save to the configured output structure.

        Args:
            data: Main dataset DataFrame
            name: Dataset name
            description: Dataset description
            audio_files: List of audio file paths to include
            qa_report: QA report to include

        Returns:
            DatasetMetadata with information about the curated dataset
        """
        output_dir = Path(self.config.output_dir)
        self._create_directory_structure(output_dir)

        splits = self._split_data(data)
        split_counts = {split: len(df) for split, df in splits.items()}

        for split, split_df in splits.items():
            self._save_split(split_df, split, output_dir)

        if audio_files:
            self._organize_audio_files(audio_files, splits, output_dir)

        metadata = self._generate_metadata(
            name=name,
            description=description,
            data=data,
            split_counts=split_counts,
        )
        self._save_metadata(metadata, output_dir)

        if self.config.include_qa_report and qa_report:
            self._save_qa_report(qa_report, output_dir)

        self.logger.info(
            "Dataset curated successfully",
            name=name,
            total_rows=len(data),
            splits=split_counts,
            output_dir=str(output_dir),
        )

        return metadata

    def _create_directory_structure(self, output_dir: Path) -> None:
        """Create the output directory structure."""
        output_dir.mkdir(parents=True, exist_ok=True)

        for split in self.config.splits:
            (output_dir / self.config.audio_subdir / split).mkdir(parents=True, exist_ok=True)
            (output_dir / self.config.transcripts_raw_subdir / split).mkdir(parents=True, exist_ok=True)
            (output_dir / self.config.transcripts_deid_subdir / split).mkdir(parents=True, exist_ok=True)

        (output_dir / self.config.metadata_subdir).mkdir(parents=True, exist_ok=True)
        (output_dir / self.config.qa_subdir).mkdir(parents=True, exist_ok=True)

    def _split_data(self, data: pl.DataFrame) -> dict[str, pl.DataFrame]:
        """Split data into train/test/val sets with deterministic allocation.

        Uses floor-plus-remainder distribution to ensure all rows are allocated.
        When rows < splits, distributes to highest-ratio splits first.
        """
        n = len(data)
        split_names = list(self.config.split_ratios.keys())

        if n == 0:
            return {name: pl.DataFrame() for name in split_names}

        shuffled = data.sample(fraction=1.0, shuffle=True, seed=42)

        total_ratio = sum(self.config.split_ratios.values())
        normalized_ratios = {
            name: self.config.split_ratios[name] / total_ratio
            for name in split_names
        }

        if n < len(split_names):
            sorted_splits = sorted(
                split_names,
                key=lambda x: normalized_ratios[x],
                reverse=True
            )
            floors = {name: 0 for name in split_names}
            for i in range(n):
                floors[sorted_splits[i % len(sorted_splits)]] += 1
        else:
            floors = {name: int(n * normalized_ratios[name]) for name in split_names}
            fractions = {name: (n * normalized_ratios[name]) - floors[name] for name in split_names}

            remainder = n - sum(floors.values())
            sorted_by_fraction = sorted(fractions.items(), key=lambda x: -x[1])

            for i in range(remainder):
                floors[sorted_by_fraction[i % len(sorted_by_fraction)][0]] += 1

        splits = {}
        start_idx = 0
        for name in split_names:
            count = floors[name]
            splits[name] = shuffled.slice(start_idx, count)
            start_idx += count

        return splits

    def _save_split(
        self,
        df: pl.DataFrame,
        split: str,
        output_dir: Path,
    ) -> None:
        """Save a data split to files."""
        metadata_path = output_dir / self.config.metadata_subdir / f"{split}.{self.config.metadata_format}"

        if self.config.metadata_format == "parquet":
            df.write_parquet(metadata_path)
        elif self.config.metadata_format == "csv":
            df.write_csv(metadata_path)
        else:
            with open(metadata_path, "w") as f:
                json.dump(df.to_dicts(), f, indent=2, default=str)

        has_transcript = any("transcript" in col.lower() for col in df.columns)
        if has_transcript:
            self._save_transcripts(df, split, output_dir)

    def _save_transcripts(
        self,
        df: pl.DataFrame,
        split: str,
        output_dir: Path,
    ) -> None:
        """Save individual transcript files."""
        id_col = self._find_id_column(df)
        transcript_cols = [col for col in df.columns if "transcript" in col.lower()]
        deid_cols = [col for col in df.columns if "redacted" in col.lower() or "deid" in col.lower()]

        for row in df.iter_rows(named=True):
            record_id = row.get(id_col, str(hash(str(row))))

            if transcript_cols:
                raw_path = output_dir / self.config.transcripts_raw_subdir / split / f"{record_id}.json"
                raw_data = {col: row.get(col) for col in transcript_cols}
                raw_data["record_id"] = record_id
                with open(raw_path, "w") as f:
                    json.dump(raw_data, f, indent=2, default=str)

            if deid_cols:
                deid_path = output_dir / self.config.transcripts_deid_subdir / split / f"{record_id}.json"
                deid_data = {col: row.get(col) for col in deid_cols}
                deid_data["record_id"] = record_id
                deid_data["deid_version"] = self.config.deid_version
                with open(deid_path, "w") as f:
                    json.dump(deid_data, f, indent=2, default=str)

    def _organize_audio_files(
        self,
        audio_files: list[str],
        splits: dict[str, pl.DataFrame],
        output_dir: Path,
    ) -> None:
        """Organize audio files into split directories."""
        import shutil

        total_files = len(audio_files)
        if total_files == 0:
            return

        file_assignments = {}
        idx = 0
        for split, df in splits.items():
            split_size = len(df)
            for _ in range(split_size):
                if idx < total_files:
                    file_assignments[audio_files[idx]] = split
                    idx += 1

        for src_path, split in file_assignments.items():
            src = Path(src_path)
            if src.exists():
                dst = output_dir / self.config.audio_subdir / split / src.name
                shutil.copy2(src, dst)

    def _generate_metadata(
        self,
        name: str,
        description: str,
        data: pl.DataFrame,
        split_counts: dict[str, int],
    ) -> DatasetMetadata:
        """Generate dataset metadata."""
        schema = {col: str(dtype) for col, dtype in zip(data.columns, data.dtypes)}

        statistics = {
            "total_rows": len(data),
            "total_columns": len(data.columns),
            "null_percentages": {},
        }

        for col in data.columns:
            null_pct = data[col].null_count() / len(data) * 100 if len(data) > 0 else 0
            statistics["null_percentages"][col] = round(null_pct, 2)

        numeric_cols = [col for col in data.columns if data[col].dtype.is_numeric()]
        if numeric_cols:
            statistics["numeric_summary"] = {}
            for col in numeric_cols[:5]:
                stats = data[col].describe()
                statistics["numeric_summary"][col] = {
                    "mean": float(data[col].mean()) if data[col].mean() is not None else None,
                    "min": float(data[col].min()) if data[col].min() is not None else None,
                    "max": float(data[col].max()) if data[col].max() is not None else None,
                }

        return DatasetMetadata(
            name=name,
            description=description,
            version=self.config.deid_version or "1.0.0",
            created_at=datetime.utcnow(),
            row_count=len(data),
            splits=split_counts,
            schema=schema,
            statistics=statistics,
            processing_info={
                "deid_version": self.config.deid_version,
                "audio_format": self.config.audio_format,
                "splits_config": self.config.split_ratios,
            },
        )

    def _save_metadata(self, metadata: DatasetMetadata, output_dir: Path) -> None:
        """Save dataset metadata to file."""
        metadata_path = output_dir / self.config.metadata_subdir / "dataset_info.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata.to_dict(), f, indent=2, default=str)

        conversations_path = output_dir / self.config.metadata_subdir / "conversations.parquet"
        if not conversations_path.exists():
            pl.DataFrame([metadata.to_dict()]).write_parquet(conversations_path)

    def _save_qa_report(self, qa_report: dict[str, Any], output_dir: Path) -> None:
        """Save QA report to file."""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        qa_path = output_dir / self.config.qa_subdir / f"qa_report_{timestamp}.json"
        with open(qa_path, "w") as f:
            json.dump(qa_report, f, indent=2, default=str)

        spot_checks_path = output_dir / self.config.qa_subdir / "deid_spot_checks.jsonl"
        if "samples" in qa_report:
            with open(spot_checks_path, "a") as f:
                for sample in qa_report["samples"]:
                    f.write(json.dumps(sample, default=str) + "\n")

    def _find_id_column(self, df: pl.DataFrame) -> str:
        """Find the ID column in a DataFrame."""
        id_patterns = ["id", "conversation_id", "record_id", "uuid", "key"]
        for pattern in id_patterns:
            for col in df.columns:
                if pattern in col.lower():
                    return col
        return df.columns[0] if df.columns else "index"

    def create_sample_metadata_row(
        self,
        conversation_id: str,
        duration_sec: float,
        num_speakers: int = 2,
        sample_rate: int = 16000,
        has_pii: bool = True,
        qa_status: str = "approved",
    ) -> dict[str, Any]:
        """Create a sample metadata row in the expected format."""
        return {
            "conversation_id": conversation_id,
            "duration_sec": duration_sec,
            "num_speakers": num_speakers,
            "sample_rate": sample_rate,
            "has_pii": has_pii,
            "deid_version": self.config.deid_version,
            "qa_status": qa_status,
        }

    def generate_manifest(self, output_dir: Path | None = None) -> dict[str, Any]:
        """Generate a manifest of all files in the curated dataset."""
        output_dir = output_dir or Path(self.config.output_dir)

        manifest = {
            "generated_at": datetime.utcnow().isoformat(),
            "root_dir": str(output_dir),
            "files": {},
        }

        for subdir in [
            self.config.audio_subdir,
            self.config.transcripts_raw_subdir,
            self.config.transcripts_deid_subdir,
            self.config.metadata_subdir,
            self.config.qa_subdir,
        ]:
            subdir_path = output_dir / subdir
            if subdir_path.exists():
                manifest["files"][subdir] = []
                for file_path in subdir_path.rglob("*"):
                    if file_path.is_file():
                        manifest["files"][subdir].append({
                            "path": str(file_path.relative_to(output_dir)),
                            "size_bytes": file_path.stat().st_size,
                        })

        manifest_path = output_dir / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        return manifest
