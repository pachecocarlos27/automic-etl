"""Audio connector for processing audio files."""

from __future__ import annotations

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
class AudioSegment:
    """Represents a segment of audio with timing information."""

    start_ms: float
    end_ms: float
    text: str | None = None
    word: str | None = None
    confidence: float | None = None
    speaker: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def duration_ms(self) -> float:
        return self.end_ms - self.start_ms

    def to_dict(self) -> dict[str, Any]:
        return {
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "duration_ms": self.duration_ms,
            "text": self.text,
            "word": self.word,
            "confidence": self.confidence,
            "speaker": self.speaker,
            **self.metadata,
        }


@dataclass
class AudioConfig(ConnectorConfig):
    """Configuration for audio connector."""

    path: str = ""
    supported_formats: list[str] = field(
        default_factory=lambda: [".wav", ".mp3", ".flac", ".ogg", ".m4a", ".aac"]
    )
    extract_metadata: bool = True
    normalize_audio: bool = False
    target_sample_rate: int | None = None
    mono: bool = False

    def __post_init__(self) -> None:
        self.connector_type = ConnectorType.FILE
        if not self.name:
            self.name = f"audio_{Path(self.path).stem}" if self.path else "audio"


class AudioConnector(FileConnector):
    """
    Connector for processing audio files.

    Supports:
    - Multiple audio formats (WAV, MP3, FLAC, OGG, M4A, AAC)
    - Metadata extraction (duration, sample rate, channels)
    - Audio normalization and conversion
    - Segment-based processing
    - Batch file processing
    """

    connector_type = ConnectorType.FILE

    def __init__(self, config: AudioConfig) -> None:
        super().__init__(config)
        self.audio_config = config
        self._pydub_available = False
        self._soundfile_available = False

    def connect(self) -> None:
        """Initialize audio processing libraries."""
        try:
            import pydub
            self._pydub_available = True
        except ImportError:
            self.logger.warning("pydub not available, some features may be limited")

        try:
            import soundfile
            self._soundfile_available = True
        except ImportError:
            self.logger.warning("soundfile not available, some features may be limited")

        self._connected = True
        self.logger.info("Audio connector initialized", path=self.audio_config.path)

    def disconnect(self) -> None:
        """No persistent connection to close."""
        self._connected = False

    def test_connection(self) -> bool:
        """Test if audio path is accessible."""
        path = Path(self.audio_config.path)
        return path.exists() or path.parent.exists()

    def list_files(self, pattern: str | None = None) -> list[str]:
        """List available audio files."""
        path = Path(self.audio_config.path)

        if path.is_file():
            return [str(path)]

        if not path.is_dir():
            return []

        files = []
        for ext in self.audio_config.supported_formats:
            if pattern:
                files.extend(str(f) for f in path.glob(f"**/{pattern}{ext}"))
            else:
                files.extend(str(f) for f in path.glob(f"**/*{ext}"))

        return sorted(files)

    def read_file(self, path: str) -> ExtractionResult:
        """Read a single audio file and extract metadata."""
        file_path = Path(path)

        if not file_path.exists():
            raise ExtractionError(f"Audio file not found: {path}", source=path)

        metadata = self.extract_metadata(path)
        
        data = {
            "file_path": [str(file_path)],
            "file_name": [file_path.name],
            "format": [file_path.suffix.lower()],
            "duration_sec": [metadata.get("duration_sec", 0)],
            "sample_rate": [metadata.get("sample_rate", 0)],
            "channels": [metadata.get("channels", 0)],
            "bit_depth": [metadata.get("bit_depth")],
            "file_size_bytes": [metadata.get("file_size_bytes", 0)],
        }

        df = pl.DataFrame(data)

        return ExtractionResult(
            data=df,
            row_count=1,
            metadata=metadata,
        )

    def extract(
        self,
        query: str | None = None,
        path: str | None = None,
        **kwargs: Any,
    ) -> ExtractionResult:
        """Extract metadata from audio file(s)."""
        self._validate_connection()

        target_path = path or self.audio_config.path
        files = self.list_files() if Path(target_path).is_dir() else [target_path]

        if not files:
            return ExtractionResult(
                data=pl.DataFrame(),
                row_count=0,
                metadata={"message": "No audio files found"},
            )

        records = []
        for file_path in files:
            try:
                result = self.read_file(file_path)
                records.append(result.data.to_dicts()[0])
            except Exception as e:
                self.logger.warning(f"Failed to read {file_path}: {e}")
                records.append({
                    "file_path": file_path,
                    "error": str(e),
                })

        df = pl.DataFrame(records)

        return ExtractionResult(
            data=df,
            row_count=len(df),
            metadata={"files_processed": len(files)},
        )

    def extract_metadata(self, path: str) -> dict[str, Any]:
        """Extract metadata from an audio file."""
        file_path = Path(path)
        metadata = {
            "file_path": str(file_path),
            "file_name": file_path.name,
            "format": file_path.suffix.lower(),
            "file_size_bytes": file_path.stat().st_size if file_path.exists() else 0,
        }

        if self._soundfile_available:
            try:
                import soundfile as sf
                info = sf.info(str(file_path))
                metadata.update({
                    "duration_sec": info.duration,
                    "sample_rate": info.samplerate,
                    "channels": info.channels,
                    "frames": info.frames,
                    "format_info": info.format,
                    "subtype": info.subtype,
                })
            except Exception as e:
                self.logger.debug(f"soundfile metadata extraction failed: {e}")

        if "duration_sec" not in metadata and self._pydub_available:
            try:
                from pydub import AudioSegment
                audio = AudioSegment.from_file(str(file_path))
                metadata.update({
                    "duration_sec": len(audio) / 1000.0,
                    "sample_rate": audio.frame_rate,
                    "channels": audio.channels,
                    "bit_depth": audio.sample_width * 8,
                })
            except Exception as e:
                self.logger.debug(f"pydub metadata extraction failed: {e}")

        return metadata

    def load_audio(self, path: str) -> tuple[Any, int]:
        """
        Load audio data from file.

        Returns:
            Tuple of (audio_data, sample_rate)
        """
        if self._soundfile_available:
            import soundfile as sf
            import numpy as np

            data, sample_rate = sf.read(path)

            if self.audio_config.mono and len(data.shape) > 1:
                data = np.mean(data, axis=1)

            if self.audio_config.target_sample_rate and sample_rate != self.audio_config.target_sample_rate:
                pass

            return data, sample_rate

        elif self._pydub_available:
            from pydub import AudioSegment as PydubSegment
            import numpy as np

            audio = PydubSegment.from_file(path)

            if self.audio_config.mono:
                audio = audio.set_channels(1)

            if self.audio_config.target_sample_rate:
                audio = audio.set_frame_rate(self.audio_config.target_sample_rate)

            samples = np.array(audio.get_array_of_samples())
            return samples, audio.frame_rate

        raise ExtractionError(
            "No audio library available. Install soundfile or pydub.",
            source=path,
        )

    def save_audio(
        self,
        data: Any,
        sample_rate: int,
        path: str,
        format: str = "wav",
    ) -> str:
        """Save audio data to file."""
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if self._soundfile_available:
            import soundfile as sf
            sf.write(str(output_path), data, sample_rate)
            return str(output_path)

        elif self._pydub_available:
            from pydub import AudioSegment as PydubSegment
            import numpy as np

            audio = PydubSegment(
                data.tobytes() if hasattr(data, "tobytes") else bytes(data),
                frame_rate=sample_rate,
                sample_width=2,
                channels=1,
            )
            audio.export(str(output_path), format=format)
            return str(output_path)

        raise ExtractionError(
            "No audio library available for saving.",
            source=path,
        )

    def mute_segments(
        self,
        input_path: str,
        segments: list[AudioSegment],
        output_path: str,
        fade_ms: int = 10,
    ) -> str:
        """
        Mute specified segments in an audio file.

        Args:
            input_path: Path to input audio file
            segments: List of AudioSegment objects defining regions to mute
            output_path: Path for output file
            fade_ms: Fade duration for smoother transitions

        Returns:
            Path to the output file
        """
        if not self._pydub_available:
            raise ExtractionError(
                "pydub required for audio segment processing. Install with: pip install pydub",
                source=input_path,
            )

        from pydub import AudioSegment as PydubSegment
        from pydub.generators import Sine

        audio = PydubSegment.from_file(input_path)
        
        sorted_segments = sorted(segments, key=lambda s: s.start_ms, reverse=True)

        for segment in sorted_segments:
            start_ms = int(segment.start_ms)
            end_ms = int(segment.end_ms)
            duration_ms = end_ms - start_ms

            if duration_ms <= 0:
                continue

            silence = PydubSegment.silent(duration=duration_ms, frame_rate=audio.frame_rate)

            if fade_ms > 0 and duration_ms > fade_ms * 2:
                silence = silence.fade_in(fade_ms).fade_out(fade_ms)

            audio = audio[:start_ms] + silence + audio[end_ms:]

        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        audio.export(str(output), format=output.suffix[1:] or "wav")

        self.logger.info(
            "Muted audio segments",
            segments_count=len(segments),
            output=str(output),
        )

        return str(output)

    def bleep_segments(
        self,
        input_path: str,
        segments: list[AudioSegment],
        output_path: str,
        bleep_freq: int = 1000,
        bleep_volume: float = -20.0,
    ) -> str:
        """
        Replace specified segments with a bleep tone.

        Args:
            input_path: Path to input audio file
            segments: List of AudioSegment objects defining regions to bleep
            output_path: Path for output file
            bleep_freq: Frequency of the bleep tone in Hz
            bleep_volume: Volume of the bleep in dB

        Returns:
            Path to the output file
        """
        if not self._pydub_available:
            raise ExtractionError(
                "pydub required for audio segment processing.",
                source=input_path,
            )

        from pydub import AudioSegment as PydubSegment
        from pydub.generators import Sine

        audio = PydubSegment.from_file(input_path)
        
        sorted_segments = sorted(segments, key=lambda s: s.start_ms, reverse=True)

        for segment in sorted_segments:
            start_ms = int(segment.start_ms)
            end_ms = int(segment.end_ms)
            duration_ms = end_ms - start_ms

            if duration_ms <= 0:
                continue

            bleep = Sine(bleep_freq).to_audio_segment(duration=duration_ms)
            bleep = bleep + bleep_volume
            bleep = bleep.set_frame_rate(audio.frame_rate).set_channels(audio.channels)

            audio = audio[:start_ms] + bleep + audio[end_ms:]

        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        audio.export(str(output), format=output.suffix[1:] or "wav")

        self.logger.info(
            "Bleeped audio segments",
            segments_count=len(segments),
            output=str(output),
        )

        return str(output)

    def get_duration(self, path: str) -> float:
        """Get duration of audio file in seconds."""
        metadata = self.extract_metadata(path)
        return metadata.get("duration_sec", 0.0)

    def get_total_duration(self, paths: list[str] | None = None) -> float:
        """Get total duration of multiple audio files."""
        paths = paths or self.list_files()
        total = 0.0
        for path in paths:
            try:
                total += self.get_duration(path)
            except Exception:
                pass
        return total
