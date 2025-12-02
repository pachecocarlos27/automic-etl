"""Media connectors for audio and video processing."""

from automic_etl.connectors.media.audio import (
    AudioConfig,
    AudioConnector,
    AudioSegment as AudioSegmentData,
)

__all__ = [
    "AudioConfig",
    "AudioConnector",
    "AudioSegmentData",
]
