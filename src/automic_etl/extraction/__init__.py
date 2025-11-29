"""Extraction module for batch and incremental data loading."""

from automic_etl.extraction.batch import BatchExtractor
from automic_etl.extraction.incremental import IncrementalExtractor
from automic_etl.extraction.watermark import WatermarkManager

__all__ = [
    "BatchExtractor",
    "IncrementalExtractor",
    "WatermarkManager",
]
