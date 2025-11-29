"""Core module for Automic ETL."""

from automic_etl.core.config import Settings, get_settings
from automic_etl.core.pipeline import Pipeline, PipelineBuilder
from automic_etl.core.exceptions import (
    AutomicETLError,
    ConfigurationError,
    ConnectionError,
    ExtractionError,
    TransformationError,
    LoadError,
    StorageError,
    IcebergError,
    LLMError,
)

__all__ = [
    "Settings",
    "get_settings",
    "Pipeline",
    "PipelineBuilder",
    "AutomicETLError",
    "ConfigurationError",
    "ConnectionError",
    "ExtractionError",
    "TransformationError",
    "LoadError",
    "StorageError",
    "IcebergError",
    "LLMError",
]
