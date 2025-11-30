"""
Automic ETL - AI-Augmented ETL Tool for Lakehouse Architecture

A comprehensive ETL platform that builds lakehouses using the medallion architecture
(Bronze/Silver/Gold) on cloud storage (AWS S3, GCS, Azure Blob) with Apache Iceberg tables.
Features LLM integration for intelligent data processing of unstructured data.
"""

__version__ = "0.1.0"
__author__ = "Datant LLC"

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

# Lazy imports for heavy modules
def __getattr__(name: str):
    """Lazy import for heavy modules."""
    if name == "Lakehouse":
        from automic_etl.medallion import Lakehouse
        return Lakehouse
    elif name == "SCDType2Manager":
        from automic_etl.medallion import SCDType2Manager
        return SCDType2Manager
    elif name == "AugmentedETL":
        from automic_etl.llm import AugmentedETL
        return AugmentedETL
    elif name == "run_ui":
        from automic_etl.ui import run_app
        return run_app
    # Validation
    elif name == "DataValidator":
        from automic_etl.validation import DataValidator
        return DataValidator
    elif name == "DataQualityChecker":
        from automic_etl.validation import DataQualityChecker
        return DataQualityChecker
    # Lineage
    elif name == "LineageTracker":
        from automic_etl.lineage import LineageTracker
        return LineageTracker
    elif name == "LineageGraph":
        from automic_etl.lineage import LineageGraph
        return LineageGraph
    # Orchestration
    elif name == "Scheduler":
        from automic_etl.orchestration import Scheduler
        return Scheduler
    elif name == "Workflow":
        from automic_etl.orchestration import Workflow
        return Workflow
    elif name == "WorkflowRunner":
        from automic_etl.orchestration import WorkflowRunner
        return WorkflowRunner
    # Notifications
    elif name == "Notifier":
        from automic_etl.notifications import Notifier
        return Notifier
    elif name == "AlertManager":
        from automic_etl.notifications import AlertManager
        return AlertManager
    # Delta Lake
    elif name == "DeltaTableManager":
        from automic_etl.storage.delta import DeltaTableManager
        return DeltaTableManager
    # Integrations (lazy)
    elif name == "SparkIntegration":
        from automic_etl.integrations import SparkIntegration
        return SparkIntegration
    elif name == "DbtIntegration":
        from automic_etl.integrations import DbtIntegration
        return DbtIntegration
    elif name == "GreatExpectationsIntegration":
        from automic_etl.integrations import GreatExpectationsIntegration
        return GreatExpectationsIntegration
    elif name == "AirflowIntegration":
        from automic_etl.integrations import AirflowIntegration
        return AirflowIntegration
    elif name == "MLflowIntegration":
        from automic_etl.integrations import MLflowIntegration
        return MLflowIntegration
    elif name == "OpenMetadataIntegration":
        from automic_etl.integrations import OpenMetadataIntegration
        return OpenMetadataIntegration
    # API (lazy)
    elif name == "create_api":
        from automic_etl.api import create_app
        return create_app
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    # Version
    "__version__",
    # Configuration
    "Settings",
    "get_settings",
    # Pipeline
    "Pipeline",
    "PipelineBuilder",
    # Lakehouse (lazy)
    "Lakehouse",
    "SCDType2Manager",
    # LLM (lazy)
    "AugmentedETL",
    # UI (lazy)
    "run_ui",
    # Validation (lazy)
    "DataValidator",
    "DataQualityChecker",
    # Lineage (lazy)
    "LineageTracker",
    "LineageGraph",
    # Orchestration (lazy)
    "Scheduler",
    "Workflow",
    "WorkflowRunner",
    # Notifications (lazy)
    "Notifier",
    "AlertManager",
    # Delta Lake (lazy)
    "DeltaTableManager",
    # Integrations (lazy)
    "SparkIntegration",
    "DbtIntegration",
    "GreatExpectationsIntegration",
    "AirflowIntegration",
    "MLflowIntegration",
    "OpenMetadataIntegration",
    # API (lazy)
    "create_api",
    # Exceptions
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
