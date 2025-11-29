"""Configuration management for Automic ETL."""

from __future__ import annotations

import os
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class StorageProvider(str, Enum):
    """Supported storage providers."""

    AWS = "aws"
    GCP = "gcp"
    AZURE = "azure"


class CatalogType(str, Enum):
    """Supported Iceberg catalog types."""

    SQL = "sql"
    GLUE = "glue"
    HIVE = "hive"
    REST = "rest"
    DYNAMODB = "dynamodb"


class LLMProvider(str, Enum):
    """Supported LLM providers."""

    ANTHROPIC = "anthropic"
    OPENAI = "openai"
    AZURE_OPENAI = "azure_openai"
    OLLAMA = "ollama"
    LITELLM = "litellm"


class ExtractionMode(str, Enum):
    """Data extraction modes."""

    BATCH = "batch"
    INCREMENTAL = "incremental"


class DataQualityAction(str, Enum):
    """Actions on data quality failures."""

    WARN = "warn"
    FAIL = "fail"
    QUARANTINE = "quarantine"


# ============================================================================
# AWS Configuration
# ============================================================================
class AWSConfig(BaseModel):
    """AWS storage configuration."""

    bucket: str = Field(default="", description="S3 bucket name")
    region: str = Field(default="us-east-1", description="AWS region")
    access_key_id: str | None = Field(default=None, description="AWS access key ID")
    secret_access_key: str | None = Field(default=None, description="AWS secret access key")
    endpoint_url: str | None = Field(default=None, description="Custom endpoint URL (for MinIO)")
    role_arn: str | None = Field(default=None, description="IAM role ARN for assume role")


class GCPConfig(BaseModel):
    """GCP storage configuration."""

    bucket: str = Field(default="", description="GCS bucket name")
    project_id: str = Field(default="", description="GCP project ID")
    credentials_file: str | None = Field(default=None, description="Service account JSON path")


class AzureConfig(BaseModel):
    """Azure storage configuration."""

    container: str = Field(default="", description="Blob container name")
    storage_account: str = Field(default="", description="Storage account name")
    connection_string: str | None = Field(default=None, description="Connection string")
    use_managed_identity: bool = Field(default=False, description="Use managed identity")


class StorageConfig(BaseModel):
    """Storage configuration."""

    provider: StorageProvider = Field(default=StorageProvider.AWS)
    aws: AWSConfig = Field(default_factory=AWSConfig)
    gcp: GCPConfig = Field(default_factory=GCPConfig)
    azure: AzureConfig = Field(default_factory=AzureConfig)


# ============================================================================
# Iceberg Configuration
# ============================================================================
class IcebergTableDefaults(BaseModel):
    """Iceberg table defaults."""

    format_version: int = Field(default=2)
    target_file_size_bytes: int = Field(default=536870912)  # 512MB
    compression: str = Field(default="zstd")
    compression_level: int = Field(default=3)


class IcebergCatalogConfig(BaseModel):
    """Iceberg catalog configuration."""

    type: CatalogType = Field(default=CatalogType.SQL)
    name: str = Field(default="automic_catalog")
    uri: str = Field(default="sqlite:///catalog.db")


class IcebergConfig(BaseModel):
    """Iceberg configuration."""

    catalog: IcebergCatalogConfig = Field(default_factory=IcebergCatalogConfig)
    warehouse: str = Field(default="warehouse/")
    table_defaults: IcebergTableDefaults = Field(default_factory=IcebergTableDefaults)


# ============================================================================
# Medallion Configuration
# ============================================================================
class MedallionLayerConfig(BaseModel):
    """Configuration for a medallion layer."""

    path: str
    description: str = ""
    retention_days: int | None = None
    partition_by: list[str] = Field(default_factory=list)


class MedallionConfig(BaseModel):
    """Medallion architecture configuration."""

    bronze: MedallionLayerConfig = Field(
        default_factory=lambda: MedallionLayerConfig(
            path="bronze/",
            description="Raw data layer",
            retention_days=90,
            partition_by=["_ingestion_date"],
        )
    )
    silver: MedallionLayerConfig = Field(
        default_factory=lambda: MedallionLayerConfig(
            path="silver/",
            description="Cleaned data layer",
            retention_days=365,
            partition_by=["_processing_date"],
        )
    )
    gold: MedallionLayerConfig = Field(
        default_factory=lambda: MedallionLayerConfig(
            path="gold/",
            description="Analytics layer",
            partition_by=[],
        )
    )


# ============================================================================
# LLM Configuration
# ============================================================================
class LLMFeatures(BaseModel):
    """LLM features configuration."""

    schema_inference: bool = Field(default=True)
    entity_extraction: bool = Field(default=True)
    data_classification: bool = Field(default=True)
    anomaly_detection: bool = Field(default=True)
    query_building: bool = Field(default=True)


class LLMConfig(BaseModel):
    """LLM configuration."""

    provider: LLMProvider = Field(default=LLMProvider.ANTHROPIC)
    model: str = Field(default="claude-sonnet-4-20250514")
    api_key: str | None = Field(default=None)
    base_url: str | None = Field(default=None)
    temperature: float = Field(default=0.1, ge=0, le=2)
    max_tokens: int = Field(default=4096)
    timeout: int = Field(default=120)
    features: LLMFeatures = Field(default_factory=LLMFeatures)


# ============================================================================
# Extraction Configuration
# ============================================================================
class BatchExtractionConfig(BaseModel):
    """Batch extraction configuration."""

    size: int = Field(default=100000)
    parallel_workers: int = Field(default=4)


class IncrementalExtractionConfig(BaseModel):
    """Incremental extraction configuration."""

    watermark_strategy: str = Field(default="timestamp")
    watermark_column: str = Field(default="updated_at")
    lookback_window: str = Field(default="1 hour")


class CDCConfig(BaseModel):
    """CDC configuration."""

    enabled: bool = Field(default=False)
    method: str = Field(default="timestamp")


class ExtractionConfig(BaseModel):
    """Extraction configuration."""

    default_mode: ExtractionMode = Field(default=ExtractionMode.INCREMENTAL)
    batch: BatchExtractionConfig = Field(default_factory=BatchExtractionConfig)
    incremental: IncrementalExtractionConfig = Field(default_factory=IncrementalExtractionConfig)
    cdc: CDCConfig = Field(default_factory=CDCConfig)


# ============================================================================
# Data Quality Configuration
# ============================================================================
class DataQualityChecks(BaseModel):
    """Data quality checks configuration."""

    null_check: bool = Field(default=True)
    duplicate_check: bool = Field(default=True)
    schema_validation: bool = Field(default=True)
    range_validation: bool = Field(default=True)
    referential_integrity: bool = Field(default=False)


class QuarantineConfig(BaseModel):
    """Quarantine configuration."""

    enabled: bool = Field(default=True)
    path: str = Field(default="quarantine/")


class DataQualityConfig(BaseModel):
    """Data quality configuration."""

    enabled: bool = Field(default=True)
    checks: DataQualityChecks = Field(default_factory=DataQualityChecks)
    on_failure: DataQualityAction = Field(default=DataQualityAction.WARN)
    quarantine: QuarantineConfig = Field(default_factory=QuarantineConfig)


# ============================================================================
# Unstructured Configuration
# ============================================================================
class OCRConfig(BaseModel):
    """OCR configuration."""

    enabled: bool = Field(default=True)
    language: str = Field(default="eng")
    dpi: int = Field(default=300)


class DocumentConfig(BaseModel):
    """Document processing configuration."""

    extract_tables: bool = Field(default=True)
    extract_images: bool = Field(default=True)
    extract_metadata: bool = Field(default=True)


class ChunkingConfig(BaseModel):
    """Chunking configuration for LLM processing."""

    strategy: str = Field(default="by_title")
    max_chunk_size: int = Field(default=1000)
    overlap: int = Field(default=200)


class UnstructuredConfig(BaseModel):
    """Unstructured data processing configuration."""

    ocr: OCRConfig = Field(default_factory=OCRConfig)
    documents: DocumentConfig = Field(default_factory=DocumentConfig)
    chunking: ChunkingConfig = Field(default_factory=ChunkingConfig)


# ============================================================================
# Connectors Configuration
# ============================================================================
class RetryConfig(BaseModel):
    """Retry configuration."""

    max_attempts: int = Field(default=3)
    backoff_factor: float = Field(default=2.0)


class DatabaseConnectorConfig(BaseModel):
    """Database connector configuration."""

    pool_size: int = Field(default=5)
    max_overflow: int = Field(default=10)


class APIConnectorConfig(BaseModel):
    """API connector configuration."""

    rate_limit: int = Field(default=100)
    concurrent_requests: int = Field(default=10)


class ConnectorsConfig(BaseModel):
    """Connectors configuration."""

    timeout: int = Field(default=30)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    databases: DatabaseConnectorConfig = Field(default_factory=DatabaseConnectorConfig)
    apis: APIConnectorConfig = Field(default_factory=APIConnectorConfig)


# ============================================================================
# Pipeline Configuration
# ============================================================================
class CheckpointConfig(BaseModel):
    """Checkpoint configuration."""

    enabled: bool = Field(default=True)
    interval: int = Field(default=1000)


class PipelineConfig(BaseModel):
    """Pipeline configuration."""

    mode: str = Field(default="parallel")
    max_parallel_jobs: int = Field(default=4)
    checkpoint: CheckpointConfig = Field(default_factory=CheckpointConfig)
    schedule: str | None = Field(default=None)


# ============================================================================
# Logging Configuration
# ============================================================================
class LoggingConfig(BaseModel):
    """Logging configuration."""

    level: str = Field(default="INFO")
    format: str = Field(default="json")
    file: str | None = Field(default=None)


# ============================================================================
# Metrics Configuration
# ============================================================================
class MetricsConfig(BaseModel):
    """Metrics configuration."""

    enabled: bool = Field(default=True)
    provider: str = Field(default="prometheus")


# ============================================================================
# Lakehouse Configuration
# ============================================================================
class LakehouseConfig(BaseModel):
    """Lakehouse configuration."""

    name: str = Field(default="default_lakehouse")
    description: str = Field(default="AI-Augmented Lakehouse")


# ============================================================================
# Main Settings
# ============================================================================
class Settings(BaseSettings):
    """Main settings for Automic ETL."""

    model_config = SettingsConfigDict(
        env_prefix="AUTOMIC_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    # Core configuration
    lakehouse: LakehouseConfig = Field(default_factory=LakehouseConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    iceberg: IcebergConfig = Field(default_factory=IcebergConfig)
    medallion: MedallionConfig = Field(default_factory=MedallionConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    extraction: ExtractionConfig = Field(default_factory=ExtractionConfig)
    data_quality: DataQualityConfig = Field(default_factory=DataQualityConfig)
    unstructured: UnstructuredConfig = Field(default_factory=UnstructuredConfig)
    connectors: ConnectorsConfig = Field(default_factory=ConnectorsConfig)
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)

    @classmethod
    def from_yaml(cls, path: str | Path) -> "Settings":
        """Load settings from a YAML file."""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")

        with open(path) as f:
            config_dict = yaml.safe_load(f)

        # Expand environment variables
        config_dict = cls._expand_env_vars(config_dict)

        return cls(**config_dict)

    @classmethod
    def _expand_env_vars(cls, config: Any) -> Any:
        """Recursively expand environment variables in config."""
        if isinstance(config, dict):
            return {k: cls._expand_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [cls._expand_env_vars(item) for item in config]
        elif isinstance(config, str):
            # Handle ${VAR} and ${VAR:default} syntax
            if config.startswith("${") and "}" in config:
                var_part = config[2 : config.index("}")]
                if ":" in var_part:
                    var_name, default = var_part.split(":", 1)
                else:
                    var_name, default = var_part, None

                value = os.environ.get(var_name, default)
                return value if value is not None else config
            return config
        return config

    def get_storage_config(self) -> AWSConfig | GCPConfig | AzureConfig:
        """Get the active storage provider configuration."""
        provider_configs = {
            StorageProvider.AWS: self.storage.aws,
            StorageProvider.GCP: self.storage.gcp,
            StorageProvider.AZURE: self.storage.azure,
        }
        return provider_configs[self.storage.provider]

    def get_warehouse_path(self) -> str:
        """Get the full warehouse path."""
        provider = self.storage.provider
        if provider == StorageProvider.AWS:
            return f"s3://{self.storage.aws.bucket}/{self.iceberg.warehouse}"
        elif provider == StorageProvider.GCP:
            return f"gs://{self.storage.gcp.bucket}/{self.iceberg.warehouse}"
        elif provider == StorageProvider.AZURE:
            account = self.storage.azure.storage_account
            container = self.storage.azure.container
            return f"abfss://{container}@{account}.dfs.core.windows.net/{self.iceberg.warehouse}"
        raise ValueError(f"Unknown storage provider: {provider}")


@lru_cache
def get_settings(config_path: str | None = None) -> Settings:
    """Get cached settings instance."""
    if config_path:
        return Settings.from_yaml(config_path)

    # Try default locations
    default_paths = [
        Path("config/settings.yaml"),
        Path("settings.yaml"),
        Path.home() / ".automic" / "settings.yaml",
    ]

    for path in default_paths:
        if path.exists():
            return Settings.from_yaml(path)

    # Return default settings if no config file found
    return Settings()
