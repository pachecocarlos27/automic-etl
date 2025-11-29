"""Tests for configuration management."""

import pytest

from automic_etl.core.config import (
    CatalogType,
    DataQualityAction,
    ExtractionMode,
    LLMProvider,
    Settings,
    StorageProvider,
)


class TestStorageConfiguration:
    """Test storage provider configuration."""

    def test_default_storage_provider(self, test_settings):
        """Default storage provider should be AWS."""
        assert test_settings.storage.provider == StorageProvider.AWS

    def test_aws_storage_config(self, test_settings):
        """AWS storage configuration should be set."""
        assert test_settings.storage.aws.bucket == "test-bucket"
        assert test_settings.storage.aws.region == "us-east-1"

    def test_change_storage_provider(self):
        """Should support changing storage provider."""
        settings = Settings(
            storage__provider="gcp",
            storage__gcp__bucket="test-gcs-bucket",
            storage__gcp__project_id="test-project",
        )

        assert settings.storage.provider == StorageProvider.GCP

    def test_azure_storage_config(self):
        """Should support Azure storage configuration."""
        settings = Settings(
            storage__provider="azure",
            storage__azure__container="test-container",
            storage__azure__storage_account="teststorage",
        )

        assert settings.storage.provider == StorageProvider.AZURE


class TestIcebergConfiguration:
    """Test Iceberg configuration."""

    def test_default_iceberg_config(self, test_settings):
        """Default Iceberg config should be valid."""
        assert test_settings.iceberg.catalog.type == CatalogType.SQL
        assert test_settings.iceberg.catalog.name == "automic_catalog"

    def test_iceberg_table_defaults(self, test_settings):
        """Iceberg table defaults should be set."""
        assert test_settings.iceberg.table_defaults.format_version == 2
        assert test_settings.iceberg.table_defaults.compression == "zstd"

    def test_custom_catalog_type(self):
        """Should support custom Iceberg catalog types."""
        settings = Settings(
            iceberg__catalog__type="glue",
            iceberg__catalog__name="custom_catalog",
        )

        assert settings.iceberg.catalog.type == CatalogType.GLUE


class TestMedallionConfiguration:
    """Test medallion layer configuration."""

    def test_default_medallion_paths(self, test_settings):
        """Default medallion paths should be configured."""
        medallion = test_settings.medallion
        assert "bronze" in medallion.bronze.path
        assert "silver" in medallion.silver.path
        assert "gold" in medallion.gold.path

    def test_bronze_retention(self, test_settings):
        """Bronze layer retention should be set."""
        assert test_settings.medallion.bronze.retention_days == 90

    def test_silver_retention(self, test_settings):
        """Silver layer retention should be set."""
        assert test_settings.medallion.silver.retention_days == 365

    def test_custom_medallion_paths(self):
        """Should support custom medallion paths."""
        settings = Settings(
            medallion__bronze__path="custom/bronze/",
            medallion__silver__path="custom/silver/",
            medallion__gold__path="custom/gold/",
        )

        assert "custom" in settings.medallion.bronze.path


class TestLLMConfiguration:
    """Test LLM configuration."""

    def test_default_llm_provider(self, test_settings):
        """Default LLM provider should be Anthropic."""
        assert test_settings.llm.provider == LLMProvider.ANTHROPIC

    def test_default_llm_model(self, test_settings):
        """Default LLM model should be set."""
        assert test_settings.llm.model == "claude-sonnet-4-20250514"

    def test_llm_features_enabled(self, test_settings):
        """LLM features should be enabled by default."""
        features = test_settings.llm.features
        assert features.schema_inference is True
        assert features.entity_extraction is True
        assert features.query_building is True

    def test_openai_provider(self):
        """Should support OpenAI provider."""
        settings = Settings(
            llm__provider="openai",
            llm__api_key="test-key",
            llm__model="gpt-4",
        )

        assert settings.llm.provider == LLMProvider.OPENAI
        assert settings.llm.model == "gpt-4"

    def test_temperature_validation(self):
        """LLM temperature should be between 0 and 2."""
        settings = Settings(llm__temperature=0.5)
        assert settings.llm.temperature == 0.5

        settings = Settings(llm__temperature=0)
        assert settings.llm.temperature == 0

        settings = Settings(llm__temperature=2)
        assert settings.llm.temperature == 2


class TestExtractionConfiguration:
    """Test extraction configuration."""

    def test_default_extraction_mode(self, test_settings):
        """Default extraction mode should be incremental."""
        assert test_settings.extraction.default_mode == ExtractionMode.INCREMENTAL

    def test_batch_extraction_config(self, test_settings):
        """Batch extraction should be configured."""
        batch = test_settings.extraction.batch
        assert batch.size > 0
        assert batch.parallel_workers > 0

    def test_incremental_extraction_config(self, test_settings):
        """Incremental extraction should be configured."""
        incremental = test_settings.extraction.incremental
        assert incremental.watermark_column is not None


class TestDataQualityConfiguration:
    """Test data quality configuration."""

    def test_data_quality_enabled(self, test_settings):
        """Data quality should be enabled by default."""
        assert test_settings.data_quality.enabled is True

    def test_data_quality_checks(self, test_settings):
        """Data quality checks should be configured."""
        checks = test_settings.data_quality.checks
        assert checks.null_check is True
        assert checks.duplicate_check is True
        assert checks.schema_validation is True

    def test_data_quality_failure_action(self, test_settings):
        """Data quality failure action should be set."""
        assert test_settings.data_quality.on_failure == DataQualityAction.WARN

    def test_quarantine_configuration(self, test_settings):
        """Quarantine configuration should be available."""
        assert test_settings.data_quality.quarantine.enabled is True


class TestConnectorsConfiguration:
    """Test connectors configuration."""

    def test_connector_timeout(self, test_settings):
        """Connector timeout should be set."""
        assert test_settings.connectors.timeout > 0

    def test_database_pool_size(self, test_settings):
        """Database connection pool should be configured."""
        assert test_settings.connectors.databases.pool_size > 0

    def test_retry_configuration(self, test_settings):
        """Retry configuration should be available."""
        assert test_settings.connectors.retry.max_attempts > 0


class TestPipelineConfiguration:
    """Test pipeline configuration."""

    def test_pipeline_mode(self, test_settings):
        """Pipeline mode should be set."""
        assert test_settings.pipeline.mode in ["parallel", "sequential"]

    def test_checkpoint_configuration(self, test_settings):
        """Checkpoint configuration should be available."""
        assert test_settings.pipeline.checkpoint.enabled is True
        assert test_settings.pipeline.checkpoint.interval > 0


class TestLoggingConfiguration:
    """Test logging configuration."""

    def test_default_log_level(self, test_settings):
        """Default log level should be set."""
        assert test_settings.logging.level in ["DEBUG", "INFO", "WARNING", "ERROR"]

    def test_log_format(self, test_settings):
        """Log format should be configurable."""
        assert test_settings.logging.format is not None


class TestSettingsValidation:
    """Test settings validation."""

    def test_settings_with_minimal_config(self):
        """Should create settings with minimal configuration."""
        settings = Settings()
        assert settings is not None

    def test_settings_env_override(self):
        """Should support environment variable overrides."""
        settings = Settings(
            storage__aws__region="eu-west-1",
        )
        assert settings.storage.aws.region == "eu-west-1"
