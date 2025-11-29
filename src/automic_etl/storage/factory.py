"""Factory for creating storage backends."""

from __future__ import annotations

from automic_etl.core.config import Settings, StorageProvider
from automic_etl.core.exceptions import ConfigurationError
from automic_etl.storage.aws_s3 import S3Storage
from automic_etl.storage.azure_blob import AzureBlobStorage
from automic_etl.storage.base import StorageBackend
from automic_etl.storage.gcs import GCSStorage


def create_storage(settings: Settings, prefix: str = "") -> StorageBackend:
    """
    Create a storage backend based on settings.

    Args:
        settings: Application settings
        prefix: Optional prefix for all operations

    Returns:
        StorageBackend: Configured storage backend
    """
    provider = settings.storage.provider

    if provider == StorageProvider.AWS:
        aws_config = settings.storage.aws
        if not aws_config.bucket:
            raise ConfigurationError(
                "AWS bucket is required",
                details={"provider": "aws"},
            )
        return S3Storage(
            bucket=aws_config.bucket,
            prefix=prefix,
            region=aws_config.region,
            access_key_id=aws_config.access_key_id,
            secret_access_key=aws_config.secret_access_key,
            endpoint_url=aws_config.endpoint_url,
            role_arn=aws_config.role_arn,
        )

    elif provider == StorageProvider.GCP:
        gcp_config = settings.storage.gcp
        if not gcp_config.bucket:
            raise ConfigurationError(
                "GCP bucket is required",
                details={"provider": "gcp"},
            )
        return GCSStorage(
            bucket=gcp_config.bucket,
            prefix=prefix,
            project_id=gcp_config.project_id,
            credentials_file=gcp_config.credentials_file,
        )

    elif provider == StorageProvider.AZURE:
        azure_config = settings.storage.azure
        if not azure_config.container:
            raise ConfigurationError(
                "Azure container is required",
                details={"provider": "azure"},
            )
        return AzureBlobStorage(
            container=azure_config.container,
            prefix=prefix,
            storage_account=azure_config.storage_account,
            connection_string=azure_config.connection_string,
            use_managed_identity=azure_config.use_managed_identity,
        )

    else:
        raise ConfigurationError(
            f"Unknown storage provider: {provider}",
            details={"provider": str(provider)},
        )


def create_storage_for_layer(
    settings: Settings,
    layer: str,
) -> StorageBackend:
    """
    Create a storage backend configured for a specific medallion layer.

    Args:
        settings: Application settings
        layer: Layer name ('bronze', 'silver', 'gold')

    Returns:
        StorageBackend: Configured storage backend with layer prefix
    """
    layer_configs = {
        "bronze": settings.medallion.bronze,
        "silver": settings.medallion.silver,
        "gold": settings.medallion.gold,
    }

    if layer not in layer_configs:
        raise ConfigurationError(
            f"Unknown medallion layer: {layer}",
            details={"layer": layer, "valid_layers": list(layer_configs.keys())},
        )

    layer_config = layer_configs[layer]
    return create_storage(settings, prefix=layer_config.path.strip("/"))
