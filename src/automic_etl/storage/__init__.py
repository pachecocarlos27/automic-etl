"""Storage module for cloud providers."""

from automic_etl.storage.base import StorageBackend, StorageObject
from automic_etl.storage.aws_s3 import S3Storage
from automic_etl.storage.gcs import GCSStorage
from automic_etl.storage.azure_blob import AzureBlobStorage
from automic_etl.storage.factory import create_storage

__all__ = [
    "StorageBackend",
    "StorageObject",
    "S3Storage",
    "GCSStorage",
    "AzureBlobStorage",
    "create_storage",
]
