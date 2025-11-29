"""Azure Blob Storage implementation."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, BinaryIO, Iterator

from azure.core.exceptions import AzureError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    BlobServiceClient,
    ContainerClient,
    generate_blob_sas,
)

from automic_etl.core.exceptions import StorageError
from automic_etl.storage.base import StorageBackend, StorageObject


class AzureBlobStorage(StorageBackend):
    """Azure Blob Storage backend."""

    def __init__(
        self,
        container: str,
        prefix: str = "",
        storage_account: str | None = None,
        connection_string: str | None = None,
        use_managed_identity: bool = False,
    ) -> None:
        # Azure uses container instead of bucket
        super().__init__(container, prefix)
        self.storage_account = storage_account
        self.container_name = container

        # Initialize client
        if connection_string:
            self._service_client = BlobServiceClient.from_connection_string(
                connection_string
            )
        elif use_managed_identity or not connection_string:
            if not storage_account:
                raise StorageError(
                    "storage_account is required when using managed identity",
                    provider="azure",
                    operation="init",
                )
            credential = DefaultAzureCredential()
            account_url = f"https://{storage_account}.blob.core.windows.net"
            self._service_client = BlobServiceClient(
                account_url=account_url,
                credential=credential,
            )
        else:
            raise StorageError(
                "Either connection_string or use_managed_identity must be provided",
                provider="azure",
                operation="init",
            )

        self._container_client = self._service_client.get_container_client(container)

    def _handle_error(self, operation: str, path: str, error: Exception) -> None:
        """Convert Azure errors to StorageError."""
        raise StorageError(
            f"Azure Blob {operation} failed: {str(error)}",
            provider="azure",
            operation=operation,
            details={"path": path, "error_type": type(error).__name__},
        )

    def _get_blob_client(self, path: str) -> BlobClient:
        """Get a blob client for the given path."""
        full_path = self._full_path(path)
        return self._container_client.get_blob_client(full_path)

    def exists(self, path: str) -> bool:
        """Check if an object exists."""
        blob_client = self._get_blob_client(path)
        return blob_client.exists()

    def get(self, path: str) -> bytes:
        """Get object contents as bytes."""
        blob_client = self._get_blob_client(path)
        try:
            downloader = blob_client.download_blob()
            return downloader.readall()
        except ResourceNotFoundError:
            raise StorageError(
                f"Blob not found: {path}",
                provider="azure",
                operation="get",
                details={"path": path},
            )
        except AzureError as e:
            self._handle_error("get", path, e)
            raise

    def put(
        self,
        path: str,
        data: bytes | BinaryIO,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> str:
        """Put an object and return its URI."""
        blob_client = self._get_blob_client(path)

        try:
            blob_client.upload_blob(
                data,
                overwrite=True,
                content_settings={"content_type": content_type} if content_type else None,
                metadata=metadata,
            )
            return self.get_uri(path)
        except AzureError as e:
            self._handle_error("put", path, e)
            raise

    def delete(self, path: str) -> None:
        """Delete an object."""
        blob_client = self._get_blob_client(path)
        try:
            blob_client.delete_blob()
        except ResourceNotFoundError:
            pass  # Already deleted
        except AzureError as e:
            self._handle_error("delete", path, e)

    def copy(self, source_path: str, dest_path: str) -> str:
        """Copy an object within the same container."""
        source_blob = self._get_blob_client(source_path)
        dest_blob = self._get_blob_client(dest_path)

        try:
            dest_blob.start_copy_from_url(source_blob.url)
            return self.get_uri(dest_path)
        except AzureError as e:
            self._handle_error("copy", source_path, e)
            raise

    def move(self, source_path: str, dest_path: str) -> str:
        """Move an object within the same container."""
        uri = self.copy(source_path, dest_path)
        self.delete(source_path)
        return uri

    def list_objects(
        self,
        prefix: str = "",
        recursive: bool = True,
        max_results: int | None = None,
    ) -> Iterator[StorageObject]:
        """List objects with optional prefix filter."""
        full_prefix = self._full_path(prefix) if prefix else self.prefix

        try:
            blobs = self._container_client.list_blobs(
                name_starts_with=full_prefix,
            )

            count = 0
            for blob in blobs:
                # Skip "directories" if not recursive
                if not recursive and "/" in blob.name[len(full_prefix) :].strip("/"):
                    continue

                yield StorageObject(
                    key=blob.name,
                    size=blob.size,
                    last_modified=blob.last_modified,
                    etag=blob.etag,
                    content_type=blob.content_settings.content_type if blob.content_settings else None,
                    metadata=blob.metadata,
                )

                count += 1
                if max_results and count >= max_results:
                    return
        except AzureError as e:
            self._handle_error("list_objects", prefix, e)

    def list_prefixes(self, prefix: str = "") -> list[str]:
        """List 'directories' at the given prefix level."""
        full_prefix = self._full_path(prefix) if prefix else self.prefix
        if full_prefix and not full_prefix.endswith("/"):
            full_prefix += "/"

        try:
            blobs = self._container_client.walk_blobs(
                name_starts_with=full_prefix,
                delimiter="/",
            )

            prefixes = []
            for blob in blobs:
                if hasattr(blob, "prefix"):
                    prefixes.append(blob.prefix)

            return prefixes
        except AzureError as e:
            self._handle_error("list_prefixes", prefix, e)
            raise

    def get_metadata(self, path: str) -> StorageObject:
        """Get object metadata without downloading content."""
        blob_client = self._get_blob_client(path)

        try:
            properties = blob_client.get_blob_properties()
            return StorageObject(
                key=self._full_path(path),
                size=properties.size,
                last_modified=properties.last_modified,
                etag=properties.etag,
                content_type=properties.content_settings.content_type if properties.content_settings else None,
                metadata=properties.metadata,
            )
        except ResourceNotFoundError:
            raise StorageError(
                f"Blob not found: {path}",
                provider="azure",
                operation="get_metadata",
                details={"path": path},
            )
        except AzureError as e:
            self._handle_error("get_metadata", path, e)
            raise

    def set_metadata(self, path: str, metadata: dict[str, str]) -> None:
        """Set/update object metadata."""
        blob_client = self._get_blob_client(path)

        try:
            blob_client.set_blob_metadata(metadata)
        except AzureError as e:
            self._handle_error("set_metadata", path, e)

    def get_uri(self, path: str) -> str:
        """Get the full Azure URI for an object."""
        full_path = self._full_path(path)
        if self.storage_account:
            return f"abfss://{self.container_name}@{self.storage_account}.dfs.core.windows.net/{full_path}"
        # Fallback to https URL
        return f"https://{self._service_client.account_name}.blob.core.windows.net/{self.container_name}/{full_path}"

    def generate_presigned_url(
        self,
        path: str,
        expiration: int = 3600,
        operation: str = "get",
    ) -> str:
        """Generate a presigned URL (SAS URL) for temporary access."""
        full_path = self._full_path(path)

        permission = BlobSasPermissions(
            read=(operation == "get"),
            write=(operation == "put"),
        )

        try:
            # Get account key from connection string or use user delegation
            sas_token = generate_blob_sas(
                account_name=self._service_client.account_name,
                container_name=self.container_name,
                blob_name=full_path,
                permission=permission,
                expiry=datetime.utcnow() + timedelta(seconds=expiration),
            )

            blob_url = f"https://{self._service_client.account_name}.blob.core.windows.net/{self.container_name}/{full_path}"
            return f"{blob_url}?{sas_token}"
        except AzureError as e:
            self._handle_error("generate_presigned_url", path, e)
            raise

    def delete_objects(self, paths: list[str]) -> int:
        """Batch delete multiple objects."""
        if not paths:
            return 0

        deleted_count = 0
        for path in paths:
            try:
                self.delete(path)
                deleted_count += 1
            except StorageError:
                pass  # Continue with other deletions

        return deleted_count
