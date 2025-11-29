"""Google Cloud Storage implementation."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, BinaryIO, Iterator

from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError, NotFound
from google.oauth2 import service_account

from automic_etl.core.exceptions import StorageError
from automic_etl.storage.base import StorageBackend, StorageObject


class GCSStorage(StorageBackend):
    """Google Cloud Storage backend."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        project_id: str | None = None,
        credentials_file: str | None = None,
    ) -> None:
        super().__init__(bucket, prefix)
        self.project_id = project_id

        # Initialize client
        if credentials_file:
            credentials = service_account.Credentials.from_service_account_file(
                credentials_file
            )
            self._client = storage.Client(
                project=project_id,
                credentials=credentials,
            )
        else:
            # Use default credentials (ADC)
            self._client = storage.Client(project=project_id)

        self._bucket = self._client.bucket(bucket)

    def _handle_error(self, operation: str, path: str, error: Exception) -> None:
        """Convert GCS errors to StorageError."""
        raise StorageError(
            f"GCS {operation} failed: {str(error)}",
            provider="gcp",
            operation=operation,
            details={"path": path, "error_type": type(error).__name__},
        )

    def exists(self, path: str) -> bool:
        """Check if an object exists."""
        full_path = self._full_path(path)
        blob = self._bucket.blob(full_path)
        return blob.exists()

    def get(self, path: str) -> bytes:
        """Get object contents as bytes."""
        full_path = self._full_path(path)
        blob = self._bucket.blob(full_path)
        try:
            return blob.download_as_bytes()
        except NotFound:
            raise StorageError(
                f"Object not found: {path}",
                provider="gcp",
                operation="get",
                details={"path": path},
            )
        except GoogleCloudError as e:
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
        full_path = self._full_path(path)
        blob = self._bucket.blob(full_path)

        if metadata:
            blob.metadata = metadata

        try:
            if isinstance(data, bytes):
                blob.upload_from_string(data, content_type=content_type)
            else:
                blob.upload_from_file(data, content_type=content_type)
            return self.get_uri(path)
        except GoogleCloudError as e:
            self._handle_error("put", path, e)
            raise

    def delete(self, path: str) -> None:
        """Delete an object."""
        full_path = self._full_path(path)
        blob = self._bucket.blob(full_path)
        try:
            blob.delete()
        except NotFound:
            pass  # Object already deleted
        except GoogleCloudError as e:
            self._handle_error("delete", path, e)

    def copy(self, source_path: str, dest_path: str) -> str:
        """Copy an object within the same bucket."""
        source_full = self._full_path(source_path)
        dest_full = self._full_path(dest_path)

        source_blob = self._bucket.blob(source_full)
        dest_blob = self._bucket.blob(dest_full)

        try:
            self._bucket.copy_blob(source_blob, self._bucket, dest_full)
            return self.get_uri(dest_path)
        except GoogleCloudError as e:
            self._handle_error("copy", source_path, e)
            raise

    def move(self, source_path: str, dest_path: str) -> str:
        """Move an object within the same bucket."""
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

        list_kwargs: dict[str, Any] = {
            "prefix": full_prefix,
        }

        if not recursive:
            list_kwargs["delimiter"] = "/"

        if max_results:
            list_kwargs["max_results"] = max_results

        try:
            blobs = self._client.list_blobs(self.bucket, **list_kwargs)
            for blob in blobs:
                yield StorageObject(
                    key=blob.name,
                    size=blob.size or 0,
                    last_modified=blob.updated or datetime.utcnow(),
                    etag=blob.etag,
                    content_type=blob.content_type,
                    metadata=blob.metadata,
                )
        except GoogleCloudError as e:
            self._handle_error("list_objects", prefix, e)

    def list_prefixes(self, prefix: str = "") -> list[str]:
        """List 'directories' at the given prefix level."""
        full_prefix = self._full_path(prefix) if prefix else self.prefix
        if full_prefix and not full_prefix.endswith("/"):
            full_prefix += "/"

        try:
            iterator = self._client.list_blobs(
                self.bucket,
                prefix=full_prefix,
                delimiter="/",
            )
            # Consume iterator to get prefixes
            list(iterator)
            return list(iterator.prefixes)
        except GoogleCloudError as e:
            self._handle_error("list_prefixes", prefix, e)
            raise

    def get_metadata(self, path: str) -> StorageObject:
        """Get object metadata without downloading content."""
        full_path = self._full_path(path)
        blob = self._bucket.blob(full_path)

        try:
            blob.reload()
            return StorageObject(
                key=full_path,
                size=blob.size or 0,
                last_modified=blob.updated or datetime.utcnow(),
                etag=blob.etag,
                content_type=blob.content_type,
                metadata=blob.metadata,
            )
        except NotFound:
            raise StorageError(
                f"Object not found: {path}",
                provider="gcp",
                operation="get_metadata",
                details={"path": path},
            )
        except GoogleCloudError as e:
            self._handle_error("get_metadata", path, e)
            raise

    def set_metadata(self, path: str, metadata: dict[str, str]) -> None:
        """Set/update object metadata."""
        full_path = self._full_path(path)
        blob = self._bucket.blob(full_path)

        try:
            blob.metadata = metadata
            blob.patch()
        except GoogleCloudError as e:
            self._handle_error("set_metadata", path, e)

    def get_uri(self, path: str) -> str:
        """Get the full GCS URI for an object."""
        full_path = self._full_path(path)
        return f"gs://{self.bucket}/{full_path}"

    def generate_presigned_url(
        self,
        path: str,
        expiration: int = 3600,
        operation: str = "get",
    ) -> str:
        """Generate a presigned URL for temporary access."""
        full_path = self._full_path(path)
        blob = self._bucket.blob(full_path)

        method = "GET" if operation == "get" else "PUT"

        try:
            url = blob.generate_signed_url(
                version="v4",
                expiration=timedelta(seconds=expiration),
                method=method,
            )
            return url
        except GoogleCloudError as e:
            self._handle_error("generate_presigned_url", path, e)
            raise

    def delete_objects(self, paths: list[str]) -> int:
        """Batch delete multiple objects."""
        if not paths:
            return 0

        blobs = [self._bucket.blob(self._full_path(p)) for p in paths]

        try:
            # GCS has a batch delete helper
            with self._client.batch():
                for blob in blobs:
                    blob.delete()
            return len(paths)
        except GoogleCloudError as e:
            self._handle_error("delete_objects", str(paths[:3]), e)
            raise
