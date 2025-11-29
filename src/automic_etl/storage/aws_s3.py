"""AWS S3 storage implementation."""

from __future__ import annotations

from datetime import datetime
from io import BytesIO
from typing import Any, BinaryIO, Iterator

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from automic_etl.core.exceptions import StorageError
from automic_etl.storage.base import StorageBackend, StorageObject


class S3Storage(StorageBackend):
    """AWS S3 storage backend."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        region: str = "us-east-1",
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        endpoint_url: str | None = None,
        role_arn: str | None = None,
    ) -> None:
        super().__init__(bucket, prefix)
        self.region = region

        # Configure boto3 client
        config = Config(
            retries={"max_attempts": 3, "mode": "adaptive"},
            signature_version="s3v4",
        )

        session_kwargs: dict[str, Any] = {}
        if access_key_id and secret_access_key:
            session_kwargs["aws_access_key_id"] = access_key_id
            session_kwargs["aws_secret_access_key"] = secret_access_key

        session = boto3.Session(**session_kwargs)

        # Handle role assumption if specified
        if role_arn:
            sts = session.client("sts")
            assumed = sts.assume_role(
                RoleArn=role_arn,
                RoleSessionName="automic-etl-session",
            )
            credentials = assumed["Credentials"]
            self._client = boto3.client(
                "s3",
                region_name=region,
                endpoint_url=endpoint_url,
                aws_access_key_id=credentials["AccessKeyId"],
                aws_secret_access_key=credentials["SecretAccessKey"],
                aws_session_token=credentials["SessionToken"],
                config=config,
            )
        else:
            self._client = session.client(
                "s3",
                region_name=region,
                endpoint_url=endpoint_url,
                config=config,
            )

    def _handle_error(self, operation: str, path: str, error: ClientError) -> None:
        """Convert boto3 errors to StorageError."""
        error_code = error.response.get("Error", {}).get("Code", "Unknown")
        error_message = error.response.get("Error", {}).get("Message", str(error))

        raise StorageError(
            f"S3 {operation} failed: {error_message}",
            provider="aws",
            operation=operation,
            details={"path": path, "error_code": error_code},
        )

    def exists(self, path: str) -> bool:
        """Check if an object exists."""
        full_path = self._full_path(path)
        try:
            self._client.head_object(Bucket=self.bucket, Key=full_path)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            self._handle_error("exists", path, e)
            return False

    def get(self, path: str) -> bytes:
        """Get object contents as bytes."""
        full_path = self._full_path(path)
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=full_path)
            return response["Body"].read()
        except ClientError as e:
            self._handle_error("get", path, e)
            raise  # Never reached, but makes type checker happy

    def put(
        self,
        path: str,
        data: bytes | BinaryIO,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> str:
        """Put an object and return its URI."""
        full_path = self._full_path(path)

        put_kwargs: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": full_path,
        }

        if isinstance(data, bytes):
            put_kwargs["Body"] = data
        else:
            put_kwargs["Body"] = data.read()

        if content_type:
            put_kwargs["ContentType"] = content_type

        if metadata:
            put_kwargs["Metadata"] = metadata

        try:
            self._client.put_object(**put_kwargs)
            return self.get_uri(path)
        except ClientError as e:
            self._handle_error("put", path, e)
            raise

    def delete(self, path: str) -> None:
        """Delete an object."""
        full_path = self._full_path(path)
        try:
            self._client.delete_object(Bucket=self.bucket, Key=full_path)
        except ClientError as e:
            self._handle_error("delete", path, e)

    def copy(self, source_path: str, dest_path: str) -> str:
        """Copy an object within the same bucket."""
        source_full = self._full_path(source_path)
        dest_full = self._full_path(dest_path)

        try:
            self._client.copy_object(
                Bucket=self.bucket,
                CopySource={"Bucket": self.bucket, "Key": source_full},
                Key=dest_full,
            )
            return self.get_uri(dest_path)
        except ClientError as e:
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

        paginator = self._client.get_paginator("list_objects_v2")

        pagination_config: dict[str, Any] = {}
        if max_results:
            pagination_config["MaxItems"] = max_results
            pagination_config["PageSize"] = min(max_results, 1000)

        list_kwargs: dict[str, Any] = {
            "Bucket": self.bucket,
            "Prefix": full_prefix,
        }

        if not recursive:
            list_kwargs["Delimiter"] = "/"

        count = 0
        for page in paginator.paginate(**list_kwargs, PaginationConfig=pagination_config):
            for obj in page.get("Contents", []):
                yield StorageObject(
                    key=obj["Key"],
                    size=obj["Size"],
                    last_modified=obj["LastModified"],
                    etag=obj.get("ETag", "").strip('"'),
                )
                count += 1
                if max_results and count >= max_results:
                    return

    def list_prefixes(self, prefix: str = "") -> list[str]:
        """List 'directories' at the given prefix level."""
        full_prefix = self._full_path(prefix) if prefix else self.prefix
        if full_prefix and not full_prefix.endswith("/"):
            full_prefix += "/"

        response = self._client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=full_prefix,
            Delimiter="/",
        )

        prefixes = []
        for prefix_obj in response.get("CommonPrefixes", []):
            prefixes.append(prefix_obj["Prefix"])

        return prefixes

    def get_metadata(self, path: str) -> StorageObject:
        """Get object metadata without downloading content."""
        full_path = self._full_path(path)
        try:
            response = self._client.head_object(Bucket=self.bucket, Key=full_path)
            return StorageObject(
                key=full_path,
                size=response["ContentLength"],
                last_modified=response["LastModified"],
                etag=response.get("ETag", "").strip('"'),
                content_type=response.get("ContentType"),
                metadata=response.get("Metadata"),
            )
        except ClientError as e:
            self._handle_error("get_metadata", path, e)
            raise

    def set_metadata(self, path: str, metadata: dict[str, str]) -> None:
        """Set/update object metadata."""
        full_path = self._full_path(path)
        try:
            # S3 requires copying the object to update metadata
            self._client.copy_object(
                Bucket=self.bucket,
                CopySource={"Bucket": self.bucket, "Key": full_path},
                Key=full_path,
                Metadata=metadata,
                MetadataDirective="REPLACE",
            )
        except ClientError as e:
            self._handle_error("set_metadata", path, e)

    def get_uri(self, path: str) -> str:
        """Get the full S3 URI for an object."""
        full_path = self._full_path(path)
        return f"s3://{self.bucket}/{full_path}"

    def generate_presigned_url(
        self,
        path: str,
        expiration: int = 3600,
        operation: str = "get",
    ) -> str:
        """Generate a presigned URL for temporary access."""
        full_path = self._full_path(path)

        client_method = "get_object" if operation == "get" else "put_object"

        try:
            url = self._client.generate_presigned_url(
                ClientMethod=client_method,
                Params={"Bucket": self.bucket, "Key": full_path},
                ExpiresIn=expiration,
            )
            return url
        except ClientError as e:
            self._handle_error("generate_presigned_url", path, e)
            raise

    def delete_objects(self, paths: list[str]) -> int:
        """Batch delete multiple objects."""
        if not paths:
            return 0

        objects = [{"Key": self._full_path(p)} for p in paths]

        # S3 allows max 1000 objects per delete request
        deleted_count = 0
        for i in range(0, len(objects), 1000):
            batch = objects[i : i + 1000]
            try:
                response = self._client.delete_objects(
                    Bucket=self.bucket,
                    Delete={"Objects": batch},
                )
                deleted_count += len(response.get("Deleted", []))
            except ClientError as e:
                self._handle_error("delete_objects", str(paths[:3]), e)

        return deleted_count
