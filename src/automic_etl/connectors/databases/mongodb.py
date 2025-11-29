"""MongoDB database connector."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl
from pymongo import MongoClient
from pymongo.database import Database

from automic_etl.connectors.base import (
    ConnectorConfig,
    ConnectorType,
    DatabaseConnector,
    ExtractionResult,
)
from automic_etl.core.exceptions import ConnectionError, ExtractionError


@dataclass
class MongoDBConfig(ConnectorConfig):
    """MongoDB connection configuration."""

    uri: str = "mongodb://localhost:27017"
    database: str = ""
    auth_source: str = "admin"
    max_pool_size: int = 10

    def __post_init__(self) -> None:
        self.connector_type = ConnectorType.DATABASE


class MongoDBConnector(DatabaseConnector):
    """MongoDB database connector."""

    def __init__(self, config: MongoDBConfig) -> None:
        super().__init__(config)
        self.mongo_config = config
        self._client: MongoClient | None = None
        self._db: Database | None = None

    def connect(self) -> None:
        """Establish connection to MongoDB."""
        try:
            self._client = MongoClient(
                self.mongo_config.uri,
                maxPoolSize=self.mongo_config.max_pool_size,
                serverSelectionTimeoutMS=self.config.timeout * 1000,
                authSource=self.mongo_config.auth_source,
            )
            self._db = self._client[self.mongo_config.database]
            # Test connection
            self._client.server_info()
            self._connected = True
            self.logger.info(
                "Connected to MongoDB",
                database=self.mongo_config.database,
            )
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to MongoDB: {str(e)}",
                connector_type="mongodb",
                details={"database": self.mongo_config.database},
            )

    def disconnect(self) -> None:
        """Close the connection."""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
        self._connected = False
        self.logger.info("Disconnected from MongoDB")

    def test_connection(self) -> bool:
        """Test if the connection is valid."""
        try:
            self._client.server_info()
            return True
        except Exception:
            return False

    def extract(
        self,
        query: str | None = None,
        collection: str | None = None,
        filter_dict: dict[str, Any] | None = None,
        projection: dict[str, int] | None = None,
        limit: int | None = None,
        skip: int | None = None,
        sort: list[tuple[str, int]] | None = None,
        **kwargs: Any,
    ) -> ExtractionResult:
        """Extract data from MongoDB collection."""
        self._validate_connection()

        if collection is None:
            raise ExtractionError(
                "Collection must be specified",
                source="mongodb",
            )

        try:
            coll = self._db[collection]
            filter_dict = filter_dict or {}

            cursor = coll.find(filter_dict, projection)

            if sort:
                cursor = cursor.sort(sort)

            if skip:
                cursor = cursor.skip(skip)

            if limit:
                cursor = cursor.limit(limit)

            # Convert to list of dicts
            documents = list(cursor)

            # Convert ObjectId to string
            for doc in documents:
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])

            # Create Polars DataFrame
            df = pl.DataFrame(documents) if documents else pl.DataFrame()

            return ExtractionResult(
                data=df,
                row_count=len(df),
                metadata={
                    "collection": collection,
                    "filter": str(filter_dict)[:200],
                },
            )
        except Exception as e:
            raise ExtractionError(
                f"Failed to extract data: {str(e)}",
                source="mongodb",
                details={"collection": collection},
            )

    def extract_incremental(
        self,
        watermark_column: str,
        last_watermark: Any | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> ExtractionResult:
        """Extract data incrementally based on watermark."""
        self._validate_connection()

        filter_dict = kwargs.get("filter_dict", {})

        if last_watermark is not None:
            filter_dict[watermark_column] = {"$gt": last_watermark}

        # Sort by watermark column
        sort = [(watermark_column, 1)]

        result = self.extract(
            collection=collection,
            filter_dict=filter_dict,
            sort=sort,
            **kwargs,
        )

        # Get new watermark
        new_watermark = None
        if not result.data.is_empty() and watermark_column in result.data.columns:
            new_watermark = result.data.select(pl.col(watermark_column).max()).item()

        result.watermark = new_watermark
        return result

    def get_tables(self) -> list[str]:
        """List available collections."""
        self._validate_connection()
        return self._db.list_collection_names()

    def get_table_schema(self, table: str) -> dict[str, str]:
        """Get schema for a collection (from sample document)."""
        self._validate_connection()

        sample = self._db[table].find_one()
        if sample is None:
            return {}

        def get_type(value: Any) -> str:
            if value is None:
                return "null"
            return type(value).__name__

        return {key: get_type(value) for key, value in sample.items()}

    def get_row_count(self, table: str) -> int:
        """Get document count for a collection."""
        self._validate_connection()
        return self._db[table].count_documents({})

    def aggregate(
        self,
        collection: str,
        pipeline: list[dict[str, Any]],
    ) -> ExtractionResult:
        """Run an aggregation pipeline."""
        self._validate_connection()

        try:
            coll = self._db[collection]
            cursor = coll.aggregate(pipeline)
            documents = list(cursor)

            for doc in documents:
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])

            df = pl.DataFrame(documents) if documents else pl.DataFrame()

            return ExtractionResult(
                data=df,
                row_count=len(df),
                metadata={
                    "collection": collection,
                    "pipeline_stages": len(pipeline),
                },
            )
        except Exception as e:
            raise ExtractionError(
                f"Aggregation failed: {str(e)}",
                source="mongodb",
                details={"collection": collection},
            )
