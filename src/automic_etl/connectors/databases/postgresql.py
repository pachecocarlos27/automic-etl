"""PostgreSQL database connector."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import polars as pl
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool

from automic_etl.connectors.base import (
    ConnectorConfig,
    ConnectorType,
    DatabaseConnector,
    ExtractionResult,
)
from automic_etl.core.exceptions import ConnectionError, ExtractionError


@dataclass
class PostgreSQLConfig(ConnectorConfig):
    """PostgreSQL connection configuration."""

    host: str = "localhost"
    port: int = 5432
    database: str = ""
    user: str = ""
    password: str = ""
    schema: str = "public"
    ssl_mode: str = "prefer"
    pool_size: int = 5
    max_overflow: int = 10

    def __post_init__(self) -> None:
        self.connector_type = ConnectorType.DATABASE


class PostgreSQLConnector(DatabaseConnector):
    """PostgreSQL database connector using SQLAlchemy and Polars."""

    def __init__(self, config: PostgreSQLConfig) -> None:
        super().__init__(config)
        self.pg_config = config
        self._engine: Engine | None = None

    def _get_connection_string(self) -> str:
        """Build the connection string."""
        return (
            f"postgresql://{self.pg_config.user}:{self.pg_config.password}"
            f"@{self.pg_config.host}:{self.pg_config.port}/{self.pg_config.database}"
            f"?sslmode={self.pg_config.ssl_mode}"
        )

    def connect(self) -> None:
        """Establish connection to PostgreSQL."""
        try:
            connection_string = self._get_connection_string()
            self._engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=self.pg_config.pool_size,
                max_overflow=self.pg_config.max_overflow,
                pool_timeout=self.config.timeout,
            )
            self._connected = True
            self.logger.info(
                "Connected to PostgreSQL",
                host=self.pg_config.host,
                database=self.pg_config.database,
            )
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to PostgreSQL: {str(e)}",
                connector_type="postgresql",
                details={
                    "host": self.pg_config.host,
                    "database": self.pg_config.database,
                },
            )

    def disconnect(self) -> None:
        """Close the connection."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
        self._connected = False
        self.logger.info("Disconnected from PostgreSQL")

    def test_connection(self) -> bool:
        """Test if the connection is valid."""
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    def extract(
        self,
        query: str | None = None,
        table: str | None = None,
        columns: list[str] | None = None,
        filter_expr: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        **kwargs: Any,
    ) -> ExtractionResult:
        """Extract data using a query or table specification."""
        self._validate_connection()

        if query is None and table is None:
            raise ExtractionError(
                "Either query or table must be specified",
                source="postgresql",
            )

        if query is None:
            # Build query from table specification
            cols = ", ".join(columns) if columns else "*"
            schema_prefix = f"{self.pg_config.schema}." if self.pg_config.schema else ""
            query = f"SELECT {cols} FROM {schema_prefix}{table}"

            if filter_expr:
                query += f" WHERE {filter_expr}"

            if limit:
                query += f" LIMIT {limit}"

            if offset:
                query += f" OFFSET {offset}"

        try:
            # Use Polars to read directly from the database
            df = pl.read_database(query, self._get_connection_string())

            return ExtractionResult(
                data=df,
                row_count=len(df),
                metadata={
                    "query": query[:500],  # Truncate for logging
                    "database": self.pg_config.database,
                },
            )
        except Exception as e:
            raise ExtractionError(
                f"Failed to extract data: {str(e)}",
                source="postgresql",
                details={"query": query[:200]},
            )

    def extract_incremental(
        self,
        watermark_column: str,
        last_watermark: Any | None = None,
        table: str | None = None,
        query: str | None = None,
        **kwargs: Any,
    ) -> ExtractionResult:
        """Extract data incrementally based on watermark."""
        self._validate_connection()

        if query is None and table is None:
            raise ExtractionError(
                "Either query or table must be specified",
                source="postgresql",
            )

        if query is None:
            schema_prefix = f"{self.pg_config.schema}." if self.pg_config.schema else ""
            query = f"SELECT * FROM {schema_prefix}{table}"

            if last_watermark is not None:
                query += f" WHERE {watermark_column} > '{last_watermark}'"

            query += f" ORDER BY {watermark_column}"

        result = self.extract(query=query, **kwargs)

        # Get new watermark
        new_watermark = None
        if not result.data.is_empty() and watermark_column in result.data.columns:
            new_watermark = result.data.select(pl.col(watermark_column).max()).item()

        result.watermark = new_watermark
        return result

    def get_tables(self) -> list[str]:
        """List available tables."""
        self._validate_connection()

        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :schema
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """

        with self._engine.connect() as conn:
            result = conn.execute(text(query), {"schema": self.pg_config.schema})
            return [row[0] for row in result]

    def get_table_schema(self, table: str) -> dict[str, str]:
        """Get schema for a table."""
        self._validate_connection()

        query = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = :schema
            AND table_name = :table
            ORDER BY ordinal_position
        """

        with self._engine.connect() as conn:
            result = conn.execute(
                text(query),
                {"schema": self.pg_config.schema, "table": table},
            )
            return {row[0]: row[1] for row in result}

    def get_row_count(self, table: str) -> int:
        """Get row count for a table."""
        self._validate_connection()

        schema_prefix = f"{self.pg_config.schema}." if self.pg_config.schema else ""
        query = f"SELECT COUNT(*) FROM {schema_prefix}{table}"

        with self._engine.connect() as conn:
            result = conn.execute(text(query))
            return result.scalar()

    def execute(self, query: str, params: dict[str, Any] | None = None) -> int:
        """Execute a non-select query."""
        self._validate_connection()

        with self._engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            conn.commit()
            return result.rowcount
