"""MySQL database connector."""

from __future__ import annotations

from dataclasses import dataclass
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
class MySQLConfig(ConnectorConfig):
    """MySQL connection configuration."""

    host: str = "localhost"
    port: int = 3306
    database: str = ""
    user: str = ""
    password: str = ""
    charset: str = "utf8mb4"
    pool_size: int = 5
    max_overflow: int = 10

    def __post_init__(self) -> None:
        self.connector_type = ConnectorType.DATABASE


class MySQLConnector(DatabaseConnector):
    """MySQL database connector."""

    def __init__(self, config: MySQLConfig) -> None:
        super().__init__(config)
        self.mysql_config = config
        self._engine: Engine | None = None

    def _get_connection_string(self) -> str:
        """Build the connection string."""
        return (
            f"mysql+pymysql://{self.mysql_config.user}:{self.mysql_config.password}"
            f"@{self.mysql_config.host}:{self.mysql_config.port}/{self.mysql_config.database}"
            f"?charset={self.mysql_config.charset}"
        )

    def connect(self) -> None:
        """Establish connection to MySQL."""
        try:
            connection_string = self._get_connection_string()
            self._engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=self.mysql_config.pool_size,
                max_overflow=self.mysql_config.max_overflow,
                pool_timeout=self.config.timeout,
            )
            self._connected = True
            self.logger.info(
                "Connected to MySQL",
                host=self.mysql_config.host,
                database=self.mysql_config.database,
            )
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to MySQL: {str(e)}",
                connector_type="mysql",
                details={
                    "host": self.mysql_config.host,
                    "database": self.mysql_config.database,
                },
            )

    def disconnect(self) -> None:
        """Close the connection."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
        self._connected = False
        self.logger.info("Disconnected from MySQL")

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
                source="mysql",
            )

        if query is None:
            cols = ", ".join(columns) if columns else "*"
            query = f"SELECT {cols} FROM {table}"

            if filter_expr:
                query += f" WHERE {filter_expr}"

            if limit:
                query += f" LIMIT {limit}"

            if offset:
                query += f" OFFSET {offset}"

        try:
            df = pl.read_database(query, self._get_connection_string())

            return ExtractionResult(
                data=df,
                row_count=len(df),
                metadata={
                    "query": query[:500],
                    "database": self.mysql_config.database,
                },
            )
        except Exception as e:
            raise ExtractionError(
                f"Failed to extract data: {str(e)}",
                source="mysql",
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
                source="mysql",
            )

        if query is None:
            query = f"SELECT * FROM {table}"

            if last_watermark is not None:
                query += f" WHERE {watermark_column} > '{last_watermark}'"

            query += f" ORDER BY {watermark_column}"

        result = self.extract(query=query, **kwargs)

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
            WHERE table_schema = :database
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """

        with self._engine.connect() as conn:
            result = conn.execute(text(query), {"database": self.mysql_config.database})
            return [row[0] for row in result]

    def get_table_schema(self, table: str) -> dict[str, str]:
        """Get schema for a table."""
        self._validate_connection()

        query = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = :database
            AND table_name = :table
            ORDER BY ordinal_position
        """

        with self._engine.connect() as conn:
            result = conn.execute(
                text(query),
                {"database": self.mysql_config.database, "table": table},
            )
            return {row[0]: row[1] for row in result}

    def get_row_count(self, table: str) -> int:
        """Get row count for a table."""
        self._validate_connection()

        query = f"SELECT COUNT(*) FROM {table}"

        with self._engine.connect() as conn:
            result = conn.execute(text(query))
            return result.scalar()
