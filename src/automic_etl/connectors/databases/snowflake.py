"""Snowflake connector for cloud data warehouse integration."""

from __future__ import annotations

from typing import Any, Iterator
from datetime import datetime

import polars as pl
import structlog

from automic_etl.connectors.base import DatabaseConnector

logger = structlog.get_logger()


class SnowflakeConnector(DatabaseConnector):
    """
    Snowflake Data Cloud connector.

    Features:
    - Multiple authentication methods (password, key pair, OAuth, SSO)
    - Warehouse and role management
    - Bulk data loading with COPY INTO
    - Time travel queries
    - Stream and task support for CDC
    - Snowpark integration
    """

    def __init__(
        self,
        account: str,
        user: str,
        password: str | None = None,
        private_key_path: str | None = None,
        private_key_passphrase: str | None = None,
        authenticator: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        warehouse: str | None = None,
        role: str | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize Snowflake connector.

        Args:
            account: Snowflake account identifier (e.g., 'xy12345.us-east-1')
            user: Username
            password: Password (for password auth)
            private_key_path: Path to private key file (for key pair auth)
            private_key_passphrase: Passphrase for private key
            authenticator: Auth method ('snowflake', 'externalbrowser', 'oauth', etc.)
            database: Default database
            schema: Default schema
            warehouse: Default warehouse
            role: Default role
        """
        self.account = account
        self.user = user
        self.password = password
        self.private_key_path = private_key_path
        self.private_key_passphrase = private_key_passphrase
        self.authenticator = authenticator
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.role = role
        self.extra_params = kwargs

        self._connection = None
        self._cursor = None
        self.logger = logger.bind(connector="snowflake", account=account)

    def connect(self) -> None:
        """Establish connection to Snowflake."""
        import snowflake.connector

        connect_params = {
            "account": self.account,
            "user": self.user,
        }

        if self.password:
            connect_params["password"] = self.password
        elif self.private_key_path:
            connect_params["private_key_file"] = self.private_key_path
            if self.private_key_passphrase:
                connect_params["private_key_file_pwd"] = self.private_key_passphrase

        if self.authenticator:
            connect_params["authenticator"] = self.authenticator

        if self.database:
            connect_params["database"] = self.database
        if self.schema:
            connect_params["schema"] = self.schema
        if self.warehouse:
            connect_params["warehouse"] = self.warehouse
        if self.role:
            connect_params["role"] = self.role

        connect_params.update(self.extra_params)

        self._connection = snowflake.connector.connect(**connect_params)
        self._cursor = self._connection.cursor()

        self.logger.info(
            "Connected to Snowflake",
            database=self.database,
            warehouse=self.warehouse,
        )

    def disconnect(self) -> None:
        """Close Snowflake connection."""
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        if self._connection:
            self._connection.close()
            self._connection = None
        self.logger.info("Disconnected from Snowflake")

    def execute(self, query: str, params: dict | tuple | None = None) -> None:
        """Execute a query without returning results."""
        self._cursor.execute(query, params)

    def fetch_all(self, query: str, params: dict | tuple | None = None) -> list[dict[str, Any]]:
        """Execute query and fetch all results."""
        self._cursor.execute(query, params)
        columns = [desc[0] for desc in self._cursor.description]
        rows = self._cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def fetch_dataframe(
        self,
        query: str,
        params: dict | tuple | None = None,
    ) -> pl.DataFrame:
        """Execute query and return as Polars DataFrame."""
        self._cursor.execute(query, params)
        columns = [desc[0].lower() for desc in self._cursor.description]
        rows = self._cursor.fetchall()

        if not rows:
            return pl.DataFrame()

        return pl.DataFrame(
            [dict(zip(columns, row)) for row in rows]
        )

    def fetch_arrow(self, query: str, params: dict | tuple | None = None) -> pl.DataFrame:
        """Execute query and fetch results as Arrow for better performance."""
        self._cursor.execute(query, params)
        arrow_table = self._cursor.fetch_arrow_all()

        if arrow_table is None:
            return pl.DataFrame()

        return pl.from_arrow(arrow_table)

    def use_warehouse(self, warehouse: str) -> None:
        """Switch to a different warehouse."""
        self.execute(f"USE WAREHOUSE {warehouse}")
        self.warehouse = warehouse

    def use_database(self, database: str) -> None:
        """Switch to a different database."""
        self.execute(f"USE DATABASE {database}")
        self.database = database

    def use_schema(self, schema: str) -> None:
        """Switch to a different schema."""
        self.execute(f"USE SCHEMA {schema}")
        self.schema = schema

    def use_role(self, role: str) -> None:
        """Switch to a different role."""
        self.execute(f"USE ROLE {role}")
        self.role = role

    def get_databases(self) -> list[str]:
        """List available databases."""
        results = self.fetch_all("SHOW DATABASES")
        return [r["name"] for r in results]

    def get_schemas(self, database: str | None = None) -> list[str]:
        """List schemas in a database."""
        db = database or self.database
        results = self.fetch_all(f"SHOW SCHEMAS IN DATABASE {db}")
        return [r["name"] for r in results]

    def get_tables(
        self,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[str]:
        """List tables in a schema."""
        db = database or self.database
        sch = schema or self.schema
        results = self.fetch_all(f"SHOW TABLES IN {db}.{sch}")
        return [r["name"] for r in results]

    def get_views(
        self,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[str]:
        """List views in a schema."""
        db = database or self.database
        sch = schema or self.schema
        results = self.fetch_all(f"SHOW VIEWS IN {db}.{sch}")
        return [r["name"] for r in results]

    def describe_table(self, table: str) -> pl.DataFrame:
        """Get table schema information."""
        return self.fetch_dataframe(f"DESCRIBE TABLE {table}")

    def get_table_ddl(self, table: str) -> str:
        """Get DDL for a table."""
        result = self.fetch_all(f"SELECT GET_DDL('TABLE', '{table}')")
        return result[0]["GET_DDL('TABLE', '{0}')".format(table.upper())]

    def query_with_time_travel(
        self,
        table: str,
        as_of: datetime | str | None = None,
        offset_seconds: int | None = None,
        statement_id: str | None = None,
    ) -> pl.DataFrame:
        """
        Query table with time travel.

        Args:
            table: Table name
            as_of: Timestamp to query at
            offset_seconds: Seconds in the past to query
            statement_id: Query ID to time travel to

        Returns:
            DataFrame with historical data
        """
        if as_of:
            if isinstance(as_of, datetime):
                as_of = as_of.strftime("%Y-%m-%d %H:%M:%S")
            query = f"SELECT * FROM {table} AT(TIMESTAMP => '{as_of}'::TIMESTAMP)"
        elif offset_seconds:
            query = f"SELECT * FROM {table} AT(OFFSET => -{offset_seconds})"
        elif statement_id:
            query = f"SELECT * FROM {table} AT(STATEMENT => '{statement_id}')"
        else:
            query = f"SELECT * FROM {table}"

        return self.fetch_arrow(query)

    def create_stream(
        self,
        stream_name: str,
        source_table: str,
        append_only: bool = False,
    ) -> None:
        """
        Create a stream for change data capture.

        Args:
            stream_name: Name for the stream
            source_table: Table to capture changes from
            append_only: If True, only capture inserts
        """
        mode = "APPEND_ONLY = TRUE" if append_only else ""
        self.execute(f"""
            CREATE OR REPLACE STREAM {stream_name}
            ON TABLE {source_table}
            {mode}
        """)
        self.logger.info("Stream created", stream=stream_name, source=source_table)

    def get_stream_changes(self, stream_name: str) -> pl.DataFrame:
        """
        Get changes from a stream.

        Returns DataFrame with METADATA$ACTION, METADATA$ISUPDATE columns.
        """
        return self.fetch_arrow(f"SELECT * FROM {stream_name}")

    def consume_stream(
        self,
        stream_name: str,
        target_table: str,
        merge_keys: list[str],
    ) -> dict[str, int]:
        """
        Consume stream changes into a target table using MERGE.

        Args:
            stream_name: Source stream
            target_table: Target table
            merge_keys: Columns to match on

        Returns:
            Counts of inserted, updated, deleted records
        """
        # Get stream data to build MERGE statement
        stream_df = self.get_stream_changes(stream_name)

        if stream_df.is_empty():
            return {"inserted": 0, "updated": 0, "deleted": 0}

        columns = [c for c in stream_df.columns if not c.startswith("METADATA$")]
        key_condition = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
        update_cols = [c for c in columns if c not in merge_keys]

        merge_sql = f"""
        MERGE INTO {target_table} t
        USING {stream_name} s
        ON {key_condition}
        WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' THEN DELETE
        WHEN MATCHED AND s.METADATA$ACTION = 'INSERT' THEN UPDATE SET
            {', '.join([f't.{c} = s.{c}' for c in update_cols])}
        WHEN NOT MATCHED AND s.METADATA$ACTION = 'INSERT' THEN INSERT
            ({', '.join(columns)})
            VALUES ({', '.join([f's.{c}' for c in columns])})
        """

        self.execute(merge_sql)
        # Get merge results
        return {"merged": self._cursor.rowcount}

    def copy_into_table(
        self,
        table: str,
        stage: str,
        file_format: str | None = None,
        pattern: str | None = None,
        options: dict[str, Any] | None = None,
    ) -> int:
        """
        Load data from stage into table using COPY INTO.

        Args:
            table: Target table
            stage: Stage name (e.g., '@my_stage/path/')
            file_format: File format name or inline format
            pattern: Regex pattern to match files
            options: Additional COPY options

        Returns:
            Number of rows loaded
        """
        copy_sql = f"COPY INTO {table} FROM {stage}"

        if file_format:
            copy_sql += f" FILE_FORMAT = ({file_format})"
        if pattern:
            copy_sql += f" PATTERN = '{pattern}'"

        if options:
            opts = " ".join([f"{k}={v}" for k, v in options.items()])
            copy_sql += f" {opts}"

        self.execute(copy_sql)
        return self._cursor.rowcount

    def unload_to_stage(
        self,
        query: str,
        stage: str,
        file_format: str | None = None,
        single: bool = False,
        overwrite: bool = True,
    ) -> None:
        """
        Unload query results to a stage.

        Args:
            query: SELECT query to unload
            stage: Target stage
            file_format: File format specification
            single: If True, create single file
            overwrite: If True, overwrite existing files
        """
        unload_sql = f"COPY INTO {stage} FROM ({query})"

        options = []
        if file_format:
            options.append(f"FILE_FORMAT = ({file_format})")
        if single:
            options.append("SINGLE = TRUE")
        if overwrite:
            options.append("OVERWRITE = TRUE")

        if options:
            unload_sql += " " + " ".join(options)

        self.execute(unload_sql)

    def create_task(
        self,
        task_name: str,
        warehouse: str,
        schedule: str,
        sql: str,
        enabled: bool = True,
    ) -> None:
        """
        Create a scheduled task.

        Args:
            task_name: Name for the task
            warehouse: Warehouse to run on
            schedule: Cron schedule (e.g., 'USING CRON 0 * * * * UTC')
            sql: SQL to execute
            enabled: Start task immediately
        """
        self.execute(f"""
            CREATE OR REPLACE TASK {task_name}
            WAREHOUSE = {warehouse}
            SCHEDULE = '{schedule}'
            AS
            {sql}
        """)

        if enabled:
            self.execute(f"ALTER TASK {task_name} RESUME")

        self.logger.info("Task created", task=task_name, schedule=schedule)

    def get_query_history(
        self,
        user: str | None = None,
        warehouse: str | None = None,
        hours: int = 24,
    ) -> pl.DataFrame:
        """Get query history for analysis."""
        filters = [f"START_TIME >= DATEADD(hour, -{hours}, CURRENT_TIMESTAMP())"]
        if user:
            filters.append(f"USER_NAME = '{user}'")
        if warehouse:
            filters.append(f"WAREHOUSE_NAME = '{warehouse}'")

        where = " AND ".join(filters)

        return self.fetch_arrow(f"""
            SELECT
                QUERY_ID,
                QUERY_TEXT,
                USER_NAME,
                WAREHOUSE_NAME,
                DATABASE_NAME,
                SCHEMA_NAME,
                QUERY_TYPE,
                EXECUTION_STATUS,
                START_TIME,
                END_TIME,
                TOTAL_ELAPSED_TIME,
                BYTES_SCANNED,
                ROWS_PRODUCED
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
            WHERE {where}
            ORDER BY START_TIME DESC
        """)

    def clone_table(
        self,
        source_table: str,
        target_table: str,
        at_timestamp: datetime | None = None,
    ) -> None:
        """
        Create a zero-copy clone of a table.

        Args:
            source_table: Source table name
            target_table: Target table name
            at_timestamp: Point in time to clone from
        """
        clone_sql = f"CREATE OR REPLACE TABLE {target_table} CLONE {source_table}"
        if at_timestamp:
            ts = at_timestamp.strftime("%Y-%m-%d %H:%M:%S")
            clone_sql += f" AT(TIMESTAMP => '{ts}'::TIMESTAMP)"

        self.execute(clone_sql)
        self.logger.info("Table cloned", source=source_table, target=target_table)

    def extract(
        self,
        table: str | None = None,
        query: str | None = None,
        batch_size: int = 100000,
    ) -> Iterator[pl.DataFrame]:
        """
        Extract data in batches.

        Args:
            table: Table to extract from
            query: Custom query (alternative to table)
            batch_size: Rows per batch

        Yields:
            DataFrame batches
        """
        if query is None:
            query = f"SELECT * FROM {table}"

        self._cursor.execute(query)

        while True:
            rows = self._cursor.fetchmany(batch_size)
            if not rows:
                break

            columns = [desc[0].lower() for desc in self._cursor.description]
            yield pl.DataFrame([dict(zip(columns, row)) for row in rows])

    def test_connection(self) -> bool:
        """Test the Snowflake connection."""
        try:
            self.fetch_all("SELECT CURRENT_VERSION()")
            return True
        except Exception:
            return False
