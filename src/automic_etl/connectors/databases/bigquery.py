"""Google BigQuery connector for cloud analytics."""

from __future__ import annotations

from typing import Any, Iterator
from datetime import datetime
from pathlib import Path

import polars as pl
import structlog

from automic_etl.connectors.base import DatabaseConnector

logger = structlog.get_logger()


class BigQueryConnector(DatabaseConnector):
    """
    Google BigQuery connector for cloud data analytics.

    Features:
    - Service account and user authentication
    - Standard and legacy SQL support
    - Batch and streaming inserts
    - Table partitioning and clustering
    - Federated queries (GCS, Drive)
    - BigQuery ML integration
    """

    def __init__(
        self,
        project: str,
        credentials_path: str | None = None,
        credentials_json: dict | None = None,
        location: str = "US",
        dataset: str | None = None,
    ) -> None:
        """
        Initialize BigQuery connector.

        Args:
            project: GCP project ID
            credentials_path: Path to service account JSON file
            credentials_json: Service account credentials as dict
            location: Default location for jobs
            dataset: Default dataset
        """
        self.project = project
        self.credentials_path = credentials_path
        self.credentials_json = credentials_json
        self.location = location
        self.dataset = dataset

        self._client = None
        self.logger = logger.bind(connector="bigquery", project=project)

    def connect(self) -> None:
        """Establish connection to BigQuery."""
        from google.cloud import bigquery
        from google.oauth2 import service_account

        if self.credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path
            )
        elif self.credentials_json:
            credentials = service_account.Credentials.from_service_account_info(
                self.credentials_json
            )
        else:
            # Use default credentials
            credentials = None

        self._client = bigquery.Client(
            project=self.project,
            credentials=credentials,
            location=self.location,
        )

        self.logger.info("Connected to BigQuery", project=self.project)

    def disconnect(self) -> None:
        """Close BigQuery connection."""
        if self._client:
            self._client.close()
            self._client = None
        self.logger.info("Disconnected from BigQuery")

    def execute(self, query: str, params: dict | None = None) -> None:
        """Execute a query without returning results."""
        job_config = None
        if params:
            from google.cloud import bigquery
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(k, "STRING", v)
                    for k, v in params.items()
                ]
            )

        query_job = self._client.query(query, job_config=job_config)
        query_job.result()  # Wait for completion

    def fetch_all(self, query: str, params: dict | None = None) -> list[dict[str, Any]]:
        """Execute query and fetch all results."""
        job_config = None
        if params:
            from google.cloud import bigquery
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(k, "STRING", v)
                    for k, v in params.items()
                ]
            )

        query_job = self._client.query(query, job_config=job_config)
        results = query_job.result()

        return [dict(row) for row in results]

    def fetch_dataframe(
        self,
        query: str,
        params: dict | None = None,
    ) -> pl.DataFrame:
        """Execute query and return as Polars DataFrame."""
        job_config = None
        if params:
            from google.cloud import bigquery
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(k, "STRING", v)
                    for k, v in params.items()
                ]
            )

        query_job = self._client.query(query, job_config=job_config)

        # Use Arrow for efficient transfer
        arrow_table = query_job.result().to_arrow()
        return pl.from_arrow(arrow_table)

    def fetch_to_dataframe_batched(
        self,
        query: str,
        batch_size: int = 100000,
    ) -> Iterator[pl.DataFrame]:
        """
        Execute query and yield results in batches.

        Args:
            query: SQL query
            batch_size: Rows per batch

        Yields:
            DataFrame batches
        """
        query_job = self._client.query(query)

        for page in query_job.result().pages:
            arrow_table = page.to_arrow()
            yield pl.from_arrow(arrow_table)

    def get_datasets(self) -> list[str]:
        """List datasets in the project."""
        return [ds.dataset_id for ds in self._client.list_datasets()]

    def get_tables(self, dataset: str | None = None) -> list[str]:
        """List tables in a dataset."""
        ds = dataset or self.dataset
        return [t.table_id for t in self._client.list_tables(ds)]

    def get_table_schema(self, table: str, dataset: str | None = None) -> list[dict]:
        """Get schema for a table."""
        ds = dataset or self.dataset
        table_ref = f"{self.project}.{ds}.{table}"
        table_obj = self._client.get_table(table_ref)

        return [
            {
                "name": field.name,
                "type": field.field_type,
                "mode": field.mode,
                "description": field.description,
            }
            for field in table_obj.schema
        ]

    def create_dataset(
        self,
        dataset_id: str,
        location: str | None = None,
        description: str | None = None,
    ) -> None:
        """Create a new dataset."""
        from google.cloud import bigquery

        dataset_ref = f"{self.project}.{dataset_id}"
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location or self.location
        if description:
            dataset.description = description

        self._client.create_dataset(dataset, exists_ok=True)
        self.logger.info("Dataset created", dataset=dataset_id)

    def create_table(
        self,
        table_id: str,
        schema: list[dict],
        dataset: str | None = None,
        partition_field: str | None = None,
        partition_type: str = "DAY",
        clustering_fields: list[str] | None = None,
        description: str | None = None,
    ) -> None:
        """
        Create a new table.

        Args:
            table_id: Table name
            schema: List of field definitions
            dataset: Dataset name
            partition_field: Field to partition by
            partition_type: HOUR, DAY, MONTH, YEAR
            clustering_fields: Fields to cluster by
            description: Table description
        """
        from google.cloud import bigquery

        ds = dataset or self.dataset
        table_ref = f"{self.project}.{ds}.{table_id}"

        bq_schema = [
            bigquery.SchemaField(
                name=f["name"],
                field_type=f["type"],
                mode=f.get("mode", "NULLABLE"),
                description=f.get("description"),
            )
            for f in schema
        ]

        table = bigquery.Table(table_ref, schema=bq_schema)

        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=partition_type,
                field=partition_field,
            )

        if clustering_fields:
            table.clustering_fields = clustering_fields

        if description:
            table.description = description

        self._client.create_table(table, exists_ok=True)
        self.logger.info("Table created", table=table_id)

    def insert_rows(
        self,
        table: str,
        rows: list[dict],
        dataset: str | None = None,
    ) -> list[dict]:
        """
        Insert rows using streaming insert.

        Args:
            table: Table name
            rows: List of row dictionaries
            dataset: Dataset name

        Returns:
            List of any errors
        """
        ds = dataset or self.dataset
        table_ref = f"{self.project}.{ds}.{table}"

        errors = self._client.insert_rows_json(table_ref, rows)
        if errors:
            self.logger.error("Insert errors", errors=errors)
        return errors

    def load_dataframe(
        self,
        df: pl.DataFrame,
        table: str,
        dataset: str | None = None,
        write_disposition: str = "WRITE_APPEND",
        schema_update_options: list[str] | None = None,
    ) -> None:
        """
        Load a DataFrame into BigQuery.

        Args:
            df: Polars DataFrame to load
            table: Target table
            dataset: Dataset name
            write_disposition: WRITE_TRUNCATE, WRITE_APPEND, or WRITE_EMPTY
            schema_update_options: Allow schema changes
        """
        from google.cloud import bigquery

        ds = dataset or self.dataset
        table_ref = f"{self.project}.{ds}.{table}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
        )
        if schema_update_options:
            job_config.schema_update_options = schema_update_options

        # Convert to Arrow for efficient loading
        arrow_table = df.to_arrow()

        load_job = self._client.load_table_from_dataframe(
            arrow_table.to_pandas(),  # BQ client expects pandas
            table_ref,
            job_config=job_config,
        )
        load_job.result()  # Wait for completion

        self.logger.info(
            "DataFrame loaded",
            table=table,
            rows=len(df),
        )

    def load_from_gcs(
        self,
        table: str,
        gcs_uri: str,
        source_format: str = "PARQUET",
        dataset: str | None = None,
        write_disposition: str = "WRITE_APPEND",
        schema: list[dict] | None = None,
    ) -> None:
        """
        Load data from Google Cloud Storage.

        Args:
            table: Target table
            gcs_uri: GCS URI (gs://bucket/path/*.parquet)
            source_format: CSV, JSON, PARQUET, AVRO, ORC
            dataset: Dataset name
            write_disposition: Write mode
            schema: Optional schema definition
        """
        from google.cloud import bigquery

        ds = dataset or self.dataset
        table_ref = f"{self.project}.{ds}.{table}"

        job_config = bigquery.LoadJobConfig(
            source_format=getattr(bigquery.SourceFormat, source_format),
            write_disposition=write_disposition,
        )

        if schema:
            job_config.schema = [
                bigquery.SchemaField(f["name"], f["type"], f.get("mode", "NULLABLE"))
                for f in schema
            ]

        load_job = self._client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config,
        )
        load_job.result()

        self.logger.info("Data loaded from GCS", table=table, uri=gcs_uri)

    def export_to_gcs(
        self,
        table: str,
        gcs_uri: str,
        destination_format: str = "PARQUET",
        dataset: str | None = None,
        compression: str | None = "SNAPPY",
    ) -> None:
        """
        Export table to Google Cloud Storage.

        Args:
            table: Source table
            gcs_uri: Destination GCS URI
            destination_format: CSV, JSON, PARQUET, AVRO
            dataset: Dataset name
            compression: Compression type
        """
        from google.cloud import bigquery

        ds = dataset or self.dataset
        table_ref = f"{self.project}.{ds}.{table}"

        job_config = bigquery.ExtractJobConfig(
            destination_format=getattr(bigquery.DestinationFormat, destination_format),
        )
        if compression:
            job_config.compression = compression

        extract_job = self._client.extract_table(
            table_ref,
            gcs_uri,
            job_config=job_config,
        )
        extract_job.result()

        self.logger.info("Table exported to GCS", table=table, uri=gcs_uri)

    def run_query_job(
        self,
        query: str,
        destination_table: str | None = None,
        dataset: str | None = None,
        write_disposition: str = "WRITE_TRUNCATE",
        priority: str = "INTERACTIVE",
    ) -> dict[str, Any]:
        """
        Run a query as a job with advanced options.

        Args:
            query: SQL query
            destination_table: Write results to this table
            dataset: Dataset for destination
            write_disposition: Write mode
            priority: INTERACTIVE or BATCH

        Returns:
            Job statistics
        """
        from google.cloud import bigquery

        job_config = bigquery.QueryJobConfig(
            priority=getattr(bigquery.QueryPriority, priority),
        )

        if destination_table:
            ds = dataset or self.dataset
            job_config.destination = f"{self.project}.{ds}.{destination_table}"
            job_config.write_disposition = write_disposition

        query_job = self._client.query(query, job_config=job_config)
        query_job.result()

        return {
            "job_id": query_job.job_id,
            "bytes_processed": query_job.total_bytes_processed,
            "bytes_billed": query_job.total_bytes_billed,
            "slot_millis": query_job.slot_millis,
            "cache_hit": query_job.cache_hit,
        }

    def create_view(
        self,
        view_name: str,
        query: str,
        dataset: str | None = None,
        description: str | None = None,
    ) -> None:
        """Create a view."""
        from google.cloud import bigquery

        ds = dataset or self.dataset
        view_ref = f"{self.project}.{ds}.{view_name}"

        view = bigquery.Table(view_ref)
        view.view_query = query
        if description:
            view.description = description

        self._client.create_table(view, exists_ok=True)
        self.logger.info("View created", view=view_name)

    def create_materialized_view(
        self,
        view_name: str,
        query: str,
        dataset: str | None = None,
        enable_refresh: bool = True,
        refresh_interval_minutes: int = 60,
    ) -> None:
        """Create a materialized view."""
        ds = dataset or self.dataset

        create_sql = f"""
        CREATE MATERIALIZED VIEW `{self.project}.{ds}.{view_name}`
        OPTIONS (
            enable_refresh = {str(enable_refresh).lower()},
            refresh_interval_minutes = {refresh_interval_minutes}
        )
        AS {query}
        """

        self.execute(create_sql)
        self.logger.info("Materialized view created", view=view_name)

    def run_ml_model(
        self,
        model_name: str,
        input_query: str,
        dataset: str | None = None,
    ) -> pl.DataFrame:
        """
        Run predictions using a BigQuery ML model.

        Args:
            model_name: ML model name
            input_query: Query for input data
            dataset: Dataset containing the model

        Returns:
            DataFrame with predictions
        """
        ds = dataset or self.dataset
        model_ref = f"`{self.project}.{ds}.{model_name}`"

        predict_sql = f"""
        SELECT *
        FROM ML.PREDICT(MODEL {model_ref}, ({input_query}))
        """

        return self.fetch_dataframe(predict_sql)

    def get_job_history(
        self,
        max_results: int = 100,
        state_filter: str | None = None,
    ) -> pl.DataFrame:
        """Get recent job history."""
        from google.cloud import bigquery

        jobs = self._client.list_jobs(max_results=max_results, state_filter=state_filter)

        job_data = []
        for job in jobs:
            job_data.append({
                "job_id": job.job_id,
                "job_type": job.job_type,
                "state": job.state,
                "created": job.created,
                "started": job.started,
                "ended": job.ended,
                "user_email": job.user_email,
                "bytes_processed": getattr(job, "total_bytes_processed", None),
            })

        return pl.DataFrame(job_data) if job_data else pl.DataFrame()

    def dry_run_query(self, query: str) -> dict[str, Any]:
        """
        Estimate query cost without executing.

        Returns:
            Estimated bytes to process and cost
        """
        from google.cloud import bigquery

        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        query_job = self._client.query(query, job_config=job_config)

        bytes_estimate = query_job.total_bytes_processed
        # BigQuery on-demand pricing: $5 per TB
        cost_estimate = (bytes_estimate / (1024**4)) * 5

        return {
            "bytes_processed": bytes_estimate,
            "estimated_cost_usd": round(cost_estimate, 4),
        }

    def test_connection(self) -> bool:
        """Test the BigQuery connection."""
        try:
            list(self._client.list_datasets(max_results=1))
            return True
        except Exception:
            return False
