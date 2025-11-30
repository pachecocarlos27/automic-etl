"""Apache Spark integration for distributed data processing."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Literal
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class SparkConfig:
    """Spark configuration."""

    app_name: str = "automic-etl"
    master: str = "local[*]"  # local[*], spark://host:port, yarn, k8s://...

    # Executor settings
    executor_memory: str = "4g"
    executor_cores: int = 2
    num_executors: int | None = None
    executor_instances: int | None = None

    # Driver settings
    driver_memory: str = "2g"
    driver_cores: int = 1

    # Spark configurations
    spark_conf: dict[str, str] = field(default_factory=dict)

    # Delta Lake
    enable_delta: bool = True
    delta_log_retention: str = "interval 30 days"

    # Iceberg
    enable_iceberg: bool = False
    iceberg_catalog: str = "spark_catalog"
    iceberg_warehouse: str | None = None

    # Hive Metastore
    enable_hive: bool = False
    hive_metastore_uri: str | None = None

    # AWS/S3 settings
    aws_access_key: str | None = None
    aws_secret_key: str | None = None
    s3_endpoint: str | None = None

    # Additional JARs
    packages: list[str] = field(default_factory=list)
    jars: list[str] = field(default_factory=list)


class SparkIntegration:
    """
    Apache Spark integration for large-scale data processing.

    Features:
    - Delta Lake and Iceberg support
    - Distributed transformations
    - SQL and DataFrame APIs
    - Medallion architecture support
    - Schema evolution handling

    Example:
        config = SparkConfig(
            app_name="my-etl",
            master="spark://localhost:7077",
            enable_delta=True
        )

        spark = SparkIntegration(config)
        spark.start()

        # Read from bronze layer
        df = spark.read_delta("s3://bucket/bronze/sales")

        # Transform
        result = spark.sql("SELECT * FROM sales WHERE amount > 100")

        # Write to silver
        spark.write_delta(result, "s3://bucket/silver/filtered_sales")
    """

    def __init__(self, config: SparkConfig):
        self.config = config
        self._spark = None
        self._started = False

    @property
    def spark(self):
        """Get the SparkSession."""
        if not self._started:
            self.start()
        return self._spark

    def start(self) -> None:
        """Start Spark session."""
        try:
            from pyspark.sql import SparkSession

            builder = SparkSession.builder.appName(self.config.app_name)

            # Set master
            builder = builder.master(self.config.master)

            # Build packages list
            packages = list(self.config.packages)
            if self.config.enable_delta:
                packages.append("io.delta:delta-spark_2.12:3.0.0")
            if self.config.enable_iceberg:
                packages.append("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2")

            if packages:
                builder = builder.config("spark.jars.packages", ",".join(packages))

            if self.config.jars:
                builder = builder.config("spark.jars", ",".join(self.config.jars))

            # Memory and executor settings
            builder = builder.config("spark.executor.memory", self.config.executor_memory)
            builder = builder.config("spark.executor.cores", str(self.config.executor_cores))
            builder = builder.config("spark.driver.memory", self.config.driver_memory)
            builder = builder.config("spark.driver.cores", str(self.config.driver_cores))

            if self.config.num_executors:
                builder = builder.config("spark.executor.instances", str(self.config.num_executors))

            # Delta Lake configuration
            if self.config.enable_delta:
                builder = builder.config(
                    "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
                )
                builder = builder.config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog"
                )
                builder = builder.config(
                    "spark.databricks.delta.retentionDurationCheck.enabled", "false"
                )

            # Iceberg configuration
            if self.config.enable_iceberg:
                builder = builder.config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
                )
                builder = builder.config(
                    f"spark.sql.catalog.{self.config.iceberg_catalog}",
                    "org.apache.iceberg.spark.SparkCatalog"
                )
                builder = builder.config(
                    f"spark.sql.catalog.{self.config.iceberg_catalog}.type", "hadoop"
                )
                if self.config.iceberg_warehouse:
                    builder = builder.config(
                        f"spark.sql.catalog.{self.config.iceberg_catalog}.warehouse",
                        self.config.iceberg_warehouse
                    )

            # Hive configuration
            if self.config.enable_hive:
                builder = builder.enableHiveSupport()
                if self.config.hive_metastore_uri:
                    builder = builder.config(
                        "spark.hadoop.hive.metastore.uris",
                        self.config.hive_metastore_uri
                    )

            # S3 configuration
            if self.config.aws_access_key:
                builder = builder.config(
                    "spark.hadoop.fs.s3a.access.key", self.config.aws_access_key
                )
                builder = builder.config(
                    "spark.hadoop.fs.s3a.secret.key", self.config.aws_secret_key
                )
            if self.config.s3_endpoint:
                builder = builder.config(
                    "spark.hadoop.fs.s3a.endpoint", self.config.s3_endpoint
                )

            # Additional configurations
            for key, value in self.config.spark_conf.items():
                builder = builder.config(key, value)

            self._spark = builder.getOrCreate()
            self._started = True

            logger.info(f"Started Spark session: {self.config.app_name}")

        except ImportError:
            raise ImportError(
                "pyspark is required. Install with: pip install pyspark"
            )

    def stop(self) -> None:
        """Stop Spark session."""
        if self._spark:
            self._spark.stop()
            self._spark = None
            self._started = False
            logger.info("Stopped Spark session")

    def sql(self, query: str) -> Any:
        """
        Execute SQL query.

        Args:
            query: SQL query string

        Returns:
            Spark DataFrame
        """
        return self.spark.sql(query)

    def read_delta(
        self,
        path: str,
        version: int | None = None,
        timestamp: str | None = None,
    ) -> Any:
        """
        Read Delta Lake table.

        Args:
            path: Path to Delta table
            version: Specific version to read
            timestamp: Timestamp for time travel

        Returns:
            Spark DataFrame
        """
        reader = self.spark.read.format("delta")

        if version is not None:
            reader = reader.option("versionAsOf", version)
        elif timestamp:
            reader = reader.option("timestampAsOf", timestamp)

        return reader.load(path)

    def write_delta(
        self,
        df: Any,
        path: str,
        mode: Literal["append", "overwrite", "merge"] = "append",
        partition_by: list[str] | None = None,
        merge_condition: str | None = None,
        merge_update: dict[str, str] | None = None,
    ) -> None:
        """
        Write to Delta Lake table.

        Args:
            df: Spark DataFrame to write
            path: Path to Delta table
            mode: Write mode
            partition_by: Columns to partition by
            merge_condition: Condition for merge (when mode='merge')
            merge_update: Update expressions for merge
        """
        if mode == "merge":
            self._merge_delta(df, path, merge_condition, merge_update)
        else:
            writer = df.write.format("delta").mode(mode)

            if partition_by:
                writer = writer.partitionBy(*partition_by)

            writer.save(path)

        logger.info(f"Wrote Delta table to {path}")

    def _merge_delta(
        self,
        df: Any,
        path: str,
        condition: str,
        update_map: dict[str, str] | None = None,
    ) -> None:
        """Perform Delta Lake merge."""
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forPath(self.spark, path)

        merge_builder = delta_table.alias("target").merge(
            df.alias("source"),
            condition
        )

        if update_map:
            merge_builder = merge_builder.whenMatchedUpdate(set=update_map)
        else:
            merge_builder = merge_builder.whenMatchedUpdateAll()

        merge_builder.whenNotMatchedInsertAll().execute()

    def read_iceberg(
        self,
        table: str,
        snapshot_id: int | None = None,
        as_of_timestamp: str | None = None,
    ) -> Any:
        """
        Read Iceberg table.

        Args:
            table: Full table name (catalog.database.table)
            snapshot_id: Specific snapshot to read
            as_of_timestamp: Timestamp for time travel

        Returns:
            Spark DataFrame
        """
        reader = self.spark.read.format("iceberg")

        if snapshot_id:
            reader = reader.option("snapshot-id", snapshot_id)
        elif as_of_timestamp:
            reader = reader.option("as-of-timestamp", as_of_timestamp)

        return reader.load(table)

    def write_iceberg(
        self,
        df: Any,
        table: str,
        mode: Literal["append", "overwrite"] = "append",
        partition_by: list[str] | None = None,
    ) -> None:
        """
        Write to Iceberg table.

        Args:
            df: Spark DataFrame
            table: Full table name
            mode: Write mode
            partition_by: Columns to partition by
        """
        writer = df.write.format("iceberg").mode(mode)

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.saveAsTable(table)
        logger.info(f"Wrote Iceberg table: {table}")

    def read_parquet(self, path: str) -> Any:
        """Read Parquet files."""
        return self.spark.read.parquet(path)

    def read_csv(
        self,
        path: str,
        header: bool = True,
        infer_schema: bool = True,
        **options,
    ) -> Any:
        """Read CSV files."""
        return self.spark.read.csv(
            path,
            header=header,
            inferSchema=infer_schema,
            **options
        )

    def read_json(self, path: str, **options) -> Any:
        """Read JSON files."""
        return self.spark.read.json(path, **options)

    def read_jdbc(
        self,
        url: str,
        table: str,
        properties: dict[str, str] | None = None,
        partition_column: str | None = None,
        lower_bound: int | None = None,
        upper_bound: int | None = None,
        num_partitions: int = 10,
    ) -> Any:
        """
        Read from JDBC source.

        Args:
            url: JDBC connection URL
            table: Table name or query
            properties: Connection properties (user, password, driver)
            partition_column: Column for partitioning
            lower_bound: Lower bound for partition column
            upper_bound: Upper bound for partition column
            num_partitions: Number of partitions

        Returns:
            Spark DataFrame
        """
        reader = self.spark.read.format("jdbc").option("url", url).option("dbtable", table)

        if properties:
            for key, value in properties.items():
                reader = reader.option(key, value)

        if partition_column:
            reader = reader.option("partitionColumn", partition_column)
            reader = reader.option("lowerBound", str(lower_bound))
            reader = reader.option("upperBound", str(upper_bound))
            reader = reader.option("numPartitions", str(num_partitions))

        return reader.load()

    def create_temp_view(self, df: Any, name: str) -> None:
        """Create a temporary view from DataFrame."""
        df.createOrReplaceTempView(name)

    def optimize_delta(
        self,
        path: str,
        z_order_by: list[str] | None = None,
    ) -> None:
        """
        Optimize Delta Lake table.

        Args:
            path: Path to Delta table
            z_order_by: Columns for Z-ordering
        """
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forPath(self.spark, path)

        if z_order_by:
            delta_table.optimize().executeZOrderBy(*z_order_by)
        else:
            delta_table.optimize().executeCompaction()

        logger.info(f"Optimized Delta table at {path}")

    def vacuum_delta(self, path: str, retention_hours: int = 168) -> None:
        """
        Vacuum Delta Lake table.

        Args:
            path: Path to Delta table
            retention_hours: Hours of history to retain
        """
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forPath(self.spark, path)
        delta_table.vacuum(retention_hours)
        logger.info(f"Vacuumed Delta table at {path}")

    def get_delta_history(self, path: str, limit: int = 10) -> list[dict]:
        """Get Delta Lake table history."""
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forPath(self.spark, path)
        history = delta_table.history(limit)
        return [row.asDict() for row in history.collect()]

    def bronze_to_silver(
        self,
        bronze_path: str,
        silver_path: str,
        transformations: list[str] | None = None,
        dedupe_columns: list[str] | None = None,
        filter_condition: str | None = None,
    ) -> dict[str, Any]:
        """
        Transform data from Bronze to Silver layer.

        Args:
            bronze_path: Path to Bronze table
            silver_path: Path to Silver table
            transformations: SQL transformations to apply
            dedupe_columns: Columns for deduplication
            filter_condition: Filter condition

        Returns:
            Statistics about the transformation
        """
        # Read bronze
        df = self.read_delta(bronze_path)
        initial_count = df.count()

        # Apply filter
        if filter_condition:
            df = df.filter(filter_condition)

        # Apply transformations
        if transformations:
            self.create_temp_view(df, "bronze_data")
            for transform in transformations:
                df = self.sql(transform)

        # Deduplicate
        if dedupe_columns:
            df = df.dropDuplicates(dedupe_columns)

        final_count = df.count()

        # Write to silver
        self.write_delta(df, silver_path, mode="overwrite")

        return {
            "input_rows": initial_count,
            "output_rows": final_count,
            "filtered_rows": initial_count - final_count,
            "bronze_path": bronze_path,
            "silver_path": silver_path,
        }

    def silver_to_gold(
        self,
        silver_path: str,
        gold_path: str,
        aggregation_sql: str,
        partition_by: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Aggregate data from Silver to Gold layer.

        Args:
            silver_path: Path to Silver table
            gold_path: Path to Gold table
            aggregation_sql: SQL for aggregation
            partition_by: Partition columns for output

        Returns:
            Statistics
        """
        # Read silver
        silver_df = self.read_delta(silver_path)
        self.create_temp_view(silver_df, "silver_data")

        # Apply aggregation
        gold_df = self.sql(aggregation_sql)
        output_count = gold_df.count()

        # Write to gold
        self.write_delta(gold_df, gold_path, mode="overwrite", partition_by=partition_by)

        return {
            "output_rows": output_count,
            "silver_path": silver_path,
            "gold_path": gold_path,
        }

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False
