"""Great Expectations integration for data validation."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal
import json

logger = logging.getLogger(__name__)


@dataclass
class GEConfig:
    """Great Expectations configuration."""

    project_dir: str | Path
    datasource_name: str = "automic_datasource"

    # Data source type
    datasource_type: Literal["pandas", "spark", "sql"] = "pandas"

    # SQL connection (if datasource_type='sql')
    connection_string: str | None = None

    # Spark config (if datasource_type='spark')
    spark_config: dict[str, Any] = field(default_factory=dict)

    # Validation settings
    result_format: Literal["BASIC", "SUMMARY", "COMPLETE"] = "SUMMARY"
    include_unexpected_rows: bool = True
    partial_unexpected_count: int = 20

    # Data Docs
    data_docs_site_name: str = "local_site"
    data_docs_base_directory: str = "data_docs"


class GreatExpectationsIntegration:
    """
    Great Expectations integration for data quality validation.

    Features:
    - Expectation suite management
    - Data validation with rich results
    - Data Docs generation
    - Checkpoint execution
    - Integration with Automic ETL pipelines

    Example:
        config = GEConfig(
            project_dir="/path/to/ge/project",
            datasource_type="pandas"
        )

        ge = GreatExpectationsIntegration(config)
        ge.init_project()

        # Create expectations
        ge.add_expectation(
            suite_name="sales_data",
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "customer_id"}
        )

        # Validate data
        results = ge.validate(data=df, suite_name="sales_data")
    """

    def __init__(self, config: GEConfig):
        self.config = config
        self.project_dir = Path(config.project_dir)
        self._context = None

    @property
    def context(self):
        """Get the Great Expectations context."""
        if self._context is None:
            self._load_or_create_context()
        return self._context

    def _load_or_create_context(self) -> None:
        """Load or create GE context."""
        try:
            import great_expectations as gx

            if (self.project_dir / "great_expectations.yml").exists():
                self._context = gx.get_context(
                    context_root_dir=str(self.project_dir)
                )
            else:
                self._context = gx.get_context(
                    project_root_dir=str(self.project_dir)
                )

            logger.info(f"Loaded GE context from {self.project_dir}")

        except ImportError:
            raise ImportError(
                "great-expectations is required. "
                "Install with: pip install great-expectations"
            )

    def init_project(self) -> Path:
        """
        Initialize a new Great Expectations project.

        Returns:
            Path to project directory
        """
        import great_expectations as gx

        self.project_dir.mkdir(parents=True, exist_ok=True)

        self._context = gx.get_context(
            project_root_dir=str(self.project_dir)
        )

        logger.info(f"Initialized GE project at {self.project_dir}")
        return self.project_dir

    def add_datasource(
        self,
        name: str | None = None,
        datasource_type: str | None = None,
        connection_string: str | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Add a datasource to the project.

        Args:
            name: Datasource name
            datasource_type: Type of datasource
            connection_string: SQL connection string
            **kwargs: Additional configuration

        Returns:
            Datasource configuration
        """
        name = name or self.config.datasource_name
        ds_type = datasource_type or self.config.datasource_type
        conn_string = connection_string or self.config.connection_string

        if ds_type == "pandas":
            datasource = self.context.sources.add_pandas(name)

        elif ds_type == "spark":
            datasource = self.context.sources.add_spark(
                name,
                spark_config=self.config.spark_config or kwargs.get("spark_config", {})
            )

        elif ds_type == "sql":
            if not conn_string:
                raise ValueError("connection_string required for SQL datasource")
            datasource = self.context.sources.add_sql(
                name,
                connection_string=conn_string
            )

        else:
            raise ValueError(f"Unknown datasource type: {ds_type}")

        logger.info(f"Added datasource: {name} ({ds_type})")
        return {"name": name, "type": ds_type}

    def create_expectation_suite(
        self,
        suite_name: str,
        expectations: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """
        Create a new expectation suite.

        Args:
            suite_name: Name of the suite
            expectations: List of expectations to add

        Returns:
            Suite metadata
        """
        suite = self.context.add_expectation_suite(suite_name)

        if expectations:
            for exp in expectations:
                suite.add_expectation(
                    expectation_configuration=self._build_expectation(exp)
                )

        self.context.save_expectation_suite(suite)
        logger.info(f"Created expectation suite: {suite_name}")

        return {
            "suite_name": suite_name,
            "expectation_count": len(suite.expectations),
        }

    def add_expectation(
        self,
        suite_name: str,
        expectation_type: str,
        kwargs: dict[str, Any] | None = None,
        meta: dict[str, Any] | None = None,
    ) -> None:
        """
        Add an expectation to a suite.

        Args:
            suite_name: Name of the suite
            expectation_type: Type of expectation
            kwargs: Expectation arguments
            meta: Metadata for the expectation
        """
        suite = self.context.get_expectation_suite(suite_name)

        expectation = self._build_expectation({
            "expectation_type": expectation_type,
            "kwargs": kwargs or {},
            "meta": meta or {},
        })

        suite.add_expectation(expectation)
        self.context.save_expectation_suite(suite)

        logger.info(f"Added {expectation_type} to {suite_name}")

    def _build_expectation(self, config: dict[str, Any]) -> Any:
        """Build an expectation configuration."""
        from great_expectations.core import ExpectationConfiguration

        return ExpectationConfiguration(
            expectation_type=config["expectation_type"],
            kwargs=config.get("kwargs", {}),
            meta=config.get("meta", {}),
        )

    def validate(
        self,
        data: Any,
        suite_name: str,
        datasource_name: str | None = None,
        asset_name: str = "data_asset",
        run_name: str | None = None,
    ) -> dict[str, Any]:
        """
        Validate data against an expectation suite.

        Args:
            data: Data to validate (DataFrame, path, or table name)
            suite_name: Name of the expectation suite
            datasource_name: Name of the datasource
            asset_name: Name for the data asset
            run_name: Name for this validation run

        Returns:
            Validation results
        """
        import pandas as pd

        ds_name = datasource_name or self.config.datasource_name

        # Get or create datasource
        try:
            datasource = self.context.get_datasource(ds_name)
        except Exception:
            self.add_datasource(ds_name)
            datasource = self.context.get_datasource(ds_name)

        # Create batch request based on data type
        if isinstance(data, pd.DataFrame):
            data_asset = datasource.add_dataframe_asset(asset_name)
            batch_request = data_asset.build_batch_request(dataframe=data)

        elif isinstance(data, str) and data.endswith((".csv", ".parquet", ".json")):
            data_asset = datasource.add_csv_asset(
                asset_name,
                filepath_or_buffer=data
            )
            batch_request = data_asset.build_batch_request()

        else:
            # Assume it's a table name for SQL datasource
            data_asset = datasource.add_table_asset(asset_name, table_name=data)
            batch_request = data_asset.build_batch_request()

        # Get expectation suite
        suite = self.context.get_expectation_suite(suite_name)

        # Run validation
        validator = self.context.get_validator(
            batch_request=batch_request,
            expectation_suite=suite,
        )

        results = validator.validate(
            result_format={
                "result_format": self.config.result_format,
                "include_unexpected_rows": self.config.include_unexpected_rows,
                "partial_unexpected_count": self.config.partial_unexpected_count,
            }
        )

        # Parse results
        return self._parse_validation_results(results)

    def _parse_validation_results(self, results) -> dict[str, Any]:
        """Parse validation results into a clean format."""
        parsed = {
            "success": results.success,
            "statistics": {
                "evaluated_expectations": results.statistics.get("evaluated_expectations", 0),
                "successful_expectations": results.statistics.get("successful_expectations", 0),
                "unsuccessful_expectations": results.statistics.get("unsuccessful_expectations", 0),
                "success_percent": results.statistics.get("success_percent", 0),
            },
            "results": [],
        }

        for result in results.results:
            exp_result = {
                "expectation_type": result.expectation_config.expectation_type,
                "success": result.success,
                "kwargs": result.expectation_config.kwargs,
            }

            if result.result:
                exp_result["result"] = {
                    "element_count": result.result.get("element_count"),
                    "unexpected_count": result.result.get("unexpected_count"),
                    "unexpected_percent": result.result.get("unexpected_percent"),
                    "partial_unexpected_list": result.result.get("partial_unexpected_list", []),
                }

            parsed["results"].append(exp_result)

        return parsed

    def create_checkpoint(
        self,
        checkpoint_name: str,
        suite_name: str,
        datasource_name: str | None = None,
        asset_name: str = "data_asset",
        action_list: list[dict] | None = None,
    ) -> dict[str, Any]:
        """
        Create a validation checkpoint.

        Args:
            checkpoint_name: Name of the checkpoint
            suite_name: Name of the expectation suite
            datasource_name: Name of the datasource
            asset_name: Name of the data asset
            action_list: List of validation actions

        Returns:
            Checkpoint configuration
        """
        ds_name = datasource_name or self.config.datasource_name

        # Default actions
        if action_list is None:
            action_list = [
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"},
                },
                {
                    "name": "update_data_docs",
                    "action": {"class_name": "UpdateDataDocsAction"},
                },
            ]

        checkpoint_config = {
            "name": checkpoint_name,
            "config_version": 1.0,
            "class_name": "Checkpoint",
            "validations": [
                {
                    "batch_request": {
                        "datasource_name": ds_name,
                        "data_asset_name": asset_name,
                    },
                    "expectation_suite_name": suite_name,
                }
            ],
            "action_list": action_list,
        }

        checkpoint = self.context.add_or_update_checkpoint(**checkpoint_config)
        logger.info(f"Created checkpoint: {checkpoint_name}")

        return {"name": checkpoint_name, "config": checkpoint_config}

    def run_checkpoint(
        self,
        checkpoint_name: str,
        batch_parameters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Run a validation checkpoint.

        Args:
            checkpoint_name: Name of the checkpoint
            batch_parameters: Optional batch parameters

        Returns:
            Checkpoint results
        """
        results = self.context.run_checkpoint(
            checkpoint_name=checkpoint_name,
            batch_request=batch_parameters,
        )

        return {
            "success": results.success,
            "run_id": str(results.run_id),
            "validation_results": [
                self._parse_validation_results(r)
                for r in results.run_results.values()
            ],
        }

    def build_data_docs(self) -> str:
        """
        Build Data Docs site.

        Returns:
            Path to Data Docs index
        """
        self.context.build_data_docs()

        data_docs_path = (
            self.project_dir / "uncommitted" / "data_docs" / "local_site" / "index.html"
        )

        logger.info(f"Built Data Docs at {data_docs_path}")
        return str(data_docs_path)

    def get_common_expectations(self) -> dict[str, list[dict]]:
        """
        Get common expectation templates for quick setup.

        Returns:
            Dictionary of expectation categories with examples
        """
        return {
            "null_checks": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "COLUMN_NAME"},
                },
                {
                    "expectation_type": "expect_column_values_to_be_null",
                    "kwargs": {"column": "COLUMN_NAME"},
                },
            ],
            "type_checks": [
                {
                    "expectation_type": "expect_column_values_to_be_of_type",
                    "kwargs": {"column": "COLUMN_NAME", "type_": "int64"},
                },
                {
                    "expectation_type": "expect_column_values_to_be_in_type_list",
                    "kwargs": {"column": "COLUMN_NAME", "type_list": ["int64", "float64"]},
                },
            ],
            "uniqueness": [
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": "COLUMN_NAME"},
                },
                {
                    "expectation_type": "expect_compound_columns_to_be_unique",
                    "kwargs": {"column_list": ["COL1", "COL2"]},
                },
            ],
            "range_checks": [
                {
                    "expectation_type": "expect_column_values_to_be_between",
                    "kwargs": {"column": "COLUMN_NAME", "min_value": 0, "max_value": 100},
                },
                {
                    "expectation_type": "expect_column_min_to_be_between",
                    "kwargs": {"column": "COLUMN_NAME", "min_value": 0, "max_value": 10},
                },
                {
                    "expectation_type": "expect_column_max_to_be_between",
                    "kwargs": {"column": "COLUMN_NAME", "min_value": 90, "max_value": 100},
                },
            ],
            "set_membership": [
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {"column": "COLUMN_NAME", "value_set": ["A", "B", "C"]},
                },
                {
                    "expectation_type": "expect_column_distinct_values_to_be_in_set",
                    "kwargs": {"column": "COLUMN_NAME", "value_set": ["X", "Y", "Z"]},
                },
            ],
            "string_checks": [
                {
                    "expectation_type": "expect_column_value_lengths_to_be_between",
                    "kwargs": {"column": "COLUMN_NAME", "min_value": 1, "max_value": 100},
                },
                {
                    "expectation_type": "expect_column_values_to_match_regex",
                    "kwargs": {"column": "COLUMN_NAME", "regex": r"^[A-Z]{2}\d{4}$"},
                },
            ],
            "table_checks": [
                {
                    "expectation_type": "expect_table_row_count_to_be_between",
                    "kwargs": {"min_value": 1, "max_value": 1000000},
                },
                {
                    "expectation_type": "expect_table_columns_to_match_set",
                    "kwargs": {"column_set": ["col1", "col2", "col3"]},
                },
            ],
            "datetime_checks": [
                {
                    "expectation_type": "expect_column_values_to_be_dateutil_parseable",
                    "kwargs": {"column": "COLUMN_NAME"},
                },
            ],
        }

    def profile_data(
        self,
        data: Any,
        suite_name: str = "auto_profiled",
    ) -> dict[str, Any]:
        """
        Auto-profile data and create expectations.

        Args:
            data: DataFrame to profile
            suite_name: Name for the generated suite

        Returns:
            Profiling results with generated expectations
        """
        import pandas as pd

        if not isinstance(data, pd.DataFrame):
            raise ValueError("Data must be a pandas DataFrame for profiling")

        expectations = []

        # Profile each column
        for column in data.columns:
            dtype = str(data[column].dtype)

            # Null check
            null_count = data[column].isnull().sum()
            if null_count == 0:
                expectations.append({
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": column},
                })

            # Unique check for potential IDs
            unique_count = data[column].nunique()
            if unique_count == len(data):
                expectations.append({
                    "expectation_type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": column},
                })

            # Numeric range checks
            if pd.api.types.is_numeric_dtype(data[column]):
                min_val = data[column].min()
                max_val = data[column].max()
                expectations.append({
                    "expectation_type": "expect_column_values_to_be_between",
                    "kwargs": {
                        "column": column,
                        "min_value": float(min_val) * 0.9 if min_val > 0 else float(min_val) * 1.1,
                        "max_value": float(max_val) * 1.1 if max_val > 0 else float(max_val) * 0.9,
                    },
                })

            # String length checks
            if pd.api.types.is_string_dtype(data[column]):
                lengths = data[column].dropna().str.len()
                if len(lengths) > 0:
                    expectations.append({
                        "expectation_type": "expect_column_value_lengths_to_be_between",
                        "kwargs": {
                            "column": column,
                            "min_value": int(lengths.min()),
                            "max_value": int(lengths.max()),
                        },
                    })

            # Categorical check
            if unique_count <= 20 and unique_count < len(data) * 0.1:
                value_set = data[column].dropna().unique().tolist()
                expectations.append({
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {"column": column, "value_set": value_set},
                })

        # Table-level expectations
        expectations.append({
            "expectation_type": "expect_table_row_count_to_be_between",
            "kwargs": {
                "min_value": int(len(data) * 0.5),
                "max_value": int(len(data) * 2),
            },
        })

        expectations.append({
            "expectation_type": "expect_table_columns_to_match_set",
            "kwargs": {"column_set": list(data.columns)},
        })

        # Create suite with generated expectations
        self.create_expectation_suite(suite_name, expectations)

        return {
            "suite_name": suite_name,
            "expectations_generated": len(expectations),
            "columns_profiled": len(data.columns),
            "rows_analyzed": len(data),
        }
