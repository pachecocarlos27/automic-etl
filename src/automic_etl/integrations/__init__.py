"""Open source tool integrations for Automic ETL."""

from automic_etl.integrations.spark import SparkIntegration, SparkConfig
from automic_etl.integrations.dbt import DbtIntegration, DbtConfig
from automic_etl.integrations.great_expectations import GreatExpectationsIntegration, GEConfig
from automic_etl.integrations.airflow import AirflowIntegration, AirflowConfig
from automic_etl.integrations.mlflow import MLflowIntegration, MLflowConfig
from automic_etl.integrations.openmetadata import OpenMetadataIntegration, OpenMetadataConfig

__all__ = [
    # Spark
    "SparkIntegration",
    "SparkConfig",
    # dbt
    "DbtIntegration",
    "DbtConfig",
    # Great Expectations
    "GreatExpectationsIntegration",
    "GEConfig",
    # Airflow
    "AirflowIntegration",
    "AirflowConfig",
    # MLflow
    "MLflowIntegration",
    "MLflowConfig",
    # OpenMetadata
    "OpenMetadataIntegration",
    "OpenMetadataConfig",
]
