"""Open source tool integrations for Automic ETL."""

from automic_etl.integrations.spark import SparkIntegration, SparkConfig
from automic_etl.integrations.dbt import DbtIntegration, DbtConfig
from automic_etl.integrations.great_expectations import GreatExpectationsIntegration, GEConfig
from automic_etl.integrations.airflow import AirflowIntegration, AirflowConfig
from automic_etl.integrations.mlflow import MLflowIntegration, MLflowConfig
from automic_etl.integrations.openmetadata import OpenMetadataIntegration, OpenMetadataConfig

# Agentic Airflow integration
from automic_etl.integrations.airflow_agentic import (
    AirflowAgent,
    AgenticAirflowOrchestrator,
    AgentAction,
    AgentDecision,
    AgentContext,
    TaskRecoveryPlan,
    DAGOptimizationSuggestion,
)

# DAG Factory
from automic_etl.integrations.airflow_dag_factory import (
    DAGFactory,
    DAGPattern,
    DAGDefinition,
    TaskDefinition,
    DataSource,
    DAGGenerationRequest,
    create_dag_from_description,
    create_medallion_dag,
)

# Airflow Operators
from automic_etl.integrations.airflow_operators import (
    AutomicBaseOperator,
    AutomicBaseSensor,
    BronzeIngestionOperator,
    BronzeBatchOperator,
    SilverTransformOperator,
    SilverSCDOperator,
    GoldAggregationOperator,
    GoldMetricOperator,
    LLMAugmentedOperator,
    NaturalLanguageQueryOperator,
    AgenticDecisionOperator,
    SelfHealingOperator,
    DataAvailabilitySensor,
    DataQualitySensor,
    PipelineCompletionSensor,
    AnomalyDetectionSensor,
)

# Airflow Optimizer
from automic_etl.integrations.airflow_optimizer import (
    PipelineOptimizer,
    ContinuousOptimizer,
    OptimizationType,
    OptimizationRecommendation,
    OptimizationPlan,
    PerformanceMetrics,
)

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
    # Airflow (basic)
    "AirflowIntegration",
    "AirflowConfig",
    # Airflow (agentic)
    "AirflowAgent",
    "AgenticAirflowOrchestrator",
    "AgentAction",
    "AgentDecision",
    "AgentContext",
    "TaskRecoveryPlan",
    "DAGOptimizationSuggestion",
    # Airflow (DAG factory)
    "DAGFactory",
    "DAGPattern",
    "DAGDefinition",
    "TaskDefinition",
    "DataSource",
    "DAGGenerationRequest",
    "create_dag_from_description",
    "create_medallion_dag",
    # Airflow (operators)
    "AutomicBaseOperator",
    "AutomicBaseSensor",
    "BronzeIngestionOperator",
    "BronzeBatchOperator",
    "SilverTransformOperator",
    "SilverSCDOperator",
    "GoldAggregationOperator",
    "GoldMetricOperator",
    "LLMAugmentedOperator",
    "NaturalLanguageQueryOperator",
    "AgenticDecisionOperator",
    "SelfHealingOperator",
    "DataAvailabilitySensor",
    "DataQualitySensor",
    "PipelineCompletionSensor",
    "AnomalyDetectionSensor",
    # Airflow (optimizer)
    "PipelineOptimizer",
    "ContinuousOptimizer",
    "OptimizationType",
    "OptimizationRecommendation",
    "OptimizationPlan",
    "PerformanceMetrics",
    # MLflow
    "MLflowIntegration",
    "MLflowConfig",
    # OpenMetadata
    "OpenMetadataIntegration",
    "OpenMetadataConfig",
]
