"""Tests for Apache Airflow integration with agentic capabilities."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, AsyncMock
import pytest

from automic_etl.integrations.airflow import AirflowIntegration, AirflowConfig
from automic_etl.integrations.airflow_agentic import (
    AirflowAgent,
    AgenticAirflowOrchestrator,
    AgentAction,
    AgentDecision,
    AgentDecisionConfidence,
    AgentContext,
    TaskRecoveryPlan,
)
from automic_etl.integrations.airflow_dag_factory import (
    DAGFactory,
    DAGPattern,
    DAGDefinition,
    TaskDefinition,
    DataSource,
    DAGGenerationRequest,
)
from automic_etl.integrations.airflow_operators import (
    AutomicBaseOperator,
    BronzeIngestionOperator,
    SilverTransformOperator,
    GoldAggregationOperator,
    DataAvailabilitySensor,
)
from automic_etl.integrations.airflow_optimizer import (
    PipelineOptimizer,
    OptimizationType,
    OptimizationRecommendation,
    PerformanceMetrics,
    RiskLevel,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def airflow_config():
    """Create a test Airflow configuration."""
    return AirflowConfig(
        base_url="http://localhost:8080",
        username="admin",
        password="admin",
        api_version="v1",
    )


@pytest.fixture
def mock_llm_client():
    """Create a mock LLM client."""
    mock = MagicMock()
    mock.complete.return_value = MagicMock(
        content='{"action": "optimize", "confidence": "high", "reasoning": "test"}',
        tokens_used=100,
    )
    return mock


@pytest.fixture
def mock_airflow_integration():
    """Create a mock Airflow integration."""
    mock = MagicMock(spec=AirflowIntegration)
    mock.list_dags.return_value = [
        {"dag_id": "test_dag", "is_paused": False, "schedule_interval": "@daily"},
    ]
    mock.get_dag.return_value = {
        "dag_id": "test_dag",
        "is_paused": False,
        "schedule_interval": "@daily",
        "tags": ["automic-etl"],
    }
    mock.list_dag_runs.return_value = [
        {
            "dag_run_id": "run_1",
            "state": "success",
            "start_date": "2024-01-15T10:00:00Z",
            "end_date": "2024-01-15T10:30:00Z",
        },
    ]
    mock.get_dag_run.return_value = {
        "dag_run_id": "run_1",
        "state": "success",
    }
    mock.trigger_dag.return_value = "run_123"
    mock.health_check.return_value = {"metadatabase": {"status": "healthy"}}
    return mock


# =============================================================================
# AirflowConfig Tests
# =============================================================================

class TestAirflowConfig:
    """Tests for AirflowConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = AirflowConfig()
        assert config.base_url == "http://localhost:8080"
        assert config.username == "admin"
        assert config.api_version == "v1"

    def test_custom_config(self, airflow_config):
        """Test custom configuration."""
        assert airflow_config.base_url == "http://localhost:8080"
        assert airflow_config.retries == 3

    def test_schedule_interval_default(self):
        """Test default schedule interval."""
        config = AirflowConfig()
        assert config.schedule_interval == "@daily"


# =============================================================================
# AirflowIntegration Tests
# =============================================================================

class TestAirflowIntegration:
    """Tests for basic Airflow integration."""

    def test_api_url_construction(self, airflow_config):
        """Test API URL is constructed correctly."""
        integration = AirflowIntegration(airflow_config)
        assert integration.api_url == "http://localhost:8080/api/v1"

    def test_session_creation(self, airflow_config):
        """Test session is created with authentication."""
        integration = AirflowIntegration(airflow_config)
        session = integration.session
        assert session.auth == ("admin", "admin")
        assert session.headers["Content-Type"] == "application/json"

    @patch("requests.Session.request")
    def test_list_dags(self, mock_request, airflow_config):
        """Test listing DAGs."""
        mock_request.return_value = MagicMock(
            status_code=200,
            json=lambda: {"dags": [{"dag_id": "test"}]},
            text='{"dags": [{"dag_id": "test"}]}',
        )

        integration = AirflowIntegration(airflow_config)
        dags = integration.list_dags()

        assert len(dags) == 1
        assert dags[0]["dag_id"] == "test"


# =============================================================================
# AgentDecision Tests
# =============================================================================

class TestAgentDecision:
    """Tests for AgentDecision."""

    def test_decision_creation(self):
        """Test creating an agent decision."""
        decision = AgentDecision(
            action=AgentAction.OPTIMIZE,
            confidence=AgentDecisionConfidence.HIGH,
            reasoning="Pipeline shows optimization opportunities",
        )

        assert decision.action == AgentAction.OPTIMIZE
        assert decision.confidence == AgentDecisionConfidence.HIGH
        assert not decision.requires_approval

    def test_decision_to_dict(self):
        """Test converting decision to dictionary."""
        decision = AgentDecision(
            action=AgentAction.RECOVER,
            confidence=AgentDecisionConfidence.MEDIUM,
            reasoning="Recovery needed",
            requires_approval=True,
        )

        result = decision.to_dict()

        assert result["action"] == "recover"
        assert result["confidence"] == "medium"
        assert result["requires_approval"] is True


# =============================================================================
# AirflowAgent Tests
# =============================================================================

class TestAirflowAgent:
    """Tests for the AI-powered Airflow agent."""

    @pytest.mark.asyncio
    async def test_analyze_pipeline(self, mock_llm_client, mock_airflow_integration):
        """Test pipeline analysis."""
        mock_llm_client.complete.return_value = MagicMock(
            content=json.dumps({
                "health_score": 85,
                "performance_summary": "Good performance",
                "bottlenecks": [],
                "optimization_opportunities": [],
                "data_quality_concerns": [],
                "recommendations": [],
                "risk_assessment": {"overall_risk": "low", "factors": []},
            }),
            tokens_used=200,
        )

        agent = AirflowAgent(
            llm_client=mock_llm_client,
            airflow=mock_airflow_integration,
        )

        result = await agent.analyze_pipeline("test_dag")

        assert "health_score" in result
        mock_llm_client.complete.assert_called_once()

    @pytest.mark.asyncio
    async def test_decide_action(self, mock_llm_client, mock_airflow_integration):
        """Test agent decision making."""
        mock_llm_client.complete.return_value = MagicMock(
            content=json.dumps({
                "action": "optimize",
                "confidence": "high",
                "reasoning": "Optimization needed",
                "parameters": {},
                "requires_approval": False,
            }),
            tokens_used=100,
        )

        agent = AirflowAgent(
            llm_client=mock_llm_client,
            airflow=mock_airflow_integration,
        )

        context = AgentContext(
            pipeline_name="test_pipeline",
            current_state={"status": "running"},
        )

        decision = await agent.decide_action(context)

        assert decision.action == AgentAction.OPTIMIZE
        assert decision.confidence == AgentDecisionConfidence.HIGH

    def test_extract_json_from_markdown(self, mock_llm_client, mock_airflow_integration):
        """Test JSON extraction from markdown responses."""
        agent = AirflowAgent(
            llm_client=mock_llm_client,
            airflow=mock_airflow_integration,
        )

        content = '```json\n{"key": "value"}\n```'
        result = agent._extract_json(content)

        assert result == '{"key": "value"}'

    def test_extract_json_direct(self, mock_llm_client, mock_airflow_integration):
        """Test direct JSON extraction."""
        agent = AirflowAgent(
            llm_client=mock_llm_client,
            airflow=mock_airflow_integration,
        )

        content = '{"key": "value"}'
        result = agent._extract_json(content)

        assert result == '{"key": "value"}'


# =============================================================================
# TaskRecoveryPlan Tests
# =============================================================================

class TestTaskRecoveryPlan:
    """Tests for task recovery plans."""

    def test_recovery_plan_creation(self):
        """Test creating a recovery plan."""
        plan = TaskRecoveryPlan(
            task_id="extract_data",
            dag_id="etl_pipeline",
            failure_reason="Connection timeout",
            recovery_steps=[
                {"order": 1, "action": "Retry task"},
                {"order": 2, "action": "Check connectivity"},
            ],
            estimated_recovery_time=300,
            requires_human_intervention=False,
            confidence=0.85,
        )

        assert plan.task_id == "extract_data"
        assert len(plan.recovery_steps) == 2
        assert plan.confidence == 0.85
        assert not plan.requires_human_intervention


# =============================================================================
# DAGFactory Tests
# =============================================================================

class TestDAGFactory:
    """Tests for the DAG factory."""

    def test_sanitize_dag_id(self):
        """Test DAG ID sanitization."""
        factory = DAGFactory()

        result = factory._sanitize_dag_id("My Test Pipeline!")
        assert result == "automic_my_test_pipeline"

        result = factory._sanitize_dag_id("pipeline-with-dashes")
        assert result == "automic_pipeline_with_dashes"

    def test_infer_medallion_pattern(self):
        """Test pattern inference for medallion."""
        factory = DAGFactory()

        request = DAGGenerationRequest(
            name="test",
            target_layer="gold",
        )

        pattern = factory._infer_pattern(request)
        assert pattern == DAGPattern.MEDALLION

    def test_suggest_schedule_daily(self):
        """Test schedule suggestion for daily data."""
        factory = DAGFactory()

        request = DAGGenerationRequest(
            name="test",
            sources=[
                DataSource(
                    name="source1",
                    source_type="postgresql",
                    connection_config={},
                    update_frequency="daily",
                ),
            ],
        )

        schedule = factory._suggest_schedule(request)
        # Should return daily schedule (either @daily or cron format)
        assert schedule in ["@daily", "0 6 * * *"]

    def test_suggest_schedule_frequent(self):
        """Test schedule suggestion for frequent data."""
        factory = DAGFactory()

        request = DAGGenerationRequest(
            name="test",
            sources=[
                DataSource(
                    name="source1",
                    source_type="postgresql",
                    connection_config={},
                    update_frequency="every minute",
                ),
            ],
        )

        schedule = factory._suggest_schedule(request)
        assert "*/15" in schedule  # Should suggest frequent schedule


class TestDAGDefinition:
    """Tests for DAG definitions."""

    def test_dag_definition_creation(self):
        """Test creating a DAG definition."""
        tasks = [
            TaskDefinition(
                task_id="extract",
                task_type="BronzeIngestionOperator",
                description="Extract data",
            ),
            TaskDefinition(
                task_id="transform",
                task_type="SilverTransformOperator",
                description="Transform data",
                dependencies=["extract"],
            ),
        ]

        dag_def = DAGDefinition(
            dag_id="test_dag",
            description="Test DAG",
            pattern=DAGPattern.MEDALLION,
            schedule="@daily",
            tasks=tasks,
        )

        assert dag_def.dag_id == "test_dag"
        assert len(dag_def.tasks) == 2
        assert dag_def.pattern == DAGPattern.MEDALLION

    def test_dag_definition_to_dict(self):
        """Test converting DAG definition to dictionary."""
        dag_def = DAGDefinition(
            dag_id="test_dag",
            description="Test",
            pattern=DAGPattern.CDC,
            schedule="*/5 * * * *",
            tasks=[],
        )

        result = dag_def.to_dict()

        assert result["dag_id"] == "test_dag"
        assert result["pattern"] == "cdc"


# =============================================================================
# Operator Tests
# =============================================================================

class TestOperators:
    """Tests for custom Airflow operators."""

    def test_bronze_operator_creation(self):
        """Test BronzeIngestionOperator creation."""
        operator = BronzeIngestionOperator(
            task_id="bronze_customers",
            source_type="postgresql",
            source_config={"host": "localhost", "database": "test"},
            table_name="customers",
        )

        assert operator.task_id == "bronze_customers"
        assert operator.source_type == "postgresql"
        assert operator.table_name == "customers"

    def test_silver_operator_creation(self):
        """Test SilverTransformOperator creation."""
        operator = SilverTransformOperator(
            task_id="silver_customers",
            source_table="customers",
            target_table="customers_clean",
            dedupe_columns=["id"],
        )

        assert operator.task_id == "silver_customers"
        assert operator.source_table == "customers"
        assert operator.dedupe_columns == ["id"]

    def test_gold_operator_creation(self):
        """Test GoldAggregationOperator creation."""
        operator = GoldAggregationOperator(
            task_id="gold_metrics",
            source_tables=["orders", "customers"],
            target_table="customer_metrics",
            aggregations=[{"column": "amount", "function": "sum"}],
            group_by=["customer_id"],
        )

        assert operator.task_id == "gold_metrics"
        assert len(operator.source_tables) == 2
        assert operator.group_by == ["customer_id"]


class TestSensors:
    """Tests for custom Airflow sensors."""

    def test_data_availability_sensor_creation(self):
        """Test DataAvailabilitySensor creation."""
        sensor = DataAvailabilitySensor(
            task_id="wait_for_data",
            table_name="customers",
            layer="bronze",
            min_rows=100,
        )

        assert sensor.task_id == "wait_for_data"
        assert sensor.table_name == "customers"
        assert sensor.min_rows == 100


# =============================================================================
# Optimizer Tests
# =============================================================================

class TestPerformanceMetrics:
    """Tests for performance metrics."""

    def test_metrics_from_runs(self):
        """Test creating metrics from run data."""
        runs = [
            {
                "state": "success",
                "start_date": "2024-01-15T10:00:00",
                "end_date": "2024-01-15T10:30:00",
            },
            {
                "state": "success",
                "start_date": "2024-01-14T10:00:00",
                "end_date": "2024-01-14T10:25:00",
            },
            {
                "state": "failed",
                "start_date": "2024-01-13T10:00:00",
                "end_date": "2024-01-13T10:10:00",
            },
        ]

        metrics = PerformanceMetrics.from_runs("test_dag", runs)

        assert metrics.dag_id == "test_dag"
        assert metrics.total_runs == 3
        assert metrics.success_rate == pytest.approx(0.666, rel=0.01)

    def test_metrics_empty_runs(self):
        """Test metrics with no runs."""
        metrics = PerformanceMetrics.from_runs("test_dag", [])

        assert metrics.total_runs == 0
        assert metrics.success_rate == 0.0
        assert metrics.avg_duration_seconds == 0.0

    def test_metrics_to_dict(self):
        """Test converting metrics to dictionary."""
        metrics = PerformanceMetrics(
            dag_id="test",
            total_runs=10,
            success_rate=0.9,
            avg_duration_seconds=300.0,
            p95_duration_seconds=450.0,
            max_duration_seconds=600.0,
            min_duration_seconds=200.0,
            avg_task_count=5,
            failed_tasks_rate=0.05,
            retry_rate=0.1,
        )

        result = metrics.to_dict()

        assert result["dag_id"] == "test"
        assert result["total_runs"] == 10
        assert "90.0%" in result["success_rate"]


class TestOptimizationRecommendation:
    """Tests for optimization recommendations."""

    def test_recommendation_creation(self):
        """Test creating an optimization recommendation."""
        rec = OptimizationRecommendation(
            optimization_type=OptimizationType.SCHEDULE,
            title="Optimize schedule",
            description="Change from daily to hourly",
            current_value="@daily",
            recommended_value="@hourly",
            expected_improvement="50% faster data availability",
            reasoning="Data updates frequently",
            risk_level=RiskLevel.LOW,
            implementation_steps=["Update schedule in DAG"],
            auto_applicable=True,
        )

        assert rec.optimization_type == OptimizationType.SCHEDULE
        assert rec.risk_level == RiskLevel.LOW
        assert rec.auto_applicable is True

    def test_recommendation_to_dict(self):
        """Test converting recommendation to dictionary."""
        rec = OptimizationRecommendation(
            optimization_type=OptimizationType.PARALLELISM,
            title="Increase parallelism",
            description="Add more workers",
            current_value=2,
            recommended_value=4,
            expected_improvement="30% faster",
            reasoning="CPU underutilized",
            risk_level=RiskLevel.MEDIUM,
            implementation_steps=["Update config"],
            auto_applicable=False,
        )

        result = rec.to_dict()

        assert result["type"] == "parallelism"
        assert result["risk_level"] == "medium"


class TestPipelineOptimizer:
    """Tests for the pipeline optimizer."""

    def test_calculate_health_score_perfect(self):
        """Test health score calculation for perfect pipeline."""
        optimizer = PipelineOptimizer()

        metrics = PerformanceMetrics(
            dag_id="test",
            total_runs=100,
            success_rate=1.0,
            avg_duration_seconds=300,
            p95_duration_seconds=350,
            max_duration_seconds=400,
            min_duration_seconds=250,
            avg_task_count=5,
            failed_tasks_rate=0.0,
            retry_rate=0.0,
        )

        score = optimizer._calculate_health_score(metrics)

        assert score >= 90  # Should be high for perfect metrics

    def test_calculate_health_score_problematic(self):
        """Test health score calculation for problematic pipeline."""
        optimizer = PipelineOptimizer()

        metrics = PerformanceMetrics(
            dag_id="test",
            total_runs=100,
            success_rate=0.7,  # 70% success
            avg_duration_seconds=300,
            p95_duration_seconds=350,
            max_duration_seconds=1000,  # High variability
            min_duration_seconds=100,
            avg_task_count=5,
            failed_tasks_rate=0.2,  # High failure rate
            retry_rate=0.3,
        )

        score = optimizer._calculate_health_score(metrics)

        assert score < 70  # Should be lower for problematic metrics

    def test_prioritize_recommendations(self):
        """Test recommendation prioritization."""
        optimizer = PipelineOptimizer()

        recommendations = [
            OptimizationRecommendation(
                optimization_type=OptimizationType.SCHEDULE,
                title="High risk change",
                description="",
                current_value="",
                recommended_value="",
                expected_improvement="",
                reasoning="",
                risk_level=RiskLevel.HIGH,
                implementation_steps=[],
                auto_applicable=False,
            ),
            OptimizationRecommendation(
                optimization_type=OptimizationType.RETRY_POLICY,
                title="Low risk auto",
                description="",
                current_value="",
                recommended_value="",
                expected_improvement="",
                reasoning="",
                risk_level=RiskLevel.LOW,
                implementation_steps=[],
                auto_applicable=True,
            ),
            OptimizationRecommendation(
                optimization_type=OptimizationType.TIMEOUT,
                title="Low risk manual",
                description="",
                current_value="",
                recommended_value="",
                expected_improvement="",
                reasoning="",
                risk_level=RiskLevel.LOW,
                implementation_steps=[],
                auto_applicable=False,
            ),
        ]

        priorities = optimizer._prioritize_recommendations(recommendations)

        # Low risk, auto-applicable should be first
        assert priorities[0] == "Low risk auto"


# =============================================================================
# AgenticAirflowOrchestrator Tests
# =============================================================================

class TestAgenticAirflowOrchestrator:
    """Tests for the agentic orchestrator."""

    def test_orchestrator_status(self, mock_llm_client):
        """Test orchestrator status reporting."""
        with patch("automic_etl.integrations.airflow_agentic.LLMClient", return_value=mock_llm_client):
            with patch("automic_etl.integrations.airflow_agentic.AirflowIntegration") as mock_airflow:
                mock_airflow.return_value.health_check.return_value = {
                    "metadatabase": {"status": "healthy"}
                }

                orchestrator = AgenticAirflowOrchestrator(
                    enable_auto_healing=True,
                    enable_auto_optimization=False,
                )

                status = orchestrator.get_status()

                assert status["auto_healing_enabled"] is True
                assert status["auto_optimization_enabled"] is False
                assert "monitored_dags" in status


# =============================================================================
# Integration Tests
# =============================================================================

class TestIntegration:
    """Integration tests for the Airflow module."""

    def test_full_dag_generation_flow(self, mock_llm_client):
        """Test complete DAG generation flow."""
        mock_llm_client.complete.return_value = MagicMock(
            content=json.dumps({
                "dag_id": "generated_etl",
                "description": "ETL pipeline",
                "schedule_interval": "@daily",
                "tasks": [
                    {
                        "task_id": "extract",
                        "operator": "BronzeIngestionOperator",
                        "description": "Extract data",
                        "config": {},
                        "dependencies": [],
                    },
                ],
                "dag_config": {"retries": 3},
                "reasoning": "Simple ETL",
            }),
            tokens_used=500,
        )

        with patch.object(DAGFactory, "__init__", lambda self, **kwargs: None):
            factory = DAGFactory()
            factory.settings = MagicMock()
            factory.dags_folder = None
            factory.llm = mock_llm_client
            factory.logger = MagicMock()

            # Test medallion DAG creation
            tasks = factory._generate_medallion_tasks(
                DAGGenerationRequest(
                    name="test",
                    sources=[
                        DataSource(
                            name="db",
                            source_type="postgresql",
                            connection_config={},
                            tables=["users"],
                        ),
                    ],
                    target_layer="gold",
                )
            )

            assert len(tasks) >= 2  # At least bronze and silver tasks

    def test_operator_lineage_recording(self):
        """Test that operators record lineage correctly."""
        operator = BronzeIngestionOperator(
            task_id="test_op",
            source_type="postgresql",
            source_config={},
            table_name="test_table",
            track_lineage=True,
        )

        # Lineage recording should not fail even without tracker
        # Patch at the module where it's imported
        with patch("automic_etl.lineage.tracker.LineageTracker") as mock_tracker:
            mock_tracker.return_value.record_transformation = MagicMock()
            operator.record_lineage(
                operation="bronze_ingestion",
                source="postgresql:users",
                target="bronze.users",
            )

    def test_operator_lineage_disabled(self):
        """Test that lineage recording can be disabled."""
        operator = BronzeIngestionOperator(
            task_id="test_op",
            source_type="postgresql",
            source_config={},
            table_name="test_table",
            track_lineage=False,
        )

        # Should not raise even with lineage disabled
        operator.record_lineage(
            operation="bronze_ingestion",
            source="postgresql:users",
            target="bronze.users",
        )
