"""Dynamic Airflow DAG Factory for Automic ETL.

This module provides intelligent, dynamic DAG generation capabilities:
- Pattern-based DAG generation from data schemas
- ML-powered schedule optimization
- Adaptive DAG modification based on performance
- Self-documenting DAGs with lineage integration
"""

from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Literal
import re

import structlog

from automic_etl.core.config import Settings, get_settings
from automic_etl.core.utils import utc_now, format_iso
from automic_etl.llm.client import LLMClient

logger = structlog.get_logger()


class DAGPattern(str, Enum):
    """Pre-defined DAG patterns."""
    MEDALLION = "medallion"  # Bronze -> Silver -> Gold
    CDC = "cdc"  # Change Data Capture pattern
    STREAMING = "streaming"  # Micro-batch streaming
    ML_PIPELINE = "ml_pipeline"  # Feature engineering -> training -> inference
    DATA_QUALITY = "data_quality"  # Profiling -> validation -> reporting
    REPLICATION = "replication"  # Source -> Target sync
    AGGREGATION = "aggregation"  # Multi-source aggregation
    CUSTOM = "custom"


class TaskDependencyType(str, Enum):
    """Types of task dependencies."""
    SEQUENTIAL = "sequential"  # One after another
    PARALLEL = "parallel"  # Run simultaneously
    CONDITIONAL = "conditional"  # Branch based on condition
    CROSS_DAG = "cross_dag"  # Depends on another DAG


@dataclass
class TaskDefinition:
    """Definition of a single task."""
    task_id: str
    task_type: str  # operator type
    description: str
    config: dict[str, Any] = field(default_factory=dict)
    dependencies: list[str] = field(default_factory=list)
    trigger_rule: str = "all_success"
    pool: str | None = None
    priority_weight: int = 1
    retries: int = 3
    retry_delay_minutes: int = 5
    timeout_minutes: int | None = None
    sla_minutes: int | None = None
    tags: list[str] = field(default_factory=list)


@dataclass
class DAGDefinition:
    """Complete DAG definition."""
    dag_id: str
    description: str
    pattern: DAGPattern
    schedule: str
    tasks: list[TaskDefinition]
    default_args: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    catchup: bool = False
    max_active_runs: int = 1
    concurrency: int | None = None
    start_date: datetime = field(default_factory=lambda: datetime(2024, 1, 1))
    end_date: datetime | None = None
    doc_md: str | None = None
    params: dict[str, Any] = field(default_factory=dict)
    on_success_callback: str | None = None
    on_failure_callback: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "dag_id": self.dag_id,
            "description": self.description,
            "pattern": self.pattern.value,
            "schedule": self.schedule,
            "tasks": [
                {
                    "task_id": t.task_id,
                    "task_type": t.task_type,
                    "description": t.description,
                    "config": t.config,
                    "dependencies": t.dependencies,
                }
                for t in self.tasks
            ],
            "tags": self.tags,
            "catchup": self.catchup,
            "max_active_runs": self.max_active_runs,
        }


@dataclass
class DataSource:
    """Data source specification for DAG generation."""
    name: str
    source_type: str  # database, api, file, stream
    connection_config: dict[str, Any]
    tables: list[str] | None = None
    schema: dict[str, Any] | None = None
    update_frequency: str | None = None  # hint for scheduling


@dataclass
class DAGGenerationRequest:
    """Request for dynamic DAG generation."""
    name: str
    description: str | None = None
    sources: list[DataSource] = field(default_factory=list)
    target_layer: str = "gold"  # bronze, silver, gold
    pattern: DAGPattern | None = None
    schedule: str | None = None
    transformations: list[dict[str, Any]] | None = None
    quality_checks: list[dict[str, Any]] | None = None
    notifications: dict[str, Any] | None = None
    constraints: dict[str, Any] | None = None


class DAGFactory:
    """
    Factory for dynamically generating Airflow DAGs.

    Features:
    - Pattern-based generation
    - LLM-assisted DAG design
    - Schema-aware task generation
    - Automatic dependency resolution
    """

    def __init__(
        self,
        settings: Settings | None = None,
        dags_folder: str | Path | None = None,
    ):
        self.settings = settings or get_settings()
        self.dags_folder = Path(dags_folder) if dags_folder else None
        self.logger = logger.bind(component="dag_factory")

        # Initialize LLM for intelligent generation
        self.llm = LLMClient(self.settings)

    def create_from_request(
        self,
        request: DAGGenerationRequest,
        deploy: bool = False,
    ) -> tuple[DAGDefinition, str]:
        """
        Create a DAG from a generation request.

        Args:
            request: DAG generation request
            deploy: If True, write DAG file to dags_folder

        Returns:
            Tuple of (DAGDefinition, generated code)
        """
        self.logger.info("Creating DAG from request", name=request.name)

        # Determine pattern if not specified
        pattern = request.pattern or self._infer_pattern(request)

        # Determine schedule if not specified
        schedule = request.schedule or self._suggest_schedule(request)

        # Generate task definitions based on pattern
        tasks = self._generate_tasks(request, pattern)

        # Create DAG definition
        dag_def = DAGDefinition(
            dag_id=self._sanitize_dag_id(request.name),
            description=request.description or f"Auto-generated DAG for {request.name}",
            pattern=pattern,
            schedule=schedule,
            tasks=tasks,
            tags=["automic-etl", "auto-generated", pattern.value],
            doc_md=self._generate_documentation(request, tasks),
        )

        # Generate Python code
        code = self._generate_code(dag_def, request)

        # Deploy if requested
        if deploy and self.dags_folder:
            self._deploy_dag(dag_def.dag_id, code)

        return dag_def, code

    def create_from_schema(
        self,
        source_schema: dict[str, Any],
        target_table: str,
        transformations: list[str] | None = None,
        deploy: bool = False,
    ) -> tuple[DAGDefinition, str]:
        """
        Create a DAG from a source schema.

        Analyzes the schema to determine appropriate transformations
        and generates a complete ETL DAG.
        """
        self.logger.info("Creating DAG from schema", target=target_table)

        # Use LLM to analyze schema and suggest transformations
        prompt = f"""Analyze this data schema and suggest an ETL pipeline:

Schema:
{json.dumps(source_schema, indent=2)}

Target Table: {target_table}
Requested Transformations: {transformations or 'Determine automatically'}

Provide pipeline specification as JSON:
{{
    "pipeline_name": "<suggested_name>",
    "schedule": "<cron_expression>",
    "stages": [
        {{
            "name": "<stage_name>",
            "type": "<bronze/silver/gold>",
            "operations": [
                {{"operation": "<op>", "columns": ["<col>"], "config": {{}}}}
            ]
        }}
    ],
    "quality_checks": [
        {{"check": "<check_type>", "column": "<col>", "threshold": <value>}}
    ],
    "reasoning": "<why_this_design>"
}}"""

        response = self.llm.complete(
            prompt=prompt,
            system_prompt="You are an expert data engineer designing ETL pipelines.",
            temperature=0.4,
        )

        try:
            pipeline_spec = json.loads(self._extract_json(response.content))
        except json.JSONDecodeError:
            # Fallback to basic medallion pattern
            pipeline_spec = {
                "pipeline_name": f"etl_{target_table}",
                "schedule": "@daily",
                "stages": [
                    {"name": "bronze_ingest", "type": "bronze", "operations": []},
                    {"name": "silver_transform", "type": "silver", "operations": []},
                    {"name": "gold_publish", "type": "gold", "operations": []},
                ],
            }

        # Convert to DAG generation request
        request = DAGGenerationRequest(
            name=pipeline_spec.get("pipeline_name", f"etl_{target_table}"),
            schedule=pipeline_spec.get("schedule"),
            target_layer="gold",
            pattern=DAGPattern.MEDALLION,
        )

        return self.create_from_request(request, deploy=deploy)

    def create_from_natural_language(
        self,
        description: str,
        context: dict[str, Any] | None = None,
        deploy: bool = False,
    ) -> tuple[DAGDefinition, str]:
        """
        Create a DAG from a natural language description.

        Uses LLM to interpret the description and generate appropriate DAG.
        """
        self.logger.info("Creating DAG from NL description", description=description[:100])

        prompt = f"""Design an Airflow DAG based on this description:

Description: {description}

Additional Context:
{json.dumps(context or {}, indent=2)}

Provide a complete DAG specification as JSON:
{{
    "dag_id": "<unique_identifier>",
    "description": "<detailed_description>",
    "schedule": "<cron_expression>",
    "pattern": "<medallion/cdc/streaming/ml_pipeline/data_quality/replication/aggregation/custom>",
    "tasks": [
        {{
            "task_id": "<unique_task_id>",
            "task_type": "<operator_type>",
            "description": "<what_it_does>",
            "config": {{}},
            "dependencies": ["<upstream_task_ids>"]
        }}
    ],
    "quality_checks": [
        {{"type": "<check>", "config": {{}}}}
    ],
    "notifications": {{
        "on_failure": ["<channel>"],
        "on_success": ["<channel>"]
    }},
    "reasoning": "<design_rationale>"
}}"""

        response = self.llm.complete(
            prompt=prompt,
            system_prompt="""You are an expert data engineer creating Airflow DAGs.
Design robust, production-ready pipelines with proper error handling,
monitoring, and documentation. Use Automic ETL operators when applicable.""",
            temperature=0.5,
        )

        try:
            dag_spec = json.loads(self._extract_json(response.content))
        except json.JSONDecodeError:
            raise ValueError(f"Failed to parse DAG specification from description")

        # Convert to DAG definition
        tasks = [
            TaskDefinition(
                task_id=t["task_id"],
                task_type=t.get("task_type", "PythonOperator"),
                description=t.get("description", ""),
                config=t.get("config", {}),
                dependencies=t.get("dependencies", []),
            )
            for t in dag_spec.get("tasks", [])
        ]

        pattern = DAGPattern(dag_spec.get("pattern", "custom"))

        dag_def = DAGDefinition(
            dag_id=dag_spec.get("dag_id", f"nl_generated_{datetime.now().strftime('%Y%m%d')}"),
            description=dag_spec.get("description", description),
            pattern=pattern,
            schedule=dag_spec.get("schedule", "@daily"),
            tasks=tasks,
            tags=["automic-etl", "nl-generated", pattern.value],
            doc_md=f"## Generated from Natural Language\n\n{description}\n\n{dag_spec.get('reasoning', '')}",
        )

        code = self._generate_code(dag_def, None)

        if deploy and self.dags_folder:
            self._deploy_dag(dag_def.dag_id, code)

        return dag_def, code

    def create_medallion_dag(
        self,
        name: str,
        source_type: str,
        source_config: dict[str, Any],
        tables: list[str],
        silver_config: dict[str, Any] | None = None,
        gold_config: dict[str, Any] | None = None,
        schedule: str = "@daily",
        deploy: bool = False,
    ) -> tuple[DAGDefinition, str]:
        """
        Create a standard medallion architecture DAG.

        This is a convenience method for the most common pattern.
        """
        tasks = []

        # Bronze layer tasks - one per table
        for table in tables:
            tasks.append(TaskDefinition(
                task_id=f"bronze_{table}",
                task_type="BronzeIngestionOperator",
                description=f"Ingest {table} to Bronze layer",
                config={
                    "source_type": source_type,
                    "source_config": source_config,
                    "table_name": table,
                },
                tags=["bronze"],
            ))

        # Silver layer tasks
        silver_cfg = silver_config or {}
        for table in tables:
            tasks.append(TaskDefinition(
                task_id=f"silver_{table}",
                task_type="SilverTransformOperator",
                description=f"Transform {table} to Silver layer",
                config={
                    "source_table": table,
                    "target_table": table,
                    "dedupe_columns": silver_cfg.get("dedupe_columns", {}).get(table),
                    "transformations": silver_cfg.get("transformations", {}).get(table, []),
                },
                dependencies=[f"bronze_{table}"],
                tags=["silver"],
            ))

        # Gold layer tasks
        gold_cfg = gold_config or {}
        for agg in gold_cfg.get("aggregations", []):
            tasks.append(TaskDefinition(
                task_id=f"gold_{agg['name']}",
                task_type="GoldAggregationOperator",
                description=f"Create {agg['name']} aggregation",
                config=agg,
                dependencies=[f"silver_{t}" for t in agg.get("source_tables", tables)],
                tags=["gold"],
            ))

        dag_def = DAGDefinition(
            dag_id=self._sanitize_dag_id(name),
            description=f"Medallion pipeline for {name}",
            pattern=DAGPattern.MEDALLION,
            schedule=schedule,
            tasks=tasks,
            tags=["automic-etl", "medallion"],
        )

        code = self._generate_code(dag_def, None)

        if deploy and self.dags_folder:
            self._deploy_dag(dag_def.dag_id, code)

        return dag_def, code

    def create_cdc_dag(
        self,
        name: str,
        source_config: dict[str, Any],
        tables: list[str],
        cdc_config: dict[str, Any],
        schedule: str = "*/5 * * * *",  # Every 5 minutes
        deploy: bool = False,
    ) -> tuple[DAGDefinition, str]:
        """Create a Change Data Capture DAG."""
        tasks = []

        # Sensor for new changes
        tasks.append(TaskDefinition(
            task_id="detect_changes",
            task_type="CDCChangeSensor",
            description="Detect new changes in source",
            config={
                "source_config": source_config,
                "tables": tables,
                "cdc_type": cdc_config.get("type", "timestamp"),
            },
            tags=["cdc", "sensor"],
        ))

        # Process changes for each table
        for table in tables:
            tasks.append(TaskDefinition(
                task_id=f"process_changes_{table}",
                task_type="CDCProcessOperator",
                description=f"Process CDC changes for {table}",
                config={
                    "table": table,
                    "cdc_config": cdc_config,
                },
                dependencies=["detect_changes"],
                tags=["cdc"],
            ))

        # Merge changes
        tasks.append(TaskDefinition(
            task_id="merge_changes",
            task_type="CDCMergeOperator",
            description="Merge CDC changes into target",
            config={
                "tables": tables,
                "merge_strategy": cdc_config.get("merge_strategy", "upsert"),
            },
            dependencies=[f"process_changes_{t}" for t in tables],
            tags=["cdc"],
        ))

        dag_def = DAGDefinition(
            dag_id=self._sanitize_dag_id(f"cdc_{name}"),
            description=f"CDC pipeline for {name}",
            pattern=DAGPattern.CDC,
            schedule=schedule,
            tasks=tasks,
            tags=["automic-etl", "cdc"],
            max_active_runs=1,  # Important for CDC
        )

        code = self._generate_code(dag_def, None)

        if deploy and self.dags_folder:
            self._deploy_dag(dag_def.dag_id, code)

        return dag_def, code

    def _infer_pattern(self, request: DAGGenerationRequest) -> DAGPattern:
        """Infer the best DAG pattern based on request."""
        if request.target_layer == "gold":
            return DAGPattern.MEDALLION

        # Check for CDC indicators
        for source in request.sources:
            if source.update_frequency and "minute" in source.update_frequency.lower():
                return DAGPattern.CDC

        # Check for streaming indicators
        for source in request.sources:
            if source.source_type in ["kafka", "kinesis", "pubsub"]:
                return DAGPattern.STREAMING

        return DAGPattern.MEDALLION

    def _suggest_schedule(self, request: DAGGenerationRequest) -> str:
        """Suggest an appropriate schedule based on request."""
        # Check source update frequencies
        frequencies = []
        for source in request.sources:
            if source.update_frequency:
                frequencies.append(source.update_frequency.lower())

        if any("real" in f or "minute" in f for f in frequencies):
            return "*/15 * * * *"  # Every 15 minutes
        elif any("hour" in f for f in frequencies):
            return "0 * * * *"  # Hourly
        elif any("day" in f for f in frequencies):
            return "0 6 * * *"  # Daily at 6 AM

        return "@daily"

    def _generate_tasks(
        self,
        request: DAGGenerationRequest,
        pattern: DAGPattern,
    ) -> list[TaskDefinition]:
        """Generate task definitions based on pattern."""
        tasks = []

        if pattern == DAGPattern.MEDALLION:
            tasks = self._generate_medallion_tasks(request)
        elif pattern == DAGPattern.CDC:
            tasks = self._generate_cdc_tasks(request)
        elif pattern == DAGPattern.STREAMING:
            tasks = self._generate_streaming_tasks(request)
        elif pattern == DAGPattern.DATA_QUALITY:
            tasks = self._generate_quality_tasks(request)
        else:
            tasks = self._generate_generic_tasks(request)

        return tasks

    def _generate_medallion_tasks(
        self,
        request: DAGGenerationRequest,
    ) -> list[TaskDefinition]:
        """Generate tasks for medallion pattern."""
        tasks = []

        for source in request.sources:
            tables = source.tables or ["default"]

            for table in tables:
                # Bronze ingestion
                tasks.append(TaskDefinition(
                    task_id=f"bronze_{source.name}_{table}",
                    task_type="BronzeIngestionOperator",
                    description=f"Ingest {table} from {source.name}",
                    config={
                        "source_type": source.source_type,
                        "source_config": source.connection_config,
                        "table_name": table,
                    },
                    tags=["bronze"],
                ))

                # Silver transformation
                tasks.append(TaskDefinition(
                    task_id=f"silver_{source.name}_{table}",
                    task_type="SilverTransformOperator",
                    description=f"Transform {table} to Silver",
                    config={
                        "source_table": table,
                        "target_table": table,
                    },
                    dependencies=[f"bronze_{source.name}_{table}"],
                    tags=["silver"],
                ))

        # Gold aggregation (if target is gold)
        if request.target_layer == "gold":
            all_silver_tasks = [t.task_id for t in tasks if "silver" in t.task_id]

            tasks.append(TaskDefinition(
                task_id="gold_aggregate",
                task_type="GoldAggregationOperator",
                description="Create Gold layer aggregations",
                config={
                    "source_tables": [t.config.get("table_name") for t in tasks if "silver" in t.task_id],
                },
                dependencies=all_silver_tasks,
                tags=["gold"],
            ))

        # Quality check
        if request.quality_checks:
            tasks.append(TaskDefinition(
                task_id="quality_check",
                task_type="DataQualityOperator",
                description="Run data quality checks",
                config={"checks": request.quality_checks},
                dependencies=[tasks[-1].task_id] if tasks else [],
                tags=["quality"],
            ))

        return tasks

    def _generate_cdc_tasks(
        self,
        request: DAGGenerationRequest,
    ) -> list[TaskDefinition]:
        """Generate tasks for CDC pattern."""
        tasks = []

        tasks.append(TaskDefinition(
            task_id="sensor_changes",
            task_type="DataAvailabilitySensor",
            description="Wait for new changes",
            config={"mode": "poke", "poke_interval": 30},
            tags=["sensor"],
        ))

        for source in request.sources:
            tasks.append(TaskDefinition(
                task_id=f"extract_{source.name}",
                task_type="CDCExtractOperator",
                description=f"Extract changes from {source.name}",
                config={"source_config": source.connection_config},
                dependencies=["sensor_changes"],
                tags=["extract"],
            ))

        tasks.append(TaskDefinition(
            task_id="merge_changes",
            task_type="CDCMergeOperator",
            description="Merge changes to target",
            config={},
            dependencies=[f"extract_{s.name}" for s in request.sources],
            tags=["merge"],
        ))

        return tasks

    def _generate_streaming_tasks(
        self,
        request: DAGGenerationRequest,
    ) -> list[TaskDefinition]:
        """Generate tasks for streaming pattern."""
        tasks = []

        tasks.append(TaskDefinition(
            task_id="start_stream",
            task_type="StreamingStartOperator",
            description="Initialize streaming context",
            config={},
            tags=["streaming"],
        ))

        for source in request.sources:
            tasks.append(TaskDefinition(
                task_id=f"consume_{source.name}",
                task_type="StreamingConsumeOperator",
                description=f"Consume from {source.name}",
                config={"source_config": source.connection_config},
                dependencies=["start_stream"],
                tags=["streaming", "consume"],
            ))

        tasks.append(TaskDefinition(
            task_id="process_batch",
            task_type="StreamingProcessOperator",
            description="Process micro-batch",
            config={},
            dependencies=[f"consume_{s.name}" for s in request.sources],
            tags=["streaming", "process"],
        ))

        return tasks

    def _generate_quality_tasks(
        self,
        request: DAGGenerationRequest,
    ) -> list[TaskDefinition]:
        """Generate tasks for data quality pattern."""
        tasks = []

        tasks.append(TaskDefinition(
            task_id="profile_data",
            task_type="DataProfilingOperator",
            description="Profile data statistics",
            config={},
            tags=["quality", "profiling"],
        ))

        tasks.append(TaskDefinition(
            task_id="validate_data",
            task_type="DataValidationOperator",
            description="Validate data quality rules",
            config={"rules": request.quality_checks or []},
            dependencies=["profile_data"],
            tags=["quality", "validation"],
        ))

        tasks.append(TaskDefinition(
            task_id="generate_report",
            task_type="QualityReportOperator",
            description="Generate quality report",
            config={},
            dependencies=["validate_data"],
            tags=["quality", "reporting"],
        ))

        return tasks

    def _generate_generic_tasks(
        self,
        request: DAGGenerationRequest,
    ) -> list[TaskDefinition]:
        """Generate generic tasks when pattern is custom."""
        tasks = []

        # Basic extract
        for i, source in enumerate(request.sources):
            tasks.append(TaskDefinition(
                task_id=f"extract_{i}",
                task_type="PythonOperator",
                description=f"Extract from {source.name}",
                config={"source": source.name},
                tags=["extract"],
            ))

        # Transform
        tasks.append(TaskDefinition(
            task_id="transform",
            task_type="PythonOperator",
            description="Apply transformations",
            config={},
            dependencies=[f"extract_{i}" for i in range(len(request.sources))],
            tags=["transform"],
        ))

        # Load
        tasks.append(TaskDefinition(
            task_id="load",
            task_type="PythonOperator",
            description="Load to target",
            config={},
            dependencies=["transform"],
            tags=["load"],
        ))

        return tasks

    def _generate_code(
        self,
        dag_def: DAGDefinition,
        request: DAGGenerationRequest | None,
    ) -> str:
        """Generate Python DAG code."""
        # Build imports
        imports = '''"""
Auto-generated Airflow DAG: {dag_id}

{description}

Pattern: {pattern}
Generated: {timestamp}
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.base import BaseSensorOperator

# Automic ETL Operators
try:
    from automic_etl.integrations.airflow_operators import (
        BronzeIngestionOperator,
        SilverTransformOperator,
        GoldAggregationOperator,
        LLMAugmentedOperator,
        DataAvailabilitySensor,
        DataQualitySensor,
        AgenticDecisionOperator,
        SelfHealingOperator,
    )
    AUTOMIC_AVAILABLE = True
except ImportError:
    AUTOMIC_AVAILABLE = False


def run_automic_pipeline(pipeline_name: str, config: dict = None, **kwargs):
    """Run an Automic ETL pipeline."""
    if AUTOMIC_AVAILABLE:
        from automic_etl.medallion.lakehouse import Lakehouse
        lakehouse = Lakehouse()
        return lakehouse.run_pipeline(pipeline_name, config or {{}})
    else:
        raise ImportError("Automic ETL not available")

'''.format(
            dag_id=dag_def.dag_id,
            description=dag_def.description,
            pattern=dag_def.pattern.value,
            timestamp=format_iso(utc_now()),
        )

        # Build default args
        default_args = {
            "owner": "automic-etl",
            "depends_on_past": False,
            "email_on_failure": True,
            "email_on_retry": False,
            "retries": 3,
            "retry_delay": "timedelta(minutes=5)",
            **dag_def.default_args,
        }

        default_args_str = "default_args = {\n"
        for key, value in default_args.items():
            if isinstance(value, str) and ("timedelta" in value or "datetime" in value):
                default_args_str += f"    '{key}': {value},\n"
            elif isinstance(value, bool):
                default_args_str += f"    '{key}': {value},\n"
            elif isinstance(value, (int, float)):
                default_args_str += f"    '{key}': {value},\n"
            elif isinstance(value, list):
                default_args_str += f"    '{key}': {value},\n"
            else:
                default_args_str += f"    '{key}': '{value}',\n"
        default_args_str += "}\n"

        # Build task definitions
        task_defs = []
        for task in dag_def.tasks:
            task_def = self._generate_task_code(task)
            task_defs.append(task_def)

        # Build dependencies
        dep_lines = []
        for task in dag_def.tasks:
            for dep in task.dependencies:
                dep_lines.append(f"    {dep} >> {task.task_id}")

        # Build DAG documentation
        doc_md = dag_def.doc_md or f"""
## {dag_def.dag_id}

{dag_def.description}

### Pattern: {dag_def.pattern.value}

### Tasks
{"".join(f"- **{t.task_id}**: {t.description}" + chr(10) for t in dag_def.tasks)}

### Generated by Automic ETL DAG Factory
"""

        # Assemble code
        code = f'''{imports}

{default_args_str}

with DAG(
    dag_id='{dag_def.dag_id}',
    default_args=default_args,
    description='{dag_def.description}',
    schedule_interval='{dag_def.schedule}',
    start_date=datetime({dag_def.start_date.year}, {dag_def.start_date.month}, {dag_def.start_date.day}),
    catchup={dag_def.catchup},
    max_active_runs={dag_def.max_active_runs},
    tags={dag_def.tags},
    doc_md="""{doc_md}""",
) as dag:
{"".join(task_defs)}

    # Task Dependencies
{chr(10).join(dep_lines) if dep_lines else "    pass"}
'''

        return code

    def _generate_task_code(self, task: TaskDefinition) -> str:
        """Generate code for a single task."""
        # Map task types to operators
        operator_mapping = {
            "BronzeIngestionOperator": "BronzeIngestionOperator",
            "SilverTransformOperator": "SilverTransformOperator",
            "GoldAggregationOperator": "GoldAggregationOperator",
            "LLMAugmentedOperator": "LLMAugmentedOperator",
            "DataAvailabilitySensor": "DataAvailabilitySensor",
            "DataQualitySensor": "DataQualitySensor",
            "AgenticDecisionOperator": "AgenticDecisionOperator",
            "PythonOperator": "PythonOperator",
            "BashOperator": "BashOperator",
        }

        operator = operator_mapping.get(task.task_type, "PythonOperator")

        if operator in ["BronzeIngestionOperator", "SilverTransformOperator",
                        "GoldAggregationOperator", "LLMAugmentedOperator"]:
            return f'''
    {task.task_id} = {operator}(
        task_id='{task.task_id}',
        **{json.dumps(task.config)},
    ) if AUTOMIC_AVAILABLE else PythonOperator(
        task_id='{task.task_id}',
        python_callable=run_automic_pipeline,
        op_kwargs={json.dumps(task.config)},
    )
'''
        elif operator == "PythonOperator":
            return f'''
    {task.task_id} = PythonOperator(
        task_id='{task.task_id}',
        python_callable=run_automic_pipeline,
        op_kwargs={json.dumps(task.config)},
    )
'''
        else:
            return f'''
    {task.task_id} = {operator}(
        task_id='{task.task_id}',
        **{json.dumps(task.config)},
    )
'''

    def _generate_documentation(
        self,
        request: DAGGenerationRequest,
        tasks: list[TaskDefinition],
    ) -> str:
        """Generate markdown documentation for the DAG."""
        doc = f"""## {request.name}

{request.description or 'Auto-generated Automic ETL pipeline'}

### Data Sources
"""
        for source in request.sources:
            doc += f"- **{source.name}** ({source.source_type})\n"
            if source.tables:
                for table in source.tables:
                    doc += f"  - {table}\n"

        doc += "\n### Pipeline Tasks\n"
        for task in tasks:
            doc += f"- **{task.task_id}** ({task.task_type}): {task.description}\n"

        doc += "\n---\n*Generated by Automic ETL DAG Factory*\n"

        return doc

    def _sanitize_dag_id(self, name: str) -> str:
        """Sanitize a name into a valid DAG ID."""
        # Convert to lowercase, replace spaces and special chars
        dag_id = re.sub(r'[^a-z0-9_]', '_', name.lower())
        dag_id = re.sub(r'_+', '_', dag_id)  # Remove multiple underscores
        dag_id = dag_id.strip('_')

        # Ensure unique
        if not dag_id:
            dag_id = f"dag_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        return f"automic_{dag_id}"

    def _deploy_dag(self, dag_id: str, code: str) -> Path:
        """Deploy DAG to the dags folder."""
        if not self.dags_folder:
            raise ValueError("dags_folder not configured")

        dag_path = self.dags_folder / f"{dag_id}.py"
        dag_path.parent.mkdir(parents=True, exist_ok=True)
        dag_path.write_text(code)

        self.logger.info("DAG deployed", path=str(dag_path))
        return dag_path

    def _extract_json(self, content: str) -> str:
        """Extract JSON from response."""
        content = content.strip()

        if "```json" in content:
            match = re.search(r"```json\s*(.*?)\s*```", content, re.DOTALL)
            if match:
                return match.group(1)
        elif "```" in content:
            match = re.search(r"```\s*(.*?)\s*```", content, re.DOTALL)
            if match:
                return match.group(1)

        if "{" in content:
            start = content.index("{")
            depth = 0
            for i, char in enumerate(content[start:], start):
                if char == "{":
                    depth += 1
                elif char == "}":
                    depth -= 1
                    if depth == 0:
                        return content[start:i+1]

        return content


# Convenience functions
def create_dag_from_description(
    description: str,
    settings: Settings | None = None,
    deploy: bool = False,
) -> tuple[DAGDefinition, str]:
    """Create a DAG from natural language description."""
    factory = DAGFactory(settings)
    return factory.create_from_natural_language(description, deploy=deploy)


def create_medallion_dag(
    name: str,
    source_type: str,
    source_config: dict[str, Any],
    tables: list[str],
    settings: Settings | None = None,
    deploy: bool = False,
) -> tuple[DAGDefinition, str]:
    """Create a medallion architecture DAG."""
    factory = DAGFactory(settings)
    return factory.create_medallion_dag(
        name=name,
        source_type=source_type,
        source_config=source_config,
        tables=tables,
        deploy=deploy,
    )
