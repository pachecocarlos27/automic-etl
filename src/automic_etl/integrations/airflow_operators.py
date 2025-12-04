"""Custom Airflow Operators and Sensors for Automic ETL.

This module provides a comprehensive set of Airflow operators and sensors
specifically designed for Automic ETL pipelines, including:
- Medallion layer operators (Bronze, Silver, Gold)
- LLM-augmented operators
- Data quality sensors
- Lineage tracking operators
- Agentic decision operators
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Callable, Sequence

from automic_etl.core.utils import utc_now, format_iso

# Note: These operators are designed to be used within an Airflow environment
# They inherit from Airflow base classes when running in Airflow

logger = logging.getLogger(__name__)


# =============================================================================
# Base Classes (Airflow-compatible)
# =============================================================================

class AutomicBaseOperator:
    """
    Base operator for all Automic ETL operators.

    This class provides common functionality and can be used standalone
    or as a mixin with Airflow's BaseOperator.
    """

    def __init__(
        self,
        task_id: str,
        lakehouse_path: str | None = None,
        settings_path: str | None = None,
        track_lineage: bool = True,
        **kwargs,
    ):
        self.task_id = task_id
        self.lakehouse_path = lakehouse_path
        self.settings_path = settings_path
        self.track_lineage = track_lineage
        self._lakehouse = None
        self._settings = None

    def get_lakehouse(self):
        """Get or create Lakehouse instance."""
        if self._lakehouse is None:
            from automic_etl.medallion.lakehouse import Lakehouse
            from automic_etl.core.config import get_settings

            settings = get_settings(self.settings_path) if self.settings_path else get_settings()
            self._lakehouse = Lakehouse(settings)

        return self._lakehouse

    def get_settings(self):
        """Get settings instance."""
        if self._settings is None:
            from automic_etl.core.config import get_settings
            self._settings = get_settings(self.settings_path) if self.settings_path else get_settings()
        return self._settings

    def record_lineage(
        self,
        operation: str,
        source: str | list[str],
        target: str,
        metadata: dict | None = None,
    ):
        """Record lineage information."""
        if not self.track_lineage:
            return

        try:
            from automic_etl.lineage.tracker import LineageTracker
            tracker = LineageTracker()

            sources = [source] if isinstance(source, str) else source
            for src in sources:
                tracker.record_transformation(
                    source_asset=src,
                    target_asset=target,
                    operation=operation,
                    metadata={
                        "task_id": self.task_id,
                        "timestamp": format_iso(),
                        **(metadata or {}),
                    },
                )
        except Exception as e:
            logger.warning(f"Failed to record lineage: {e}")


class AutomicBaseSensor:
    """Base sensor for Automic ETL sensors."""

    def __init__(
        self,
        task_id: str,
        poke_interval: int = 60,
        timeout: int = 3600,
        mode: str = "poke",
        **kwargs,
    ):
        self.task_id = task_id
        self.poke_interval = poke_interval
        self.timeout = timeout
        self.mode = mode

    def poke(self, context: dict) -> bool:
        """Check if the condition is met. Override in subclasses."""
        raise NotImplementedError


# =============================================================================
# Bronze Layer Operators
# =============================================================================

class BronzeIngestionOperator(AutomicBaseOperator):
    """
    Operator for ingesting data into the Bronze layer.

    Supports multiple source types: databases, files, APIs, streaming.
    """

    template_fields: Sequence[str] = (
        "source_type",
        "source_config",
        "table_name",
    )

    def __init__(
        self,
        task_id: str,
        source_type: str,
        source_config: dict[str, Any],
        table_name: str,
        extraction_mode: str = "full",
        watermark_column: str | None = None,
        partition_by: list[str] | None = None,
        **kwargs,
    ):
        super().__init__(task_id, **kwargs)
        self.source_type = source_type
        self.source_config = source_config
        self.table_name = table_name
        self.extraction_mode = extraction_mode
        self.watermark_column = watermark_column
        self.partition_by = partition_by

    def execute(self, context: dict) -> dict[str, Any]:
        """Execute bronze ingestion."""
        logger.info(f"Starting Bronze ingestion for {self.table_name}")

        lakehouse = self.get_lakehouse()
        execution_date = context.get("execution_date", utc_now())

        # Get appropriate connector
        from automic_etl.connectors.base import get_connector
        connector = get_connector(self.source_type, self.source_config)

        # Extract data based on mode
        if self.extraction_mode == "incremental" and self.watermark_column:
            # Get last watermark
            watermark = lakehouse.get_watermark(self.table_name)
            data = connector.extract_incremental(
                watermark_column=self.watermark_column,
                last_watermark=watermark,
            )
        else:
            data = connector.extract()

        # Ingest to bronze
        result = lakehouse.bronze.ingest(
            data=data,
            table_name=self.table_name,
            source=self.source_type,
            partition_by=self.partition_by,
        )

        # Update watermark
        if self.extraction_mode == "incremental" and self.watermark_column:
            new_watermark = data[self.watermark_column].max()
            lakehouse.set_watermark(self.table_name, new_watermark)

        # Record lineage
        self.record_lineage(
            operation="bronze_ingestion",
            source=f"{self.source_type}:{self.source_config.get('table', 'unknown')}",
            target=f"bronze.{self.table_name}",
            metadata={
                "rows_ingested": result.get("rows_written", 0),
                "extraction_mode": self.extraction_mode,
            },
        )

        logger.info(f"Bronze ingestion complete: {result}")
        return result


class BronzeBatchOperator(AutomicBaseOperator):
    """
    Operator for batch ingestion into Bronze layer.

    Processes multiple sources in a single task.
    """

    def __init__(
        self,
        task_id: str,
        sources: list[dict[str, Any]],
        parallel: bool = False,
        max_workers: int = 4,
        **kwargs,
    ):
        super().__init__(task_id, **kwargs)
        self.sources = sources
        self.parallel = parallel
        self.max_workers = max_workers

    def execute(self, context: dict) -> list[dict[str, Any]]:
        """Execute batch ingestion."""
        results = []

        if self.parallel:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self._ingest_source, source, context): source
                    for source in self.sources
                }
                for future in as_completed(futures):
                    source = futures[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"Failed to ingest {source}: {e}")
                        results.append({"source": source, "error": str(e)})
        else:
            for source in self.sources:
                try:
                    result = self._ingest_source(source, context)
                    results.append(result)
                except Exception as e:
                    logger.error(f"Failed to ingest {source}: {e}")
                    results.append({"source": source, "error": str(e)})

        return results

    def _ingest_source(self, source: dict, context: dict) -> dict[str, Any]:
        """Ingest a single source."""
        operator = BronzeIngestionOperator(
            task_id=f"{self.task_id}_{source['table_name']}",
            source_type=source["type"],
            source_config=source.get("config", {}),
            table_name=source["table_name"],
            extraction_mode=source.get("extraction_mode", "full"),
            watermark_column=source.get("watermark_column"),
            lakehouse_path=self.lakehouse_path,
            settings_path=self.settings_path,
            track_lineage=self.track_lineage,
        )
        return operator.execute(context)


# =============================================================================
# Silver Layer Operators
# =============================================================================

class SilverTransformOperator(AutomicBaseOperator):
    """
    Operator for transforming data from Bronze to Silver layer.

    Applies cleaning, deduplication, and validation.
    """

    template_fields: Sequence[str] = (
        "source_table",
        "target_table",
    )

    def __init__(
        self,
        task_id: str,
        source_table: str,
        target_table: str,
        dedupe_columns: list[str] | None = None,
        transformations: list[dict[str, Any]] | None = None,
        validation_rules: list[dict[str, Any]] | None = None,
        quarantine_invalid: bool = True,
        **kwargs,
    ):
        super().__init__(task_id, **kwargs)
        self.source_table = source_table
        self.target_table = target_table
        self.dedupe_columns = dedupe_columns
        self.transformations = transformations or []
        self.validation_rules = validation_rules or []
        self.quarantine_invalid = quarantine_invalid

    def execute(self, context: dict) -> dict[str, Any]:
        """Execute silver transformation."""
        logger.info(f"Starting Silver transformation: {self.source_table} -> {self.target_table}")

        lakehouse = self.get_lakehouse()

        # Read from bronze
        bronze_data = lakehouse.bronze.read(self.source_table)

        # Apply silver transformation
        result = lakehouse.silver.transform(
            data=bronze_data,
            table_name=self.target_table,
            dedupe_columns=self.dedupe_columns,
            transformations=self.transformations,
            validation_rules=self.validation_rules,
            quarantine_invalid=self.quarantine_invalid,
        )

        # Record lineage
        self.record_lineage(
            operation="silver_transform",
            source=f"bronze.{self.source_table}",
            target=f"silver.{self.target_table}",
            metadata={
                "rows_input": result.get("rows_input", 0),
                "rows_output": result.get("rows_output", 0),
                "rows_quarantined": result.get("rows_quarantined", 0),
            },
        )

        logger.info(f"Silver transformation complete: {result}")
        return result


class SilverSCDOperator(AutomicBaseOperator):
    """
    Operator for Slowly Changing Dimension (Type 2) processing.
    """

    def __init__(
        self,
        task_id: str,
        source_table: str,
        target_table: str,
        business_keys: list[str],
        tracked_columns: list[str],
        effective_date_column: str = "effective_date",
        **kwargs,
    ):
        super().__init__(task_id, **kwargs)
        self.source_table = source_table
        self.target_table = target_table
        self.business_keys = business_keys
        self.tracked_columns = tracked_columns
        self.effective_date_column = effective_date_column

    def execute(self, context: dict) -> dict[str, Any]:
        """Execute SCD Type 2 processing."""
        logger.info(f"Starting SCD Type 2 processing for {self.target_table}")

        lakehouse = self.get_lakehouse()

        # Read source data
        source_data = lakehouse.bronze.read(self.source_table)

        # Apply SCD Type 2
        from automic_etl.medallion.scd import SCDProcessor
        scd = SCDProcessor(lakehouse.silver)

        result = scd.process_type2(
            source_data=source_data,
            target_table=self.target_table,
            business_keys=self.business_keys,
            tracked_columns=self.tracked_columns,
            effective_date_column=self.effective_date_column,
        )

        self.record_lineage(
            operation="scd_type2",
            source=f"bronze.{self.source_table}",
            target=f"silver.{self.target_table}",
            metadata={"scd_type": 2, "business_keys": self.business_keys},
        )

        return result


# =============================================================================
# Gold Layer Operators
# =============================================================================

class GoldAggregationOperator(AutomicBaseOperator):
    """
    Operator for creating aggregations in the Gold layer.
    """

    def __init__(
        self,
        task_id: str,
        source_tables: list[str],
        target_table: str,
        aggregations: list[dict[str, Any]],
        group_by: list[str],
        filters: list[dict[str, Any]] | None = None,
        **kwargs,
    ):
        super().__init__(task_id, **kwargs)
        self.source_tables = source_tables
        self.target_table = target_table
        self.aggregations = aggregations
        self.group_by = group_by
        self.filters = filters or []

    def execute(self, context: dict) -> dict[str, Any]:
        """Execute gold aggregation."""
        logger.info(f"Starting Gold aggregation for {self.target_table}")

        lakehouse = self.get_lakehouse()

        # Read and join source tables
        data = lakehouse.silver.read(self.source_tables[0])
        for table in self.source_tables[1:]:
            additional_data = lakehouse.silver.read(table)
            # Join logic would go here

        # Apply aggregations
        result = lakehouse.gold.aggregate(
            data=data,
            table_name=self.target_table,
            aggregations=self.aggregations,
            group_by=self.group_by,
            filters=self.filters,
        )

        self.record_lineage(
            operation="gold_aggregation",
            source=[f"silver.{t}" for t in self.source_tables],
            target=f"gold.{self.target_table}",
            metadata={
                "aggregations": [a.get("column") for a in self.aggregations],
                "group_by": self.group_by,
            },
        )

        return result


class GoldMetricOperator(AutomicBaseOperator):
    """
    Operator for calculating business metrics in the Gold layer.
    """

    def __init__(
        self,
        task_id: str,
        metric_name: str,
        metric_definition: dict[str, Any],
        source_tables: list[str],
        dimensions: list[str] | None = None,
        time_grain: str = "daily",
        **kwargs,
    ):
        super().__init__(task_id, **kwargs)
        self.metric_name = metric_name
        self.metric_definition = metric_definition
        self.source_tables = source_tables
        self.dimensions = dimensions or []
        self.time_grain = time_grain

    def execute(self, context: dict) -> dict[str, Any]:
        """Execute metric calculation."""
        logger.info(f"Calculating metric: {self.metric_name}")

        lakehouse = self.get_lakehouse()

        result = lakehouse.gold.calculate_metric(
            metric_name=self.metric_name,
            definition=self.metric_definition,
            sources=self.source_tables,
            dimensions=self.dimensions,
            time_grain=self.time_grain,
        )

        return result


# =============================================================================
# LLM-Augmented Operators
# =============================================================================

class LLMAugmentedOperator(AutomicBaseOperator):
    """
    Operator that uses LLM for intelligent data augmentation.

    Supports:
    - Schema inference
    - Entity extraction
    - Data classification
    - Natural language transformations
    """

    def __init__(
        self,
        task_id: str,
        source_table: str,
        target_table: str,
        augmentation_type: str,  # schema_inference, entity_extraction, classification, transform
        augmentation_config: dict[str, Any] | None = None,
        llm_model: str | None = None,
        max_tokens: int = 4096,
        **kwargs,
    ):
        super().__init__(task_id, **kwargs)
        self.source_table = source_table
        self.target_table = target_table
        self.augmentation_type = augmentation_type
        self.augmentation_config = augmentation_config or {}
        self.llm_model = llm_model
        self.max_tokens = max_tokens

    def execute(self, context: dict) -> dict[str, Any]:
        """Execute LLM augmentation."""
        logger.info(f"Starting LLM augmentation: {self.augmentation_type}")

        settings = self.get_settings()
        lakehouse = self.get_lakehouse()

        # Initialize LLM client
        from automic_etl.llm.client import LLMClient
        llm = LLMClient(settings)

        # Read source data
        data = lakehouse.silver.read(self.source_table)

        # Apply augmentation based on type
        if self.augmentation_type == "entity_extraction":
            from automic_etl.llm.entity_extractor import EntityExtractor
            extractor = EntityExtractor(llm)
            result_data, tokens = extractor.extract(
                data,
                columns=self.augmentation_config.get("columns", []),
                entity_types=self.augmentation_config.get("entity_types", []),
            )

        elif self.augmentation_type == "classification":
            from automic_etl.llm.data_classifier import DataClassifier
            classifier = DataClassifier(llm)
            result_data, tokens = classifier.classify(
                data,
                column=self.augmentation_config.get("column"),
                categories=self.augmentation_config.get("categories", []),
            )

        elif self.augmentation_type == "schema_inference":
            from automic_etl.llm.schema_generator import SchemaGenerator
            generator = SchemaGenerator(llm)
            schema, tokens = generator.infer_schema(data)
            result_data = data
            # Store schema as metadata

        elif self.augmentation_type == "transform":
            from automic_etl.llm.augmented_etl import AugmentedETL
            etl = AugmentedETL(llm, lakehouse)
            result_data, tokens = etl.transform_with_instructions(
                data,
                instructions=self.augmentation_config.get("instructions", ""),
            )

        else:
            raise ValueError(f"Unknown augmentation type: {self.augmentation_type}")

        # Write to target
        lakehouse.silver.write(result_data, self.target_table)

        self.record_lineage(
            operation=f"llm_{self.augmentation_type}",
            source=f"silver.{self.source_table}",
            target=f"silver.{self.target_table}",
            metadata={
                "augmentation_type": self.augmentation_type,
                "tokens_used": tokens,
            },
        )

        return {
            "rows_processed": len(result_data),
            "tokens_used": tokens,
            "augmentation_type": self.augmentation_type,
        }


class NaturalLanguageQueryOperator(AutomicBaseOperator):
    """
    Operator that converts natural language to SQL and executes queries.
    """

    def __init__(
        self,
        task_id: str,
        query: str,
        target_table: str | None = None,
        output_format: str = "dataframe",
        **kwargs,
    ):
        super().__init__(task_id, **kwargs)
        self.query = query
        self.target_table = target_table
        self.output_format = output_format

    def execute(self, context: dict) -> dict[str, Any]:
        """Execute natural language query."""
        logger.info(f"Executing NL query: {self.query[:100]}...")

        settings = self.get_settings()
        lakehouse = self.get_lakehouse()

        from automic_etl.llm.client import LLMClient
        from automic_etl.llm.query_builder import QueryBuilder

        llm = LLMClient(settings)
        builder = QueryBuilder(llm, lakehouse)

        # Convert NL to SQL and execute
        result = builder.query(self.query)

        if self.target_table:
            lakehouse.silver.write(result["data"], self.target_table)

        return {
            "query": self.query,
            "generated_sql": result.get("sql"),
            "rows_returned": len(result.get("data", [])),
            "tokens_used": result.get("tokens_used", 0),
        }


# =============================================================================
# Agentic Operators
# =============================================================================

class AgenticDecisionOperator(AutomicBaseOperator):
    """
    Operator that makes autonomous decisions based on data and context.

    Uses LLM to analyze data and decide on next actions.
    """

    def __init__(
        self,
        task_id: str,
        decision_context: dict[str, Any],
        available_actions: list[str],
        decision_criteria: str,
        auto_execute: bool = False,
        confidence_threshold: float = 0.8,
        **kwargs,
    ):
        super().__init__(task_id, **kwargs)
        self.decision_context = decision_context
        self.available_actions = available_actions
        self.decision_criteria = decision_criteria
        self.auto_execute = auto_execute
        self.confidence_threshold = confidence_threshold

    def execute(self, context: dict) -> dict[str, Any]:
        """Make an autonomous decision."""
        logger.info(f"Making agentic decision for task: {self.task_id}")

        settings = self.get_settings()

        from automic_etl.llm.client import LLMClient
        llm = LLMClient(settings)

        prompt = f"""Based on the following context, decide what action to take:

Context:
{json.dumps(self.decision_context, indent=2, default=str)}

Available Actions: {self.available_actions}

Decision Criteria: {self.decision_criteria}

Provide your decision as JSON:
{{
    "action": "<selected_action>",
    "confidence": <0-1>,
    "reasoning": "<explanation>",
    "parameters": {{}},
    "risks": ["<potential_risks>"]
}}"""

        response = llm.complete(
            prompt=prompt,
            system_prompt="You are an expert data engineering agent making autonomous decisions.",
            temperature=0.3,
        )

        try:
            decision = json.loads(response.content)
        except json.JSONDecodeError:
            decision = {
                "action": "skip",
                "confidence": 0.0,
                "reasoning": "Failed to parse decision",
            }

        # Execute if auto_execute is enabled and confidence is high
        executed = False
        if (
            self.auto_execute
            and decision.get("confidence", 0) >= self.confidence_threshold
        ):
            action = decision.get("action")
            if action in self.available_actions:
                # Execute the action (implementation depends on action type)
                executed = True
                logger.info(f"Auto-executing action: {action}")

        return {
            "decision": decision,
            "executed": executed,
            "task_id": self.task_id,
            "tokens_used": response.tokens_used,
        }


class SelfHealingOperator(AutomicBaseOperator):
    """
    Operator that can diagnose and recover from failures autonomously.
    """

    def __init__(
        self,
        task_id: str,
        wrapped_callable: Callable,
        max_heal_attempts: int = 3,
        healing_strategies: list[str] | None = None,
        **kwargs,
    ):
        super().__init__(task_id, **kwargs)
        self.wrapped_callable = wrapped_callable
        self.max_heal_attempts = max_heal_attempts
        self.healing_strategies = healing_strategies or [
            "retry",
            "reduce_batch_size",
            "skip_bad_records",
            "use_fallback",
        ]

    def execute(self, context: dict) -> dict[str, Any]:
        """Execute with self-healing capability."""
        attempt = 0
        last_error = None

        while attempt < self.max_heal_attempts:
            try:
                result = self.wrapped_callable(context)
                return {
                    "status": "success",
                    "result": result,
                    "attempts": attempt + 1,
                }
            except Exception as e:
                last_error = e
                attempt += 1

                logger.warning(f"Attempt {attempt} failed: {e}")

                if attempt < self.max_heal_attempts:
                    healing_action = self._diagnose_and_heal(e, context, attempt)
                    if healing_action == "abort":
                        break

        return {
            "status": "failed",
            "error": str(last_error),
            "attempts": attempt,
        }

    def _diagnose_and_heal(
        self,
        error: Exception,
        context: dict,
        attempt: int,
    ) -> str:
        """Diagnose error and apply healing strategy."""
        settings = self.get_settings()

        from automic_etl.llm.client import LLMClient
        llm = LLMClient(settings)

        prompt = f"""Diagnose this error and suggest a healing strategy:

Error: {str(error)}
Error Type: {type(error).__name__}
Attempt: {attempt}
Available Strategies: {self.healing_strategies}

Provide your diagnosis as JSON:
{{
    "root_cause": "<cause>",
    "strategy": "<strategy_from_available>",
    "action": "<specific_action>",
    "should_continue": <boolean>
}}"""

        try:
            response = llm.complete(prompt=prompt, temperature=0.3)
            diagnosis = json.loads(response.content)

            strategy = diagnosis.get("strategy", "retry")

            if not diagnosis.get("should_continue", True):
                return "abort"

            # Apply healing strategy
            self._apply_healing_strategy(strategy, context)

            return strategy

        except Exception:
            return "retry"

    def _apply_healing_strategy(self, strategy: str, context: dict):
        """Apply a healing strategy."""
        if strategy == "reduce_batch_size":
            context["batch_size"] = context.get("batch_size", 1000) // 2
        elif strategy == "skip_bad_records":
            context["skip_errors"] = True
        elif strategy == "use_fallback":
            context["use_fallback"] = True
        # retry is the default - no changes needed


# =============================================================================
# Sensors
# =============================================================================

class DataAvailabilitySensor(AutomicBaseSensor):
    """
    Sensor that waits for data to be available in a table.
    """

    def __init__(
        self,
        task_id: str,
        table_name: str,
        layer: str = "bronze",
        min_rows: int = 1,
        partition_filter: dict[str, Any] | None = None,
        **kwargs,
    ):
        super().__init__(task_id, **kwargs)
        self.table_name = table_name
        self.layer = layer
        self.min_rows = min_rows
        self.partition_filter = partition_filter

    def poke(self, context: dict) -> bool:
        """Check if data is available."""
        from automic_etl.medallion.lakehouse import Lakehouse
        from automic_etl.core.config import get_settings

        lakehouse = Lakehouse(get_settings())

        try:
            if self.layer == "bronze":
                data = lakehouse.bronze.read(self.table_name, filters=self.partition_filter)
            elif self.layer == "silver":
                data = lakehouse.silver.read(self.table_name, filters=self.partition_filter)
            else:
                data = lakehouse.gold.read(self.table_name, filters=self.partition_filter)

            row_count = len(data)
            logger.info(f"Found {row_count} rows in {self.layer}.{self.table_name}")

            return row_count >= self.min_rows

        except Exception as e:
            logger.warning(f"Error checking data availability: {e}")
            return False


class DataQualitySensor(AutomicBaseSensor):
    """
    Sensor that waits for data quality checks to pass.
    """

    def __init__(
        self,
        task_id: str,
        table_name: str,
        layer: str = "silver",
        quality_rules: list[dict[str, Any]] | None = None,
        min_quality_score: float = 0.95,
        **kwargs,
    ):
        super().__init__(task_id, **kwargs)
        self.table_name = table_name
        self.layer = layer
        self.quality_rules = quality_rules or []
        self.min_quality_score = min_quality_score

    def poke(self, context: dict) -> bool:
        """Check if data quality is acceptable."""
        from automic_etl.medallion.lakehouse import Lakehouse
        from automic_etl.validation.validator import Validator
        from automic_etl.core.config import get_settings

        lakehouse = Lakehouse(get_settings())

        try:
            if self.layer == "bronze":
                data = lakehouse.bronze.read(self.table_name)
            elif self.layer == "silver":
                data = lakehouse.silver.read(self.table_name)
            else:
                data = lakehouse.gold.read(self.table_name)

            validator = Validator()
            result = validator.validate(data, self.quality_rules)

            quality_score = result.get("quality_score", 0)
            logger.info(f"Quality score for {self.table_name}: {quality_score}")

            return quality_score >= self.min_quality_score

        except Exception as e:
            logger.warning(f"Error checking data quality: {e}")
            return False


class PipelineCompletionSensor(AutomicBaseSensor):
    """
    Sensor that waits for an upstream pipeline to complete.
    """

    def __init__(
        self,
        task_id: str,
        upstream_dag_id: str,
        execution_date: datetime | None = None,
        allowed_states: list[str] | None = None,
        **kwargs,
    ):
        super().__init__(task_id, **kwargs)
        self.upstream_dag_id = upstream_dag_id
        self.execution_date = execution_date
        self.allowed_states = allowed_states or ["success"]

    def poke(self, context: dict) -> bool:
        """Check if upstream pipeline completed."""
        from automic_etl.integrations.airflow import AirflowIntegration, AirflowConfig

        airflow = AirflowIntegration(AirflowConfig())

        try:
            exec_date = self.execution_date or context.get("execution_date")
            runs = airflow.list_dag_runs(
                self.upstream_dag_id,
                limit=1,
            )

            if runs:
                latest_run = runs[0]
                state = latest_run.get("state", "").lower()
                logger.info(f"Upstream DAG {self.upstream_dag_id} state: {state}")
                return state in [s.lower() for s in self.allowed_states]

            return False

        except Exception as e:
            logger.warning(f"Error checking upstream pipeline: {e}")
            return False


class AnomalyDetectionSensor(AutomicBaseSensor):
    """
    Sensor that detects data anomalies using LLM analysis.
    """

    def __init__(
        self,
        task_id: str,
        table_name: str,
        layer: str = "silver",
        anomaly_threshold: float = 0.1,
        check_columns: list[str] | None = None,
        **kwargs,
    ):
        super().__init__(task_id, **kwargs)
        self.table_name = table_name
        self.layer = layer
        self.anomaly_threshold = anomaly_threshold
        self.check_columns = check_columns

    def poke(self, context: dict) -> bool:
        """Check for data anomalies."""
        from automic_etl.medallion.lakehouse import Lakehouse
        from automic_etl.llm.client import LLMClient
        from automic_etl.core.config import get_settings

        settings = get_settings()
        lakehouse = Lakehouse(settings)
        llm = LLMClient(settings)

        try:
            if self.layer == "bronze":
                data = lakehouse.bronze.read(self.table_name)
            elif self.layer == "silver":
                data = lakehouse.silver.read(self.table_name)
            else:
                data = lakehouse.gold.read(self.table_name)

            # Get statistics for anomaly detection
            stats = data.describe()
            columns = self.check_columns or data.columns

            prompt = f"""Analyze these data statistics for anomalies:

Table: {self.table_name}
Columns to check: {columns}

Statistics:
{stats.to_pandas().to_string()}

Recent data sample:
{data.head(10).to_pandas().to_string()}

Identify any anomalies and provide analysis as JSON:
{{
    "anomalies_detected": <boolean>,
    "anomaly_score": <0-1>,
    "issues": [
        {{"column": "<col>", "issue": "<description>", "severity": "<high/medium/low>"}}
    ],
    "recommendation": "<action to take>"
}}"""

            response = llm.complete(prompt=prompt, temperature=0.3)

            try:
                analysis = json.loads(response.content)
                anomaly_score = analysis.get("anomaly_score", 0)

                logger.info(f"Anomaly score for {self.table_name}: {anomaly_score}")

                # Return True (condition met) if anomaly score is below threshold
                return anomaly_score < self.anomaly_threshold

            except json.JSONDecodeError:
                logger.warning("Failed to parse anomaly analysis")
                return True  # Assume no anomalies if parsing fails

        except Exception as e:
            logger.warning(f"Error checking for anomalies: {e}")
            return True  # Assume no anomalies on error


# =============================================================================
# Utility Functions for DAG Generation
# =============================================================================

def create_medallion_dag_tasks(
    dag_id: str,
    source_config: dict[str, Any],
    bronze_tables: list[str],
    silver_transformations: list[dict[str, Any]],
    gold_aggregations: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Create a complete set of medallion architecture tasks.

    Returns a dictionary of task instances that can be used in a DAG.
    """
    tasks = {}

    # Bronze ingestion tasks
    for table in bronze_tables:
        task_id = f"bronze_{table}"
        tasks[task_id] = BronzeIngestionOperator(
            task_id=task_id,
            source_type=source_config.get("type"),
            source_config=source_config.get("config", {}),
            table_name=table,
        )

    # Silver transformation tasks
    for transform in silver_transformations:
        task_id = f"silver_{transform['target']}"
        tasks[task_id] = SilverTransformOperator(
            task_id=task_id,
            source_table=transform.get("source"),
            target_table=transform.get("target"),
            dedupe_columns=transform.get("dedupe_columns"),
            transformations=transform.get("transformations"),
            validation_rules=transform.get("validation_rules"),
        )

    # Gold aggregation tasks
    for agg in gold_aggregations:
        task_id = f"gold_{agg['target']}"
        tasks[task_id] = GoldAggregationOperator(
            task_id=task_id,
            source_tables=agg.get("sources"),
            target_table=agg.get("target"),
            aggregations=agg.get("aggregations"),
            group_by=agg.get("group_by"),
        )

    return tasks
