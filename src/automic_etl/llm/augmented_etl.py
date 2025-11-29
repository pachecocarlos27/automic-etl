"""
Augmented ETL - LLM-powered intelligent data processing.

This module provides high-level LLM-augmented ETL operations that
intelligently process data using AI capabilities.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

import polars as pl
import structlog

from automic_etl.core.config import Settings
from automic_etl.llm.client import LLMClient
from automic_etl.llm.schema_generator import SchemaGenerator
from automic_etl.llm.entity_extractor import EntityExtractor
from automic_etl.llm.data_classifier import DataClassifier
from automic_etl.llm.query_builder import QueryBuilder

logger = structlog.get_logger()


@dataclass
class AugmentedResult:
    """Result from augmented ETL operation."""

    data: pl.DataFrame
    insights: dict[str, Any]
    recommendations: list[str]
    quality_score: float
    tokens_used: int
    processing_time: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DataProfile:
    """Comprehensive data profile."""

    row_count: int
    column_count: int
    columns: list[dict[str, Any]]
    quality_metrics: dict[str, float]
    patterns: list[dict[str, Any]]
    anomalies: list[dict[str, Any]]
    recommendations: list[str]


class AugmentedETL:
    """
    High-level LLM-augmented ETL processor.

    Combines multiple LLM capabilities for intelligent data processing:
    - Automatic schema inference and optimization
    - Data quality assessment and remediation
    - Entity extraction and enrichment
    - Intelligent transformations
    - Natural language data exploration
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = LLMClient(settings)
        self.schema_gen = SchemaGenerator(settings)
        self.entity_extractor = EntityExtractor(settings)
        self.classifier = DataClassifier(settings)
        self.query_builder = QueryBuilder(settings)
        self.logger = logger.bind(component="augmented_etl")

    def smart_ingest(
        self,
        data: pl.DataFrame | str | bytes | dict | list,
        source_name: str,
        data_type: str = "auto",
        extract_entities: bool = True,
        classify_data: bool = True,
        infer_schema: bool = True,
    ) -> AugmentedResult:
        """
        Intelligently ingest data with automatic processing.

        Args:
            data: Input data (DataFrame, file path, bytes, or dict/list)
            source_name: Source identifier
            data_type: Data type hint ('auto', 'structured', 'semi_structured', 'unstructured')
            extract_entities: Whether to extract entities from text columns
            classify_data: Whether to classify data sensitivity
            infer_schema: Whether to infer optimal schema

        Returns:
            AugmentedResult with processed data and insights
        """
        start_time = datetime.utcnow()
        tokens_used = 0
        insights: dict[str, Any] = {}
        recommendations: list[str] = []

        # Convert to DataFrame if needed
        df = self._to_dataframe(data, data_type)

        self.logger.info(
            "Smart ingestion started",
            source=source_name,
            rows=len(df),
            columns=len(df.columns),
        )

        # 1. Classify data
        if classify_data:
            classification = self.classifier.classify(df)
            insights["classification"] = {
                "primary_class": classification.primary_class,
                "sensitivity": classification.sensitivity_level,
                "contains_pii": classification.contains_pii,
                "pii_types": classification.pii_types,
            }

            if classification.contains_pii:
                recommendations.append(
                    f"PII detected ({', '.join(classification.pii_types)}). "
                    "Consider masking or encrypting sensitive columns."
                )

        # 2. Infer optimal schema
        if infer_schema:
            schema = self.schema_gen.infer_schema(df)
            insights["schema"] = schema

            # Apply suggested renames
            for col in schema.get("columns", []):
                if col.get("suggested_rename") and col["suggested_rename"] != col["name"]:
                    recommendations.append(
                        f"Consider renaming '{col['name']}' to '{col['suggested_rename']}'"
                    )

        # 3. Extract entities from text columns
        if extract_entities:
            text_columns = self._identify_text_columns(df)
            entity_results = {}

            for col in text_columns[:3]:  # Limit to first 3 text columns
                sample_text = " ".join(df[col].drop_nulls().head(5).to_list())
                if sample_text:
                    entities = self.entity_extractor.extract(sample_text)
                    entity_results[col] = {
                        "entity_count": len(entities),
                        "entity_types": list(set(e.entity_type for e in entities)),
                    }

            if entity_results:
                insights["entities"] = entity_results

        # 4. Generate quality score
        quality_score = self._calculate_quality_score(df, insights)
        insights["quality_score"] = quality_score

        if quality_score < 0.7:
            recommendations.append(
                "Data quality score is below 70%. Review null values and data consistency."
            )

        processing_time = (datetime.utcnow() - start_time).total_seconds()

        return AugmentedResult(
            data=df,
            insights=insights,
            recommendations=recommendations,
            quality_score=quality_score,
            tokens_used=tokens_used,
            processing_time=processing_time,
            metadata={"source": source_name},
        )

    def smart_transform(
        self,
        df: pl.DataFrame,
        instructions: str,
        validate: bool = True,
    ) -> AugmentedResult:
        """
        Apply intelligent transformations based on natural language instructions.

        Args:
            df: Input DataFrame
            instructions: Natural language transformation instructions
            validate: Whether to validate transformations

        Returns:
            AugmentedResult with transformed data
        """
        start_time = datetime.utcnow()

        prompt = f"""Analyze this DataFrame and generate Polars transformation code.

DataFrame Schema:
{self._format_schema(df)}

Sample Data:
{df.head(5).to_dicts()}

User Instructions:
{instructions}

Generate Python code using Polars (pl) to transform the DataFrame 'df'.
The code should be a single expression or series of expressions that can be executed.
Only use valid Polars operations.

Respond with JSON:
{{
    "transformations": [
        {{
            "description": "what this does",
            "code": "df = df.with_columns(...)"
        }}
    ],
    "warnings": ["any warnings"],
    "explanation": "overall explanation"
}}"""

        result, tokens = self.client.complete_json(
            prompt=prompt,
            system_prompt="You are a Polars data transformation expert. Generate safe, efficient code.",
        )

        transformed_df = df
        applied_transforms = []

        for transform in result.get("transformations", []):
            code = transform.get("code", "")
            if code and self._is_safe_code(code):
                try:
                    # Create a safe execution environment
                    local_vars = {"df": transformed_df, "pl": pl}
                    exec(code, {"pl": pl}, local_vars)
                    transformed_df = local_vars.get("df", transformed_df)
                    applied_transforms.append(transform["description"])
                except Exception as e:
                    self.logger.warning(f"Transform failed: {e}")

        processing_time = (datetime.utcnow() - start_time).total_seconds()

        return AugmentedResult(
            data=transformed_df,
            insights={
                "applied_transforms": applied_transforms,
                "explanation": result.get("explanation", ""),
            },
            recommendations=result.get("warnings", []),
            quality_score=1.0,
            tokens_used=tokens,
            processing_time=processing_time,
        )

    def analyze_data(
        self,
        df: pl.DataFrame,
        questions: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Analyze data and answer questions using LLM.

        Args:
            df: DataFrame to analyze
            questions: Optional list of questions to answer

        Returns:
            Analysis results and answers
        """
        # Generate comprehensive statistics
        stats = self._generate_statistics(df)

        prompt = f"""Analyze this dataset and provide insights.

Dataset Statistics:
{stats}

Sample Data:
{df.head(10).to_dicts()}

{"Questions to answer:" + chr(10) + chr(10).join(f"- {q}" for q in questions) if questions else "Provide general insights about this data."}

Respond with JSON:
{{
    "summary": "brief summary of the data",
    "key_findings": ["finding1", "finding2"],
    "data_quality_issues": ["issue1", "issue2"],
    "answers": {{"question": "answer"}} if questions provided,
    "suggested_analyses": ["analysis1", "analysis2"],
    "visualization_recommendations": ["chart1", "chart2"]
}}"""

        result, _ = self.client.complete_json(
            prompt=prompt,
            system_prompt="You are a data analyst expert. Provide actionable insights.",
        )

        return result

    def generate_profile(self, df: pl.DataFrame) -> DataProfile:
        """
        Generate a comprehensive data profile.

        Args:
            df: DataFrame to profile

        Returns:
            DataProfile with detailed statistics and insights
        """
        columns = []

        for col in df.columns:
            col_data = df[col]
            dtype = str(col_data.dtype)

            col_profile = {
                "name": col,
                "dtype": dtype,
                "null_count": col_data.null_count(),
                "null_pct": col_data.null_count() / len(df) * 100,
                "unique_count": col_data.n_unique(),
                "unique_pct": col_data.n_unique() / len(df) * 100,
            }

            # Numeric stats
            if col_data.dtype in [pl.Int64, pl.Float64, pl.Int32, pl.Float32]:
                col_profile.update({
                    "min": col_data.min(),
                    "max": col_data.max(),
                    "mean": col_data.mean(),
                    "std": col_data.std(),
                    "median": col_data.median(),
                })

            # String stats
            elif col_data.dtype in [pl.Utf8, pl.String]:
                lengths = col_data.drop_nulls().str.len_chars()
                col_profile.update({
                    "min_length": lengths.min() if len(lengths) > 0 else 0,
                    "max_length": lengths.max() if len(lengths) > 0 else 0,
                    "avg_length": lengths.mean() if len(lengths) > 0 else 0,
                })

            columns.append(col_profile)

        # Calculate quality metrics
        quality_metrics = {
            "completeness": 1 - (df.null_count().sum_horizontal().item() / (len(df) * len(df.columns))),
            "uniqueness": sum(c["unique_pct"] for c in columns) / len(columns) / 100,
        }

        # Use LLM to identify patterns and anomalies
        analysis = self.analyze_data(df)

        return DataProfile(
            row_count=len(df),
            column_count=len(df.columns),
            columns=columns,
            quality_metrics=quality_metrics,
            patterns=analysis.get("key_findings", []),
            anomalies=analysis.get("data_quality_issues", []),
            recommendations=analysis.get("suggested_analyses", []),
        )

    def suggest_pipeline(
        self,
        source_description: str,
        target_description: str,
        requirements: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Suggest an ETL pipeline based on requirements.

        Args:
            source_description: Description of source data
            target_description: Description of target/output
            requirements: Additional requirements

        Returns:
            Pipeline suggestion with steps and code
        """
        prompt = f"""Design an ETL pipeline for this scenario:

Source: {source_description}
Target: {target_description}
{"Requirements: " + ", ".join(requirements) if requirements else ""}

Suggest a complete pipeline using the Automic ETL framework.

Respond with JSON:
{{
    "pipeline_name": "name",
    "description": "what this pipeline does",
    "steps": [
        {{
            "step_number": 1,
            "name": "step name",
            "type": "ingest|transform|aggregate|export",
            "description": "what this step does",
            "code": "Python code using automic_etl"
        }}
    ],
    "estimated_complexity": "low|medium|high",
    "considerations": ["consideration1", "consideration2"],
    "alternative_approaches": ["approach1"]
}}"""

        result, _ = self.client.complete_json(
            prompt=prompt,
            system_prompt="You are an ETL architect expert with deep knowledge of the Automic ETL framework.",
        )

        return result

    def auto_clean(
        self,
        df: pl.DataFrame,
        aggressive: bool = False,
    ) -> AugmentedResult:
        """
        Automatically clean data based on detected issues.

        Args:
            df: DataFrame to clean
            aggressive: Whether to apply aggressive cleaning

        Returns:
            AugmentedResult with cleaned data
        """
        start_time = datetime.utcnow()
        cleaned_df = df.clone()
        actions_taken = []

        # 1. Handle null values
        for col in cleaned_df.columns:
            null_pct = cleaned_df[col].null_count() / len(cleaned_df)

            if null_pct > 0.5 and aggressive:
                cleaned_df = cleaned_df.drop(col)
                actions_taken.append(f"Dropped column '{col}' (>{50}% nulls)")
            elif null_pct > 0:
                dtype = cleaned_df[col].dtype
                if dtype in [pl.Int64, pl.Float64, pl.Int32, pl.Float32]:
                    median_val = cleaned_df[col].median()
                    cleaned_df = cleaned_df.with_columns(
                        pl.col(col).fill_null(median_val)
                    )
                    actions_taken.append(f"Filled nulls in '{col}' with median")
                elif dtype in [pl.Utf8, pl.String]:
                    cleaned_df = cleaned_df.with_columns(
                        pl.col(col).fill_null("")
                    )
                    actions_taken.append(f"Filled nulls in '{col}' with empty string")

        # 2. Remove duplicate rows
        original_len = len(cleaned_df)
        cleaned_df = cleaned_df.unique()
        if len(cleaned_df) < original_len:
            actions_taken.append(f"Removed {original_len - len(cleaned_df)} duplicate rows")

        # 3. Trim whitespace from string columns
        for col in cleaned_df.columns:
            if cleaned_df[col].dtype in [pl.Utf8, pl.String]:
                cleaned_df = cleaned_df.with_columns(
                    pl.col(col).str.strip_chars()
                )

        actions_taken.append("Trimmed whitespace from string columns")

        # 4. Standardize null string values
        null_strings = ["", "null", "NULL", "None", "N/A", "n/a", "NA", "-"]
        for col in cleaned_df.columns:
            if cleaned_df[col].dtype in [pl.Utf8, pl.String]:
                cleaned_df = cleaned_df.with_columns(
                    pl.when(pl.col(col).is_in(null_strings))
                    .then(None)
                    .otherwise(pl.col(col))
                    .alias(col)
                )

        actions_taken.append("Standardized null string values")

        processing_time = (datetime.utcnow() - start_time).total_seconds()

        return AugmentedResult(
            data=cleaned_df,
            insights={"actions_taken": actions_taken},
            recommendations=[],
            quality_score=self._calculate_quality_score(cleaned_df, {}),
            tokens_used=0,
            processing_time=processing_time,
        )

    def enrich_with_entities(
        self,
        df: pl.DataFrame,
        text_column: str,
        entity_types: list[str] | None = None,
    ) -> pl.DataFrame:
        """
        Enrich DataFrame by extracting entities from a text column.

        Args:
            df: Input DataFrame
            text_column: Column containing text to process
            entity_types: Entity types to extract

        Returns:
            DataFrame with new entity columns
        """
        if text_column not in df.columns:
            raise ValueError(f"Column '{text_column}' not found in DataFrame")

        # Process each row
        entity_data: dict[str, list] = {et: [] for et in (entity_types or self.entity_extractor.DEFAULT_ENTITY_TYPES)}

        for row in df.iter_rows(named=True):
            text = row.get(text_column, "")
            if text:
                entities = self.entity_extractor.extract(str(text), entity_types)
                entity_by_type = {}
                for entity in entities:
                    if entity.entity_type not in entity_by_type:
                        entity_by_type[entity.entity_type] = entity.value

                for et in entity_data.keys():
                    entity_data[et].append(entity_by_type.get(et))
            else:
                for et in entity_data.keys():
                    entity_data[et].append(None)

        # Add entity columns
        for entity_type, values in entity_data.items():
            col_name = f"_extracted_{entity_type.lower()}"
            df = df.with_columns(pl.Series(col_name, values))

        return df

    def _to_dataframe(self, data: Any, data_type: str) -> pl.DataFrame:
        """Convert various data types to DataFrame."""
        if isinstance(data, pl.DataFrame):
            return data
        elif isinstance(data, dict):
            return pl.DataFrame([data])
        elif isinstance(data, list):
            return pl.DataFrame(data)
        elif isinstance(data, str):
            # Assume file path
            if data.endswith(".csv"):
                return pl.read_csv(data)
            elif data.endswith(".json"):
                return pl.read_json(data)
            elif data.endswith(".parquet"):
                return pl.read_parquet(data)
        raise ValueError(f"Cannot convert {type(data)} to DataFrame")

    def _identify_text_columns(self, df: pl.DataFrame) -> list[str]:
        """Identify columns likely containing text."""
        text_cols = []
        for col in df.columns:
            if df[col].dtype in [pl.Utf8, pl.String]:
                # Check if values are long enough to be text
                sample = df[col].drop_nulls().head(10)
                if len(sample) > 0:
                    avg_len = sample.str.len_chars().mean()
                    if avg_len and avg_len > 20:
                        text_cols.append(col)
        return text_cols

    def _calculate_quality_score(self, df: pl.DataFrame, insights: dict) -> float:
        """Calculate overall data quality score."""
        scores = []

        # Completeness score
        null_ratio = df.null_count().sum_horizontal().item() / (len(df) * len(df.columns))
        scores.append(1 - null_ratio)

        # Uniqueness (check for duplicate rows)
        dup_ratio = 1 - (df.n_unique() / len(df))
        scores.append(1 - dup_ratio)

        return sum(scores) / len(scores) if scores else 0.5

    def _format_schema(self, df: pl.DataFrame) -> str:
        """Format DataFrame schema for display."""
        lines = []
        for col, dtype in df.schema.items():
            lines.append(f"  {col}: {dtype}")
        return "\n".join(lines)

    def _generate_statistics(self, df: pl.DataFrame) -> str:
        """Generate statistical summary."""
        lines = [
            f"Rows: {len(df)}",
            f"Columns: {len(df.columns)}",
            f"Memory: {df.estimated_size() / 1024:.2f} KB",
            "",
            "Column Statistics:",
        ]

        for col in df.columns:
            col_data = df[col]
            null_pct = col_data.null_count() / len(df) * 100
            unique = col_data.n_unique()
            lines.append(f"  {col} ({col_data.dtype}): {null_pct:.1f}% null, {unique} unique")

        return "\n".join(lines)

    def _is_safe_code(self, code: str) -> bool:
        """Check if generated code is safe to execute."""
        dangerous_patterns = [
            "import os",
            "import sys",
            "import subprocess",
            "eval(",
            "exec(",
            "__import__",
            "open(",
            "file(",
            "input(",
            "raw_input(",
        ]
        code_lower = code.lower()
        return not any(pattern in code_lower for pattern in dangerous_patterns)
