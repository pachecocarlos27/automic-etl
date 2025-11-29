"""LLM-based schema inference and generation."""

from __future__ import annotations

from typing import Any

import polars as pl
import structlog

from automic_etl.core.config import Settings
from automic_etl.llm.client import LLMClient
from automic_etl.llm.prompts.templates import SCHEMA_INFERENCE_PROMPT

logger = structlog.get_logger()


class SchemaGenerator:
    """
    Generate and infer schemas using LLM.

    Capabilities:
    - Infer schema from unstructured text
    - Suggest optimal data types
    - Identify primary keys and relationships
    - Generate column descriptions
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = LLMClient(settings)
        self.logger = logger.bind(component="schema_generator")

    def infer_schema(
        self,
        df: pl.DataFrame,
        sample_size: int = 10,
        context: str | None = None,
    ) -> dict[str, Any]:
        """
        Infer an optimized schema from a DataFrame.

        Args:
            df: Input DataFrame
            sample_size: Number of sample rows to analyze
            context: Additional context about the data

        Returns:
            Schema definition with types, descriptions, and recommendations
        """
        # Get sample data
        sample_df = df.head(sample_size)
        sample_data = sample_df.to_dicts()

        # Format prompt
        prompt = SCHEMA_INFERENCE_PROMPT.format(
            sample_data=str(sample_data)[:4000],  # Limit size
            columns=list(df.columns),
        )

        if context:
            prompt += f"\n\nAdditional Context: {context}"

        # Get LLM response
        result, tokens = self.client.complete_json(
            prompt=prompt,
            system_prompt="You are a data engineering expert specializing in schema design.",
        )

        self.logger.info(
            "Generated schema",
            columns=len(result.get("columns", [])),
            tokens_used=tokens,
        )

        return result

    def infer_from_text(
        self,
        text: str,
        entity_hints: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Infer a schema from unstructured text.

        Args:
            text: Unstructured text to analyze
            entity_hints: Hints about expected entities

        Returns:
            Schema definition
        """
        prompt = f"""Analyze this unstructured text and infer a schema for structured data extraction.

Text:
{text[:3000]}

{"Expected entities: " + ", ".join(entity_hints) if entity_hints else ""}

Provide a JSON schema that could capture the structured information in this text.
Include:
- Column names
- Data types
- Whether fields are required
- Example values
"""

        result, _ = self.client.complete_json(
            prompt=prompt,
            system_prompt="You are an expert at converting unstructured data to structured schemas.",
        )

        return result

    def suggest_partition_strategy(
        self,
        df: pl.DataFrame,
        use_case: str = "general",
    ) -> dict[str, Any]:
        """
        Suggest optimal partitioning strategy.

        Args:
            df: Input DataFrame
            use_case: How the data will be queried

        Returns:
            Partitioning recommendations
        """
        # Analyze column characteristics
        column_info = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            null_count = df[col].null_count()
            unique_count = df[col].n_unique()

            column_info.append({
                "name": col,
                "dtype": dtype,
                "null_pct": null_count / len(df) * 100,
                "unique_count": unique_count,
                "cardinality": "high" if unique_count > len(df) * 0.5 else "low",
            })

        prompt = f"""Analyze these columns and recommend a partitioning strategy for Apache Iceberg.

Columns:
{column_info}

Use Case: {use_case}
Row Count: {len(df)}

Consider:
1. Query patterns (time-based, category-based)
2. Cardinality (avoid too many small partitions)
3. Data growth patterns
4. File sizes (aim for 128MB-512MB files)

Respond with JSON:
{{
    "partition_columns": ["col1"],
    "partition_transforms": {{"col1": "month"}},
    "reasoning": "why this strategy",
    "sort_columns": ["col1", "col2"],
    "estimated_partition_count": 100
}}"""

        result, _ = self.client.complete_json(
            prompt=prompt,
            system_prompt="You are an Apache Iceberg optimization expert.",
        )

        return result

    def generate_table_ddl(
        self,
        schema: dict[str, Any],
        table_name: str,
        catalog: str = "lakehouse",
        namespace: str = "default",
    ) -> str:
        """
        Generate Iceberg DDL from schema.

        Args:
            schema: Schema definition
            table_name: Target table name
            catalog: Iceberg catalog name
            namespace: Namespace/database

        Returns:
            CREATE TABLE statement
        """
        prompt = f"""Generate an Apache Iceberg CREATE TABLE statement.

Schema:
{schema}

Table: {catalog}.{namespace}.{table_name}

Include:
1. All columns with appropriate Iceberg types
2. Partitioning if suggested in schema
3. Table properties for optimization
4. Comments/descriptions

Output only the SQL statement, no explanations."""

        response = self.client.complete(
            prompt=prompt,
            system_prompt="You are an Apache Iceberg SQL expert.",
        )

        return response.content.strip()

    def suggest_column_renames(
        self,
        columns: list[str],
        convention: str = "snake_case",
    ) -> dict[str, str]:
        """
        Suggest better column names.

        Args:
            columns: Current column names
            convention: Naming convention to follow

        Returns:
            Mapping of old names to suggested new names
        """
        prompt = f"""Suggest better column names following {convention} convention.

Current columns:
{columns}

Requirements:
1. Use {convention} format
2. Make names descriptive but concise
3. Avoid abbreviations unless common (id, url, etc.)
4. Use consistent prefixes/suffixes

Respond with JSON mapping old names to new names:
{{"old_name": "new_name"}}"""

        result, _ = self.client.complete_json(
            prompt=prompt,
            system_prompt="You are a data modeling expert.",
        )

        return result
