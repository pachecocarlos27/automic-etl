"""LLM-based natural language to SQL query builder."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import polars as pl
import structlog

from automic_etl.core.config import Settings
from automic_etl.llm.client import LLMClient
from automic_etl.llm.prompts.templates import QUERY_BUILDER_PROMPT

logger = structlog.get_logger()


@dataclass
class QueryResult:
    """Result of query generation."""

    sql: str
    explanation: str
    tables_used: list[str]
    complexity: str
    warnings: list[str]
    is_valid: bool
    metadata: dict[str, Any] = field(default_factory=dict)


class QueryBuilder:
    """
    Convert natural language to SQL queries using LLM.

    Capabilities:
    - Natural language to SQL
    - Query optimization suggestions
    - Query explanation
    - Schema-aware generation
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = LLMClient(settings)
        self.logger = logger.bind(component="query_builder")
        self._table_schemas: dict[str, dict[str, str]] = {}

    def register_table(
        self,
        table_name: str,
        schema: dict[str, str],
        description: str | None = None,
    ) -> None:
        """
        Register a table schema for query generation.

        Args:
            table_name: Fully qualified table name
            schema: Column name -> type mapping
            description: Optional table description
        """
        self._table_schemas[table_name] = {
            "columns": schema,
            "description": description or "",
        }

    def register_dataframe(
        self,
        table_name: str,
        df: pl.DataFrame,
        description: str | None = None,
    ) -> None:
        """
        Register a DataFrame's schema.

        Args:
            table_name: Table name to use
            df: DataFrame to get schema from
            description: Optional description
        """
        schema = {col: str(dtype) for col, dtype in df.schema.items()}
        self.register_table(table_name, schema, description)

    def build_query(
        self,
        natural_query: str,
        tables: list[str] | None = None,
    ) -> QueryResult:
        """
        Convert natural language to SQL.

        Args:
            natural_query: Natural language query
            tables: Specific tables to use (uses all registered if None)

        Returns:
            Generated SQL query with metadata
        """
        # Use specified tables or all registered
        tables_to_use = tables or list(self._table_schemas.keys())

        if not tables_to_use:
            raise ValueError("No tables registered. Use register_table() first.")

        # Format table schemas for prompt
        schema_info = []
        for table in tables_to_use:
            if table in self._table_schemas:
                info = self._table_schemas[table]
                schema_info.append(f"""
Table: {table}
Description: {info.get('description', 'N/A')}
Columns:
{self._format_columns(info['columns'])}
""")

        prompt = QUERY_BUILDER_PROMPT.format(
            table_schemas="\n".join(schema_info),
            natural_query=natural_query,
        )

        result, tokens = self.client.complete_json(
            prompt=prompt,
            system_prompt="You are a SQL expert specializing in Apache Iceberg and analytical queries.",
        )

        self.logger.info(
            "Generated query",
            query_length=len(result.get("sql", "")),
            tokens_used=tokens,
        )

        return QueryResult(
            sql=result.get("sql", ""),
            explanation=result.get("explanation", ""),
            tables_used=result.get("tables_used", []),
            complexity=result.get("estimated_complexity", "unknown"),
            warnings=result.get("warnings", []),
            is_valid=bool(result.get("sql")),
            metadata=result,
        )

    def _format_columns(self, columns: dict[str, str]) -> str:
        """Format columns for display."""
        lines = []
        for name, dtype in columns.items():
            lines.append(f"  - {name}: {dtype}")
        return "\n".join(lines)

    def explain_query(self, sql: str) -> str:
        """
        Get a natural language explanation of a SQL query.

        Args:
            sql: SQL query to explain

        Returns:
            Natural language explanation
        """
        prompt = f"""Explain this SQL query in simple terms:

```sql
{sql}
```

Provide:
1. What the query does overall
2. Step-by-step breakdown
3. What data it returns
4. Any potential performance considerations"""

        response = self.client.complete(
            prompt=prompt,
            system_prompt="You are a SQL teacher explaining queries to beginners.",
        )

        return response.content

    def optimize_query(
        self,
        sql: str,
        context: str | None = None,
    ) -> dict[str, Any]:
        """
        Suggest optimizations for a SQL query.

        Args:
            sql: SQL query to optimize
            context: Additional context (data size, usage patterns)

        Returns:
            Optimization suggestions
        """
        prompt = f"""Analyze and optimize this SQL query for Apache Iceberg:

```sql
{sql}
```

{"Context: " + context if context else ""}

Consider:
1. Partition pruning opportunities
2. Column pruning
3. Predicate pushdown
4. Join optimization
5. Aggregation efficiency

Respond with JSON:
{{
    "optimized_sql": "improved query",
    "changes_made": ["change1", "change2"],
    "expected_improvement": "description",
    "warnings": ["any caveats"]
}}"""

        result, _ = self.client.complete_json(
            prompt=prompt,
            system_prompt="You are a query optimization expert for Apache Iceberg.",
        )

        return result

    def suggest_indexes(
        self,
        query: str,
        table_name: str,
    ) -> list[dict[str, Any]]:
        """
        Suggest indexes/sort orders for a query pattern.

        Args:
            query: Sample query
            table_name: Table being queried

        Returns:
            Index/sort order suggestions
        """
        prompt = f"""Based on this query pattern, suggest optimal sort orders for Apache Iceberg:

Query:
```sql
{query}
```

Table: {table_name}

For Iceberg, suggest:
1. Sort order columns
2. Partition columns
3. Z-order columns for multi-dimensional queries

Respond with JSON:
{{
    "sort_columns": ["col1", "col2"],
    "partition_columns": ["col1"],
    "z_order_columns": ["col1", "col2"],
    "reasoning": "why these choices"
}}"""

        result, _ = self.client.complete_json(
            prompt=prompt,
            system_prompt="You are an Apache Iceberg performance expert.",
        )

        return [result]

    def validate_query(self, sql: str) -> dict[str, Any]:
        """
        Validate a SQL query for common issues.

        Args:
            sql: SQL query to validate

        Returns:
            Validation results
        """
        prompt = f"""Validate this SQL query for issues:

```sql
{sql}
```

Check for:
1. Syntax errors
2. Missing table/column references
3. Type mismatches
4. Performance anti-patterns
5. Security issues (SQL injection risks)
6. Iceberg-specific issues

Respond with JSON:
{{
    "is_valid": true/false,
    "syntax_issues": [],
    "semantic_issues": [],
    "performance_issues": [],
    "security_issues": [],
    "suggestions": []
}}"""

        result, _ = self.client.complete_json(
            prompt=prompt,
            system_prompt="You are a SQL validation expert.",
        )

        return result

    def generate_test_queries(
        self,
        table_name: str,
        num_queries: int = 5,
    ) -> list[str]:
        """
        Generate sample queries for testing.

        Args:
            table_name: Table to generate queries for
            num_queries: Number of queries to generate

        Returns:
            List of sample queries
        """
        if table_name not in self._table_schemas:
            raise ValueError(f"Table {table_name} not registered")

        schema = self._table_schemas[table_name]

        prompt = f"""Generate {num_queries} diverse SQL queries for this table:

Table: {table_name}
Columns:
{self._format_columns(schema['columns'])}

Generate queries that cover:
1. Simple selects with filters
2. Aggregations
3. Window functions
4. Complex predicates
5. Time-based queries (if applicable)

Respond with JSON:
{{
    "queries": [
        {{"sql": "query", "description": "what it does"}}
    ]
}}"""

        result, _ = self.client.complete_json(
            prompt=prompt,
            system_prompt="You are a SQL query generator.",
        )

        return [q["sql"] for q in result.get("queries", [])]
