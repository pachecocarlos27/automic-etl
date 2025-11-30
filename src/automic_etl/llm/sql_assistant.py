"""LLM-powered SQL Assistant for natural language queries."""

from __future__ import annotations

import uuid
import re
import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable
from enum import Enum

import structlog

from automic_etl.core.config import Settings
from automic_etl.llm.client import LLMClient

logger = structlog.get_logger()


class QueryIntent(Enum):
    """Types of user query intents."""
    SELECT = "select"
    AGGREGATE = "aggregate"
    JOIN = "join"
    FILTER = "filter"
    COMPARISON = "comparison"
    TREND = "trend"
    ANOMALY = "anomaly"
    EXPLAIN = "explain"
    SUGGESTION = "suggestion"
    CLARIFICATION = "clarification"


@dataclass
class TableSchema:
    """Schema information for a table."""
    name: str
    columns: dict[str, str]  # column_name -> type
    description: str = ""
    tier: str = "silver"  # bronze, silver, gold
    sample_values: dict[str, list[Any]] = field(default_factory=dict)
    relationships: list[dict[str, str]] = field(default_factory=list)
    row_count: int | None = None


@dataclass
class ConversationMessage:
    """A message in the conversation history."""
    role: str  # user, assistant, system
    content: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    query_result: dict | None = None
    sql_generated: str | None = None


@dataclass
class ConversationContext:
    """Maintains conversation context for multi-turn queries."""
    conversation_id: str
    user_id: str
    company_id: str
    messages: list[ConversationMessage] = field(default_factory=list)
    referenced_tables: set[str] = field(default_factory=set)
    last_sql: str | None = None
    last_result_summary: str | None = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    def add_message(self, role: str, content: str, **kwargs) -> None:
        """Add a message to the conversation."""
        self.messages.append(ConversationMessage(
            role=role,
            content=content,
            **kwargs
        ))
        self.updated_at = datetime.utcnow()

    def get_recent_context(self, max_messages: int = 10) -> list[dict]:
        """Get recent messages for context."""
        recent = self.messages[-max_messages:]
        return [
            {"role": m.role, "content": m.content}
            for m in recent
        ]

    def to_context_string(self) -> str:
        """Convert conversation to context string for LLM."""
        context_parts = []
        for msg in self.messages[-5:]:  # Last 5 messages
            if msg.sql_generated:
                context_parts.append(f"Previous SQL: {msg.sql_generated}")
            if msg.query_result:
                context_parts.append(f"Result summary: {msg.query_result.get('summary', '')}")
        return "\n".join(context_parts)


@dataclass
class SQLGenerationResult:
    """Result of SQL generation from natural language."""
    query_id: str
    original_query: str
    sql: str
    explanation: str
    intent: QueryIntent
    confidence: float
    tables_used: list[str]
    columns_used: list[str]
    warnings: list[str] = field(default_factory=list)
    suggestions: list[str] = field(default_factory=list)
    is_safe: bool = True
    requires_confirmation: bool = False
    estimated_rows: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryExecutionResult:
    """Result of query execution."""
    query_id: str
    sql: str
    columns: list[str]
    data: list[list[Any]]
    row_count: int
    execution_time_ms: float
    from_cache: bool = False
    natural_language_summary: str | None = None
    visualization_suggestion: str | None = None
    follow_up_questions: list[str] = field(default_factory=list)


# Prompt templates
NL_TO_SQL_PROMPT = """You are an expert SQL analyst helping users query a data lakehouse.

## Available Tables
{table_schemas}

## Conversation Context
{conversation_context}

## User Query
{user_query}

## Instructions
1. Convert the natural language query to SQL
2. Use only the tables and columns provided
3. Apply appropriate filters, aggregations, and sorting
4. For trend queries, include time-based grouping
5. Add LIMIT if not specified (default 100)
6. Use table aliases for readability
7. Include comments explaining key parts

## Response Format (JSON)
{{
    "sql": "SELECT ...",
    "explanation": "Plain English explanation of what the query does",
    "intent": "select|aggregate|join|filter|comparison|trend|anomaly",
    "confidence": 0.95,
    "tables_used": ["table1", "table2"],
    "columns_used": ["col1", "col2"],
    "warnings": ["Any issues or caveats"],
    "suggestions": ["Follow-up queries user might want"],
    "estimated_complexity": "low|medium|high",
    "requires_confirmation": false
}}"""

QUERY_REFINEMENT_PROMPT = """The user wants to modify or refine a previous query.

## Previous SQL
{previous_sql}

## Previous Result Summary
{result_summary}

## User's Refinement Request
{refinement_request}

## Available Tables
{table_schemas}

## Instructions
1. Understand what aspect the user wants to change
2. Modify the SQL accordingly
3. Preserve parts that don't need changing

Respond with the same JSON format as before."""

RESULT_EXPLANATION_PROMPT = """Explain the query results in natural language.

## SQL Query
{sql}

## Query Explanation
{explanation}

## Results
Columns: {columns}
Row Count: {row_count}
Sample Data:
{sample_data}

## Instructions
1. Summarize what the data shows
2. Highlight key insights or patterns
3. Note any anomalies or interesting findings
4. Suggest 2-3 follow-up questions

Respond with JSON:
{{
    "summary": "Brief summary of results",
    "key_insights": ["insight1", "insight2"],
    "anomalies": ["Any unusual findings"],
    "follow_up_questions": ["question1", "question2"],
    "visualization_type": "bar|line|pie|table|scatter"
}}"""

SECURITY_CHECK_PROMPT = """Analyze this SQL query for security issues.

## SQL Query
{sql}

## User Context
- Company ID: {company_id}
- User Role: {user_role}
- Allowed Tiers: {allowed_tiers}

## Check for:
1. SQL injection attempts
2. Unauthorized table access
3. Data exfiltration patterns
4. Excessive data retrieval
5. Schema modification attempts

Respond with JSON:
{{
    "is_safe": true/false,
    "risk_level": "none|low|medium|high|critical",
    "issues": ["list of security issues found"],
    "sanitized_sql": "cleaned SQL if needed",
    "blocked_reason": "reason if blocked"
}}"""


class SQLAssistant:
    """
    LLM-powered SQL assistant for natural language to SQL conversion
    with conversation context and multi-turn support.
    """

    def __init__(
        self,
        settings: Settings,
        schema_provider: Callable[[], dict[str, TableSchema]] | None = None,
    ) -> None:
        self.settings = settings
        self.client = LLMClient(settings)
        self.logger = logger.bind(component="sql_assistant")
        self._schema_provider = schema_provider
        self._table_schemas: dict[str, TableSchema] = {}
        self._conversations: dict[str, ConversationContext] = {}
        self._query_cache: dict[str, SQLGenerationResult] = {}

    def register_table(self, schema: TableSchema) -> None:
        """Register a table schema."""
        self._table_schemas[schema.name] = schema

    def register_tables(self, schemas: list[TableSchema]) -> None:
        """Register multiple table schemas."""
        for schema in schemas:
            self.register_table(schema)

    def load_schemas_from_provider(self) -> None:
        """Load schemas from the schema provider."""
        if self._schema_provider:
            self._table_schemas = self._schema_provider()

    def get_or_create_conversation(
        self,
        user_id: str,
        company_id: str,
        conversation_id: str | None = None,
    ) -> ConversationContext:
        """Get existing or create new conversation context."""
        if conversation_id and conversation_id in self._conversations:
            return self._conversations[conversation_id]

        conv_id = conversation_id or str(uuid.uuid4())
        context = ConversationContext(
            conversation_id=conv_id,
            user_id=user_id,
            company_id=company_id,
        )
        self._conversations[conv_id] = context
        return context

    def _format_schemas_for_prompt(
        self,
        tables: list[str] | None = None,
        company_id: str | None = None,
    ) -> str:
        """Format table schemas for the LLM prompt."""
        schemas_to_use = tables or list(self._table_schemas.keys())
        parts = []

        for table_name in schemas_to_use:
            if table_name not in self._table_schemas:
                continue

            schema = self._table_schemas[table_name]
            columns_str = "\n".join(
                f"    - {col}: {dtype}"
                for col, dtype in schema.columns.items()
            )

            sample_values_str = ""
            if schema.sample_values:
                samples = []
                for col, values in schema.sample_values.items():
                    samples.append(f"    {col}: {values[:3]}")
                sample_values_str = f"\n  Sample Values:\n" + "\n".join(samples)

            relationships_str = ""
            if schema.relationships:
                rels = []
                for rel in schema.relationships:
                    rels.append(f"    - {rel['column']} -> {rel['references']}")
                relationships_str = f"\n  Relationships:\n" + "\n".join(rels)

            parts.append(f"""
Table: {schema.name} ({schema.tier} tier)
Description: {schema.description}
Columns:
{columns_str}{sample_values_str}{relationships_str}
Row Count: {schema.row_count or 'Unknown'}
""")

        return "\n".join(parts)

    def natural_language_to_sql(
        self,
        query: str,
        user_id: str,
        company_id: str,
        conversation_id: str | None = None,
        tables: list[str] | None = None,
        allowed_tiers: list[str] | None = None,
    ) -> SQLGenerationResult:
        """
        Convert natural language query to SQL.

        Args:
            query: Natural language query
            user_id: User making the request
            company_id: Company context
            conversation_id: Optional conversation ID for context
            tables: Specific tables to use
            allowed_tiers: Data tiers user can access

        Returns:
            SQL generation result
        """
        query_id = str(uuid.uuid4())

        # Get or create conversation
        context = self.get_or_create_conversation(user_id, company_id, conversation_id)

        # Check cache
        cache_key = self._get_cache_key(query, company_id, tables)
        if cache_key in self._query_cache:
            cached = self._query_cache[cache_key]
            self.logger.info("Using cached SQL", query_id=query_id)
            return cached

        # Build prompt
        schema_str = self._format_schemas_for_prompt(tables, company_id)
        context_str = context.to_context_string() if context.messages else "No previous context"

        prompt = NL_TO_SQL_PROMPT.format(
            table_schemas=schema_str,
            conversation_context=context_str,
            user_query=query,
        )

        # Generate SQL
        try:
            result, tokens = self.client.complete_json(
                prompt=prompt,
                system_prompt="You are an expert SQL analyst. Generate precise, efficient SQL queries.",
            )
        except Exception as e:
            self.logger.error("SQL generation failed", error=str(e))
            raise

        # Parse intent
        intent_str = result.get("intent", "select")
        try:
            intent = QueryIntent(intent_str)
        except ValueError:
            intent = QueryIntent.SELECT

        # Build result
        gen_result = SQLGenerationResult(
            query_id=query_id,
            original_query=query,
            sql=result.get("sql", ""),
            explanation=result.get("explanation", ""),
            intent=intent,
            confidence=result.get("confidence", 0.0),
            tables_used=result.get("tables_used", []),
            columns_used=result.get("columns_used", []),
            warnings=result.get("warnings", []),
            suggestions=result.get("suggestions", []),
            requires_confirmation=result.get("requires_confirmation", False),
            metadata={"tokens_used": tokens},
        )

        # Security check
        gen_result = self._security_check(gen_result, company_id, allowed_tiers or ["bronze", "silver", "gold"])

        # Update conversation
        context.add_message("user", query)
        context.add_message("assistant", gen_result.explanation, sql_generated=gen_result.sql)
        context.last_sql = gen_result.sql
        context.referenced_tables.update(gen_result.tables_used)

        # Cache result
        self._query_cache[cache_key] = gen_result

        self.logger.info(
            "Generated SQL",
            query_id=query_id,
            intent=intent.value,
            confidence=gen_result.confidence,
            tables=gen_result.tables_used,
        )

        return gen_result

    def refine_query(
        self,
        refinement: str,
        conversation_id: str,
        user_id: str,
        company_id: str,
    ) -> SQLGenerationResult:
        """
        Refine a previous query based on user feedback.

        Args:
            refinement: User's refinement request
            conversation_id: Conversation to refine
            user_id: User making the request
            company_id: Company context

        Returns:
            Updated SQL generation result
        """
        if conversation_id not in self._conversations:
            raise ValueError(f"Conversation {conversation_id} not found")

        context = self._conversations[conversation_id]
        if not context.last_sql:
            raise ValueError("No previous query to refine")

        query_id = str(uuid.uuid4())

        prompt = QUERY_REFINEMENT_PROMPT.format(
            previous_sql=context.last_sql,
            result_summary=context.last_result_summary or "No results available",
            refinement_request=refinement,
            table_schemas=self._format_schemas_for_prompt(list(context.referenced_tables)),
        )

        result, tokens = self.client.complete_json(
            prompt=prompt,
            system_prompt="You are an expert SQL analyst. Modify queries based on user feedback.",
        )

        intent_str = result.get("intent", "select")
        try:
            intent = QueryIntent(intent_str)
        except ValueError:
            intent = QueryIntent.SELECT

        gen_result = SQLGenerationResult(
            query_id=query_id,
            original_query=refinement,
            sql=result.get("sql", ""),
            explanation=result.get("explanation", ""),
            intent=intent,
            confidence=result.get("confidence", 0.0),
            tables_used=result.get("tables_used", []),
            columns_used=result.get("columns_used", []),
            warnings=result.get("warnings", []),
            suggestions=result.get("suggestions", []),
            metadata={"tokens_used": tokens, "is_refinement": True},
        )

        # Update conversation
        context.add_message("user", f"Refine: {refinement}")
        context.add_message("assistant", gen_result.explanation, sql_generated=gen_result.sql)
        context.last_sql = gen_result.sql

        return gen_result

    def explain_results(
        self,
        sql: str,
        columns: list[str],
        data: list[list[Any]],
        row_count: int,
    ) -> dict[str, Any]:
        """
        Generate natural language explanation of query results.

        Args:
            sql: Executed SQL query
            columns: Result column names
            data: Result data rows
            row_count: Total row count

        Returns:
            Explanation with insights and follow-up suggestions
        """
        # Format sample data
        sample_rows = data[:5]
        sample_str = "\n".join(
            str(dict(zip(columns, row)))
            for row in sample_rows
        )

        prompt = RESULT_EXPLANATION_PROMPT.format(
            sql=sql,
            explanation="",
            columns=columns,
            row_count=row_count,
            sample_data=sample_str,
        )

        result, _ = self.client.complete_json(
            prompt=prompt,
            system_prompt="You are a data analyst explaining query results to business users.",
        )

        return result

    def _security_check(
        self,
        result: SQLGenerationResult,
        company_id: str,
        allowed_tiers: list[str],
    ) -> SQLGenerationResult:
        """Check generated SQL for security issues."""
        sql_lower = result.sql.lower()

        # Check for dangerous operations
        dangerous_patterns = [
            r'\bdrop\b', r'\btruncate\b', r'\bdelete\b',
            r'\balter\b', r'\bcreate\b', r'\binsert\b',
            r'\bupdate\b', r'\bgrant\b', r'\brevoke\b',
        ]

        for pattern in dangerous_patterns:
            if re.search(pattern, sql_lower):
                result.is_safe = False
                result.warnings.append(f"Potentially dangerous operation detected: {pattern}")
                result.requires_confirmation = True

        # Check table access
        for table in result.tables_used:
            if table in self._table_schemas:
                schema = self._table_schemas[table]
                if schema.tier not in allowed_tiers:
                    result.is_safe = False
                    result.warnings.append(f"Access denied to {schema.tier} tier table: {table}")

        # Check for SQL injection patterns
        injection_patterns = [
            r';\s*--', r"'\s*or\s*'", r"'\s*;\s*",
            r'union\s+select', r'exec\s*\(',
        ]

        for pattern in injection_patterns:
            if re.search(pattern, sql_lower):
                result.is_safe = False
                result.warnings.append("Potential SQL injection pattern detected")

        return result

    def _get_cache_key(
        self,
        query: str,
        company_id: str,
        tables: list[str] | None,
    ) -> str:
        """Generate cache key for query."""
        key_parts = [query.lower().strip(), company_id]
        if tables:
            key_parts.extend(sorted(tables))
        key_str = "|".join(key_parts)
        return hashlib.md5(key_str.encode()).hexdigest()

    def get_suggested_queries(
        self,
        tables: list[str] | None = None,
        user_role: str = "analyst",
    ) -> list[dict[str, str]]:
        """
        Get suggested queries based on available tables.

        Args:
            tables: Specific tables to focus on
            user_role: User's role for relevance

        Returns:
            List of suggested queries with descriptions
        """
        tables_to_use = tables or list(self._table_schemas.keys())

        if not tables_to_use:
            return []

        schema_str = self._format_schemas_for_prompt(tables_to_use)

        prompt = f"""Based on these tables, suggest 5 useful analytical queries:

{schema_str}

For a {user_role}, suggest queries that would provide business value.

Respond with JSON:
{{
    "suggestions": [
        {{
            "natural_language": "question in plain English",
            "description": "what insights this provides",
            "complexity": "simple|moderate|complex"
        }}
    ]
}}"""

        result, _ = self.client.complete_json(
            prompt=prompt,
            system_prompt="You are a business analyst suggesting useful data queries.",
        )

        return result.get("suggestions", [])

    def autocomplete(
        self,
        partial_query: str,
        tables: list[str] | None = None,
    ) -> list[str]:
        """
        Provide autocomplete suggestions for partial queries.

        Args:
            partial_query: Partial natural language query
            tables: Available tables

        Returns:
            List of completion suggestions
        """
        if len(partial_query) < 3:
            return []

        tables_to_use = tables or list(self._table_schemas.keys())
        table_names = ", ".join(tables_to_use[:5])

        prompt = f"""Complete this partial query with 3 suggestions.

Partial query: "{partial_query}"
Available tables: {table_names}

Respond with JSON:
{{
    "completions": ["completion1", "completion2", "completion3"]
}}"""

        result, _ = self.client.complete_json(
            prompt=prompt,
            system_prompt="You are an autocomplete system for data queries.",
        )

        return result.get("completions", [])

    def clear_conversation(self, conversation_id: str) -> bool:
        """Clear a conversation context."""
        if conversation_id in self._conversations:
            del self._conversations[conversation_id]
            return True
        return False

    def get_conversation_history(
        self,
        conversation_id: str,
    ) -> list[dict[str, Any]] | None:
        """Get conversation history."""
        if conversation_id not in self._conversations:
            return None

        context = self._conversations[conversation_id]
        return [
            {
                "role": msg.role,
                "content": msg.content,
                "timestamp": msg.timestamp.isoformat(),
                "sql": msg.sql_generated,
            }
            for msg in context.messages
        ]
