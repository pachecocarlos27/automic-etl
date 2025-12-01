"""LLM augmentation module for intelligent data processing."""

from automic_etl.llm.client import (
    LLMClient,
    LLMResponse,
    RetryConfig,
    RateLimitConfig,
    RateLimiter,
    estimate_tokens,
    with_retry,
)
from automic_etl.llm.schema_generator import SchemaGenerator
from automic_etl.llm.entity_extractor import EntityExtractor
from automic_etl.llm.data_classifier import DataClassifier
from automic_etl.llm.query_builder import QueryBuilder
from automic_etl.llm.augmented_etl import AugmentedETL
from automic_etl.llm.sql_assistant import (
    SQLAssistant,
    TableSchema,
    QueryIntent,
    SQLGenerationResult,
    QueryExecutionResult,
    ConversationContext,
    ConversationMessage,
)

__all__ = [
    # Client
    "LLMClient",
    "LLMResponse",
    "RetryConfig",
    "RateLimitConfig",
    "RateLimiter",
    "estimate_tokens",
    "with_retry",
    # Generators
    "SchemaGenerator",
    "EntityExtractor",
    "DataClassifier",
    "QueryBuilder",
    "AugmentedETL",
    # SQL Assistant
    "SQLAssistant",
    "TableSchema",
    "QueryIntent",
    "SQLGenerationResult",
    "QueryExecutionResult",
    "ConversationContext",
    "ConversationMessage",
]
