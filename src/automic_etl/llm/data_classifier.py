"""LLM-based data classification."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import polars as pl
import structlog

from automic_etl.core.config import Settings
from automic_etl.llm.client import LLMClient
from automic_etl.llm.prompts.templates import DATA_CLASSIFICATION_PROMPT

logger = structlog.get_logger()


@dataclass
class ClassificationResult:
    """Result of data classification."""

    primary_class: str
    secondary_classes: list[str]
    confidence: float
    sensitivity_level: str
    contains_pii: bool
    pii_types: list[str]
    reasoning: str
    metadata: dict[str, Any] = field(default_factory=dict)


class DataClassifier:
    """
    Classify data using LLM.

    Capabilities:
    - Content classification
    - Sensitivity detection
    - PII identification
    - Data categorization
    """

    # Default classification categories
    DEFAULT_CATEGORIES = [
        "financial",
        "healthcare",
        "legal",
        "technical",
        "marketing",
        "hr",
        "operations",
        "customer",
        "product",
        "research",
    ]

    # Sensitivity levels
    SENSITIVITY_LEVELS = ["public", "internal", "confidential", "restricted"]

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = LLMClient(settings)
        self.logger = logger.bind(component="data_classifier")

    def classify(
        self,
        data: pl.DataFrame | str | dict,
        categories: list[str] | None = None,
        sample_size: int = 10,
    ) -> ClassificationResult:
        """
        Classify data into categories.

        Args:
            data: Data to classify (DataFrame, text, or dict)
            categories: Classification categories
            sample_size: Number of samples to analyze

        Returns:
            Classification result
        """
        categories = categories or self.DEFAULT_CATEGORIES

        # Prepare sample data
        if isinstance(data, pl.DataFrame):
            sample_data = data.head(sample_size).to_dicts()
        elif isinstance(data, dict):
            sample_data = data
        else:
            sample_data = {"text": str(data)[:3000]}

        prompt = DATA_CLASSIFICATION_PROMPT.format(
            categories=", ".join(categories),
            sample_data=str(sample_data)[:4000],
        )

        result, tokens = self.client.complete_json(
            prompt=prompt,
            system_prompt="You are a data classification expert with expertise in data governance.",
        )

        self.logger.info(
            "Classified data",
            primary=result.get("primary_classification"),
            tokens_used=tokens,
        )

        return ClassificationResult(
            primary_class=result.get("primary_classification", "unknown"),
            secondary_classes=result.get("secondary_classifications", []),
            confidence=result.get("confidence", 0),
            sensitivity_level=result.get("sensitivity_level", "internal"),
            contains_pii=result.get("contains_pii", False),
            pii_types=result.get("pii_types", []),
            reasoning=result.get("reasoning", ""),
            metadata=result,
        )

    def detect_pii(
        self,
        data: pl.DataFrame,
        columns: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Detect PII in data.

        Args:
            data: DataFrame to analyze
            columns: Specific columns to check

        Returns:
            PII detection results by column
        """
        columns_to_check = columns or data.columns
        sample = data.head(10)

        results = {}

        for col in columns_to_check:
            if col not in sample.columns:
                continue

            sample_values = sample[col].to_list()
            non_null_values = [v for v in sample_values if v is not None]

            if not non_null_values:
                continue

            prompt = f"""Analyze these values for PII (Personally Identifiable Information):

Column: {col}
Sample values: {non_null_values[:5]}

Identify if this column likely contains:
- Names
- Email addresses
- Phone numbers
- Social Security Numbers
- Credit card numbers
- Addresses
- Dates of birth
- Medical information
- Financial information
- Other PII

Respond with JSON:
{{
    "contains_pii": true/false,
    "pii_type": "type if applicable",
    "confidence": 0.9,
    "reasoning": "why"
}}"""

            result, _ = self.client.complete_json(
                prompt=prompt,
                system_prompt="You are a data privacy expert.",
            )

            results[col] = result

        # Summarize findings
        pii_columns = [col for col, r in results.items() if r.get("contains_pii")]

        return {
            "columns_analyzed": columns_to_check,
            "pii_columns": pii_columns,
            "details": results,
            "overall_pii_risk": "high" if len(pii_columns) > 3 else "medium" if pii_columns else "low",
        }

    def classify_columns(
        self,
        df: pl.DataFrame,
        sample_size: int = 10,
    ) -> dict[str, dict[str, Any]]:
        """
        Classify each column in a DataFrame.

        Args:
            df: DataFrame to analyze
            sample_size: Number of samples per column

        Returns:
            Classification results by column
        """
        results = {}

        for col in df.columns:
            sample_values = df[col].head(sample_size).to_list()
            dtype = str(df[col].dtype)

            prompt = f"""Classify this data column:

Column Name: {col}
Data Type: {dtype}
Sample Values: {sample_values}

Provide:
1. Semantic type (e.g., email, date, currency, name, id, etc.)
2. Business category
3. Potential uses
4. Data quality observations

Respond with JSON:
{{
    "semantic_type": "type",
    "business_category": "category",
    "potential_uses": ["use1", "use2"],
    "quality_notes": "observations",
    "is_identifier": true/false,
    "is_sensitive": true/false
}}"""

            result, _ = self.client.complete_json(
                prompt=prompt,
                system_prompt="You are a data modeling expert.",
            )

            results[col] = result

        return results

    def suggest_access_controls(
        self,
        classification: ClassificationResult,
    ) -> dict[str, Any]:
        """
        Suggest access controls based on classification.

        Args:
            classification: Classification result

        Returns:
            Access control recommendations
        """
        prompt = f"""Based on this data classification, suggest access controls:

Classification: {classification.primary_class}
Sensitivity: {classification.sensitivity_level}
Contains PII: {classification.contains_pii}
PII Types: {classification.pii_types}

Suggest:
1. Who should have access (roles)
2. Access restrictions
3. Audit requirements
4. Retention policies
5. Encryption requirements

Respond with JSON:
{{
    "allowed_roles": ["role1", "role2"],
    "denied_roles": ["role3"],
    "access_restrictions": ["restriction1"],
    "requires_audit": true/false,
    "retention_days": 365,
    "encryption_required": true/false,
    "encryption_type": "at_rest/in_transit/both",
    "additional_controls": []
}}"""

        result, _ = self.client.complete_json(
            prompt=prompt,
            system_prompt="You are a data governance and security expert.",
        )

        return result

    def batch_classify(
        self,
        dataframes: dict[str, pl.DataFrame],
        categories: list[str] | None = None,
    ) -> dict[str, ClassificationResult]:
        """
        Classify multiple datasets.

        Args:
            dataframes: Dict of name -> DataFrame
            categories: Classification categories

        Returns:
            Classification results by dataset name
        """
        results = {}
        for name, df in dataframes.items():
            results[name] = self.classify(df, categories)
        return results
