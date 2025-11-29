"""LLM-based entity extraction from unstructured data."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl
import structlog

from automic_etl.core.config import Settings
from automic_etl.llm.client import LLMClient
from automic_etl.llm.prompts.templates import ENTITY_EXTRACTION_PROMPT

logger = structlog.get_logger()


@dataclass
class ExtractedEntity:
    """Represents an extracted entity."""

    entity_type: str
    value: str
    confidence: float
    context: str
    start_pos: int | None = None
    end_pos: int | None = None
    metadata: dict[str, Any] | None = None


class EntityExtractor:
    """
    Extract structured entities from unstructured text using LLM.

    Capabilities:
    - Named entity recognition (NER)
    - Custom entity extraction
    - Relationship extraction
    - Key-value pair extraction
    """

    # Default entity types
    DEFAULT_ENTITY_TYPES = [
        "PERSON",
        "ORGANIZATION",
        "LOCATION",
        "DATE",
        "TIME",
        "MONEY",
        "PERCENTAGE",
        "EMAIL",
        "PHONE",
        "URL",
        "ADDRESS",
        "PRODUCT",
    ]

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = LLMClient(settings)
        self.logger = logger.bind(component="entity_extractor")

    def extract(
        self,
        text: str,
        entity_types: list[str] | None = None,
        min_confidence: float = 0.7,
    ) -> list[ExtractedEntity]:
        """
        Extract entities from text.

        Args:
            text: Text to analyze
            entity_types: Types of entities to extract
            min_confidence: Minimum confidence threshold

        Returns:
            List of extracted entities
        """
        entity_types = entity_types or self.DEFAULT_ENTITY_TYPES

        # Handle long text by chunking
        if len(text) > 3000:
            return self._extract_from_chunks(text, entity_types, min_confidence)

        prompt = ENTITY_EXTRACTION_PROMPT.format(
            entity_types=", ".join(entity_types),
            text=text,
        )

        result, tokens = self.client.complete_json(
            prompt=prompt,
            system_prompt="You are an expert at extracting structured entities from unstructured text.",
        )

        entities = []
        for entity_data in result.get("entities", []):
            if entity_data.get("confidence", 0) >= min_confidence:
                entities.append(ExtractedEntity(
                    entity_type=entity_data.get("type", "UNKNOWN"),
                    value=entity_data.get("value", ""),
                    confidence=entity_data.get("confidence", 0),
                    context=entity_data.get("context", ""),
                    start_pos=entity_data.get("start_pos"),
                    end_pos=entity_data.get("end_pos"),
                ))

        self.logger.info(
            "Extracted entities",
            count=len(entities),
            tokens_used=tokens,
        )

        return entities

    def _extract_from_chunks(
        self,
        text: str,
        entity_types: list[str],
        min_confidence: float,
    ) -> list[ExtractedEntity]:
        """Extract entities from long text by chunking."""
        chunk_size = 2500
        overlap = 200
        all_entities = []
        seen_values: set[tuple[str, str]] = set()

        start = 0
        while start < len(text):
            end = min(start + chunk_size, len(text))
            chunk = text[start:end]

            chunk_entities = self.extract(
                chunk,
                entity_types=entity_types,
                min_confidence=min_confidence,
            )

            # Deduplicate
            for entity in chunk_entities:
                key = (entity.entity_type, entity.value)
                if key not in seen_values:
                    seen_values.add(key)
                    # Adjust positions for chunk offset
                    if entity.start_pos is not None:
                        entity.start_pos += start
                    if entity.end_pos is not None:
                        entity.end_pos += start
                    all_entities.append(entity)

            start = end - overlap

        return all_entities

    def extract_to_dataframe(
        self,
        text: str,
        entity_types: list[str] | None = None,
    ) -> pl.DataFrame:
        """
        Extract entities and return as a DataFrame.

        Args:
            text: Text to analyze
            entity_types: Types of entities to extract

        Returns:
            DataFrame with extracted entities
        """
        entities = self.extract(text, entity_types)

        if not entities:
            return pl.DataFrame({
                "entity_type": [],
                "value": [],
                "confidence": [],
                "context": [],
            })

        return pl.DataFrame({
            "entity_type": [e.entity_type for e in entities],
            "value": [e.value for e in entities],
            "confidence": [e.confidence for e in entities],
            "context": [e.context for e in entities],
            "start_pos": [e.start_pos for e in entities],
            "end_pos": [e.end_pos for e in entities],
        })

    def extract_key_values(
        self,
        text: str,
        expected_keys: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Extract key-value pairs from text.

        Args:
            text: Text containing key-value information
            expected_keys: Keys to look for

        Returns:
            Dictionary of extracted key-value pairs
        """
        prompt = f"""Extract key-value pairs from this text.

Text:
{text[:3000]}

{"Expected keys: " + ", ".join(expected_keys) if expected_keys else "Extract all identifiable key-value pairs."}

Respond with JSON containing the extracted key-value pairs.
Use null for values that cannot be determined."""

        result, _ = self.client.complete_json(
            prompt=prompt,
            system_prompt="You are an expert at extracting structured data from unstructured text.",
        )

        return result

    def extract_relationships(
        self,
        text: str,
        relationship_types: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Extract relationships between entities.

        Args:
            text: Text to analyze
            relationship_types: Types of relationships to look for

        Returns:
            List of relationship tuples
        """
        default_relationships = [
            "works_for",
            "located_in",
            "owns",
            "reports_to",
            "created_by",
            "part_of",
        ]
        relationship_types = relationship_types or default_relationships

        prompt = f"""Extract relationships between entities in this text.

Text:
{text[:3000]}

Relationship types to identify:
{", ".join(relationship_types)}

For each relationship, identify:
- Subject entity
- Relationship type
- Object entity
- Confidence score

Respond with JSON:
{{
    "relationships": [
        {{
            "subject": "entity1",
            "subject_type": "PERSON",
            "relationship": "works_for",
            "object": "entity2",
            "object_type": "ORGANIZATION",
            "confidence": 0.9
        }}
    ]
}}"""

        result, _ = self.client.complete_json(
            prompt=prompt,
            system_prompt="You are an expert at extracting relationships from text.",
        )

        return result.get("relationships", [])

    def batch_extract(
        self,
        texts: list[str],
        entity_types: list[str] | None = None,
    ) -> list[list[ExtractedEntity]]:
        """
        Extract entities from multiple texts.

        Args:
            texts: List of texts to process
            entity_types: Types of entities to extract

        Returns:
            List of entity lists for each text
        """
        results = []
        for text in texts:
            entities = self.extract(text, entity_types)
            results.append(entities)
        return results

    def extract_from_dataframe(
        self,
        df: pl.DataFrame,
        text_column: str,
        entity_types: list[str] | None = None,
        output_format: str = "wide",
    ) -> pl.DataFrame:
        """
        Extract entities from a DataFrame column.

        Args:
            df: Input DataFrame
            text_column: Column containing text
            entity_types: Types of entities to extract
            output_format: 'wide' (one column per entity type) or 'long'

        Returns:
            DataFrame with extracted entities
        """
        all_entities = []

        for idx, row in enumerate(df.iter_rows(named=True)):
            text = row.get(text_column, "")
            if text:
                entities = self.extract(str(text), entity_types)
                for entity in entities:
                    all_entities.append({
                        "_row_idx": idx,
                        "entity_type": entity.entity_type,
                        "value": entity.value,
                        "confidence": entity.confidence,
                    })

        if not all_entities:
            return df

        entities_df = pl.DataFrame(all_entities)

        if output_format == "wide":
            # Pivot to get one column per entity type
            pivoted = entities_df.pivot(
                values="value",
                index="_row_idx",
                on="entity_type",
                aggregate_function="first",
            )
            # Join back to original
            df = df.with_row_index("_row_idx")
            result = df.join(pivoted, on="_row_idx", how="left")
            return result.drop("_row_idx")
        else:
            # Return long format with entity details
            return entities_df
