"""Text redaction service for de-identifying sensitive information."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable
from enum import Enum

import polars as pl
import structlog

logger = structlog.get_logger()


class RedactionStrategy(str, Enum):
    """Strategy for how to redact matched text."""

    TAG = "tag"
    MASK = "mask"
    HASH = "hash"
    REMOVE = "remove"
    CUSTOM = "custom"


@dataclass
class EntityPattern:
    """
    Defines a pattern for entity detection and redaction.

    Attributes:
        name: Name of the entity type (e.g., "CITY", "DATE", "SSN")
        patterns: List of regex patterns or exact strings to match
        tag: Replacement tag (e.g., "[CITY]")
        case_insensitive: Whether matching should ignore case
        word_boundary: Whether to match only whole words
        priority: Higher priority patterns are applied first
        custom_replacer: Optional custom function for replacement
    """

    name: str
    patterns: list[str] = field(default_factory=list)
    tag: str | None = None
    case_insensitive: bool = True
    word_boundary: bool = True
    priority: int = 0
    custom_replacer: Callable[[str], str] | None = None

    def __post_init__(self) -> None:
        if self.tag is None:
            self.tag = f"[{self.name.upper()}]"

    def get_compiled_patterns(self) -> list[re.Pattern]:
        """Compile all patterns with appropriate flags."""
        compiled = []
        flags = re.IGNORECASE if self.case_insensitive else 0

        for pattern in self.patterns:
            if self.word_boundary and not pattern.startswith(r"\b"):
                escaped = re.escape(pattern) if not self._is_regex(pattern) else pattern
                pattern = rf"\b{escaped}\b"
            elif not self._is_regex(pattern):
                pattern = re.escape(pattern)

            try:
                compiled.append(re.compile(pattern, flags))
            except re.error as e:
                logger.warning(f"Invalid regex pattern '{pattern}': {e}")

        return compiled

    def _is_regex(self, pattern: str) -> bool:
        """Check if pattern appears to be a regex."""
        regex_chars = r"[](){}*+?|^$\\"
        return any(c in pattern for c in regex_chars)


@dataclass
class RedactionMatch:
    """Represents a single redaction match."""

    original: str
    replacement: str
    entity_type: str
    start: int
    end: int
    context: str = ""


@dataclass
class RedactionResult:
    """Result of a redaction operation."""

    original_text: str
    redacted_text: str
    matches: list[RedactionMatch] = field(default_factory=list)
    entity_counts: dict[str, int] = field(default_factory=dict)
    total_redactions: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "original_text": self.original_text,
            "redacted_text": self.redacted_text,
            "matches": [
                {
                    "original": m.original,
                    "replacement": m.replacement,
                    "entity_type": m.entity_type,
                    "start": m.start,
                    "end": m.end,
                }
                for m in self.matches
            ],
            "entity_counts": self.entity_counts,
            "total_redactions": self.total_redactions,
        }


@dataclass
class RedactionConfig:
    """Configuration for the redaction service."""

    patterns: list[EntityPattern] = field(default_factory=list)
    strategy: RedactionStrategy = RedactionStrategy.TAG
    mask_char: str = "*"
    preserve_length: bool = False
    log_replacements: bool = True
    include_context: bool = True
    context_chars: int = 20

    @classmethod
    def with_common_patterns(cls) -> "RedactionConfig":
        """Create config with common PII patterns."""
        return cls(
            patterns=[
                EntityPattern(
                    name="EMAIL",
                    patterns=[r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"],
                    word_boundary=False,
                    priority=10,
                ),
                EntityPattern(
                    name="PHONE",
                    patterns=[
                        r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}",
                        r"\+\d{1,3}[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}",
                    ],
                    word_boundary=False,
                    priority=10,
                ),
                EntityPattern(
                    name="SSN",
                    patterns=[r"\d{3}[-\s]?\d{2}[-\s]?\d{4}"],
                    word_boundary=True,
                    priority=15,
                ),
                EntityPattern(
                    name="CREDIT_CARD",
                    patterns=[r"\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}"],
                    word_boundary=True,
                    priority=15,
                ),
                EntityPattern(
                    name="IP_ADDRESS",
                    patterns=[r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}"],
                    word_boundary=True,
                    priority=5,
                ),
            ]
        )

    @classmethod
    def with_temporal_patterns(cls) -> "RedactionConfig":
        """Create config with temporal patterns (days, months, dates)."""
        days = [
            "Monday", "Tuesday", "Wednesday", "Thursday",
            "Friday", "Saturday", "Sunday",
        ]
        months = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December",
        ]

        return cls(
            patterns=[
                EntityPattern(
                    name="DAY",
                    patterns=days,
                    priority=5,
                ),
                EntityPattern(
                    name="MONTH",
                    patterns=months,
                    priority=5,
                ),
                EntityPattern(
                    name="DATE",
                    patterns=[
                        r"\d{1,2}/\d{1,2}/\d{2,4}",
                        r"\d{4}-\d{2}-\d{2}",
                        r"\d{1,2}-\d{1,2}-\d{2,4}",
                    ],
                    word_boundary=False,
                    priority=8,
                ),
            ]
        )

    @classmethod
    def with_location_patterns(cls) -> "RedactionConfig":
        """Create config with location patterns (cities, states)."""
        us_states = [
            "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
            "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
            "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
            "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
            "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
            "New Hampshire", "New Jersey", "New Mexico", "New York",
            "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
            "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
            "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
            "West Virginia", "Wisconsin", "Wyoming",
        ]

        major_cities = [
            "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
            "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
            "Austin", "Jacksonville", "Fort Worth", "Columbus", "Indianapolis",
            "Charlotte", "San Francisco", "Seattle", "Denver", "Boston",
            "Nashville", "Detroit", "Portland", "Memphis", "Atlanta",
            "Miami", "Las Vegas", "Baltimore", "Milwaukee", "Minneapolis",
        ]

        return cls(
            patterns=[
                EntityPattern(
                    name="STATE",
                    patterns=us_states,
                    priority=5,
                ),
                EntityPattern(
                    name="CITY",
                    patterns=major_cities,
                    priority=5,
                ),
            ]
        )

    @classmethod
    def with_custom_terms(
        cls,
        entity_name: str,
        terms: list[str],
        tag: str | None = None,
    ) -> "RedactionConfig":
        """Create config with custom terms to redact."""
        return cls(
            patterns=[
                EntityPattern(
                    name=entity_name,
                    patterns=terms,
                    tag=tag,
                    priority=5,
                )
            ]
        )

    def add_pattern(self, pattern: EntityPattern) -> "RedactionConfig":
        """Add a pattern to the configuration."""
        self.patterns.append(pattern)
        return self

    def merge(self, other: "RedactionConfig") -> "RedactionConfig":
        """Merge another config's patterns into this one."""
        self.patterns.extend(other.patterns)
        return self


class RedactionService:
    """
    Service for redacting sensitive information from text.

    Supports:
    - Configurable entity patterns (regex or exact match)
    - Multiple redaction strategies (tag, mask, hash, remove)
    - Case-insensitive and word-boundary matching
    - Detailed logging of replacements
    - Batch processing for DataFrames
    """

    def __init__(self, config: RedactionConfig) -> None:
        self.config = config
        self.logger = logger.bind(component="redaction_service")
        self._compiled_patterns: dict[str, list[re.Pattern]] = {}
        self._compile_patterns()

    def _compile_patterns(self) -> None:
        """Pre-compile all patterns for efficiency."""
        sorted_patterns = sorted(
            self.config.patterns,
            key=lambda p: p.priority,
            reverse=True,
        )

        for pattern in sorted_patterns:
            self._compiled_patterns[pattern.name] = pattern.get_compiled_patterns()

    def redact(self, text: str) -> RedactionResult:
        """
        Redact sensitive information from text.

        Args:
            text: Input text to redact

        Returns:
            RedactionResult with redacted text and match details
        """
        if not text:
            return RedactionResult(
                original_text=text,
                redacted_text=text,
                total_redactions=0,
            )

        redacted = text
        all_matches: list[RedactionMatch] = []
        entity_counts: dict[str, int] = {}

        sorted_patterns = sorted(
            self.config.patterns,
            key=lambda p: p.priority,
            reverse=True,
        )

        for pattern in sorted_patterns:
            compiled = self._compiled_patterns.get(pattern.name, [])

            for regex in compiled:
                for match in regex.finditer(redacted):
                    original = match.group()
                    replacement = self._get_replacement(original, pattern)

                    context = ""
                    if self.config.include_context:
                        start = max(0, match.start() - self.config.context_chars)
                        end = min(len(redacted), match.end() + self.config.context_chars)
                        context = redacted[start:end]

                    all_matches.append(RedactionMatch(
                        original=original,
                        replacement=replacement,
                        entity_type=pattern.name,
                        start=match.start(),
                        end=match.end(),
                        context=context,
                    ))

                    entity_counts[pattern.name] = entity_counts.get(pattern.name, 0) + 1

                redacted = regex.sub(
                    lambda m: self._get_replacement(m.group(), pattern),
                    redacted,
                )

        result = RedactionResult(
            original_text=text,
            redacted_text=redacted,
            matches=all_matches,
            entity_counts=entity_counts,
            total_redactions=len(all_matches),
        )

        if self.config.log_replacements and all_matches:
            self.logger.info(
                "Redaction completed",
                total_redactions=len(all_matches),
                entity_counts=entity_counts,
            )

        return result

    def _get_replacement(self, original: str, pattern: EntityPattern) -> str:
        """Get the replacement string based on strategy."""
        if pattern.custom_replacer:
            return pattern.custom_replacer(original)

        if self.config.strategy == RedactionStrategy.TAG:
            return pattern.tag or f"[{pattern.name}]"

        elif self.config.strategy == RedactionStrategy.MASK:
            if self.config.preserve_length:
                return self.config.mask_char * len(original)
            return self.config.mask_char * 4

        elif self.config.strategy == RedactionStrategy.HASH:
            import hashlib
            return hashlib.md5(original.encode()).hexdigest()[:8]

        elif self.config.strategy == RedactionStrategy.REMOVE:
            return ""

        return pattern.tag or f"[{pattern.name}]"

    def redact_batch(self, texts: list[str]) -> list[RedactionResult]:
        """Redact a batch of texts."""
        return [self.redact(text) for text in texts]

    def redact_dataframe(
        self,
        df: pl.DataFrame,
        columns: list[str],
        output_suffix: str = "_redacted",
    ) -> tuple[pl.DataFrame, dict[str, Any]]:
        """
        Redact specified columns in a DataFrame.

        Args:
            df: Input DataFrame
            columns: Columns to redact
            output_suffix: Suffix for redacted column names

        Returns:
            Tuple of (redacted DataFrame, summary statistics)
        """
        result_df = df.clone()
        total_stats = {
            "columns_processed": len(columns),
            "rows_processed": len(df),
            "total_redactions": 0,
            "entity_counts": {},
        }

        for col in columns:
            if col not in df.columns:
                self.logger.warning(f"Column '{col}' not found in DataFrame")
                continue

            redacted_values = []
            for value in df[col].to_list():
                if value is None:
                    redacted_values.append(None)
                else:
                    result = self.redact(str(value))
                    redacted_values.append(result.redacted_text)
                    total_stats["total_redactions"] += result.total_redactions

                    for entity, count in result.entity_counts.items():
                        total_stats["entity_counts"][entity] = (
                            total_stats["entity_counts"].get(entity, 0) + count
                        )

            new_col_name = f"{col}{output_suffix}"
            result_df = result_df.with_columns(
                pl.Series(new_col_name, redacted_values)
            )

        return result_df, total_stats

    def get_entity_counts(self, text: str) -> dict[str, int]:
        """Count entities in text without redacting."""
        result = self.redact(text)
        return result.entity_counts

    def has_sensitive_content(self, text: str) -> bool:
        """Check if text contains any sensitive content."""
        result = self.redact(text)
        return result.total_redactions > 0

    def extract_entities(self, text: str) -> dict[str, list[str]]:
        """Extract all matched entities from text."""
        result = self.redact(text)
        entities: dict[str, list[str]] = {}

        for match in result.matches:
            if match.entity_type not in entities:
                entities[match.entity_type] = []
            if match.original not in entities[match.entity_type]:
                entities[match.entity_type].append(match.original)

        return entities

    @classmethod
    def create_combined(cls, *configs: RedactionConfig) -> "RedactionService":
        """Create a service with combined patterns from multiple configs."""
        combined = RedactionConfig()
        for config in configs:
            combined.merge(config)
        return cls(combined)
