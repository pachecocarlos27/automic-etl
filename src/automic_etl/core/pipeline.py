"""Pipeline orchestration for Automic ETL."""

from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Generic, TypeVar

import polars as pl
import structlog

from automic_etl.core.config import Settings, get_settings
from automic_etl.core.exceptions import AutomicETLError
from automic_etl.core.utils import utc_now

logger = structlog.get_logger()

T = TypeVar("T")


class PipelineStatus(str, Enum):
    """Pipeline execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class LayerType(str, Enum):
    """Medallion layer types."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


@dataclass
class PipelineMetrics:
    """Metrics collected during pipeline execution."""

    rows_read: int = 0
    rows_written: int = 0
    bytes_read: int = 0
    bytes_written: int = 0
    files_processed: int = 0
    errors_count: int = 0
    llm_tokens_used: int = 0
    start_time: datetime | None = None
    end_time: datetime | None = None

    @property
    def duration_seconds(self) -> float | None:
        """Calculate execution duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    @property
    def rows_per_second(self) -> float | None:
        """Calculate processing rate."""
        duration = self.duration_seconds
        if duration and duration > 0:
            return self.rows_written / duration
        return None


@dataclass
class PipelineContext:
    """Context passed through pipeline stages."""

    pipeline_id: str
    settings: Settings
    metrics: PipelineMetrics
    metadata: dict[str, Any] = field(default_factory=dict)
    watermarks: dict[str, Any] = field(default_factory=dict)
    errors: list[Exception] = field(default_factory=list)

    def log_error(self, error: Exception) -> None:
        """Log an error to the context."""
        self.errors.append(error)
        self.metrics.errors_count += 1

    def update_watermark(self, source: str, value: Any) -> None:
        """Update watermark for a source."""
        self.watermarks[source] = value


class PipelineStage(ABC, Generic[T]):
    """Base class for pipeline stages."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.logger = structlog.get_logger().bind(stage=name)

    @abstractmethod
    async def execute(
        self, data: T | None, context: PipelineContext
    ) -> T | None:
        """Execute the pipeline stage."""
        pass

    async def before_execute(self, context: PipelineContext) -> None:
        """Hook called before execution."""
        self.logger.info("Starting stage execution")

    async def after_execute(self, context: PipelineContext) -> None:
        """Hook called after execution."""
        self.logger.info("Completed stage execution")

    async def on_error(self, error: Exception, context: PipelineContext) -> None:
        """Hook called on error."""
        self.logger.error("Stage execution failed", error=str(error))
        context.log_error(error)


class ExtractStage(PipelineStage[pl.DataFrame]):
    """Stage for extracting data from sources."""

    def __init__(
        self,
        name: str,
        extractor: Callable[..., pl.DataFrame],
        **kwargs: Any,
    ) -> None:
        super().__init__(name)
        self.extractor = extractor
        self.kwargs = kwargs

    async def execute(
        self, data: pl.DataFrame | None, context: PipelineContext
    ) -> pl.DataFrame:
        """Execute the extraction."""
        result = self.extractor(**self.kwargs)
        context.metrics.rows_read += len(result)
        return result


class TransformStage(PipelineStage[pl.DataFrame]):
    """Stage for transforming data."""

    def __init__(
        self,
        name: str,
        transformer: Callable[[pl.DataFrame], pl.DataFrame],
    ) -> None:
        super().__init__(name)
        self.transformer = transformer

    async def execute(
        self, data: pl.DataFrame | None, context: PipelineContext
    ) -> pl.DataFrame | None:
        """Execute the transformation."""
        if data is None:
            return None
        return self.transformer(data)


class LoadStage(PipelineStage[pl.DataFrame]):
    """Stage for loading data to destinations."""

    def __init__(
        self,
        name: str,
        loader: Callable[[pl.DataFrame], int],
        layer: LayerType = LayerType.BRONZE,
    ) -> None:
        super().__init__(name)
        self.loader = loader
        self.layer = layer

    async def execute(
        self, data: pl.DataFrame | None, context: PipelineContext
    ) -> pl.DataFrame | None:
        """Execute the load."""
        if data is None:
            return None
        rows_written = self.loader(data)
        context.metrics.rows_written += rows_written
        return data


class LLMAugmentStage(PipelineStage[pl.DataFrame]):
    """Stage for LLM-based augmentation."""

    def __init__(
        self,
        name: str,
        augmenter: Callable[[pl.DataFrame, PipelineContext], tuple[pl.DataFrame, int]],
    ) -> None:
        super().__init__(name)
        self.augmenter = augmenter

    async def execute(
        self, data: pl.DataFrame | None, context: PipelineContext
    ) -> pl.DataFrame | None:
        """Execute the LLM augmentation."""
        if data is None:
            return None
        result, tokens_used = self.augmenter(data, context)
        context.metrics.llm_tokens_used += tokens_used
        return result


@dataclass
class PipelineResult:
    """Result of a pipeline execution."""

    pipeline_id: str
    status: PipelineStatus
    metrics: PipelineMetrics
    errors: list[Exception]
    metadata: dict[str, Any]

    @property
    def success(self) -> bool:
        """Check if pipeline completed successfully."""
        return self.status == PipelineStatus.COMPLETED and not self.errors


class Pipeline:
    """Main pipeline orchestrator."""

    def __init__(
        self,
        name: str,
        settings: Settings | None = None,
    ) -> None:
        self.name = name
        self.settings = settings or get_settings()
        self.stages: list[PipelineStage[Any]] = []
        self.logger = structlog.get_logger().bind(pipeline=name)

    def add_stage(self, stage: PipelineStage[Any]) -> "Pipeline":
        """Add a stage to the pipeline."""
        self.stages.append(stage)
        return self

    def extract(
        self,
        name: str,
        extractor: Callable[..., pl.DataFrame],
        **kwargs: Any,
    ) -> "Pipeline":
        """Add an extraction stage."""
        return self.add_stage(ExtractStage(name, extractor, **kwargs))

    def transform(
        self,
        name: str,
        transformer: Callable[[pl.DataFrame], pl.DataFrame],
    ) -> "Pipeline":
        """Add a transformation stage."""
        return self.add_stage(TransformStage(name, transformer))

    def load(
        self,
        name: str,
        loader: Callable[[pl.DataFrame], int],
        layer: LayerType = LayerType.BRONZE,
    ) -> "Pipeline":
        """Add a load stage."""
        return self.add_stage(LoadStage(name, loader, layer))

    def augment(
        self,
        name: str,
        augmenter: Callable[[pl.DataFrame, PipelineContext], tuple[pl.DataFrame, int]],
    ) -> "Pipeline":
        """Add an LLM augmentation stage."""
        return self.add_stage(LLMAugmentStage(name, augmenter))

    async def run(self, metadata: dict[str, Any] | None = None) -> PipelineResult:
        """Execute the pipeline."""
        pipeline_id = str(uuid.uuid4())
        metrics = PipelineMetrics(start_time=utc_now())
        context = PipelineContext(
            pipeline_id=pipeline_id,
            settings=self.settings,
            metrics=metrics,
            metadata=metadata or {},
        )

        self.logger.info(
            "Starting pipeline execution",
            pipeline_id=pipeline_id,
            stages=len(self.stages),
        )

        status = PipelineStatus.RUNNING
        data: Any = None

        try:
            for stage in self.stages:
                await stage.before_execute(context)
                try:
                    data = await stage.execute(data, context)
                    await stage.after_execute(context)
                except Exception as e:
                    await stage.on_error(e, context)
                    raise

            status = PipelineStatus.COMPLETED
            self.logger.info(
                "Pipeline completed successfully",
                pipeline_id=pipeline_id,
                rows_written=metrics.rows_written,
            )

        except Exception as e:
            status = PipelineStatus.FAILED
            self.logger.error(
                "Pipeline failed",
                pipeline_id=pipeline_id,
                error=str(e),
            )

        finally:
            metrics.end_time = utc_now()

        return PipelineResult(
            pipeline_id=pipeline_id,
            status=status,
            metrics=metrics,
            errors=context.errors,
            metadata=context.metadata,
        )


class PipelineBuilder:
    """Fluent builder for creating pipelines."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._settings: Settings | None = None
        self._source_config: dict[str, Any] = {}
        self._transformations: list[Callable[[pl.DataFrame], pl.DataFrame]] = []
        self._target_layer: LayerType = LayerType.BRONZE
        self._llm_augmentations: list[str] = []

    def with_settings(self, settings: Settings) -> "PipelineBuilder":
        """Set custom settings."""
        self._settings = settings
        return self

    def from_source(
        self,
        source_type: str,
        **config: Any,
    ) -> "PipelineBuilder":
        """Configure the data source."""
        self._source_config = {"type": source_type, **config}
        return self

    def with_transformation(
        self,
        transformer: Callable[[pl.DataFrame], pl.DataFrame],
    ) -> "PipelineBuilder":
        """Add a transformation."""
        self._transformations.append(transformer)
        return self

    def to_layer(self, layer: LayerType) -> "PipelineBuilder":
        """Set the target medallion layer."""
        self._target_layer = layer
        return self

    def with_llm_augmentation(self, *augmentations: str) -> "PipelineBuilder":
        """Enable LLM augmentations."""
        self._llm_augmentations.extend(augmentations)
        return self

    def build(self) -> Pipeline:
        """Build the pipeline."""
        pipeline = Pipeline(self.name, self._settings)
        # Pipeline stages will be added based on configuration
        # This will be implemented with the connectors and medallion modules
        return pipeline


# Convenience functions for common operations
def create_pipeline(name: str, settings: Settings | None = None) -> Pipeline:
    """Create a new pipeline instance."""
    return Pipeline(name, settings)


def build_pipeline(name: str) -> PipelineBuilder:
    """Create a new pipeline builder."""
    return PipelineBuilder(name)
