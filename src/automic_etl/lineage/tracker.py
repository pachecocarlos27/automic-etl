"""Data lineage tracker for recording data transformations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable
from datetime import datetime
from enum import Enum
import uuid
import json
from pathlib import Path

import polars as pl
import structlog

logger = structlog.get_logger()


class OperationType(Enum):
    """Types of data operations."""
    READ = "read"
    WRITE = "write"
    TRANSFORM = "transform"
    MERGE = "merge"
    FILTER = "filter"
    JOIN = "join"
    AGGREGATE = "aggregate"
    DEDUPLICATE = "deduplicate"
    VALIDATE = "validate"
    ENRICH = "enrich"


@dataclass
class DataAsset:
    """Represents a data asset (table, file, etc.)."""
    name: str
    asset_type: str  # table, file, api, etc.
    location: str | None = None
    schema: dict[str, str] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": self.asset_type,
            "location": self.location,
            "schema": self.schema,
            "metadata": self.metadata,
        }


@dataclass
class LineageEvent:
    """A single lineage event recording a data operation."""
    event_id: str
    timestamp: datetime
    operation: OperationType
    source_assets: list[DataAsset]
    target_assets: list[DataAsset]
    transformation: str | None = None
    pipeline_id: str | None = None
    pipeline_name: str | None = None
    job_id: str | None = None
    user: str | None = None
    row_count_in: int | None = None
    row_count_out: int | None = None
    duration_ms: int | None = None
    status: str = "success"
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "timestamp": self.timestamp.isoformat(),
            "operation": self.operation.value,
            "sources": [s.to_dict() for s in self.source_assets],
            "targets": [t.to_dict() for t in self.target_assets],
            "transformation": self.transformation,
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "job_id": self.job_id,
            "user": self.user,
            "row_count_in": self.row_count_in,
            "row_count_out": self.row_count_out,
            "duration_ms": self.duration_ms,
            "status": self.status,
            "error": self.error,
            "metadata": self.metadata,
        }


class LineageTracker:
    """
    Track data lineage across ETL operations.

    Features:
    - Record data operations and transformations
    - Track column-level lineage
    - Store lineage in local or remote backends
    - Query lineage history
    - Export lineage for visualization
    """

    def __init__(
        self,
        storage_path: str | None = None,
        pipeline_id: str | None = None,
        pipeline_name: str | None = None,
    ) -> None:
        """
        Initialize lineage tracker.

        Args:
            storage_path: Path to store lineage data (JSON files)
            pipeline_id: Current pipeline ID
            pipeline_name: Current pipeline name
        """
        self.storage_path = Path(storage_path) if storage_path else None
        self.pipeline_id = pipeline_id or str(uuid.uuid4())
        self.pipeline_name = pipeline_name
        self.events: list[LineageEvent] = []
        self.job_id = str(uuid.uuid4())
        self.logger = logger.bind(component="lineage_tracker", pipeline=pipeline_name)

        if self.storage_path:
            self.storage_path.mkdir(parents=True, exist_ok=True)

    def start_job(self, job_name: str | None = None) -> str:
        """Start a new job and return job ID."""
        self.job_id = str(uuid.uuid4())
        self.logger.info("Lineage job started", job_id=self.job_id, job_name=job_name)
        return self.job_id

    def record_read(
        self,
        source: DataAsset | str,
        row_count: int | None = None,
        **metadata: Any,
    ) -> LineageEvent:
        """Record a data read operation."""
        if isinstance(source, str):
            source = DataAsset(name=source, asset_type="unknown")

        event = LineageEvent(
            event_id=str(uuid.uuid4()),
            timestamp=datetime.utcnow(),
            operation=OperationType.READ,
            source_assets=[source],
            target_assets=[],
            pipeline_id=self.pipeline_id,
            pipeline_name=self.pipeline_name,
            job_id=self.job_id,
            row_count_in=row_count,
            row_count_out=row_count,
            metadata=metadata,
        )

        self._record_event(event)
        return event

    def record_write(
        self,
        target: DataAsset | str,
        sources: list[DataAsset | str] | None = None,
        row_count: int | None = None,
        **metadata: Any,
    ) -> LineageEvent:
        """Record a data write operation."""
        if isinstance(target, str):
            target = DataAsset(name=target, asset_type="unknown")

        source_assets = []
        if sources:
            for s in sources:
                if isinstance(s, str):
                    source_assets.append(DataAsset(name=s, asset_type="unknown"))
                else:
                    source_assets.append(s)

        event = LineageEvent(
            event_id=str(uuid.uuid4()),
            timestamp=datetime.utcnow(),
            operation=OperationType.WRITE,
            source_assets=source_assets,
            target_assets=[target],
            pipeline_id=self.pipeline_id,
            pipeline_name=self.pipeline_name,
            job_id=self.job_id,
            row_count_out=row_count,
            metadata=metadata,
        )

        self._record_event(event)
        return event

    def record_transform(
        self,
        source: DataAsset | str,
        target: DataAsset | str,
        transformation: str,
        row_count_in: int | None = None,
        row_count_out: int | None = None,
        duration_ms: int | None = None,
        **metadata: Any,
    ) -> LineageEvent:
        """Record a data transformation."""
        if isinstance(source, str):
            source = DataAsset(name=source, asset_type="unknown")
        if isinstance(target, str):
            target = DataAsset(name=target, asset_type="unknown")

        event = LineageEvent(
            event_id=str(uuid.uuid4()),
            timestamp=datetime.utcnow(),
            operation=OperationType.TRANSFORM,
            source_assets=[source],
            target_assets=[target],
            transformation=transformation,
            pipeline_id=self.pipeline_id,
            pipeline_name=self.pipeline_name,
            job_id=self.job_id,
            row_count_in=row_count_in,
            row_count_out=row_count_out,
            duration_ms=duration_ms,
            metadata=metadata,
        )

        self._record_event(event)
        return event

    def record_join(
        self,
        left_source: DataAsset | str,
        right_source: DataAsset | str,
        target: DataAsset | str,
        join_keys: list[str],
        join_type: str = "inner",
        **metadata: Any,
    ) -> LineageEvent:
        """Record a join operation."""
        sources = []
        for s in [left_source, right_source]:
            if isinstance(s, str):
                sources.append(DataAsset(name=s, asset_type="unknown"))
            else:
                sources.append(s)

        if isinstance(target, str):
            target = DataAsset(name=target, asset_type="unknown")

        event = LineageEvent(
            event_id=str(uuid.uuid4()),
            timestamp=datetime.utcnow(),
            operation=OperationType.JOIN,
            source_assets=sources,
            target_assets=[target],
            transformation=f"{join_type} join on {join_keys}",
            pipeline_id=self.pipeline_id,
            pipeline_name=self.pipeline_name,
            job_id=self.job_id,
            metadata={"join_keys": join_keys, "join_type": join_type, **metadata},
        )

        self._record_event(event)
        return event

    def record_aggregate(
        self,
        source: DataAsset | str,
        target: DataAsset | str,
        group_by: list[str],
        aggregations: dict[str, str],
        **metadata: Any,
    ) -> LineageEvent:
        """Record an aggregation operation."""
        if isinstance(source, str):
            source = DataAsset(name=source, asset_type="unknown")
        if isinstance(target, str):
            target = DataAsset(name=target, asset_type="unknown")

        event = LineageEvent(
            event_id=str(uuid.uuid4()),
            timestamp=datetime.utcnow(),
            operation=OperationType.AGGREGATE,
            source_assets=[source],
            target_assets=[target],
            transformation=f"group by {group_by}, agg: {aggregations}",
            pipeline_id=self.pipeline_id,
            pipeline_name=self.pipeline_name,
            job_id=self.job_id,
            metadata={"group_by": group_by, "aggregations": aggregations, **metadata},
        )

        self._record_event(event)
        return event

    def record_merge(
        self,
        source: DataAsset | str,
        target: DataAsset | str,
        merge_keys: list[str],
        rows_inserted: int = 0,
        rows_updated: int = 0,
        rows_deleted: int = 0,
        **metadata: Any,
    ) -> LineageEvent:
        """Record a merge/upsert operation."""
        if isinstance(source, str):
            source = DataAsset(name=source, asset_type="unknown")
        if isinstance(target, str):
            target = DataAsset(name=target, asset_type="unknown")

        event = LineageEvent(
            event_id=str(uuid.uuid4()),
            timestamp=datetime.utcnow(),
            operation=OperationType.MERGE,
            source_assets=[source],
            target_assets=[target],
            transformation=f"merge on {merge_keys}",
            pipeline_id=self.pipeline_id,
            pipeline_name=self.pipeline_name,
            job_id=self.job_id,
            metadata={
                "merge_keys": merge_keys,
                "rows_inserted": rows_inserted,
                "rows_updated": rows_updated,
                "rows_deleted": rows_deleted,
                **metadata,
            },
        )

        self._record_event(event)
        return event

    def record_error(
        self,
        operation: OperationType,
        source: DataAsset | str | None,
        error: str,
        **metadata: Any,
    ) -> LineageEvent:
        """Record an error event."""
        source_assets = []
        if source:
            if isinstance(source, str):
                source_assets.append(DataAsset(name=source, asset_type="unknown"))
            else:
                source_assets.append(source)

        event = LineageEvent(
            event_id=str(uuid.uuid4()),
            timestamp=datetime.utcnow(),
            operation=operation,
            source_assets=source_assets,
            target_assets=[],
            pipeline_id=self.pipeline_id,
            pipeline_name=self.pipeline_name,
            job_id=self.job_id,
            status="error",
            error=error,
            metadata=metadata,
        )

        self._record_event(event)
        return event

    def _record_event(self, event: LineageEvent) -> None:
        """Record an event and optionally persist."""
        self.events.append(event)
        self.logger.debug(
            "Lineage event recorded",
            event_id=event.event_id,
            operation=event.operation.value,
        )

        if self.storage_path:
            self._persist_event(event)

    def _persist_event(self, event: LineageEvent) -> None:
        """Persist event to storage."""
        file_path = self.storage_path / f"{event.timestamp.strftime('%Y%m%d')}_{self.job_id}.jsonl"

        with open(file_path, "a") as f:
            f.write(json.dumps(event.to_dict()) + "\n")

    def get_events(
        self,
        job_id: str | None = None,
        operation: OperationType | None = None,
        since: datetime | None = None,
    ) -> list[LineageEvent]:
        """Query events with optional filters."""
        results = self.events

        if job_id:
            results = [e for e in results if e.job_id == job_id]
        if operation:
            results = [e for e in results if e.operation == operation]
        if since:
            results = [e for e in results if e.timestamp >= since]

        return results

    def get_upstream(self, asset_name: str) -> list[DataAsset]:
        """Get all upstream dependencies for an asset."""
        upstream = set()

        def find_upstream(name: str, visited: set) -> None:
            if name in visited:
                return
            visited.add(name)

            for event in self.events:
                for target in event.target_assets:
                    if target.name == name:
                        for source in event.source_assets:
                            upstream.add(source.name)
                            find_upstream(source.name, visited)

        find_upstream(asset_name, set())
        return [DataAsset(name=n, asset_type="unknown") for n in upstream]

    def get_downstream(self, asset_name: str) -> list[DataAsset]:
        """Get all downstream dependents for an asset."""
        downstream = set()

        def find_downstream(name: str, visited: set) -> None:
            if name in visited:
                return
            visited.add(name)

            for event in self.events:
                for source in event.source_assets:
                    if source.name == name:
                        for target in event.target_assets:
                            downstream.add(target.name)
                            find_downstream(target.name, visited)

        find_downstream(asset_name, set())
        return [DataAsset(name=n, asset_type="unknown") for n in downstream]

    def to_dataframe(self) -> pl.DataFrame:
        """Convert all events to a DataFrame."""
        rows = []
        for event in self.events:
            rows.append({
                "event_id": event.event_id,
                "timestamp": event.timestamp,
                "operation": event.operation.value,
                "sources": [s.name for s in event.source_assets],
                "targets": [t.name for t in event.target_assets],
                "transformation": event.transformation,
                "pipeline": event.pipeline_name,
                "job_id": event.job_id,
                "row_count_in": event.row_count_in,
                "row_count_out": event.row_count_out,
                "status": event.status,
            })
        return pl.DataFrame(rows) if rows else pl.DataFrame()

    def export_json(self, path: str) -> None:
        """Export all events to JSON file."""
        with open(path, "w") as f:
            json.dump([e.to_dict() for e in self.events], f, indent=2)

    def clear(self) -> None:
        """Clear all recorded events."""
        self.events = []


# Context manager for tracking
class LineageContext:
    """Context manager for automatic lineage tracking."""

    def __init__(self, tracker: LineageTracker, job_name: str | None = None) -> None:
        self.tracker = tracker
        self.job_name = job_name
        self.start_time: datetime | None = None

    def __enter__(self) -> LineageTracker:
        self.tracker.start_job(self.job_name)
        self.start_time = datetime.utcnow()
        return self.tracker

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type:
            self.tracker.record_error(
                operation=OperationType.TRANSFORM,
                source=None,
                error=str(exc_val),
            )
