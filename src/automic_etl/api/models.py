"""Pydantic models for API request/response schemas."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal
from pydantic import BaseModel, Field


# ========================
# Enums
# ========================

class PipelineStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class DataTier(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class ConnectorType(str, Enum):
    DATABASE = "database"
    FILE = "file"
    API = "api"
    STREAMING = "streaming"
    CLOUD_STORAGE = "cloud_storage"


class JobStatus(str, Enum):
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"


# ========================
# Base Models
# ========================

class BaseResponse(BaseModel):
    """Base response model."""
    success: bool = True
    message: str | None = None


class PaginatedResponse(BaseModel):
    """Paginated response model."""
    items: list[Any]
    total: int
    page: int
    page_size: int
    pages: int


class ErrorResponse(BaseModel):
    """Error response model."""
    error: str
    detail: str | None = None
    code: str | None = None


# ========================
# Pipeline Models
# ========================

class PipelineStage(BaseModel):
    """Pipeline stage definition."""
    name: str
    type: Literal["extract", "transform", "load", "llm_augment"]
    config: dict[str, Any] = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list)


class PipelineCreate(BaseModel):
    """Create pipeline request."""
    name: str = Field(..., min_length=1, max_length=100)
    description: str = ""
    stages: list[PipelineStage] = Field(default_factory=list)
    schedule: str | None = None  # Cron expression
    enabled: bool = True
    tags: list[str] = Field(default_factory=list)
    config: dict[str, Any] = Field(default_factory=dict)


class PipelineUpdate(BaseModel):
    """Update pipeline request."""
    description: str | None = None
    stages: list[PipelineStage] | None = None
    schedule: str | None = None
    enabled: bool | None = None
    tags: list[str] | None = None
    config: dict[str, Any] | None = None


class PipelineResponse(BaseModel):
    """Pipeline response."""
    id: str
    name: str
    description: str
    stages: list[PipelineStage]
    schedule: str | None
    enabled: bool
    tags: list[str]
    config: dict[str, Any]
    created_at: datetime
    updated_at: datetime
    last_run: datetime | None = None
    last_status: PipelineStatus | None = None


class PipelineRunRequest(BaseModel):
    """Request to run a pipeline."""
    config_overrides: dict[str, Any] = Field(default_factory=dict)
    async_execution: bool = True


class PipelineRunResponse(BaseModel):
    """Pipeline run response."""
    run_id: str
    pipeline_id: str
    pipeline_name: str
    status: PipelineStatus
    started_at: datetime
    completed_at: datetime | None = None
    metrics: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None


# ========================
# Table Models
# ========================

class ColumnSchema(BaseModel):
    """Column schema definition."""
    name: str
    data_type: str
    nullable: bool = True
    description: str = ""
    primary_key: bool = False
    tags: list[str] = Field(default_factory=list)


class TableCreate(BaseModel):
    """Create table request."""
    name: str = Field(..., min_length=1, max_length=100)
    tier: DataTier
    columns: list[ColumnSchema]
    description: str = ""
    partition_by: list[str] = Field(default_factory=list)
    location: str | None = None
    format: Literal["delta", "iceberg", "parquet"] = "delta"
    tags: list[str] = Field(default_factory=list)


class TableResponse(BaseModel):
    """Table response."""
    id: str
    name: str
    tier: DataTier
    columns: list[ColumnSchema]
    description: str
    partition_by: list[str]
    location: str
    format: str
    row_count: int | None = None
    size_bytes: int | None = None
    created_at: datetime
    updated_at: datetime
    tags: list[str]


class TableDataRequest(BaseModel):
    """Request for table data."""
    columns: list[str] | None = None
    filters: dict[str, Any] = Field(default_factory=dict)
    order_by: list[str] = Field(default_factory=list)
    limit: int = Field(default=100, le=10000)
    offset: int = 0


class TableDataResponse(BaseModel):
    """Table data response."""
    columns: list[str]
    data: list[list[Any]]
    total_rows: int
    returned_rows: int


# ========================
# Query Models
# ========================

class QueryRequest(BaseModel):
    """Query execution request."""
    query: str = Field(..., min_length=1)
    query_type: Literal["sql", "natural_language"] = "sql"
    parameters: dict[str, Any] = Field(default_factory=dict)
    limit: int = Field(default=1000, le=100000)
    timeout_seconds: int = Field(default=60, le=3600)


class QueryResponse(BaseModel):
    """Query execution response."""
    query_id: str
    original_query: str
    executed_sql: str | None = None
    columns: list[str]
    data: list[list[Any]]
    row_count: int
    execution_time_ms: float
    from_cache: bool = False


class QueryHistoryItem(BaseModel):
    """Query history item."""
    query_id: str
    query: str
    executed_sql: str | None
    status: Literal["completed", "failed", "cancelled"]
    row_count: int | None
    execution_time_ms: float | None
    error: str | None
    executed_at: datetime
    user: str | None


# ========================
# Connector Models
# ========================

class ConnectorConfig(BaseModel):
    """Connector configuration."""
    type: ConnectorType
    subtype: str  # e.g., "postgres", "mysql", "s3"
    name: str = Field(..., min_length=1, max_length=100)
    description: str = ""
    connection_params: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True


class ConnectorResponse(BaseModel):
    """Connector response."""
    id: str
    type: ConnectorType
    subtype: str
    name: str
    description: str
    enabled: bool
    status: Literal["connected", "disconnected", "error"]
    last_used: datetime | None
    created_at: datetime


class ConnectorTestResult(BaseModel):
    """Connector test result."""
    success: bool
    message: str
    latency_ms: float | None = None
    details: dict[str, Any] = Field(default_factory=dict)


# ========================
# Lineage Models
# ========================

class LineageNode(BaseModel):
    """Lineage graph node."""
    id: str
    name: str
    type: Literal["table", "pipeline", "source", "destination"]
    tier: DataTier | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class LineageEdge(BaseModel):
    """Lineage graph edge."""
    source: str
    target: str
    type: Literal["derives_from", "feeds_into", "transforms"]
    pipeline: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class LineageGraph(BaseModel):
    """Lineage graph."""
    nodes: list[LineageNode]
    edges: list[LineageEdge]


class ImpactAnalysis(BaseModel):
    """Impact analysis result."""
    source: str
    impacted_tables: list[str]
    impacted_pipelines: list[str]
    downstream_count: int
    risk_level: Literal["low", "medium", "high"]


# ========================
# Job Models
# ========================

class JobCreate(BaseModel):
    """Create job request."""
    name: str = Field(..., min_length=1, max_length=100)
    pipeline_id: str
    schedule: str  # Cron expression
    enabled: bool = True
    config: dict[str, Any] = Field(default_factory=dict)
    notifications: dict[str, Any] = Field(default_factory=dict)


class JobResponse(BaseModel):
    """Job response."""
    id: str
    name: str
    pipeline_id: str
    pipeline_name: str
    schedule: str
    enabled: bool
    status: JobStatus
    last_run: datetime | None
    next_run: datetime | None
    run_count: int
    success_count: int
    failure_count: int
    created_at: datetime


class JobRunResponse(BaseModel):
    """Job run response."""
    run_id: str
    job_id: str
    job_name: str
    status: JobStatus
    started_at: datetime
    completed_at: datetime | None
    duration_seconds: float | None
    metrics: dict[str, Any]
    logs: list[str]
    error: str | None


# ========================
# Health Models
# ========================

class ServiceHealth(BaseModel):
    """Individual service health."""
    name: str
    status: Literal["healthy", "degraded", "unhealthy"]
    latency_ms: float | None = None
    message: str | None = None


class HealthResponse(BaseModel):
    """Health check response."""
    status: Literal["healthy", "degraded", "unhealthy"]
    version: str
    uptime_seconds: float
    services: list[ServiceHealth]


# ========================
# Metrics Models
# ========================

class LakehouseMetrics(BaseModel):
    """Lakehouse metrics."""
    total_tables: int
    bronze_tables: int
    silver_tables: int
    gold_tables: int
    total_storage_gb: float
    total_rows: int
    pipelines_active: int
    pipelines_running: int
    jobs_scheduled: int
    queries_today: int


class PipelineMetrics(BaseModel):
    """Pipeline execution metrics."""
    rows_read: int
    rows_written: int
    bytes_processed: int
    execution_time_ms: float
    stages_completed: int
    stages_failed: int
    llm_tokens_used: int | None = None
