"""Connector management endpoints."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Depends

from automic_etl.api.models import (
    ConnectorConfig,
    ConnectorResponse,
    ConnectorTestResult,
    ConnectorType,
    PaginatedResponse,
    BaseResponse,
)
from automic_etl.api.middleware import (
    get_security_context,
    require_permission,
    filter_by_company,
    check_resource_access,
)
from automic_etl.auth.models import PermissionType
from automic_etl.auth.security import (
    SecurityContext,
    ResourceType,
    AccessLevel,
)
from automic_etl.db.connector_service import get_connector_service
from automic_etl.db.models import ConnectorConfigModel

router = APIRouter()


# Supported connector types
CONNECTOR_TYPES = {
    ConnectorType.DATABASE: [
        {"subtype": "postgres", "name": "PostgreSQL", "required_fields": ["host", "port", "database", "user", "password"]},
        {"subtype": "mysql", "name": "MySQL", "required_fields": ["host", "port", "database", "user", "password"]},
        {"subtype": "snowflake", "name": "Snowflake", "required_fields": ["account", "database", "warehouse", "user", "password"]},
        {"subtype": "bigquery", "name": "BigQuery", "required_fields": ["project_id", "credentials_json"]},
        {"subtype": "mongodb", "name": "MongoDB", "required_fields": ["connection_string"]},
    ],
    ConnectorType.FILE: [
        {"subtype": "csv", "name": "CSV Files", "required_fields": ["path"]},
        {"subtype": "parquet", "name": "Parquet Files", "required_fields": ["path"]},
        {"subtype": "json", "name": "JSON Files", "required_fields": ["path"]},
        {"subtype": "excel", "name": "Excel Files", "required_fields": ["path"]},
    ],
    ConnectorType.API: [
        {"subtype": "rest", "name": "REST API", "required_fields": ["base_url"]},
        {"subtype": "salesforce", "name": "Salesforce", "required_fields": ["username", "password", "security_token"]},
        {"subtype": "hubspot", "name": "HubSpot", "required_fields": ["api_key"]},
        {"subtype": "stripe", "name": "Stripe", "required_fields": ["api_key"]},
    ],
    ConnectorType.STREAMING: [
        {"subtype": "kafka", "name": "Apache Kafka", "required_fields": ["bootstrap_servers", "topic"]},
        {"subtype": "kinesis", "name": "AWS Kinesis", "required_fields": ["stream_name", "region"]},
        {"subtype": "pubsub", "name": "Google Pub/Sub", "required_fields": ["project_id", "subscription_id"]},
    ],
    ConnectorType.CLOUD_STORAGE: [
        {"subtype": "s3", "name": "AWS S3", "required_fields": ["bucket", "region"]},
        {"subtype": "gcs", "name": "Google Cloud Storage", "required_fields": ["bucket", "project_id"]},
        {"subtype": "azure_blob", "name": "Azure Blob Storage", "required_fields": ["container", "account_name"]},
    ],
}


def _connector_to_dict(connector: ConnectorConfigModel) -> dict:
    """Convert database model to API response format."""
    return {
        "id": connector.id,
        "company_id": connector.created_by,  # Use created_by for tenant tracking
        "created_by": connector.created_by,
        "type": connector.category,
        "subtype": connector.connector_type,
        "name": connector.name,
        "description": connector.metadata_.get("description", "") if connector.metadata_ else "",
        "connection_params": connector.config or {},
        "enabled": connector.status != "disabled",
        "status": connector.status or "disconnected",
        "last_used": connector.last_tested_at,
        "created_at": connector.created_at,
    }


@router.get("/types")
async def list_connector_types():
    """
    List all available connector types and their configurations.
    """
    return CONNECTOR_TYPES


@router.get("", response_model=PaginatedResponse)
async def list_connectors(
    page: int = 1,
    page_size: int = 20,
    type: ConnectorType | None = None,
    enabled: bool | None = None,
    ctx: SecurityContext = Depends(require_permission(PermissionType.CONNECTOR_READ)),
):
    """
    List all configured connectors.

    Args:
        page: Page number
        page_size: Items per page
        type: Filter by connector type
        enabled: Filter by enabled status
    """
    service = get_connector_service()

    # Get connectors from database
    db_connectors = service.list_connectors(
        category=type.value if type else None,
    )

    # Convert to response format
    connectors = [_connector_to_dict(c) for c in db_connectors]

    # Filter by company (multi-tenant isolation)
    connectors = filter_by_company(ctx, connectors)

    # Apply enabled filter
    if enabled is not None:
        connectors = [c for c in connectors if c["enabled"] == enabled]

    # Paginate
    total = len(connectors)
    start = (page - 1) * page_size
    end = start + page_size

    return PaginatedResponse(
        items=connectors[start:end],
        total=total,
        page=page,
        page_size=page_size,
        pages=(total + page_size - 1) // page_size,
    )


@router.post("", response_model=ConnectorResponse, status_code=201)
async def create_connector(
    config: ConnectorConfig,
    ctx: SecurityContext = Depends(require_permission(PermissionType.CONNECTOR_CREATE)),
):
    """
    Create a new connector.

    Args:
        config: Connector configuration
    """
    # Validate subtype
    valid_subtypes = [c["subtype"] for c in CONNECTOR_TYPES.get(config.type, [])]
    if config.subtype not in valid_subtypes:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid subtype '{config.subtype}' for type '{config.type}'"
        )

    service = get_connector_service()

    # Check for duplicate name within company
    existing = service.list_connectors()
    company_connectors = [c for c in existing if c.created_by == ctx.user.user_id]
    if any(c.name == config.name for c in company_connectors):
        raise HTTPException(status_code=400, detail=f"Connector '{config.name}' already exists")

    connector = service.create_connector(
        name=config.name,
        connector_type=config.subtype,
        category=config.type.value,
        config=config.connection_params,
        created_by=ctx.user.user_id,
        metadata={"description": config.description or ""},
    )

    return ConnectorResponse(**_connector_to_dict(connector))


@router.get("/{connector_id}", response_model=ConnectorResponse)
async def get_connector(
    connector_id: str,
    ctx: SecurityContext = Depends(require_permission(PermissionType.CONNECTOR_READ)),
):
    """
    Get a connector by ID.

    Args:
        connector_id: Connector ID
    """
    service = get_connector_service()
    connector = service.get_connector(connector_id)

    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    connector_dict = _connector_to_dict(connector)

    # Check tenant access
    check_resource_access(
        ctx, ResourceType.CONNECTOR, connector_id,
        connector_dict.get("company_id", ""), AccessLevel.READ
    )

    return ConnectorResponse(**connector_dict)


@router.put("/{connector_id}", response_model=ConnectorResponse)
async def update_connector(
    connector_id: str,
    config: ConnectorConfig,
    ctx: SecurityContext = Depends(require_permission(PermissionType.CONNECTOR_UPDATE)),
):
    """
    Update a connector.

    Args:
        connector_id: Connector ID
        config: New configuration
    """
    service = get_connector_service()
    connector = service.get_connector(connector_id)

    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    connector_dict = _connector_to_dict(connector)

    # Check tenant access with WRITE level
    check_resource_access(
        ctx, ResourceType.CONNECTOR, connector_id,
        connector_dict.get("company_id", ""), AccessLevel.WRITE
    )

    # Update fields
    updated = service.update_connector(
        connector_id=connector_id,
        name=config.name,
        config=config.connection_params,
        metadata={"description": config.description or ""},
    )

    if config.enabled:
        service.update_connector(connector_id=connector_id, status="disconnected")
    else:
        service.update_connector(connector_id=connector_id, status="disabled")

    return ConnectorResponse(**_connector_to_dict(updated))


@router.delete("/{connector_id}", response_model=BaseResponse)
async def delete_connector(
    connector_id: str,
    ctx: SecurityContext = Depends(require_permission(PermissionType.CONNECTOR_DELETE)),
):
    """
    Delete a connector.

    Args:
        connector_id: Connector ID
    """
    service = get_connector_service()
    connector = service.get_connector(connector_id)

    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    connector_dict = _connector_to_dict(connector)

    # Check tenant access with ADMIN level for delete
    check_resource_access(
        ctx, ResourceType.CONNECTOR, connector_id,
        connector_dict.get("company_id", ""), AccessLevel.ADMIN
    )

    name = connector.name
    service.delete_connector(connector_id)

    return BaseResponse(success=True, message=f"Connector '{name}' deleted")


@router.post("/{connector_id}/test", response_model=ConnectorTestResult)
async def test_connector(
    connector_id: str,
    ctx: SecurityContext = Depends(require_permission(PermissionType.CONNECTOR_READ)),
):
    """
    Test a connector connection.

    Args:
        connector_id: Connector ID
    """
    import time

    service = get_connector_service()
    connector = service.get_connector(connector_id)

    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    connector_dict = _connector_to_dict(connector)

    # Check tenant access
    check_resource_access(
        ctx, ResourceType.CONNECTOR, connector_id,
        connector_dict.get("company_id", ""), AccessLevel.READ
    )

    # Test the actual connection
    start = time.time()
    try:
        result = service.test_connection(connector_id)
        latency = (time.time() - start) * 1000

        if result.get("success", False):
            service.update_test_status(connector_id, "connected")
            return ConnectorTestResult(
                success=True,
                message=result.get("message", f"Successfully connected to {connector.connector_type}"),
                latency_ms=latency,
                details=result.get("details", {})
            )
        else:
            service.update_test_status(connector_id, "error")
            return ConnectorTestResult(
                success=False,
                message=result.get("message", "Connection test failed"),
                latency_ms=latency,
                details=result.get("details", {})
            )
    except Exception as e:
        latency = (time.time() - start) * 1000
        service.update_test_status(connector_id, "error")
        return ConnectorTestResult(
            success=False,
            message=f"Connection test failed: {str(e)}",
            latency_ms=latency,
            details={"error": str(e)}
        )


@router.post("/{connector_id}/enable", response_model=ConnectorResponse)
async def enable_connector(
    connector_id: str,
    ctx: SecurityContext = Depends(require_permission(PermissionType.CONNECTOR_UPDATE)),
):
    """
    Enable a connector.

    Args:
        connector_id: Connector ID
    """
    service = get_connector_service()
    connector = service.get_connector(connector_id)

    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    connector_dict = _connector_to_dict(connector)

    # Check tenant access with WRITE level
    check_resource_access(
        ctx, ResourceType.CONNECTOR, connector_id,
        connector_dict.get("company_id", ""), AccessLevel.WRITE
    )

    updated = service.update_connector(connector_id=connector_id, status="disconnected")
    return ConnectorResponse(**_connector_to_dict(updated))


@router.post("/{connector_id}/disable", response_model=ConnectorResponse)
async def disable_connector(
    connector_id: str,
    ctx: SecurityContext = Depends(require_permission(PermissionType.CONNECTOR_UPDATE)),
):
    """
    Disable a connector.

    Args:
        connector_id: Connector ID
    """
    service = get_connector_service()
    connector = service.get_connector(connector_id)

    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    connector_dict = _connector_to_dict(connector)

    # Check tenant access with WRITE level
    check_resource_access(
        ctx, ResourceType.CONNECTOR, connector_id,
        connector_dict.get("company_id", ""), AccessLevel.WRITE
    )

    updated = service.update_connector(connector_id=connector_id, status="disabled")
    return ConnectorResponse(**_connector_to_dict(updated))


@router.get("/{connector_id}/schema")
async def get_connector_schema(
    connector_id: str,
    ctx: SecurityContext = Depends(require_permission(PermissionType.CONNECTOR_READ)),
):
    """
    Get schema/metadata from a connector.

    Args:
        connector_id: Connector ID
    """
    service = get_connector_service()
    connector = service.get_connector(connector_id)

    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    connector_dict = _connector_to_dict(connector)

    # Check tenant access
    check_resource_access(
        ctx, ResourceType.CONNECTOR, connector_id,
        connector_dict.get("company_id", ""), AccessLevel.READ
    )

    # Fetch actual schema from connector
    try:
        schema = service.get_schema(connector_id)
        if schema:
            return schema
    except Exception:
        pass

    # Return empty schema structure based on category
    category = connector.category

    if category == "database":
        return {"databases": [], "schemas": {}, "tables": []}
    elif category == "api":
        return {"endpoints": [], "rate_limit": None}
    elif category == "streaming":
        return {"topics": [], "partitions": 0, "consumer_groups": []}
    elif category == "cloud_storage":
        return {"buckets": [], "prefixes": [], "file_count": 0, "total_size_gb": 0}
    else:
        return {"files": []}


@router.post("/{connector_id}/preview")
async def preview_connector_data(
    connector_id: str,
    table: str | None = None,
    path: str | None = None,
    limit: int = 10,
    ctx: SecurityContext = Depends(require_permission(PermissionType.CONNECTOR_READ)),
):
    """
    Preview data from a connector.

    Args:
        connector_id: Connector ID
        table: Table name (for database connectors)
        path: File path (for file/storage connectors)
        limit: Number of rows to preview
    """
    service = get_connector_service()
    connector = service.get_connector(connector_id)

    if not connector:
        raise HTTPException(status_code=404, detail="Connector not found")

    connector_dict = _connector_to_dict(connector)

    # Check tenant access
    check_resource_access(
        ctx, ResourceType.CONNECTOR, connector_id,
        connector_dict.get("company_id", ""), AccessLevel.READ
    )

    # Update last used time
    service.update_test_status(connector_id, connector.status or "connected")

    # Fetch actual preview data from connector
    try:
        preview = service.preview_data(connector_id, table=table, path=path, limit=limit)
        if preview:
            return {
                "connector_id": connector_id,
                "connector_name": connector.name,
                "source": table or path or "default",
                "columns": preview.get("columns", []),
                "data": preview.get("data", []),
                "total_available": preview.get("total_available", len(preview.get("data", []))),
            }
    except Exception:
        pass

    # Return empty preview if unable to fetch
    return {
        "connector_id": connector_id,
        "connector_name": connector.name,
        "source": table or path or "default",
        "columns": [],
        "data": [],
        "total_available": 0,
    }
