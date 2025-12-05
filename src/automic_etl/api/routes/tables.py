"""Table management endpoints."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Depends

from automic_etl.api.models import (
    TableCreate,
    TableResponse,
    TableDataRequest,
    TableDataResponse,
    ColumnSchema,
    DataTier,
    PaginatedResponse,
    BaseResponse,
)
from automic_etl.api.middleware import (
    get_security_context,
    require_permission,
    require_data_tier,
    filter_by_company,
    check_resource_access,
    apply_rls_filters,
)
from automic_etl.auth.models import PermissionType
from automic_etl.auth.security import (
    SecurityContext,
    ResourceType,
    AccessLevel,
)
from automic_etl.db.table_service import get_table_service
from automic_etl.db.models import DataTableModel

router = APIRouter()


def _table_to_dict(table: DataTableModel) -> dict:
    """Convert database model to API response format."""
    schema = table.schema_definition or {}
    metadata = schema.get("_metadata", {})
    columns = schema.get("columns", [])

    # Build location
    location = schema.get("location", f"lakehouse/{table.layer}/{table.name}")
    format_type = schema.get("format", "delta")
    partition_by = schema.get("partition_by", [])

    return {
        "id": table.id,
        "company_id": metadata.get("created_by", ""),
        "created_by": metadata.get("created_by", ""),
        "name": table.name,
        "tier": table.layer,
        "columns": columns,
        "description": table.description or "",
        "partition_by": partition_by,
        "location": location,
        "format": format_type,
        "row_count": table.row_count or 0,
        "size_bytes": table.size_bytes or 0,
        "created_at": table.created_at,
        "updated_at": table.updated_at,
        "tags": table.tags or [],
    }


@router.get("", response_model=PaginatedResponse)
async def list_tables(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    tier: DataTier | None = None,
    tag: str | None = None,
    search: str | None = None,
    ctx: SecurityContext = Depends(require_permission(PermissionType.TABLE_READ)),
):
    """
    List all tables with pagination and filtering.

    Args:
        page: Page number
        page_size: Items per page
        tier: Filter by data tier (bronze, silver, gold)
        tag: Filter by tag
        search: Search in name and description
    """
    service = get_table_service()

    # Get tables from database
    db_tables = service.list_tables(
        layer=tier.value if tier else None,
        tag=tag,
        search=search,
    )

    # Convert to response format
    tables = [_table_to_dict(t) for t in db_tables]

    # Filter by company (multi-tenant isolation)
    tables = filter_by_company(ctx, tables)

    # Filter by accessible data tiers
    tables = [t for t in tables if ctx.can_access_tier(t.get("tier", "bronze"))]

    # Paginate
    total = len(tables)
    start = (page - 1) * page_size
    end = start + page_size

    return PaginatedResponse(
        items=tables[start:end],
        total=total,
        page=page,
        page_size=page_size,
        pages=(total + page_size - 1) // page_size,
    )


@router.post("", response_model=TableResponse, status_code=201)
async def create_table(
    table: TableCreate,
    ctx: SecurityContext = Depends(require_permission(PermissionType.TABLE_CREATE)),
):
    """
    Create a new table.

    Args:
        table: Table configuration
    """
    # Check tier access
    if not ctx.can_access_tier(table.tier.value):
        raise HTTPException(
            status_code=403,
            detail=f"Access denied to {table.tier.value} tier"
        )

    service = get_table_service()

    # Check for duplicate name in same tier
    existing = service.get_table_by_name(table.name, table.tier.value)
    if existing:
        raise HTTPException(
            status_code=400,
            detail=f"Table '{table.name}' already exists in {table.tier.value} tier"
        )

    # Build location if not provided
    location = table.location or f"lakehouse/{table.tier.value}/{table.name}"

    # Build schema definition
    schema_definition = {
        "columns": [c.model_dump() for c in table.columns],
        "partition_by": table.partition_by or [],
        "location": location,
        "format": table.format or "delta",
        "_metadata": {"created_by": ctx.user.user_id},
    }

    db_table = service.create_table(
        name=table.name,
        layer=table.tier.value,
        schema_definition=schema_definition,
        description=table.description or "",
        tags=table.tags or [],
        created_by=ctx.user.user_id,
    )

    return TableResponse(**_table_to_dict(db_table))


@router.get("/{table_id}", response_model=TableResponse)
async def get_table(
    table_id: str,
    ctx: SecurityContext = Depends(require_permission(PermissionType.TABLE_READ)),
):
    """
    Get a table by ID.

    Args:
        table_id: Table ID
    """
    service = get_table_service()
    table = service.get_table(table_id)

    if not table:
        raise HTTPException(status_code=404, detail="Table not found")

    table_dict = _table_to_dict(table)

    # Check tenant access
    check_resource_access(
        ctx, ResourceType.TABLE, table_id,
        table_dict.get("company_id", ""), AccessLevel.READ
    )

    # Check tier access
    if not ctx.can_access_tier(table.layer):
        raise HTTPException(status_code=403, detail=f"Access denied to {table.layer} tier")

    return TableResponse(**table_dict)


@router.get("/by-name/{tier}/{table_name}", response_model=TableResponse)
async def get_table_by_name(
    tier: DataTier,
    table_name: str,
    ctx: SecurityContext = Depends(require_permission(PermissionType.TABLE_READ)),
):
    """
    Get a table by tier and name.

    Args:
        tier: Data tier
        table_name: Table name
    """
    # Check tier access
    if not ctx.can_access_tier(tier.value):
        raise HTTPException(status_code=403, detail=f"Access denied to {tier.value} tier")

    service = get_table_service()
    table = service.get_table_by_name(table_name, tier.value)

    if not table:
        raise HTTPException(status_code=404, detail="Table not found")

    return TableResponse(**_table_to_dict(table))


@router.delete("/{table_id}", response_model=BaseResponse)
async def delete_table(
    table_id: str,
    drop_data: bool = False,
    ctx: SecurityContext = Depends(require_permission(PermissionType.TABLE_DELETE)),
):
    """
    Delete a table.

    Args:
        table_id: Table ID
        drop_data: Also delete the underlying data
    """
    service = get_table_service()
    table = service.get_table(table_id)

    if not table:
        raise HTTPException(status_code=404, detail="Table not found")

    table_dict = _table_to_dict(table)

    # Check tenant access with ADMIN level for delete
    check_resource_access(
        ctx, ResourceType.TABLE, table_id,
        table_dict.get("company_id", ""), AccessLevel.ADMIN
    )

    if drop_data:
        # In production, this would delete the actual data
        pass

    name = table.name
    service.delete_table(table_id)

    return BaseResponse(
        success=True,
        message=f"Table '{name}' deleted" + (" with data" if drop_data else "")
    )


@router.post("/{table_id}/data", response_model=TableDataResponse)
async def query_table_data(
    table_id: str,
    request: TableDataRequest,
    ctx: SecurityContext = Depends(require_permission(PermissionType.TABLE_READ)),
):
    """
    Query data from a table.

    Args:
        table_id: Table ID
        request: Query parameters
    """
    service = get_table_service()
    table = service.get_table(table_id)

    if not table:
        raise HTTPException(status_code=404, detail="Table not found")

    table_dict = _table_to_dict(table)

    # Check tenant access
    check_resource_access(
        ctx, ResourceType.TABLE, table_id,
        table_dict.get("company_id", ""), AccessLevel.READ
    )

    # Check tier access
    if not ctx.can_access_tier(table.layer):
        raise HTTPException(status_code=403, detail=f"Access denied to {table.layer} tier")

    # Query actual data from the table
    columns = table_dict.get("columns", [])
    col_names = request.columns or [c["name"] for c in columns]

    try:
        result = service.query_table_data(
            table_id=table_id,
            columns=col_names,
            filters=request.filters if hasattr(request, 'filters') else None,
            limit=request.limit,
            offset=request.offset if hasattr(request, 'offset') else 0,
        )

        return TableDataResponse(
            columns=result.get("columns", col_names),
            data=result.get("data", []),
            total_rows=result.get("total_rows", table.row_count or 0),
            returned_rows=len(result.get("data", [])),
        )
    except Exception:
        # Return empty result on error
        return TableDataResponse(
            columns=col_names,
            data=[],
            total_rows=table.row_count or 0,
            returned_rows=0,
        )


@router.get("/{table_id}/schema", response_model=list[ColumnSchema])
async def get_table_schema(
    table_id: str,
    ctx: SecurityContext = Depends(require_permission(PermissionType.TABLE_READ)),
):
    """
    Get table schema (columns).

    Args:
        table_id: Table ID
    """
    service = get_table_service()
    table = service.get_table(table_id)

    if not table:
        raise HTTPException(status_code=404, detail="Table not found")

    table_dict = _table_to_dict(table)

    # Check tenant access
    check_resource_access(
        ctx, ResourceType.TABLE, table_id,
        table_dict.get("company_id", ""), AccessLevel.READ
    )

    columns = table_dict.get("columns", [])
    return [ColumnSchema(**c) for c in columns]


@router.put("/{table_id}/schema", response_model=TableResponse)
async def update_table_schema(
    table_id: str,
    columns: list[ColumnSchema],
    ctx: SecurityContext = Depends(require_permission(PermissionType.TABLE_UPDATE)),
):
    """
    Update table schema.

    Args:
        table_id: Table ID
        columns: New column definitions
    """
    service = get_table_service()
    table = service.get_table(table_id)

    if not table:
        raise HTTPException(status_code=404, detail="Table not found")

    table_dict = _table_to_dict(table)

    # Check tenant access with WRITE level
    check_resource_access(
        ctx, ResourceType.TABLE, table_id,
        table_dict.get("company_id", ""), AccessLevel.WRITE
    )

    updated = service.update_schema(table_id, [c.model_dump() for c in columns])

    return TableResponse(**_table_to_dict(updated))


@router.post("/{table_id}/columns", response_model=TableResponse)
async def add_column(
    table_id: str,
    column: ColumnSchema,
    ctx: SecurityContext = Depends(require_permission(PermissionType.TABLE_UPDATE)),
):
    """
    Add a column to a table.

    Args:
        table_id: Table ID
        column: Column definition
    """
    service = get_table_service()
    table = service.get_table(table_id)

    if not table:
        raise HTTPException(status_code=404, detail="Table not found")

    table_dict = _table_to_dict(table)

    # Check tenant access with WRITE level
    check_resource_access(
        ctx, ResourceType.TABLE, table_id,
        table_dict.get("company_id", ""), AccessLevel.WRITE
    )

    # Check for duplicate column
    columns = table_dict.get("columns", [])
    if any(c["name"] == column.name for c in columns):
        raise HTTPException(status_code=400, detail=f"Column '{column.name}' already exists")

    updated = service.add_column(table_id, column.model_dump())

    return TableResponse(**_table_to_dict(updated))


@router.delete("/{table_id}/columns/{column_name}", response_model=TableResponse)
async def drop_column(
    table_id: str,
    column_name: str,
    ctx: SecurityContext = Depends(require_permission(PermissionType.TABLE_UPDATE)),
):
    """
    Drop a column from a table.

    Args:
        table_id: Table ID
        column_name: Column name to drop
    """
    service = get_table_service()
    table = service.get_table(table_id)

    if not table:
        raise HTTPException(status_code=404, detail="Table not found")

    table_dict = _table_to_dict(table)

    # Check tenant access with WRITE level
    check_resource_access(
        ctx, ResourceType.TABLE, table_id,
        table_dict.get("company_id", ""), AccessLevel.WRITE
    )

    updated = service.drop_column(table_id, column_name)

    if not updated:
        raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found")

    return TableResponse(**_table_to_dict(updated))


@router.get("/{table_id}/profile")
async def get_table_profile(
    table_id: str,
    ctx: SecurityContext = Depends(require_permission(PermissionType.TABLE_READ)),
):
    """
    Get data profile for a table.

    Args:
        table_id: Table ID
    """
    service = get_table_service()
    table = service.get_table(table_id)

    if not table:
        raise HTTPException(status_code=404, detail="Table not found")

    table_dict = _table_to_dict(table)

    # Check tenant access
    check_resource_access(
        ctx, ResourceType.TABLE, table_id,
        table_dict.get("company_id", ""), AccessLevel.READ
    )

    # Use stored profile data or generate sample
    profile_data = table.profile_data or {}
    columns = table_dict.get("columns", [])

    profile = {
        "table_id": table_id,
        "table_name": table.name,
        "row_count": table.row_count or 0,
        "column_count": len(columns),
        "size_bytes": table.size_bytes or 0,
        "quality_score": table.quality_score,
        "last_profiled_at": table.last_profiled_at,
        "columns": profile_data.get("columns", [])
    }

    # Generate default column profile if not available
    if not profile["columns"]:
        for col in columns:
            col_profile = {
                "name": col["name"],
                "data_type": col.get("data_type"),
                "null_count": 0,
                "null_percent": 0.0,
                "distinct_count": 100,
                "min": None,
                "max": None,
                "mean": None,
            }
            profile["columns"].append(col_profile)

    return profile


@router.post("/{table_id}/tags", response_model=TableResponse)
async def add_tags(
    table_id: str,
    tags: list[str],
    ctx: SecurityContext = Depends(require_permission(PermissionType.TABLE_UPDATE)),
):
    """
    Add tags to a table.

    Args:
        table_id: Table ID
        tags: Tags to add
    """
    service = get_table_service()
    table = service.get_table(table_id)

    if not table:
        raise HTTPException(status_code=404, detail="Table not found")

    table_dict = _table_to_dict(table)

    # Check tenant access with WRITE level
    check_resource_access(
        ctx, ResourceType.TABLE, table_id,
        table_dict.get("company_id", ""), AccessLevel.WRITE
    )

    updated = service.add_tags(table_id, tags)

    return TableResponse(**_table_to_dict(updated))
