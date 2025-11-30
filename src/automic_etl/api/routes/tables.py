"""Table management endpoints."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query

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

router = APIRouter()

# In-memory storage for demo
_tables: dict[str, dict] = {}


@router.get("", response_model=PaginatedResponse)
async def list_tables(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    tier: DataTier | None = None,
    tag: str | None = None,
    search: str | None = None,
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
    tables = list(_tables.values())

    # Apply filters
    if tier:
        tables = [t for t in tables if t.get("tier") == tier]
    if tag:
        tables = [t for t in tables if tag in t.get("tags", [])]
    if search:
        search_lower = search.lower()
        tables = [
            t for t in tables
            if search_lower in t["name"].lower() or search_lower in t.get("description", "").lower()
        ]

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
async def create_table(table: TableCreate):
    """
    Create a new table.

    Args:
        table: Table configuration
    """
    # Check for duplicate name in same tier
    if any(t["name"] == table.name and t["tier"] == table.tier for t in _tables.values()):
        raise HTTPException(
            status_code=400,
            detail=f"Table '{table.name}' already exists in {table.tier} tier"
        )

    table_id = str(uuid.uuid4())
    now = datetime.utcnow()

    # Build location if not provided
    location = table.location or f"lakehouse/{table.tier.value}/{table.name}"

    table_data = {
        "id": table_id,
        "name": table.name,
        "tier": table.tier,
        "columns": [c.model_dump() for c in table.columns],
        "description": table.description,
        "partition_by": table.partition_by,
        "location": location,
        "format": table.format,
        "row_count": 0,
        "size_bytes": 0,
        "created_at": now,
        "updated_at": now,
        "tags": table.tags,
    }

    _tables[table_id] = table_data
    return TableResponse(**table_data)


@router.get("/{table_id}", response_model=TableResponse)
async def get_table(table_id: str):
    """
    Get a table by ID.

    Args:
        table_id: Table ID
    """
    if table_id not in _tables:
        raise HTTPException(status_code=404, detail="Table not found")

    return TableResponse(**_tables[table_id])


@router.get("/by-name/{tier}/{table_name}", response_model=TableResponse)
async def get_table_by_name(tier: DataTier, table_name: str):
    """
    Get a table by tier and name.

    Args:
        tier: Data tier
        table_name: Table name
    """
    for table in _tables.values():
        if table["tier"] == tier and table["name"] == table_name:
            return TableResponse(**table)

    raise HTTPException(status_code=404, detail="Table not found")


@router.delete("/{table_id}", response_model=BaseResponse)
async def delete_table(table_id: str, drop_data: bool = False):
    """
    Delete a table.

    Args:
        table_id: Table ID
        drop_data: Also delete the underlying data
    """
    if table_id not in _tables:
        raise HTTPException(status_code=404, detail="Table not found")

    table = _tables[table_id]

    if drop_data:
        # In production, this would delete the actual data
        pass

    del _tables[table_id]

    return BaseResponse(
        success=True,
        message=f"Table '{table['name']}' deleted" + (" with data" if drop_data else "")
    )


@router.post("/{table_id}/data", response_model=TableDataResponse)
async def query_table_data(table_id: str, request: TableDataRequest):
    """
    Query data from a table.

    Args:
        table_id: Table ID
        request: Query parameters
    """
    if table_id not in _tables:
        raise HTTPException(status_code=404, detail="Table not found")

    table = _tables[table_id]

    # In production, this would query actual data
    # For demo, return sample data
    columns = request.columns or [c["name"] for c in table["columns"]]

    # Generate sample data
    sample_data = []
    for i in range(min(request.limit, 10)):
        row = []
        for col in columns:
            col_def = next((c for c in table["columns"] if c["name"] == col), None)
            if col_def:
                dtype = col_def.get("data_type", "STRING")
                if dtype in ("INT", "INTEGER", "BIGINT"):
                    row.append(i + 1)
                elif dtype in ("FLOAT", "DOUBLE", "DECIMAL"):
                    row.append(float(i) * 1.5)
                elif dtype in ("BOOLEAN", "BOOL"):
                    row.append(i % 2 == 0)
                else:
                    row.append(f"value_{i}")
            else:
                row.append(None)
        sample_data.append(row)

    return TableDataResponse(
        columns=columns,
        data=sample_data,
        total_rows=table.get("row_count", len(sample_data)),
        returned_rows=len(sample_data),
    )


@router.get("/{table_id}/schema", response_model=list[ColumnSchema])
async def get_table_schema(table_id: str):
    """
    Get table schema (columns).

    Args:
        table_id: Table ID
    """
    if table_id not in _tables:
        raise HTTPException(status_code=404, detail="Table not found")

    return [ColumnSchema(**c) for c in _tables[table_id]["columns"]]


@router.put("/{table_id}/schema", response_model=TableResponse)
async def update_table_schema(table_id: str, columns: list[ColumnSchema]):
    """
    Update table schema.

    Args:
        table_id: Table ID
        columns: New column definitions
    """
    if table_id not in _tables:
        raise HTTPException(status_code=404, detail="Table not found")

    table = _tables[table_id]
    table["columns"] = [c.model_dump() for c in columns]
    table["updated_at"] = datetime.utcnow()

    return TableResponse(**table)


@router.post("/{table_id}/columns", response_model=TableResponse)
async def add_column(table_id: str, column: ColumnSchema):
    """
    Add a column to a table.

    Args:
        table_id: Table ID
        column: Column definition
    """
    if table_id not in _tables:
        raise HTTPException(status_code=404, detail="Table not found")

    table = _tables[table_id]

    # Check for duplicate column
    if any(c["name"] == column.name for c in table["columns"]):
        raise HTTPException(status_code=400, detail=f"Column '{column.name}' already exists")

    table["columns"].append(column.model_dump())
    table["updated_at"] = datetime.utcnow()

    return TableResponse(**table)


@router.delete("/{table_id}/columns/{column_name}", response_model=TableResponse)
async def drop_column(table_id: str, column_name: str):
    """
    Drop a column from a table.

    Args:
        table_id: Table ID
        column_name: Column name to drop
    """
    if table_id not in _tables:
        raise HTTPException(status_code=404, detail="Table not found")

    table = _tables[table_id]

    # Find and remove column
    original_count = len(table["columns"])
    table["columns"] = [c for c in table["columns"] if c["name"] != column_name]

    if len(table["columns"]) == original_count:
        raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found")

    table["updated_at"] = datetime.utcnow()

    return TableResponse(**table)


@router.get("/{table_id}/profile")
async def get_table_profile(table_id: str):
    """
    Get data profile for a table.

    Args:
        table_id: Table ID
    """
    if table_id not in _tables:
        raise HTTPException(status_code=404, detail="Table not found")

    table = _tables[table_id]

    # In production, this would compute actual profile
    profile = {
        "table_id": table_id,
        "table_name": table["name"],
        "row_count": table.get("row_count", 0),
        "column_count": len(table["columns"]),
        "size_bytes": table.get("size_bytes", 0),
        "columns": []
    }

    for col in table["columns"]:
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
async def add_tags(table_id: str, tags: list[str]):
    """
    Add tags to a table.

    Args:
        table_id: Table ID
        tags: Tags to add
    """
    if table_id not in _tables:
        raise HTTPException(status_code=404, detail="Table not found")

    table = _tables[table_id]
    existing_tags = set(table.get("tags", []))
    existing_tags.update(tags)
    table["tags"] = list(existing_tags)
    table["updated_at"] = datetime.utcnow()

    return TableResponse(**table)
