"""Query execution endpoints."""

from __future__ import annotations

import uuid
import time
from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query as QueryParam

from automic_etl.api.models import (
    QueryRequest,
    QueryResponse,
    QueryHistoryItem,
    PaginatedResponse,
)

router = APIRouter()

# Query history storage
_query_history: list[dict] = []
_query_cache: dict[str, dict] = {}


@router.post("/execute", response_model=QueryResponse)
async def execute_query(request: QueryRequest):
    """
    Execute a SQL or natural language query.

    For natural language queries, the LLM will convert to SQL first.

    Args:
        request: Query configuration
    """
    start_time = time.time()
    query_id = str(uuid.uuid4())

    # Check cache
    cache_key = f"{request.query}:{request.query_type}:{hash(str(request.parameters))}"
    if cache_key in _query_cache:
        cached = _query_cache[cache_key]
        return QueryResponse(
            query_id=query_id,
            original_query=request.query,
            executed_sql=cached.get("executed_sql"),
            columns=cached["columns"],
            data=cached["data"],
            row_count=len(cached["data"]),
            execution_time_ms=(time.time() - start_time) * 1000,
            from_cache=True,
        )

    executed_sql = request.query

    # Handle natural language queries
    if request.query_type == "natural_language":
        executed_sql = _convert_nl_to_sql(request.query, request.parameters)

    # Execute the query (in production, this would use actual data engine)
    try:
        result = _execute_sql(executed_sql, request.limit)
    except Exception as e:
        # Log failed query
        _query_history.append({
            "query_id": query_id,
            "query": request.query,
            "executed_sql": executed_sql,
            "status": "failed",
            "row_count": None,
            "execution_time_ms": (time.time() - start_time) * 1000,
            "error": str(e),
            "executed_at": datetime.utcnow(),
            "user": None,
        })
        raise HTTPException(status_code=400, detail=str(e))

    execution_time_ms = (time.time() - start_time) * 1000

    # Cache result
    _query_cache[cache_key] = {
        "executed_sql": executed_sql,
        "columns": result["columns"],
        "data": result["data"],
    }

    # Log to history
    _query_history.append({
        "query_id": query_id,
        "query": request.query,
        "executed_sql": executed_sql,
        "status": "completed",
        "row_count": len(result["data"]),
        "execution_time_ms": execution_time_ms,
        "error": None,
        "executed_at": datetime.utcnow(),
        "user": None,
    })

    return QueryResponse(
        query_id=query_id,
        original_query=request.query,
        executed_sql=executed_sql if request.query_type == "natural_language" else None,
        columns=result["columns"],
        data=result["data"],
        row_count=len(result["data"]),
        execution_time_ms=execution_time_ms,
        from_cache=False,
    )


def _convert_nl_to_sql(query: str, parameters: dict) -> str:
    """Convert natural language to SQL using LLM."""
    # In production, this would call the LLM service
    # For demo, return a simple conversion

    query_lower = query.lower()

    if "top" in query_lower and "customer" in query_lower:
        return "SELECT customer_id, customer_name, total_orders FROM gold.customer_summary ORDER BY total_orders DESC LIMIT 10"
    elif "sales" in query_lower and "month" in query_lower:
        return "SELECT date_trunc('month', order_date) as month, SUM(amount) as total_sales FROM silver.orders GROUP BY 1 ORDER BY 1"
    elif "product" in query_lower:
        return "SELECT product_id, product_name, category, price FROM silver.products LIMIT 100"
    else:
        return f"SELECT * FROM bronze.raw_data LIMIT 100"


def _execute_sql(sql: str, limit: int) -> dict:
    """Execute SQL query and return results."""
    # In production, this would execute against actual data engine
    # For demo, return sample data based on query

    sql_lower = sql.lower()

    if "customer" in sql_lower:
        return {
            "columns": ["customer_id", "customer_name", "email", "total_orders"],
            "data": [
                [1, "John Doe", "john@example.com", 45],
                [2, "Jane Smith", "jane@example.com", 38],
                [3, "Bob Johnson", "bob@example.com", 32],
                [4, "Alice Brown", "alice@example.com", 28],
                [5, "Charlie Wilson", "charlie@example.com", 25],
            ][:limit]
        }
    elif "sales" in sql_lower or "order" in sql_lower:
        return {
            "columns": ["month", "total_sales", "order_count"],
            "data": [
                ["2024-01", 125000.00, 450],
                ["2024-02", 132000.00, 478],
                ["2024-03", 145000.00, 512],
                ["2024-04", 138000.00, 490],
                ["2024-05", 152000.00, 534],
            ][:limit]
        }
    elif "product" in sql_lower:
        return {
            "columns": ["product_id", "product_name", "category", "price"],
            "data": [
                [101, "Widget A", "Electronics", 29.99],
                [102, "Widget B", "Electronics", 39.99],
                [103, "Gadget X", "Accessories", 14.99],
                [104, "Gadget Y", "Accessories", 19.99],
                [105, "Tool Z", "Hardware", 49.99],
            ][:limit]
        }
    else:
        return {
            "columns": ["id", "name", "value", "created_at"],
            "data": [
                [1, "Record 1", 100, "2024-01-15T10:30:00"],
                [2, "Record 2", 200, "2024-01-16T11:45:00"],
                [3, "Record 3", 150, "2024-01-17T09:15:00"],
            ][:limit]
        }


@router.get("/history", response_model=PaginatedResponse)
async def get_query_history(
    page: int = QueryParam(1, ge=1),
    page_size: int = QueryParam(20, ge=1, le=100),
    status: str | None = None,
):
    """
    Get query execution history.

    Args:
        page: Page number
        page_size: Items per page
        status: Filter by status (completed, failed)
    """
    history = list(_query_history)

    # Filter by status
    if status:
        history = [h for h in history if h["status"] == status]

    # Sort by most recent first
    history.sort(key=lambda x: x["executed_at"], reverse=True)

    # Paginate
    total = len(history)
    start = (page - 1) * page_size
    end = start + page_size

    return PaginatedResponse(
        items=history[start:end],
        total=total,
        page=page,
        page_size=page_size,
        pages=(total + page_size - 1) // page_size,
    )


@router.get("/history/{query_id}", response_model=QueryHistoryItem)
async def get_query_details(query_id: str):
    """
    Get details of a specific query execution.

    Args:
        query_id: Query ID
    """
    for item in _query_history:
        if item["query_id"] == query_id:
            return QueryHistoryItem(**item)

    raise HTTPException(status_code=404, detail="Query not found")


@router.post("/explain")
async def explain_query(request: QueryRequest):
    """
    Get execution plan for a query.

    Args:
        request: Query to explain
    """
    # In production, this would return actual query plan
    return {
        "query": request.query,
        "plan": [
            {
                "operation": "Scan",
                "table": "silver.customers",
                "estimated_rows": 10000,
                "estimated_cost": 100,
            },
            {
                "operation": "Filter",
                "condition": "status = 'active'",
                "estimated_rows": 8500,
                "estimated_cost": 50,
            },
            {
                "operation": "Project",
                "columns": ["id", "name", "email"],
                "estimated_rows": 8500,
                "estimated_cost": 10,
            },
            {
                "operation": "Limit",
                "limit": 100,
                "estimated_rows": 100,
                "estimated_cost": 1,
            },
        ],
        "total_estimated_cost": 161,
        "estimated_time_ms": 250,
    }


@router.post("/validate")
async def validate_query(request: QueryRequest):
    """
    Validate a query without executing it.

    Args:
        request: Query to validate
    """
    # In production, this would parse and validate the query
    errors = []

    sql = request.query.strip()

    if not sql:
        errors.append({"type": "syntax", "message": "Query cannot be empty"})

    # Simple validation
    sql_lower = sql.lower()
    dangerous_keywords = ["drop", "truncate", "delete", "alter"]
    for keyword in dangerous_keywords:
        if keyword in sql_lower:
            errors.append({
                "type": "security",
                "message": f"Potentially dangerous keyword '{keyword}' detected"
            })

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "query": request.query,
        "query_type": request.query_type,
    }


@router.delete("/cache")
async def clear_query_cache():
    """Clear the query cache."""
    global _query_cache
    count = len(_query_cache)
    _query_cache = {}

    return {
        "success": True,
        "message": f"Cleared {count} cached queries"
    }
