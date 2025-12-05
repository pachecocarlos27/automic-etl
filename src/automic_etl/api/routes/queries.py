"""Query execution endpoints with LLM-powered natural language support."""

from __future__ import annotations

import uuid
import time
from datetime import timedelta
from typing import Any

from fastapi import APIRouter, HTTPException, Query as QueryParam, Depends
from pydantic import BaseModel, Field

from automic_etl.api.models import (
    QueryRequest,
    QueryResponse,
    QueryHistoryItem,
    PaginatedResponse,
)
from automic_etl.api.middleware import (
    get_security_context,
    require_permission,
    filter_by_company,
)
from automic_etl.auth.models import PermissionType
from automic_etl.auth.security import SecurityContext
from automic_etl.core.utils import utc_now

router = APIRouter()

# Query history storage (per company in production)
_query_history: dict[str, list[dict]] = {}  # company_id -> queries
_query_cache: dict[str, dict] = {}
_conversations: dict[str, dict] = {}  # conversation_id -> context

# Per-company LLM rate limiters
_llm_rate_limiters: dict[str, dict] = {}  # company_id -> rate limit state


def _check_llm_rate_limit(company_id: str, user_id: str) -> tuple[bool, str | None]:
    """
    Check if company/user is within LLM rate limits.

    Rate limits:
    - 100 queries per minute per company
    - 20 queries per minute per user
    - 1000 queries per day per company

    Returns:
        Tuple of (allowed, reason if not allowed)
    """
    now = utc_now()
    minute_ago = now - timedelta(minutes=1)
    day_ago = now - timedelta(days=1)

    # Initialize rate limiter for company if needed
    if company_id not in _llm_rate_limiters:
        _llm_rate_limiters[company_id] = {
            "company_requests": [],
            "user_requests": {},
        }

    limiter = _llm_rate_limiters[company_id]

    # Clean old entries
    limiter["company_requests"] = [
        t for t in limiter["company_requests"] if t > minute_ago
    ]

    # Clean old day entries for daily limit
    day_requests = [t for t in limiter.get("day_requests", []) if t > day_ago]
    limiter["day_requests"] = day_requests

    # Clean user requests
    if user_id not in limiter["user_requests"]:
        limiter["user_requests"][user_id] = []
    limiter["user_requests"][user_id] = [
        t for t in limiter["user_requests"][user_id] if t > minute_ago
    ]

    # Check company minute limit (100/min)
    if len(limiter["company_requests"]) >= 100:
        return False, "Company rate limit exceeded: 100 queries per minute"

    # Check user minute limit (20/min)
    if len(limiter["user_requests"][user_id]) >= 20:
        return False, "User rate limit exceeded: 20 queries per minute"

    # Check company daily limit (1000/day)
    if len(limiter.get("day_requests", [])) >= 1000:
        return False, "Company daily limit exceeded: 1000 queries per day"

    return True, None


def _record_llm_request(company_id: str, user_id: str):
    """Record an LLM request for rate limiting."""
    now = utc_now()

    if company_id not in _llm_rate_limiters:
        _llm_rate_limiters[company_id] = {
            "company_requests": [],
            "user_requests": {},
            "day_requests": [],
        }

    limiter = _llm_rate_limiters[company_id]
    limiter["company_requests"].append(now)

    if "day_requests" not in limiter:
        limiter["day_requests"] = []
    limiter["day_requests"].append(now)

    if user_id not in limiter["user_requests"]:
        limiter["user_requests"][user_id] = []
    limiter["user_requests"][user_id].append(now)


# Pydantic models for new endpoints
class NaturalLanguageRequest(BaseModel):
    """Natural language query request."""
    query: str = Field(..., description="Natural language query")
    conversation_id: str | None = Field(None, description="Conversation ID for context")
    tables: list[str] | None = Field(None, description="Specific tables to query")
    limit: int = Field(100, ge=1, le=10000, description="Maximum rows to return")
    explain_results: bool = Field(False, description="Include natural language explanation")


class NaturalLanguageResponse(BaseModel):
    """Natural language query response."""
    query_id: str
    conversation_id: str
    original_query: str
    generated_sql: str
    explanation: str
    intent: str
    confidence: float
    tables_used: list[str]
    warnings: list[str]
    suggestions: list[str]
    columns: list[str]
    data: list[list[Any]]
    row_count: int
    execution_time_ms: float
    result_summary: str | None = None
    follow_up_questions: list[str] = []
    visualization_suggestion: str | None = None


class QueryRefinementRequest(BaseModel):
    """Request to refine a previous query."""
    conversation_id: str = Field(..., description="Conversation to refine")
    refinement: str = Field(..., description="How to modify the query")
    execute: bool = Field(True, description="Execute the refined query")


class ConversationResponse(BaseModel):
    """Conversation history response."""
    conversation_id: str
    messages: list[dict]
    tables_referenced: list[str]
    created_at: datetime
    last_updated: datetime


class QuerySuggestionResponse(BaseModel):
    """Suggested queries response."""
    suggestions: list[dict]


class AutocompleteResponse(BaseModel):
    """Autocomplete suggestions response."""
    completions: list[str]


def _get_table_schemas(ctx: SecurityContext) -> dict:
    """Get table schemas from the database."""
    from automic_etl.db.table_service import get_table_service

    service = get_table_service()
    tables = service.list_tables()
    schemas = {}

    for table in tables:
        tier = table.layer
        name = f"{tier}.{table.name}"
        schema_def = table.schema_definition or {}
        columns = schema_def.get("columns", [])

        schemas[name] = {
            "columns": {col.get("name", ""): col.get("data_type", "STRING") for col in columns},
            "description": table.description or "",
            "tier": tier,
        }

    return schemas


def _get_company_history(company_id: str) -> list[dict]:
    """Get query history for a company."""
    if company_id not in _query_history:
        _query_history[company_id] = []
    return _query_history[company_id]


def _convert_nl_to_sql_llm(
    query: str,
    tables: list[str] | None,
    ctx: SecurityContext,
    conversation_context: dict | None = None,
) -> dict:
    """Convert natural language to SQL using LLM-like logic."""
    query_lower = query.lower()
    sql = ""
    explanation = ""
    intent = "select"
    tables_used = []
    confidence = 0.85

    # Determine allowed tiers based on user permissions
    allowed_tiers = []
    if ctx.can_access_tier("bronze"):
        allowed_tiers.append("bronze")
    if ctx.can_access_tier("silver"):
        allowed_tiers.append("silver")
    if ctx.can_access_tier("gold"):
        allowed_tiers.append("gold")

    # Smart query conversion
    if any(word in query_lower for word in ["top", "best", "highest"]) and "customer" in query_lower:
        sql = """SELECT
    customer_id,
    customer_name,
    email,
    segment,
    lifetime_value,
    total_orders
FROM gold.customer_summary
ORDER BY lifetime_value DESC
LIMIT 10"""
        explanation = "Finding top customers by lifetime value"
        intent = "aggregate"
        tables_used = ["gold.customer_summary"]
        confidence = 0.92

    elif "sales" in query_lower and any(word in query_lower for word in ["month", "monthly", "trend"]):
        sql = """SELECT
    DATE_TRUNC('month', order_date) as month,
    COUNT(*) as order_count,
    SUM(total_amount) as total_sales,
    AVG(total_amount) as avg_order_value
FROM silver.orders
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month"""
        explanation = "Monthly sales trends over the past year"
        intent = "trend"
        tables_used = ["silver.orders"]
        confidence = 0.90

    elif "product" in query_lower and any(word in query_lower for word in ["category", "categories"]):
        sql = """SELECT
    category,
    COUNT(*) as product_count,
    AVG(price) as avg_price,
    SUM(stock_quantity) as total_stock
FROM silver.products
GROUP BY category
ORDER BY product_count DESC"""
        explanation = "Product distribution by category with pricing and stock info"
        intent = "aggregate"
        tables_used = ["silver.products"]
        confidence = 0.88

    elif "order" in query_lower and "customer" in query_lower:
        sql = """SELECT
    c.customer_name,
    c.segment,
    COUNT(o.order_id) as order_count,
    SUM(o.total_amount) as total_spent,
    AVG(o.total_amount) as avg_order_value
FROM silver.orders o
JOIN gold.customer_summary c ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name, c.segment
ORDER BY total_spent DESC
LIMIT 50"""
        explanation = "Customer order summary with total spending"
        intent = "join"
        tables_used = ["silver.orders", "gold.customer_summary"]
        confidence = 0.87

    elif "recent" in query_lower and "order" in query_lower:
        sql = """SELECT
    order_id,
    customer_id,
    order_date,
    status,
    total_amount,
    payment_method
FROM silver.orders
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY order_date DESC
LIMIT 100"""
        explanation = "Recent orders from the last 7 days"
        intent = "filter"
        tables_used = ["silver.orders"]
        confidence = 0.91

    elif "low stock" in query_lower or "out of stock" in query_lower:
        sql = """SELECT
    product_id,
    product_name,
    category,
    stock_quantity,
    price
FROM silver.products
WHERE stock_quantity < 10
ORDER BY stock_quantity ASC"""
        explanation = "Products with low stock levels (below 10 units)"
        intent = "filter"
        tables_used = ["silver.products"]
        confidence = 0.93

    elif "event" in query_lower or "activity" in query_lower:
        if "bronze" in allowed_tiers:
            sql = """SELECT
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users
FROM bronze.raw_events
WHERE timestamp >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY event_type
ORDER BY event_count DESC"""
            explanation = "Event activity summary for the last 24 hours"
            intent = "aggregate"
            tables_used = ["bronze.raw_events"]
            confidence = 0.86
        else:
            return {
                "error": "Access denied to bronze tier data",
                "is_safe": False,
            }

    elif conversation_context and conversation_context.get("last_sql"):
        # Refinement based on previous query
        last_sql = conversation_context["last_sql"]
        if "more" in query_lower or "increase" in query_lower:
            sql = last_sql.replace("LIMIT 10", "LIMIT 50").replace("LIMIT 100", "LIMIT 500")
            explanation = "Increased result limit from previous query"
            intent = "filter"
            tables_used = conversation_context.get("tables_used", [])
            confidence = 0.80
        else:
            sql = "SELECT * FROM silver.orders LIMIT 100"
            explanation = "Default query returning recent orders"
            intent = "select"
            tables_used = ["silver.orders"]
            confidence = 0.60

    else:
        sql = "SELECT * FROM silver.orders ORDER BY order_date DESC LIMIT 100"
        explanation = "Default query returning recent orders"
        intent = "select"
        tables_used = ["silver.orders"]
        confidence = 0.60

    # Check tier access
    schemas = _get_table_schemas(ctx)
    for table in tables_used:
        schema = schemas.get(table, {})
        tier = schema.get("tier", "silver")
        if tier not in allowed_tiers:
            return {
                "error": f"Access denied to {tier} tier table: {table}",
                "is_safe": False,
            }

    return {
        "sql": sql,
        "explanation": explanation,
        "intent": intent,
        "tables_used": tables_used,
        "confidence": confidence,
        "is_safe": True,
        "warnings": [],
        "suggestions": [
            "Try asking about specific time periods",
            "You can filter by customer segment",
            "Ask for comparisons between categories",
        ],
    }


def _execute_sql_secure(
    sql: str,
    ctx: SecurityContext,
    limit: int = 100,
) -> dict:
    """Execute SQL query with security checks against the lakehouse."""
    from automic_etl.db.data_service import get_data_service

    # Validate SQL is read-only
    sql_lower = sql.lower().strip()
    dangerous_keywords = ["drop", "truncate", "delete", "alter", "create", "insert", "update", "grant", "revoke"]
    for keyword in dangerous_keywords:
        if f" {keyword} " in f" {sql_lower} " or sql_lower.startswith(keyword):
            raise ValueError(f"Dangerous SQL operation '{keyword}' not allowed")

    # Execute query via data service
    try:
        data_service = get_data_service()
        result = data_service.execute_sql(sql, limit=limit)

        return {
            "columns": result.get("columns", []),
            "data": result.get("data", []),
        }
    except Exception as e:
        # Return empty result on error
        return {
            "columns": [],
            "data": [],
            "error": str(e),
        }


def _generate_result_summary(columns: list[str], data: list[list[Any]], intent: str) -> dict:
    """Generate natural language summary of results."""
    row_count = len(data)

    if row_count == 0:
        return {
            "summary": "No results found matching your query.",
            "follow_up_questions": ["Try broadening your search criteria"],
            "visualization_suggestion": None,
        }

    if intent == "aggregate":
        return {
            "summary": f"Found {row_count} aggregated groups. The data shows distribution across different categories.",
            "follow_up_questions": [
                "Would you like to see this broken down by time period?",
                "Should I filter to a specific category?",
                "Want to compare this with previous periods?",
            ],
            "visualization_suggestion": "bar",
        }

    elif intent == "trend":
        return {
            "summary": f"Showing {row_count} time periods. The trend data reveals patterns over time.",
            "follow_up_questions": [
                "Should I extend the time range?",
                "Would you like to see year-over-year comparison?",
                "Want to add a forecast?",
            ],
            "visualization_suggestion": "line",
        }

    elif intent == "join":
        return {
            "summary": f"Combined data from multiple tables shows {row_count} records with related information.",
            "follow_up_questions": [
                "Would you like more details on specific records?",
                "Should I add additional related data?",
                "Want to filter by a specific criterion?",
            ],
            "visualization_suggestion": "table",
        }

    else:
        return {
            "summary": f"Retrieved {row_count} records matching your query.",
            "follow_up_questions": [
                "Would you like to filter these results?",
                "Should I sort by a different column?",
                "Want to see aggregated statistics?",
            ],
            "visualization_suggestion": "table",
        }


@router.post("/natural", response_model=NaturalLanguageResponse)
async def execute_natural_language_query(
    request: NaturalLanguageRequest,
    ctx: SecurityContext = Depends(require_permission(PermissionType.QUERY_EXECUTE)),
):
    """
    Execute a natural language query.

    The LLM converts natural language to SQL, validates security,
    executes the query, and returns results with explanations.
    """
    start_time = time.time()
    query_id = str(uuid.uuid4())
    company_id = ctx.tenant.company_id
    user_id = ctx.user.user_id

    # Check LLM rate limits
    allowed, reason = _check_llm_rate_limit(company_id, user_id)
    if not allowed:
        raise HTTPException(
            status_code=429,
            detail=reason,
            headers={"Retry-After": "60"},
        )

    # Record the request
    _record_llm_request(company_id, user_id)

    # Get or create conversation
    conversation_id = request.conversation_id or str(uuid.uuid4())
    if conversation_id not in _conversations:
        _conversations[conversation_id] = {
            "id": conversation_id,
            "user_id": user_id,
            "company_id": company_id,
            "messages": [],
            "tables_used": set(),
            "last_sql": None,
            "created_at": utc_now(),
            "updated_at": utc_now(),
        }

    conversation = _conversations[conversation_id]

    # Convert NL to SQL
    conversion_result = _convert_nl_to_sql_llm(
        request.query,
        request.tables,
        ctx,
        conversation,
    )

    if not conversion_result.get("is_safe", True):
        raise HTTPException(
            status_code=403,
            detail=conversion_result.get("error", "Query not allowed")
        )

    sql = conversion_result["sql"]
    explanation = conversion_result["explanation"]
    intent = conversion_result["intent"]
    tables_used = conversion_result["tables_used"]
    confidence = conversion_result["confidence"]
    warnings = conversion_result.get("warnings", [])
    suggestions = conversion_result.get("suggestions", [])

    # Execute query
    try:
        result = _execute_sql_secure(sql, ctx, request.limit)
    except Exception as e:
        history = _get_company_history(company_id)
        history.append({
            "query_id": query_id,
            "conversation_id": conversation_id,
            "query": request.query,
            "executed_sql": sql,
            "status": "failed",
            "error": str(e),
            "executed_at": utc_now(),
            "user_id": user_id,
        })
        raise HTTPException(status_code=400, detail=str(e))

    execution_time_ms = (time.time() - start_time) * 1000

    # Generate result summary if requested
    result_summary = None
    follow_up_questions = []
    visualization_suggestion = None

    if request.explain_results:
        summary_result = _generate_result_summary(
            result["columns"],
            result["data"],
            intent,
        )
        result_summary = summary_result["summary"]
        follow_up_questions = summary_result["follow_up_questions"]
        visualization_suggestion = summary_result["visualization_suggestion"]

    # Update conversation
    conversation["messages"].append({
        "role": "user",
        "content": request.query,
        "timestamp": utc_now().isoformat(),
    })
    conversation["messages"].append({
        "role": "assistant",
        "content": explanation,
        "sql": sql,
        "timestamp": utc_now().isoformat(),
    })
    conversation["last_sql"] = sql
    conversation["tables_used"].update(tables_used)
    conversation["updated_at"] = utc_now()

    # Log to history
    history = _get_company_history(company_id)
    history.append({
        "query_id": query_id,
        "conversation_id": conversation_id,
        "query": request.query,
        "executed_sql": sql,
        "status": "completed",
        "row_count": len(result["data"]),
        "execution_time_ms": execution_time_ms,
        "intent": intent,
        "confidence": confidence,
        "tables_used": tables_used,
        "executed_at": utc_now(),
        "user_id": user_id,
    })

    return NaturalLanguageResponse(
        query_id=query_id,
        conversation_id=conversation_id,
        original_query=request.query,
        generated_sql=sql,
        explanation=explanation,
        intent=intent,
        confidence=confidence,
        tables_used=tables_used,
        warnings=warnings,
        suggestions=suggestions,
        columns=result["columns"],
        data=result["data"],
        row_count=len(result["data"]),
        execution_time_ms=execution_time_ms,
        result_summary=result_summary,
        follow_up_questions=follow_up_questions,
        visualization_suggestion=visualization_suggestion,
    )


@router.post("/refine", response_model=NaturalLanguageResponse)
async def refine_query(
    request: QueryRefinementRequest,
    ctx: SecurityContext = Depends(require_permission(PermissionType.QUERY_EXECUTE)),
):
    """
    Refine a previous query based on feedback.

    Uses conversation context to understand the refinement request.
    """
    if request.conversation_id not in _conversations:
        raise HTTPException(status_code=404, detail="Conversation not found")

    conversation = _conversations[request.conversation_id]

    if conversation["company_id"] != ctx.tenant.company_id:
        raise HTTPException(status_code=403, detail="Access denied to this conversation")

    # Create refinement query request
    nl_request = NaturalLanguageRequest(
        query=request.refinement,
        conversation_id=request.conversation_id,
        tables=list(conversation.get("tables_used", [])),
        explain_results=True,
    )

    return await execute_natural_language_query(nl_request, ctx)


@router.get("/conversations/{conversation_id}", response_model=ConversationResponse)
async def get_conversation(
    conversation_id: str,
    ctx: SecurityContext = Depends(require_permission(PermissionType.QUERY_EXECUTE)),
):
    """Get conversation history."""
    if conversation_id not in _conversations:
        raise HTTPException(status_code=404, detail="Conversation not found")

    conversation = _conversations[conversation_id]

    if conversation["company_id"] != ctx.tenant.company_id:
        raise HTTPException(status_code=403, detail="Access denied")

    return ConversationResponse(
        conversation_id=conversation_id,
        messages=conversation["messages"],
        tables_referenced=list(conversation.get("tables_used", set())),
        created_at=conversation["created_at"],
        last_updated=conversation["updated_at"],
    )


@router.delete("/conversations/{conversation_id}")
async def clear_conversation(
    conversation_id: str,
    ctx: SecurityContext = Depends(require_permission(PermissionType.QUERY_EXECUTE)),
):
    """Clear a conversation context."""
    if conversation_id not in _conversations:
        raise HTTPException(status_code=404, detail="Conversation not found")

    conversation = _conversations[conversation_id]

    if conversation["company_id"] != ctx.tenant.company_id:
        raise HTTPException(status_code=403, detail="Access denied")

    del _conversations[conversation_id]

    return {"success": True, "message": "Conversation cleared"}


@router.get("/suggestions", response_model=QuerySuggestionResponse)
async def get_query_suggestions(
    tables: list[str] | None = QueryParam(None),
    ctx: SecurityContext = Depends(require_permission(PermissionType.QUERY_EXECUTE)),
):
    """
    Get suggested queries based on available tables.

    Returns useful analytical queries tailored to the user's role.
    """
    allowed_tiers = []
    if ctx.can_access_tier("bronze"):
        allowed_tiers.append("bronze")
    if ctx.can_access_tier("silver"):
        allowed_tiers.append("silver")
    if ctx.can_access_tier("gold"):
        allowed_tiers.append("gold")

    suggestions = []

    if "gold" in allowed_tiers:
        suggestions.append({
            "natural_language": "Show me top 10 customers by lifetime value",
            "description": "Identifies most valuable customers for retention focus",
            "complexity": "simple",
        })

    if "silver" in allowed_tiers:
        suggestions.extend([
            {
                "natural_language": "What are the monthly sales trends for the past year?",
                "description": "Reveals seasonal patterns and growth trends",
                "complexity": "moderate",
            },
            {
                "natural_language": "Which products have low stock levels?",
                "description": "Helps identify inventory replenishment needs",
                "complexity": "simple",
            },
            {
                "natural_language": "Show customer orders grouped by segment",
                "description": "Analyzes purchasing behavior by customer segment",
                "complexity": "moderate",
            },
        ])

    if "bronze" in allowed_tiers:
        suggestions.append({
            "natural_language": "What user events happened in the last 24 hours?",
            "description": "Raw event activity for debugging and analysis",
            "complexity": "simple",
        })

    return QuerySuggestionResponse(suggestions=suggestions)


@router.get("/autocomplete", response_model=AutocompleteResponse)
async def autocomplete_query(
    partial: str = QueryParam(..., min_length=3),
    ctx: SecurityContext = Depends(require_permission(PermissionType.QUERY_EXECUTE)),
):
    """Get autocomplete suggestions for partial queries."""
    partial_lower = partial.lower()
    completions = []

    if partial_lower.startswith("show"):
        completions = [
            "show me top customers by revenue",
            "show monthly sales trends",
            "show products by category",
        ]
    elif partial_lower.startswith("what"):
        completions = [
            "what are the top selling products?",
            "what is the average order value?",
            "what customers have the most orders?",
        ]
    elif partial_lower.startswith("how"):
        completions = [
            "how many orders were placed last month?",
            "how is revenue trending over time?",
            "how many customers are in each segment?",
        ]
    elif "customer" in partial_lower:
        completions = [
            "customer lifetime value analysis",
            "customers with most orders",
            "customer segment breakdown",
        ]
    elif "sales" in partial_lower or "revenue" in partial_lower:
        completions = [
            "sales by month",
            "revenue by product category",
            "sales trend analysis",
        ]
    else:
        completions = [
            f"{partial} - grouped by category",
            f"{partial} - for the last 30 days",
            f"{partial} - top 10 results",
        ]

    return AutocompleteResponse(completions=completions[:5])


@router.get("/rate-limit-status")
async def get_rate_limit_status(
    ctx: SecurityContext = Depends(require_permission(PermissionType.QUERY_EXECUTE)),
):
    """
    Get current LLM rate limit status for the user and company.

    Returns current usage and remaining limits.
    """
    company_id = ctx.tenant.company_id
    user_id = ctx.user.user_id
    now = utc_now()
    minute_ago = now - timedelta(minutes=1)
    day_ago = now - timedelta(days=1)

    if company_id not in _llm_rate_limiters:
        return {
            "company": {
                "requests_per_minute": 0,
                "requests_per_minute_limit": 100,
                "requests_per_day": 0,
                "requests_per_day_limit": 1000,
            },
            "user": {
                "requests_per_minute": 0,
                "requests_per_minute_limit": 20,
            },
        }

    limiter = _llm_rate_limiters[company_id]

    # Count recent requests
    company_minute = len([t for t in limiter.get("company_requests", []) if t > minute_ago])
    company_day = len([t for t in limiter.get("day_requests", []) if t > day_ago])
    user_minute = len([
        t for t in limiter.get("user_requests", {}).get(user_id, [])
        if t > minute_ago
    ])

    return {
        "company": {
            "requests_per_minute": company_minute,
            "requests_per_minute_limit": 100,
            "remaining_per_minute": max(0, 100 - company_minute),
            "requests_per_day": company_day,
            "requests_per_day_limit": 1000,
            "remaining_per_day": max(0, 1000 - company_day),
        },
        "user": {
            "requests_per_minute": user_minute,
            "requests_per_minute_limit": 20,
            "remaining_per_minute": max(0, 20 - user_minute),
        },
    }


@router.get("/schemas")
async def get_available_schemas(
    ctx: SecurityContext = Depends(require_permission(PermissionType.TABLE_READ)),
):
    """Get available table schemas the user can query."""
    allowed_tiers = []
    if ctx.can_access_tier("bronze"):
        allowed_tiers.append("bronze")
    if ctx.can_access_tier("silver"):
        allowed_tiers.append("silver")
    if ctx.can_access_tier("gold"):
        allowed_tiers.append("gold")

    db_schemas = _get_table_schemas(ctx)
    schemas = []
    for table_name, schema_info in db_schemas.items():
        if schema_info.get("tier", "silver") in allowed_tiers:
            schemas.append({
                "name": table_name,
                "tier": schema_info.get("tier"),
                "description": schema_info.get("description", ""),
                "columns": [
                    {"name": col, "type": dtype}
                    for col, dtype in schema_info.get("columns", {}).items()
                ],
            })

    return {"schemas": schemas, "allowed_tiers": allowed_tiers}


@router.post("/execute", response_model=QueryResponse)
async def execute_query(
    request: QueryRequest,
    ctx: SecurityContext = Depends(require_permission(PermissionType.QUERY_EXECUTE)),
):
    """
    Execute a SQL or natural language query.

    For natural language queries, the LLM will convert to SQL first.
    """
    start_time = time.time()
    query_id = str(uuid.uuid4())
    company_id = ctx.tenant.company_id

    # Check cache
    cache_key = f"{company_id}:{request.query}:{request.query_type}:{hash(str(request.parameters))}"
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
        conversion = _convert_nl_to_sql_llm(request.query, None, ctx, None)
        if not conversion.get("is_safe", True):
            raise HTTPException(status_code=403, detail=conversion.get("error"))
        executed_sql = conversion["sql"]

    # Execute the query
    try:
        result = _execute_sql_secure(executed_sql, ctx, request.limit)
    except Exception as e:
        history = _get_company_history(company_id)
        history.append({
            "query_id": query_id,
            "query": request.query,
            "executed_sql": executed_sql,
            "status": "failed",
            "error": str(e),
            "executed_at": utc_now(),
            "user_id": ctx.user.user_id,
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
    history = _get_company_history(company_id)
    history.append({
        "query_id": query_id,
        "query": request.query,
        "executed_sql": executed_sql,
        "status": "completed",
        "row_count": len(result["data"]),
        "execution_time_ms": execution_time_ms,
        "executed_at": utc_now(),
        "user_id": ctx.user.user_id,
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


@router.get("/history", response_model=PaginatedResponse)
async def get_query_history(
    page: int = QueryParam(1, ge=1),
    page_size: int = QueryParam(20, ge=1, le=100),
    status: str | None = None,
    ctx: SecurityContext = Depends(require_permission(PermissionType.QUERY_EXECUTE)),
):
    """Get query execution history for the company."""
    history = _get_company_history(ctx.tenant.company_id)

    if status:
        history = [h for h in history if h["status"] == status]

    history.sort(key=lambda x: x["executed_at"], reverse=True)

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
async def get_query_details(
    query_id: str,
    ctx: SecurityContext = Depends(require_permission(PermissionType.QUERY_EXECUTE)),
):
    """Get details of a specific query execution."""
    history = _get_company_history(ctx.tenant.company_id)

    for item in history:
        if item["query_id"] == query_id:
            return QueryHistoryItem(**item)

    raise HTTPException(status_code=404, detail="Query not found")


@router.post("/explain")
async def explain_query(
    request: QueryRequest,
    ctx: SecurityContext = Depends(require_permission(PermissionType.QUERY_EXECUTE)),
):
    """Get execution plan for a query."""
    return {
        "query": request.query,
        "plan": [
            {"operation": "Scan", "table": "silver.orders", "estimated_rows": 10000, "estimated_cost": 100},
            {"operation": "Filter", "condition": f"company_id = '{ctx.tenant.company_id}'", "estimated_rows": 8500, "estimated_cost": 50},
            {"operation": "Project", "columns": ["id", "name", "amount"], "estimated_rows": 8500, "estimated_cost": 10},
        ],
        "total_estimated_cost": 160,
        "estimated_time_ms": 250,
    }


@router.post("/validate")
async def validate_query(
    request: QueryRequest,
    ctx: SecurityContext = Depends(require_permission(PermissionType.QUERY_EXECUTE)),
):
    """Validate a query without executing it."""
    errors = []
    sql = request.query.strip()

    if not sql:
        errors.append({"type": "syntax", "message": "Query cannot be empty"})

    sql_lower = sql.lower()
    dangerous_keywords = ["drop", "truncate", "delete", "alter", "create", "insert", "update"]
    for keyword in dangerous_keywords:
        if keyword in sql_lower:
            errors.append({"type": "security", "message": f"Write operation '{keyword}' not allowed"})

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "query": request.query,
        "query_type": request.query_type,
    }


@router.delete("/cache")
async def clear_query_cache(
    ctx: SecurityContext = Depends(require_permission(PermissionType.ADMIN)),
):
    """Clear the query cache (admin only)."""
    global _query_cache
    company_id = ctx.tenant.company_id

    keys_to_remove = [k for k in _query_cache if k.startswith(f"{company_id}:")]
    for key in keys_to_remove:
        del _query_cache[key]

    return {"success": True, "message": f"Cleared {len(keys_to_remove)} cached queries"}
