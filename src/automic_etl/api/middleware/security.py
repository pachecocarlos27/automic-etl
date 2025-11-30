"""
Security middleware for FastAPI.

Provides:
- Authentication middleware
- Tenant context injection
- Permission checking
- Rate limiting
- Audit logging
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Optional
from functools import wraps

from fastapi import Request, HTTPException, Depends, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

import structlog

from automic_etl.auth.security import (
    SecurityContext,
    SecurityManager,
    get_security_manager,
    ResourceType,
    AccessLevel,
    AccessDeniedError,
    TenantMismatchError,
)
from automic_etl.auth.models import PermissionType

logger = structlog.get_logger()

# HTTP Bearer token scheme
security_scheme = HTTPBearer(auto_error=False)


# ============================================================================
# Authentication Dependencies
# ============================================================================

async def get_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(security_scheme),
):
    """
    Get the current authenticated user from the request.

    Returns:
        User object if authenticated, None otherwise
    """
    # Check for token in header
    if credentials:
        token = credentials.credentials
    else:
        # Also check for token in cookie
        token = request.cookies.get("access_token")

    if not token:
        return None

    # Get auth manager (would be injected in production)
    auth_manager = request.app.state.auth_manager if hasattr(request.app.state, 'auth_manager') else None

    if not auth_manager:
        # Demo mode - create mock user
        return _get_demo_user(request)

    # Validate session
    result = auth_manager.validate_session(token)
    if not result:
        return None

    user, session = result
    return user


async def get_current_user_required(
    user = Depends(get_current_user),
):
    """
    Require authenticated user.

    Raises:
        HTTPException: If user is not authenticated
    """
    if not user:
        raise HTTPException(
            status_code=401,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


async def get_security_context(
    request: Request,
    user = Depends(get_current_user_required),
    x_company_id: str | None = Header(None, alias="X-Company-ID"),
):
    """
    Get security context for the current request.

    Returns:
        SecurityContext with user, tenant, and permission info
    """
    # Get company manager
    company_manager = getattr(request.app.state, 'company_manager', None)
    sec_manager = get_security_manager()

    # Determine company ID
    company_id = x_company_id

    # If no company specified, get user's primary company
    if not company_id and company_manager:
        memberships = company_manager.get_user_memberships(user.user_id)
        if memberships:
            company_id = memberships[0].company_id

    # Get tenant context
    if company_manager and company_id:
        tenant = company_manager.get_tenant_context(
            user_id=user.user_id,
            company_id=company_id,
            is_superadmin=user.is_superadmin,
        )
    else:
        # Create minimal tenant context
        from automic_etl.auth.tenant import TenantContext
        tenant = TenantContext(
            company_id=company_id or "default",
            user_id=user.user_id,
            is_superadmin=user.is_superadmin,
        )

    # Create security context
    context = sec_manager.create_security_context(user, tenant)

    # Store in request state for later use
    request.state.security_context = context

    return context


def _get_demo_user(request: Request):
    """Get demo user for development/testing."""
    from automic_etl.auth.models import User, UserStatus

    # Check for demo user in session
    demo_user_id = request.headers.get("X-Demo-User-ID", "demo-user")
    is_admin = request.headers.get("X-Demo-Admin", "false").lower() == "true"

    return User(
        user_id=demo_user_id,
        username="demo_user",
        email="demo@example.com",
        password_hash="",
        salt="",
        status=UserStatus.ACTIVE,
        is_superadmin=is_admin,
        roles=["admin"] if is_admin else ["viewer"],
    )


# ============================================================================
# Permission Dependencies
# ============================================================================

def require_permission(permission: str | PermissionType):
    """
    Create a dependency that requires a specific permission.

    Usage:
        @router.get("/resource")
        async def get_resource(
            ctx: SecurityContext = Depends(require_permission(PermissionType.PIPELINE_READ))
        ):
            ...
    """
    async def check_permission(
        context: SecurityContext = Depends(get_security_context),
    ):
        if not context.has_permission(permission):
            perm_name = permission.value if isinstance(permission, PermissionType) else permission
            raise HTTPException(
                status_code=403,
                detail=f"Permission denied: {perm_name}",
            )
        return context

    return check_permission


def require_data_tier(tier: str):
    """
    Create a dependency that requires access to a data tier.

    Usage:
        @router.get("/bronze/{table}")
        async def get_bronze(
            ctx: SecurityContext = Depends(require_data_tier("bronze"))
        ):
            ...
    """
    async def check_tier(
        context: SecurityContext = Depends(get_security_context),
    ):
        if not context.can_access_tier(tier):
            raise HTTPException(
                status_code=403,
                detail=f"Access denied to {tier} tier",
            )
        return context

    return check_tier


def require_resource_access(resource_type: ResourceType, level: AccessLevel):
    """
    Create a dependency that requires access to a resource type.

    Usage:
        @router.post("/pipelines")
        async def create_pipeline(
            ctx: SecurityContext = Depends(require_resource_access(ResourceType.PIPELINE, AccessLevel.WRITE))
        ):
            ...
    """
    async def check_access(
        context: SecurityContext = Depends(get_security_context),
    ):
        actual = context.get_resource_access(resource_type)
        access_order = [AccessLevel.NONE, AccessLevel.READ, AccessLevel.WRITE,
                       AccessLevel.EXECUTE, AccessLevel.ADMIN, AccessLevel.OWNER]

        if access_order.index(actual) < access_order.index(level):
            raise HTTPException(
                status_code=403,
                detail=f"Insufficient access to {resource_type.value}",
            )
        return context

    return check_access


def require_superadmin():
    """
    Create a dependency that requires superadmin access.

    Usage:
        @router.delete("/companies/{id}")
        async def delete_company(
            ctx: SecurityContext = Depends(require_superadmin())
        ):
            ...
    """
    async def check_superadmin(
        context: SecurityContext = Depends(get_security_context),
    ):
        if not context.is_superadmin:
            raise HTTPException(
                status_code=403,
                detail="Superadmin access required",
            )
        return context

    return check_superadmin


def require_company_admin():
    """
    Create a dependency that requires company admin access.
    """
    async def check_company_admin(
        context: SecurityContext = Depends(get_security_context),
    ):
        if not context.is_superadmin and not context.is_company_admin:
            raise HTTPException(
                status_code=403,
                detail="Company admin access required",
            )
        return context

    return check_company_admin


# ============================================================================
# Tenant Middleware
# ============================================================================

class TenantMiddleware(BaseHTTPMiddleware):
    """
    Middleware to handle tenant context.

    - Extracts company ID from headers/path
    - Validates tenant access
    - Adds tenant context to request
    """

    async def dispatch(self, request: Request, call_next):
        # Skip for public endpoints
        if self._is_public_endpoint(request.url.path):
            return await call_next(request)

        # Extract company ID
        company_id = (
            request.headers.get("X-Company-ID") or
            request.path_params.get("company_id") or
            request.query_params.get("company_id")
        )

        # Store in request state
        request.state.company_id = company_id

        response = await call_next(request)

        # Add company ID to response headers
        if company_id:
            response.headers["X-Company-ID"] = company_id

        return response

    def _is_public_endpoint(self, path: str) -> bool:
        """Check if endpoint is public."""
        public_paths = [
            "/api/v1/health",
            "/docs",
            "/redoc",
            "/openapi.json",
            "/api/v1/auth/login",
            "/api/v1/auth/register",
        ]
        return any(path.startswith(p) for p in public_paths)


# ============================================================================
# Audit Middleware
# ============================================================================

class AuditMiddleware(BaseHTTPMiddleware):
    """
    Middleware to log API requests for audit purposes.
    """

    async def dispatch(self, request: Request, call_next):
        # Skip for health checks
        if request.url.path.endswith("/health"):
            return await call_next(request)

        start_time = datetime.utcnow()

        # Extract request info
        method = request.method
        path = request.url.path
        client_ip = request.client.host if request.client else "unknown"
        user_agent = request.headers.get("user-agent", "unknown")

        # Get user ID if available
        user_id = None
        try:
            if hasattr(request.state, 'security_context'):
                user_id = request.state.security_context.user.user_id
        except Exception:
            pass

        try:
            response = await call_next(request)

            # Log successful request
            duration = (datetime.utcnow() - start_time).total_seconds()

            logger.info(
                "API request",
                method=method,
                path=path,
                status_code=response.status_code,
                duration=duration,
                user_id=user_id,
                client_ip=client_ip,
            )

            return response

        except Exception as exc:
            # Log failed request
            duration = (datetime.utcnow() - start_time).total_seconds()

            logger.error(
                "API request failed",
                method=method,
                path=path,
                error=str(exc),
                duration=duration,
                user_id=user_id,
                client_ip=client_ip,
            )

            raise


# ============================================================================
# Rate Limiting
# ============================================================================

class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Simple rate limiting middleware.

    Uses in-memory storage (use Redis in production).
    """

    def __init__(self, app, requests_per_minute: int = 100):
        super().__init__(app)
        self.requests_per_minute = requests_per_minute
        self.request_counts: dict[str, list[datetime]] = {}

    async def dispatch(self, request: Request, call_next):
        # Get client identifier
        client_id = self._get_client_id(request)

        # Check rate limit
        if not self._check_rate_limit(client_id):
            return JSONResponse(
                status_code=429,
                content={
                    "error": "Rate limit exceeded",
                    "detail": f"Maximum {self.requests_per_minute} requests per minute",
                },
            )

        return await call_next(request)

    def _get_client_id(self, request: Request) -> str:
        """Get unique client identifier."""
        # Try to get user ID from auth header
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            return f"token:{auth_header[7:20]}"

        # Fall back to IP address
        if request.client:
            return f"ip:{request.client.host}"

        return "unknown"

    def _check_rate_limit(self, client_id: str) -> bool:
        """Check if client is within rate limit."""
        now = datetime.utcnow()

        # Initialize if needed
        if client_id not in self.request_counts:
            self.request_counts[client_id] = []

        # Remove old requests (older than 1 minute)
        from datetime import timedelta
        cutoff = now - timedelta(minutes=1)
        self.request_counts[client_id] = [
            t for t in self.request_counts[client_id]
            if t > cutoff
        ]

        # Check limit
        if len(self.request_counts[client_id]) >= self.requests_per_minute:
            return False

        # Add current request
        self.request_counts[client_id].append(now)
        return True


# ============================================================================
# Maintenance Mode Middleware
# ============================================================================

class MaintenanceModeMiddleware(BaseHTTPMiddleware):
    """
    Middleware to handle maintenance mode.
    """

    async def dispatch(self, request: Request, call_next):
        # Get superadmin controller
        superadmin = getattr(request.app.state, 'superadmin_controller', None)

        if superadmin:
            client_ip = request.client.host if request.client else None
            is_maintenance, message = superadmin.is_maintenance_mode(client_ip)

            if is_maintenance:
                # Allow admin endpoints
                if not request.url.path.startswith("/api/v1/admin"):
                    return JSONResponse(
                        status_code=503,
                        content={
                            "error": "Service Unavailable",
                            "detail": message or "System is under maintenance",
                        },
                    )

        return await call_next(request)


# ============================================================================
# Helper Functions for Routes
# ============================================================================

def apply_rls_filters(context: SecurityContext, table_name: str, query: str) -> str:
    """
    Apply row-level security filters to a SQL query.

    Args:
        context: Security context
        table_name: Name of the table being queried
        query: Original SQL query

    Returns:
        Modified query with RLS filters applied
    """
    filters = context.get_query_filters(table_name)

    if not filters:
        return query

    # Combine filters with AND
    rls_clause = " AND ".join(f"({f})" for f in filters)

    # Add WHERE clause or append to existing
    query_upper = query.upper()
    if " WHERE " in query_upper:
        # Find WHERE position and add filters
        where_pos = query_upper.index(" WHERE ")
        return f"{query[:where_pos + 7]}({rls_clause}) AND ({query[where_pos + 7:]})"
    else:
        # Add new WHERE clause before any GROUP BY, ORDER BY, etc.
        for keyword in [" GROUP BY", " ORDER BY", " LIMIT", " HAVING"]:
            if keyword in query_upper:
                pos = query_upper.index(keyword)
                return f"{query[:pos]} WHERE {rls_clause}{query[pos:]}"

        # No other clauses, add at end
        return f"{query} WHERE {rls_clause}"


def check_resource_access(
    context: SecurityContext,
    resource_type: ResourceType,
    resource_id: str,
    company_id: str,
    required_level: AccessLevel,
) -> None:
    """
    Check if context has required access to a specific resource.

    Raises:
        HTTPException: If access is denied
    """
    # Check tenant match
    if not context.is_superadmin and context.tenant.company_id != company_id:
        raise HTTPException(
            status_code=403,
            detail="Resource belongs to different company",
        )

    # Check access level
    actual = context.get_resource_access(resource_type, resource_id)
    access_order = [AccessLevel.NONE, AccessLevel.READ, AccessLevel.WRITE,
                   AccessLevel.EXECUTE, AccessLevel.ADMIN, AccessLevel.OWNER]

    if access_order.index(actual) < access_order.index(required_level):
        raise HTTPException(
            status_code=403,
            detail=f"Insufficient access to {resource_type.value}:{resource_id}",
        )


def filter_by_company(
    context: SecurityContext,
    items: list[dict],
    company_id_field: str = "company_id",
) -> list[dict]:
    """
    Filter items to only those belonging to user's company.

    Args:
        context: Security context
        items: List of items to filter
        company_id_field: Field name containing company ID

    Returns:
        Filtered list
    """
    if context.is_superadmin:
        return items

    company_id = context.tenant.company_id
    return [
        item for item in items
        if item.get(company_id_field) == company_id
    ]
