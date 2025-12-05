"""
Role-Level Security (RLS) and Access Control for Multi-Tenant Automic ETL.

This module provides:
- Company-scoped role management
- Resource-level access control (tables, pipelines, connectors)
- Row-level security policies
- Data tier access controls
- API security middleware helpers
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, TypeVar, Generic
from functools import wraps
import uuid

import structlog

from automic_etl.core.utils import utc_now
from automic_etl.auth.models import User, PermissionType, RoleType
from automic_etl.auth.tenant import (
    Company,
    CompanyMembership,
    TenantContext,
)

logger = structlog.get_logger()

T = TypeVar('T')


# ============================================================================
# Access Control Enums
# ============================================================================

class ResourceType(Enum):
    """Types of resources that can have access controls."""
    PIPELINE = "pipeline"
    TABLE = "table"
    CONNECTOR = "connector"
    JOB = "job"
    QUERY = "query"
    DASHBOARD = "dashboard"
    REPORT = "report"
    API_KEY = "api_key"


class AccessLevel(Enum):
    """Access levels for resources."""
    NONE = "none"
    READ = "read"
    WRITE = "write"
    EXECUTE = "execute"
    ADMIN = "admin"
    OWNER = "owner"


class DataTierAccess(Enum):
    """Access levels for data tiers."""
    NONE = "none"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    ALL = "all"


class PolicyAction(Enum):
    """Actions for security policies."""
    ALLOW = "allow"
    DENY = "deny"


# ============================================================================
# Company-Scoped Roles
# ============================================================================

@dataclass
class CompanyRole:
    """
    A role scoped to a specific company.

    Allows companies to define custom roles beyond system defaults.
    """
    role_id: str
    company_id: str
    name: str
    description: str
    permissions: list[str] = field(default_factory=list)
    data_tier_access: DataTierAccess = DataTierAccess.GOLD
    resource_access: dict[str, AccessLevel] = field(default_factory=dict)
    is_default: bool = False
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    created_by: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        company_id: str,
        name: str,
        description: str = "",
        permissions: list[str] | None = None,
        data_tier_access: DataTierAccess = DataTierAccess.GOLD,
        created_by: str | None = None,
    ) -> "CompanyRole":
        """Create a new company role."""
        return cls(
            role_id=str(uuid.uuid4()),
            company_id=company_id,
            name=name,
            description=description,
            permissions=permissions or [],
            data_tier_access=data_tier_access,
            created_by=created_by,
        )

    def has_permission(self, permission: str | PermissionType) -> bool:
        """Check if role has a permission."""
        if isinstance(permission, PermissionType):
            permission = permission.value
        return permission in self.permissions

    def can_access_tier(self, tier: str) -> bool:
        """Check if role can access a data tier."""
        if self.data_tier_access == DataTierAccess.ALL:
            return True
        if self.data_tier_access == DataTierAccess.NONE:
            return False

        tier_order = ["bronze", "silver", "gold"]
        role_tier = self.data_tier_access.value

        if role_tier not in tier_order or tier not in tier_order:
            return False

        return tier_order.index(tier) >= tier_order.index(role_tier)

    def get_resource_access(self, resource_type: ResourceType) -> AccessLevel:
        """Get access level for a resource type."""
        return self.resource_access.get(resource_type.value, AccessLevel.NONE)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "role_id": self.role_id,
            "company_id": self.company_id,
            "name": self.name,
            "description": self.description,
            "permissions": self.permissions,
            "data_tier_access": self.data_tier_access.value,
            "resource_access": {k: v.value for k, v in self.resource_access.items()},
            "is_default": self.is_default,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "created_by": self.created_by,
        }


# Default company roles
def get_default_company_roles(company_id: str) -> list[CompanyRole]:
    """Get default roles for a new company."""
    return [
        CompanyRole(
            role_id=f"{company_id}:owner",
            company_id=company_id,
            name="Owner",
            description="Full access to all company resources",
            permissions=[p.value for p in PermissionType],
            data_tier_access=DataTierAccess.ALL,
            resource_access={rt.value: AccessLevel.OWNER for rt in ResourceType},
            is_default=True,
        ),
        CompanyRole(
            role_id=f"{company_id}:admin",
            company_id=company_id,
            name="Admin",
            description="Administrative access to company resources",
            permissions=[
                PermissionType.USER_CREATE.value,
                PermissionType.USER_READ.value,
                PermissionType.USER_UPDATE.value,
                PermissionType.PIPELINE_CREATE.value,
                PermissionType.PIPELINE_READ.value,
                PermissionType.PIPELINE_UPDATE.value,
                PermissionType.PIPELINE_DELETE.value,
                PermissionType.PIPELINE_EXECUTE.value,
                PermissionType.DATA_READ_BRONZE.value,
                PermissionType.DATA_READ_SILVER.value,
                PermissionType.DATA_READ_GOLD.value,
                PermissionType.DATA_WRITE.value,
                PermissionType.CONNECTOR_CREATE.value,
                PermissionType.CONNECTOR_READ.value,
                PermissionType.CONNECTOR_UPDATE.value,
                PermissionType.CONNECTOR_DELETE.value,
                PermissionType.NOTIFICATION_MANAGE.value,
            ],
            data_tier_access=DataTierAccess.ALL,
            resource_access={
                ResourceType.PIPELINE.value: AccessLevel.ADMIN,
                ResourceType.TABLE.value: AccessLevel.ADMIN,
                ResourceType.CONNECTOR.value: AccessLevel.ADMIN,
                ResourceType.JOB.value: AccessLevel.ADMIN,
                ResourceType.QUERY.value: AccessLevel.ADMIN,
            },
            is_default=True,
        ),
        CompanyRole(
            role_id=f"{company_id}:data_engineer",
            company_id=company_id,
            name="Data Engineer",
            description="Create and manage pipelines and data transformations",
            permissions=[
                PermissionType.PIPELINE_CREATE.value,
                PermissionType.PIPELINE_READ.value,
                PermissionType.PIPELINE_UPDATE.value,
                PermissionType.PIPELINE_EXECUTE.value,
                PermissionType.DATA_READ_BRONZE.value,
                PermissionType.DATA_READ_SILVER.value,
                PermissionType.DATA_READ_GOLD.value,
                PermissionType.DATA_WRITE.value,
                PermissionType.CONNECTOR_READ.value,
            ],
            data_tier_access=DataTierAccess.ALL,
            resource_access={
                ResourceType.PIPELINE.value: AccessLevel.WRITE,
                ResourceType.TABLE.value: AccessLevel.WRITE,
                ResourceType.CONNECTOR.value: AccessLevel.READ,
                ResourceType.JOB.value: AccessLevel.WRITE,
                ResourceType.QUERY.value: AccessLevel.WRITE,
            },
            is_default=True,
        ),
        CompanyRole(
            role_id=f"{company_id}:analyst",
            company_id=company_id,
            name="Analyst",
            description="Read and analyze silver/gold data",
            permissions=[
                PermissionType.PIPELINE_READ.value,
                PermissionType.DATA_READ_SILVER.value,
                PermissionType.DATA_READ_GOLD.value,
                PermissionType.CONNECTOR_READ.value,
            ],
            data_tier_access=DataTierAccess.SILVER,
            resource_access={
                ResourceType.PIPELINE.value: AccessLevel.READ,
                ResourceType.TABLE.value: AccessLevel.READ,
                ResourceType.CONNECTOR.value: AccessLevel.READ,
                ResourceType.QUERY.value: AccessLevel.EXECUTE,
                ResourceType.DASHBOARD.value: AccessLevel.READ,
            },
            is_default=True,
        ),
        CompanyRole(
            role_id=f"{company_id}:viewer",
            company_id=company_id,
            name="Viewer",
            description="Read-only access to gold data",
            permissions=[
                PermissionType.DATA_READ_GOLD.value,
            ],
            data_tier_access=DataTierAccess.GOLD,
            resource_access={
                ResourceType.TABLE.value: AccessLevel.READ,
                ResourceType.DASHBOARD.value: AccessLevel.READ,
                ResourceType.REPORT.value: AccessLevel.READ,
            },
            is_default=True,
        ),
    ]


# ============================================================================
# Resource Access Control
# ============================================================================

@dataclass
class ResourcePermission:
    """
    Permission for a specific resource.

    Allows fine-grained access control on individual resources.
    """
    permission_id: str
    resource_type: ResourceType
    resource_id: str
    company_id: str

    # Who has access
    user_id: str | None = None  # Specific user
    role_id: str | None = None  # Role-based

    access_level: AccessLevel = AccessLevel.READ

    # Optional constraints
    expires_at: datetime | None = None
    conditions: dict[str, Any] = field(default_factory=dict)

    granted_by: str | None = None
    granted_at: datetime = field(default_factory=datetime.utcnow)

    @classmethod
    def grant_to_user(
        cls,
        resource_type: ResourceType,
        resource_id: str,
        company_id: str,
        user_id: str,
        access_level: AccessLevel,
        granted_by: str | None = None,
        expires_at: datetime | None = None,
    ) -> "ResourcePermission":
        """Grant permission to a specific user."""
        return cls(
            permission_id=str(uuid.uuid4()),
            resource_type=resource_type,
            resource_id=resource_id,
            company_id=company_id,
            user_id=user_id,
            access_level=access_level,
            granted_by=granted_by,
            expires_at=expires_at,
        )

    @classmethod
    def grant_to_role(
        cls,
        resource_type: ResourceType,
        resource_id: str,
        company_id: str,
        role_id: str,
        access_level: AccessLevel,
        granted_by: str | None = None,
    ) -> "ResourcePermission":
        """Grant permission to a role."""
        return cls(
            permission_id=str(uuid.uuid4()),
            resource_type=resource_type,
            resource_id=resource_id,
            company_id=company_id,
            role_id=role_id,
            access_level=access_level,
            granted_by=granted_by,
        )

    @property
    def is_expired(self) -> bool:
        """Check if permission has expired."""
        if not self.expires_at:
            return False
        return utc_now() > self.expires_at

    @property
    def is_valid(self) -> bool:
        """Check if permission is valid."""
        return not self.is_expired

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "permission_id": self.permission_id,
            "resource_type": self.resource_type.value,
            "resource_id": self.resource_id,
            "company_id": self.company_id,
            "user_id": self.user_id,
            "role_id": self.role_id,
            "access_level": self.access_level.value,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "conditions": self.conditions,
            "granted_by": self.granted_by,
            "granted_at": self.granted_at.isoformat(),
            "is_valid": self.is_valid,
        }


# ============================================================================
# Row-Level Security Policies
# ============================================================================

@dataclass
class RowLevelPolicy:
    """
    Row-level security policy for data access.

    Defines filters applied to queries based on user/role context.
    """
    policy_id: str
    name: str
    company_id: str
    table_name: str  # Which table this applies to

    action: PolicyAction = PolicyAction.ALLOW

    # Who this policy applies to
    applies_to_roles: list[str] = field(default_factory=list)
    applies_to_users: list[str] = field(default_factory=list)

    # Filter expression (SQL WHERE clause fragment)
    filter_expression: str = ""

    # Column restrictions
    allowed_columns: list[str] | None = None  # None = all columns
    denied_columns: list[str] = field(default_factory=list)

    # Data masking for sensitive columns
    masked_columns: dict[str, str] = field(default_factory=dict)  # column -> mask type

    enabled: bool = True
    priority: int = 0  # Higher priority policies evaluated first

    created_at: datetime = field(default_factory=datetime.utcnow)
    created_by: str | None = None
    description: str = ""

    @classmethod
    def create(
        cls,
        name: str,
        company_id: str,
        table_name: str,
        filter_expression: str = "",
        applies_to_roles: list[str] | None = None,
        created_by: str | None = None,
    ) -> "RowLevelPolicy":
        """Create a new RLS policy."""
        return cls(
            policy_id=str(uuid.uuid4()),
            name=name,
            company_id=company_id,
            table_name=table_name,
            filter_expression=filter_expression,
            applies_to_roles=applies_to_roles or [],
            created_by=created_by,
        )

    def applies_to(self, user_id: str, role_ids: list[str]) -> bool:
        """Check if policy applies to a user."""
        if user_id in self.applies_to_users:
            return True
        return any(role_id in self.applies_to_roles for role_id in role_ids)

    def get_filter(self, context: dict[str, Any] | None = None) -> str:
        """
        Get the SQL filter expression with context substitution.

        Supports placeholders like {user_id}, {company_id}, {department}.
        """
        if not self.filter_expression:
            return ""

        if not context:
            return self.filter_expression

        result = self.filter_expression
        for key, value in context.items():
            placeholder = "{" + key + "}"
            if placeholder in result:
                # Safely escape the value
                safe_value = str(value).replace("'", "''")
                result = result.replace(placeholder, f"'{safe_value}'")

        return result

    def get_allowed_columns(self, requested: list[str] | None = None) -> list[str]:
        """Get list of allowed columns after applying restrictions."""
        if requested is None:
            requested = self.allowed_columns or []

        if self.allowed_columns:
            requested = [c for c in requested if c in self.allowed_columns]

        return [c for c in requested if c not in self.denied_columns]

    def mask_value(self, column: str, value: Any) -> Any:
        """Apply data masking to a value if configured."""
        mask_type = self.masked_columns.get(column)
        if not mask_type:
            return value

        if value is None:
            return None

        str_value = str(value)

        if mask_type == "full":
            return "***MASKED***"
        elif mask_type == "partial":
            if len(str_value) > 4:
                return str_value[:2] + "*" * (len(str_value) - 4) + str_value[-2:]
            return "*" * len(str_value)
        elif mask_type == "email":
            if "@" in str_value:
                local, domain = str_value.split("@", 1)
                return local[0] + "***@" + domain
            return str_value
        elif mask_type == "phone":
            return "*" * (len(str_value) - 4) + str_value[-4:]
        elif mask_type == "ssn":
            return "***-**-" + str_value[-4:] if len(str_value) >= 4 else "***"

        return value

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "policy_id": self.policy_id,
            "name": self.name,
            "company_id": self.company_id,
            "table_name": self.table_name,
            "action": self.action.value,
            "applies_to_roles": self.applies_to_roles,
            "applies_to_users": self.applies_to_users,
            "filter_expression": self.filter_expression,
            "allowed_columns": self.allowed_columns,
            "denied_columns": self.denied_columns,
            "masked_columns": self.masked_columns,
            "enabled": self.enabled,
            "priority": self.priority,
            "created_at": self.created_at.isoformat(),
            "description": self.description,
        }


# ============================================================================
# Security Context and Checker
# ============================================================================

@dataclass
class SecurityContext:
    """
    Security context for the current request/operation.

    Combines tenant context with detailed permission information.
    """
    user: User
    tenant: TenantContext
    company_roles: list[CompanyRole] = field(default_factory=list)
    resource_permissions: list[ResourcePermission] = field(default_factory=list)
    applicable_policies: list[RowLevelPolicy] = field(default_factory=list)

    @property
    def is_superadmin(self) -> bool:
        return self.user.is_superadmin

    @property
    def is_company_admin(self) -> bool:
        if self.tenant.membership:
            return self.tenant.membership.is_company_admin
        return False

    @property
    def is_company_owner(self) -> bool:
        if self.tenant.membership:
            return self.tenant.membership.is_owner
        return False

    def get_all_permissions(self) -> set[str]:
        """Get all permissions from all assigned roles."""
        permissions = set()

        if self.is_superadmin:
            return set(p.value for p in PermissionType)

        for role in self.company_roles:
            permissions.update(role.permissions)

        return permissions

    def get_data_tier_access(self) -> DataTierAccess:
        """Get the highest data tier access from assigned roles."""
        if self.is_superadmin or self.is_company_owner:
            return DataTierAccess.ALL

        tier_order = [DataTierAccess.NONE, DataTierAccess.GOLD, DataTierAccess.SILVER,
                      DataTierAccess.BRONZE, DataTierAccess.ALL]

        highest = DataTierAccess.NONE
        for role in self.company_roles:
            if tier_order.index(role.data_tier_access) > tier_order.index(highest):
                highest = role.data_tier_access

        return highest

    def can_access_tier(self, tier: str) -> bool:
        """Check if context allows access to a data tier."""
        if self.is_superadmin:
            return True

        return any(role.can_access_tier(tier) for role in self.company_roles)

    def has_permission(self, permission: str | PermissionType) -> bool:
        """Check if context has a specific permission."""
        if isinstance(permission, PermissionType):
            permission = permission.value

        if self.is_superadmin:
            return True

        return permission in self.get_all_permissions()

    def get_resource_access(
        self,
        resource_type: ResourceType,
        resource_id: str | None = None,
    ) -> AccessLevel:
        """Get access level for a resource."""
        if self.is_superadmin or self.is_company_owner:
            return AccessLevel.OWNER

        # Check specific resource permissions first
        if resource_id:
            for perm in self.resource_permissions:
                if (perm.resource_type == resource_type and
                    perm.resource_id == resource_id and
                    perm.is_valid):
                    # Check if permission is for this user or their role
                    if perm.user_id == self.user.user_id:
                        return perm.access_level
                    if perm.role_id and any(
                        r.role_id == perm.role_id for r in self.company_roles
                    ):
                        return perm.access_level

        # Fall back to role-based access
        highest = AccessLevel.NONE
        access_order = [AccessLevel.NONE, AccessLevel.READ, AccessLevel.WRITE,
                       AccessLevel.EXECUTE, AccessLevel.ADMIN, AccessLevel.OWNER]

        for role in self.company_roles:
            level = role.get_resource_access(resource_type)
            if access_order.index(level) > access_order.index(highest):
                highest = level

        return highest

    def can_read(self, resource_type: ResourceType, resource_id: str | None = None) -> bool:
        """Check if context can read a resource."""
        level = self.get_resource_access(resource_type, resource_id)
        return level != AccessLevel.NONE

    def can_write(self, resource_type: ResourceType, resource_id: str | None = None) -> bool:
        """Check if context can write to a resource."""
        level = self.get_resource_access(resource_type, resource_id)
        return level in [AccessLevel.WRITE, AccessLevel.ADMIN, AccessLevel.OWNER]

    def can_execute(self, resource_type: ResourceType, resource_id: str | None = None) -> bool:
        """Check if context can execute a resource."""
        level = self.get_resource_access(resource_type, resource_id)
        return level in [AccessLevel.EXECUTE, AccessLevel.WRITE, AccessLevel.ADMIN, AccessLevel.OWNER]

    def can_admin(self, resource_type: ResourceType, resource_id: str | None = None) -> bool:
        """Check if context has admin access to a resource."""
        level = self.get_resource_access(resource_type, resource_id)
        return level in [AccessLevel.ADMIN, AccessLevel.OWNER]

    def get_query_filters(self, table_name: str) -> list[str]:
        """Get RLS filters to apply to a query on a table."""
        if self.is_superadmin or self.is_company_owner:
            return []

        filters = []
        role_ids = [r.role_id for r in self.company_roles]

        # Sort by priority (higher first)
        sorted_policies = sorted(
            self.applicable_policies,
            key=lambda p: p.priority,
            reverse=True
        )

        for policy in sorted_policies:
            if not policy.enabled:
                continue
            if policy.table_name != table_name:
                continue
            if not policy.applies_to(self.user.user_id, role_ids):
                continue

            context = {
                "user_id": self.user.user_id,
                "company_id": self.tenant.company_id,
            }

            filter_expr = policy.get_filter(context)
            if filter_expr:
                if policy.action == PolicyAction.ALLOW:
                    filters.append(filter_expr)
                else:
                    filters.append(f"NOT ({filter_expr})")

        return filters

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "user_id": self.user.user_id,
            "username": self.user.username,
            "is_superadmin": self.is_superadmin,
            "is_company_admin": self.is_company_admin,
            "is_company_owner": self.is_company_owner,
            "company_id": self.tenant.company_id,
            "roles": [r.name for r in self.company_roles],
            "permissions": list(self.get_all_permissions()),
            "data_tier_access": self.get_data_tier_access().value,
        }


# ============================================================================
# Access Control Decorators
# ============================================================================

class AccessDeniedError(Exception):
    """Raised when access is denied."""
    def __init__(self, message: str, required: str | None = None):
        self.message = message
        self.required = required
        super().__init__(message)


class TenantMismatchError(Exception):
    """Raised when tenant context doesn't match resource."""
    pass


def require_company_access(func: Callable) -> Callable:
    """
    Decorator to require valid company membership.

    Expects security_context in kwargs.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        ctx: SecurityContext | None = kwargs.get('security_context')

        if not ctx:
            raise AccessDeniedError("Security context not provided")

        if ctx.is_superadmin:
            return func(*args, **kwargs)

        if not ctx.tenant.membership:
            raise AccessDeniedError("No company membership found")

        if ctx.tenant.membership.status != "active":
            raise AccessDeniedError("Company membership is not active")

        return func(*args, **kwargs)
    return wrapper


def require_data_tier(tier: str):
    """
    Decorator to require access to a specific data tier.

    Args:
        tier: Required tier (bronze, silver, gold)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            ctx: SecurityContext | None = kwargs.get('security_context')

            if not ctx:
                raise AccessDeniedError("Security context not provided")

            if not ctx.can_access_tier(tier):
                raise AccessDeniedError(
                    f"Access denied to {tier} tier",
                    required=f"data_tier:{tier}"
                )

            return func(*args, **kwargs)
        return wrapper
    return decorator


def require_resource_access(resource_type: ResourceType, level: AccessLevel):
    """
    Decorator to require access level for a resource type.

    Args:
        resource_type: Type of resource
        level: Required access level
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            ctx: SecurityContext | None = kwargs.get('security_context')
            resource_id: str | None = kwargs.get('resource_id')

            if not ctx:
                raise AccessDeniedError("Security context not provided")

            actual_level = ctx.get_resource_access(resource_type, resource_id)

            access_order = [AccessLevel.NONE, AccessLevel.READ, AccessLevel.WRITE,
                          AccessLevel.EXECUTE, AccessLevel.ADMIN, AccessLevel.OWNER]

            if access_order.index(actual_level) < access_order.index(level):
                raise AccessDeniedError(
                    f"Insufficient access to {resource_type.value}",
                    required=f"{resource_type.value}:{level.value}"
                )

            return func(*args, **kwargs)
        return wrapper
    return decorator


def require_permission_check(permission: str | PermissionType):
    """
    Decorator to require a specific permission.

    Args:
        permission: Required permission
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            ctx: SecurityContext | None = kwargs.get('security_context')

            if not ctx:
                raise AccessDeniedError("Security context not provided")

            if not ctx.has_permission(permission):
                perm_name = permission.value if isinstance(permission, PermissionType) else permission
                raise AccessDeniedError(
                    f"Permission denied: {perm_name}",
                    required=perm_name
                )

            return func(*args, **kwargs)
        return wrapper
    return decorator


def check_tenant_match(company_id: str):
    """
    Decorator to verify resource belongs to user's company.

    Args:
        company_id: Company ID to check (from resource)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            ctx: SecurityContext | None = kwargs.get('security_context')
            resource_company_id: str | None = kwargs.get(company_id)

            if not ctx:
                raise AccessDeniedError("Security context not provided")

            if ctx.is_superadmin:
                return func(*args, **kwargs)

            if resource_company_id and resource_company_id != ctx.tenant.company_id:
                raise TenantMismatchError(
                    "Resource belongs to different company"
                )

            return func(*args, **kwargs)
        return wrapper
    return decorator


# ============================================================================
# Security Manager
# ============================================================================

class SecurityManager:
    """
    Central security manager for role-level security.

    Manages:
    - Company roles
    - Resource permissions
    - Row-level policies
    - Security context creation
    """

    def __init__(self) -> None:
        """Initialize security manager."""
        self.company_roles: dict[str, dict[str, CompanyRole]] = {}  # company_id -> role_id -> role
        self.resource_permissions: dict[str, list[ResourcePermission]] = {}  # company_id -> permissions
        self.rls_policies: dict[str, list[RowLevelPolicy]] = {}  # company_id -> policies

        self.logger = logger.bind(component="security_manager")

    # Company role management

    def init_company_roles(self, company_id: str) -> list[CompanyRole]:
        """Initialize default roles for a new company."""
        roles = get_default_company_roles(company_id)
        self.company_roles[company_id] = {r.role_id: r for r in roles}
        return roles

    def get_company_role(self, company_id: str, role_id: str) -> CompanyRole | None:
        """Get a specific company role."""
        company_roles = self.company_roles.get(company_id, {})
        return company_roles.get(role_id)

    def list_company_roles(self, company_id: str) -> list[CompanyRole]:
        """List all roles for a company."""
        return list(self.company_roles.get(company_id, {}).values())

    def create_company_role(
        self,
        company_id: str,
        name: str,
        description: str = "",
        permissions: list[str] | None = None,
        data_tier_access: DataTierAccess = DataTierAccess.GOLD,
        created_by: str | None = None,
    ) -> CompanyRole:
        """Create a new custom role for a company."""
        role = CompanyRole.create(
            company_id=company_id,
            name=name,
            description=description,
            permissions=permissions,
            data_tier_access=data_tier_access,
            created_by=created_by,
        )

        if company_id not in self.company_roles:
            self.company_roles[company_id] = {}

        self.company_roles[company_id][role.role_id] = role

        self.logger.info(
            "Company role created",
            company_id=company_id,
            role_id=role.role_id,
            name=name,
        )

        return role

    def update_company_role(
        self,
        company_id: str,
        role_id: str,
        name: str | None = None,
        description: str | None = None,
        permissions: list[str] | None = None,
        data_tier_access: DataTierAccess | None = None,
        resource_access: dict[str, AccessLevel] | None = None,
    ) -> CompanyRole | None:
        """Update a company role."""
        role = self.get_company_role(company_id, role_id)
        if not role:
            return None

        if role.is_default:
            self.logger.warning("Cannot modify default role", role_id=role_id)
            return None

        if name:
            role.name = name
        if description:
            role.description = description
        if permissions is not None:
            role.permissions = permissions
        if data_tier_access:
            role.data_tier_access = data_tier_access
        if resource_access:
            role.resource_access = resource_access

        role.updated_at = utc_now()

        return role

    def delete_company_role(self, company_id: str, role_id: str) -> bool:
        """Delete a company role."""
        role = self.get_company_role(company_id, role_id)
        if not role:
            return False

        if role.is_default:
            self.logger.warning("Cannot delete default role", role_id=role_id)
            return False

        del self.company_roles[company_id][role_id]
        return True

    # Resource permission management

    def grant_resource_permission(
        self,
        resource_type: ResourceType,
        resource_id: str,
        company_id: str,
        user_id: str | None = None,
        role_id: str | None = None,
        access_level: AccessLevel = AccessLevel.READ,
        granted_by: str | None = None,
        expires_at: datetime | None = None,
    ) -> ResourcePermission:
        """Grant permission to a specific resource."""
        if user_id:
            perm = ResourcePermission.grant_to_user(
                resource_type=resource_type,
                resource_id=resource_id,
                company_id=company_id,
                user_id=user_id,
                access_level=access_level,
                granted_by=granted_by,
                expires_at=expires_at,
            )
        elif role_id:
            perm = ResourcePermission.grant_to_role(
                resource_type=resource_type,
                resource_id=resource_id,
                company_id=company_id,
                role_id=role_id,
                access_level=access_level,
                granted_by=granted_by,
            )
        else:
            raise ValueError("Must specify either user_id or role_id")

        if company_id not in self.resource_permissions:
            self.resource_permissions[company_id] = []

        self.resource_permissions[company_id].append(perm)

        return perm

    def revoke_resource_permission(
        self,
        company_id: str,
        permission_id: str,
    ) -> bool:
        """Revoke a resource permission."""
        if company_id not in self.resource_permissions:
            return False

        original_len = len(self.resource_permissions[company_id])
        self.resource_permissions[company_id] = [
            p for p in self.resource_permissions[company_id]
            if p.permission_id != permission_id
        ]

        return len(self.resource_permissions[company_id]) < original_len

    def get_resource_permissions(
        self,
        company_id: str,
        resource_type: ResourceType | None = None,
        resource_id: str | None = None,
        user_id: str | None = None,
    ) -> list[ResourcePermission]:
        """Get resource permissions with optional filters."""
        perms = self.resource_permissions.get(company_id, [])

        if resource_type:
            perms = [p for p in perms if p.resource_type == resource_type]
        if resource_id:
            perms = [p for p in perms if p.resource_id == resource_id]
        if user_id:
            perms = [p for p in perms if p.user_id == user_id]

        # Filter expired
        perms = [p for p in perms if p.is_valid]

        return perms

    # RLS policy management

    def create_rls_policy(
        self,
        name: str,
        company_id: str,
        table_name: str,
        filter_expression: str = "",
        applies_to_roles: list[str] | None = None,
        allowed_columns: list[str] | None = None,
        denied_columns: list[str] | None = None,
        masked_columns: dict[str, str] | None = None,
        priority: int = 0,
        created_by: str | None = None,
    ) -> RowLevelPolicy:
        """Create a new RLS policy."""
        policy = RowLevelPolicy.create(
            name=name,
            company_id=company_id,
            table_name=table_name,
            filter_expression=filter_expression,
            applies_to_roles=applies_to_roles,
            created_by=created_by,
        )

        if allowed_columns:
            policy.allowed_columns = allowed_columns
        if denied_columns:
            policy.denied_columns = denied_columns
        if masked_columns:
            policy.masked_columns = masked_columns
        policy.priority = priority

        if company_id not in self.rls_policies:
            self.rls_policies[company_id] = []

        self.rls_policies[company_id].append(policy)

        self.logger.info(
            "RLS policy created",
            company_id=company_id,
            policy_id=policy.policy_id,
            table=table_name,
        )

        return policy

    def get_rls_policies(
        self,
        company_id: str,
        table_name: str | None = None,
    ) -> list[RowLevelPolicy]:
        """Get RLS policies for a company."""
        policies = self.rls_policies.get(company_id, [])

        if table_name:
            policies = [p for p in policies if p.table_name == table_name]

        return [p for p in policies if p.enabled]

    def delete_rls_policy(self, company_id: str, policy_id: str) -> bool:
        """Delete an RLS policy."""
        if company_id not in self.rls_policies:
            return False

        original_len = len(self.rls_policies[company_id])
        self.rls_policies[company_id] = [
            p for p in self.rls_policies[company_id]
            if p.policy_id != policy_id
        ]

        return len(self.rls_policies[company_id]) < original_len

    # Security context creation

    def create_security_context(
        self,
        user: User,
        tenant: TenantContext,
    ) -> SecurityContext:
        """Create a security context for a user."""
        company_id = tenant.company_id

        # Get user's company roles
        company_roles = []
        if tenant.membership:
            role_ids = tenant.membership.role_ids
            for role_id in role_ids:
                role = self.get_company_role(company_id, role_id)
                if role:
                    company_roles.append(role)

        # Get resource permissions for user
        resource_permissions = self.get_resource_permissions(
            company_id=company_id,
            user_id=user.user_id,
        )

        # Also get permissions for user's roles
        for role in company_roles:
            role_perms = [
                p for p in self.resource_permissions.get(company_id, [])
                if p.role_id == role.role_id and p.is_valid
            ]
            resource_permissions.extend(role_perms)

        # Get applicable RLS policies
        policies = self.rls_policies.get(company_id, [])

        return SecurityContext(
            user=user,
            tenant=tenant,
            company_roles=company_roles,
            resource_permissions=resource_permissions,
            applicable_policies=policies,
        )

    def check_access(
        self,
        context: SecurityContext,
        resource_type: ResourceType,
        resource_id: str,
        required_level: AccessLevel,
    ) -> bool:
        """Check if context has required access to a resource."""
        actual_level = context.get_resource_access(resource_type, resource_id)

        access_order = [AccessLevel.NONE, AccessLevel.READ, AccessLevel.WRITE,
                       AccessLevel.EXECUTE, AccessLevel.ADMIN, AccessLevel.OWNER]

        return access_order.index(actual_level) >= access_order.index(required_level)


# Global security manager instance
_security_manager: SecurityManager | None = None


def get_security_manager() -> SecurityManager:
    """Get or create global security manager."""
    global _security_manager
    if _security_manager is None:
        _security_manager = SecurityManager()
    return _security_manager
