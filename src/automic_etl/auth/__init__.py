"""Authentication and authorization for Automic ETL."""

from automic_etl.auth.models import (
    User,
    Role,
    Permission,
    AuditLog,
    UserStatus,
    RoleType,
    PermissionType,
    AuditAction,
)
from automic_etl.auth.manager import AuthManager
from automic_etl.auth.session import SessionManager
from automic_etl.auth.rbac import RBACManager, require_permission, require_role, require_superadmin

# Multi-tenant models
from automic_etl.auth.tenant import (
    Company,
    CompanyStatus,
    CompanyTier,
    CompanyLimits,
    CompanySettings,
    CompanyUsage,
    CompanyMembership,
    UserInvitation,
    InvitationStatus,
    TenantContext,
)
from automic_etl.auth.company_manager import (
    CompanyManager,
    CompanyError,
    CompanyLimitExceededError,
)
from automic_etl.auth.superadmin import (
    SuperadminController,
    SuperadminError,
    GlobalSettings,
    SystemHealth,
)

__all__ = [
    # Core models
    "User",
    "Role",
    "Permission",
    "AuditLog",
    "UserStatus",
    "RoleType",
    "PermissionType",
    "AuditAction",
    # Managers
    "AuthManager",
    "SessionManager",
    "RBACManager",
    # Decorators
    "require_permission",
    "require_role",
    "require_superadmin",
    # Multi-tenant models
    "Company",
    "CompanyStatus",
    "CompanyTier",
    "CompanyLimits",
    "CompanySettings",
    "CompanyUsage",
    "CompanyMembership",
    "UserInvitation",
    "InvitationStatus",
    "TenantContext",
    # Company management
    "CompanyManager",
    "CompanyError",
    "CompanyLimitExceededError",
    # Superadmin
    "SuperadminController",
    "SuperadminError",
    "GlobalSettings",
    "SystemHealth",
]
