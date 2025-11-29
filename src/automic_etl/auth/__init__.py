"""Authentication and authorization for Automic ETL."""

from automic_etl.auth.models import User, Role, Permission, AuditLog
from automic_etl.auth.manager import AuthManager
from automic_etl.auth.session import SessionManager
from automic_etl.auth.rbac import RBACManager, require_permission, require_role

__all__ = [
    "User",
    "Role",
    "Permission",
    "AuditLog",
    "AuthManager",
    "SessionManager",
    "RBACManager",
    "require_permission",
    "require_role",
]
