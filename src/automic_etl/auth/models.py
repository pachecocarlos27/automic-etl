"""User and authentication models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any
import uuid
import hashlib
import secrets


class UserStatus(Enum):
    """User account status."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    SUSPENDED = "suspended"
    PENDING = "pending"


class RoleType(Enum):
    """Built-in role types."""
    SUPERADMIN = "superadmin"
    ADMIN = "admin"
    MANAGER = "manager"
    ANALYST = "analyst"
    VIEWER = "viewer"


class PermissionType(Enum):
    """Permission types."""
    # User management
    USER_CREATE = "user:create"
    USER_READ = "user:read"
    USER_UPDATE = "user:update"
    USER_DELETE = "user:delete"
    USER_MANAGE_ROLES = "user:manage_roles"

    # Pipeline management
    PIPELINE_CREATE = "pipeline:create"
    PIPELINE_READ = "pipeline:read"
    PIPELINE_UPDATE = "pipeline:update"
    PIPELINE_DELETE = "pipeline:delete"
    PIPELINE_EXECUTE = "pipeline:execute"

    # Data access
    DATA_READ_BRONZE = "data:read:bronze"
    DATA_READ_SILVER = "data:read:silver"
    DATA_READ_GOLD = "data:read:gold"
    DATA_WRITE = "data:write"
    DATA_DELETE = "data:delete"

    # Connector management
    CONNECTOR_CREATE = "connector:create"
    CONNECTOR_READ = "connector:read"
    CONNECTOR_UPDATE = "connector:update"
    CONNECTOR_DELETE = "connector:delete"

    # System administration
    SYSTEM_CONFIG = "system:config"
    SYSTEM_LOGS = "system:logs"
    SYSTEM_AUDIT = "system:audit"
    SYSTEM_BACKUP = "system:backup"

    # Notifications
    NOTIFICATION_MANAGE = "notification:manage"
    ALERT_MANAGE = "alert:manage"


@dataclass
class Permission:
    """A permission that can be assigned to roles."""
    permission_id: str
    name: str
    description: str
    category: str
    created_at: datetime = field(default_factory=datetime.utcnow)

    @classmethod
    def from_type(cls, ptype: PermissionType) -> "Permission":
        """Create permission from type."""
        category, action = ptype.value.split(":", 1)
        return cls(
            permission_id=ptype.value,
            name=ptype.name.replace("_", " ").title(),
            description=f"Permission to {action.replace(':', ' ')} {category}",
            category=category,
        )


@dataclass
class Role:
    """A role with associated permissions."""
    role_id: str
    name: str
    description: str
    role_type: RoleType
    permissions: list[str] = field(default_factory=list)
    is_system: bool = False
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    def has_permission(self, permission: str | PermissionType) -> bool:
        """Check if role has a permission."""
        if isinstance(permission, PermissionType):
            permission = permission.value
        return permission in self.permissions

    def add_permission(self, permission: str | PermissionType) -> None:
        """Add a permission to the role."""
        if isinstance(permission, PermissionType):
            permission = permission.value
        if permission not in self.permissions:
            self.permissions.append(permission)
            self.updated_at = datetime.utcnow()

    def remove_permission(self, permission: str | PermissionType) -> None:
        """Remove a permission from the role."""
        if isinstance(permission, PermissionType):
            permission = permission.value
        if permission in self.permissions:
            self.permissions.remove(permission)
            self.updated_at = datetime.utcnow()

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "role_id": self.role_id,
            "name": self.name,
            "description": self.description,
            "role_type": self.role_type.value,
            "permissions": self.permissions,
            "is_system": self.is_system,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


@dataclass
class User:
    """A user account."""
    user_id: str
    username: str
    email: str
    password_hash: str
    salt: str
    first_name: str = ""
    last_name: str = ""
    status: UserStatus = UserStatus.PENDING
    roles: list[str] = field(default_factory=list)
    is_superadmin: bool = False
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    last_login: datetime | None = None
    failed_login_attempts: int = 0
    locked_until: datetime | None = None
    mfa_enabled: bool = False
    mfa_secret: str | None = None
    settings: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        username: str,
        email: str,
        password: str,
        first_name: str = "",
        last_name: str = "",
        is_superadmin: bool = False,
    ) -> "User":
        """Create a new user with hashed password."""
        user_id = str(uuid.uuid4())
        salt = secrets.token_hex(32)
        password_hash = cls._hash_password(password, salt)

        return cls(
            user_id=user_id,
            username=username,
            email=email,
            password_hash=password_hash,
            salt=salt,
            first_name=first_name,
            last_name=last_name,
            is_superadmin=is_superadmin,
            status=UserStatus.ACTIVE if is_superadmin else UserStatus.PENDING,
        )

    @staticmethod
    def _hash_password(password: str, salt: str) -> str:
        """Hash a password with salt."""
        return hashlib.pbkdf2_hmac(
            'sha256',
            password.encode('utf-8'),
            salt.encode('utf-8'),
            100000
        ).hex()

    def verify_password(self, password: str) -> bool:
        """Verify a password against the hash."""
        test_hash = self._hash_password(password, self.salt)
        return secrets.compare_digest(test_hash, self.password_hash)

    def set_password(self, password: str) -> None:
        """Set a new password."""
        self.salt = secrets.token_hex(32)
        self.password_hash = self._hash_password(password, self.salt)
        self.updated_at = datetime.utcnow()

    def add_role(self, role_id: str) -> None:
        """Add a role to the user."""
        if role_id not in self.roles:
            self.roles.append(role_id)
            self.updated_at = datetime.utcnow()

    def remove_role(self, role_id: str) -> None:
        """Remove a role from the user."""
        if role_id in self.roles:
            self.roles.remove(role_id)
            self.updated_at = datetime.utcnow()

    def record_login(self, success: bool) -> None:
        """Record a login attempt."""
        if success:
            self.last_login = datetime.utcnow()
            self.failed_login_attempts = 0
            self.locked_until = None
        else:
            self.failed_login_attempts += 1

    @property
    def is_locked(self) -> bool:
        """Check if account is locked."""
        if self.locked_until and datetime.utcnow() < self.locked_until:
            return True
        return False

    @property
    def full_name(self) -> str:
        """Get full name."""
        if self.first_name and self.last_name:
            return f"{self.first_name} {self.last_name}"
        return self.first_name or self.last_name or self.username

    @property
    def display_name(self) -> str:
        """Get display name."""
        return self.full_name or self.username

    def to_dict(self, include_sensitive: bool = False) -> dict[str, Any]:
        """Convert to dictionary."""
        data = {
            "user_id": self.user_id,
            "username": self.username,
            "email": self.email,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "full_name": self.full_name,
            "status": self.status.value,
            "roles": self.roles,
            "is_superadmin": self.is_superadmin,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "last_login": self.last_login.isoformat() if self.last_login else None,
            "mfa_enabled": self.mfa_enabled,
        }
        if include_sensitive:
            data["settings"] = self.settings
            data["metadata"] = self.metadata
        return data


class AuditAction(Enum):
    """Audit log action types."""
    # Authentication
    LOGIN = "login"
    LOGOUT = "logout"
    LOGIN_FAILED = "login_failed"
    PASSWORD_CHANGE = "password_change"
    PASSWORD_RESET = "password_reset"
    MFA_ENABLED = "mfa_enabled"
    MFA_DISABLED = "mfa_disabled"

    # User management
    USER_CREATED = "user_created"
    USER_UPDATED = "user_updated"
    USER_DELETED = "user_deleted"
    USER_SUSPENDED = "user_suspended"
    USER_ACTIVATED = "user_activated"
    ROLE_ASSIGNED = "role_assigned"
    ROLE_REMOVED = "role_removed"

    # Pipeline operations
    PIPELINE_CREATED = "pipeline_created"
    PIPELINE_UPDATED = "pipeline_updated"
    PIPELINE_DELETED = "pipeline_deleted"
    PIPELINE_EXECUTED = "pipeline_executed"
    PIPELINE_FAILED = "pipeline_failed"

    # Data operations
    DATA_EXPORTED = "data_exported"
    DATA_IMPORTED = "data_imported"
    DATA_DELETED = "data_deleted"

    # System
    SYSTEM_CONFIG_CHANGED = "system_config_changed"
    BACKUP_CREATED = "backup_created"
    BACKUP_RESTORED = "backup_restored"


@dataclass
class AuditLog:
    """Audit log entry."""
    log_id: str
    timestamp: datetime
    user_id: str | None
    username: str | None
    action: AuditAction
    resource_type: str | None
    resource_id: str | None
    details: dict[str, Any]
    ip_address: str | None
    user_agent: str | None
    success: bool = True

    @classmethod
    def create(
        cls,
        action: AuditAction,
        user_id: str | None = None,
        username: str | None = None,
        resource_type: str | None = None,
        resource_id: str | None = None,
        details: dict[str, Any] | None = None,
        ip_address: str | None = None,
        user_agent: str | None = None,
        success: bool = True,
    ) -> "AuditLog":
        """Create a new audit log entry."""
        return cls(
            log_id=str(uuid.uuid4()),
            timestamp=datetime.utcnow(),
            user_id=user_id,
            username=username,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            details=details or {},
            ip_address=ip_address,
            user_agent=user_agent,
            success=success,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "log_id": self.log_id,
            "timestamp": self.timestamp.isoformat(),
            "user_id": self.user_id,
            "username": self.username,
            "action": self.action.value,
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
            "details": self.details,
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
            "success": self.success,
        }


# Default role definitions
DEFAULT_ROLES = {
    RoleType.SUPERADMIN: Role(
        role_id="superadmin",
        name="Super Administrator",
        description="Full system access with all permissions",
        role_type=RoleType.SUPERADMIN,
        permissions=[p.value for p in PermissionType],
        is_system=True,
    ),
    RoleType.ADMIN: Role(
        role_id="admin",
        name="Administrator",
        description="Administrative access to manage users and system",
        role_type=RoleType.ADMIN,
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
            PermissionType.SYSTEM_LOGS.value,
            PermissionType.NOTIFICATION_MANAGE.value,
            PermissionType.ALERT_MANAGE.value,
        ],
        is_system=True,
    ),
    RoleType.MANAGER: Role(
        role_id="manager",
        name="Pipeline Manager",
        description="Manage and execute pipelines",
        role_type=RoleType.MANAGER,
        permissions=[
            PermissionType.USER_READ.value,
            PermissionType.PIPELINE_CREATE.value,
            PermissionType.PIPELINE_READ.value,
            PermissionType.PIPELINE_UPDATE.value,
            PermissionType.PIPELINE_EXECUTE.value,
            PermissionType.DATA_READ_BRONZE.value,
            PermissionType.DATA_READ_SILVER.value,
            PermissionType.DATA_READ_GOLD.value,
            PermissionType.DATA_WRITE.value,
            PermissionType.CONNECTOR_READ.value,
            PermissionType.NOTIFICATION_MANAGE.value,
        ],
        is_system=True,
    ),
    RoleType.ANALYST: Role(
        role_id="analyst",
        name="Data Analyst",
        description="Read and analyze data",
        role_type=RoleType.ANALYST,
        permissions=[
            PermissionType.PIPELINE_READ.value,
            PermissionType.DATA_READ_SILVER.value,
            PermissionType.DATA_READ_GOLD.value,
            PermissionType.CONNECTOR_READ.value,
        ],
        is_system=True,
    ),
    RoleType.VIEWER: Role(
        role_id="viewer",
        name="Viewer",
        description="Read-only access to gold data",
        role_type=RoleType.VIEWER,
        permissions=[
            PermissionType.DATA_READ_GOLD.value,
        ],
        is_system=True,
    ),
}
