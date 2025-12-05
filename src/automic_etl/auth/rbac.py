"""Role-Based Access Control (RBAC) for Automic ETL."""

from __future__ import annotations

from functools import wraps
from typing import Any, Callable

import structlog

from automic_etl.auth.models import (
    User,
    Role,
    Permission,
    PermissionType,
    RoleType,
    DEFAULT_ROLES,
)
from automic_etl.core.utils import utc_now

logger = structlog.get_logger()


class AccessDeniedError(Exception):
    """Raised when access is denied."""
    pass


class RBACManager:
    """
    Role-Based Access Control manager.

    Features:
    - Role management
    - Permission checking
    - Hierarchical roles
    - Dynamic permission assignment
    """

    def __init__(self) -> None:
        """Initialize RBAC manager."""
        self.roles: dict[str, Role] = {}
        self.permissions: dict[str, Permission] = {}
        self.logger = logger.bind(component="rbac")

        # Initialize default roles and permissions
        self._init_defaults()

    def _init_defaults(self) -> None:
        """Initialize default roles and permissions."""
        # Add all permissions
        for ptype in PermissionType:
            perm = Permission.from_type(ptype)
            self.permissions[perm.permission_id] = perm

        # Add default roles
        for role in DEFAULT_ROLES.values():
            self.roles[role.role_id] = role

    def create_role(
        self,
        name: str,
        description: str,
        role_type: RoleType,
        permissions: list[str] | None = None,
    ) -> Role:
        """
        Create a new role.

        Args:
            name: Role name
            description: Role description
            role_type: Role type
            permissions: List of permission IDs

        Returns:
            Created role
        """
        import uuid

        role_id = str(uuid.uuid4())

        role = Role(
            role_id=role_id,
            name=name,
            description=description,
            role_type=role_type,
            permissions=permissions or [],
        )

        self.roles[role_id] = role
        self.logger.info("Role created", role_id=role_id, name=name)

        return role

    def get_role(self, role_id: str) -> Role | None:
        """Get role by ID."""
        return self.roles.get(role_id)

    def update_role(
        self,
        role_id: str,
        name: str | None = None,
        description: str | None = None,
        permissions: list[str] | None = None,
    ) -> Role | None:
        """Update a role."""
        role = self.roles.get(role_id)
        if not role:
            return None

        if role.is_system:
            self.logger.warning("Cannot modify system role", role_id=role_id)
            return None

        if name:
            role.name = name
        if description:
            role.description = description
        if permissions is not None:
            role.permissions = permissions

        role.updated_at = utc_now()

        return role

    def delete_role(self, role_id: str) -> bool:
        """Delete a role."""
        role = self.roles.get(role_id)
        if not role:
            return False

        if role.is_system:
            self.logger.warning("Cannot delete system role", role_id=role_id)
            return False

        del self.roles[role_id]
        self.logger.info("Role deleted", role_id=role_id)
        return True

    def get_user_permissions(
        self,
        user: User,
        roles_dict: dict[str, Role] | None = None,
    ) -> set[str]:
        """
        Get all permissions for a user.

        Args:
            user: User to check
            roles_dict: Optional roles dictionary

        Returns:
            Set of permission IDs
        """
        roles_dict = roles_dict or self.roles
        permissions = set()

        # Superadmin has all permissions
        if user.is_superadmin:
            return set(p.value for p in PermissionType)

        for role_id in user.roles:
            role = roles_dict.get(role_id)
            if role:
                permissions.update(role.permissions)

        return permissions

    def check_permission(
        self,
        user: User,
        permission: str | PermissionType,
        roles_dict: dict[str, Role] | None = None,
    ) -> bool:
        """
        Check if user has a permission.

        Args:
            user: User to check
            permission: Permission to check
            roles_dict: Optional roles dictionary

        Returns:
            True if user has permission
        """
        if isinstance(permission, PermissionType):
            permission = permission.value

        # Superadmin always has access
        if user.is_superadmin:
            return True

        user_permissions = self.get_user_permissions(user, roles_dict)
        return permission in user_permissions

    def check_any_permission(
        self,
        user: User,
        permissions: list[str | PermissionType],
        roles_dict: dict[str, Role] | None = None,
    ) -> bool:
        """Check if user has any of the permissions."""
        return any(
            self.check_permission(user, p, roles_dict)
            for p in permissions
        )

    def check_all_permissions(
        self,
        user: User,
        permissions: list[str | PermissionType],
        roles_dict: dict[str, Role] | None = None,
    ) -> bool:
        """Check if user has all permissions."""
        return all(
            self.check_permission(user, p, roles_dict)
            for p in permissions
        )

    def check_role(self, user: User, role_type: RoleType) -> bool:
        """Check if user has a specific role type."""
        if user.is_superadmin:
            return True

        for role_id in user.roles:
            role = self.roles.get(role_id)
            if role and role.role_type == role_type:
                return True

        return False

    def get_accessible_resources(
        self,
        user: User,
        resource_type: str,
    ) -> list[str]:
        """
        Get list of accessible resource types for user.

        Args:
            user: User to check
            resource_type: Type of resource (e.g., 'data', 'pipeline')

        Returns:
            List of accessible sub-resources
        """
        if user.is_superadmin:
            if resource_type == "data":
                return ["bronze", "silver", "gold"]
            return ["all"]

        permissions = self.get_user_permissions(user)
        accessible = []

        for perm in permissions:
            if perm.startswith(f"{resource_type}:"):
                parts = perm.split(":")
                if len(parts) >= 2:
                    accessible.append(parts[-1])

        return list(set(accessible))

    def list_roles(self) -> list[Role]:
        """List all roles."""
        return list(self.roles.values())

    def list_permissions(self) -> list[Permission]:
        """List all permissions."""
        return list(self.permissions.values())

    def get_permissions_by_category(self) -> dict[str, list[Permission]]:
        """Get permissions grouped by category."""
        by_category: dict[str, list[Permission]] = {}

        for perm in self.permissions.values():
            if perm.category not in by_category:
                by_category[perm.category] = []
            by_category[perm.category].append(perm)

        return by_category


# Global RBAC manager instance
_rbac_manager: RBACManager | None = None


def get_rbac_manager() -> RBACManager:
    """Get or create global RBAC manager."""
    global _rbac_manager
    if _rbac_manager is None:
        _rbac_manager = RBACManager()
    return _rbac_manager


def require_permission(permission: str | PermissionType):
    """
    Decorator to require a permission for a function.

    Usage:
        @require_permission(PermissionType.PIPELINE_CREATE)
        def create_pipeline(user, ...):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get user from args or kwargs
            user = kwargs.get('user') or (args[0] if args else None)

            if not isinstance(user, User):
                raise AccessDeniedError("User not provided")

            rbac = get_rbac_manager()
            if not rbac.check_permission(user, permission):
                perm_name = permission.value if isinstance(permission, PermissionType) else permission
                raise AccessDeniedError(f"Permission denied: {perm_name}")

            return func(*args, **kwargs)
        return wrapper
    return decorator


def require_role(role_type: RoleType):
    """
    Decorator to require a role for a function.

    Usage:
        @require_role(RoleType.ADMIN)
        def admin_function(user, ...):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            user = kwargs.get('user') or (args[0] if args else None)

            if not isinstance(user, User):
                raise AccessDeniedError("User not provided")

            rbac = get_rbac_manager()
            if not rbac.check_role(user, role_type):
                raise AccessDeniedError(f"Role required: {role_type.value}")

            return func(*args, **kwargs)
        return wrapper
    return decorator


def require_superadmin(func: Callable) -> Callable:
    """Decorator to require superadmin access."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        user = kwargs.get('user') or (args[0] if args else None)

        if not isinstance(user, User):
            raise AccessDeniedError("User not provided")

        if not user.is_superadmin:
            raise AccessDeniedError("Superadmin access required")

        return func(*args, **kwargs)
    return wrapper
