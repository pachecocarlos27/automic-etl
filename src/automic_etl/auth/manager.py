"""Authentication manager for Automic ETL."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
import json
from pathlib import Path

import structlog

from automic_etl.auth.models import (
    User,
    Role,
    AuditLog,
    AuditAction,
    UserStatus,
    RoleType,
    DEFAULT_ROLES,
)
from automic_etl.auth.session import SessionManager, Session
from automic_etl.auth.rbac import RBACManager

logger = structlog.get_logger()


class AuthenticationError(Exception):
    """Authentication error."""
    pass


class AuthManager:
    """
    Central authentication manager.

    Features:
    - User registration and authentication
    - Session management
    - Password policies
    - Account lockout
    - Audit logging
    """

    def __init__(
        self,
        data_dir: str | Path | None = None,
        session_duration_hours: int = 24,
        max_failed_attempts: int = 5,
        lockout_minutes: int = 30,
        password_min_length: int = 8,
    ) -> None:
        """
        Initialize auth manager.

        Args:
            data_dir: Directory for storing user data
            session_duration_hours: Session duration
            max_failed_attempts: Max failed login attempts before lockout
            lockout_minutes: Lockout duration
            password_min_length: Minimum password length
        """
        self.data_dir = Path(data_dir) if data_dir else Path.home() / ".automic_etl"
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.session_manager = SessionManager(
            session_duration_hours=session_duration_hours,
        )
        self.rbac_manager = RBACManager()

        self.max_failed_attempts = max_failed_attempts
        self.lockout_minutes = lockout_minutes
        self.password_min_length = password_min_length

        self.users: dict[str, User] = {}
        self.audit_logs: list[AuditLog] = []
        self.logger = logger.bind(component="auth_manager")

        # Load existing data
        self._load_data()

        # Ensure superadmin exists
        self._ensure_superadmin()

    def _load_data(self) -> None:
        """Load user data from disk."""
        users_file = self.data_dir / "users.json"
        if users_file.exists():
            try:
                with open(users_file, "r") as f:
                    data = json.load(f)
                    for user_data in data:
                        user = self._user_from_dict(user_data)
                        self.users[user.user_id] = user
                self.logger.info("Loaded users", count=len(self.users))
            except Exception as e:
                self.logger.error("Failed to load users", error=str(e))

    def _save_data(self) -> None:
        """Save user data to disk."""
        users_file = self.data_dir / "users.json"
        try:
            data = [self._user_to_dict(u) for u in self.users.values()]
            with open(users_file, "w") as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            self.logger.error("Failed to save users", error=str(e))

    def _user_to_dict(self, user: User) -> dict[str, Any]:
        """Convert user to dictionary for storage."""
        return {
            "user_id": user.user_id,
            "username": user.username,
            "email": user.email,
            "password_hash": user.password_hash,
            "salt": user.salt,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "status": user.status.value,
            "roles": user.roles,
            "is_superadmin": user.is_superadmin,
            "created_at": user.created_at.isoformat(),
            "updated_at": user.updated_at.isoformat(),
            "last_login": user.last_login.isoformat() if user.last_login else None,
            "failed_login_attempts": user.failed_login_attempts,
            "locked_until": user.locked_until.isoformat() if user.locked_until else None,
            "mfa_enabled": user.mfa_enabled,
            "mfa_secret": user.mfa_secret,
            "settings": user.settings,
            "metadata": user.metadata,
        }

    def _user_from_dict(self, data: dict[str, Any]) -> User:
        """Create user from dictionary."""
        return User(
            user_id=data["user_id"],
            username=data["username"],
            email=data["email"],
            password_hash=data["password_hash"],
            salt=data["salt"],
            first_name=data.get("first_name", ""),
            last_name=data.get("last_name", ""),
            status=UserStatus(data.get("status", "pending")),
            roles=data.get("roles", []),
            is_superadmin=data.get("is_superadmin", False),
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            last_login=datetime.fromisoformat(data["last_login"]) if data.get("last_login") else None,
            failed_login_attempts=data.get("failed_login_attempts", 0),
            locked_until=datetime.fromisoformat(data["locked_until"]) if data.get("locked_until") else None,
            mfa_enabled=data.get("mfa_enabled", False),
            mfa_secret=data.get("mfa_secret"),
            settings=data.get("settings", {}),
            metadata=data.get("metadata", {}),
        )

    def _ensure_superadmin(self) -> None:
        """Ensure a superadmin account exists."""
        superadmins = [u for u in self.users.values() if u.is_superadmin]

        if not superadmins:
            # Create default superadmin
            user = User.create(
                username="admin",
                email="admin@localhost",
                password="admin123",  # Should be changed on first login
                first_name="Super",
                last_name="Admin",
                is_superadmin=True,
            )
            user.status = UserStatus.ACTIVE
            user.roles = ["superadmin"]
            user.metadata["force_password_change"] = True

            self.users[user.user_id] = user
            self._save_data()

            self.logger.warning(
                "Created default superadmin account",
                username="admin",
                note="Change password immediately!",
            )

    def register(
        self,
        username: str,
        email: str,
        password: str,
        first_name: str = "",
        last_name: str = "",
        auto_activate: bool = False,
    ) -> User:
        """
        Register a new user.

        Args:
            username: Username
            email: Email address
            password: Password
            first_name: First name
            last_name: Last name
            auto_activate: Automatically activate account

        Returns:
            Created user

        Raises:
            AuthenticationError: If registration fails
        """
        # Validate username
        if self.get_user_by_username(username):
            raise AuthenticationError("Username already exists")

        # Validate email
        if self.get_user_by_email(email):
            raise AuthenticationError("Email already registered")

        # Validate password
        self._validate_password(password)

        # Create user
        user = User.create(
            username=username,
            email=email,
            password=password,
            first_name=first_name,
            last_name=last_name,
        )

        if auto_activate:
            user.status = UserStatus.ACTIVE

        # Assign default role
        user.roles = ["viewer"]

        self.users[user.user_id] = user
        self._save_data()

        # Audit log
        self._log_action(
            AuditAction.USER_CREATED,
            user_id=user.user_id,
            username=user.username,
            resource_type="user",
            resource_id=user.user_id,
        )

        self.logger.info("User registered", username=username)
        return user

    def authenticate(
        self,
        username: str,
        password: str,
        ip_address: str | None = None,
        user_agent: str | None = None,
    ) -> tuple[User, str, Session]:
        """
        Authenticate a user.

        Args:
            username: Username or email
            password: Password
            ip_address: Client IP
            user_agent: Client user agent

        Returns:
            Tuple of (user, token, session)

        Raises:
            AuthenticationError: If authentication fails
        """
        # Find user
        user = self.get_user_by_username(username) or self.get_user_by_email(username)

        if not user:
            self._log_action(
                AuditAction.LOGIN_FAILED,
                username=username,
                ip_address=ip_address,
                details={"reason": "user_not_found"},
                success=False,
            )
            raise AuthenticationError("Invalid credentials")

        # Check account status
        if user.status == UserStatus.SUSPENDED:
            raise AuthenticationError("Account suspended")

        if user.status == UserStatus.INACTIVE:
            raise AuthenticationError("Account inactive")

        if user.status == UserStatus.PENDING:
            raise AuthenticationError("Account pending activation")

        # Check lockout
        if user.is_locked:
            raise AuthenticationError("Account locked. Try again later.")

        # Verify password
        if not user.verify_password(password):
            user.record_login(success=False)

            # Check if should lock
            if user.failed_login_attempts >= self.max_failed_attempts:
                user.locked_until = datetime.utcnow() + timedelta(minutes=self.lockout_minutes)

            self._save_data()

            self._log_action(
                AuditAction.LOGIN_FAILED,
                user_id=user.user_id,
                username=user.username,
                ip_address=ip_address,
                details={"attempts": user.failed_login_attempts},
                success=False,
            )

            raise AuthenticationError("Invalid credentials")

        # Successful login
        user.record_login(success=True)
        self._save_data()

        # Create session
        token, session = self.session_manager.create_session(
            user_id=user.user_id,
            ip_address=ip_address,
            user_agent=user_agent,
        )

        self._log_action(
            AuditAction.LOGIN,
            user_id=user.user_id,
            username=user.username,
            ip_address=ip_address,
        )

        self.logger.info("User authenticated", username=user.username)
        return user, token, session

    def logout(self, token: str) -> bool:
        """
        Logout a user.

        Args:
            token: Session token

        Returns:
            True if logout successful
        """
        session = self.session_manager.validate_token(token)
        if not session:
            return False

        user = self.users.get(session.user_id)

        self.session_manager.invalidate_session(session.session_id)

        if user:
            self._log_action(
                AuditAction.LOGOUT,
                user_id=user.user_id,
                username=user.username,
            )

        return True

    def validate_session(self, token: str) -> tuple[User, Session] | None:
        """
        Validate a session token.

        Args:
            token: Session token

        Returns:
            Tuple of (user, session) if valid, None otherwise
        """
        session = self.session_manager.validate_token(token)
        if not session:
            return None

        user = self.users.get(session.user_id)
        if not user or user.status != UserStatus.ACTIVE:
            self.session_manager.invalidate_session(session.session_id)
            return None

        return user, session

    def change_password(
        self,
        user_id: str,
        current_password: str,
        new_password: str,
    ) -> bool:
        """Change a user's password."""
        user = self.users.get(user_id)
        if not user:
            return False

        if not user.verify_password(current_password):
            raise AuthenticationError("Current password incorrect")

        self._validate_password(new_password)

        user.set_password(new_password)
        user.metadata.pop("force_password_change", None)
        self._save_data()

        # Invalidate all other sessions
        self.session_manager.invalidate_user_sessions(user_id)

        self._log_action(
            AuditAction.PASSWORD_CHANGE,
            user_id=user.user_id,
            username=user.username,
        )

        return True

    def reset_password(
        self,
        user_id: str,
        new_password: str,
        admin_user_id: str | None = None,
    ) -> bool:
        """Reset a user's password (admin action)."""
        user = self.users.get(user_id)
        if not user:
            return False

        self._validate_password(new_password)

        user.set_password(new_password)
        user.metadata["force_password_change"] = True
        user.failed_login_attempts = 0
        user.locked_until = None
        self._save_data()

        # Invalidate all sessions
        self.session_manager.invalidate_user_sessions(user_id)

        self._log_action(
            AuditAction.PASSWORD_RESET,
            user_id=admin_user_id,
            resource_type="user",
            resource_id=user_id,
            details={"target_user": user.username},
        )

        return True

    def _validate_password(self, password: str) -> None:
        """Validate password meets requirements."""
        if len(password) < self.password_min_length:
            raise AuthenticationError(
                f"Password must be at least {self.password_min_length} characters"
            )

        # Check for complexity
        has_upper = any(c.isupper() for c in password)
        has_lower = any(c.islower() for c in password)
        has_digit = any(c.isdigit() for c in password)

        if not (has_upper and has_lower and has_digit):
            raise AuthenticationError(
                "Password must contain uppercase, lowercase, and numbers"
            )

    # User management methods

    def get_user(self, user_id: str) -> User | None:
        """Get user by ID."""
        return self.users.get(user_id)

    def get_user_by_username(self, username: str) -> User | None:
        """Get user by username."""
        for user in self.users.values():
            if user.username.lower() == username.lower():
                return user
        return None

    def get_user_by_email(self, email: str) -> User | None:
        """Get user by email."""
        for user in self.users.values():
            if user.email.lower() == email.lower():
                return user
        return None

    def list_users(
        self,
        status: UserStatus | None = None,
        role_id: str | None = None,
    ) -> list[User]:
        """List users with optional filters."""
        users = list(self.users.values())

        if status:
            users = [u for u in users if u.status == status]

        if role_id:
            users = [u for u in users if role_id in u.roles]

        return users

    def update_user(
        self,
        user_id: str,
        first_name: str | None = None,
        last_name: str | None = None,
        email: str | None = None,
        status: UserStatus | None = None,
        settings: dict[str, Any] | None = None,
    ) -> User | None:
        """Update user details."""
        user = self.users.get(user_id)
        if not user:
            return None

        if first_name is not None:
            user.first_name = first_name
        if last_name is not None:
            user.last_name = last_name
        if email is not None:
            # Check email not taken
            existing = self.get_user_by_email(email)
            if existing and existing.user_id != user_id:
                raise AuthenticationError("Email already in use")
            user.email = email
        if status is not None:
            user.status = status
        if settings is not None:
            user.settings.update(settings)

        user.updated_at = datetime.utcnow()
        self._save_data()

        self._log_action(
            AuditAction.USER_UPDATED,
            resource_type="user",
            resource_id=user_id,
        )

        return user

    def delete_user(self, user_id: str, admin_user_id: str | None = None) -> bool:
        """Delete a user."""
        user = self.users.get(user_id)
        if not user:
            return False

        if user.is_superadmin:
            # Check if this is the last superadmin
            superadmins = [u for u in self.users.values() if u.is_superadmin]
            if len(superadmins) <= 1:
                raise AuthenticationError("Cannot delete the last superadmin")

        # Invalidate sessions
        self.session_manager.invalidate_user_sessions(user_id)

        del self.users[user_id]
        self._save_data()

        self._log_action(
            AuditAction.USER_DELETED,
            user_id=admin_user_id,
            resource_type="user",
            resource_id=user_id,
            details={"deleted_user": user.username},
        )

        return True

    def suspend_user(self, user_id: str, admin_user_id: str | None = None) -> bool:
        """Suspend a user account."""
        user = self.users.get(user_id)
        if not user:
            return False

        if user.is_superadmin:
            raise AuthenticationError("Cannot suspend superadmin")

        user.status = UserStatus.SUSPENDED
        self._save_data()

        self.session_manager.invalidate_user_sessions(user_id)

        self._log_action(
            AuditAction.USER_SUSPENDED,
            user_id=admin_user_id,
            resource_type="user",
            resource_id=user_id,
        )

        return True

    def activate_user(self, user_id: str, admin_user_id: str | None = None) -> bool:
        """Activate a user account."""
        user = self.users.get(user_id)
        if not user:
            return False

        user.status = UserStatus.ACTIVE
        self._save_data()

        self._log_action(
            AuditAction.USER_ACTIVATED,
            user_id=admin_user_id,
            resource_type="user",
            resource_id=user_id,
        )

        return True

    # Role management

    def assign_role(
        self,
        user_id: str,
        role_id: str,
        admin_user_id: str | None = None,
    ) -> bool:
        """Assign a role to a user."""
        user = self.users.get(user_id)
        if not user:
            return False

        role = self.rbac_manager.get_role(role_id)
        if not role:
            return False

        # Only superadmin can assign superadmin role
        if role.role_type == RoleType.SUPERADMIN:
            admin = self.users.get(admin_user_id) if admin_user_id else None
            if not admin or not admin.is_superadmin:
                raise AuthenticationError("Only superadmin can assign superadmin role")

        user.add_role(role_id)
        self._save_data()

        self._log_action(
            AuditAction.ROLE_ASSIGNED,
            user_id=admin_user_id,
            resource_type="user",
            resource_id=user_id,
            details={"role": role_id},
        )

        return True

    def remove_role(
        self,
        user_id: str,
        role_id: str,
        admin_user_id: str | None = None,
    ) -> bool:
        """Remove a role from a user."""
        user = self.users.get(user_id)
        if not user:
            return False

        user.remove_role(role_id)
        self._save_data()

        self._log_action(
            AuditAction.ROLE_REMOVED,
            user_id=admin_user_id,
            resource_type="user",
            resource_id=user_id,
            details={"role": role_id},
        )

        return True

    # Audit logging

    def _log_action(
        self,
        action: AuditAction,
        user_id: str | None = None,
        username: str | None = None,
        resource_type: str | None = None,
        resource_id: str | None = None,
        details: dict[str, Any] | None = None,
        ip_address: str | None = None,
        user_agent: str | None = None,
        success: bool = True,
    ) -> None:
        """Log an audit action."""
        log = AuditLog.create(
            action=action,
            user_id=user_id,
            username=username,
            resource_type=resource_type,
            resource_id=resource_id,
            details=details,
            ip_address=ip_address,
            user_agent=user_agent,
            success=success,
        )
        self.audit_logs.append(log)

        # Keep only last 10000 logs in memory
        if len(self.audit_logs) > 10000:
            self.audit_logs = self.audit_logs[-10000:]

    def get_audit_logs(
        self,
        user_id: str | None = None,
        action: AuditAction | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 100,
    ) -> list[AuditLog]:
        """Get audit logs with filters."""
        logs = self.audit_logs.copy()

        if user_id:
            logs = [l for l in logs if l.user_id == user_id]

        if action:
            logs = [l for l in logs if l.action == action]

        if start_time:
            logs = [l for l in logs if l.timestamp >= start_time]

        if end_time:
            logs = [l for l in logs if l.timestamp <= end_time]

        # Sort by timestamp descending
        logs.sort(key=lambda x: x.timestamp, reverse=True)

        return logs[:limit]

    def get_statistics(self) -> dict[str, Any]:
        """Get authentication statistics."""
        active_users = len([u for u in self.users.values() if u.status == UserStatus.ACTIVE])
        pending_users = len([u for u in self.users.values() if u.status == UserStatus.PENDING])
        suspended_users = len([u for u in self.users.values() if u.status == UserStatus.SUSPENDED])

        session_stats = self.session_manager.get_statistics()

        return {
            "total_users": len(self.users),
            "active_users": active_users,
            "pending_users": pending_users,
            "suspended_users": suspended_users,
            "superadmins": len([u for u in self.users.values() if u.is_superadmin]),
            **session_stats,
            "audit_log_count": len(self.audit_logs),
        }
