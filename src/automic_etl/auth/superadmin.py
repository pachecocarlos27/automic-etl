"""Superadmin controls and system-wide management for Automic ETL."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any
from functools import wraps
import json
from pathlib import Path

import structlog

from automic_etl.core.utils import utc_now
from automic_etl.auth.models import User, AuditLog, AuditAction, UserStatus
from automic_etl.auth.tenant import (
    Company,
    CompanyStatus,
    CompanyTier,
    CompanyMembership,
)

logger = structlog.get_logger()


class SuperadminError(Exception):
    """Superadmin operation error."""
    pass


def require_superadmin(func):
    """Decorator to require superadmin access for a function."""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        user = kwargs.get('admin_user') or kwargs.get('user')
        if not user:
            raise SuperadminError("User not provided")
        if not isinstance(user, User):
            raise SuperadminError("Invalid user object")
        if not user.is_superadmin:
            raise SuperadminError("Superadmin access required")
        return func(self, *args, **kwargs)
    return wrapper


@dataclass
class GlobalSettings:
    """System-wide global settings."""
    # Platform settings
    platform_name: str = "Automic ETL"
    platform_url: str = "http://localhost:8501"
    support_email: str = "support@automic-etl.com"

    # Registration settings
    allow_self_registration: bool = True
    require_email_verification: bool = True
    auto_approve_companies: bool = False
    default_company_tier: str = "free"

    # Security settings
    password_min_length: int = 8
    require_mfa_for_admins: bool = False
    session_timeout_minutes: int = 480
    max_failed_login_attempts: int = 5
    lockout_duration_minutes: int = 30

    # Feature flags
    enable_llm_features: bool = True
    enable_streaming_connectors: bool = True
    enable_spark_integration: bool = True
    enable_api_access: bool = True

    # Limits (platform-wide defaults)
    max_companies: int = 1000
    max_users_total: int = 10000
    default_api_rate_limit: int = 1000

    # Maintenance
    maintenance_mode: bool = False
    maintenance_message: str = ""
    maintenance_allowed_ips: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "platform_name": self.platform_name,
            "platform_url": self.platform_url,
            "support_email": self.support_email,
            "allow_self_registration": self.allow_self_registration,
            "require_email_verification": self.require_email_verification,
            "auto_approve_companies": self.auto_approve_companies,
            "default_company_tier": self.default_company_tier,
            "password_min_length": self.password_min_length,
            "require_mfa_for_admins": self.require_mfa_for_admins,
            "session_timeout_minutes": self.session_timeout_minutes,
            "max_failed_login_attempts": self.max_failed_login_attempts,
            "lockout_duration_minutes": self.lockout_duration_minutes,
            "enable_llm_features": self.enable_llm_features,
            "enable_streaming_connectors": self.enable_streaming_connectors,
            "enable_spark_integration": self.enable_spark_integration,
            "enable_api_access": self.enable_api_access,
            "max_companies": self.max_companies,
            "max_users_total": self.max_users_total,
            "default_api_rate_limit": self.default_api_rate_limit,
            "maintenance_mode": self.maintenance_mode,
            "maintenance_message": self.maintenance_message,
            "maintenance_allowed_ips": self.maintenance_allowed_ips,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GlobalSettings":
        """Create from dictionary."""
        return cls(
            platform_name=data.get("platform_name", "Automic ETL"),
            platform_url=data.get("platform_url", "http://localhost:8501"),
            support_email=data.get("support_email", "support@automic-etl.com"),
            allow_self_registration=data.get("allow_self_registration", True),
            require_email_verification=data.get("require_email_verification", True),
            auto_approve_companies=data.get("auto_approve_companies", False),
            default_company_tier=data.get("default_company_tier", "free"),
            password_min_length=data.get("password_min_length", 8),
            require_mfa_for_admins=data.get("require_mfa_for_admins", False),
            session_timeout_minutes=data.get("session_timeout_minutes", 480),
            max_failed_login_attempts=data.get("max_failed_login_attempts", 5),
            lockout_duration_minutes=data.get("lockout_duration_minutes", 30),
            enable_llm_features=data.get("enable_llm_features", True),
            enable_streaming_connectors=data.get("enable_streaming_connectors", True),
            enable_spark_integration=data.get("enable_spark_integration", True),
            enable_api_access=data.get("enable_api_access", True),
            max_companies=data.get("max_companies", 1000),
            max_users_total=data.get("max_users_total", 10000),
            default_api_rate_limit=data.get("default_api_rate_limit", 1000),
            maintenance_mode=data.get("maintenance_mode", False),
            maintenance_message=data.get("maintenance_message", ""),
            maintenance_allowed_ips=data.get("maintenance_allowed_ips", []),
        )


@dataclass
class SystemHealth:
    """System health status."""
    status: str = "healthy"  # healthy, degraded, unhealthy
    timestamp: datetime = field(default_factory=datetime.utcnow)
    components: dict[str, dict[str, Any]] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "status": self.status,
            "timestamp": self.timestamp.isoformat(),
            "components": self.components,
            "warnings": self.warnings,
            "errors": self.errors,
        }


class SuperadminController:
    """
    Superadmin controls for system-wide management.

    Features:
    - Global settings management
    - Company oversight and management
    - User management across companies
    - System health monitoring
    - Audit log access
    - Maintenance mode control
    """

    def __init__(
        self,
        data_dir: str | Path | None = None,
        auth_manager: Any = None,
        company_manager: Any = None,
    ) -> None:
        """
        Initialize superadmin controller.

        Args:
            data_dir: Directory for storing settings
            auth_manager: AuthManager instance
            company_manager: CompanyManager instance
        """
        self.data_dir = Path(data_dir) if data_dir else Path.home() / ".automic_etl"
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.auth_manager = auth_manager
        self.company_manager = company_manager

        self.global_settings = GlobalSettings()
        self.audit_logs: list[AuditLog] = []

        self.logger = logger.bind(component="superadmin")
        self._load_settings()

    def _load_settings(self) -> None:
        """Load global settings from disk."""
        settings_file = self.data_dir / "global_settings.json"
        if settings_file.exists():
            try:
                with open(settings_file, "r") as f:
                    data = json.load(f)
                    self.global_settings = GlobalSettings.from_dict(data)
                self.logger.info("Loaded global settings")
            except Exception as e:
                self.logger.error("Failed to load global settings", error=str(e))

    def _save_settings(self) -> None:
        """Save global settings to disk."""
        settings_file = self.data_dir / "global_settings.json"
        try:
            with open(settings_file, "w") as f:
                json.dump(self.global_settings.to_dict(), f, indent=2)
        except Exception as e:
            self.logger.error("Failed to save global settings", error=str(e))

    # Global settings management

    def get_global_settings(self) -> GlobalSettings:
        """Get current global settings."""
        return self.global_settings

    @require_superadmin
    def update_global_settings(
        self,
        settings: dict[str, Any],
        admin_user: User,
    ) -> GlobalSettings:
        """
        Update global settings.

        Args:
            settings: Settings to update
            admin_user: Superadmin user making the change
        """
        old_settings = self.global_settings.to_dict()

        for key, value in settings.items():
            if hasattr(self.global_settings, key):
                setattr(self.global_settings, key, value)

        self._save_settings()

        self._log_action(
            AuditAction.SYSTEM_CONFIG_CHANGED,
            user_id=admin_user.user_id,
            username=admin_user.username,
            resource_type="global_settings",
            details={"updated_keys": list(settings.keys())},
        )

        self.logger.info(
            "Global settings updated",
            by_user=admin_user.username,
            updated_keys=list(settings.keys()),
        )

        return self.global_settings

    # Maintenance mode

    @require_superadmin
    def enable_maintenance_mode(
        self,
        message: str,
        allowed_ips: list[str] | None = None,
        admin_user: User = None,
    ) -> None:
        """Enable maintenance mode."""
        self.global_settings.maintenance_mode = True
        self.global_settings.maintenance_message = message
        if allowed_ips:
            self.global_settings.maintenance_allowed_ips = allowed_ips
        self._save_settings()

        self._log_action(
            AuditAction.SYSTEM_CONFIG_CHANGED,
            user_id=admin_user.user_id,
            username=admin_user.username,
            resource_type="maintenance_mode",
            details={"enabled": True, "message": message},
        )

        self.logger.warning(
            "Maintenance mode enabled",
            by_user=admin_user.username,
            message=message,
        )

    @require_superadmin
    def disable_maintenance_mode(self, admin_user: User) -> None:
        """Disable maintenance mode."""
        self.global_settings.maintenance_mode = False
        self.global_settings.maintenance_message = ""
        self.global_settings.maintenance_allowed_ips = []
        self._save_settings()

        self._log_action(
            AuditAction.SYSTEM_CONFIG_CHANGED,
            user_id=admin_user.user_id,
            username=admin_user.username,
            resource_type="maintenance_mode",
            details={"enabled": False},
        )

        self.logger.info("Maintenance mode disabled", by_user=admin_user.username)

    def is_maintenance_mode(self, client_ip: str | None = None) -> tuple[bool, str]:
        """
        Check if maintenance mode is active.

        Returns:
            Tuple of (is_in_maintenance, message)
        """
        if not self.global_settings.maintenance_mode:
            return False, ""

        # Check if IP is allowed
        if client_ip and client_ip in self.global_settings.maintenance_allowed_ips:
            return False, ""

        return True, self.global_settings.maintenance_message

    # Company management

    @require_superadmin
    def list_all_companies(
        self,
        admin_user: User,
        status: CompanyStatus | None = None,
        tier: CompanyTier | None = None,
        search: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[Company], int]:
        """List all companies with filters."""
        if not self.company_manager:
            return [], 0

        return self.company_manager.list_companies(
            status=status,
            tier=tier,
            search=search,
            limit=limit,
            offset=offset,
        )

    @require_superadmin
    def approve_company(
        self,
        company_id: str,
        admin_user: User,
    ) -> Company | None:
        """Approve a pending company."""
        if not self.company_manager:
            return None

        company = self.company_manager.update_company_status(
            company_id=company_id,
            status=CompanyStatus.ACTIVE,
            reason="Approved by superadmin",
            updated_by_user_id=admin_user.user_id,
        )

        if company:
            self._log_action(
                AuditAction.USER_ACTIVATED,
                user_id=admin_user.user_id,
                username=admin_user.username,
                resource_type="company",
                resource_id=company_id,
                details={"action": "approved"},
            )

        return company

    @require_superadmin
    def suspend_company(
        self,
        company_id: str,
        reason: str,
        admin_user: User,
    ) -> Company | None:
        """Suspend a company."""
        if not self.company_manager:
            return None

        company = self.company_manager.update_company_status(
            company_id=company_id,
            status=CompanyStatus.SUSPENDED,
            reason=reason,
            updated_by_user_id=admin_user.user_id,
        )

        if company:
            self._log_action(
                AuditAction.USER_SUSPENDED,
                user_id=admin_user.user_id,
                username=admin_user.username,
                resource_type="company",
                resource_id=company_id,
                details={"reason": reason},
            )

        return company

    @require_superadmin
    def update_company_tier(
        self,
        company_id: str,
        tier: CompanyTier,
        custom_limits: dict[str, Any] | None = None,
        admin_user: User = None,
    ) -> Company | None:
        """Update company subscription tier."""
        if not self.company_manager:
            return None

        return self.company_manager.update_company_tier(
            company_id=company_id,
            tier=tier,
            custom_limits=custom_limits,
            updated_by_user_id=admin_user.user_id,
        )

    @require_superadmin
    def delete_company(
        self,
        company_id: str,
        hard_delete: bool = False,
        admin_user: User = None,
    ) -> bool:
        """Delete a company."""
        if not self.company_manager:
            return False

        return self.company_manager.delete_company(
            company_id=company_id,
            deleted_by_user_id=admin_user.user_id,
            hard_delete=hard_delete,
        )

    # User management across companies

    @require_superadmin
    def list_all_users(
        self,
        admin_user: User,
        company_id: str | None = None,
        status: UserStatus | None = None,
        search: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        """List all users across companies."""
        if not self.auth_manager:
            return [], 0

        users = self.auth_manager.list_users(status=status)

        # Filter by company if specified
        if company_id and self.company_manager:
            memberships = self.company_manager.get_company_members(company_id)
            member_user_ids = {m.user_id for m in memberships}
            users = [u for u in users if u.user_id in member_user_ids]

        # Search filter
        if search:
            search_lower = search.lower()
            users = [
                u for u in users
                if search_lower in u.username.lower()
                or search_lower in u.email.lower()
                or search_lower in u.full_name.lower()
            ]

        total = len(users)
        users = users[offset:offset + limit]

        # Add company info to each user
        result = []
        for user in users:
            user_dict = user.to_dict()
            if self.company_manager:
                memberships = self.company_manager.get_user_memberships(user.user_id)
                user_dict["companies"] = [
                    {
                        "company_id": m.company_id,
                        "is_admin": m.is_company_admin,
                        "is_owner": m.is_owner,
                    }
                    for m in memberships
                ]
            result.append(user_dict)

        return result, total

    @require_superadmin
    def create_superadmin(
        self,
        username: str,
        email: str,
        password: str,
        first_name: str = "",
        last_name: str = "",
        admin_user: User = None,
    ) -> User:
        """Create a new superadmin user."""
        if not self.auth_manager:
            raise SuperadminError("Auth manager not available")

        # Create user
        user = self.auth_manager.register(
            username=username,
            email=email,
            password=password,
            first_name=first_name,
            last_name=last_name,
            auto_activate=True,
        )

        # Make superadmin
        user.is_superadmin = True
        user.roles = ["superadmin"]
        user.status = UserStatus.ACTIVE
        self.auth_manager._save_data()

        self._log_action(
            AuditAction.USER_CREATED,
            user_id=admin_user.user_id,
            username=admin_user.username,
            resource_type="superadmin",
            resource_id=user.user_id,
            details={"new_superadmin": username},
        )

        self.logger.warning(
            "New superadmin created",
            by_user=admin_user.username,
            new_superadmin=username,
        )

        return user

    @require_superadmin
    def revoke_superadmin(
        self,
        user_id: str,
        admin_user: User,
    ) -> bool:
        """Revoke superadmin status from a user."""
        if not self.auth_manager:
            return False

        user = self.auth_manager.get_user(user_id)
        if not user:
            return False

        if user.user_id == admin_user.user_id:
            raise SuperadminError("Cannot revoke your own superadmin status")

        # Check not the last superadmin
        superadmins = [u for u in self.auth_manager.users.values() if u.is_superadmin]
        if len(superadmins) <= 1:
            raise SuperadminError("Cannot revoke the last superadmin")

        user.is_superadmin = False
        if "superadmin" in user.roles:
            user.roles.remove("superadmin")
        self.auth_manager._save_data()

        self._log_action(
            AuditAction.USER_UPDATED,
            user_id=admin_user.user_id,
            username=admin_user.username,
            resource_type="superadmin",
            resource_id=user_id,
            details={"action": "revoked_superadmin", "target_user": user.username},
        )

        return True

    @require_superadmin
    def impersonate_user(
        self,
        target_user_id: str,
        admin_user: User,
    ) -> tuple[str, Any] | None:
        """
        Create an impersonation session for a user.

        Returns:
            Tuple of (token, session) for the impersonated user
        """
        if not self.auth_manager:
            return None

        target_user = self.auth_manager.get_user(target_user_id)
        if not target_user:
            return None

        if target_user.is_superadmin:
            raise SuperadminError("Cannot impersonate another superadmin")

        # Create impersonation session
        token, session = self.auth_manager.session_manager.create_session(
            user_id=target_user_id,
            metadata={
                "impersonated_by": admin_user.user_id,
                "impersonated_by_username": admin_user.username,
                "is_impersonation": True,
            }
        )

        self._log_action(
            AuditAction.LOGIN,
            user_id=admin_user.user_id,
            username=admin_user.username,
            resource_type="impersonation",
            resource_id=target_user_id,
            details={"target_user": target_user.username},
        )

        self.logger.warning(
            "User impersonation started",
            admin=admin_user.username,
            target=target_user.username,
        )

        return token, session

    # System health

    def get_system_health(self) -> SystemHealth:
        """Get system health status."""
        health = SystemHealth()

        # Check auth manager
        if self.auth_manager:
            try:
                stats = self.auth_manager.get_statistics()
                health.components["auth"] = {
                    "status": "healthy",
                    "total_users": stats.get("total_users", 0),
                    "active_sessions": stats.get("active_sessions", 0),
                }
            except Exception as e:
                health.components["auth"] = {"status": "error", "error": str(e)}
                health.errors.append(f"Auth manager: {e}")

        # Check company manager
        if self.company_manager:
            try:
                stats = self.company_manager.get_statistics()
                health.components["companies"] = {
                    "status": "healthy",
                    "total_companies": stats.get("total_companies", 0),
                    "total_users": stats.get("total_users", 0),
                }
            except Exception as e:
                health.components["companies"] = {"status": "error", "error": str(e)}
                health.errors.append(f"Company manager: {e}")

        # Check maintenance mode
        if self.global_settings.maintenance_mode:
            health.warnings.append("System is in maintenance mode")

        # Determine overall status
        if health.errors:
            health.status = "unhealthy"
        elif health.warnings:
            health.status = "degraded"
        else:
            health.status = "healthy"

        return health

    def get_platform_statistics(self) -> dict[str, Any]:
        """Get comprehensive platform statistics."""
        stats = {
            "timestamp": utc_now().isoformat(),
            "platform_name": self.global_settings.platform_name,
            "maintenance_mode": self.global_settings.maintenance_mode,
        }

        if self.auth_manager:
            auth_stats = self.auth_manager.get_statistics()
            stats["auth"] = auth_stats

        if self.company_manager:
            company_stats = self.company_manager.get_statistics()
            stats["companies"] = company_stats

        stats["audit_logs"] = {
            "total": len(self.audit_logs),
            "recent_24h": len([
                log for log in self.audit_logs
                if log.timestamp > utc_now() - timedelta(hours=24)
            ]),
        }

        return stats

    # Audit logs

    @require_superadmin
    def get_audit_logs(
        self,
        admin_user: User,
        user_id: str | None = None,
        company_id: str | None = None,
        action: AuditAction | None = None,
        resource_type: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        """Get audit logs with filters."""
        # Combine logs from all sources
        logs = list(self.audit_logs)

        if self.auth_manager:
            logs.extend(self.auth_manager.audit_logs)

        if self.company_manager:
            logs.extend(self.company_manager.audit_logs)

        # Apply filters
        if user_id:
            logs = [l for l in logs if l.user_id == user_id]

        if action:
            logs = [l for l in logs if l.action == action]

        if resource_type:
            logs = [l for l in logs if l.resource_type == resource_type]

        if start_time:
            logs = [l for l in logs if l.timestamp >= start_time]

        if end_time:
            logs = [l for l in logs if l.timestamp <= end_time]

        # Sort by timestamp descending
        logs.sort(key=lambda x: x.timestamp, reverse=True)

        total = len(logs)
        logs = logs[offset:offset + limit]

        return [log.to_dict() for log in logs], total

    def _log_action(
        self,
        action: AuditAction,
        user_id: str | None = None,
        username: str | None = None,
        resource_type: str | None = None,
        resource_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Log an audit action."""
        log = AuditLog.create(
            action=action,
            user_id=user_id,
            username=username,
            resource_type=resource_type,
            resource_id=resource_id,
            details=details,
        )
        self.audit_logs.append(log)

        if len(self.audit_logs) > 10000:
            self.audit_logs = self.audit_logs[-10000:]
