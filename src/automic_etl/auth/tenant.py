"""Multi-tenant models and management for Automic ETL."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any
import uuid


class CompanyStatus(Enum):
    """Company account status."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    SUSPENDED = "suspended"
    TRIAL = "trial"
    PENDING = "pending"


class CompanyTier(Enum):
    """Company subscription tiers."""
    FREE = "free"
    STARTER = "starter"
    PROFESSIONAL = "professional"
    ENTERPRISE = "enterprise"


class InvitationStatus(Enum):
    """User invitation status."""
    PENDING = "pending"
    ACCEPTED = "accepted"
    EXPIRED = "expired"
    REVOKED = "revoked"


@dataclass
class CompanyLimits:
    """Resource limits for a company based on tier."""
    max_users: int = 5
    max_pipelines: int = 10
    max_connectors: int = 5
    max_storage_gb: int = 10
    max_jobs_per_day: int = 100
    max_api_calls_per_day: int = 1000
    retention_days: int = 30
    enable_advanced_features: bool = False
    enable_sso: bool = False
    enable_audit_logs: bool = True
    enable_custom_roles: bool = False
    support_level: str = "community"

    @classmethod
    def for_tier(cls, tier: CompanyTier) -> "CompanyLimits":
        """Get default limits for a tier."""
        if tier == CompanyTier.FREE:
            return cls(
                max_users=3,
                max_pipelines=5,
                max_connectors=3,
                max_storage_gb=5,
                max_jobs_per_day=50,
                max_api_calls_per_day=500,
                retention_days=7,
                enable_advanced_features=False,
                enable_sso=False,
                enable_audit_logs=False,
                enable_custom_roles=False,
                support_level="community",
            )
        elif tier == CompanyTier.STARTER:
            return cls(
                max_users=10,
                max_pipelines=25,
                max_connectors=10,
                max_storage_gb=50,
                max_jobs_per_day=500,
                max_api_calls_per_day=5000,
                retention_days=30,
                enable_advanced_features=False,
                enable_sso=False,
                enable_audit_logs=True,
                enable_custom_roles=False,
                support_level="email",
            )
        elif tier == CompanyTier.PROFESSIONAL:
            return cls(
                max_users=50,
                max_pipelines=100,
                max_connectors=50,
                max_storage_gb=500,
                max_jobs_per_day=2000,
                max_api_calls_per_day=50000,
                retention_days=90,
                enable_advanced_features=True,
                enable_sso=True,
                enable_audit_logs=True,
                enable_custom_roles=True,
                support_level="priority",
            )
        else:  # ENTERPRISE
            return cls(
                max_users=999999,
                max_pipelines=999999,
                max_connectors=999999,
                max_storage_gb=999999,
                max_jobs_per_day=999999,
                max_api_calls_per_day=999999,
                retention_days=365,
                enable_advanced_features=True,
                enable_sso=True,
                enable_audit_logs=True,
                enable_custom_roles=True,
                support_level="dedicated",
            )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "max_users": self.max_users,
            "max_pipelines": self.max_pipelines,
            "max_connectors": self.max_connectors,
            "max_storage_gb": self.max_storage_gb,
            "max_jobs_per_day": self.max_jobs_per_day,
            "max_api_calls_per_day": self.max_api_calls_per_day,
            "retention_days": self.retention_days,
            "enable_advanced_features": self.enable_advanced_features,
            "enable_sso": self.enable_sso,
            "enable_audit_logs": self.enable_audit_logs,
            "enable_custom_roles": self.enable_custom_roles,
            "support_level": self.support_level,
        }


@dataclass
class CompanyUsage:
    """Current resource usage for a company."""
    user_count: int = 0
    pipeline_count: int = 0
    connector_count: int = 0
    storage_used_gb: float = 0.0
    jobs_today: int = 0
    api_calls_today: int = 0
    last_updated: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "user_count": self.user_count,
            "pipeline_count": self.pipeline_count,
            "connector_count": self.connector_count,
            "storage_used_gb": self.storage_used_gb,
            "jobs_today": self.jobs_today,
            "api_calls_today": self.api_calls_today,
            "last_updated": self.last_updated.isoformat(),
        }


@dataclass
class CompanySettings:
    """Company-specific settings."""
    # Branding
    logo_url: str | None = None
    primary_color: str = "#1E88E5"
    company_domain: str | None = None

    # Authentication
    require_mfa: bool = False
    allowed_email_domains: list[str] = field(default_factory=list)
    session_timeout_minutes: int = 480  # 8 hours
    password_policy: dict[str, Any] = field(default_factory=lambda: {
        "min_length": 8,
        "require_uppercase": True,
        "require_lowercase": True,
        "require_numbers": True,
        "require_special": False,
        "max_age_days": 90,
    })

    # Data settings
    default_storage_provider: str = "local"
    encryption_at_rest: bool = True
    data_region: str = "us-east-1"

    # Notifications
    notification_email: str | None = None
    slack_webhook: str | None = None

    # Features
    enabled_features: list[str] = field(default_factory=list)
    disabled_connectors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "logo_url": self.logo_url,
            "primary_color": self.primary_color,
            "company_domain": self.company_domain,
            "require_mfa": self.require_mfa,
            "allowed_email_domains": self.allowed_email_domains,
            "session_timeout_minutes": self.session_timeout_minutes,
            "password_policy": self.password_policy,
            "default_storage_provider": self.default_storage_provider,
            "encryption_at_rest": self.encryption_at_rest,
            "data_region": self.data_region,
            "notification_email": self.notification_email,
            "slack_webhook": self.slack_webhook,
            "enabled_features": self.enabled_features,
            "disabled_connectors": self.disabled_connectors,
        }


@dataclass
class Company:
    """A company/organization in the multi-tenant system."""
    company_id: str
    name: str
    slug: str  # URL-friendly identifier
    status: CompanyStatus = CompanyStatus.PENDING
    tier: CompanyTier = CompanyTier.FREE

    # Contact info
    owner_user_id: str | None = None
    billing_email: str | None = None
    support_email: str | None = None
    phone: str | None = None

    # Address
    address_line1: str | None = None
    address_line2: str | None = None
    city: str | None = None
    state: str | None = None
    postal_code: str | None = None
    country: str = "US"

    # Settings and limits
    limits: CompanyLimits = field(default_factory=CompanyLimits)
    settings: CompanySettings = field(default_factory=CompanySettings)
    usage: CompanyUsage = field(default_factory=CompanyUsage)

    # Metadata
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    trial_ends_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        name: str,
        slug: str | None = None,
        tier: CompanyTier = CompanyTier.FREE,
        owner_user_id: str | None = None,
        billing_email: str | None = None,
    ) -> "Company":
        """Create a new company."""
        if not slug:
            # Generate slug from name
            slug = name.lower().replace(" ", "-").replace("_", "-")
            slug = "".join(c for c in slug if c.isalnum() or c == "-")

        company = cls(
            company_id=str(uuid.uuid4()),
            name=name,
            slug=slug,
            tier=tier,
            owner_user_id=owner_user_id,
            billing_email=billing_email,
            limits=CompanyLimits.for_tier(tier),
        )

        if tier == CompanyTier.TRIAL:
            from datetime import timedelta
            company.trial_ends_at = datetime.utcnow() + timedelta(days=14)
            company.status = CompanyStatus.TRIAL
        else:
            company.status = CompanyStatus.ACTIVE

        return company

    def is_within_limits(self, resource: str, count: int = 1) -> bool:
        """Check if adding resources is within limits."""
        if resource == "users":
            return self.usage.user_count + count <= self.limits.max_users
        elif resource == "pipelines":
            return self.usage.pipeline_count + count <= self.limits.max_pipelines
        elif resource == "connectors":
            return self.usage.connector_count + count <= self.limits.max_connectors
        elif resource == "storage_gb":
            return self.usage.storage_used_gb + count <= self.limits.max_storage_gb
        elif resource == "jobs":
            return self.usage.jobs_today + count <= self.limits.max_jobs_per_day
        elif resource == "api_calls":
            return self.usage.api_calls_today + count <= self.limits.max_api_calls_per_day
        return True

    def get_limit_status(self) -> dict[str, dict[str, Any]]:
        """Get status of all resource limits."""
        return {
            "users": {
                "used": self.usage.user_count,
                "limit": self.limits.max_users,
                "percentage": self.usage.user_count / self.limits.max_users * 100 if self.limits.max_users > 0 else 0,
            },
            "pipelines": {
                "used": self.usage.pipeline_count,
                "limit": self.limits.max_pipelines,
                "percentage": self.usage.pipeline_count / self.limits.max_pipelines * 100 if self.limits.max_pipelines > 0 else 0,
            },
            "connectors": {
                "used": self.usage.connector_count,
                "limit": self.limits.max_connectors,
                "percentage": self.usage.connector_count / self.limits.max_connectors * 100 if self.limits.max_connectors > 0 else 0,
            },
            "storage_gb": {
                "used": self.usage.storage_used_gb,
                "limit": self.limits.max_storage_gb,
                "percentage": self.usage.storage_used_gb / self.limits.max_storage_gb * 100 if self.limits.max_storage_gb > 0 else 0,
            },
            "jobs_today": {
                "used": self.usage.jobs_today,
                "limit": self.limits.max_jobs_per_day,
                "percentage": self.usage.jobs_today / self.limits.max_jobs_per_day * 100 if self.limits.max_jobs_per_day > 0 else 0,
            },
            "api_calls_today": {
                "used": self.usage.api_calls_today,
                "limit": self.limits.max_api_calls_per_day,
                "percentage": self.usage.api_calls_today / self.limits.max_api_calls_per_day * 100 if self.limits.max_api_calls_per_day > 0 else 0,
            },
        }

    @property
    def is_trial_expired(self) -> bool:
        """Check if trial has expired."""
        if self.status != CompanyStatus.TRIAL:
            return False
        if not self.trial_ends_at:
            return False
        return datetime.utcnow() > self.trial_ends_at

    def to_dict(self, include_sensitive: bool = False) -> dict[str, Any]:
        """Convert to dictionary."""
        data = {
            "company_id": self.company_id,
            "name": self.name,
            "slug": self.slug,
            "status": self.status.value,
            "tier": self.tier.value,
            "owner_user_id": self.owner_user_id,
            "billing_email": self.billing_email,
            "support_email": self.support_email,
            "country": self.country,
            "limits": self.limits.to_dict(),
            "usage": self.usage.to_dict(),
            "limit_status": self.get_limit_status(),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "trial_ends_at": self.trial_ends_at.isoformat() if self.trial_ends_at else None,
            "is_trial_expired": self.is_trial_expired,
        }
        if include_sensitive:
            data["settings"] = self.settings.to_dict()
            data["metadata"] = self.metadata
            data["address"] = {
                "line1": self.address_line1,
                "line2": self.address_line2,
                "city": self.city,
                "state": self.state,
                "postal_code": self.postal_code,
                "country": self.country,
            }
        return data


@dataclass
class UserInvitation:
    """Invitation for a user to join a company."""
    invitation_id: str
    company_id: str
    email: str
    role_ids: list[str] = field(default_factory=list)
    invited_by_user_id: str | None = None
    status: InvitationStatus = InvitationStatus.PENDING
    token: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: datetime = field(default_factory=lambda: datetime.utcnow())
    accepted_at: datetime | None = None
    accepted_user_id: str | None = None
    message: str | None = None

    @classmethod
    def create(
        cls,
        company_id: str,
        email: str,
        role_ids: list[str] | None = None,
        invited_by_user_id: str | None = None,
        expires_in_days: int = 7,
        message: str | None = None,
    ) -> "UserInvitation":
        """Create a new invitation."""
        import secrets
        from datetime import timedelta

        return cls(
            invitation_id=str(uuid.uuid4()),
            company_id=company_id,
            email=email.lower(),
            role_ids=role_ids or ["viewer"],
            invited_by_user_id=invited_by_user_id,
            token=secrets.token_urlsafe(32),
            expires_at=datetime.utcnow() + timedelta(days=expires_in_days),
            message=message,
        )

    @property
    def is_expired(self) -> bool:
        """Check if invitation has expired."""
        return datetime.utcnow() > self.expires_at

    @property
    def is_valid(self) -> bool:
        """Check if invitation is valid."""
        return self.status == InvitationStatus.PENDING and not self.is_expired

    def accept(self, user_id: str) -> None:
        """Accept the invitation."""
        self.status = InvitationStatus.ACCEPTED
        self.accepted_at = datetime.utcnow()
        self.accepted_user_id = user_id

    def revoke(self) -> None:
        """Revoke the invitation."""
        self.status = InvitationStatus.REVOKED

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "invitation_id": self.invitation_id,
            "company_id": self.company_id,
            "email": self.email,
            "role_ids": self.role_ids,
            "invited_by_user_id": self.invited_by_user_id,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "is_expired": self.is_expired,
            "is_valid": self.is_valid,
            "accepted_at": self.accepted_at.isoformat() if self.accepted_at else None,
            "message": self.message,
        }


@dataclass
class CompanyMembership:
    """A user's membership in a company."""
    membership_id: str
    user_id: str
    company_id: str
    role_ids: list[str] = field(default_factory=list)
    is_company_admin: bool = False
    is_owner: bool = False
    joined_at: datetime = field(default_factory=datetime.utcnow)
    invited_by_user_id: str | None = None
    status: str = "active"  # active, suspended, removed
    last_accessed: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        user_id: str,
        company_id: str,
        role_ids: list[str] | None = None,
        is_company_admin: bool = False,
        is_owner: bool = False,
        invited_by_user_id: str | None = None,
    ) -> "CompanyMembership":
        """Create a new membership."""
        return cls(
            membership_id=str(uuid.uuid4()),
            user_id=user_id,
            company_id=company_id,
            role_ids=role_ids or ["viewer"],
            is_company_admin=is_company_admin or is_owner,
            is_owner=is_owner,
            invited_by_user_id=invited_by_user_id,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "membership_id": self.membership_id,
            "user_id": self.user_id,
            "company_id": self.company_id,
            "role_ids": self.role_ids,
            "is_company_admin": self.is_company_admin,
            "is_owner": self.is_owner,
            "joined_at": self.joined_at.isoformat(),
            "invited_by_user_id": self.invited_by_user_id,
            "status": self.status,
            "last_accessed": self.last_accessed.isoformat() if self.last_accessed else None,
        }


# Tenant context for request handling
@dataclass
class TenantContext:
    """
    Context for the current tenant/company.

    Used to scope all operations to a specific company.
    """
    company_id: str
    company: Company | None = None
    user_id: str | None = None
    membership: CompanyMembership | None = None
    is_superadmin: bool = False

    def can_access_company(self, target_company_id: str) -> bool:
        """Check if user can access a company."""
        if self.is_superadmin:
            return True
        return self.company_id == target_company_id

    def has_company_permission(self, permission: str) -> bool:
        """Check if user has permission within company context."""
        if self.is_superadmin:
            return True
        if not self.membership:
            return False
        # Company admin has all permissions within company
        if self.membership.is_company_admin:
            return True
        # Check role permissions (would need RBAC manager)
        return False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "company_id": self.company_id,
            "user_id": self.user_id,
            "is_superadmin": self.is_superadmin,
            "is_company_admin": self.membership.is_company_admin if self.membership else False,
            "is_owner": self.membership.is_owner if self.membership else False,
        }
