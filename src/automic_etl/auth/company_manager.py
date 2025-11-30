"""Company/organization management for multi-tenant Automic ETL."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
import json
from pathlib import Path

import structlog

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
from automic_etl.auth.models import User, AuditLog, AuditAction

logger = structlog.get_logger()


class CompanyError(Exception):
    """Company management error."""
    pass


class CompanyLimitExceededError(CompanyError):
    """Raised when a company limit is exceeded."""
    pass


class CompanyManager:
    """
    Manages companies/organizations in the multi-tenant system.

    Features:
    - Company CRUD operations
    - User membership management
    - Invitation system
    - Usage tracking and limits
    - Company settings
    """

    def __init__(self, data_dir: str | Path | None = None) -> None:
        """Initialize company manager."""
        self.data_dir = Path(data_dir) if data_dir else Path.home() / ".automic_etl"
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.companies: dict[str, Company] = {}
        self.memberships: dict[str, CompanyMembership] = {}
        self.invitations: dict[str, UserInvitation] = {}
        self.audit_logs: list[AuditLog] = []

        self.logger = logger.bind(component="company_manager")
        self._load_data()

    def _load_data(self) -> None:
        """Load data from disk."""
        companies_file = self.data_dir / "companies.json"
        if companies_file.exists():
            try:
                with open(companies_file, "r") as f:
                    data = json.load(f)
                    for company_data in data.get("companies", []):
                        company = self._company_from_dict(company_data)
                        self.companies[company.company_id] = company
                    for membership_data in data.get("memberships", []):
                        membership = self._membership_from_dict(membership_data)
                        self.memberships[membership.membership_id] = membership
                    for invitation_data in data.get("invitations", []):
                        invitation = self._invitation_from_dict(invitation_data)
                        self.invitations[invitation.invitation_id] = invitation
                self.logger.info("Loaded companies", count=len(self.companies))
            except Exception as e:
                self.logger.error("Failed to load companies", error=str(e))

    def _save_data(self) -> None:
        """Save data to disk."""
        companies_file = self.data_dir / "companies.json"
        try:
            data = {
                "companies": [self._company_to_dict(c) for c in self.companies.values()],
                "memberships": [self._membership_to_dict(m) for m in self.memberships.values()],
                "invitations": [self._invitation_to_dict(i) for i in self.invitations.values()],
            }
            with open(companies_file, "w") as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            self.logger.error("Failed to save companies", error=str(e))

    def _company_to_dict(self, company: Company) -> dict[str, Any]:
        """Convert company to dictionary."""
        return {
            "company_id": company.company_id,
            "name": company.name,
            "slug": company.slug,
            "status": company.status.value,
            "tier": company.tier.value,
            "owner_user_id": company.owner_user_id,
            "billing_email": company.billing_email,
            "support_email": company.support_email,
            "phone": company.phone,
            "address_line1": company.address_line1,
            "address_line2": company.address_line2,
            "city": company.city,
            "state": company.state,
            "postal_code": company.postal_code,
            "country": company.country,
            "limits": company.limits.to_dict(),
            "settings": company.settings.to_dict(),
            "usage": company.usage.to_dict(),
            "created_at": company.created_at.isoformat(),
            "updated_at": company.updated_at.isoformat(),
            "trial_ends_at": company.trial_ends_at.isoformat() if company.trial_ends_at else None,
            "metadata": company.metadata,
        }

    def _company_from_dict(self, data: dict[str, Any]) -> Company:
        """Create company from dictionary."""
        limits_data = data.get("limits", {})
        limits = CompanyLimits(
            max_users=limits_data.get("max_users", 5),
            max_pipelines=limits_data.get("max_pipelines", 10),
            max_connectors=limits_data.get("max_connectors", 5),
            max_storage_gb=limits_data.get("max_storage_gb", 10),
            max_jobs_per_day=limits_data.get("max_jobs_per_day", 100),
            max_api_calls_per_day=limits_data.get("max_api_calls_per_day", 1000),
            retention_days=limits_data.get("retention_days", 30),
            enable_advanced_features=limits_data.get("enable_advanced_features", False),
            enable_sso=limits_data.get("enable_sso", False),
            enable_audit_logs=limits_data.get("enable_audit_logs", True),
            enable_custom_roles=limits_data.get("enable_custom_roles", False),
            support_level=limits_data.get("support_level", "community"),
        )

        settings_data = data.get("settings", {})
        settings = CompanySettings(
            logo_url=settings_data.get("logo_url"),
            primary_color=settings_data.get("primary_color", "#1E88E5"),
            company_domain=settings_data.get("company_domain"),
            require_mfa=settings_data.get("require_mfa", False),
            allowed_email_domains=settings_data.get("allowed_email_domains", []),
            session_timeout_minutes=settings_data.get("session_timeout_minutes", 480),
            password_policy=settings_data.get("password_policy", {}),
            default_storage_provider=settings_data.get("default_storage_provider", "local"),
            encryption_at_rest=settings_data.get("encryption_at_rest", True),
            data_region=settings_data.get("data_region", "us-east-1"),
            notification_email=settings_data.get("notification_email"),
            slack_webhook=settings_data.get("slack_webhook"),
            enabled_features=settings_data.get("enabled_features", []),
            disabled_connectors=settings_data.get("disabled_connectors", []),
        )

        usage_data = data.get("usage", {})
        usage = CompanyUsage(
            user_count=usage_data.get("user_count", 0),
            pipeline_count=usage_data.get("pipeline_count", 0),
            connector_count=usage_data.get("connector_count", 0),
            storage_used_gb=usage_data.get("storage_used_gb", 0.0),
            jobs_today=usage_data.get("jobs_today", 0),
            api_calls_today=usage_data.get("api_calls_today", 0),
        )

        return Company(
            company_id=data["company_id"],
            name=data["name"],
            slug=data["slug"],
            status=CompanyStatus(data.get("status", "pending")),
            tier=CompanyTier(data.get("tier", "free")),
            owner_user_id=data.get("owner_user_id"),
            billing_email=data.get("billing_email"),
            support_email=data.get("support_email"),
            phone=data.get("phone"),
            address_line1=data.get("address_line1"),
            address_line2=data.get("address_line2"),
            city=data.get("city"),
            state=data.get("state"),
            postal_code=data.get("postal_code"),
            country=data.get("country", "US"),
            limits=limits,
            settings=settings,
            usage=usage,
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            trial_ends_at=datetime.fromisoformat(data["trial_ends_at"]) if data.get("trial_ends_at") else None,
            metadata=data.get("metadata", {}),
        )

    def _membership_to_dict(self, membership: CompanyMembership) -> dict[str, Any]:
        """Convert membership to dictionary."""
        return {
            "membership_id": membership.membership_id,
            "user_id": membership.user_id,
            "company_id": membership.company_id,
            "role_ids": membership.role_ids,
            "is_company_admin": membership.is_company_admin,
            "is_owner": membership.is_owner,
            "joined_at": membership.joined_at.isoformat(),
            "invited_by_user_id": membership.invited_by_user_id,
            "status": membership.status,
            "last_accessed": membership.last_accessed.isoformat() if membership.last_accessed else None,
            "metadata": membership.metadata,
        }

    def _membership_from_dict(self, data: dict[str, Any]) -> CompanyMembership:
        """Create membership from dictionary."""
        return CompanyMembership(
            membership_id=data["membership_id"],
            user_id=data["user_id"],
            company_id=data["company_id"],
            role_ids=data.get("role_ids", []),
            is_company_admin=data.get("is_company_admin", False),
            is_owner=data.get("is_owner", False),
            joined_at=datetime.fromisoformat(data["joined_at"]),
            invited_by_user_id=data.get("invited_by_user_id"),
            status=data.get("status", "active"),
            last_accessed=datetime.fromisoformat(data["last_accessed"]) if data.get("last_accessed") else None,
            metadata=data.get("metadata", {}),
        )

    def _invitation_to_dict(self, invitation: UserInvitation) -> dict[str, Any]:
        """Convert invitation to dictionary."""
        return {
            "invitation_id": invitation.invitation_id,
            "company_id": invitation.company_id,
            "email": invitation.email,
            "role_ids": invitation.role_ids,
            "invited_by_user_id": invitation.invited_by_user_id,
            "status": invitation.status.value,
            "token": invitation.token,
            "created_at": invitation.created_at.isoformat(),
            "expires_at": invitation.expires_at.isoformat(),
            "accepted_at": invitation.accepted_at.isoformat() if invitation.accepted_at else None,
            "accepted_user_id": invitation.accepted_user_id,
            "message": invitation.message,
        }

    def _invitation_from_dict(self, data: dict[str, Any]) -> UserInvitation:
        """Create invitation from dictionary."""
        return UserInvitation(
            invitation_id=data["invitation_id"],
            company_id=data["company_id"],
            email=data["email"],
            role_ids=data.get("role_ids", []),
            invited_by_user_id=data.get("invited_by_user_id"),
            status=InvitationStatus(data.get("status", "pending")),
            token=data["token"],
            created_at=datetime.fromisoformat(data["created_at"]),
            expires_at=datetime.fromisoformat(data["expires_at"]),
            accepted_at=datetime.fromisoformat(data["accepted_at"]) if data.get("accepted_at") else None,
            accepted_user_id=data.get("accepted_user_id"),
            message=data.get("message"),
        )

    # Company CRUD operations

    def create_company(
        self,
        name: str,
        slug: str | None = None,
        tier: CompanyTier = CompanyTier.FREE,
        owner_user_id: str | None = None,
        billing_email: str | None = None,
        created_by_user_id: str | None = None,
    ) -> Company:
        """
        Create a new company.

        Args:
            name: Company name
            slug: URL-friendly identifier (auto-generated if not provided)
            tier: Subscription tier
            owner_user_id: Owner user ID
            billing_email: Billing email
            created_by_user_id: User creating the company

        Returns:
            Created company
        """
        # Check slug uniqueness
        if slug:
            existing = self.get_company_by_slug(slug)
            if existing:
                raise CompanyError(f"Company with slug '{slug}' already exists")

        company = Company.create(
            name=name,
            slug=slug,
            tier=tier,
            owner_user_id=owner_user_id,
            billing_email=billing_email,
        )

        # Ensure slug is unique
        while self.get_company_by_slug(company.slug):
            company.slug = f"{company.slug}-{company.company_id[:8]}"

        self.companies[company.company_id] = company

        # Create owner membership if owner specified
        if owner_user_id:
            membership = CompanyMembership.create(
                user_id=owner_user_id,
                company_id=company.company_id,
                role_ids=["admin"],
                is_company_admin=True,
                is_owner=True,
            )
            self.memberships[membership.membership_id] = membership
            company.usage.user_count = 1

        self._save_data()

        self._log_action(
            AuditAction.USER_CREATED,  # Using existing action type
            user_id=created_by_user_id,
            resource_type="company",
            resource_id=company.company_id,
            details={"company_name": name, "tier": tier.value},
        )

        self.logger.info("Company created", company_id=company.company_id, name=name)
        return company

    def get_company(self, company_id: str) -> Company | None:
        """Get company by ID."""
        return self.companies.get(company_id)

    def get_company_by_slug(self, slug: str) -> Company | None:
        """Get company by slug."""
        for company in self.companies.values():
            if company.slug.lower() == slug.lower():
                return company
        return None

    def list_companies(
        self,
        status: CompanyStatus | None = None,
        tier: CompanyTier | None = None,
        search: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[Company], int]:
        """
        List companies with filters.

        Returns:
            Tuple of (companies, total_count)
        """
        companies = list(self.companies.values())

        if status:
            companies = [c for c in companies if c.status == status]

        if tier:
            companies = [c for c in companies if c.tier == tier]

        if search:
            search_lower = search.lower()
            companies = [
                c for c in companies
                if search_lower in c.name.lower()
                or search_lower in c.slug.lower()
                or (c.billing_email and search_lower in c.billing_email.lower())
            ]

        total = len(companies)
        companies = companies[offset:offset + limit]

        return companies, total

    def update_company(
        self,
        company_id: str,
        name: str | None = None,
        billing_email: str | None = None,
        support_email: str | None = None,
        phone: str | None = None,
        address: dict[str, str] | None = None,
        updated_by_user_id: str | None = None,
    ) -> Company | None:
        """Update company details."""
        company = self.companies.get(company_id)
        if not company:
            return None

        if name:
            company.name = name
        if billing_email:
            company.billing_email = billing_email
        if support_email:
            company.support_email = support_email
        if phone:
            company.phone = phone
        if address:
            company.address_line1 = address.get("line1", company.address_line1)
            company.address_line2 = address.get("line2", company.address_line2)
            company.city = address.get("city", company.city)
            company.state = address.get("state", company.state)
            company.postal_code = address.get("postal_code", company.postal_code)
            company.country = address.get("country", company.country)

        company.updated_at = datetime.utcnow()
        self._save_data()

        self._log_action(
            AuditAction.USER_UPDATED,
            user_id=updated_by_user_id,
            resource_type="company",
            resource_id=company_id,
        )

        return company

    def update_company_settings(
        self,
        company_id: str,
        settings: dict[str, Any],
        updated_by_user_id: str | None = None,
    ) -> Company | None:
        """Update company settings."""
        company = self.companies.get(company_id)
        if not company:
            return None

        # Update settings
        for key, value in settings.items():
            if hasattr(company.settings, key):
                setattr(company.settings, key, value)

        company.updated_at = datetime.utcnow()
        self._save_data()

        self._log_action(
            AuditAction.SYSTEM_CONFIG_CHANGED,
            user_id=updated_by_user_id,
            resource_type="company_settings",
            resource_id=company_id,
            details={"updated_settings": list(settings.keys())},
        )

        return company

    def update_company_tier(
        self,
        company_id: str,
        tier: CompanyTier,
        custom_limits: dict[str, Any] | None = None,
        updated_by_user_id: str | None = None,
    ) -> Company | None:
        """
        Update company tier and limits (superadmin only).

        Args:
            company_id: Company ID
            tier: New tier
            custom_limits: Optional custom limits override
            updated_by_user_id: Admin user ID
        """
        company = self.companies.get(company_id)
        if not company:
            return None

        old_tier = company.tier
        company.tier = tier
        company.limits = CompanyLimits.for_tier(tier)

        # Apply custom limits if provided
        if custom_limits:
            for key, value in custom_limits.items():
                if hasattr(company.limits, key):
                    setattr(company.limits, key, value)

        # Handle trial status
        if tier == CompanyTier.TRIAL:
            company.status = CompanyStatus.TRIAL
            company.trial_ends_at = datetime.utcnow() + timedelta(days=14)
        elif old_tier == CompanyTier.TRIAL:
            company.status = CompanyStatus.ACTIVE
            company.trial_ends_at = None

        company.updated_at = datetime.utcnow()
        self._save_data()

        self._log_action(
            AuditAction.SYSTEM_CONFIG_CHANGED,
            user_id=updated_by_user_id,
            resource_type="company_tier",
            resource_id=company_id,
            details={"old_tier": old_tier.value, "new_tier": tier.value},
        )

        return company

    def update_company_status(
        self,
        company_id: str,
        status: CompanyStatus,
        reason: str | None = None,
        updated_by_user_id: str | None = None,
    ) -> Company | None:
        """Update company status (superadmin only)."""
        company = self.companies.get(company_id)
        if not company:
            return None

        old_status = company.status
        company.status = status
        company.updated_at = datetime.utcnow()

        if reason:
            company.metadata["status_change_reason"] = reason
            company.metadata["status_changed_at"] = datetime.utcnow().isoformat()
            company.metadata["status_changed_by"] = updated_by_user_id

        self._save_data()

        self._log_action(
            AuditAction.USER_SUSPENDED if status == CompanyStatus.SUSPENDED else AuditAction.USER_ACTIVATED,
            user_id=updated_by_user_id,
            resource_type="company",
            resource_id=company_id,
            details={"old_status": old_status.value, "new_status": status.value, "reason": reason},
        )

        return company

    def delete_company(
        self,
        company_id: str,
        deleted_by_user_id: str | None = None,
        hard_delete: bool = False,
    ) -> bool:
        """
        Delete a company.

        Args:
            company_id: Company ID
            deleted_by_user_id: User deleting the company
            hard_delete: If True, permanently delete; if False, just mark as inactive
        """
        company = self.companies.get(company_id)
        if not company:
            return False

        if hard_delete:
            # Remove all memberships
            self.memberships = {
                k: v for k, v in self.memberships.items()
                if v.company_id != company_id
            }
            # Remove all invitations
            self.invitations = {
                k: v for k, v in self.invitations.items()
                if v.company_id != company_id
            }
            del self.companies[company_id]
        else:
            company.status = CompanyStatus.INACTIVE
            company.updated_at = datetime.utcnow()
            company.metadata["deleted_at"] = datetime.utcnow().isoformat()
            company.metadata["deleted_by"] = deleted_by_user_id

        self._save_data()

        self._log_action(
            AuditAction.USER_DELETED,
            user_id=deleted_by_user_id,
            resource_type="company",
            resource_id=company_id,
            details={"company_name": company.name, "hard_delete": hard_delete},
        )

        return True

    # Membership management

    def add_member(
        self,
        company_id: str,
        user_id: str,
        role_ids: list[str] | None = None,
        is_company_admin: bool = False,
        invited_by_user_id: str | None = None,
    ) -> CompanyMembership:
        """Add a user to a company."""
        company = self.companies.get(company_id)
        if not company:
            raise CompanyError(f"Company '{company_id}' not found")

        # Check limits
        if not company.is_within_limits("users"):
            raise CompanyLimitExceededError(
                f"Company has reached maximum user limit ({company.limits.max_users})"
            )

        # Check if already a member
        existing = self.get_membership(user_id, company_id)
        if existing:
            raise CompanyError(f"User is already a member of this company")

        membership = CompanyMembership.create(
            user_id=user_id,
            company_id=company_id,
            role_ids=role_ids,
            is_company_admin=is_company_admin,
            invited_by_user_id=invited_by_user_id,
        )

        self.memberships[membership.membership_id] = membership
        company.usage.user_count += 1
        company.updated_at = datetime.utcnow()

        self._save_data()

        self._log_action(
            AuditAction.ROLE_ASSIGNED,
            user_id=invited_by_user_id,
            resource_type="membership",
            resource_id=membership.membership_id,
            details={"company_id": company_id, "user_id": user_id},
        )

        return membership

    def remove_member(
        self,
        company_id: str,
        user_id: str,
        removed_by_user_id: str | None = None,
    ) -> bool:
        """Remove a user from a company."""
        membership = self.get_membership(user_id, company_id)
        if not membership:
            return False

        # Prevent removing the owner
        if membership.is_owner:
            raise CompanyError("Cannot remove the company owner")

        company = self.companies.get(company_id)
        if company:
            company.usage.user_count = max(0, company.usage.user_count - 1)
            company.updated_at = datetime.utcnow()

        del self.memberships[membership.membership_id]
        self._save_data()

        self._log_action(
            AuditAction.ROLE_REMOVED,
            user_id=removed_by_user_id,
            resource_type="membership",
            resource_id=membership.membership_id,
            details={"company_id": company_id, "user_id": user_id},
        )

        return True

    def get_membership(self, user_id: str, company_id: str) -> CompanyMembership | None:
        """Get a user's membership in a company."""
        for membership in self.memberships.values():
            if membership.user_id == user_id and membership.company_id == company_id:
                if membership.status == "active":
                    return membership
        return None

    def get_user_memberships(self, user_id: str) -> list[CompanyMembership]:
        """Get all company memberships for a user."""
        return [
            m for m in self.memberships.values()
            if m.user_id == user_id and m.status == "active"
        ]

    def get_company_members(
        self,
        company_id: str,
        include_inactive: bool = False,
    ) -> list[CompanyMembership]:
        """Get all members of a company."""
        memberships = [
            m for m in self.memberships.values()
            if m.company_id == company_id
        ]
        if not include_inactive:
            memberships = [m for m in memberships if m.status == "active"]
        return memberships

    def update_membership(
        self,
        membership_id: str,
        role_ids: list[str] | None = None,
        is_company_admin: bool | None = None,
        updated_by_user_id: str | None = None,
    ) -> CompanyMembership | None:
        """Update a membership."""
        membership = self.memberships.get(membership_id)
        if not membership:
            return None

        if role_ids is not None:
            membership.role_ids = role_ids
        if is_company_admin is not None:
            membership.is_company_admin = is_company_admin

        self._save_data()

        self._log_action(
            AuditAction.ROLE_ASSIGNED,
            user_id=updated_by_user_id,
            resource_type="membership",
            resource_id=membership_id,
            details={"role_ids": role_ids, "is_company_admin": is_company_admin},
        )

        return membership

    def transfer_ownership(
        self,
        company_id: str,
        new_owner_user_id: str,
        transferred_by_user_id: str | None = None,
    ) -> bool:
        """Transfer company ownership to another user."""
        company = self.companies.get(company_id)
        if not company:
            return False

        new_owner_membership = self.get_membership(new_owner_user_id, company_id)
        if not new_owner_membership:
            raise CompanyError("New owner must be a member of the company")

        # Remove owner status from current owner
        if company.owner_user_id:
            old_owner_membership = self.get_membership(company.owner_user_id, company_id)
            if old_owner_membership:
                old_owner_membership.is_owner = False

        # Set new owner
        company.owner_user_id = new_owner_user_id
        new_owner_membership.is_owner = True
        new_owner_membership.is_company_admin = True

        company.updated_at = datetime.utcnow()
        self._save_data()

        self._log_action(
            AuditAction.SYSTEM_CONFIG_CHANGED,
            user_id=transferred_by_user_id,
            resource_type="company_ownership",
            resource_id=company_id,
            details={"new_owner": new_owner_user_id},
        )

        return True

    # Invitation management

    def create_invitation(
        self,
        company_id: str,
        email: str,
        role_ids: list[str] | None = None,
        invited_by_user_id: str | None = None,
        expires_in_days: int = 7,
        message: str | None = None,
    ) -> UserInvitation:
        """Create an invitation for a user to join a company."""
        company = self.companies.get(company_id)
        if not company:
            raise CompanyError(f"Company '{company_id}' not found")

        # Check if there's already a pending invitation
        existing = [
            i for i in self.invitations.values()
            if i.company_id == company_id
            and i.email.lower() == email.lower()
            and i.is_valid
        ]
        if existing:
            raise CompanyError(f"Invitation already pending for {email}")

        # Check user limits
        if not company.is_within_limits("users"):
            raise CompanyLimitExceededError(
                f"Company has reached maximum user limit ({company.limits.max_users})"
            )

        # Check email domain restriction
        if company.settings.allowed_email_domains:
            email_domain = email.split("@")[-1].lower()
            if email_domain not in [d.lower() for d in company.settings.allowed_email_domains]:
                raise CompanyError(f"Email domain '{email_domain}' not allowed for this company")

        invitation = UserInvitation.create(
            company_id=company_id,
            email=email,
            role_ids=role_ids,
            invited_by_user_id=invited_by_user_id,
            expires_in_days=expires_in_days,
            message=message,
        )

        self.invitations[invitation.invitation_id] = invitation
        self._save_data()

        self._log_action(
            AuditAction.USER_CREATED,
            user_id=invited_by_user_id,
            resource_type="invitation",
            resource_id=invitation.invitation_id,
            details={"company_id": company_id, "email": email},
        )

        return invitation

    def get_invitation(self, invitation_id: str) -> UserInvitation | None:
        """Get invitation by ID."""
        return self.invitations.get(invitation_id)

    def get_invitation_by_token(self, token: str) -> UserInvitation | None:
        """Get invitation by token."""
        for invitation in self.invitations.values():
            if invitation.token == token:
                return invitation
        return None

    def list_company_invitations(
        self,
        company_id: str,
        status: InvitationStatus | None = None,
    ) -> list[UserInvitation]:
        """List invitations for a company."""
        invitations = [
            i for i in self.invitations.values()
            if i.company_id == company_id
        ]
        if status:
            invitations = [i for i in invitations if i.status == status]
        return invitations

    def accept_invitation(
        self,
        invitation_id: str,
        user_id: str,
    ) -> CompanyMembership:
        """Accept an invitation."""
        invitation = self.invitations.get(invitation_id)
        if not invitation:
            raise CompanyError("Invitation not found")

        if not invitation.is_valid:
            if invitation.is_expired:
                invitation.status = InvitationStatus.EXPIRED
                self._save_data()
                raise CompanyError("Invitation has expired")
            raise CompanyError("Invitation is no longer valid")

        # Create membership
        membership = self.add_member(
            company_id=invitation.company_id,
            user_id=user_id,
            role_ids=invitation.role_ids,
            invited_by_user_id=invitation.invited_by_user_id,
        )

        # Mark invitation as accepted
        invitation.accept(user_id)
        self._save_data()

        return membership

    def revoke_invitation(
        self,
        invitation_id: str,
        revoked_by_user_id: str | None = None,
    ) -> bool:
        """Revoke an invitation."""
        invitation = self.invitations.get(invitation_id)
        if not invitation:
            return False

        invitation.revoke()
        self._save_data()

        self._log_action(
            AuditAction.USER_DELETED,
            user_id=revoked_by_user_id,
            resource_type="invitation",
            resource_id=invitation_id,
        )

        return True

    # Usage tracking

    def increment_usage(
        self,
        company_id: str,
        resource: str,
        amount: int = 1,
    ) -> bool:
        """Increment usage counter for a resource."""
        company = self.companies.get(company_id)
        if not company:
            return False

        if resource == "jobs":
            company.usage.jobs_today += amount
        elif resource == "api_calls":
            company.usage.api_calls_today += amount
        elif resource == "pipelines":
            company.usage.pipeline_count += amount
        elif resource == "connectors":
            company.usage.connector_count += amount
        elif resource == "storage_gb":
            company.usage.storage_used_gb += amount

        company.usage.last_updated = datetime.utcnow()
        self._save_data()
        return True

    def decrement_usage(
        self,
        company_id: str,
        resource: str,
        amount: int = 1,
    ) -> bool:
        """Decrement usage counter for a resource."""
        company = self.companies.get(company_id)
        if not company:
            return False

        if resource == "pipelines":
            company.usage.pipeline_count = max(0, company.usage.pipeline_count - amount)
        elif resource == "connectors":
            company.usage.connector_count = max(0, company.usage.connector_count - amount)
        elif resource == "storage_gb":
            company.usage.storage_used_gb = max(0, company.usage.storage_used_gb - amount)

        company.usage.last_updated = datetime.utcnow()
        self._save_data()
        return True

    def reset_daily_usage(self) -> None:
        """Reset daily usage counters for all companies."""
        for company in self.companies.values():
            company.usage.jobs_today = 0
            company.usage.api_calls_today = 0
            company.usage.last_updated = datetime.utcnow()
        self._save_data()
        self.logger.info("Reset daily usage for all companies")

    # Tenant context

    def get_tenant_context(
        self,
        user_id: str,
        company_id: str | None = None,
        is_superadmin: bool = False,
    ) -> TenantContext | None:
        """
        Get tenant context for a user.

        Args:
            user_id: User ID
            company_id: Specific company ID (optional)
            is_superadmin: Whether user is a superadmin
        """
        if is_superadmin and company_id:
            company = self.get_company(company_id)
            return TenantContext(
                company_id=company_id,
                company=company,
                user_id=user_id,
                is_superadmin=True,
            )

        memberships = self.get_user_memberships(user_id)
        if not memberships:
            return None

        if company_id:
            membership = next((m for m in memberships if m.company_id == company_id), None)
            if not membership:
                return None
        else:
            # Use first membership (primary company)
            membership = memberships[0]

        company = self.get_company(membership.company_id)

        return TenantContext(
            company_id=membership.company_id,
            company=company,
            user_id=user_id,
            membership=membership,
            is_superadmin=is_superadmin,
        )

    # Statistics

    def get_statistics(self) -> dict[str, Any]:
        """Get company statistics (superadmin)."""
        companies = list(self.companies.values())

        status_counts = {}
        tier_counts = {}
        total_users = 0
        total_pipelines = 0
        total_storage = 0.0

        for company in companies:
            status_counts[company.status.value] = status_counts.get(company.status.value, 0) + 1
            tier_counts[company.tier.value] = tier_counts.get(company.tier.value, 0) + 1
            total_users += company.usage.user_count
            total_pipelines += company.usage.pipeline_count
            total_storage += company.usage.storage_used_gb

        return {
            "total_companies": len(companies),
            "status_counts": status_counts,
            "tier_counts": tier_counts,
            "total_users": total_users,
            "total_pipelines": total_pipelines,
            "total_storage_gb": total_storage,
            "total_memberships": len(self.memberships),
            "pending_invitations": len([i for i in self.invitations.values() if i.status == InvitationStatus.PENDING]),
        }

    def _log_action(
        self,
        action: AuditAction,
        user_id: str | None = None,
        resource_type: str | None = None,
        resource_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Log an audit action."""
        log = AuditLog.create(
            action=action,
            user_id=user_id,
            resource_type=resource_type,
            resource_id=resource_id,
            details=details,
        )
        self.audit_logs.append(log)

        if len(self.audit_logs) > 10000:
            self.audit_logs = self.audit_logs[-10000:]
