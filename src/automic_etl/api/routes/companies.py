"""Company management API endpoints."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Depends
from pydantic import BaseModel, EmailStr, Field

from automic_etl.api.models import PaginatedResponse, BaseResponse

router = APIRouter()


# Request/Response models

class CompanyCreate(BaseModel):
    """Company creation request."""
    name: str = Field(..., min_length=2, max_length=100)
    slug: str | None = Field(None, min_length=2, max_length=50, pattern=r"^[a-z0-9-]+$")
    tier: str = Field("free", pattern=r"^(free|starter|professional|enterprise|trial)$")
    billing_email: EmailStr | None = None
    support_email: EmailStr | None = None


class CompanyUpdate(BaseModel):
    """Company update request."""
    name: str | None = Field(None, min_length=2, max_length=100)
    billing_email: EmailStr | None = None
    support_email: EmailStr | None = None
    phone: str | None = None
    address: dict[str, str] | None = None


class CompanySettingsUpdate(BaseModel):
    """Company settings update request."""
    logo_url: str | None = None
    primary_color: str | None = None
    require_mfa: bool | None = None
    allowed_email_domains: list[str] | None = None
    session_timeout_minutes: int | None = Field(None, ge=15, le=1440)
    notification_email: EmailStr | None = None
    slack_webhook: str | None = None


class CompanyTierUpdate(BaseModel):
    """Company tier update request (superadmin only)."""
    tier: str = Field(..., pattern=r"^(free|starter|professional|enterprise|trial)$")
    custom_limits: dict[str, Any] | None = None


class MemberAdd(BaseModel):
    """Add member request."""
    user_id: str
    role_ids: list[str] = ["viewer"]
    is_company_admin: bool = False


class MemberUpdate(BaseModel):
    """Update member request."""
    role_ids: list[str] | None = None
    is_company_admin: bool | None = None


class InvitationCreate(BaseModel):
    """Invitation creation request."""
    email: EmailStr
    role_ids: list[str] = ["viewer"]
    message: str | None = None
    expires_in_days: int = Field(7, ge=1, le=30)


class CompanyResponse(BaseModel):
    """Company response model."""
    company_id: str
    name: str
    slug: str
    status: str
    tier: str
    owner_user_id: str | None
    billing_email: str | None
    support_email: str | None
    country: str
    limits: dict[str, Any]
    usage: dict[str, Any]
    limit_status: dict[str, Any]
    created_at: str
    updated_at: str
    trial_ends_at: str | None = None
    is_trial_expired: bool = False


class MembershipResponse(BaseModel):
    """Membership response model."""
    membership_id: str
    user_id: str
    company_id: str
    role_ids: list[str]
    is_company_admin: bool
    is_owner: bool
    joined_at: str
    status: str


class InvitationResponse(BaseModel):
    """Invitation response model."""
    invitation_id: str
    company_id: str
    email: str
    role_ids: list[str]
    status: str
    created_at: str
    expires_at: str
    is_expired: bool
    is_valid: bool
    message: str | None = None


# In-memory storage (in production, use database)
from automic_etl.auth import (
    CompanyManager,
    CompanyTier,
    CompanyStatus,
    InvitationStatus,
)

_company_manager: CompanyManager | None = None


def get_company_manager() -> CompanyManager:
    """Get or create company manager."""
    global _company_manager
    if _company_manager is None:
        _company_manager = CompanyManager()
    return _company_manager


# Company CRUD endpoints

@router.get("", response_model=PaginatedResponse)
async def list_companies(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: str | None = Query(None, pattern=r"^(active|inactive|suspended|trial|pending)$"),
    tier: str | None = Query(None, pattern=r"^(free|starter|professional|enterprise|trial)$"),
    search: str | None = None,
):
    """
    List companies.

    Requires superadmin for full list, otherwise returns user's companies.
    """
    manager = get_company_manager()

    status_enum = CompanyStatus(status) if status else None
    tier_enum = CompanyTier(tier) if tier else None

    companies, total = manager.list_companies(
        status=status_enum,
        tier=tier_enum,
        search=search,
        limit=page_size,
        offset=(page - 1) * page_size,
    )

    return PaginatedResponse(
        items=[c.to_dict() for c in companies],
        total=total,
        page=page,
        page_size=page_size,
        pages=(total + page_size - 1) // page_size if page_size > 0 else 0,
    )


@router.post("", response_model=CompanyResponse, status_code=201)
async def create_company(company: CompanyCreate):
    """Create a new company."""
    manager = get_company_manager()

    tier_enum = CompanyTier(company.tier)

    try:
        new_company = manager.create_company(
            name=company.name,
            slug=company.slug,
            tier=tier_enum,
            billing_email=company.billing_email,
        )
        return CompanyResponse(**new_company.to_dict())
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{company_id}", response_model=CompanyResponse)
async def get_company(company_id: str):
    """Get company by ID."""
    manager = get_company_manager()

    company = manager.get_company(company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    return CompanyResponse(**company.to_dict())


@router.get("/slug/{slug}", response_model=CompanyResponse)
async def get_company_by_slug(slug: str):
    """Get company by slug."""
    manager = get_company_manager()

    company = manager.get_company_by_slug(slug)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    return CompanyResponse(**company.to_dict())


@router.put("/{company_id}", response_model=CompanyResponse)
async def update_company(company_id: str, update: CompanyUpdate):
    """Update company details."""
    manager = get_company_manager()

    company = manager.update_company(
        company_id=company_id,
        name=update.name,
        billing_email=update.billing_email,
        support_email=update.support_email,
        phone=update.phone,
        address=update.address,
    )

    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    return CompanyResponse(**company.to_dict())


@router.put("/{company_id}/settings", response_model=CompanyResponse)
async def update_company_settings(company_id: str, settings: CompanySettingsUpdate):
    """Update company settings."""
    manager = get_company_manager()

    settings_dict = settings.model_dump(exclude_unset=True)

    company = manager.update_company_settings(
        company_id=company_id,
        settings=settings_dict,
    )

    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    return CompanyResponse(**company.to_dict(include_sensitive=True))


@router.put("/{company_id}/tier", response_model=CompanyResponse)
async def update_company_tier(company_id: str, update: CompanyTierUpdate):
    """
    Update company tier (superadmin only).

    Changes the subscription tier and associated resource limits.
    """
    manager = get_company_manager()

    tier_enum = CompanyTier(update.tier)

    company = manager.update_company_tier(
        company_id=company_id,
        tier=tier_enum,
        custom_limits=update.custom_limits,
    )

    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    return CompanyResponse(**company.to_dict())


@router.post("/{company_id}/suspend", response_model=CompanyResponse)
async def suspend_company(company_id: str, reason: str = Query(..., min_length=5)):
    """Suspend a company (superadmin only)."""
    manager = get_company_manager()

    company = manager.update_company_status(
        company_id=company_id,
        status=CompanyStatus.SUSPENDED,
        reason=reason,
    )

    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    return CompanyResponse(**company.to_dict())


@router.post("/{company_id}/activate", response_model=CompanyResponse)
async def activate_company(company_id: str):
    """Activate a company (superadmin only)."""
    manager = get_company_manager()

    company = manager.update_company_status(
        company_id=company_id,
        status=CompanyStatus.ACTIVE,
    )

    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    return CompanyResponse(**company.to_dict())


@router.delete("/{company_id}", response_model=BaseResponse)
async def delete_company(
    company_id: str,
    hard_delete: bool = Query(False),
):
    """
    Delete a company.

    By default, performs soft delete (marks as inactive).
    Use hard_delete=true to permanently delete (superadmin only).
    """
    manager = get_company_manager()

    company = manager.get_company(company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    success = manager.delete_company(
        company_id=company_id,
        hard_delete=hard_delete,
    )

    return BaseResponse(
        success=success,
        message=f"Company '{company.name}' {'deleted' if hard_delete else 'deactivated'}",
    )


# Member management endpoints

@router.get("/{company_id}/members", response_model=PaginatedResponse)
async def list_company_members(
    company_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    include_inactive: bool = False,
):
    """List members of a company."""
    manager = get_company_manager()

    company = manager.get_company(company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    memberships = manager.get_company_members(
        company_id=company_id,
        include_inactive=include_inactive,
    )

    # Paginate
    total = len(memberships)
    start = (page - 1) * page_size
    end = start + page_size

    return PaginatedResponse(
        items=[m.to_dict() for m in memberships[start:end]],
        total=total,
        page=page,
        page_size=page_size,
        pages=(total + page_size - 1) // page_size if page_size > 0 else 0,
    )


@router.post("/{company_id}/members", response_model=MembershipResponse, status_code=201)
async def add_member(company_id: str, member: MemberAdd):
    """Add a member to a company."""
    manager = get_company_manager()

    try:
        membership = manager.add_member(
            company_id=company_id,
            user_id=member.user_id,
            role_ids=member.role_ids,
            is_company_admin=member.is_company_admin,
        )
        return MembershipResponse(**membership.to_dict())
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/{company_id}/members/{user_id}", response_model=MembershipResponse)
async def update_member(company_id: str, user_id: str, update: MemberUpdate):
    """Update a member's role in a company."""
    manager = get_company_manager()

    membership = manager.get_membership(user_id, company_id)
    if not membership:
        raise HTTPException(status_code=404, detail="Membership not found")

    updated = manager.update_membership(
        membership_id=membership.membership_id,
        role_ids=update.role_ids,
        is_company_admin=update.is_company_admin,
    )

    if not updated:
        raise HTTPException(status_code=400, detail="Failed to update membership")

    return MembershipResponse(**updated.to_dict())


@router.delete("/{company_id}/members/{user_id}", response_model=BaseResponse)
async def remove_member(company_id: str, user_id: str):
    """Remove a member from a company."""
    manager = get_company_manager()

    try:
        success = manager.remove_member(
            company_id=company_id,
            user_id=user_id,
        )
        if not success:
            raise HTTPException(status_code=404, detail="Membership not found")
        return BaseResponse(success=True, message="Member removed")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{company_id}/transfer-ownership", response_model=BaseResponse)
async def transfer_ownership(company_id: str, new_owner_user_id: str):
    """Transfer company ownership to another member."""
    manager = get_company_manager()

    try:
        success = manager.transfer_ownership(
            company_id=company_id,
            new_owner_user_id=new_owner_user_id,
        )
        if not success:
            raise HTTPException(status_code=400, detail="Failed to transfer ownership")
        return BaseResponse(success=True, message="Ownership transferred")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# Invitation endpoints

@router.get("/{company_id}/invitations", response_model=PaginatedResponse)
async def list_invitations(
    company_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: str | None = Query(None, pattern=r"^(pending|accepted|expired|revoked)$"),
):
    """List invitations for a company."""
    manager = get_company_manager()

    company = manager.get_company(company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    status_enum = InvitationStatus(status) if status else None
    invitations = manager.list_company_invitations(
        company_id=company_id,
        status=status_enum,
    )

    # Paginate
    total = len(invitations)
    start = (page - 1) * page_size
    end = start + page_size

    return PaginatedResponse(
        items=[i.to_dict() for i in invitations[start:end]],
        total=total,
        page=page,
        page_size=page_size,
        pages=(total + page_size - 1) // page_size if page_size > 0 else 0,
    )


@router.post("/{company_id}/invitations", response_model=InvitationResponse, status_code=201)
async def create_invitation(company_id: str, invitation: InvitationCreate):
    """Create an invitation to join a company."""
    manager = get_company_manager()

    try:
        new_invitation = manager.create_invitation(
            company_id=company_id,
            email=invitation.email,
            role_ids=invitation.role_ids,
            message=invitation.message,
            expires_in_days=invitation.expires_in_days,
        )
        return InvitationResponse(**new_invitation.to_dict())
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/invitations/{invitation_id}/accept", response_model=MembershipResponse)
async def accept_invitation(invitation_id: str, user_id: str):
    """Accept an invitation."""
    manager = get_company_manager()

    try:
        membership = manager.accept_invitation(
            invitation_id=invitation_id,
            user_id=user_id,
        )
        return MembershipResponse(**membership.to_dict())
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/invitations/accept-by-token", response_model=MembershipResponse)
async def accept_invitation_by_token(token: str, user_id: str):
    """Accept an invitation using the token."""
    manager = get_company_manager()

    invitation = manager.get_invitation_by_token(token)
    if not invitation:
        raise HTTPException(status_code=404, detail="Invitation not found")

    try:
        membership = manager.accept_invitation(
            invitation_id=invitation.invitation_id,
            user_id=user_id,
        )
        return MembershipResponse(**membership.to_dict())
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/{company_id}/invitations/{invitation_id}", response_model=BaseResponse)
async def revoke_invitation(company_id: str, invitation_id: str):
    """Revoke an invitation."""
    manager = get_company_manager()

    success = manager.revoke_invitation(invitation_id=invitation_id)
    if not success:
        raise HTTPException(status_code=404, detail="Invitation not found")

    return BaseResponse(success=True, message="Invitation revoked")


# Usage and statistics endpoints

@router.get("/{company_id}/usage")
async def get_company_usage(company_id: str):
    """Get company resource usage and limits."""
    manager = get_company_manager()

    company = manager.get_company(company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    return {
        "company_id": company_id,
        "usage": company.usage.to_dict(),
        "limits": company.limits.to_dict(),
        "limit_status": company.get_limit_status(),
        "is_trial_expired": company.is_trial_expired,
        "trial_ends_at": company.trial_ends_at.isoformat() if company.trial_ends_at else None,
    }


@router.get("/stats/summary")
async def get_company_stats():
    """Get summary statistics for all companies (superadmin only)."""
    manager = get_company_manager()
    return manager.get_statistics()
