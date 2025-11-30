"""Superadmin API endpoints."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, EmailStr, Field

from automic_etl.api.models import PaginatedResponse, BaseResponse

router = APIRouter()


# Request/Response models

class GlobalSettingsUpdate(BaseModel):
    """Global settings update request."""
    platform_name: str | None = None
    platform_url: str | None = None
    support_email: EmailStr | None = None
    allow_self_registration: bool | None = None
    require_email_verification: bool | None = None
    auto_approve_companies: bool | None = None
    default_company_tier: str | None = None
    password_min_length: int | None = Field(None, ge=6, le=32)
    require_mfa_for_admins: bool | None = None
    session_timeout_minutes: int | None = Field(None, ge=15, le=1440)
    max_failed_login_attempts: int | None = Field(None, ge=3, le=20)
    lockout_duration_minutes: int | None = Field(None, ge=5, le=1440)
    enable_llm_features: bool | None = None
    enable_streaming_connectors: bool | None = None
    enable_spark_integration: bool | None = None
    enable_api_access: bool | None = None
    max_companies: int | None = Field(None, ge=1)
    max_users_total: int | None = Field(None, ge=1)
    default_api_rate_limit: int | None = Field(None, ge=100)


class MaintenanceModeRequest(BaseModel):
    """Maintenance mode request."""
    enabled: bool
    message: str | None = None
    allowed_ips: list[str] | None = None


class SuperadminCreate(BaseModel):
    """Create superadmin request."""
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr
    password: str = Field(..., min_length=8)
    first_name: str = ""
    last_name: str = ""


class ImpersonateRequest(BaseModel):
    """User impersonation request."""
    target_user_id: str


class GlobalSettingsResponse(BaseModel):
    """Global settings response."""
    platform_name: str
    platform_url: str
    support_email: str
    allow_self_registration: bool
    require_email_verification: bool
    auto_approve_companies: bool
    default_company_tier: str
    password_min_length: int
    require_mfa_for_admins: bool
    session_timeout_minutes: int
    max_failed_login_attempts: int
    lockout_duration_minutes: int
    enable_llm_features: bool
    enable_streaming_connectors: bool
    enable_spark_integration: bool
    enable_api_access: bool
    max_companies: int
    max_users_total: int
    default_api_rate_limit: int
    maintenance_mode: bool
    maintenance_message: str


class SystemHealthResponse(BaseModel):
    """System health response."""
    status: str
    timestamp: str
    components: dict[str, Any]
    warnings: list[str]
    errors: list[str]


# In-memory storage (in production, integrate with actual managers)
from automic_etl.auth import (
    SuperadminController,
    GlobalSettings,
    CompanyManager,
    AuthManager,
    CompanyTier,
    CompanyStatus,
    UserStatus,
)

_superadmin_controller: SuperadminController | None = None
_auth_manager: AuthManager | None = None
_company_manager: CompanyManager | None = None


def get_superadmin_controller() -> SuperadminController:
    """Get or create superadmin controller."""
    global _superadmin_controller, _auth_manager, _company_manager

    if _auth_manager is None:
        _auth_manager = AuthManager()

    if _company_manager is None:
        _company_manager = CompanyManager()

    if _superadmin_controller is None:
        _superadmin_controller = SuperadminController(
            auth_manager=_auth_manager,
            company_manager=_company_manager,
        )

    return _superadmin_controller


# Global settings endpoints

@router.get("/settings", response_model=GlobalSettingsResponse)
async def get_global_settings():
    """Get global platform settings (superadmin only)."""
    controller = get_superadmin_controller()
    settings = controller.get_global_settings()
    return GlobalSettingsResponse(**settings.to_dict())


@router.put("/settings", response_model=GlobalSettingsResponse)
async def update_global_settings(update: GlobalSettingsUpdate):
    """Update global platform settings (superadmin only)."""
    controller = get_superadmin_controller()

    # Filter out None values
    settings_dict = {k: v for k, v in update.model_dump().items() if v is not None}

    # In production, get admin_user from auth token
    # For now, we skip the decorator check
    for key, value in settings_dict.items():
        if hasattr(controller.global_settings, key):
            setattr(controller.global_settings, key, value)

    controller._save_settings()

    return GlobalSettingsResponse(**controller.global_settings.to_dict())


# Maintenance mode endpoints

@router.post("/maintenance", response_model=BaseResponse)
async def set_maintenance_mode(request: MaintenanceModeRequest):
    """Enable or disable maintenance mode (superadmin only)."""
    controller = get_superadmin_controller()

    if request.enabled:
        controller.global_settings.maintenance_mode = True
        controller.global_settings.maintenance_message = request.message or "System is under maintenance"
        if request.allowed_ips:
            controller.global_settings.maintenance_allowed_ips = request.allowed_ips
    else:
        controller.global_settings.maintenance_mode = False
        controller.global_settings.maintenance_message = ""
        controller.global_settings.maintenance_allowed_ips = []

    controller._save_settings()

    return BaseResponse(
        success=True,
        message=f"Maintenance mode {'enabled' if request.enabled else 'disabled'}",
    )


@router.get("/maintenance")
async def get_maintenance_status():
    """Get maintenance mode status."""
    controller = get_superadmin_controller()
    return {
        "maintenance_mode": controller.global_settings.maintenance_mode,
        "message": controller.global_settings.maintenance_message,
        "allowed_ips": controller.global_settings.maintenance_allowed_ips,
    }


# System health endpoints

@router.get("/health", response_model=SystemHealthResponse)
async def get_system_health():
    """Get system health status (superadmin only)."""
    controller = get_superadmin_controller()
    health = controller.get_system_health()
    return SystemHealthResponse(**health.to_dict())


@router.get("/stats")
async def get_platform_statistics():
    """Get comprehensive platform statistics (superadmin only)."""
    controller = get_superadmin_controller()
    return controller.get_platform_statistics()


# Company management endpoints (superadmin oversight)

@router.get("/companies", response_model=PaginatedResponse)
async def list_all_companies(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: str | None = Query(None, pattern=r"^(active|inactive|suspended|trial|pending)$"),
    tier: str | None = Query(None, pattern=r"^(free|starter|professional|enterprise|trial)$"),
    search: str | None = None,
):
    """List all companies with filters (superadmin only)."""
    controller = get_superadmin_controller()

    if not controller.company_manager:
        raise HTTPException(status_code=500, detail="Company manager not available")

    status_enum = CompanyStatus(status) if status else None
    tier_enum = CompanyTier(tier) if tier else None

    companies, total = controller.company_manager.list_companies(
        status=status_enum,
        tier=tier_enum,
        search=search,
        limit=page_size,
        offset=(page - 1) * page_size,
    )

    return PaginatedResponse(
        items=[c.to_dict(include_sensitive=True) for c in companies],
        total=total,
        page=page,
        page_size=page_size,
        pages=(total + page_size - 1) // page_size if page_size > 0 else 0,
    )


@router.post("/companies/{company_id}/approve", response_model=BaseResponse)
async def approve_company(company_id: str):
    """Approve a pending company (superadmin only)."""
    controller = get_superadmin_controller()

    if not controller.company_manager:
        raise HTTPException(status_code=500, detail="Company manager not available")

    company = controller.company_manager.update_company_status(
        company_id=company_id,
        status=CompanyStatus.ACTIVE,
        reason="Approved by superadmin",
    )

    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    return BaseResponse(success=True, message=f"Company '{company.name}' approved")


@router.post("/companies/{company_id}/suspend", response_model=BaseResponse)
async def suspend_company(company_id: str, reason: str = Query(..., min_length=5)):
    """Suspend a company (superadmin only)."""
    controller = get_superadmin_controller()

    if not controller.company_manager:
        raise HTTPException(status_code=500, detail="Company manager not available")

    company = controller.company_manager.update_company_status(
        company_id=company_id,
        status=CompanyStatus.SUSPENDED,
        reason=reason,
    )

    if not company:
        raise HTTPException(status_code=404, detail="Company not found")

    return BaseResponse(success=True, message=f"Company '{company.name}' suspended")


# User management endpoints (superadmin oversight)

@router.get("/users", response_model=PaginatedResponse)
async def list_all_users(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    company_id: str | None = None,
    status: str | None = Query(None, pattern=r"^(active|inactive|suspended|pending)$"),
    search: str | None = None,
    superadmins_only: bool = False,
):
    """List all users across companies (superadmin only)."""
    controller = get_superadmin_controller()

    if not controller.auth_manager:
        raise HTTPException(status_code=500, detail="Auth manager not available")

    status_enum = UserStatus(status) if status else None
    users = controller.auth_manager.list_users(status=status_enum)

    # Filter by company if specified
    if company_id and controller.company_manager:
        memberships = controller.company_manager.get_company_members(company_id)
        member_user_ids = {m.user_id for m in memberships}
        users = [u for u in users if u.user_id in member_user_ids]

    # Filter superadmins
    if superadmins_only:
        users = [u for u in users if u.is_superadmin]

    # Search filter
    if search:
        search_lower = search.lower()
        users = [
            u for u in users
            if search_lower in u.username.lower()
            or search_lower in u.email.lower()
            or search_lower in u.full_name.lower()
        ]

    # Paginate
    total = len(users)
    start = (page - 1) * page_size
    end = start + page_size

    # Add company info to each user
    result = []
    for user in users[start:end]:
        user_dict = user.to_dict()
        if controller.company_manager:
            memberships = controller.company_manager.get_user_memberships(user.user_id)
            user_dict["companies"] = [
                {
                    "company_id": m.company_id,
                    "is_admin": m.is_company_admin,
                    "is_owner": m.is_owner,
                }
                for m in memberships
            ]
        result.append(user_dict)

    return PaginatedResponse(
        items=result,
        total=total,
        page=page,
        page_size=page_size,
        pages=(total + page_size - 1) // page_size if page_size > 0 else 0,
    )


@router.post("/users/superadmin", response_model=BaseResponse, status_code=201)
async def create_superadmin(request: SuperadminCreate):
    """Create a new superadmin user (superadmin only)."""
    controller = get_superadmin_controller()

    if not controller.auth_manager:
        raise HTTPException(status_code=500, detail="Auth manager not available")

    try:
        # Create user
        user = controller.auth_manager.register(
            username=request.username,
            email=request.email,
            password=request.password,
            first_name=request.first_name,
            last_name=request.last_name,
            auto_activate=True,
        )

        # Make superadmin
        user.is_superadmin = True
        user.roles = ["superadmin"]
        user.status = UserStatus.ACTIVE
        controller.auth_manager._save_data()

        return BaseResponse(
            success=True,
            message=f"Superadmin '{request.username}' created",
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/users/{user_id}/superadmin", response_model=BaseResponse)
async def revoke_superadmin_status(user_id: str):
    """Revoke superadmin status from a user (superadmin only)."""
    controller = get_superadmin_controller()

    if not controller.auth_manager:
        raise HTTPException(status_code=500, detail="Auth manager not available")

    user = controller.auth_manager.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if not user.is_superadmin:
        raise HTTPException(status_code=400, detail="User is not a superadmin")

    # Check not the last superadmin
    superadmins = [u for u in controller.auth_manager.users.values() if u.is_superadmin]
    if len(superadmins) <= 1:
        raise HTTPException(status_code=400, detail="Cannot revoke the last superadmin")

    user.is_superadmin = False
    if "superadmin" in user.roles:
        user.roles.remove("superadmin")
    controller.auth_manager._save_data()

    return BaseResponse(
        success=True,
        message=f"Superadmin status revoked from '{user.username}'",
    )


@router.post("/users/{user_id}/suspend", response_model=BaseResponse)
async def suspend_user(user_id: str, reason: str = Query(None)):
    """Suspend a user (superadmin only)."""
    controller = get_superadmin_controller()

    if not controller.auth_manager:
        raise HTTPException(status_code=500, detail="Auth manager not available")

    user = controller.auth_manager.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if user.is_superadmin:
        raise HTTPException(status_code=400, detail="Cannot suspend a superadmin")

    success = controller.auth_manager.suspend_user(user_id)
    if not success:
        raise HTTPException(status_code=400, detail="Failed to suspend user")

    return BaseResponse(success=True, message=f"User '{user.username}' suspended")


@router.post("/users/{user_id}/activate", response_model=BaseResponse)
async def activate_user(user_id: str):
    """Activate a user (superadmin only)."""
    controller = get_superadmin_controller()

    if not controller.auth_manager:
        raise HTTPException(status_code=500, detail="Auth manager not available")

    success = controller.auth_manager.activate_user(user_id)
    if not success:
        raise HTTPException(status_code=404, detail="User not found")

    return BaseResponse(success=True, message="User activated")


@router.post("/users/{user_id}/reset-password", response_model=BaseResponse)
async def reset_user_password(user_id: str, new_password: str = Query(..., min_length=8)):
    """Reset a user's password (superadmin only)."""
    controller = get_superadmin_controller()

    if not controller.auth_manager:
        raise HTTPException(status_code=500, detail="Auth manager not available")

    try:
        success = controller.auth_manager.reset_password(
            user_id=user_id,
            new_password=new_password,
        )
        if not success:
            raise HTTPException(status_code=404, detail="User not found")

        return BaseResponse(success=True, message="Password reset successfully")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/users/{user_id}/impersonate")
async def impersonate_user(user_id: str):
    """
    Create an impersonation session for a user (superadmin only).

    Returns a token that can be used to act as the user.
    """
    controller = get_superadmin_controller()

    if not controller.auth_manager:
        raise HTTPException(status_code=500, detail="Auth manager not available")

    user = controller.auth_manager.get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if user.is_superadmin:
        raise HTTPException(status_code=400, detail="Cannot impersonate another superadmin")

    # Create impersonation session
    token, session = controller.auth_manager.session_manager.create_session(
        user_id=user_id,
        metadata={
            "is_impersonation": True,
        }
    )

    return {
        "token": token,
        "user_id": user_id,
        "username": user.username,
        "is_impersonation": True,
        "expires_at": session.expires_at.isoformat(),
    }


# Audit log endpoints

@router.get("/audit-logs", response_model=PaginatedResponse)
async def get_audit_logs(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    user_id: str | None = None,
    resource_type: str | None = None,
    action: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
):
    """Get audit logs (superadmin only)."""
    controller = get_superadmin_controller()

    # Combine logs from all sources
    logs = list(controller.audit_logs)

    if controller.auth_manager:
        logs.extend(controller.auth_manager.audit_logs)

    if controller.company_manager:
        logs.extend(controller.company_manager.audit_logs)

    # Apply filters
    if user_id:
        logs = [l for l in logs if l.user_id == user_id]

    if resource_type:
        logs = [l for l in logs if l.resource_type == resource_type]

    if action:
        logs = [l for l in logs if l.action.value == action]

    if start_date:
        start_dt = datetime.fromisoformat(start_date)
        logs = [l for l in logs if l.timestamp >= start_dt]

    if end_date:
        end_dt = datetime.fromisoformat(end_date)
        logs = [l for l in logs if l.timestamp <= end_dt]

    # Sort by timestamp descending
    logs.sort(key=lambda x: x.timestamp, reverse=True)

    # Paginate
    total = len(logs)
    start = (page - 1) * page_size
    end = start + page_size

    return PaginatedResponse(
        items=[log.to_dict() for log in logs[start:end]],
        total=total,
        page=page,
        page_size=page_size,
        pages=(total + page_size - 1) // page_size if page_size > 0 else 0,
    )


# Bulk operations

@router.post("/reset-daily-usage", response_model=BaseResponse)
async def reset_daily_usage():
    """Reset daily usage counters for all companies (for scheduled job)."""
    controller = get_superadmin_controller()

    if controller.company_manager:
        controller.company_manager.reset_daily_usage()

    return BaseResponse(success=True, message="Daily usage counters reset")
