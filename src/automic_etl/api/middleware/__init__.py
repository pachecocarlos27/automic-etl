"""API middleware for Automic ETL."""

from automic_etl.api.middleware.security import (
    # Dependencies
    get_current_user,
    get_current_user_required,
    get_security_context,
    require_permission,
    require_data_tier,
    require_resource_access,
    require_superadmin,
    require_company_admin,
    # Middleware
    TenantMiddleware,
    AuditMiddleware,
    RateLimitMiddleware,
    MaintenanceModeMiddleware,
    # Helpers
    apply_rls_filters,
    check_resource_access,
    filter_by_company,
    # Schemes
    security_scheme,
)

__all__ = [
    # Dependencies
    "get_current_user",
    "get_current_user_required",
    "get_security_context",
    "require_permission",
    "require_data_tier",
    "require_resource_access",
    "require_superadmin",
    "require_company_admin",
    # Middleware
    "TenantMiddleware",
    "AuditMiddleware",
    "RateLimitMiddleware",
    "MaintenanceModeMiddleware",
    # Helpers
    "apply_rls_filters",
    "check_resource_access",
    "filter_by_company",
    # Schemes
    "security_scheme",
]
