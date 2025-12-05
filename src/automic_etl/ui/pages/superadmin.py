"""Superadmin dashboard UI page."""

from __future__ import annotations

import httpx
import streamlit as st
from datetime import datetime, timedelta
from typing import Any

# API base URL
API_BASE_URL = "http://localhost:8000/api/v1"


def _get_api_client() -> httpx.Client:
    """Get configured HTTP client for API calls."""
    token = st.session_state.get("access_token", "")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return httpx.Client(base_url=API_BASE_URL, timeout=30.0, headers=headers)


def _get_platform_stats() -> dict[str, Any]:
    """Fetch platform statistics from API."""
    try:
        with _get_api_client() as client:
            response = client.get("/admin/statistics")
            if response.status_code == 200:
                return response.json()
    except Exception:
        pass
    return {
        "total_companies": 0,
        "total_users": 0,
        "active_pipelines": 0,
        "total_storage_bytes": 0,
        "companies_by_status": {},
        "companies_by_tier": {},
    }


def _get_companies(status: str | None = None, tier: str | None = None, search: str | None = None) -> list[dict]:
    """Fetch companies from API."""
    try:
        params = {"page_size": 50}
        if status and status != "All":
            params["status"] = status.lower()
        if tier and tier != "All":
            params["tier"] = tier.lower()
        if search:
            params["search"] = search

        with _get_api_client() as client:
            response = client.get("/admin/companies", params=params)
            if response.status_code == 200:
                data = response.json()
                return data.get("companies", [])
    except Exception:
        pass
    return []


def _get_users(user_type: str | None = None, status: str | None = None, search: str | None = None) -> list[dict]:
    """Fetch users from API."""
    try:
        params = {"page_size": 50}
        if user_type == "Superadmins Only":
            params["superadmin_only"] = True
        if status and status != "All":
            params["status"] = status.lower()
        if search:
            params["search"] = search

        with _get_api_client() as client:
            response = client.get("/admin/users", params=params)
            if response.status_code == 200:
                data = response.json()
                return data.get("users", [])
    except Exception:
        pass
    return []


def _get_global_settings() -> dict[str, Any]:
    """Fetch global settings from API."""
    try:
        with _get_api_client() as client:
            response = client.get("/admin/settings")
            if response.status_code == 200:
                return response.json()
    except Exception:
        pass
    return {}


def _get_system_health() -> dict[str, Any]:
    """Fetch system health from API."""
    try:
        with _get_api_client() as client:
            response = client.get("/admin/health")
            if response.status_code == 200:
                return response.json()
    except Exception:
        pass
    return {"status": "unknown", "components": []}


def _get_audit_logs(action_type: str | None = None, days: int = 7) -> list[dict]:
    """Fetch audit logs from API."""
    try:
        params = {"page_size": 100, "days": days}
        if action_type and action_type != "All":
            params["action_type"] = action_type

        with _get_api_client() as client:
            response = client.get("/admin/audit-logs", params=params)
            if response.status_code == 200:
                data = response.json()
                return data.get("logs", [])
    except Exception:
        pass
    return []


def _get_recent_activity() -> list[dict]:
    """Fetch recent platform activity from API."""
    try:
        with _get_api_client() as client:
            response = client.get("/admin/audit-logs", params={"page_size": 10})
            if response.status_code == 200:
                data = response.json()
                return data.get("logs", [])
    except Exception:
        pass
    return []


def _save_global_settings(settings: dict[str, Any]) -> bool:
    """Save global settings via API."""
    try:
        with _get_api_client() as client:
            response = client.put("/admin/settings", json=settings)
            return response.status_code in (200, 201)
    except Exception:
        pass
    return False


def _set_maintenance_mode(enabled: bool, message: str = "", allowed_ips: list[str] | None = None) -> bool:
    """Enable or disable maintenance mode via API."""
    try:
        with _get_api_client() as client:
            response = client.post(
                "/admin/maintenance",
                json={
                    "enabled": enabled,
                    "message": message,
                    "allowed_ips": allowed_ips or [],
                }
            )
            return response.status_code in (200, 201)
    except Exception:
        pass
    return False


def render_superadmin_page():
    """Render the superadmin dashboard page."""
    st.title("🛡️ Superadmin Dashboard")

    # Check if user is superadmin
    user = st.session_state.get("user", {})
    if not user.get("is_superadmin", False):
        st.error("Access denied. Superadmin privileges required.")
        return

    # Tabs for different sections
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "Overview",
        "Companies",
        "Users",
        "Global Settings",
        "System Health",
        "Audit Logs"
    ])

    with tab1:
        render_overview()

    with tab2:
        render_companies_section()

    with tab3:
        render_users_section()

    with tab4:
        render_global_settings()

    with tab5:
        render_system_health()

    with tab6:
        render_audit_logs()


def render_overview():
    """Render platform overview dashboard."""
    st.subheader("Platform Overview")

    # Fetch platform statistics
    stats = _get_platform_stats()

    # Key metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Companies", stats.get("total_companies", 0))

    with col2:
        st.metric("Total Users", stats.get("total_users", 0))

    with col3:
        st.metric("Active Pipelines", stats.get("active_pipelines", 0))

    with col4:
        storage_bytes = stats.get("total_storage_bytes", 0)
        if storage_bytes >= 1e12:
            storage_str = f"{storage_bytes / 1e12:.1f} TB"
        elif storage_bytes >= 1e9:
            storage_str = f"{storage_bytes / 1e9:.1f} GB"
        else:
            storage_str = f"{storage_bytes / 1e6:.1f} MB"
        st.metric("Storage Used", storage_str)

    st.divider()

    # Status breakdown
    col1, col2 = st.columns(2)
    total_companies = max(stats.get("total_companies", 1), 1)

    with col1:
        st.markdown("**Companies by Status**")
        status_data = stats.get("companies_by_status", {})
        if status_data:
            for status, count in status_data.items():
                st.progress(min(count / total_companies, 1.0), text=f"{status.title()}: {count}")
        else:
            st.info("No company data available")

    with col2:
        st.markdown("**Companies by Tier**")
        tier_data = stats.get("companies_by_tier", {})
        if tier_data:
            for tier, count in tier_data.items():
                st.progress(min(count / total_companies, 1.0), text=f"{tier.title()}: {count}")
        else:
            st.info("No tier data available")

    st.divider()

    # Recent activity
    st.markdown("**Recent Activity**")
    activities = _get_recent_activity()

    if activities:
        for activity in activities[:5]:
            col1, col2, col3 = st.columns([1, 2, 2])
            with col1:
                timestamp = activity.get("timestamp", "")
                if timestamp:
                    try:
                        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                        delta = datetime.now(dt.tzinfo) - dt if dt.tzinfo else datetime.now() - dt
                        if delta.days > 0:
                            time_str = f"{delta.days}d ago"
                        elif delta.seconds > 3600:
                            time_str = f"{delta.seconds // 3600}h ago"
                        elif delta.seconds > 60:
                            time_str = f"{delta.seconds // 60}m ago"
                        else:
                            time_str = "just now"
                        st.caption(time_str)
                    except Exception:
                        st.caption(timestamp[:16] if len(timestamp) > 16 else timestamp)
                else:
                    st.caption("—")
            with col2:
                st.text(activity.get("action", "Unknown action"))
            with col3:
                details = activity.get("details", {})
                detail_str = details.get("description", "") or details.get("target", "") or str(details)[:30]
                st.text(detail_str)
    else:
        st.info("No recent activity")

    # Quick actions
    st.divider()
    st.markdown("**Quick Actions**")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if st.button("🏢 New Company", use_container_width=True):
            st.session_state["show_new_company_modal"] = True

    with col2:
        if st.button("👤 New Superadmin", use_container_width=True):
            st.session_state["show_new_superadmin_modal"] = True

    with col3:
        if st.button("🔧 Maintenance Mode", use_container_width=True):
            st.session_state["show_maintenance_modal"] = True

    with col4:
        if st.button("📊 Export Report", use_container_width=True):
            _export_platform_report()


def _export_platform_report():
    """Export platform report."""
    import json
    stats = _get_platform_stats()
    companies = _get_companies()
    users = _get_users()

    report = {
        "generated_at": datetime.now().isoformat(),
        "statistics": stats,
        "companies_count": len(companies),
        "users_count": len(users),
    }

    st.download_button(
        "Download Report",
        data=json.dumps(report, indent=2, default=str),
        file_name=f"platform_report_{datetime.now().strftime('%Y%m%d')}.json",
        mime="application/json",
    )


def render_companies_section():
    """Render companies management section."""
    st.subheader("Company Management")

    # Filters
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        status_filter = st.selectbox(
            "Status",
            ["All", "Active", "Trial", "Suspended", "Pending", "Inactive"]
        )

    with col2:
        tier_filter = st.selectbox(
            "Tier",
            ["All", "Free", "Starter", "Professional", "Enterprise"]
        )

    with col3:
        search = st.text_input("Search", placeholder="Company name or email...")

    with col4:
        st.write("")  # Spacer
        st.write("")
        if st.button("+ Create Company"):
            st.session_state["show_new_company_modal"] = True

    # Fetch companies from API
    companies = _get_companies(
        status=status_filter if status_filter != "All" else None,
        tier=tier_filter if tier_filter != "All" else None,
        search=search if search else None,
    )

    if not companies:
        st.info("No companies found matching the filters.")
        _render_new_company_modal()
        return

    # Companies table
    for company in companies:
        with st.container():
            col1, col2, col3, col4, col5, col6 = st.columns([2, 1, 1, 1, 1, 2])

            with col1:
                status_icon = {
                    "active": "🟢",
                    "trial": "🟡",
                    "suspended": "🔴",
                    "pending": "🟠",
                    "inactive": "⚫",
                }.get(company["status"], "⚪")
                st.markdown(f"**{company['name']}** {status_icon}")
                st.caption(f"/{company['slug']}")

            with col2:
                tier_badge = company["tier"].title()
                st.text(f"📦 {tier_badge}")

            with col3:
                st.text(f"👥 {company['user_count']}")

            with col4:
                st.text(f"📊 {company['pipeline_count']}")

            with col5:
                st.caption(company["created_at"])

            with col6:
                btn_col1, btn_col2, btn_col3 = st.columns(3)
                company_id = company.get("company_id") or company.get("id")
                with btn_col1:
                    if st.button("👁️", key=f"view_{company_id}", help="View"):
                        st.session_state["viewing_company"] = company
                with btn_col2:
                    if st.button("✏️", key=f"edit_{company_id}", help="Edit"):
                        st.session_state["editing_company"] = company
                with btn_col3:
                    status = company.get("status", "").lower()
                    if status == "active":
                        if st.button("⏸️", key=f"suspend_{company_id}", help="Suspend"):
                            _suspend_company(company_id, company.get("name", ""))
                    elif status == "suspended":
                        if st.button("▶️", key=f"activate_{company_id}", help="Activate"):
                            _activate_company(company_id, company.get("name", ""))

            st.divider()

    # Edit company modal
    if st.session_state.get("editing_company"):
        company = st.session_state["editing_company"]
        company_id = company.get("company_id") or company.get("id")
        current_tier = company.get("tier", "free").lower()
        current_status = company.get("status", "active").lower()

        with st.expander(f"Edit {company.get('name', 'Company')}", expanded=True):
            col1, col2 = st.columns(2)

            tier_options = ["free", "starter", "professional", "enterprise"]
            status_options = ["active", "trial", "suspended", "pending"]

            with col1:
                new_tier = st.selectbox(
                    "Tier",
                    tier_options,
                    index=tier_options.index(current_tier) if current_tier in tier_options else 0
                )

            with col2:
                new_status = st.selectbox(
                    "Status",
                    status_options,
                    index=status_options.index(current_status) if current_status in status_options else 0
                )

            custom_limits = st.checkbox("Set Custom Limits")
            limits = {}
            if custom_limits:
                col1, col2, col3 = st.columns(3)
                with col1:
                    limits["max_users"] = st.number_input("Max Users", min_value=1, value=50)
                with col2:
                    limits["max_pipelines"] = st.number_input("Max Pipelines", min_value=1, value=100)
                with col3:
                    limits["max_storage_gb"] = st.number_input("Max Storage (GB)", min_value=1, value=500)

            col1, col2 = st.columns(2)
            with col1:
                if st.button("Save Changes"):
                    _update_company(company_id, new_tier, new_status, limits if custom_limits else None)
                    del st.session_state["editing_company"]
                    st.rerun()
            with col2:
                if st.button("Cancel"):
                    del st.session_state["editing_company"]
                    st.rerun()

    # New company modal
    _render_new_company_modal()


def _suspend_company(company_id: str, name: str):
    """Suspend a company via API."""
    try:
        with _get_api_client() as client:
            response = client.post(f"/admin/companies/{company_id}/suspend")
            if response.status_code == 200:
                st.success(f"Suspended {name}")
                st.rerun()
            else:
                st.error(f"Failed to suspend: {response.text}")
    except Exception as e:
        st.error(f"Error: {e}")


def _activate_company(company_id: str, name: str):
    """Activate a company via API."""
    try:
        with _get_api_client() as client:
            response = client.post(f"/admin/companies/{company_id}/approve")
            if response.status_code == 200:
                st.success(f"Activated {name}")
                st.rerun()
            else:
                st.error(f"Failed to activate: {response.text}")
    except Exception as e:
        st.error(f"Error: {e}")


def _update_company(company_id: str, tier: str, status: str, limits: dict | None):
    """Update company via API."""
    try:
        with _get_api_client() as client:
            # Update tier if changed
            response = client.put(
                f"/admin/companies/{company_id}/tier",
                json={"tier": tier, "custom_limits": limits}
            )
            if response.status_code == 200:
                st.success("Company updated!")
            else:
                st.error(f"Failed to update: {response.text}")
    except Exception as e:
        st.error(f"Error: {e}")


def _render_new_company_modal():
    """Render new company creation modal."""
    if st.session_state.get("show_new_company_modal"):
        with st.expander("Create New Company", expanded=True):
            col1, col2 = st.columns(2)

            with col1:
                name = st.text_input("Company Name", key="new_company_name")
                slug = st.text_input("Slug", key="new_company_slug",
                                    value=name.lower().replace(" ", "-") if name else "")

            with col2:
                tier = st.selectbox("Tier", ["free", "starter", "professional", "enterprise"],
                                   key="new_company_tier")
                owner_email = st.text_input("Owner Email", key="new_company_owner")

            col1, col2 = st.columns(2)
            with col1:
                if st.button("Create Company"):
                    if name and owner_email:
                        _create_company(name, slug, tier, owner_email)
                        st.session_state["show_new_company_modal"] = False
                        st.rerun()
                    else:
                        st.error("Name and owner email are required")
            with col2:
                if st.button("Cancel", key="cancel_new_company"):
                    st.session_state["show_new_company_modal"] = False
                    st.rerun()


def _create_company(name: str, slug: str, tier: str, owner_email: str):
    """Create a new company via API."""
    try:
        with _get_api_client() as client:
            response = client.post(
                "/companies",
                json={
                    "name": name,
                    "slug": slug or name.lower().replace(" ", "-"),
                    "tier": tier,
                    "owner_email": owner_email,
                }
            )
            if response.status_code in (200, 201):
                st.success(f"Company '{name}' created!")
            else:
                st.error(f"Failed to create company: {response.text}")
    except Exception as e:
        st.error(f"Error: {e}")


def render_users_section():
    """Render users management section."""
    st.subheader("User Management")

    # Filters
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        user_type = st.selectbox(
            "User Type",
            ["All Users", "Superadmins Only", "Regular Users"]
        )

    with col2:
        status_filter = st.selectbox(
            "Status",
            ["All", "Active", "Pending", "Suspended", "Inactive"],
            key="user_status_filter"
        )

    with col3:
        search = st.text_input("Search", placeholder="Username or email...", key="user_search")

    with col4:
        st.write("")
        st.write("")
        if st.button("+ Create Superadmin"):
            st.session_state["show_new_superadmin_modal"] = True

    # Fetch users from API
    users = _get_users(
        user_type=user_type if user_type != "All Users" else None,
        status=status_filter if status_filter != "All" else None,
        search=search if search else None,
    )

    if not users:
        st.info("No users found matching the filters.")
        _render_new_superadmin_modal()
        return

    # Users table
    for user in users:
        with st.container():
            col1, col2, col3, col4, col5 = st.columns([2, 2, 2, 1, 2])
            user_id = user.get("user_id") or user.get("id")
            is_superadmin = user.get("is_superadmin", False)
            status = user.get("status", "active").lower()

            with col1:
                badge = " 🛡️" if is_superadmin else ""
                status_icon = "🟢" if status == "active" else "🔴"
                st.markdown(f"**{user.get('username', 'N/A')}**{badge} {status_icon}")
                st.caption(user.get("email", ""))

            with col2:
                companies = user.get("companies", [])
                if companies:
                    companies_str = ", ".join([c.get("name", "") for c in companies if isinstance(c, dict)])
                    st.text(companies_str or "—")
                else:
                    st.text("Platform admin" if is_superadmin else "No company")

            with col3:
                last_login = user.get("last_login", "")
                last_login_str = last_login[:10] if last_login else "Never"
                st.text(f"Last login: {last_login_str}")

            with col4:
                st.text(status.title())

            with col5:
                btn_col1, btn_col2, btn_col3 = st.columns(3)
                with btn_col1:
                    if st.button("🔑", key=f"reset_pw_{user_id}", help="Reset Password"):
                        _reset_user_password(user_id, user.get("email", ""))
                with btn_col2:
                    if st.button("👤", key=f"impersonate_{user_id}", help="Impersonate"):
                        if not is_superadmin:
                            _impersonate_user(user_id, user.get("username", ""))
                        else:
                            st.error("Cannot impersonate superadmins")
                with btn_col3:
                    if status == "active" and not is_superadmin:
                        if st.button("⏸️", key=f"suspend_user_{user_id}", help="Suspend"):
                            _suspend_user(user_id, user.get("username", ""))
                    elif status == "suspended":
                        if st.button("▶️", key=f"activate_user_{user_id}", help="Activate"):
                            _activate_user(user_id, user.get("username", ""))

            st.divider()

    # New superadmin modal
    _render_new_superadmin_modal()


def _reset_user_password(user_id: str, email: str):
    """Reset user password via API."""
    try:
        with _get_api_client() as client:
            response = client.post(f"/admin/users/{user_id}/reset-password")
            if response.status_code == 200:
                st.info(f"Password reset email sent to {email}")
            else:
                st.error(f"Failed to reset password: {response.text}")
    except Exception as e:
        st.error(f"Error: {e}")


def _impersonate_user(user_id: str, username: str):
    """Impersonate a user via API."""
    try:
        with _get_api_client() as client:
            response = client.post(f"/admin/users/{user_id}/impersonate")
            if response.status_code == 200:
                data = response.json()
                st.session_state["impersonating"] = True
                st.session_state["impersonating_user"] = username
                st.session_state["impersonation_token"] = data.get("token")
                st.warning(f"Now impersonating {username}. Actions will be performed as this user.")
            else:
                st.error(f"Failed to impersonate: {response.text}")
    except Exception as e:
        st.error(f"Error: {e}")


def _suspend_user(user_id: str, username: str):
    """Suspend a user via API."""
    try:
        with _get_api_client() as client:
            response = client.post(f"/admin/users/{user_id}/suspend")
            if response.status_code == 200:
                st.warning(f"Suspended {username}")
                st.rerun()
            else:
                st.error(f"Failed to suspend: {response.text}")
    except Exception as e:
        st.error(f"Error: {e}")


def _activate_user(user_id: str, username: str):
    """Activate a user via API."""
    try:
        with _get_api_client() as client:
            response = client.post(f"/admin/users/{user_id}/activate")
            if response.status_code == 200:
                st.success(f"Activated {username}")
                st.rerun()
            else:
                st.error(f"Failed to activate: {response.text}")
    except Exception as e:
        st.error(f"Error: {e}")


def _render_new_superadmin_modal():
    """Render new superadmin creation modal."""
    if st.session_state.get("show_new_superadmin_modal"):
        with st.expander("Create New Superadmin", expanded=True):
            with st.form("new_superadmin_form"):
                username = st.text_input("Username")
                email = st.text_input("Email")
                password = st.text_input("Password", type="password")
                confirm_password = st.text_input("Confirm Password", type="password")

                col1, col2 = st.columns(2)
                with col1:
                    submitted = st.form_submit_button("Create Superadmin")
                with col2:
                    if st.form_submit_button("Cancel"):
                        st.session_state["show_new_superadmin_modal"] = False
                        st.rerun()

                if submitted:
                    if not username or not email or not password:
                        st.error("All fields are required")
                    elif password != confirm_password:
                        st.error("Passwords don't match")
                    else:
                        _create_superadmin(username, email, password)


def _create_superadmin(username: str, email: str, password: str):
    """Create a new superadmin via API."""
    try:
        with _get_api_client() as client:
            response = client.post(
                "/admin/users/superadmin",
                json={
                    "username": username,
                    "email": email,
                    "password": password,
                }
            )
            if response.status_code in (200, 201):
                st.success(f"Superadmin '{username}' created!")
                st.session_state["show_new_superadmin_modal"] = False
                st.rerun()
            else:
                st.error(f"Failed to create superadmin: {response.text}")
    except Exception as e:
        st.error(f"Error: {e}")


def render_global_settings():
    """Render global settings section."""
    st.subheader("Global Platform Settings")

    # Fetch current settings from API
    current_settings = _get_global_settings()

    with st.form("global_settings_form"):
        # Platform settings
        st.markdown("**Platform Configuration**")
        col1, col2 = st.columns(2)

        with col1:
            platform_name = st.text_input(
                "Platform Name",
                value=current_settings.get("platform_name", "Automic ETL")
            )
            platform_url = st.text_input(
                "Platform URL",
                value=current_settings.get("platform_url", "http://localhost:8501")
            )

        with col2:
            support_email = st.text_input(
                "Support Email",
                value=current_settings.get("support_email", "support@automic-etl.com")
            )
            tier_options = ["free", "starter", "trial"]
            default_tier_value = current_settings.get("default_tier", "free")
            default_tier_idx = tier_options.index(default_tier_value) if default_tier_value in tier_options else 0
            default_tier = st.selectbox("Default Company Tier", tier_options, index=default_tier_idx)

        st.divider()

        # Registration settings
        st.markdown("**Registration & Access**")
        col1, col2 = st.columns(2)

        with col1:
            allow_registration = st.checkbox(
                "Allow Self Registration",
                value=current_settings.get("allow_registration", True)
            )
            require_email_verification = st.checkbox(
                "Require Email Verification",
                value=current_settings.get("require_email_verification", True)
            )

        with col2:
            auto_approve = st.checkbox(
                "Auto-Approve Companies",
                value=current_settings.get("auto_approve_companies", False)
            )
            enable_api = st.checkbox(
                "Enable API Access",
                value=current_settings.get("enable_api", True)
            )

        st.divider()

        # Security settings
        st.markdown("**Security**")
        col1, col2 = st.columns(2)

        with col1:
            password_min_length = st.number_input(
                "Min Password Length",
                min_value=6,
                max_value=32,
                value=current_settings.get("password_min_length", 8)
            )
            max_login_attempts = st.number_input(
                "Max Failed Login Attempts",
                min_value=3,
                max_value=20,
                value=current_settings.get("max_login_attempts", 5)
            )

        with col2:
            lockout_duration = st.number_input(
                "Lockout Duration (minutes)",
                min_value=5,
                max_value=1440,
                value=current_settings.get("lockout_duration_minutes", 30)
            )
            session_timeout = st.number_input(
                "Session Timeout (minutes)",
                min_value=15,
                max_value=1440,
                value=current_settings.get("session_timeout_minutes", 480)
            )

        require_mfa_admins = st.checkbox(
            "Require MFA for Admins",
            value=current_settings.get("require_mfa_admins", False)
        )

        st.divider()

        # Feature flags
        st.markdown("**Feature Flags**")
        col1, col2 = st.columns(2)

        with col1:
            enable_llm = st.checkbox(
                "Enable LLM Features",
                value=current_settings.get("enable_llm", True)
            )
            enable_streaming = st.checkbox(
                "Enable Streaming Connectors",
                value=current_settings.get("enable_streaming", True)
            )

        with col2:
            enable_spark = st.checkbox(
                "Enable Spark Integration",
                value=current_settings.get("enable_spark", True)
            )

        st.divider()

        # Platform limits
        st.markdown("**Platform Limits**")
        col1, col2, col3 = st.columns(3)

        with col1:
            max_companies = st.number_input(
                "Max Companies",
                min_value=1,
                value=current_settings.get("max_companies", 1000)
            )

        with col2:
            max_users = st.number_input(
                "Max Total Users",
                min_value=1,
                value=current_settings.get("max_users", 10000)
            )

        with col3:
            api_rate_limit = st.number_input(
                "Default API Rate Limit",
                min_value=100,
                value=current_settings.get("api_rate_limit", 1000)
            )

        submitted = st.form_submit_button("Save Settings")
        if submitted:
            settings_to_save = {
                "platform_name": platform_name,
                "platform_url": platform_url,
                "support_email": support_email,
                "default_tier": default_tier,
                "allow_registration": allow_registration,
                "require_email_verification": require_email_verification,
                "auto_approve_companies": auto_approve,
                "enable_api": enable_api,
                "password_min_length": password_min_length,
                "max_login_attempts": max_login_attempts,
                "lockout_duration_minutes": lockout_duration,
                "session_timeout_minutes": session_timeout,
                "require_mfa_admins": require_mfa_admins,
                "enable_llm": enable_llm,
                "enable_streaming": enable_streaming,
                "enable_spark": enable_spark,
                "max_companies": max_companies,
                "max_users": max_users,
                "api_rate_limit": api_rate_limit,
            }
            if _save_global_settings(settings_to_save):
                st.success("Global settings saved!")
            else:
                st.error("Failed to save settings. Please try again.")

    # Maintenance mode
    st.divider()
    st.markdown("**Maintenance Mode**")

    maintenance_mode = current_settings.get("maintenance_mode", {}).get("enabled", False)

    col1, col2 = st.columns([3, 1])

    with col1:
        if maintenance_mode:
            st.error("⚠️ Maintenance mode is ACTIVE")
        else:
            st.success("✅ System is operational")

    with col2:
        if maintenance_mode:
            if st.button("Disable Maintenance"):
                if _set_maintenance_mode(enabled=False):
                    st.success("Maintenance mode disabled")
                    st.rerun()
                else:
                    st.error("Failed to disable maintenance mode")
        else:
            if st.button("Enable Maintenance"):
                st.session_state["show_maintenance_modal"] = True

    if st.session_state.get("show_maintenance_modal"):
        with st.expander("Enable Maintenance Mode", expanded=True):
            message = st.text_area(
                "Maintenance Message",
                value="System is under maintenance. Please try again later."
            )
            allowed_ips = st.text_area(
                "Allowed IPs (one per line)",
                placeholder="127.0.0.1"
            )

            col1, col2 = st.columns(2)
            with col1:
                if st.button("Confirm Enable"):
                    ip_list = [ip.strip() for ip in allowed_ips.split("\n") if ip.strip()]
                    if _set_maintenance_mode(enabled=True, message=message, allowed_ips=ip_list):
                        st.session_state["show_maintenance_modal"] = False
                        st.warning("Maintenance mode enabled!")
                        st.rerun()
                    else:
                        st.error("Failed to enable maintenance mode")
            with col2:
                if st.button("Cancel", key="cancel_maintenance"):
                    st.session_state["show_maintenance_modal"] = False
                    st.rerun()


def render_system_health():
    """Render system health section."""
    st.subheader("System Health")

    # Fetch system health from API
    health = _get_system_health()

    # Overall status
    overall_status = health.get("status", "unknown")
    uptime = health.get("uptime_percent", 0)
    response_time = health.get("avg_response_time_ms", 0)

    col1, col2, col3 = st.columns(3)

    with col1:
        if overall_status == "healthy":
            st.markdown("### 🟢 Healthy")
        elif overall_status == "degraded":
            st.markdown("### 🟡 Degraded")
        elif overall_status == "unhealthy":
            st.markdown("### 🔴 Unhealthy")
        else:
            st.markdown("### ⚪ Unknown")
        st.caption("Overall system status")

    with col2:
        st.metric("Uptime", f"{uptime:.1f}%" if uptime else "N/A")

    with col3:
        st.metric("Response Time", f"{response_time:.0f}ms" if response_time else "N/A")

    st.divider()

    # Refresh button
    if st.button("Refresh Health Status"):
        st.rerun()

    # Component status
    st.markdown("**Component Status**")

    components = health.get("components", [])

    if not components:
        st.info("No component health data available. The health API endpoint may not be configured.")
    else:
        for component in components:
            col1, col2, col3, col4 = st.columns([2, 1, 1, 2])

            status = component.get("status", "unknown")
            with col1:
                if status == "healthy":
                    status_icon = "🟢"
                elif status == "degraded":
                    status_icon = "🟡"
                elif status == "unhealthy":
                    status_icon = "🔴"
                else:
                    status_icon = "⚪"
                st.markdown(f"{status_icon} **{component.get('name', 'Unknown')}**")

            with col2:
                st.text(status.title())

            with col3:
                latency = component.get("latency_ms", component.get("latency"))
                if latency:
                    st.text(f"{latency}ms" if isinstance(latency, (int, float)) else latency)
                else:
                    st.text("-")

            with col4:
                st.caption(component.get("details", "-"))

    st.divider()

    # Warnings and errors
    st.markdown("**Recent Warnings & Errors**")

    alerts = health.get("alerts", health.get("warnings", []))

    if not alerts:
        st.success("No recent warnings or errors")
    else:
        for alert in alerts:
            level = alert.get("level", "warning")
            icon = "⚠️" if level == "warning" else "❌"
            col1, col2 = st.columns([1, 4])
            with col1:
                st.caption(alert.get("time", alert.get("timestamp", "-")))
            with col2:
                st.text(f"{icon} {alert.get('message', '-')}")


def render_audit_logs():
    """Render audit logs section."""
    st.subheader("Audit Logs")

    # Filters
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        action_filter = st.selectbox(
            "Action Type",
            ["All", "login", "user_created", "user_updated", "company_created", "settings_changed", "login_failed"]
        )

    with col2:
        resource_filter = st.selectbox(
            "Resource Type",
            ["All", "user", "company", "pipeline", "system"]
        )

    with col3:
        days_back = st.selectbox("Time Period", [7, 14, 30, 90], format_func=lambda x: f"Last {x} days")

    with col4:
        if st.button("Refresh Logs"):
            st.rerun()

    # Fetch audit logs from API
    logs = _get_audit_logs(action_type=action_filter, days=days_back)

    # Apply resource type filter
    if resource_filter != "All":
        logs = [log for log in logs if log.get("resource_type") == resource_filter]

    # Logs table
    st.markdown("---")

    if not logs:
        st.info("No audit logs found for the selected filters.")
    else:
        for log in logs:
            col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 3, 1])

            with col1:
                timestamp = log.get("timestamp", "")
                if timestamp:
                    st.caption(timestamp[:19].replace("T", " "))
                else:
                    st.caption("-")

            with col2:
                st.text(log.get("user", log.get("actor", "-")))

            with col3:
                action = log.get("action", "-")
                st.text(action.replace("_", " ").title())

            with col4:
                st.caption(log.get("details", log.get("description", "-")))

            with col5:
                success = log.get("success", True)
                icon = "✅" if success else "❌"
                st.text(icon)

    # Pagination info
    st.divider()
    col1, col2, col3 = st.columns([2, 1, 2])
    with col2:
        st.caption(f"Showing {len(logs)} entries")

    # Export button
    if st.button("Export Audit Logs"):
        _export_audit_logs(logs)


def _export_audit_logs(logs: list[dict]):
    """Export audit logs to CSV."""
    import csv
    import io

    if not logs:
        st.warning("No logs to export")
        return

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["timestamp", "user", "action", "resource_type", "resource_id", "details", "success"])
    writer.writeheader()

    for log in logs:
        writer.writerow({
            "timestamp": log.get("timestamp", ""),
            "user": log.get("user", log.get("actor", "")),
            "action": log.get("action", ""),
            "resource_type": log.get("resource_type", ""),
            "resource_id": log.get("resource_id", ""),
            "details": log.get("details", log.get("description", "")),
            "success": log.get("success", True),
        })

    st.download_button(
        label="Download CSV",
        data=output.getvalue(),
        file_name=f"audit_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )


# Helper to be called from main app
def show_superadmin():
    """Entry point for superadmin page."""
    render_superadmin_page()

