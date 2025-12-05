"""Company administration UI page."""

from __future__ import annotations

import httpx
import streamlit as st
from datetime import datetime
from typing import Any

# API base URL
API_BASE_URL = "http://localhost:8000/api/v1"


def _get_api_client() -> httpx.Client:
    """Get configured HTTP client for API calls."""
    token = st.session_state.get("access_token", "")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return httpx.Client(base_url=API_BASE_URL, timeout=30.0, headers=headers)


def _get_company_members() -> list[dict[str, Any]]:
    """Fetch company members from API."""
    try:
        company_id = st.session_state.get("current_company", {}).get("id")
        if not company_id:
            return []
        with _get_api_client() as client:
            response = client.get(f"/companies/{company_id}/members")
            if response.status_code == 200:
                return response.json().get("members", [])
            return []
    except Exception:
        return []


def _get_company_invitations() -> list[dict[str, Any]]:
    """Fetch company invitations from API."""
    try:
        company_id = st.session_state.get("current_company", {}).get("id")
        if not company_id:
            return []
        with _get_api_client() as client:
            response = client.get(f"/companies/{company_id}/invitations")
            if response.status_code == 200:
                return response.json().get("invitations", [])
            return []
    except Exception:
        return []


def _send_invitation(email: str, roles: list[str], message: str = "", expires_days: int = 7) -> bool:
    """Send invitation via API."""
    try:
        company_id = st.session_state.get("current_company", {}).get("id")
        if not company_id:
            return False
        with _get_api_client() as client:
            response = client.post(
                f"/companies/{company_id}/invitations",
                json={
                    "email": email,
                    "role_ids": roles,
                    "message": message,
                    "expires_days": expires_days,
                }
            )
            return response.status_code in (200, 201)
    except Exception:
        return False


def _revoke_invitation(invitation_id: str) -> bool:
    """Revoke invitation via API."""
    try:
        company_id = st.session_state.get("current_company", {}).get("id")
        if not company_id:
            return False
        with _get_api_client() as client:
            response = client.delete(f"/companies/{company_id}/invitations/{invitation_id}")
            return response.status_code in (200, 204)
    except Exception:
        return False


def _update_member(user_id: str, roles: list[str], is_admin: bool) -> bool:
    """Update member via API."""
    try:
        company_id = st.session_state.get("current_company", {}).get("id")
        if not company_id:
            return False
        with _get_api_client() as client:
            response = client.put(
                f"/companies/{company_id}/members/{user_id}",
                json={
                    "role_ids": roles,
                    "is_company_admin": is_admin,
                }
            )
            return response.status_code == 200
    except Exception:
        return False


def _remove_member(user_id: str) -> bool:
    """Remove member via API."""
    try:
        company_id = st.session_state.get("current_company", {}).get("id")
        if not company_id:
            return False
        with _get_api_client() as client:
            response = client.delete(f"/companies/{company_id}/members/{user_id}")
            return response.status_code in (200, 204)
    except Exception:
        return False


def _save_company_settings(settings: dict[str, Any]) -> bool:
    """Save company settings via API."""
    try:
        company_id = st.session_state.get("current_company", {}).get("id")
        if not company_id:
            return False
        with _get_api_client() as client:
            response = client.put(
                f"/companies/{company_id}/settings",
                json=settings
            )
            return response.status_code == 200
    except Exception:
        return False


def render_company_admin_page():
    """Render the company administration page."""
    st.title("Company Administration")

    # Check if user is company admin
    user = st.session_state.get("user", {})
    membership = st.session_state.get("current_membership", {})

    if not membership.get("is_company_admin", False) and not user.get("is_superadmin", False):
        st.warning("You don't have permission to access company administration.")
        return

    # Company selector for superadmins
    company = st.session_state.get("current_company", {})

    st.markdown(f"### Managing: **{company.get('name', 'Unknown Company')}**")

    # Tabs for different sections
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Overview",
        "Members",
        "Invitations",
        "Settings",
        "Usage & Limits"
    ])

    with tab1:
        render_company_overview(company)

    with tab2:
        render_members_section(company)

    with tab3:
        render_invitations_section(company)

    with tab4:
        render_company_settings(company)

    with tab5:
        render_usage_section(company)


def render_company_overview(company: dict):
    """Render company overview section."""
    st.subheader("Company Overview")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Status", company.get("status", "unknown").title())

    with col2:
        st.metric("Tier", company.get("tier", "free").title())

    with col3:
        usage = company.get("usage", {})
        st.metric("Members", usage.get("user_count", 0))

    with col4:
        st.metric("Pipelines", usage.get("pipeline_count", 0))

    st.divider()

    # Company details
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Company Information**")
        st.text(f"Slug: {company.get('slug', 'N/A')}")
        st.text(f"Owner: {company.get('owner_user_id', 'N/A')[:8]}...")
        st.text(f"Billing Email: {company.get('billing_email', 'Not set')}")
        st.text(f"Support Email: {company.get('support_email', 'Not set')}")

    with col2:
        st.markdown("**Dates**")
        created_at = company.get("created_at", "")
        if created_at:
            st.text(f"Created: {created_at[:10]}")
        if company.get("trial_ends_at"):
            st.text(f"Trial Ends: {company['trial_ends_at'][:10]}")
            if company.get("is_trial_expired"):
                st.error("Trial has expired!")

    # Quick actions
    st.divider()
    st.markdown("**Quick Actions**")

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("Invite Member", key="quick_invite"):
            st.session_state["show_invite_modal"] = True

    with col2:
        if st.button("Update Settings", key="quick_settings"):
            st.rerun()

    with col3:
        if st.button("View Audit Log", key="quick_audit"):
            st.info("Audit log viewing coming soon")


def render_members_section(company: dict):
    """Render members management section."""
    st.subheader("Team Members")

    # Add member button
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("+ Invite Member"):
            st.session_state["show_invite_modal"] = True

    # Fetch members from API
    members = _get_company_members()

    # Members table
    for member in members:
        with st.container():
            col1, col2, col3, col4, col5 = st.columns([2, 2, 2, 1, 1])

            with col1:
                badges = ""
                if member["is_owner"]:
                    badges = " 👑"
                elif member["is_company_admin"]:
                    badges = " ⭐"
                st.markdown(f"**{member['username']}**{badges}")
                st.caption(member["email"])

            with col2:
                roles = ", ".join(member["role_ids"])
                st.text(f"Roles: {roles}")

            with col3:
                joined = member["joined_at"][:10] if member["joined_at"] else "N/A"
                st.text(f"Joined: {joined}")

            with col4:
                status_color = "🟢" if member["status"] == "active" else "🔴"
                st.text(f"{status_color} {member['status']}")

            with col5:
                if not member["is_owner"]:
                    if st.button("⚙️", key=f"edit_{member['user_id']}"):
                        st.session_state["editing_member"] = member

            st.divider()

    # Edit member modal
    if st.session_state.get("editing_member"):
        member = st.session_state["editing_member"]
        with st.expander(f"Edit {member['username']}", expanded=True):
            new_roles = st.multiselect(
                "Roles",
                ["admin", "manager", "analyst", "viewer"],
                default=member.get("role_ids", []),
                key=f"roles_{member['user_id']}"
            )

            is_admin = st.checkbox(
                "Company Admin",
                value=member.get("is_company_admin", False),
                key=f"admin_{member['user_id']}"
            )

            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("Save Changes", key=f"save_{member['user_id']}"):
                    if _update_member(member["user_id"], new_roles, is_admin):
                        st.success("Member updated!")
                        del st.session_state["editing_member"]
                        st.rerun()
                    else:
                        st.error("Failed to update member")

            with col2:
                if st.button("Remove Member", key=f"remove_{member['user_id']}", type="secondary"):
                    st.session_state["confirm_remove_member"] = member["user_id"]

            with col3:
                if st.button("Cancel", key=f"cancel_{member['user_id']}"):
                    del st.session_state["editing_member"]
                    st.rerun()

            # Confirmation for removal
            if st.session_state.get("confirm_remove_member") == member["user_id"]:
                st.warning(f"Are you sure you want to remove {member['username']} from the company?")
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Yes, Remove", key=f"confirm_yes_{member['user_id']}"):
                        if _remove_member(member["user_id"]):
                            st.success("Member removed")
                            del st.session_state["editing_member"]
                            del st.session_state["confirm_remove_member"]
                            st.rerun()
                        else:
                            st.error("Failed to remove member")
                with col2:
                    if st.button("No, Cancel", key=f"confirm_no_{member['user_id']}"):
                        del st.session_state["confirm_remove_member"]
                        st.rerun()


def render_invitations_section(company: dict):
    """Render invitations management section."""
    st.subheader("Pending Invitations")

    # Invite form
    with st.expander("Send New Invitation", expanded=st.session_state.get("show_invite_modal", False)):
        with st.form("invite_form"):
            email = st.text_input("Email Address")
            roles = st.multiselect(
                "Roles",
                ["admin", "manager", "analyst", "viewer"],
                default=["viewer"]
            )
            message = st.text_area("Personal Message (optional)")
            expires_days = st.slider("Expires in (days)", 1, 30, 7)

            submitted = st.form_submit_button("Send Invitation")
            if submitted:
                if email:
                    if _send_invitation(email, roles, message, expires_days):
                        st.success(f"Invitation sent to {email}!")
                        st.session_state["show_invite_modal"] = False
                        st.rerun()
                    else:
                        st.error("Failed to send invitation. Please try again.")
                else:
                    st.error("Please enter an email address")

    # Fetch pending invitations from API
    invitations = _get_company_invitations()

    if not invitations:
        st.info("No pending invitations")
    else:
        for inv in invitations:
            with st.container():
                col1, col2, col3, col4 = st.columns([3, 2, 2, 1])

                with col1:
                    st.text(inv.get("email", "-"))

                with col2:
                    role_ids = inv.get("role_ids", [])
                    st.text(f"Roles: {', '.join(role_ids)}")

                with col3:
                    expires_at = inv.get("expires_at", "")
                    st.text(f"Expires: {expires_at[:10] if expires_at else 'N/A'}")

                with col4:
                    inv_id = inv.get("invitation_id", inv.get("id", ""))
                    if st.button("Revoke", key=f"revoke_{inv_id}"):
                        if _revoke_invitation(inv_id):
                            st.success("Invitation revoked")
                            st.rerun()
                        else:
                            st.error("Failed to revoke invitation")

                st.divider()


def render_company_settings(company: dict):
    """Render company settings section."""
    st.subheader("Company Settings")

    settings = company.get("settings", {})

    with st.form("company_settings_form"):
        st.markdown("**Branding**")
        col1, col2 = st.columns(2)

        with col1:
            logo_url = st.text_input(
                "Logo URL",
                value=settings.get("logo_url", "")
            )

        with col2:
            primary_color = st.color_picker(
                "Primary Color",
                value=settings.get("primary_color", "#1E88E5")
            )

        st.divider()
        st.markdown("**Security**")

        col1, col2 = st.columns(2)

        with col1:
            require_mfa = st.checkbox(
                "Require MFA for all users",
                value=settings.get("require_mfa", False)
            )

            session_timeout = st.number_input(
                "Session Timeout (minutes)",
                min_value=15,
                max_value=1440,
                value=settings.get("session_timeout_minutes", 480)
            )

        with col2:
            allowed_domains = st.text_area(
                "Allowed Email Domains (one per line)",
                value="\n".join(settings.get("allowed_email_domains", []))
            )

        st.divider()
        st.markdown("**Notifications**")

        col1, col2 = st.columns(2)

        with col1:
            notification_email = st.text_input(
                "Notification Email",
                value=settings.get("notification_email", "")
            )

        with col2:
            slack_webhook = st.text_input(
                "Slack Webhook URL",
                value=settings.get("slack_webhook", ""),
                type="password"
            )

        submitted = st.form_submit_button("Save Settings")
        if submitted:
            # Prepare settings to save
            settings_to_save = {
                "logo_url": logo_url,
                "primary_color": primary_color,
                "require_mfa": require_mfa,
                "session_timeout_minutes": session_timeout,
                "allowed_email_domains": [d.strip() for d in allowed_domains.split("\n") if d.strip()],
                "notification_email": notification_email,
                "slack_webhook": slack_webhook,
            }
            if _save_company_settings(settings_to_save):
                st.success("Settings saved successfully!")
            else:
                st.error("Failed to save settings. Please try again.")


def render_usage_section(company: dict):
    """Render usage and limits section."""
    st.subheader("Usage & Limits")

    usage = company.get("usage", {})
    limits = company.get("limits", {})
    limit_status = company.get("limit_status", {})

    # Resource usage bars
    resources = [
        ("Users", "users", "user_count", "max_users"),
        ("Pipelines", "pipelines", "pipeline_count", "max_pipelines"),
        ("Connectors", "connectors", "connector_count", "max_connectors"),
        ("Storage (GB)", "storage_gb", "storage_used_gb", "max_storage_gb"),
        ("Jobs Today", "jobs_today", "jobs_today", "max_jobs_per_day"),
        ("API Calls Today", "api_calls_today", "api_calls_today", "max_api_calls_per_day"),
    ]

    for label, key, usage_key, limit_key in resources:
        status = limit_status.get(key, {})
        used = status.get("used", usage.get(usage_key, 0))
        limit = status.get("limit", limits.get(limit_key, 100))
        percentage = status.get("percentage", 0)

        col1, col2, col3 = st.columns([2, 4, 1])

        with col1:
            st.text(label)

        with col2:
            # Color based on usage
            if percentage >= 90:
                color = "red"
            elif percentage >= 75:
                color = "orange"
            else:
                color = "green"

            st.progress(min(percentage / 100, 1.0))

        with col3:
            st.text(f"{used}/{limit}")

    st.divider()

    # Feature flags
    st.markdown("**Enabled Features**")

    col1, col2 = st.columns(2)

    with col1:
        st.checkbox("Advanced Features", value=limits.get("enable_advanced_features", False), disabled=True)
        st.checkbox("SSO", value=limits.get("enable_sso", False), disabled=True)
        st.checkbox("Audit Logs", value=limits.get("enable_audit_logs", True), disabled=True)

    with col2:
        st.checkbox("Custom Roles", value=limits.get("enable_custom_roles", False), disabled=True)
        st.text(f"Support Level: {limits.get('support_level', 'community').title()}")
        st.text(f"Retention: {limits.get('retention_days', 30)} days")

    # Upgrade CTA
    if company.get("tier") in ["free", "starter"]:
        st.divider()
        st.info("🚀 Upgrade your plan to unlock more features and higher limits!")
        if st.button("View Plans"):
            st.info("Contact sales for upgrade options")


# Helper to be called from main app
def show_company_admin():
    """Entry point for company admin page."""
    render_company_admin_page()

