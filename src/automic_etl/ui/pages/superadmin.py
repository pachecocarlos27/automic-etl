"""Superadmin dashboard UI page."""

from __future__ import annotations

import streamlit as st
from datetime import datetime, timedelta
from typing import Any


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

    # Key metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Companies", 42, delta="+3 this week")

    with col2:
        st.metric("Total Users", 256, delta="+12 this week")

    with col3:
        st.metric("Active Pipelines", 89, delta="+5 today")

    with col4:
        st.metric("Storage Used", "1.2 TB", delta="+50 GB")

    st.divider()

    # Status breakdown
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Companies by Status**")
        status_data = {
            "Active": 35,
            "Trial": 5,
            "Suspended": 1,
            "Pending": 1,
        }
        for status, count in status_data.items():
            st.progress(count / 42, text=f"{status}: {count}")

    with col2:
        st.markdown("**Companies by Tier**")
        tier_data = {
            "Free": 20,
            "Starter": 12,
            "Professional": 8,
            "Enterprise": 2,
        }
        for tier, count in tier_data.items():
            st.progress(count / 42, text=f"{tier}: {count}")

    st.divider()

    # Recent activity
    st.markdown("**Recent Activity**")
    activities = [
        {"time": "5 min ago", "action": "New company registered", "detail": "Acme Corp"},
        {"time": "15 min ago", "action": "User suspended", "detail": "user@spam.com"},
        {"time": "1 hour ago", "action": "Company upgraded", "detail": "TechStart -> Professional"},
        {"time": "2 hours ago", "action": "Pipeline failure", "detail": "Company: DataCo, Pipeline: ETL-Main"},
        {"time": "3 hours ago", "action": "New superadmin created", "detail": "admin2@platform.com"},
    ]

    for activity in activities:
        col1, col2, col3 = st.columns([1, 2, 2])
        with col1:
            st.caption(activity["time"])
        with col2:
            st.text(activity["action"])
        with col3:
            st.text(activity["detail"])

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
            st.info("Report export coming soon")


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

    # Sample companies data
    companies = [
        {
            "company_id": "c1",
            "name": "Acme Corporation",
            "slug": "acme-corp",
            "status": "active",
            "tier": "professional",
            "user_count": 25,
            "pipeline_count": 15,
            "created_at": "2024-01-01",
        },
        {
            "company_id": "c2",
            "name": "TechStart Inc",
            "slug": "techstart",
            "status": "trial",
            "tier": "professional",
            "user_count": 5,
            "pipeline_count": 3,
            "created_at": "2024-02-15",
            "trial_ends_at": "2024-03-01",
        },
        {
            "company_id": "c3",
            "name": "DataCo",
            "slug": "dataco",
            "status": "active",
            "tier": "starter",
            "user_count": 8,
            "pipeline_count": 7,
            "created_at": "2024-01-20",
        },
        {
            "company_id": "c4",
            "name": "Suspicious LLC",
            "slug": "suspicious",
            "status": "suspended",
            "tier": "free",
            "user_count": 1,
            "pipeline_count": 0,
            "created_at": "2024-02-01",
        },
    ]

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
                with btn_col1:
                    if st.button("👁️", key=f"view_{company['company_id']}", help="View"):
                        st.session_state["viewing_company"] = company
                with btn_col2:
                    if st.button("✏️", key=f"edit_{company['company_id']}", help="Edit"):
                        st.session_state["editing_company"] = company
                with btn_col3:
                    if company["status"] == "active":
                        if st.button("⏸️", key=f"suspend_{company['company_id']}", help="Suspend"):
                            st.warning(f"Suspend {company['name']}?")
                    elif company["status"] == "suspended":
                        if st.button("▶️", key=f"activate_{company['company_id']}", help="Activate"):
                            st.success(f"Activated {company['name']}")

            st.divider()

    # Edit company modal
    if st.session_state.get("editing_company"):
        company = st.session_state["editing_company"]
        with st.expander(f"Edit {company['name']}", expanded=True):
            col1, col2 = st.columns(2)

            with col1:
                new_tier = st.selectbox(
                    "Tier",
                    ["free", "starter", "professional", "enterprise"],
                    index=["free", "starter", "professional", "enterprise"].index(company["tier"])
                )

            with col2:
                new_status = st.selectbox(
                    "Status",
                    ["active", "trial", "suspended", "pending"],
                    index=["active", "trial", "suspended", "pending"].index(company["status"])
                    if company["status"] in ["active", "trial", "suspended", "pending"] else 0
                )

            custom_limits = st.checkbox("Set Custom Limits")
            if custom_limits:
                col1, col2, col3 = st.columns(3)
                with col1:
                    max_users = st.number_input("Max Users", min_value=1, value=50)
                with col2:
                    max_pipelines = st.number_input("Max Pipelines", min_value=1, value=100)
                with col3:
                    max_storage = st.number_input("Max Storage (GB)", min_value=1, value=500)

            col1, col2 = st.columns(2)
            with col1:
                if st.button("Save Changes"):
                    st.success("Company updated!")
                    del st.session_state["editing_company"]
                    st.rerun()
            with col2:
                if st.button("Cancel"):
                    del st.session_state["editing_company"]
                    st.rerun()


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

    # Sample users data
    users = [
        {
            "user_id": "u1",
            "username": "admin",
            "email": "admin@platform.com",
            "is_superadmin": True,
            "status": "active",
            "companies": [],
            "last_login": "2024-02-20T10:30:00Z",
        },
        {
            "user_id": "u2",
            "username": "john.doe",
            "email": "john@acme.com",
            "is_superadmin": False,
            "status": "active",
            "companies": [{"name": "Acme Corp", "is_admin": True}],
            "last_login": "2024-02-20T09:15:00Z",
        },
        {
            "user_id": "u3",
            "username": "jane.smith",
            "email": "jane@techstart.com",
            "is_superadmin": False,
            "status": "active",
            "companies": [{"name": "TechStart", "is_admin": False}],
            "last_login": "2024-02-19T16:45:00Z",
        },
        {
            "user_id": "u4",
            "username": "spam.user",
            "email": "spam@suspicious.com",
            "is_superadmin": False,
            "status": "suspended",
            "companies": [{"name": "Suspicious LLC", "is_admin": False}],
            "last_login": "2024-02-01T00:00:00Z",
        },
    ]

    # Users table
    for user in users:
        with st.container():
            col1, col2, col3, col4, col5 = st.columns([2, 2, 2, 1, 2])

            with col1:
                badge = " 🛡️" if user["is_superadmin"] else ""
                status_icon = "🟢" if user["status"] == "active" else "🔴"
                st.markdown(f"**{user['username']}**{badge} {status_icon}")
                st.caption(user["email"])

            with col2:
                if user["companies"]:
                    companies_str = ", ".join([c["name"] for c in user["companies"]])
                    st.text(companies_str)
                else:
                    st.text("Platform admin")

            with col3:
                last_login = user["last_login"][:10] if user["last_login"] else "Never"
                st.text(f"Last login: {last_login}")

            with col4:
                st.text(user["status"].title())

            with col5:
                btn_col1, btn_col2, btn_col3 = st.columns(3)
                with btn_col1:
                    if st.button("🔑", key=f"reset_pw_{user['user_id']}", help="Reset Password"):
                        st.info("Password reset email sent")
                with btn_col2:
                    if st.button("👤", key=f"impersonate_{user['user_id']}", help="Impersonate"):
                        if not user["is_superadmin"]:
                            st.warning(f"Impersonating {user['username']}...")
                        else:
                            st.error("Cannot impersonate superadmins")
                with btn_col3:
                    if user["status"] == "active" and not user["is_superadmin"]:
                        if st.button("⏸️", key=f"suspend_user_{user['user_id']}", help="Suspend"):
                            st.warning(f"Suspended {user['username']}")
                    elif user["status"] == "suspended":
                        if st.button("▶️", key=f"activate_user_{user['user_id']}", help="Activate"):
                            st.success(f"Activated {user['username']}")

            st.divider()

    # New superadmin modal
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
                    if password == confirm_password:
                        st.success(f"Superadmin '{username}' created!")
                        st.session_state["show_new_superadmin_modal"] = False
                    else:
                        st.error("Passwords don't match")


def render_global_settings():
    """Render global settings section."""
    st.subheader("Global Platform Settings")

    with st.form("global_settings_form"):
        # Platform settings
        st.markdown("**Platform Configuration**")
        col1, col2 = st.columns(2)

        with col1:
            platform_name = st.text_input("Platform Name", value="Automic ETL")
            platform_url = st.text_input("Platform URL", value="http://localhost:8501")

        with col2:
            support_email = st.text_input("Support Email", value="support@automic-etl.com")
            default_tier = st.selectbox("Default Company Tier", ["free", "starter", "trial"])

        st.divider()

        # Registration settings
        st.markdown("**Registration & Access**")
        col1, col2 = st.columns(2)

        with col1:
            allow_registration = st.checkbox("Allow Self Registration", value=True)
            require_email_verification = st.checkbox("Require Email Verification", value=True)

        with col2:
            auto_approve = st.checkbox("Auto-Approve Companies", value=False)
            enable_api = st.checkbox("Enable API Access", value=True)

        st.divider()

        # Security settings
        st.markdown("**Security**")
        col1, col2 = st.columns(2)

        with col1:
            password_min_length = st.number_input("Min Password Length", min_value=6, max_value=32, value=8)
            max_login_attempts = st.number_input("Max Failed Login Attempts", min_value=3, max_value=20, value=5)

        with col2:
            lockout_duration = st.number_input("Lockout Duration (minutes)", min_value=5, max_value=1440, value=30)
            session_timeout = st.number_input("Session Timeout (minutes)", min_value=15, max_value=1440, value=480)

        require_mfa_admins = st.checkbox("Require MFA for Admins", value=False)

        st.divider()

        # Feature flags
        st.markdown("**Feature Flags**")
        col1, col2 = st.columns(2)

        with col1:
            enable_llm = st.checkbox("Enable LLM Features", value=True)
            enable_streaming = st.checkbox("Enable Streaming Connectors", value=True)

        with col2:
            enable_spark = st.checkbox("Enable Spark Integration", value=True)

        st.divider()

        # Platform limits
        st.markdown("**Platform Limits**")
        col1, col2, col3 = st.columns(3)

        with col1:
            max_companies = st.number_input("Max Companies", min_value=1, value=1000)

        with col2:
            max_users = st.number_input("Max Total Users", min_value=1, value=10000)

        with col3:
            api_rate_limit = st.number_input("Default API Rate Limit", min_value=100, value=1000)

        submitted = st.form_submit_button("Save Settings")
        if submitted:
            st.success("Global settings saved!")

    # Maintenance mode
    st.divider()
    st.markdown("**Maintenance Mode**")

    maintenance_mode = st.session_state.get("maintenance_mode", False)

    col1, col2 = st.columns([3, 1])

    with col1:
        if maintenance_mode:
            st.error("⚠️ Maintenance mode is ACTIVE")
        else:
            st.success("✅ System is operational")

    with col2:
        if maintenance_mode:
            if st.button("Disable Maintenance"):
                st.session_state["maintenance_mode"] = False
                st.success("Maintenance mode disabled")
                st.rerun()
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
                    st.session_state["maintenance_mode"] = True
                    st.session_state["show_maintenance_modal"] = False
                    st.warning("Maintenance mode enabled!")
                    st.rerun()
            with col2:
                if st.button("Cancel", key="cancel_maintenance"):
                    st.session_state["show_maintenance_modal"] = False
                    st.rerun()


def render_system_health():
    """Render system health section."""
    st.subheader("System Health")

    # Overall status
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("### 🟢 Healthy")
        st.caption("Overall system status")

    with col2:
        st.metric("Uptime", "99.9%", delta="0.1%")

    with col3:
        st.metric("Response Time", "45ms", delta="-5ms")

    st.divider()

    # Component status
    st.markdown("**Component Status**")

    components = [
        {"name": "Auth Service", "status": "healthy", "latency": "12ms", "details": "1,234 active sessions"},
        {"name": "Company Service", "status": "healthy", "latency": "8ms", "details": "42 companies"},
        {"name": "Pipeline Engine", "status": "healthy", "latency": "25ms", "details": "89 active pipelines"},
        {"name": "Database", "status": "healthy", "latency": "5ms", "details": "Connections: 15/100"},
        {"name": "Redis Cache", "status": "healthy", "latency": "2ms", "details": "Hit rate: 94%"},
        {"name": "Storage (S3)", "status": "healthy", "latency": "45ms", "details": "1.2 TB used"},
    ]

    for component in components:
        col1, col2, col3, col4 = st.columns([2, 1, 1, 2])

        with col1:
            status_icon = "🟢" if component["status"] == "healthy" else "🔴"
            st.markdown(f"{status_icon} **{component['name']}**")

        with col2:
            st.text(component["status"].title())

        with col3:
            st.text(component["latency"])

        with col4:
            st.caption(component["details"])

    st.divider()

    # Warnings and errors
    st.markdown("**Recent Warnings & Errors**")

    warnings = [
        {"time": "10 min ago", "level": "warning", "message": "High memory usage on worker-3"},
        {"time": "1 hour ago", "level": "error", "message": "Pipeline ETL-Main failed: timeout"},
        {"time": "2 hours ago", "level": "warning", "message": "Rate limit exceeded for company: DataCo"},
    ]

    for warning in warnings:
        icon = "⚠️" if warning["level"] == "warning" else "❌"
        col1, col2 = st.columns([1, 4])
        with col1:
            st.caption(warning["time"])
        with col2:
            st.text(f"{icon} {warning['message']}")


def render_audit_logs():
    """Render audit logs section."""
    st.subheader("Audit Logs")

    # Filters
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        action_filter = st.selectbox(
            "Action Type",
            ["All", "Login", "User Created", "User Updated", "Company Created", "Settings Changed"]
        )

    with col2:
        resource_filter = st.selectbox(
            "Resource Type",
            ["All", "user", "company", "pipeline", "system"]
        )

    with col3:
        date_from = st.date_input("From Date", value=datetime.now() - timedelta(days=7))

    with col4:
        date_to = st.date_input("To Date", value=datetime.now())

    # Sample audit logs
    logs = [
        {
            "timestamp": "2024-02-20T10:30:00Z",
            "user": "admin",
            "action": "login",
            "resource_type": "user",
            "resource_id": "u1",
            "details": "Login from 192.168.1.1",
            "success": True,
        },
        {
            "timestamp": "2024-02-20T09:15:00Z",
            "user": "admin",
            "action": "user_created",
            "resource_type": "user",
            "resource_id": "u5",
            "details": "Created superadmin: admin2",
            "success": True,
        },
        {
            "timestamp": "2024-02-20T08:00:00Z",
            "user": "system",
            "action": "system_config_changed",
            "resource_type": "system",
            "resource_id": None,
            "details": "Updated global settings: password_min_length",
            "success": True,
        },
        {
            "timestamp": "2024-02-19T23:45:00Z",
            "user": "john.doe",
            "action": "login_failed",
            "resource_type": "user",
            "resource_id": "u2",
            "details": "Invalid password, attempt 3/5",
            "success": False,
        },
        {
            "timestamp": "2024-02-19T16:30:00Z",
            "user": "admin",
            "action": "company_suspended",
            "resource_type": "company",
            "resource_id": "c4",
            "details": "Suspended: Suspicious LLC - Terms violation",
            "success": True,
        },
    ]

    # Logs table
    st.markdown("---")

    for log in logs:
        col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 3, 1])

        with col1:
            st.caption(log["timestamp"][:19].replace("T", " "))

        with col2:
            st.text(log["user"])

        with col3:
            st.text(log["action"].replace("_", " ").title())

        with col4:
            st.caption(log["details"])

        with col5:
            icon = "✅" if log["success"] else "❌"
            st.text(icon)

    # Pagination
    st.divider()
    col1, col2, col3 = st.columns([2, 1, 2])
    with col2:
        st.text("Page 1 of 10")

    # Export button
    if st.button("Export Audit Logs"):
        st.info("Audit log export coming soon")


# Helper to be called from main app
def show_superadmin():
    """Entry point for superadmin page."""
    render_superadmin_page()
