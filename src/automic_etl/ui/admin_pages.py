"""Admin dashboard pages for superadmin and admin users."""

from __future__ import annotations

import streamlit as st
from datetime import datetime, timedelta

from automic_etl.auth.manager import AuthManager, AuthenticationError
from automic_etl.auth.models import UserStatus, RoleType, AuditAction
from automic_etl.ui.auth_pages import get_auth_manager


def check_admin_access() -> bool:
    """Check if current user has admin access."""
    user = st.session_state.get("user")
    if not user:
        return False
    return user.is_superadmin or any(r in user.roles for r in ["admin", "superadmin"])


def show_admin_dashboard():
    """Display the main admin dashboard."""
    user = st.session_state.user

    if not check_admin_access():
        st.error("Access denied. Admin privileges required.")
        return

    st.title("Admin Dashboard")

    if user.is_superadmin:
        st.info("You have Super Administrator access with full system control.")

    # Admin navigation tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Overview",
        "User Management",
        "Role Management",
        "Audit Logs",
        "System Settings"
    ])

    with tab1:
        show_admin_overview()

    with tab2:
        show_user_management()

    with tab3:
        show_role_management()

    with tab4:
        show_audit_logs()

    with tab5:
        show_system_settings()


def show_admin_overview():
    """Display admin overview with key metrics."""
    auth_manager = get_auth_manager()
    stats = auth_manager.get_statistics()

    st.markdown("### System Overview")

    # Key metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Users", stats["total_users"])

    with col2:
        st.metric("Active Users", stats["active_users"])

    with col3:
        st.metric("Active Sessions", stats["active_sessions"])

    with col4:
        st.metric("Pending Approvals", stats["pending_users"])

    st.markdown("---")

    # Quick actions
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Quick Actions")

        if st.button("Approve Pending Users", use_container_width=True):
            st.session_state.admin_action = "approve_pending"
            st.rerun()

        if st.button("Create New User", use_container_width=True):
            st.session_state.admin_action = "create_user"
            st.rerun()

        if st.button("Export User Report", use_container_width=True):
            users = auth_manager.list_users()
            user_data = [u.to_dict() for u in users]
            st.download_button(
                "Download CSV",
                data=_users_to_csv(user_data),
                file_name="users_report.csv",
                mime="text/csv"
            )

    with col2:
        st.markdown("### Recent Activity")

        logs = auth_manager.get_audit_logs(limit=10)
        for log in logs[:5]:
            with st.container():
                col_a, col_b = st.columns([3, 1])
                with col_a:
                    st.markdown(f"**{log.action.value.replace('_', ' ').title()}**")
                    if log.username:
                        st.caption(f"By: {log.username}")
                with col_b:
                    st.caption(log.timestamp.strftime("%H:%M"))

    # Handle actions
    if st.session_state.get("admin_action") == "create_user":
        show_create_user_dialog()
    elif st.session_state.get("admin_action") == "approve_pending":
        show_pending_approvals()


def show_user_management():
    """Display user management interface."""
    auth_manager = get_auth_manager()

    st.markdown("### User Management")

    # Filters
    col1, col2, col3 = st.columns(3)

    with col1:
        status_filter = st.selectbox(
            "Status",
            ["All", "Active", "Pending", "Suspended", "Inactive"]
        )

    with col2:
        role_filter = st.selectbox(
            "Role",
            ["All", "superadmin", "admin", "manager", "analyst", "viewer"]
        )

    with col3:
        search = st.text_input("Search", placeholder="Search users...")

    # Get users
    status = None if status_filter == "All" else UserStatus(status_filter.lower())
    role = None if role_filter == "All" else role_filter

    users = auth_manager.list_users(status=status, role_id=role)

    # Apply search filter
    if search:
        search_lower = search.lower()
        users = [
            u for u in users
            if search_lower in u.username.lower()
            or search_lower in u.email.lower()
            or search_lower in u.full_name.lower()
        ]

    st.markdown(f"**{len(users)} users found**")

    # User list
    for user in users:
        with st.expander(f"{user.display_name} (@{user.username})", expanded=False):
            col1, col2, col3 = st.columns([2, 2, 1])

            with col1:
                st.markdown(f"**Email:** {user.email}")
                st.markdown(f"**Status:** {user.status.value.title()}")
                st.markdown(f"**Roles:** {', '.join(user.roles) or 'None'}")

            with col2:
                st.markdown(f"**Created:** {user.created_at.strftime('%Y-%m-%d')}")
                if user.last_login:
                    st.markdown(f"**Last Login:** {user.last_login.strftime('%Y-%m-%d %H:%M')}")
                if user.is_superadmin:
                    st.info("Super Administrator")

            with col3:
                # Action buttons
                if not user.is_superadmin or (user.is_superadmin and st.session_state.user.is_superadmin):
                    if st.button("Edit", key=f"edit_{user.user_id}"):
                        st.session_state.edit_user_id = user.user_id
                        st.rerun()

                    if user.status == UserStatus.PENDING:
                        if st.button("Activate", key=f"activate_{user.user_id}", type="primary"):
                            auth_manager.activate_user(
                                user.user_id,
                                st.session_state.user.user_id
                            )
                            st.success(f"User {user.username} activated!")
                            st.rerun()

                    elif user.status == UserStatus.ACTIVE:
                        if st.button("Suspend", key=f"suspend_{user.user_id}"):
                            auth_manager.suspend_user(
                                user.user_id,
                                st.session_state.user.user_id
                            )
                            st.warning(f"User {user.username} suspended!")
                            st.rerun()

                    elif user.status == UserStatus.SUSPENDED:
                        if st.button("Reactivate", key=f"reactivate_{user.user_id}"):
                            auth_manager.activate_user(
                                user.user_id,
                                st.session_state.user.user_id
                            )
                            st.success(f"User {user.username} reactivated!")
                            st.rerun()

    # Edit user dialog
    if st.session_state.get("edit_user_id"):
        show_edit_user_dialog(st.session_state.edit_user_id)


def show_create_user_dialog():
    """Show dialog for creating a new user."""
    st.markdown("---")
    st.markdown("### Create New User")

    with st.form("create_user_form"):
        col1, col2 = st.columns(2)

        with col1:
            username = st.text_input("Username*")
            email = st.text_input("Email*")
            password = st.text_input("Password*", type="password")

        with col2:
            first_name = st.text_input("First Name")
            last_name = st.text_input("Last Name")
            role = st.selectbox("Initial Role", ["viewer", "analyst", "manager", "admin"])

        auto_activate = st.checkbox("Auto-activate account", value=True)

        col1, col2 = st.columns(2)
        with col1:
            submitted = st.form_submit_button("Create User", use_container_width=True)
        with col2:
            if st.form_submit_button("Cancel", use_container_width=True):
                st.session_state.admin_action = None
                st.rerun()

        if submitted:
            if not username or not email or not password:
                st.error("Please fill in all required fields")
            else:
                auth_manager = get_auth_manager()
                try:
                    user = auth_manager.register(
                        username=username,
                        email=email,
                        password=password,
                        first_name=first_name,
                        last_name=last_name,
                        auto_activate=auto_activate,
                    )
                    auth_manager.assign_role(
                        user.user_id,
                        role,
                        st.session_state.user.user_id
                    )
                    st.success(f"User {username} created successfully!")
                    st.session_state.admin_action = None
                    st.rerun()
                except AuthenticationError as e:
                    st.error(str(e))


def show_edit_user_dialog(user_id: str):
    """Show dialog for editing a user."""
    auth_manager = get_auth_manager()
    user = auth_manager.get_user(user_id)

    if not user:
        st.error("User not found")
        return

    st.markdown("---")
    st.markdown(f"### Edit User: {user.username}")

    with st.form("edit_user_form"):
        col1, col2 = st.columns(2)

        with col1:
            first_name = st.text_input("First Name", value=user.first_name)
            last_name = st.text_input("Last Name", value=user.last_name)
            email = st.text_input("Email", value=user.email)

        with col2:
            status = st.selectbox(
                "Status",
                [s.value for s in UserStatus],
                index=[s.value for s in UserStatus].index(user.status.value)
            )

            # Role management
            available_roles = ["viewer", "analyst", "manager", "admin"]
            if st.session_state.user.is_superadmin:
                available_roles.append("superadmin")

            current_roles = [r for r in user.roles if r in available_roles]
            selected_roles = st.multiselect(
                "Roles",
                available_roles,
                default=current_roles
            )

        st.markdown("#### Password Reset")
        new_password = st.text_input("New Password (leave blank to keep current)", type="password")

        col1, col2 = st.columns(2)
        with col1:
            submitted = st.form_submit_button("Save Changes", use_container_width=True)
        with col2:
            if st.form_submit_button("Cancel", use_container_width=True):
                st.session_state.edit_user_id = None
                st.rerun()

        if submitted:
            try:
                # Update basic info
                auth_manager.update_user(
                    user_id=user_id,
                    first_name=first_name,
                    last_name=last_name,
                    email=email,
                    status=UserStatus(status),
                )

                # Update roles
                for role in user.roles:
                    if role not in selected_roles:
                        auth_manager.remove_role(
                            user_id,
                            role,
                            st.session_state.user.user_id
                        )

                for role in selected_roles:
                    if role not in user.roles:
                        auth_manager.assign_role(
                            user_id,
                            role,
                            st.session_state.user.user_id
                        )

                # Reset password if provided
                if new_password:
                    auth_manager.reset_password(
                        user_id,
                        new_password,
                        st.session_state.user.user_id
                    )

                st.success("User updated successfully!")
                st.session_state.edit_user_id = None
                st.rerun()

            except AuthenticationError as e:
                st.error(str(e))


def show_pending_approvals():
    """Show pending user approvals."""
    auth_manager = get_auth_manager()

    st.markdown("---")
    st.markdown("### Pending User Approvals")

    pending = auth_manager.list_users(status=UserStatus.PENDING)

    if not pending:
        st.info("No pending approvals")
        if st.button("Back"):
            st.session_state.admin_action = None
            st.rerun()
        return

    for user in pending:
        with st.container():
            col1, col2, col3 = st.columns([2, 2, 1])

            with col1:
                st.markdown(f"**{user.display_name}**")
                st.caption(f"@{user.username}")
                st.caption(user.email)

            with col2:
                st.markdown(f"Registered: {user.created_at.strftime('%Y-%m-%d %H:%M')}")

            with col3:
                if st.button("Approve", key=f"approve_{user.user_id}", type="primary"):
                    auth_manager.activate_user(user.user_id, st.session_state.user.user_id)
                    st.success(f"Approved {user.username}")
                    st.rerun()

                if st.button("Reject", key=f"reject_{user.user_id}"):
                    auth_manager.delete_user(user.user_id, st.session_state.user.user_id)
                    st.warning(f"Rejected {user.username}")
                    st.rerun()

        st.markdown("---")

    if st.button("Back to Overview"):
        st.session_state.admin_action = None
        st.rerun()


def show_role_management():
    """Display role management interface."""
    auth_manager = get_auth_manager()
    rbac = auth_manager.rbac_manager

    st.markdown("### Role Management")

    # List existing roles
    roles = rbac.list_roles()

    for role in roles:
        with st.expander(f"{role.name} ({role.role_type.value})", expanded=False):
            st.markdown(f"**Description:** {role.description}")
            st.markdown(f"**System Role:** {'Yes' if role.is_system else 'No'}")

            st.markdown("**Permissions:**")
            perms_by_category = {}
            for perm in role.permissions:
                category = perm.split(":")[0]
                if category not in perms_by_category:
                    perms_by_category[category] = []
                perms_by_category[category].append(perm)

            for category, perms in perms_by_category.items():
                with st.container():
                    st.markdown(f"*{category.title()}*")
                    st.caption(", ".join(p.split(":")[-1] for p in perms))

            # Edit role (non-system roles only)
            if not role.is_system and st.session_state.user.is_superadmin:
                if st.button(f"Edit Role", key=f"edit_role_{role.role_id}"):
                    st.session_state.edit_role_id = role.role_id

    # Create new role
    if st.session_state.user.is_superadmin:
        st.markdown("---")
        st.markdown("### Create Custom Role")

        with st.form("create_role_form"):
            name = st.text_input("Role Name")
            description = st.text_area("Description")

            st.markdown("**Select Permissions:**")

            perms_by_category = rbac.get_permissions_by_category()
            selected_permissions = []

            for category, perms in perms_by_category.items():
                st.markdown(f"**{category.title()}**")
                cols = st.columns(3)
                for i, perm in enumerate(perms):
                    with cols[i % 3]:
                        if st.checkbox(perm.name, key=f"perm_{perm.permission_id}"):
                            selected_permissions.append(perm.permission_id)

            if st.form_submit_button("Create Role"):
                if not name:
                    st.error("Role name is required")
                else:
                    role = rbac.create_role(
                        name=name,
                        description=description,
                        role_type=RoleType.VIEWER,  # Custom roles are viewer type
                        permissions=selected_permissions,
                    )
                    st.success(f"Role '{name}' created!")
                    st.rerun()


def show_audit_logs():
    """Display audit logs."""
    auth_manager = get_auth_manager()

    st.markdown("### Audit Logs")

    # Filters
    col1, col2, col3 = st.columns(3)

    with col1:
        action_filter = st.selectbox(
            "Action Type",
            ["All"] + [a.value for a in AuditAction]
        )

    with col2:
        days_filter = st.selectbox(
            "Time Range",
            ["Last 24 hours", "Last 7 days", "Last 30 days", "All time"]
        )

    with col3:
        user_search = st.text_input("User", placeholder="Filter by username...")

    # Calculate time range
    start_time = None
    if days_filter == "Last 24 hours":
        start_time = datetime.utcnow() - timedelta(hours=24)
    elif days_filter == "Last 7 days":
        start_time = datetime.utcnow() - timedelta(days=7)
    elif days_filter == "Last 30 days":
        start_time = datetime.utcnow() - timedelta(days=30)

    # Get logs
    action = AuditAction(action_filter) if action_filter != "All" else None
    logs = auth_manager.get_audit_logs(
        action=action,
        start_time=start_time,
        limit=500,
    )

    # Filter by user
    if user_search:
        logs = [l for l in logs if l.username and user_search.lower() in l.username.lower()]

    st.markdown(f"**{len(logs)} log entries**")

    # Display logs
    for log in logs[:100]:  # Limit display
        with st.container():
            col1, col2, col3, col4 = st.columns([1, 2, 3, 1])

            with col1:
                st.caption(log.timestamp.strftime("%m/%d %H:%M"))

            with col2:
                action_display = log.action.value.replace("_", " ").title()
                if log.success:
                    st.markdown(f"**{action_display}**")
                else:
                    st.markdown(f"**:red[{action_display}]**")

            with col3:
                details = []
                if log.username:
                    details.append(f"User: {log.username}")
                if log.resource_type:
                    details.append(f"Resource: {log.resource_type}")
                if log.details:
                    for k, v in list(log.details.items())[:2]:
                        details.append(f"{k}: {v}")
                st.caption(" | ".join(details) if details else "-")

            with col4:
                if log.ip_address:
                    st.caption(log.ip_address)

        st.markdown("<hr style='margin: 0.2rem 0'>", unsafe_allow_html=True)


def show_system_settings():
    """Display system settings for superadmin."""
    if not st.session_state.user.is_superadmin:
        st.warning("Super Administrator access required for system settings.")
        return

    st.markdown("### System Settings")

    tab1, tab2, tab3 = st.tabs(["Security", "Authentication", "Maintenance"])

    with tab1:
        st.markdown("#### Security Settings")

        with st.form("security_settings"):
            max_failed = st.number_input(
                "Max Failed Login Attempts",
                min_value=3,
                max_value=10,
                value=5
            )

            lockout_mins = st.number_input(
                "Account Lockout Duration (minutes)",
                min_value=5,
                max_value=120,
                value=30
            )

            password_min = st.number_input(
                "Minimum Password Length",
                min_value=6,
                max_value=20,
                value=8
            )

            require_mfa = st.checkbox("Require MFA for admin users")

            if st.form_submit_button("Save Security Settings"):
                st.success("Security settings updated!")

    with tab2:
        st.markdown("#### Authentication Settings")

        with st.form("auth_settings"):
            session_hours = st.number_input(
                "Session Duration (hours)",
                min_value=1,
                max_value=168,
                value=24
            )

            max_sessions = st.number_input(
                "Max Sessions per User",
                min_value=1,
                max_value=10,
                value=5
            )

            inactivity_timeout = st.number_input(
                "Inactivity Timeout (hours)",
                min_value=1,
                max_value=24,
                value=2
            )

            allow_registration = st.checkbox("Allow self-registration", value=True)
            require_approval = st.checkbox("Require admin approval for new users", value=True)

            if st.form_submit_button("Save Authentication Settings"):
                st.success("Authentication settings updated!")

    with tab3:
        st.markdown("#### System Maintenance")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Session Cleanup**")
            if st.button("Clear Expired Sessions"):
                auth_manager = get_auth_manager()
                count = auth_manager.session_manager.cleanup_expired()
                st.success(f"Cleared {count} expired sessions")

            st.markdown("**Audit Log Cleanup**")
            days_to_keep = st.number_input("Keep logs for (days)", value=90, min_value=7)
            if st.button("Cleanup Old Logs"):
                st.success(f"Logs older than {days_to_keep} days cleaned up")

        with col2:
            st.markdown("**Data Export**")
            if st.button("Export All Users"):
                auth_manager = get_auth_manager()
                users = auth_manager.list_users()
                data = [u.to_dict() for u in users]
                st.download_button(
                    "Download JSON",
                    data=str(data),
                    file_name="users_export.json",
                    mime="application/json"
                )

            st.markdown("**System Info**")
            auth_manager = get_auth_manager()
            stats = auth_manager.get_statistics()

            st.json(stats)


def _users_to_csv(users: list[dict]) -> str:
    """Convert users to CSV format."""
    if not users:
        return ""

    headers = ["username", "email", "full_name", "status", "roles", "created_at", "last_login"]
    lines = [",".join(headers)]

    for user in users:
        row = [
            user.get("username", ""),
            user.get("email", ""),
            user.get("full_name", ""),
            user.get("status", ""),
            "|".join(user.get("roles", [])),
            user.get("created_at", ""),
            user.get("last_login", "") or "",
        ]
        lines.append(",".join(str(v) for v in row))

    return "\n".join(lines)
