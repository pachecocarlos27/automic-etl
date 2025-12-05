"""Admin dashboard pages for superadmin and admin users."""

from __future__ import annotations

import streamlit as st

from automic_etl.db.auth_service import get_auth_service, AuthenticationError
from automic_etl.db.engine import get_session
from automic_etl.db.models import UserModel, SessionModel, AuditLogModel
from automic_etl.core.utils import utc_now


def check_admin_access() -> bool:
    """Check if current user has admin access."""
    user = st.session_state.get("user")
    if not user:
        return False
    return user.is_superadmin or any(r in (user.roles or []) for r in ["admin", "superadmin"])


def show_admin_dashboard():
    """Display the main admin dashboard."""
    user = st.session_state.user

    if not check_admin_access():
        st.error("Access denied. Admin privileges required.")
        return

    st.title("Admin Dashboard")

    if user.is_superadmin:
        st.info("You have Super Administrator access with full system control.")

    tab1, tab2, tab3 = st.tabs([
        "Overview",
        "User Management",
        "Audit Logs",
    ])

    with tab1:
        show_admin_overview()

    with tab2:
        show_user_management()

    with tab3:
        show_audit_logs()


def show_admin_overview():
    """Display admin overview with key metrics."""
    st.markdown("### System Overview")

    with get_session() as session:
        total_users = session.query(UserModel).count()
        active_users = session.query(UserModel).filter(UserModel.status == "active").count()
        pending_users = session.query(UserModel).filter(UserModel.status == "pending").count()
        active_sessions = session.query(SessionModel).filter(
            SessionModel.is_valid == True,
            SessionModel.expires_at > utc_now()
        ).count()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Users", total_users)

    with col2:
        st.metric("Active Users", active_users)

    with col3:
        st.metric("Active Sessions", active_sessions)

    with col4:
        st.metric("Pending Approvals", pending_users)

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Quick Actions")

        if st.button("Approve Pending Users", use_container_width=True):
            st.session_state.admin_action = "approve_pending"
            st.rerun()

        if st.button("Create New User", use_container_width=True):
            st.session_state.admin_action = "create_user"
            st.rerun()

    with col2:
        st.markdown("### Recent Activity")

        with get_session() as session:
            logs = session.query(AuditLogModel).order_by(
                AuditLogModel.timestamp.desc()
            ).limit(5).all()

            for log in logs:
                with st.container():
                    col_a, col_b = st.columns([3, 1])
                    with col_a:
                        st.markdown(f"**{log.action.replace('_', ' ').title()}**")
                        if log.username:
                            st.caption(f"By: {log.username}")
                    with col_b:
                        st.caption(log.timestamp.strftime("%H:%M"))

    if st.session_state.get("admin_action") == "create_user":
        show_create_user_dialog()
    elif st.session_state.get("admin_action") == "approve_pending":
        show_pending_approvals()


def show_user_management():
    """Display user management interface."""
    st.markdown("### User Management")

    col1, col2, col3 = st.columns(3)

    with col1:
        status_filter = st.selectbox(
            "Status",
            ["All", "active", "pending", "suspended", "inactive"]
        )

    with col2:
        role_filter = st.selectbox(
            "Role",
            ["All", "superadmin", "admin", "manager", "analyst", "viewer"]
        )

    with col3:
        search = st.text_input("Search", placeholder="Search users...")

    with get_session() as session:
        query = session.query(UserModel)

        if status_filter != "All":
            query = query.filter(UserModel.status == status_filter)

        users = query.order_by(UserModel.created_at.desc()).all()

        if search:
            search_lower = search.lower()
            users = [
                u for u in users
                if search_lower in u.username.lower()
                or search_lower in u.email.lower()
                or search_lower in (u.full_name or "").lower()
            ]

        if role_filter != "All":
            users = [u for u in users if role_filter in (u.roles or [])]

    st.markdown(f"**{len(users)} users found**")

    for user in users:
        with st.expander(f"{user.full_name} (@{user.username})", expanded=False):
            col1, col2, col3 = st.columns([2, 2, 1])

            with col1:
                st.markdown(f"**Email:** {user.email}")
                st.markdown(f"**Status:** {user.status.title()}")
                st.markdown(f"**Roles:** {', '.join(user.roles or []) or 'None'}")

            with col2:
                st.markdown(f"**Created:** {user.created_at.strftime('%Y-%m-%d')}")
                if user.last_login:
                    st.markdown(f"**Last Login:** {user.last_login.strftime('%Y-%m-%d %H:%M')}")
                if user.is_superadmin:
                    st.info("Super Administrator")

            with col3:
                if not user.is_superadmin or st.session_state.user.is_superadmin:
                    if user.status == "pending":
                        if st.button("Activate", key=f"activate_{user.id}", type="primary"):
                            with get_session() as sess:
                                u = sess.query(UserModel).filter(UserModel.id == user.id).first()
                                if u:
                                    u.status = "active"
                            st.success(f"User {user.username} activated!")
                            st.rerun()

                    elif user.status == "active" and not user.is_superadmin:
                        if st.button("Suspend", key=f"suspend_{user.id}"):
                            with get_session() as sess:
                                u = sess.query(UserModel).filter(UserModel.id == user.id).first()
                                if u:
                                    u.status = "suspended"
                            st.warning(f"User {user.username} suspended!")
                            st.rerun()

                    elif user.status == "suspended":
                        if st.button("Reactivate", key=f"reactivate_{user.id}"):
                            with get_session() as sess:
                                u = sess.query(UserModel).filter(UserModel.id == user.id).first()
                                if u:
                                    u.status = "active"
                            st.success(f"User {user.username} reactivated!")
                            st.rerun()


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
                auth_service = get_auth_service()
                try:
                    user = auth_service.register(
                        username=username,
                        email=email,
                        password=password,
                        first_name=first_name,
                        last_name=last_name,
                        auto_activate=auto_activate,
                    )
                    st.success(f"User {username} created successfully!")
                    st.session_state.admin_action = None
                    st.rerun()
                except AuthenticationError as e:
                    st.error(str(e))


def show_pending_approvals():
    """Show pending user approvals."""
    st.markdown("---")
    st.markdown("### Pending User Approvals")

    with get_session() as session:
        pending = session.query(UserModel).filter(UserModel.status == "pending").all()

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
                st.markdown(f"**{user.full_name}**")
                st.caption(f"@{user.username}")
                st.caption(user.email)

            with col2:
                st.markdown(f"Registered: {user.created_at.strftime('%Y-%m-%d %H:%M')}")

            with col3:
                if st.button("Approve", key=f"approve_{user.id}", type="primary"):
                    with get_session() as sess:
                        u = sess.query(UserModel).filter(UserModel.id == user.id).first()
                        if u:
                            u.status = "active"
                    st.success(f"Approved {user.username}")
                    st.rerun()

                if st.button("Reject", key=f"reject_{user.id}"):
                    with get_session() as sess:
                        u = sess.query(UserModel).filter(UserModel.id == user.id).first()
                        if u:
                            sess.delete(u)
                    st.warning(f"Rejected {user.username}")
                    st.rerun()

        st.markdown("---")

    if st.button("Back to Overview"):
        st.session_state.admin_action = None
        st.rerun()


def show_audit_logs():
    """Display audit logs."""
    st.markdown("### Audit Logs")

    col1, col2 = st.columns(2)

    with col1:
        action_filter = st.selectbox(
            "Action Type",
            ["All", "login", "logout", "login_failed", "user_created", "password_change"]
        )

    with col2:
        user_search = st.text_input("User", placeholder="Filter by username...")

    with get_session() as session:
        query = session.query(AuditLogModel).order_by(AuditLogModel.timestamp.desc())

        if action_filter != "All":
            query = query.filter(AuditLogModel.action == action_filter)

        logs = query.limit(100).all()

        if user_search:
            logs = [l for l in logs if l.username and user_search.lower() in l.username.lower()]

    st.markdown(f"**{len(logs)} log entries**")

    for log in logs:
        with st.container():
            col1, col2, col3, col4 = st.columns([1, 2, 3, 1])

            with col1:
                st.caption(log.timestamp.strftime("%m/%d %H:%M"))

            with col2:
                action_display = log.action.replace("_", " ").title()
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
                st.caption(" | ".join(details) if details else "-")

            with col4:
                if log.ip_address:
                    st.caption(log.ip_address)

        st.markdown("<hr style='margin: 0.2rem 0'>", unsafe_allow_html=True)
