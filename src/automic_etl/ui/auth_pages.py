"""Authentication pages for the Streamlit UI."""

from __future__ import annotations

import streamlit as st
from datetime import datetime

from automic_etl.db.auth_service import get_auth_service, AuthenticationError


def get_auth_service_instance():
    """Get the auth service, initializing database on first access."""
    if "auth_initialized" not in st.session_state:
        auth_service = get_auth_service()
        auth_service.initialize()
        st.session_state.auth_initialized = True
    return get_auth_service()


def init_session_state():
    """Initialize session state variables."""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if "user" not in st.session_state:
        st.session_state.user = None
    if "token" not in st.session_state:
        st.session_state.token = None


def check_authentication() -> bool:
    """Check if user is authenticated."""
    init_session_state()

    if not st.session_state.authenticated or not st.session_state.token:
        return False

    auth_service = get_auth_service_instance()
    user = auth_service.validate_session(st.session_state.token)

    if not user:
        logout()
        return False

    st.session_state.user = user
    return True


def logout():
    """Log out the current user."""
    if st.session_state.get("token"):
        auth_service = get_auth_service_instance()
        auth_service.logout(st.session_state.token)

    st.session_state.authenticated = False
    st.session_state.user = None
    st.session_state.token = None


def show_login_page():
    """Display minimal login page."""
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    [data-testid="stSidebar"] { display: none !important; }
    
    /* Hide skip link */
    a[href="#main-content"],
    a[data-testid="stSkipLink"] {
        position: absolute !important;
        width: 1px !important;
        height: 1px !important;
        padding: 0 !important;
        margin: -1px !important;
        overflow: hidden !important;
        clip: rect(0, 0, 0, 0) !important;
        white-space: nowrap !important;
        border: 0 !important;
    }
    
    .main .block-container {
        max-width: 100% !important;
        padding: 0 !important;
    }
    
    .stTextInput > div > div > input {
        border-radius: 8px !important;
        border: 1px solid #E5E7EB !important;
        padding: 0.75rem 1rem !important;
        font-size: 0.9375rem !important;
    }
    
    .stTextInput > div > div > input:focus {
        border-color: #2563EB !important;
        box-shadow: 0 0 0 2px #EFF6FF !important;
    }
    
    .stButton > button {
        width: 100% !important;
        background: #2563EB !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 0.75rem 1.5rem !important;
        font-weight: 600 !important;
        font-size: 0.9375rem !important;
    }
    
    .stButton > button:hover {
        background: #1D4ED8 !important;
    }
    
    .stTabs [data-baseweb="tab-list"] {
        gap: 0 !important;
        background: #F3F4F6 !important;
        padding: 4px !important;
        border-radius: 8px !important;
        border: none !important;
        margin-bottom: 1.5rem !important;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 6px !important;
        padding: 0.625rem 1rem !important;
        font-weight: 500 !important;
        font-size: 0.875rem !important;
        color: #6B7280 !important;
        flex: 1 !important;
        justify-content: center !important;
    }
    
    .stTabs [aria-selected="true"] {
        background: white !important;
        color: #111827 !important;
        box-shadow: 0 1px 2px rgba(0, 0, 0, 0.05) !important;
    }
    
    .stTabs [data-baseweb="tab-highlight"],
    .stTabs [data-baseweb="tab-border"] {
        display: none !important;
    }
    </style>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 1.5, 1])
    
    with col2:
        st.markdown("<div style='height: 10vh'></div>", unsafe_allow_html=True)
        
        st.markdown("""
        <div style="text-align: center; margin-bottom: 2rem;">
            <div style="display: inline-flex; align-items: center; justify-content: center; width: 48px; height: 48px; background: #2563EB; border-radius: 12px; margin-bottom: 1rem;">
                <span style="font-size: 1.5rem;">⚡</span>
            </div>
            <h1 style="font-size: 1.5rem; font-weight: 700; color: #111827; margin: 0 0 0.25rem; letter-spacing: -0.025em;">Automic ETL</h1>
            <p style="font-size: 0.875rem; color: #6B7280; margin: 0;">Sign in to continue</p>
        </div>
        """, unsafe_allow_html=True)
        
        tab1, tab2 = st.tabs(["Sign In", "Create Account"])

        with tab1:
            show_login_form()

        with tab2:
            show_register_form()
        
        st.markdown("""
        <div style="text-align: center; margin-top: 1.5rem;">
            <p style="font-size: 0.75rem; color: #9CA3AF; margin: 0;">Secure enterprise data platform</p>
        </div>
        """, unsafe_allow_html=True)


def show_login_form():
    """Display the login form."""
    with st.form("login_form"):
        username = st.text_input("Username or Email", placeholder="Enter username or email")
        password = st.text_input("Password", type="password", placeholder="Enter password")

        remember = st.checkbox("Remember me")

        submitted = st.form_submit_button("Login", use_container_width=True)

        if submitted:
            if not username or not password:
                st.error("Please enter username and password")
                return

            auth_service = get_auth_service_instance()

            try:
                user, token = auth_service.authenticate(
                    username=username,
                    password=password,
                )

                st.session_state.authenticated = True
                st.session_state.user = user
                st.session_state.token = token

                st.success("Login successful!")
                st.rerun()

            except AuthenticationError as e:
                st.error(str(e))


def show_register_form():
    """Display the registration form."""
    with st.form("register_form"):
        col1, col2 = st.columns(2)

        with col1:
            first_name = st.text_input("First Name")
        with col2:
            last_name = st.text_input("Last Name")

        username = st.text_input("Username", placeholder="Choose a username")
        email = st.text_input("Email", placeholder="your.email@example.com")
        password = st.text_input("Password", type="password", placeholder="Min 8 chars, upper/lower/number")
        confirm_password = st.text_input("Confirm Password", type="password")

        terms = st.checkbox("I agree to the Terms of Service and Privacy Policy")

        submitted = st.form_submit_button("Create Account", use_container_width=True)

        if submitted:
            if not all([username, email, password, confirm_password]):
                st.error("Please fill in all required fields")
                return

            if password != confirm_password:
                st.error("Passwords do not match")
                return

            if not terms:
                st.error("Please accept the Terms of Service")
                return

            auth_service = get_auth_service_instance()

            try:
                user = auth_service.register(
                    username=username,
                    email=email,
                    password=password,
                    first_name=first_name,
                    last_name=last_name,
                )

                st.success("Account created! Please wait for activation or contact an administrator.")
                st.info("Your account is pending activation. An administrator will review your request.")

            except AuthenticationError as e:
                st.error(str(e))


def show_password_change_modal():
    """Show forced password change dialog."""
    st.warning("You must change your password before continuing.")

    with st.form("password_change_form"):
        current_password = st.text_input("Current Password", type="password")
        new_password = st.text_input("New Password", type="password")
        confirm_password = st.text_input("Confirm New Password", type="password")

        submitted = st.form_submit_button("Change Password", use_container_width=True)

        if submitted:
            if not all([current_password, new_password, confirm_password]):
                st.error("Please fill in all fields")
                return

            if new_password != confirm_password:
                st.error("Passwords do not match")
                return

            auth_service = get_auth_service_instance()

            try:
                success = auth_service.change_password(
                    user_id=st.session_state.user.id,
                    current_password=current_password,
                    new_password=new_password,
                )

                if success:
                    st.session_state.force_password_change = False
                    st.success("Password changed successfully!")
                    st.rerun()

            except AuthenticationError as e:
                st.error(str(e))


def show_user_menu():
    """Display the user menu in the sidebar."""
    user = st.session_state.user

    if not user:
        return

    with st.sidebar:
        st.markdown("---")

        # User info
        st.markdown(f"**{user.full_name}**")
        st.caption(f"@{user.username}")

        if user.is_superadmin:
            st.markdown("*Super Administrator*")
        elif user.roles:
            st.markdown(f"*{', '.join(user.roles or [])}*")

        # User actions
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Profile", use_container_width=True):
                st.session_state.current_page = "profile"
                st.rerun()

        with col2:
            if st.button("Logout", use_container_width=True):
                logout()
                st.rerun()

        # Admin link for authorized users
        if user.is_superadmin or any(r in (user.roles or []) for r in ["admin", "superadmin"]):
            st.markdown("---")
            if st.button("Admin Dashboard", use_container_width=True, type="primary"):
                st.session_state.current_page = "admin"
                st.rerun()


def show_profile_page():
    """Display the user profile page."""
    user = st.session_state.user

    st.title("User Profile")

    col1, col2 = st.columns([1, 2])

    with col1:
        # Avatar placeholder
        avatar_letter = user.first_name[0] if user.first_name else user.username[0].upper()
        st.markdown(f"""
        <div style="
            width: 150px;
            height: 150px;
            background: linear-gradient(135deg, #1f77b4, #4fa3d6);
            border-radius: 75px;
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-size: 48px;
            font-weight: bold;
            margin: 1rem auto;
        ">
            {avatar_letter}
        </div>
        """, unsafe_allow_html=True)

        st.markdown(f"### {user.full_name}")
        st.caption(f"@{user.username}")

        if user.is_superadmin:
            st.info("Super Administrator")

    with col2:
        tab1, tab2, tab3 = st.tabs(["Profile Info", "Security", "Settings"])

        with tab1:
            st.markdown("#### Profile Details")
            st.text_input("First Name", value=user.first_name or "", disabled=True)
            st.text_input("Last Name", value=user.last_name or "", disabled=True)
            st.text_input("Email", value=user.email, disabled=True)
            st.info("Contact an administrator to update your profile information.")

            st.markdown("---")

            # Account info
            st.markdown("#### Account Information")
            col_a, col_b = st.columns(2)
            with col_a:
                st.markdown(f"**User ID:** `{user.id[:8]}...`")
                st.markdown(f"**Status:** {user.status.title()}")
            with col_b:
                st.markdown(f"**Created:** {user.created_at.strftime('%Y-%m-%d')}")
                if user.last_login:
                    st.markdown(f"**Last Login:** {user.last_login.strftime('%Y-%m-%d %H:%M')}")

        with tab2:
            st.markdown("#### Change Password")

            with st.form("change_password_form"):
                current_pwd = st.text_input("Current Password", type="password")
                new_pwd = st.text_input("New Password", type="password")
                confirm_pwd = st.text_input("Confirm Password", type="password")

                submitted = st.form_submit_button("Change Password")

                if submitted:
                    if new_pwd != confirm_pwd:
                        st.error("Passwords do not match")
                    else:
                        auth_service = get_auth_service_instance()
                        try:
                            auth_service.change_password(
                                user_id=user.id,
                                current_password=current_pwd,
                                new_password=new_pwd,
                            )
                            st.success("Password changed successfully!")
                        except AuthenticationError as e:
                            st.error(str(e))

            st.markdown("---")
            st.markdown("#### Session Information")
            st.info("You are currently logged in. Click Logout in the sidebar to end your session.")

        with tab3:
            st.markdown("#### User Settings")

            # Theme preference
            theme = st.selectbox(
                "Theme",
                ["System Default", "Light", "Dark"],
                index=0
            )

            # Notifications
            st.markdown("#### Notification Preferences")
            email_notif = st.checkbox("Email notifications", value=True)
            pipeline_alerts = st.checkbox("Pipeline failure alerts", value=True)
            data_quality_alerts = st.checkbox("Data quality alerts", value=True)

            if st.button("Save Settings"):
                st.success("Settings saved!")
