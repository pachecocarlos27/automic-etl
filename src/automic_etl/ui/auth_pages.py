"""Authentication pages for the Streamlit UI."""

from __future__ import annotations

import streamlit as st
from datetime import datetime

from automic_etl.auth.manager import AuthManager, AuthenticationError
from automic_etl.auth.models import UserStatus


def get_auth_manager() -> AuthManager:
    """Get or create auth manager in session state."""
    if "auth_manager" not in st.session_state:
        st.session_state.auth_manager = AuthManager()
    return st.session_state.auth_manager


def init_session_state():
    """Initialize session state variables."""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if "user" not in st.session_state:
        st.session_state.user = None
    if "token" not in st.session_state:
        st.session_state.token = None
    if "session" not in st.session_state:
        st.session_state.session = None


def check_authentication() -> bool:
    """Check if user is authenticated."""
    init_session_state()

    if not st.session_state.authenticated or not st.session_state.token:
        return False

    auth_manager = get_auth_manager()
    result = auth_manager.validate_session(st.session_state.token)

    if not result:
        logout()
        return False

    user, session = result
    st.session_state.user = user
    st.session_state.session = session
    return True


def logout():
    """Log out the current user."""
    if st.session_state.token:
        auth_manager = get_auth_manager()
        auth_manager.logout(st.session_state.token)

    st.session_state.authenticated = False
    st.session_state.user = None
    st.session_state.token = None
    st.session_state.session = None


def show_login_page():
    """Display the login page with modern professional design."""
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    /* Screen-reader only: hide skip link visually but keep accessible */
    a[href="#main-content"],
    .stApp a[href="#main-content"],
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
    
    /* Show skip link on focus for keyboard users */
    a[href="#main-content"]:focus,
    .stApp a[href="#main-content"]:focus,
    a[data-testid="stSkipLink"]:focus {
        position: fixed !important;
        top: 1rem !important;
        left: 1rem !important;
        width: auto !important;
        height: auto !important;
        padding: 0.75rem 1.25rem !important;
        margin: 0 !important;
        overflow: visible !important;
        clip: auto !important;
        white-space: normal !important;
        background: #0066FF !important;
        color: white !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        font-size: 0.875rem !important;
        z-index: 9999 !important;
        box-shadow: 0 4px 12px rgba(0, 102, 255, 0.3) !important;
        text-decoration: none !important;
    }
    
    /* Hide sidebar on login page */
    [data-testid="stSidebar"] {
        display: none !important;
    }
    
    /* Expand main content to full width */
    .main .block-container {
        max-width: 100% !important;
        padding-left: 1rem !important;
        padding-right: 1rem !important;
    }

    /* Login page styles */
    .login-wrapper {
        min-height: 85vh;
        display: flex;
        align-items: center;
        justify-content: center;
        padding: 2rem 1rem;
    }
    
    .login-card {
        background: white;
        border-radius: 24px;
        padding: 2.5rem;
        width: 100%;
        max-width: 420px;
        box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.15),
                    0 0 0 1px rgba(0, 0, 0, 0.05);
    }
    
    .login-logo {
        text-align: center;
        margin-bottom: 2rem;
    }
    
    .login-logo-icon {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 72px;
        height: 72px;
        background: linear-gradient(135deg, #0066FF 0%, #00D4AA 100%);
        border-radius: 18px;
        margin-bottom: 1.25rem;
        box-shadow: 0 16px 32px -8px rgba(0, 102, 255, 0.35);
        font-size: 2rem;
    }
    
    .login-logo h1 {
        font-family: 'Inter', sans-serif;
        font-size: 1.75rem;
        font-weight: 800;
        letter-spacing: -0.04em;
        margin: 0 0 0.375rem;
        color: #0F172A;
    }
    
    .login-logo p {
        color: #64748B;
        font-size: 0.875rem;
        font-weight: 500;
        margin: 0;
    }
    
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0.375rem !important;
        background: #F1F5F9 !important;
        padding: 0.375rem !important;
        border-radius: 14px !important;
        border: none !important;
        margin-bottom: 1.5rem !important;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 10px !important;
        padding: 0.75rem 1.25rem !important;
        font-weight: 600 !important;
        font-size: 0.875rem !important;
        color: #64748B !important;
        transition: all 0.2s ease !important;
        flex: 1 !important;
        justify-content: center !important;
    }
    
    .stTabs [aria-selected="true"] {
        background: white !important;
        color: #0066FF !important;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.06) !important;
    }
    
    .stTabs [data-baseweb="tab-highlight"],
    .stTabs [data-baseweb="tab-border"] {
        display: none !important;
    }
    
    /* Form inputs */
    .stTextInput label {
        font-weight: 600 !important;
        color: #334155 !important;
        font-size: 0.8125rem !important;
        margin-bottom: 0.25rem !important;
    }
    
    .stTextInput > div > div > input {
        border-radius: 10px !important;
        border: 2px solid #E2E8F0 !important;
        padding: 0.75rem 1rem !important;
        font-size: 0.9375rem !important;
        transition: all 0.2s ease !important;
        background: #FAFBFC !important;
    }
    
    .stTextInput > div > div > input:hover {
        border-color: #CBD5E1 !important;
    }
    
    .stTextInput > div > div > input:focus {
        border-color: #0066FF !important;
        box-shadow: 0 0 0 3px rgba(0, 102, 255, 0.1) !important;
        background: white !important;
    }
    
    /* Button styling */
    .stButton > button {
        width: 100% !important;
        background: linear-gradient(135deg, #0066FF 0%, #0052CC 100%) !important;
        color: white !important;
        border: none !important;
        border-radius: 10px !important;
        padding: 0.875rem 1.5rem !important;
        font-weight: 600 !important;
        font-size: 0.9375rem !important;
        box-shadow: 0 4px 14px rgba(0, 102, 255, 0.25) !important;
        transition: all 0.2s ease !important;
        margin-top: 0.5rem !important;
    }
    
    .stButton > button:hover {
        transform: translateY(-1px) !important;
        box-shadow: 0 6px 20px rgba(0, 102, 255, 0.35) !important;
    }
    
    /* Checkbox */
    .stCheckbox {
        padding: 0.5rem 0 !important;
    }
    
    /* Alerts */
    .stAlert {
        border-radius: 10px !important;
        border: none !important;
    }
    
    .login-footer {
        text-align: center;
        margin-top: 1.5rem;
        padding-top: 1.25rem;
        border-top: 1px solid #E2E8F0;
    }
    
    .login-footer p {
        color: #94A3B8;
        font-size: 0.75rem;
        margin: 0;
    }

    /* Main container */
    .main .block-container {
        padding-top: 1rem !important;
    }
    </style>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
        <div class="login-wrapper">
            <div class="login-card">
                <div class="login-logo">
                    <div class="login-logo-icon">
                        <span style="filter: drop-shadow(0 2px 4px rgba(0,0,0,0.2));">⚡</span>
                    </div>
                    <h1>Automic ETL</h1>
                    <p>AI-Augmented Data Lakehouse Platform</p>
                </div>
        """, unsafe_allow_html=True)

        tab1, tab2 = st.tabs(["Login", "Register"])

        with tab1:
            show_login_form()

        with tab2:
            show_register_form()
        
        st.markdown("""
                <div class="login-footer">
                    <p>Secure enterprise data platform</p>
                </div>
            </div>
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

            auth_manager = get_auth_manager()

            try:
                user, token, session = auth_manager.authenticate(
                    username=username,
                    password=password,
                )

                st.session_state.authenticated = True
                st.session_state.user = user
                st.session_state.token = token
                st.session_state.session = session

                # Check if password change required
                if user.metadata.get("force_password_change"):
                    st.session_state.force_password_change = True

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

            auth_manager = get_auth_manager()

            try:
                user = auth_manager.register(
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

            auth_manager = get_auth_manager()

            try:
                success = auth_manager.change_password(
                    user_id=st.session_state.user.user_id,
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
        st.markdown(f"**{user.display_name}**")
        st.caption(f"@{user.username}")

        if user.is_superadmin:
            st.markdown("*Super Administrator*")
        elif user.roles:
            st.markdown(f"*{', '.join(user.roles)}*")

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
        if user.is_superadmin or any(r in user.roles for r in ["admin", "superadmin"]):
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
            {user.first_name[0] if user.first_name else user.username[0].upper()}
        </div>
        """, unsafe_allow_html=True)

        st.markdown(f"### {user.display_name}")
        st.caption(f"@{user.username}")

        if user.is_superadmin:
            st.info("Super Administrator")

    with col2:
        tab1, tab2, tab3 = st.tabs(["Profile Info", "Security", "Settings"])

        with tab1:
            with st.form("profile_form"):
                first_name = st.text_input("First Name", value=user.first_name)
                last_name = st.text_input("Last Name", value=user.last_name)
                email = st.text_input("Email", value=user.email)

                submitted = st.form_submit_button("Update Profile")

                if submitted:
                    auth_manager = get_auth_manager()
                    try:
                        updated_user = auth_manager.update_user(
                            user_id=user.user_id,
                            first_name=first_name,
                            last_name=last_name,
                            email=email,
                        )
                        if updated_user:
                            st.session_state.user = updated_user
                            st.success("Profile updated!")
                            st.rerun()
                    except AuthenticationError as e:
                        st.error(str(e))

            st.markdown("---")

            # Account info
            st.markdown("#### Account Information")
            col_a, col_b = st.columns(2)
            with col_a:
                st.markdown(f"**User ID:** `{user.user_id[:8]}...`")
                st.markdown(f"**Status:** {user.status.value.title()}")
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
                        auth_manager = get_auth_manager()
                        try:
                            auth_manager.change_password(
                                user_id=user.user_id,
                                current_password=current_pwd,
                                new_password=new_pwd,
                            )
                            st.success("Password changed successfully!")
                        except AuthenticationError as e:
                            st.error(str(e))

            st.markdown("---")

            # Active sessions
            st.markdown("#### Active Sessions")
            auth_manager = get_auth_manager()
            sessions = auth_manager.session_manager.get_user_sessions(user.user_id)

            for sess in sessions:
                with st.container():
                    col1, col2, col3 = st.columns([2, 2, 1])
                    with col1:
                        current = " (current)" if sess.session_id == st.session_state.session.session_id else ""
                        st.markdown(f"**Session{current}**")
                        st.caption(f"IP: {sess.ip_address or 'Unknown'}")
                    with col2:
                        st.markdown(f"Created: {sess.created_at.strftime('%Y-%m-%d %H:%M')}")
                        st.caption(f"Expires: {sess.expires_at.strftime('%Y-%m-%d %H:%M')}")
                    with col3:
                        if sess.session_id != st.session_state.session.session_id:
                            if st.button("Revoke", key=f"revoke_{sess.session_id}"):
                                auth_manager.session_manager.invalidate_session(sess.session_id)
                                st.rerun()

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
                auth_manager = get_auth_manager()
                auth_manager.update_user(
                    user_id=user.user_id,
                    settings={
                        "theme": theme,
                        "email_notifications": email_notif,
                        "pipeline_alerts": pipeline_alerts,
                        "data_quality_alerts": data_quality_alerts,
                    }
                )
                st.success("Settings saved!")
