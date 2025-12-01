"""Main Streamlit application for Automic ETL."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable
import streamlit as st
from pathlib import Path


@dataclass
class NavItem:
    """Navigation item definition."""
    key: str
    label: str
    icon: str
    page_func: Callable | None = None
    requires_admin: bool = False


# Define navigation structure
NAV_ITEMS = [
    NavItem("home", "Home", "🏠"),
    NavItem("ingestion", "Data Ingestion", "📥"),
    NavItem("pipelines", "Pipeline Builder", "🔧"),
    NavItem("jobs", "Jobs & Orchestration", "📅"),
    NavItem("profiling", "Data Profiling", "📊"),
    NavItem("validation", "Data Validation", "✅"),
    NavItem("lineage", "Data Lineage", "🌳"),
    NavItem("query", "Query Studio", "💬"),
    NavItem("ai_services", "AI Services", "🤖"),
    NavItem("monitoring", "Monitoring", "📈"),
    NavItem("alerts", "Alerts", "🔔"),
    NavItem("connectors", "Connectors", "🔌"),
    NavItem("integrations", "Integrations", "🔗"),
    NavItem("settings", "Settings", "⚙️"),
]


def create_app():
    """Configure the Streamlit app settings."""
    st.set_page_config(
        page_title="Automic ETL",
        page_icon="🔄",
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={
            "Get Help": "https://github.com/datantllc/automic-etl",
            "Report a bug": "https://github.com/datantllc/automic-etl/issues",
            "About": "# Automic ETL\nAI-Augmented Data Lakehouse Platform",
        },
    )


def apply_custom_css():
    """Apply custom CSS styling using the minimal theme system."""
    from automic_etl.ui.theme import get_streamlit_css, get_theme, ThemeMode

    theme_mode = st.session_state.get("theme_mode", ThemeMode.LIGHT)
    theme = get_theme(theme_mode)
    st.markdown(get_streamlit_css(theme), unsafe_allow_html=True)


def init_app_state():
    """Initialize application state."""
    defaults = {
        "current_page": "home",
        "theme_mode": "light",
        "sidebar_expanded": True,
        "notifications": [],
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def navigate_to(page: str):
    """Navigate to a specific page."""
    st.session_state.current_page = page
    st.rerun()


def get_page_function(page_key: str) -> Callable | None:
    """Get the page function for a given page key."""
    page_map = {
        "home": show_home_page,
    }

    # Try to get from map first
    if page_key in page_map:
        return page_map[page_key]

    # Try to import from pages module
    try:
        if page_key == "ingestion":
            from automic_etl.ui.pages.ingestion import show_ingestion_page
            return show_ingestion_page
        elif page_key == "pipelines":
            from automic_etl.ui.pages.pipeline_builder import show_pipeline_builder_page
            return show_pipeline_builder_page
        elif page_key == "jobs":
            from automic_etl.ui.pages.jobs import show_jobs_page
            return show_jobs_page
        elif page_key == "profiling":
            from automic_etl.ui.pages.data_profiling import show_data_profiling_page
            return show_data_profiling_page
        elif page_key == "validation":
            from automic_etl.ui.pages.validation import show_validation_page
            return show_validation_page
        elif page_key == "lineage":
            from automic_etl.ui.pages.lineage import show_lineage_page
            return show_lineage_page
        elif page_key == "query":
            from automic_etl.ui.pages.query_studio import show_query_studio_page
            return show_query_studio_page
        elif page_key == "ai_services":
            from automic_etl.ui.pages.ai_services import show_ai_services_page
            return show_ai_services_page
        elif page_key == "monitoring":
            from automic_etl.ui.pages.monitoring import show_monitoring_page
            return show_monitoring_page
        elif page_key == "alerts":
            from automic_etl.ui.pages.alerts import show_alerts_page
            return show_alerts_page
        elif page_key == "connectors":
            from automic_etl.ui.pages.connectors_mgmt import show_connectors_management_page
            return show_connectors_management_page
        elif page_key == "integrations":
            from automic_etl.ui.pages.integrations import show_integrations_page
            return show_integrations_page
        elif page_key == "settings":
            from automic_etl.ui.pages.settings import show_settings_page
            return show_settings_page
        elif page_key == "company_admin":
            from automic_etl.ui.pages.company_admin import render_company_admin_page
            return render_company_admin_page
        elif page_key == "superadmin":
            from automic_etl.ui.pages.superadmin import render_superadmin_page
            return render_superadmin_page
    except ImportError:
        pass

    return None


def run_app():
    """Run the main Streamlit application."""
    create_app()

    # Initialize database and seed superadmin from environment variables
    from automic_etl.db.auth_service import get_auth_service
    auth_service = get_auth_service()
    auth_service.initialize()

    # Initialize app state first
    init_app_state()

    # Initialize centralized state manager
    from automic_etl.ui.state import get_state
    state = get_state()
    state.init_state()

    # Apply custom CSS with theme
    apply_custom_css()

    # Inject keyboard shortcuts and notifications
    from automic_etl.ui.shortcuts import inject_keyboard_shortcuts, add_aria_labels, render_skip_link
    from automic_etl.ui.notifications import inject_notification_styles, render_toast_container
    from automic_etl.ui.search import render_command_palette

    inject_keyboard_shortcuts()
    inject_notification_styles()
    add_aria_labels()
    render_skip_link()
    render_command_palette()

    # Import auth components
    from automic_etl.ui.auth_pages import (
        check_authentication,
        show_login_page,
        show_user_menu,
        show_profile_page,
        show_password_change_modal,
        init_session_state,
    )
    from automic_etl.ui.admin_pages import show_admin_dashboard

    init_session_state()

    # Check authentication
    if not check_authentication():
        show_login_page()
        return

    # Check for forced password change
    if st.session_state.get("force_password_change"):
        show_password_change_modal()
        return

    # Authenticated user - show main app
    user = st.session_state.user

    # Render toast notifications
    render_toast_container()

    # Sidebar navigation
    with st.sidebar:
        _render_sidebar_header()

        # Global search in sidebar
        _render_sidebar_search()

        st.markdown("---")

        # Build navigation menu
        current_page = st.session_state.get("current_page", "home")
        nav_keys = [item.key for item in NAV_ITEMS]
        nav_labels = {item.key: f"{item.icon} {item.label}" for item in NAV_ITEMS}

        page = st.radio(
            "Navigation",
            nav_keys,
            format_func=lambda x: nav_labels[x],
            label_visibility="collapsed",
            key="nav_radio",
            index=nav_keys.index(current_page) if current_page in nav_keys else 0,
        )

        if page != current_page and page in nav_keys:
            navigate_to(page)

        # User menu
        show_user_menu()

    # Route to page
    _route_to_page(current_page, show_admin_dashboard, show_profile_page)


def _render_sidebar_header():
    """Render minimal sidebar header."""
    st.markdown("""
    <div style="padding: 1.25rem 1rem 0.75rem;">
        <div style="display: flex; align-items: center; gap: 0.75rem;">
            <div style="
                width: 36px;
                height: 36px;
                background: #2563EB;
                border-radius: 8px;
                display: flex;
                align-items: center;
                justify-content: center;
            ">
                <span style="font-size: 1.125rem;">⚡</span>
            </div>
            <div>
                <div style="font-size: 1.125rem; font-weight: 700; color: white; letter-spacing: -0.02em;">Automic</div>
                <div style="font-size: 0.6875rem; color: #6B7280; font-weight: 500; text-transform: uppercase; letter-spacing: 0.04em;">ETL Platform</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def _render_sidebar_search():
    """Render the global search bar in sidebar."""
    from automic_etl.ui.search import get_global_search

    query = st.text_input(
        "Search",
        placeholder="🔍 Search... (Ctrl+K)",
        key="sidebar_search",
        label_visibility="collapsed",
    )

    if query and len(query) >= 2:
        search = get_global_search()
        results = search.search(query)

        if results:
            st.markdown("##### Quick Results")
            for result in results[:5]:
                if st.button(
                    f"{result.icon} {result.title}",
                    key=f"search_{result.title}_{result.type}",
                    use_container_width=True,
                ):
                    navigate_to(result.page)


def _route_to_page(current_page: str, admin_func: Callable, profile_func: Callable):
    """Route to the appropriate page based on current_page."""
    # Handle special pages first
    if current_page == "admin":
        admin_func()
        return
    elif current_page == "profile":
        profile_func()
        return

    # Get page function
    page_func = get_page_function(current_page)

    if page_func:
        page_func()
    else:
        # Find the nav item for placeholder
        nav_item = next((item for item in NAV_ITEMS if item.key == current_page), None)
        if nav_item:
            show_placeholder_page(nav_item.label, f"Manage {nav_item.label.lower()}")
        else:
            show_placeholder_page("Unknown Page", "Page not found")


def show_home_page():
    """Display Material Design 3 home dashboard."""
    user = st.session_state.user
    
    st.markdown(f"""
    <div style="margin-bottom: 2.5rem;">
        <h1 style="font-size: 2rem; font-weight: 700; color: #212121; margin: 0 0 0.5rem; letter-spacing: -0.03em; font-family: 'Inter', sans-serif;">
            Welcome back, {user.first_name or user.username}
        </h1>
        <p style="font-size: 1rem; color: #757575; margin: 0; font-family: 'Inter', sans-serif;">Your data lakehouse overview</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Tables", "12", "+2")
    with col2:
        st.metric("Pipelines", "5", "1 active")
    with col3:
        st.metric("Data Size", "2.4 GB", "+450 MB")
    with col4:
        st.metric("AI Queries", "156", "+23 today")
    
    st.markdown("<div style='height: 1.5rem'></div>", unsafe_allow_html=True)
    
    col_main, col_side = st.columns([2, 1])
    
    with col_main:
        st.markdown("""<h4 style="font-size: 1.125rem; font-weight: 600; color: #212121; margin: 0 0 1rem; font-family: 'Inter', sans-serif;">Data Layers</h4>""", unsafe_allow_html=True)
        
        layer_cols = st.columns(3)
        
        with layer_cols[0]:
            st.markdown("""
            <div style="background: #EFEBE9; border-radius: 16px; padding: 1.25rem; border-left: 4px solid #8D6E63; transition: box-shadow 0.2s ease;">
                <div style="font-weight: 600; color: #212121; margin-bottom: 0.5rem; font-size: 1rem;">Bronze</div>
                <div style="font-size: 0.8125rem; color: #757575; margin-bottom: 0.75rem;">Raw data</div>
                <div style="display: flex; justify-content: space-between; font-size: 0.8125rem; color: #9E9E9E;">
                    <span>8 tables</span>
                    <span>1.2 GB</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with layer_cols[1]:
            st.markdown("""
            <div style="background: #ECEFF1; border-radius: 16px; padding: 1.25rem; border-left: 4px solid #78909C; transition: box-shadow 0.2s ease;">
                <div style="font-weight: 600; color: #212121; margin-bottom: 0.5rem; font-size: 1rem;">Silver</div>
                <div style="font-size: 0.8125rem; color: #757575; margin-bottom: 0.75rem;">Cleaned</div>
                <div style="display: flex; justify-content: space-between; font-size: 0.8125rem; color: #9E9E9E;">
                    <span>6 tables</span>
                    <span>890 MB</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with layer_cols[2]:
            st.markdown("""
            <div style="background: #FFF8E1; border-radius: 16px; padding: 1.25rem; border-left: 4px solid #FFA000; transition: box-shadow 0.2s ease;">
                <div style="font-weight: 600; color: #212121; margin-bottom: 0.5rem; font-size: 1rem;">Gold</div>
                <div style="font-size: 0.8125rem; color: #757575; margin-bottom: 0.75rem;">Enriched</div>
                <div style="display: flex; justify-content: space-between; font-size: 0.8125rem; color: #9E9E9E;">
                    <span>4 tables</span>
                    <span>320 MB</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("<div style='height: 1.5rem'></div>", unsafe_allow_html=True)
        
        st.markdown("""<h4 style="font-size: 1.125rem; font-weight: 600; color: #212121; margin: 0 0 1rem; font-family: 'Inter', sans-serif;">Recent Activity</h4>""", unsafe_allow_html=True)
        
        activities = [
            ("Data ingested", "sales_data.csv", "2m ago", "#2E7D32", "#E8F5E9"),
            ("Pipeline completed", "Bronze to Silver", "5m ago", "#3F51B5", "#E8EAF6"),
            ("Query executed", "Top customers", "12m ago", "#009688", "#E0F2F1"),
            ("Pipeline failed", "daily_etl", "1h ago", "#D32F2F", "#FFEBEE"),
        ]
        
        for title, detail, time, color, bg in activities:
            st.markdown(f"""
            <div style="display: flex; align-items: center; padding: 0.875rem 1rem; margin-bottom: 0.5rem; background: {bg}; border-radius: 12px; transition: all 0.15s ease;">
                <div style="width: 8px; height: 8px; border-radius: 50%; background: {color}; margin-right: 1rem;"></div>
                <div style="flex: 1;">
                    <div style="font-size: 0.875rem; color: #212121; font-weight: 500;">{title}</div>
                    <div style="font-size: 0.75rem; color: #757575;">{detail}</div>
                </div>
                <div style="font-size: 0.75rem; color: #9E9E9E; font-weight: 500;">{time}</div>
            </div>
            """, unsafe_allow_html=True)
    
    with col_side:
        st.markdown("""<h4 style="font-size: 1.125rem; font-weight: 600; color: #212121; margin: 0 0 1rem; font-family: 'Inter', sans-serif;">Quick Actions</h4>""", unsafe_allow_html=True)
        
        if st.button("Upload Data", use_container_width=True, type="primary"):
            navigate_to("ingestion")
        if st.button("Create Pipeline", use_container_width=True):
            navigate_to("pipelines")
        if st.button("Query Data", use_container_width=True):
            navigate_to("query")
        
        st.markdown("<div style='height: 1.5rem'></div>", unsafe_allow_html=True)
        
        st.markdown("""<h4 style="font-size: 1.125rem; font-weight: 600; color: #212121; margin: 0 0 1rem; font-family: 'Inter', sans-serif;">Data Quality</h4>""", unsafe_allow_html=True)
        st.progress(0.87)
        st.markdown("""<p style="font-size: 0.8125rem; color: #757575; margin-top: 0.5rem;">87% overall score</p>""", unsafe_allow_html=True)


def show_connectors_page():
    """Display the connectors management page."""
    st.title("Connectors")
    st.markdown("Manage your data source and destination connectors.")

    tab1, tab2, tab3 = st.tabs(["Databases", "APIs", "Cloud Storage"])

    with tab1:
        st.markdown("### Database Connectors")

        col1, col2 = st.columns(2)

        with col1:
            with st.expander("PostgreSQL", expanded=True):
                st.markdown("Connect to PostgreSQL databases")
                st.text_input("Host", placeholder="localhost", key="pg_host")
                st.number_input("Port", value=5432, key="pg_port")
                st.text_input("Database", placeholder="mydb", key="pg_db")
                st.text_input("Username", placeholder="postgres", key="pg_user")
                st.text_input("Password", type="password", key="pg_pass")
                if st.button("Test Connection", key="pg_test"):
                    st.success("Connection successful!")

            with st.expander("Snowflake"):
                st.markdown("Connect to Snowflake data warehouse")
                st.text_input("Account", placeholder="account.region", key="sf_account")
                st.text_input("Database", placeholder="MYDB", key="sf_db")
                st.text_input("Warehouse", placeholder="COMPUTE_WH", key="sf_warehouse")

        with col2:
            with st.expander("BigQuery"):
                st.markdown("Connect to Google BigQuery")
                st.text_input("Project ID", placeholder="my-project", key="bq_project")
                st.text_input("Dataset", placeholder="my_dataset", key="bq_dataset")
                st.file_uploader("Service Account JSON", key="bq_sa")

            with st.expander("MySQL"):
                st.markdown("Connect to MySQL databases")
                st.text_input("Host", placeholder="localhost", key="mysql_host")
                st.number_input("Port", value=3306, key="mysql_port")

    with tab2:
        st.markdown("### API Connectors")

        col1, col2 = st.columns(2)

        with col1:
            with st.expander("Salesforce"):
                st.markdown("Connect to Salesforce CRM")
                st.text_input("Consumer Key", key="sf_consumer_key")
                st.text_input("Consumer Secret", type="password", key="sf_consumer_secret")
                st.text_input("Username", key="sf_username")
                st.text_input("Security Token", type="password", key="sf_token")

            with st.expander("HubSpot"):
                st.markdown("Connect to HubSpot CRM")
                st.text_input("API Key", type="password", key="hs_key")

        with col2:
            with st.expander("Stripe"):
                st.markdown("Connect to Stripe payments")
                st.text_input("API Key", type="password", key="stripe_key")

            with st.expander("REST API"):
                st.markdown("Connect to generic REST APIs")
                st.text_input("Base URL", placeholder="https://api.example.com", key="rest_url")
                st.selectbox("Auth Type", ["None", "API Key", "Bearer Token", "Basic Auth", "OAuth2"], key="rest_auth")

    with tab3:
        st.markdown("### Cloud Storage")

        col1, col2, col3 = st.columns(3)

        with col1:
            with st.expander("AWS S3"):
                st.markdown("Connect to Amazon S3")
                st.text_input("Access Key ID", key="aws_key")
                st.text_input("Secret Access Key", type="password", key="aws_secret")
                st.text_input("Region", placeholder="us-east-1", key="aws_region")
                st.text_input("Bucket", placeholder="my-bucket", key="aws_bucket")

        with col2:
            with st.expander("Google Cloud Storage"):
                st.markdown("Connect to GCS")
                st.text_input("Project ID", key="gcs_project")
                st.file_uploader("Service Account JSON", key="gcs_sa")
                st.text_input("Bucket", placeholder="my-bucket", key="gcs_bucket")

        with col3:
            with st.expander("Azure Blob Storage"):
                st.markdown("Connect to Azure Blob")
                st.text_input("Account Name", key="azure_account")
                st.text_input("Account Key", type="password", key="azure_key")
                st.text_input("Container", placeholder="my-container", key="azure_container")


def show_placeholder_page(title: str, description: str):
    """Show a placeholder page for unimplemented features."""
    st.title(title)
    st.markdown(f"*{description}*")

    st.info("This feature is coming soon. Check back for updates!")

    st.markdown("---")

    st.markdown("""
    ### Coming Features:
    - Full implementation of this module
    - Integration with all connectors
    - AI-powered automation
    """)


if __name__ == "__main__":
    run_app()
