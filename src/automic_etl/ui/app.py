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
    NavItem("profiling", "Data Profiling", "📊"),
    NavItem("lineage", "Data Lineage", "🌳"),
    NavItem("query", "Query Studio", "💬"),
    NavItem("monitoring", "Monitoring", "📈"),
    NavItem("connectors", "Connectors", "🔌"),
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
    """Apply custom CSS styling using the theme system."""
    from automic_etl.ui.theme import get_streamlit_css, get_theme, ThemeMode

    # Get user's theme preference from session state
    theme_mode = st.session_state.get("theme_mode", ThemeMode.LIGHT)
    theme = get_theme(theme_mode)

    # Apply theme CSS
    st.markdown(get_streamlit_css(theme), unsafe_allow_html=True)

    # Additional custom CSS
    st.markdown("""
    <style>
    /* Main header styling */
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        background: linear-gradient(135deg, var(--primary), var(--primary-light));
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: var(--text-secondary);
        margin-bottom: 2rem;
    }

    /* Cards and containers */
    .metric-card {
        background: linear-gradient(135deg, var(--surface), var(--background));
        padding: 1.5rem;
        border-radius: var(--radius-lg);
        border: 1px solid var(--border-light);
        margin: 0.5rem 0;
        box-shadow: var(--shadow-sm);
    }
    .info-card {
        background: var(--background);
        padding: 1.5rem;
        border-radius: var(--radius-lg);
        border-left: 4px solid var(--primary);
        margin: 1rem 0;
        box-shadow: var(--shadow-md);
    }

    /* User avatar */
    .user-avatar {
        width: 40px;
        height: 40px;
        border-radius: var(--radius-full);
        background: linear-gradient(135deg, var(--primary), var(--primary-light));
        display: flex;
        align-items: center;
        justify-content: center;
        color: var(--text-inverse);
        font-weight: bold;
        font-size: 1.2rem;
    }

    /* Navigation highlight */
    .nav-active {
        background: linear-gradient(135deg, var(--primary), var(--primary-hover));
        color: var(--text-inverse);
        border-radius: var(--radius-md);
    }
    </style>
    """, unsafe_allow_html=True)


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
        "connectors": show_connectors_page,
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
        elif page_key == "profiling":
            from automic_etl.ui.pages.data_profiling import show_data_profiling_page
            return show_data_profiling_page
        elif page_key == "lineage":
            from automic_etl.ui.pages.lineage import show_lineage_page
            return show_lineage_page
        elif page_key == "query":
            from automic_etl.ui.pages.query_studio import show_query_studio_page
            return show_query_studio_page
        elif page_key == "monitoring":
            from automic_etl.ui.pages.monitoring import show_monitoring_page
            return show_monitoring_page
        elif page_key == "settings":
            from automic_etl.ui.pages.settings import show_settings_page
            return show_settings_page
    except ImportError:
        pass

    return None


def run_app():
    """Run the main Streamlit application."""
    create_app()

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
    """Render the sidebar header with branding."""
    st.markdown("""
    <div style="text-align: center; padding: 1rem 0;">
        <h1 style="margin: 0; font-size: 1.5rem;">
            <span style="color: var(--primary);">Automic</span> ETL
        </h1>
        <p style="color: var(--text-secondary); font-size: 0.8rem; margin: 0;">AI-Powered Data Platform</p>
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
    """Display the home dashboard."""
    from automic_etl.ui.widgets import (
        dashboard_header,
        metrics_row,
        MetricData,
        pipeline_status_widget,
        activity_feed_widget,
        data_quality_score_widget,
        storage_breakdown_widget,
        system_health_widget,
    )
    from datetime import datetime

    user = st.session_state.user

    # Dashboard header
    dashboard_header(
        title=f"Welcome, {user.first_name or user.username}!",
        subtitle="AI-Augmented Data Lakehouse Platform",
        last_updated=datetime.now(),
    )

    # Key Metrics Row
    metrics = [
        MetricData(label="Tables", value="12", delta=2, delta_suffix=" new", icon="📊"),
        MetricData(label="Pipelines", value="5", delta=1, delta_suffix=" running", icon="🔧"),
        MetricData(label="Data Processed", value="2.4 GB", delta=450, delta_suffix=" MB", icon="💾"),
        MetricData(label="LLM Queries", value="156", delta=23, delta_suffix=" today", icon="🤖"),
    ]
    metrics_row(metrics, columns=4)

    st.markdown("---")

    # Main dashboard content
    col_left, col_right = st.columns([2, 1])

    with col_left:
        # Lakehouse Overview
        st.markdown("### Lakehouse Overview")
        st.markdown("#### Medallion Architecture")

        bronze_col, silver_col, gold_col = st.columns(3)

        with bronze_col:
            st.markdown("""
            <div style="
                background: linear-gradient(135deg, #cd7f32 0%, #8b5a2b 100%);
                color: white;
                padding: 1rem;
                border-radius: var(--radius-lg);
                text-align: center;
            ">
                <h4 style="margin: 0;">Bronze Layer</h4>
                <p style="margin: 0.5rem 0; font-size: 0.9rem;">Raw Data</p>
                <hr style="border-color: rgba(255,255,255,0.3);">
                <p style="margin: 0; font-size: 0.85rem;">8 tables • 1.2 GB</p>
                <p style="margin: 0; font-size: 0.75rem; opacity: 0.9;">Last ingestion: 2 min ago</p>
            </div>
            """, unsafe_allow_html=True)

        with silver_col:
            st.markdown("""
            <div style="
                background: linear-gradient(135deg, #c0c0c0 0%, #a9a9a9 100%);
                color: #333;
                padding: 1rem;
                border-radius: var(--radius-lg);
                text-align: center;
            ">
                <h4 style="margin: 0;">Silver Layer</h4>
                <p style="margin: 0.5rem 0; font-size: 0.9rem;">Cleaned & Validated</p>
                <hr style="border-color: rgba(0,0,0,0.2);">
                <p style="margin: 0; font-size: 0.85rem;">6 tables • 890 MB</p>
                <p style="margin: 0; font-size: 0.75rem; opacity: 0.8;">Last transform: 5 min ago</p>
            </div>
            """, unsafe_allow_html=True)

        with gold_col:
            st.markdown("""
            <div style="
                background: linear-gradient(135deg, #ffd700 0%, #daa520 100%);
                color: #333;
                padding: 1rem;
                border-radius: var(--radius-lg);
                text-align: center;
            ">
                <h4 style="margin: 0;">Gold Layer</h4>
                <p style="margin: 0.5rem 0; font-size: 0.9rem;">Aggregated & Enriched</p>
                <hr style="border-color: rgba(0,0,0,0.2);">
                <p style="margin: 0; font-size: 0.85rem;">4 tables • 320 MB</p>
                <p style="margin: 0; font-size: 0.75rem; opacity: 0.8;">Last aggregation: 1 hr ago</p>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("---")

        # Recent Activity
        activity_data = [
            {"action": "create", "user": "System", "timestamp": datetime.now(), "details": "Data Ingestion: sales_data.csv → Bronze"},
            {"action": "complete", "user": "Pipeline", "timestamp": datetime.now(), "details": "Transform: Bronze → Silver (customers)"},
            {"action": "run", "user": user.username, "timestamp": datetime.now(), "details": "LLM Query: 'Show top customers'"},
            {"action": "complete", "user": "AI", "timestamp": datetime.now(), "details": "Entity Extraction: 45 entities from documents"},
            {"action": "fail", "user": "Pipeline", "timestamp": datetime.now(), "details": "daily_etl_pipeline (timeout)"},
        ]
        activity_feed_widget(activity_data, title="Recent Activity", max_items=5)

    with col_right:
        # Data Quality Score
        data_quality_score_widget(
            score=87,
            title="Data Quality Score",
            breakdown={
                "Completeness": 92,
                "Accuracy": 88,
                "Consistency": 85,
                "Timeliness": 83,
            }
        )

        st.markdown("---")

        # Storage Breakdown
        storage_breakdown_widget(
            breakdown={
                "Bronze": 1.2,
                "Silver": 0.89,
                "Gold": 0.32,
            },
            title="Storage Usage",
            total=5.0
        )

        st.markdown("---")

        # Quick Actions
        st.markdown("### Quick Actions")

        if st.button("📥 Ingest Data", use_container_width=True):
            navigate_to("ingestion")

        if st.button("🔧 Create Pipeline", use_container_width=True):
            navigate_to("pipelines")

        if st.button("💬 Query Data", use_container_width=True):
            navigate_to("query")

        if st.button("🌳 View Lineage", use_container_width=True):
            navigate_to("lineage")

    # System Health
    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        system_health_widget({
            "Delta Lake": "healthy",
            "LLM Service": "healthy",
            "Scheduler": "healthy",
            "Notification Service": "degraded",
        }, title="System Health")

    with col2:
        # Pipeline Status
        pipelines = [
            {"name": "customer_processing", "status": "running", "last_run": datetime.now(), "duration": 45},
            {"name": "orders_etl", "status": "completed", "last_run": datetime.now(), "duration": 120},
            {"name": "daily_aggregation", "status": "pending", "last_run": None, "duration": None},
            {"name": "data_quality_check", "status": "failed", "last_run": datetime.now(), "duration": 30},
        ]
        pipeline_status_widget(pipelines, title="Pipeline Status", show_details=True)

    # Quick Start Guide
    st.markdown("---")
    with st.expander("Quick Start Guide", expanded=False):
        st.markdown("""
        ### Getting Started with Automic ETL

        1. **Configure Your Data Source**
           - Go to Settings → Data Sources
           - Add your cloud storage credentials (AWS/GCS/Azure)

        2. **Ingest Data**
           - Use Data Ingestion to upload files or connect to databases
           - Data automatically lands in the Bronze layer

        3. **Build Pipelines**
           - Use Pipeline Builder to create transformation pipelines
           - Define Bronze → Silver → Gold processing

        4. **Query with AI**
           - Use Query Studio to ask questions in natural language
           - The LLM converts your questions to SQL automatically

        5. **Monitor & Profile**
           - View data quality metrics in Data Profiling
           - Track pipeline runs in Monitoring

        6. **View Data Lineage**
           - Use Data Lineage to understand data flow
           - Perform impact analysis before making changes
        """)


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
