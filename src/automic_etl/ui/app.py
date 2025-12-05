"""Main Streamlit application for Automic ETL - Sleek, minimal design."""

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


# Define navigation structure with cleaner icons
NAV_ITEMS = [
    NavItem("home", "Overview", "○"),
    NavItem("ingestion", "Ingestion", "↓"),
    NavItem("processing", "Processing", "⟳"),
    NavItem("pipelines", "Pipelines", "⊞"),
    NavItem("jobs", "Jobs", "◷"),
    NavItem("profiling", "Profiling", "◈"),
    NavItem("validation", "Validation", "✓"),
    NavItem("lineage", "Lineage", "⋈"),
    NavItem("query", "Query Studio", "▹"),
    NavItem("ai_services", "AI Services", "◇"),
    NavItem("monitoring", "Monitoring", "◉"),
    NavItem("alerts", "Alerts", "◌"),
    NavItem("connectors", "Connectors", "⊗"),
    NavItem("integrations", "Integrations", "⊕"),
    NavItem("settings", "Settings", "≡"),
]


def create_app():
    """Configure the Streamlit app settings."""
    st.set_page_config(
        page_title="Automic ETL",
        page_icon="⚡",
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={
            "Get Help": "https://github.com/datantllc/automic-etl",
            "Report a bug": "https://github.com/datantllc/automic-etl/issues",
            "About": "# Automic ETL\nAI-Augmented Data Lakehouse Platform",
        },
    )


def apply_custom_css():
    """Apply sleek, minimal CSS styling."""
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

    if page_key in page_map:
        return page_map[page_key]

    # Import from pages module
    try:
        if page_key == "ingestion":
            from automic_etl.ui.pages.ingestion import show_ingestion_page
            return show_ingestion_page
        elif page_key == "processing":
            from automic_etl.ui.pages.data_processing import show_data_processing_page
            return show_data_processing_page
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

    # Initialize database and seed superadmin
    from automic_etl.db.auth_service import get_auth_service
    auth_service = get_auth_service()
    auth_service.initialize()

    init_app_state()

    # Initialize centralized state
    from automic_etl.ui.state import get_state
    state = get_state()
    state.init_state()

    apply_custom_css()

    # Inject shortcuts and notifications
    from automic_etl.ui.shortcuts import inject_keyboard_shortcuts, add_aria_labels, render_skip_link
    from automic_etl.ui.notifications import inject_notification_styles, render_toast_container
    from automic_etl.ui.search import render_command_palette

    inject_keyboard_shortcuts()
    inject_notification_styles()
    add_aria_labels()
    render_skip_link()
    render_command_palette()

    # Auth
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

    if not check_authentication():
        show_login_page()
        return

    if st.session_state.get("force_password_change"):
        show_password_change_modal()
        return

    user = st.session_state.user
    render_toast_container()

    # Sidebar
    with st.sidebar:
        _render_sidebar_header()
        _render_sidebar_search()
        st.markdown('<div style="height: 0.5rem;"></div>', unsafe_allow_html=True)

        # Navigation
        current_page = st.session_state.get("current_page", "home")
        nav_keys = [item.key for item in NAV_ITEMS]
        nav_labels = {item.key: f"{item.label}" for item in NAV_ITEMS}

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

        # Spacer
        st.markdown('<div style="flex: 1;"></div>', unsafe_allow_html=True)

        # User menu at bottom
        show_user_menu()

    # Route to page
    _route_to_page(current_page, show_admin_dashboard, show_profile_page)


def _render_sidebar_header():
    """Render sleek sidebar header."""
    st.markdown("""
    <div style="padding: 1rem 0.75rem 1.25rem;">
        <div style="display: flex; align-items: center; gap: 0.625rem;">
            <div style="
                width: 32px;
                height: 32px;
                background: linear-gradient(135deg, #6366F1 0%, #4F46E5 100%);
                border-radius: 8px;
                display: flex;
                align-items: center;
                justify-content: center;
                box-shadow: 0 2px 8px rgba(99, 102, 241, 0.3);
            ">
                <span style="font-size: 1rem; color: white;">⚡</span>
            </div>
            <div>
                <div style="font-size: 1rem; font-weight: 600; color: white; letter-spacing: -0.01em;">Automic</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def _render_sidebar_search():
    """Render minimal search bar."""
    from automic_etl.ui.search import get_global_search

    query = st.text_input(
        "Search",
        placeholder="Search...",
        key="sidebar_search",
        label_visibility="collapsed",
    )

    if query and len(query) >= 2:
        search = get_global_search()
        results = search.search(query)

        if results:
            st.markdown("""
            <p style="font-size: 0.7rem; color: #9CA3AF; text-transform: uppercase; letter-spacing: 0.05em; padding: 0 0.25rem; margin-bottom: 0.5rem;">Results</p>
            """, unsafe_allow_html=True)
            for result in results[:4]:
                if st.button(
                    f"{result.title}",
                    key=f"search_{result.title}_{result.type}",
                    use_container_width=True,
                ):
                    navigate_to(result.page)


def _route_to_page(current_page: str, admin_func: Callable, profile_func: Callable):
    """Route to the appropriate page."""
    if current_page == "admin":
        admin_func()
        return
    elif current_page == "profile":
        profile_func()
        return

    page_func = get_page_function(current_page)

    if page_func:
        page_func()
    else:
        nav_item = next((item for item in NAV_ITEMS if item.key == current_page), None)
        if nav_item:
            show_placeholder_page(nav_item.label, f"Manage {nav_item.label.lower()}")
        else:
            show_placeholder_page("Not Found", "Page not found")


def show_home_page():
    """Display sleek, minimal home dashboard."""
    user = st.session_state.user

    # Page header
    st.markdown(f"""
    <div style="margin-bottom: 2rem;">
        <h1 style="font-size: 1.625rem; font-weight: 600; color: var(--text); margin: 0 0 0.25rem; letter-spacing: -0.02em;">
            Welcome back, {user.first_name or user.username}
        </h1>
        <p style="font-size: 0.9rem; color: var(--text-muted); margin: 0;">Your data lakehouse at a glance</p>
    </div>
    """, unsafe_allow_html=True)

    # Stats row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        _render_stat_card("Tables", "12", "+2 this week", "positive")
    with col2:
        _render_stat_card("Pipelines", "5", "1 running", "neutral")
    with col3:
        _render_stat_card("Storage", "2.4 GB", "+450 MB", "positive")
    with col4:
        _render_stat_card("Queries", "156", "+23 today", "positive")

    st.markdown('<div style="height: 1.5rem;"></div>', unsafe_allow_html=True)

    # Main content
    col_main, col_side = st.columns([2, 1])

    with col_main:
        # Data layers
        st.markdown("""
        <h3 style="font-size: 0.95rem; font-weight: 600; color: var(--text); margin: 0 0 1rem; letter-spacing: -0.01em;">Data Layers</h3>
        """, unsafe_allow_html=True)

        layer_cols = st.columns(3)

        with layer_cols[0]:
            _render_layer_card("Bronze", "Raw ingested data", "8", "1.2 GB", "#A16207", "#FEF9C3")
        with layer_cols[1]:
            _render_layer_card("Silver", "Cleaned & validated", "6", "890 MB", "#6B7280", "#F3F4F6")
        with layer_cols[2]:
            _render_layer_card("Gold", "Business-ready", "4", "320 MB", "#CA8A04", "#FEF3C7")

        st.markdown('<div style="height: 1.5rem;"></div>', unsafe_allow_html=True)

        # Recent activity
        st.markdown("""
        <h3 style="font-size: 0.95rem; font-weight: 600; color: var(--text); margin: 0 0 1rem; letter-spacing: -0.01em;">Recent Activity</h3>
        """, unsafe_allow_html=True)

        activities = [
            ("Data ingested", "sales_data.csv uploaded", "2m ago", "success"),
            ("Pipeline completed", "Bronze → Silver transform", "5m ago", "info"),
            ("Query executed", "Top customers analysis", "12m ago", "neutral"),
            ("Alert triggered", "Data quality threshold", "1h ago", "warning"),
        ]

        for title, detail, time, status in activities:
            _render_activity_item(title, detail, time, status)

    with col_side:
        # Quick actions
        st.markdown("""
        <h3 style="font-size: 0.95rem; font-weight: 600; color: var(--text); margin: 0 0 1rem; letter-spacing: -0.01em;">Quick Actions</h3>
        """, unsafe_allow_html=True)

        if st.button("↓ Upload Data", use_container_width=True, type="primary"):
            navigate_to("ingestion")
        if st.button("⊞ New Pipeline", use_container_width=True):
            navigate_to("pipelines")
        if st.button("▹ Query Data", use_container_width=True):
            navigate_to("query")

        st.markdown('<div style="height: 1.5rem;"></div>', unsafe_allow_html=True)

        # Data quality
        st.markdown("""
        <h3 style="font-size: 0.95rem; font-weight: 600; color: var(--text); margin: 0 0 0.75rem; letter-spacing: -0.01em;">Data Quality</h3>
        """, unsafe_allow_html=True)

        st.markdown("""
        <div style="background: var(--surface-muted); border-radius: 8px; padding: 1rem;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                <span style="font-size: 0.8rem; color: var(--text-secondary);">Overall Score</span>
                <span style="font-size: 1.25rem; font-weight: 600; color: var(--success);">87%</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        st.progress(0.87)

        st.markdown('<div style="height: 1rem;"></div>', unsafe_allow_html=True)

        # System status
        st.markdown("""
        <div style="background: var(--surface-muted); border-radius: 8px; padding: 1rem;">
            <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.5rem;">
                <div style="width: 8px; height: 8px; background: var(--success); border-radius: 50%;"></div>
                <span style="font-size: 0.8rem; color: var(--text-secondary);">All systems operational</span>
            </div>
            <p style="font-size: 0.75rem; color: var(--text-muted); margin: 0;">Last checked: Just now</p>
        </div>
        """, unsafe_allow_html=True)


def _render_stat_card(label: str, value: str, delta: str, delta_type: str):
    """Render a minimal stat card."""
    delta_colors = {
        "positive": "var(--success)",
        "negative": "var(--error)",
        "neutral": "var(--text-muted)",
    }
    color = delta_colors.get(delta_type, "var(--text-muted)")

    st.markdown(f"""
    <div style="background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 1.25rem;">
        <p style="font-size: 0.7rem; font-weight: 500; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.05em; margin: 0 0 0.5rem;">{label}</p>
        <p style="font-size: 1.75rem; font-weight: 700; color: var(--text); margin: 0 0 0.25rem; letter-spacing: -0.02em;">{value}</p>
        <p style="font-size: 0.75rem; color: {color}; margin: 0;">{delta}</p>
    </div>
    """, unsafe_allow_html=True)


def _render_layer_card(name: str, description: str, tables: str, size: str, accent_color: str, bg_color: str):
    """Render a data layer card."""
    st.markdown(f"""
    <div style="background: {bg_color}; border-radius: 12px; padding: 1.25rem; border-left: 3px solid {accent_color};">
        <p style="font-size: 0.95rem; font-weight: 600; color: var(--text); margin: 0 0 0.25rem;">{name}</p>
        <p style="font-size: 0.8rem; color: var(--text-muted); margin: 0 0 0.75rem;">{description}</p>
        <div style="display: flex; justify-content: space-between;">
            <span style="font-size: 0.75rem; color: var(--text-secondary);">{tables} tables</span>
            <span style="font-size: 0.75rem; color: var(--text-secondary);">{size}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)


def _render_activity_item(title: str, detail: str, time: str, status: str):
    """Render an activity item."""
    status_colors = {
        "success": "var(--success)",
        "warning": "var(--warning)",
        "error": "var(--error)",
        "info": "var(--accent)",
        "neutral": "var(--text-muted)",
    }
    color = status_colors.get(status, "var(--text-muted)")

    st.markdown(f"""
    <div style="display: flex; align-items: flex-start; padding: 0.75rem 0; border-bottom: 1px solid var(--border);">
        <div style="width: 6px; height: 6px; background: {color}; border-radius: 50%; margin-top: 6px; margin-right: 0.75rem; flex-shrink: 0;"></div>
        <div style="flex: 1; min-width: 0;">
            <p style="font-size: 0.875rem; color: var(--text); margin: 0; font-weight: 500;">{title}</p>
            <p style="font-size: 0.8rem; color: var(--text-muted); margin: 0;">{detail}</p>
        </div>
        <span style="font-size: 0.7rem; color: var(--text-muted); flex-shrink: 0;">{time}</span>
    </div>
    """, unsafe_allow_html=True)


def show_placeholder_page(title: str, description: str):
    """Show a placeholder page."""
    st.markdown(f"""
    <div style="margin-bottom: 2rem;">
        <h1 style="font-size: 1.625rem; font-weight: 600; color: var(--text); margin: 0 0 0.25rem; letter-spacing: -0.02em;">{title}</h1>
        <p style="font-size: 0.9rem; color: var(--text-muted); margin: 0;">{description}</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div style="text-align: center; padding: 4rem 2rem; background: var(--surface-muted); border-radius: 12px; border: 2px dashed var(--border);">
        <p style="font-size: 2.5rem; margin-bottom: 1rem; opacity: 0.5;">🚧</p>
        <h3 style="font-size: 1.125rem; font-weight: 600; color: var(--text); margin: 0 0 0.5rem;">Coming Soon</h3>
        <p style="font-size: 0.875rem; color: var(--text-muted); margin: 0;">This feature is under development</p>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    run_app()
