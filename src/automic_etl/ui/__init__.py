"""Streamlit Web UI for Automic ETL."""

from automic_etl.ui.app import create_app, run_app, navigate_to
from automic_etl.ui.theme import Theme, get_theme, ThemeMode, get_streamlit_css
from automic_etl.ui.components import (
    status_badge,
    tier_badge,
    card,
    stat_card,
    info_card,
    page_header,
    breadcrumb,
    data_table,
    key_value_list,
    search_box,
    empty_state,
    progress_steps,
    spacer,
    divider,
)
from automic_etl.ui.widgets import (
    MetricData,
    dashboard_header,
    metrics_row,
    pipeline_status_widget,
    activity_feed_widget,
    data_quality_score_widget,
    storage_breakdown_widget,
    system_health_widget,
    alerts_widget,
)

__all__ = [
    # App
    "create_app",
    "run_app",
    "navigate_to",
    # Theme
    "Theme",
    "get_theme",
    "ThemeMode",
    "get_streamlit_css",
    # Components
    "status_badge",
    "tier_badge",
    "card",
    "stat_card",
    "info_card",
    "page_header",
    "breadcrumb",
    "data_table",
    "key_value_list",
    "search_box",
    "empty_state",
    "progress_steps",
    "spacer",
    "divider",
    # Widgets
    "MetricData",
    "dashboard_header",
    "metrics_row",
    "pipeline_status_widget",
    "activity_feed_widget",
    "data_quality_score_widget",
    "storage_breakdown_widget",
    "system_health_widget",
    "alerts_widget",
]
