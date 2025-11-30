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
from automic_etl.ui.state import (
    StateManager,
    get_state,
    notify_success,
    notify_error,
    notify_warning,
    notify_info,
    NotificationType,
    LakehouseStats,
)
from automic_etl.ui.shortcuts import (
    inject_keyboard_shortcuts,
    show_shortcuts_modal,
    shortcut_hint,
)
from automic_etl.ui.notifications import (
    inject_notification_styles,
    render_toast_container,
    render_notification_bell,
    render_alert_banner,
)
from automic_etl.ui.export import (
    export_dataframe,
    export_dialog,
    export_report,
    bulk_export_button,
    copy_to_clipboard_button,
)
from automic_etl.ui.search import (
    GlobalSearch,
    get_global_search,
    render_search_bar,
    render_search_results,
    render_command_palette,
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
    # State Management
    "StateManager",
    "get_state",
    "notify_success",
    "notify_error",
    "notify_warning",
    "notify_info",
    "NotificationType",
    "LakehouseStats",
    # Shortcuts
    "inject_keyboard_shortcuts",
    "show_shortcuts_modal",
    "shortcut_hint",
    # Notifications
    "inject_notification_styles",
    "render_toast_container",
    "render_notification_bell",
    "render_alert_banner",
    # Export
    "export_dataframe",
    "export_dialog",
    "export_report",
    "bulk_export_button",
    "copy_to_clipboard_button",
    # Search
    "GlobalSearch",
    "get_global_search",
    "render_search_bar",
    "render_search_results",
    "render_command_palette",
]
