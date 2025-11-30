"""Centralized state management for Automic ETL UI."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar, Generic
from datetime import datetime, timedelta
from enum import Enum
import streamlit as st
import json


T = TypeVar('T')


class NotificationType(str, Enum):
    """Notification types."""
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"
    INFO = "info"


@dataclass
class Notification:
    """Notification data."""
    id: str
    type: NotificationType
    title: str
    message: str
    timestamp: datetime = field(default_factory=datetime.now)
    dismissible: bool = True
    auto_dismiss: int = 5000  # milliseconds, 0 for no auto-dismiss
    action_label: str | None = None
    action_callback: str | None = None  # page to navigate to


@dataclass
class LakehouseStats:
    """Real-time lakehouse statistics."""
    bronze_tables: int = 0
    bronze_size_gb: float = 0.0
    bronze_last_ingestion: datetime | None = None

    silver_tables: int = 0
    silver_size_gb: float = 0.0
    silver_last_transform: datetime | None = None

    gold_tables: int = 0
    gold_size_gb: float = 0.0
    gold_last_aggregation: datetime | None = None

    total_pipelines: int = 0
    running_pipelines: int = 0
    failed_pipelines: int = 0

    total_llm_queries: int = 0
    llm_queries_today: int = 0

    data_quality_score: float = 0.0

    last_updated: datetime = field(default_factory=datetime.now)


@dataclass
class SearchResult:
    """Search result item."""
    type: str  # table, pipeline, query, connector, etc.
    title: str
    subtitle: str
    page: str
    icon: str
    metadata: dict = field(default_factory=dict)


class StateManager:
    """
    Centralized state manager for the application.

    Provides:
    - Global state access
    - State persistence
    - State subscriptions
    - Computed state
    """

    _instance: 'StateManager | None' = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True
            self._subscribers: dict[str, list[Callable]] = {}

    def init_state(self):
        """Initialize all application state."""
        defaults = {
            # Navigation
            "current_page": "home",
            "page_history": [],
            "sidebar_collapsed": False,

            # Theme
            "theme_mode": "light",

            # Notifications
            "notifications": [],
            "unread_notifications": 0,

            # Lakehouse stats
            "lakehouse_stats": None,
            "stats_last_refresh": None,

            # Search
            "global_search_query": "",
            "search_results": [],
            "search_history": [],

            # User preferences
            "preferences": {
                "auto_refresh": True,
                "refresh_interval": 30,  # seconds
                "show_tips": True,
                "compact_mode": False,
                "keyboard_shortcuts": True,
            },

            # Filters and selections
            "selected_tier": "all",
            "date_range": "7d",
            "pipeline_filter": "all",

            # Modal states
            "show_search_modal": False,
            "show_notifications_panel": False,
            "show_help_modal": False,

            # Loading states
            "is_loading": False,
            "loading_message": "",
        }

        for key, value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Get state value."""
        return st.session_state.get(key, default)

    def set(self, key: str, value: Any, notify: bool = True):
        """Set state value and optionally notify subscribers."""
        old_value = st.session_state.get(key)
        st.session_state[key] = value

        if notify and key in self._subscribers:
            for callback in self._subscribers[key]:
                callback(key, old_value, value)

    def subscribe(self, key: str, callback: Callable[[str, Any, Any], None]):
        """Subscribe to state changes."""
        if key not in self._subscribers:
            self._subscribers[key] = []
        self._subscribers[key].append(callback)

    def unsubscribe(self, key: str, callback: Callable):
        """Unsubscribe from state changes."""
        if key in self._subscribers:
            self._subscribers[key].remove(callback)

    # Navigation helpers
    def navigate(self, page: str, add_to_history: bool = True):
        """Navigate to a page."""
        if add_to_history:
            history = self.get("page_history", [])
            current = self.get("current_page")
            if current and current != page:
                history.append(current)
                # Keep last 10 pages
                self.set("page_history", history[-10:], notify=False)

        self.set("current_page", page)
        st.rerun()

    def go_back(self) -> bool:
        """Go back to previous page."""
        history = self.get("page_history", [])
        if history:
            page = history.pop()
            self.set("page_history", history, notify=False)
            self.set("current_page", page)
            st.rerun()
            return True
        return False

    # Notification helpers
    def add_notification(
        self,
        title: str,
        message: str,
        type: NotificationType = NotificationType.INFO,
        auto_dismiss: int = 5000,
        action_label: str | None = None,
        action_page: str | None = None,
    ):
        """Add a notification."""
        import uuid

        notification = Notification(
            id=str(uuid.uuid4()),
            type=type,
            title=title,
            message=message,
            auto_dismiss=auto_dismiss,
            action_label=action_label,
            action_callback=action_page,
        )

        notifications = self.get("notifications", [])
        notifications.insert(0, notification)

        # Keep last 50 notifications
        self.set("notifications", notifications[:50])
        self.set("unread_notifications", self.get("unread_notifications", 0) + 1)

    def dismiss_notification(self, notification_id: str):
        """Dismiss a notification."""
        notifications = self.get("notifications", [])
        notifications = [n for n in notifications if n.id != notification_id]
        self.set("notifications", notifications)

    def clear_notifications(self):
        """Clear all notifications."""
        self.set("notifications", [])
        self.set("unread_notifications", 0)

    def mark_notifications_read(self):
        """Mark all notifications as read."""
        self.set("unread_notifications", 0)

    # Lakehouse stats helpers
    def update_lakehouse_stats(self, stats: LakehouseStats):
        """Update lakehouse statistics."""
        self.set("lakehouse_stats", stats)
        self.set("stats_last_refresh", datetime.now())

    def get_lakehouse_stats(self) -> LakehouseStats:
        """Get lakehouse statistics, with mock data if not available."""
        stats = self.get("lakehouse_stats")
        if stats is None:
            # Return mock stats for demo
            stats = LakehouseStats(
                bronze_tables=8,
                bronze_size_gb=1.2,
                bronze_last_ingestion=datetime.now() - timedelta(minutes=2),
                silver_tables=6,
                silver_size_gb=0.89,
                silver_last_transform=datetime.now() - timedelta(minutes=5),
                gold_tables=4,
                gold_size_gb=0.32,
                gold_last_aggregation=datetime.now() - timedelta(hours=1),
                total_pipelines=5,
                running_pipelines=1,
                failed_pipelines=1,
                total_llm_queries=156,
                llm_queries_today=23,
                data_quality_score=87.0,
            )
        return stats

    def should_refresh_stats(self) -> bool:
        """Check if stats should be refreshed."""
        last_refresh = self.get("stats_last_refresh")
        if last_refresh is None:
            return True

        refresh_interval = self.get("preferences", {}).get("refresh_interval", 30)
        return datetime.now() - last_refresh > timedelta(seconds=refresh_interval)

    # Search helpers
    def search(self, query: str) -> list[SearchResult]:
        """Perform global search."""
        if not query or len(query) < 2:
            return []

        query_lower = query.lower()
        results = []

        # Search tables
        tables = [
            ("bronze.raw_customers", "Customer data from CRM", "bronze"),
            ("bronze.raw_orders", "Order transactions", "bronze"),
            ("bronze.raw_products", "Product catalog", "bronze"),
            ("silver.customers", "Cleaned customer data", "silver"),
            ("silver.orders", "Validated orders", "silver"),
            ("gold.customer_360", "Customer analytics view", "gold"),
            ("gold.sales_metrics", "Sales KPIs", "gold"),
        ]

        for name, desc, tier in tables:
            if query_lower in name.lower() or query_lower in desc.lower():
                results.append(SearchResult(
                    type="table",
                    title=name,
                    subtitle=desc,
                    page="profiling",
                    icon="📊",
                    metadata={"tier": tier}
                ))

        # Search pipelines
        pipelines = [
            ("customer_processing", "Process customer data"),
            ("orders_etl", "Order ETL pipeline"),
            ("daily_aggregation", "Daily metrics aggregation"),
            ("data_quality_check", "Quality validation"),
        ]

        for name, desc in pipelines:
            if query_lower in name.lower() or query_lower in desc.lower():
                results.append(SearchResult(
                    type="pipeline",
                    title=name,
                    subtitle=desc,
                    page="pipelines",
                    icon="🔧",
                ))

        # Search connectors
        connectors = [
            ("PostgreSQL", "Database connector"),
            ("Snowflake", "Cloud data warehouse"),
            ("Salesforce", "CRM API"),
            ("AWS S3", "Cloud storage"),
        ]

        for name, desc in connectors:
            if query_lower in name.lower() or query_lower in desc.lower():
                results.append(SearchResult(
                    type="connector",
                    title=name,
                    subtitle=desc,
                    page="connectors",
                    icon="🔌",
                ))

        # Add to search history
        history = self.get("search_history", [])
        if query not in history:
            history.insert(0, query)
            self.set("search_history", history[:10])

        self.set("search_results", results)
        return results

    # Preference helpers
    def get_preference(self, key: str, default: Any = None) -> Any:
        """Get a user preference."""
        prefs = self.get("preferences", {})
        return prefs.get(key, default)

    def set_preference(self, key: str, value: Any):
        """Set a user preference."""
        prefs = self.get("preferences", {})
        prefs[key] = value
        self.set("preferences", prefs)

    # Loading state helpers
    def start_loading(self, message: str = "Loading..."):
        """Start loading state."""
        self.set("is_loading", True, notify=False)
        self.set("loading_message", message, notify=False)

    def stop_loading(self):
        """Stop loading state."""
        self.set("is_loading", False, notify=False)
        self.set("loading_message", "", notify=False)


# Global state manager instance
def get_state() -> StateManager:
    """Get the global state manager."""
    if "state_manager" not in st.session_state:
        st.session_state.state_manager = StateManager()
    return st.session_state.state_manager


# Convenience functions
def notify_success(title: str, message: str = "", auto_dismiss: int = 5000):
    """Show a success notification."""
    get_state().add_notification(title, message, NotificationType.SUCCESS, auto_dismiss)


def notify_error(title: str, message: str = "", auto_dismiss: int = 0):
    """Show an error notification."""
    get_state().add_notification(title, message, NotificationType.ERROR, auto_dismiss)


def notify_warning(title: str, message: str = "", auto_dismiss: int = 5000):
    """Show a warning notification."""
    get_state().add_notification(title, message, NotificationType.WARNING, auto_dismiss)


def notify_info(title: str, message: str = "", auto_dismiss: int = 5000):
    """Show an info notification."""
    get_state().add_notification(title, message, NotificationType.INFO, auto_dismiss)
