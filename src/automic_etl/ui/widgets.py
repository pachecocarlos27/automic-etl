"""Dashboard widgets for Automic ETL."""

from __future__ import annotations

from typing import Any, Callable, Literal
from datetime import datetime, timedelta
from dataclasses import dataclass
import streamlit as st


@dataclass
class MetricData:
    """Data for a metric widget."""
    label: str
    value: Any
    delta: float | None = None
    delta_suffix: str = ""
    icon: str | None = None
    trend_data: list[float] | None = None


# ============================================================================
# Overview Dashboard Widgets
# ============================================================================

def dashboard_header(
    title: str = "Dashboard",
    subtitle: str | None = None,
    last_updated: datetime | None = None,
    refresh_callback: Callable | None = None,
) -> None:
    """
    Display dashboard header with title and refresh button.

    Args:
        title: Dashboard title
        subtitle: Optional subtitle
        last_updated: Last update timestamp
        refresh_callback: Callback for refresh button
    """
    col1, col2 = st.columns([3, 1])

    with col1:
        st.markdown(f"# {title}")
        if subtitle:
            st.markdown(f"*{subtitle}*")

    with col2:
        if last_updated:
            st.caption(f"Last updated: {last_updated.strftime('%H:%M:%S')}")
        if refresh_callback:
            if st.button("🔄 Refresh", use_container_width=True):
                refresh_callback()


def metrics_row(metrics: list[MetricData], columns: int = 4) -> None:
    """
    Display a row of metric cards.

    Args:
        metrics: List of MetricData objects
        columns: Number of columns
    """
    cols = st.columns(columns)

    for i, metric in enumerate(metrics):
        with cols[i % columns]:
            _render_metric_card(metric)


def _render_metric_card(metric: MetricData) -> None:
    """Render a single metric card."""
    delta_str = None
    if metric.delta is not None:
        delta_str = f"{metric.delta:+.1f}{metric.delta_suffix}"

    container_style = """
    <div style="
        background: linear-gradient(135deg, var(--surface) 0%, var(--background) 100%);
        border: 1px solid var(--border-light);
        border-radius: var(--radius-lg);
        padding: 1rem;
        box-shadow: var(--shadow-sm);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    ">
    """
    st.markdown(container_style, unsafe_allow_html=True)

    if metric.icon:
        st.markdown(f"<span style='font-size: 1.5rem;'>{metric.icon}</span>", unsafe_allow_html=True)

    st.metric(
        label=metric.label,
        value=metric.value,
        delta=delta_str,
    )

    if metric.trend_data:
        import pandas as pd
        df = pd.DataFrame({"value": metric.trend_data})
        st.line_chart(df, height=50, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)


def quick_stats_widget(
    stats: dict[str, tuple[Any, str | None]],
    title: str = "Quick Stats",
) -> None:
    """
    Display quick statistics in a compact format.

    Args:
        stats: Dict of {label: (value, icon)} pairs
        title: Widget title
    """
    st.markdown(f"### {title}")

    for label, (value, icon) in stats.items():
        col1, col2 = st.columns([3, 1])
        with col1:
            display = f"{icon} {label}" if icon else label
            st.markdown(display)
        with col2:
            st.markdown(f"**{value}**")


# ============================================================================
# Pipeline Widgets
# ============================================================================

def pipeline_status_widget(
    pipelines: list[dict[str, Any]],
    title: str = "Pipeline Status",
    show_details: bool = True,
) -> None:
    """
    Display pipeline status overview.

    Args:
        pipelines: List of pipeline data dicts
        title: Widget title
        show_details: Whether to show detailed info
    """
    st.markdown(f"### {title}")

    if not pipelines:
        st.info("No pipelines configured")
        return

    # Count by status
    status_counts = {}
    for p in pipelines:
        status = p.get("status", "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

    # Display status summary
    cols = st.columns(4)
    status_config = [
        ("running", "🟢", "Running"),
        ("completed", "✅", "Completed"),
        ("failed", "🔴", "Failed"),
        ("pending", "🟡", "Pending"),
    ]

    for i, (status, icon, label) in enumerate(status_config):
        with cols[i]:
            count = status_counts.get(status, 0)
            st.markdown(f"""
            <div style="text-align: center; padding: 0.5rem;">
                <div style="font-size: 1.5rem;">{icon}</div>
                <div style="font-size: 1.5rem; font-weight: bold;">{count}</div>
                <div style="font-size: 0.75rem; color: var(--text-secondary);">{label}</div>
            </div>
            """, unsafe_allow_html=True)

    if show_details and pipelines:
        st.markdown("---")
        with st.expander("View All Pipelines", expanded=False):
            for p in pipelines[:10]:
                _render_pipeline_row(p)


def _render_pipeline_row(pipeline: dict[str, Any]) -> None:
    """Render a single pipeline row."""
    col1, col2, col3, col4 = st.columns([3, 1, 1, 1])

    with col1:
        st.markdown(f"**{pipeline.get('name', 'Unnamed')}**")

    with col2:
        status = pipeline.get("status", "unknown")
        status_colors = {
            "running": "var(--success)",
            "completed": "var(--info)",
            "failed": "var(--danger)",
            "pending": "var(--warning)",
        }
        color = status_colors.get(status, "var(--text-muted)")
        st.markdown(f'<span style="color: {color};">{status.title()}</span>', unsafe_allow_html=True)

    with col3:
        last_run = pipeline.get("last_run")
        if last_run:
            if isinstance(last_run, str):
                st.caption(last_run)
            else:
                st.caption(last_run.strftime("%H:%M"))
        else:
            st.caption("Never")

    with col4:
        duration = pipeline.get("duration")
        if duration:
            st.caption(f"{duration}s")


def pipeline_run_timeline(
    runs: list[dict[str, Any]],
    title: str = "Recent Runs",
    max_items: int = 10,
) -> None:
    """
    Display pipeline run timeline.

    Args:
        runs: List of run data
        title: Widget title
        max_items: Maximum items to show
    """
    st.markdown(f"### {title}")

    if not runs:
        st.info("No recent runs")
        return

    for run in runs[:max_items]:
        status = run.get("status", "unknown")
        status_icons = {
            "success": "✅",
            "failed": "❌",
            "running": "🔄",
            "pending": "⏳",
        }
        icon = status_icons.get(status, "❓")

        timestamp = run.get("timestamp", "")
        if isinstance(timestamp, datetime):
            timestamp = timestamp.strftime("%Y-%m-%d %H:%M")

        st.markdown(f"""
        <div style="
            display: flex;
            align-items: center;
            gap: 0.75rem;
            padding: 0.5rem;
            border-left: 3px solid {'var(--success)' if status == 'success' else 'var(--danger)' if status == 'failed' else 'var(--warning)'};
            margin-bottom: 0.5rem;
            background: var(--surface);
            border-radius: 0 var(--radius-md) var(--radius-md) 0;
        ">
            <span style="font-size: 1.25rem;">{icon}</span>
            <div style="flex: 1;">
                <div style="font-weight: 500;">{run.get('pipeline_name', 'Pipeline')}</div>
                <div style="font-size: 0.75rem; color: var(--text-secondary);">{timestamp}</div>
            </div>
            <div style="font-size: 0.875rem; color: var(--text-muted);">
                {run.get('duration', '')}
            </div>
        </div>
        """, unsafe_allow_html=True)


# ============================================================================
# Data Quality Widgets
# ============================================================================

def data_quality_score_widget(
    score: float,
    title: str = "Data Quality Score",
    breakdown: dict[str, float] | None = None,
) -> None:
    """
    Display overall data quality score.

    Args:
        score: Quality score (0-100)
        title: Widget title
        breakdown: Optional score breakdown by category
    """
    st.markdown(f"### {title}")

    # Determine color based on score
    if score >= 90:
        color = "var(--success)"
        label = "Excellent"
    elif score >= 70:
        color = "var(--warning)"
        label = "Good"
    elif score >= 50:
        color = "var(--warning)"
        label = "Fair"
    else:
        color = "var(--danger)"
        label = "Poor"

    st.markdown(f"""
    <div style="text-align: center; padding: 1rem;">
        <div style="
            width: 120px;
            height: 120px;
            border-radius: 50%;
            background: conic-gradient({color} {score * 3.6}deg, var(--surface) 0deg);
            display: inline-flex;
            align-items: center;
            justify-content: center;
            margin-bottom: 0.5rem;
        ">
            <div style="
                width: 100px;
                height: 100px;
                border-radius: 50%;
                background: var(--background);
                display: flex;
                align-items: center;
                justify-content: center;
                flex-direction: column;
            ">
                <span style="font-size: 1.75rem; font-weight: bold; color: {color};">{score:.0f}</span>
                <span style="font-size: 0.75rem; color: var(--text-muted);">/ 100</span>
            </div>
        </div>
        <div style="font-weight: 600; color: {color};">{label}</div>
    </div>
    """, unsafe_allow_html=True)

    if breakdown:
        st.markdown("---")
        for category, cat_score in breakdown.items():
            col1, col2 = st.columns([3, 1])
            with col1:
                st.progress(cat_score / 100)
            with col2:
                st.markdown(f"**{cat_score:.0f}%**")
            st.caption(category)


def data_freshness_widget(
    tables: list[dict[str, Any]],
    title: str = "Data Freshness",
) -> None:
    """
    Display data freshness overview.

    Args:
        tables: List of table info dicts with last_updated timestamps
        title: Widget title
    """
    st.markdown(f"### {title}")

    if not tables:
        st.info("No tables to display")
        return

    now = datetime.utcnow()

    for table in tables:
        name = table.get("name", "Unknown")
        last_updated = table.get("last_updated")

        if last_updated:
            if isinstance(last_updated, str):
                last_updated = datetime.fromisoformat(last_updated.replace("Z", "+00:00"))

            age = now - last_updated.replace(tzinfo=None)

            if age < timedelta(hours=1):
                freshness = "Fresh"
                color = "var(--success)"
                icon = "🟢"
            elif age < timedelta(hours=24):
                freshness = "Recent"
                color = "var(--warning)"
                icon = "🟡"
            else:
                freshness = "Stale"
                color = "var(--danger)"
                icon = "🔴"

            age_str = _format_timedelta(age)
        else:
            freshness = "Unknown"
            color = "var(--text-muted)"
            icon = "⚪"
            age_str = "N/A"

        col1, col2, col3 = st.columns([3, 1, 1])
        with col1:
            st.markdown(f"**{name}**")
        with col2:
            st.markdown(f"{icon} {freshness}")
        with col3:
            st.caption(age_str)


def _format_timedelta(td: timedelta) -> str:
    """Format timedelta to human readable string."""
    total_seconds = int(td.total_seconds())

    if total_seconds < 60:
        return f"{total_seconds}s ago"
    elif total_seconds < 3600:
        return f"{total_seconds // 60}m ago"
    elif total_seconds < 86400:
        return f"{total_seconds // 3600}h ago"
    else:
        return f"{total_seconds // 86400}d ago"


# ============================================================================
# Activity Widgets
# ============================================================================

def activity_feed_widget(
    activities: list[dict[str, Any]],
    title: str = "Recent Activity",
    max_items: int = 10,
) -> None:
    """
    Display activity feed.

    Args:
        activities: List of activity dicts
        title: Widget title
        max_items: Maximum items to show
    """
    st.markdown(f"### {title}")

    if not activities:
        st.info("No recent activity")
        return

    for activity in activities[:max_items]:
        action = activity.get("action", "unknown")
        user = activity.get("user", "System")
        timestamp = activity.get("timestamp", "")
        details = activity.get("details", "")

        action_icons = {
            "create": "➕",
            "update": "✏️",
            "delete": "🗑️",
            "run": "▶️",
            "complete": "✅",
            "fail": "❌",
            "login": "🔐",
            "logout": "🚪",
        }
        icon = action_icons.get(action, "📝")

        if isinstance(timestamp, datetime):
            timestamp = timestamp.strftime("%H:%M")

        st.markdown(f"""
        <div style="
            display: flex;
            gap: 0.75rem;
            padding: 0.5rem 0;
            border-bottom: 1px solid var(--border-light);
        ">
            <span style="font-size: 1.25rem;">{icon}</span>
            <div style="flex: 1;">
                <div style="font-weight: 500;">{details or action.title()}</div>
                <div style="font-size: 0.75rem; color: var(--text-secondary);">
                    by {user} • {timestamp}
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)


def user_activity_chart(
    data: dict[str, int],
    title: str = "User Activity",
    chart_type: Literal["bar", "line"] = "bar",
) -> None:
    """
    Display user activity chart.

    Args:
        data: Dict of {date: count} pairs
        title: Widget title
        chart_type: Type of chart
    """
    import pandas as pd

    st.markdown(f"### {title}")

    if not data:
        st.info("No activity data")
        return

    df = pd.DataFrame(list(data.items()), columns=["Date", "Activity"])
    df = df.set_index("Date")

    if chart_type == "bar":
        st.bar_chart(df)
    else:
        st.line_chart(df)


# ============================================================================
# Resource Widgets
# ============================================================================

def resource_usage_widget(
    resources: dict[str, tuple[float, float]],
    title: str = "Resource Usage",
) -> None:
    """
    Display resource usage meters.

    Args:
        resources: Dict of {name: (used, total)} pairs
        title: Widget title
    """
    st.markdown(f"### {title}")

    for name, (used, total) in resources.items():
        pct = (used / total * 100) if total > 0 else 0

        if pct >= 90:
            color = "var(--danger)"
        elif pct >= 70:
            color = "var(--warning)"
        else:
            color = "var(--success)"

        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown(f"**{name}**")
            st.progress(pct / 100)
        with col2:
            st.markdown(f"""
            <div style="text-align: right; color: {color}; font-weight: 600;">
                {pct:.1f}%
            </div>
            <div style="text-align: right; font-size: 0.75rem; color: var(--text-muted);">
                {used:.1f} / {total:.1f}
            </div>
            """, unsafe_allow_html=True)


def storage_breakdown_widget(
    breakdown: dict[str, float],
    title: str = "Storage Breakdown",
    total: float | None = None,
) -> None:
    """
    Display storage breakdown by tier/category.

    Args:
        breakdown: Dict of {category: size_gb} pairs
        title: Widget title
        total: Optional total storage
    """
    st.markdown(f"### {title}")

    if not breakdown:
        st.info("No storage data")
        return

    total_used = sum(breakdown.values())
    if total is None:
        total = total_used

    # Tier colors
    tier_colors = {
        "bronze": "#cd7f32",
        "silver": "#c0c0c0",
        "gold": "#ffd700",
    }

    for category, size in breakdown.items():
        pct = (size / total * 100) if total > 0 else 0
        color = tier_colors.get(category.lower(), "var(--primary)")

        st.markdown(f"""
        <div style="margin-bottom: 0.75rem;">
            <div style="display: flex; justify-content: space-between; margin-bottom: 0.25rem;">
                <span style="font-weight: 500; text-transform: capitalize;">{category}</span>
                <span style="color: var(--text-secondary);">{size:.2f} GB ({pct:.1f}%)</span>
            </div>
            <div style="
                height: 8px;
                background: var(--surface);
                border-radius: var(--radius-full);
                overflow: hidden;
            ">
                <div style="
                    width: {pct}%;
                    height: 100%;
                    background: {color};
                    border-radius: var(--radius-full);
                "></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown(f"""
    <div style="
        margin-top: 1rem;
        padding-top: 0.75rem;
        border-top: 1px solid var(--border-light);
        display: flex;
        justify-content: space-between;
        font-weight: 600;
    ">
        <span>Total</span>
        <span>{total_used:.2f} GB / {total:.2f} GB</span>
    </div>
    """, unsafe_allow_html=True)


# ============================================================================
# Alert Widgets
# ============================================================================

def alerts_widget(
    alerts: list[dict[str, Any]],
    title: str = "Active Alerts",
    on_dismiss: Callable[[str], None] | None = None,
) -> None:
    """
    Display active alerts.

    Args:
        alerts: List of alert dicts
        title: Widget title
        on_dismiss: Optional callback to dismiss an alert
    """
    st.markdown(f"### {title}")

    if not alerts:
        st.success("No active alerts")
        return

    for alert in alerts:
        severity = alert.get("severity", "info")
        message = alert.get("message", "")
        timestamp = alert.get("timestamp", "")
        alert_id = alert.get("id", "")

        severity_config = {
            "critical": ("🔴", "var(--danger)", "var(--danger-light)"),
            "warning": ("🟡", "var(--warning)", "var(--warning-light)"),
            "info": ("🔵", "var(--info)", "var(--info-light)"),
        }
        icon, border_color, bg_color = severity_config.get(severity, severity_config["info"])

        if isinstance(timestamp, datetime):
            timestamp = timestamp.strftime("%Y-%m-%d %H:%M")

        col1, col2 = st.columns([6, 1])

        with col1:
            st.markdown(f"""
            <div style="
                background: {bg_color};
                border-left: 4px solid {border_color};
                padding: 0.75rem;
                border-radius: 0 var(--radius-md) var(--radius-md) 0;
                margin-bottom: 0.5rem;
            ">
                <div style="display: flex; align-items: center; gap: 0.5rem;">
                    <span>{icon}</span>
                    <span style="font-weight: 500;">{message}</span>
                </div>
                <div style="font-size: 0.75rem; color: var(--text-secondary); margin-top: 0.25rem;">
                    {timestamp}
                </div>
            </div>
            """, unsafe_allow_html=True)

        with col2:
            if on_dismiss:
                if st.button("✕", key=f"dismiss_{alert_id}"):
                    on_dismiss(alert_id)


def system_health_widget(
    services: dict[str, str],
    title: str = "System Health",
) -> None:
    """
    Display system health status.

    Args:
        services: Dict of {service_name: status} pairs
        title: Widget title
    """
    st.markdown(f"### {title}")

    status_icons = {
        "healthy": ("🟢", "Healthy"),
        "degraded": ("🟡", "Degraded"),
        "unhealthy": ("🔴", "Unhealthy"),
        "unknown": ("⚪", "Unknown"),
    }

    for service, status in services.items():
        icon, label = status_icons.get(status.lower(), status_icons["unknown"])

        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown(f"**{service}**")
        with col2:
            st.markdown(f"{icon} {label}")
