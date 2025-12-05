"""Dashboard widgets for Automic ETL - Sleek minimal design."""

from __future__ import annotations

from typing import Any, Callable, Literal
from datetime import timedelta
from dataclasses import dataclass
import streamlit as st

from automic_etl.core.utils import utc_now


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
    """Display minimal dashboard header."""
    st.markdown(f"""
    <div style="
        display: flex;
        align-items: flex-end;
        justify-content: space-between;
        padding-bottom: 1.5rem;
        margin-bottom: 1.5rem;
        border-bottom: 1px solid #E2E8F0;
    ">
        <div>
            <h1 style="
                font-size: 1.75rem;
                font-weight: 600;
                color: #0F172A;
                margin: 0;
                letter-spacing: -0.025em;
            ">{title}</h1>
            {f'<p style="color: #64748B; font-size: 0.875rem; margin: 0.375rem 0 0 0;">{subtitle}</p>' if subtitle else ''}
        </div>
        <div style="display: flex; align-items: center; gap: 1rem;">
            {f'<span style="color: #94A3B8; font-size: 0.75rem;">Updated {last_updated.strftime("%H:%M")}</span>' if last_updated else ''}
        </div>
    </div>
    """, unsafe_allow_html=True)

    if refresh_callback:
        col1, col2, col3 = st.columns([8, 1, 1])
        with col3:
            if st.button("↻", help="Refresh"):
                refresh_callback()


def metrics_row(metrics: list[MetricData], columns: int = 4) -> None:
    """Display a row of sleek metric cards."""
    cols = st.columns(columns)

    for i, metric in enumerate(metrics):
        with cols[i % columns]:
            _render_metric_card(metric)


def _render_metric_card(metric: MetricData) -> None:
    """Render a minimal metric card."""
    # Determine delta display
    delta_html = ""
    if metric.delta is not None:
        delta_color = "#10B981" if metric.delta >= 0 else "#EF4444"
        delta_icon = "↑" if metric.delta >= 0 else "↓"
        delta_html = f"""
        <div style="
            display: inline-flex;
            align-items: center;
            gap: 0.25rem;
            padding: 0.125rem 0.5rem;
            background: {'#ECFDF5' if metric.delta >= 0 else '#FEF2F2'};
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 500;
            color: {delta_color};
        ">
            <span>{delta_icon}</span>
            <span>{abs(metric.delta):.1f}{metric.delta_suffix}</span>
        </div>
        """

    icon_html = ""
    if metric.icon:
        icon_html = f"""
        <div style="
            width: 36px;
            height: 36px;
            background: #F1F5F9;
            border-radius: 8px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1rem;
            margin-bottom: 0.75rem;
        ">{metric.icon}</div>
        """

    st.markdown(f"""
    <div style="
        background: white;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 1.25rem;
        transition: all 0.2s ease;
    ">
        {icon_html}
        <div style="color: #64748B; font-size: 0.8125rem; font-weight: 500; margin-bottom: 0.375rem;">
            {metric.label}
        </div>
        <div style="
            font-size: 1.75rem;
            font-weight: 600;
            color: #0F172A;
            letter-spacing: -0.025em;
            margin-bottom: 0.5rem;
        ">{metric.value}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)

    if metric.trend_data:
        import pandas as pd
        df = pd.DataFrame({"value": metric.trend_data})
        st.line_chart(df, height=40, use_container_width=True)


def quick_stats_widget(
    stats: dict[str, tuple[Any, str | None]],
    title: str = "Quick Stats",
) -> None:
    """Display quick stats in a minimal card."""
    st.markdown(f"""
    <div style="
        background: white;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        overflow: hidden;
    ">
        <div style="
            padding: 1rem 1.25rem;
            border-bottom: 1px solid #E2E8F0;
            font-weight: 600;
            color: #0F172A;
            font-size: 0.9375rem;
        ">{title}</div>
    """, unsafe_allow_html=True)

    items_html = ""
    for i, (label, (value, icon)) in enumerate(stats.items()):
        border = "" if i == len(stats) - 1 else "border-bottom: 1px solid #F1F5F9;"
        icon_html = f'<span style="margin-right: 0.5rem;">{icon}</span>' if icon else ""
        items_html += f"""
        <div style="
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 0.875rem 1.25rem;
            {border}
        ">
            <span style="color: #64748B; font-size: 0.875rem;">{icon_html}{label}</span>
            <span style="font-weight: 600; color: #0F172A;">{value}</span>
        </div>
        """

    st.markdown(f"""
        {items_html}
    </div>
    """, unsafe_allow_html=True)


# ============================================================================
# Pipeline Widgets
# ============================================================================

def pipeline_status_widget(
    pipelines: list[dict[str, Any]],
    title: str = "Pipeline Status",
    show_details: bool = True,
) -> None:
    """Display sleek pipeline status overview."""
    st.markdown(f"""
    <div style="margin-bottom: 1.25rem;">
        <h3 style="
            font-size: 1rem;
            font-weight: 600;
            color: #0F172A;
            margin: 0;
        ">{title}</h3>
    </div>
    """, unsafe_allow_html=True)

    if not pipelines:
        _render_empty_state("◇", "No pipelines", "Create your first pipeline to get started")
        return

    status_counts = {}
    for p in pipelines:
        status = p.get("status", "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

    status_config = [
        ("running", "●", "#10B981", "#ECFDF5", "Running"),
        ("completed", "✓", "#6366F1", "#EEF2FF", "Done"),
        ("failed", "✕", "#EF4444", "#FEF2F2", "Failed"),
        ("pending", "○", "#F59E0B", "#FFFBEB", "Pending"),
    ]

    cols = st.columns(4)
    for i, (status, icon, color, bg, label) in enumerate(status_config):
        with cols[i]:
            count = status_counts.get(status, 0)
            st.markdown(f"""
            <div style="
                background: {bg};
                border-radius: 10px;
                padding: 1rem;
                text-align: center;
            ">
                <div style="
                    font-size: 1.5rem;
                    font-weight: 700;
                    color: {color};
                    letter-spacing: -0.025em;
                ">{count}</div>
                <div style="
                    font-size: 0.75rem;
                    color: {color};
                    font-weight: 500;
                    margin-top: 0.25rem;
                ">{label}</div>
            </div>
            """, unsafe_allow_html=True)

    if show_details and pipelines:
        st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)
        with st.expander("View pipelines", expanded=False):
            for p in pipelines[:10]:
                _render_pipeline_row(p)


def _render_pipeline_row(pipeline: dict[str, Any]) -> None:
    """Render a minimal pipeline row."""
    status = pipeline.get("status", "unknown")
    status_config = {
        "running": ("●", "#10B981"),
        "completed": ("✓", "#6366F1"),
        "failed": ("✕", "#EF4444"),
        "pending": ("○", "#F59E0B"),
    }
    icon, color = status_config.get(status, ("?", "#94A3B8"))

    last_run = pipeline.get("last_run")
    time_str = "Never"
    if last_run:
        if isinstance(last_run, str):
            time_str = last_run
        else:
            time_str = last_run.strftime("%H:%M")

    duration = pipeline.get("duration", "")
    duration_str = f"{duration}s" if duration else ""

    st.markdown(f"""
    <div style="
        display: flex;
        align-items: center;
        padding: 0.75rem 0;
        border-bottom: 1px solid #F1F5F9;
    ">
        <span style="color: {color}; margin-right: 0.75rem;">{icon}</span>
        <span style="flex: 1; font-weight: 500; color: #0F172A; font-size: 0.875rem;">
            {pipeline.get('name', 'Unnamed')}
        </span>
        <span style="color: #94A3B8; font-size: 0.75rem; margin-right: 1rem;">{time_str}</span>
        <span style="color: #64748B; font-size: 0.75rem; font-weight: 500;">{duration_str}</span>
    </div>
    """, unsafe_allow_html=True)


def pipeline_run_timeline(
    runs: list[dict[str, Any]],
    title: str = "Recent Runs",
    max_items: int = 10,
) -> None:
    """Display minimal pipeline run timeline."""
    st.markdown(f"""
    <div style="margin-bottom: 1rem;">
        <h3 style="
            font-size: 1rem;
            font-weight: 600;
            color: #0F172A;
            margin: 0;
        ">{title}</h3>
    </div>
    """, unsafe_allow_html=True)

    if not runs:
        st.markdown("""
        <div style="color: #94A3B8; font-size: 0.875rem; padding: 1rem 0;">
            No recent runs
        </div>
        """, unsafe_allow_html=True)
        return

    for run in runs[:max_items]:
        status = run.get("status", "unknown")
        status_config = {
            "success": ("#10B981", "#ECFDF5"),
            "failed": ("#EF4444", "#FEF2F2"),
            "running": ("#6366F1", "#EEF2FF"),
            "pending": ("#F59E0B", "#FFFBEB"),
        }
        color, bg = status_config.get(status, ("#94A3B8", "#F8FAFC"))

        timestamp = run.get("timestamp", "")
        if isinstance(timestamp, datetime):
            timestamp = timestamp.strftime("%m/%d %H:%M")

        st.markdown(f"""
        <div style="
            display: flex;
            align-items: center;
            gap: 0.75rem;
            padding: 0.75rem 1rem;
            background: {bg};
            border-radius: 8px;
            margin-bottom: 0.5rem;
        ">
            <div style="
                width: 8px;
                height: 8px;
                background: {color};
                border-radius: 50%;
            "></div>
            <div style="flex: 1;">
                <div style="font-weight: 500; color: #0F172A; font-size: 0.875rem;">
                    {run.get('pipeline_name', 'Pipeline')}
                </div>
            </div>
            <div style="font-size: 0.75rem; color: #64748B;">
                {timestamp}
            </div>
            <div style="font-size: 0.75rem; color: #94A3B8;">
                {run.get('duration', '')}
            </div>
        </div>
        """, unsafe_allow_html=True)


# ============================================================================
# Data Quality Widgets
# ============================================================================

def data_quality_score_widget(
    score: float,
    title: str = "Data Quality",
    breakdown: dict[str, float] | None = None,
) -> None:
    """Display elegant data quality score."""
    # Determine quality level
    if score >= 90:
        color = "#10B981"
        label = "Excellent"
        bg = "#ECFDF5"
    elif score >= 70:
        color = "#6366F1"
        label = "Good"
        bg = "#EEF2FF"
    elif score >= 50:
        color = "#F59E0B"
        label = "Fair"
        bg = "#FFFBEB"
    else:
        color = "#EF4444"
        label = "Poor"
        bg = "#FEF2F2"

    st.markdown(f"""
    <div style="
        background: white;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 1.5rem;
    ">
        <div style="
            font-size: 0.875rem;
            font-weight: 600;
            color: #0F172A;
            margin-bottom: 1.25rem;
        ">{title}</div>

        <div style="text-align: center; margin-bottom: 1.25rem;">
            <div style="
                position: relative;
                width: 120px;
                height: 120px;
                margin: 0 auto;
            ">
                <svg width="120" height="120" viewBox="0 0 120 120">
                    <circle cx="60" cy="60" r="54" fill="none" stroke="#F1F5F9" stroke-width="8"/>
                    <circle cx="60" cy="60" r="54" fill="none" stroke="{color}" stroke-width="8"
                        stroke-dasharray="{score * 3.39} 339"
                        stroke-linecap="round"
                        transform="rotate(-90 60 60)"/>
                </svg>
                <div style="
                    position: absolute;
                    top: 50%;
                    left: 50%;
                    transform: translate(-50%, -50%);
                    text-align: center;
                ">
                    <div style="font-size: 1.75rem; font-weight: 700; color: {color}; letter-spacing: -0.025em;">
                        {score:.0f}
                    </div>
                    <div style="font-size: 0.6875rem; color: #94A3B8;">/ 100</div>
                </div>
            </div>
            <div style="
                display: inline-block;
                padding: 0.25rem 0.75rem;
                background: {bg};
                color: {color};
                border-radius: 9999px;
                font-size: 0.75rem;
                font-weight: 500;
                margin-top: 0.75rem;
            ">{label}</div>
        </div>
    """, unsafe_allow_html=True)

    if breakdown:
        breakdown_html = ""
        for category, cat_score in breakdown.items():
            cat_color = "#10B981" if cat_score >= 85 else "#F59E0B" if cat_score >= 70 else "#EF4444"
            breakdown_html += f"""
            <div style="margin-bottom: 0.75rem;">
                <div style="display: flex; justify-content: space-between; margin-bottom: 0.375rem;">
                    <span style="font-size: 0.8125rem; color: #64748B;">{category}</span>
                    <span style="font-size: 0.8125rem; font-weight: 600; color: {cat_color};">{cat_score:.0f}%</span>
                </div>
                <div style="height: 6px; background: #F1F5F9; border-radius: 3px; overflow: hidden;">
                    <div style="width: {cat_score}%; height: 100%; background: {cat_color}; border-radius: 3px;"></div>
                </div>
            </div>
            """

        st.markdown(f"""
        <div style="border-top: 1px solid #E2E8F0; padding-top: 1rem; margin-top: 0.5rem;">
            {breakdown_html}
        </div>
        """, unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)


def data_freshness_widget(
    tables: list[dict[str, Any]],
    title: str = "Data Freshness",
) -> None:
    """Display minimal data freshness overview."""
    st.markdown(f"""
    <div style="
        background: white;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        overflow: hidden;
    ">
        <div style="
            padding: 1rem 1.25rem;
            border-bottom: 1px solid #E2E8F0;
            font-weight: 600;
            color: #0F172A;
            font-size: 0.875rem;
        ">{title}</div>
    """, unsafe_allow_html=True)

    if not tables:
        st.markdown("""
        <div style="padding: 2rem; text-align: center; color: #94A3B8; font-size: 0.875rem;">
            No tables to display
        </div>
        """, unsafe_allow_html=True)
    else:
        now = utc_now()
        items_html = ""

        for i, table in enumerate(tables):
            name = table.get("name", "Unknown")
            last_updated = table.get("last_updated")

            if last_updated:
                if isinstance(last_updated, str):
                    try:
                        last_updated = datetime.fromisoformat(last_updated.replace("Z", "+00:00"))
                    except:
                        last_updated = None

                if last_updated:
                    age = now - last_updated.replace(tzinfo=None)

                    if age < timedelta(hours=1):
                        color = "#10B981"
                        icon = "●"
                    elif age < timedelta(hours=24):
                        color = "#F59E0B"
                        icon = "●"
                    else:
                        color = "#EF4444"
                        icon = "●"

                    age_str = _format_timedelta(age)
                else:
                    color = "#94A3B8"
                    icon = "○"
                    age_str = "N/A"
            else:
                color = "#94A3B8"
                icon = "○"
                age_str = "N/A"

            border = "" if i == len(tables) - 1 else "border-bottom: 1px solid #F1F5F9;"
            items_html += f"""
            <div style="
                display: flex;
                align-items: center;
                padding: 0.75rem 1.25rem;
                {border}
            ">
                <span style="color: {color}; margin-right: 0.75rem; font-size: 0.625rem;">{icon}</span>
                <span style="flex: 1; font-size: 0.875rem; color: #0F172A;">{name}</span>
                <span style="font-size: 0.75rem; color: #94A3B8;">{age_str}</span>
            </div>
            """

        st.markdown(items_html, unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)


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
    """Display minimal activity feed."""
    st.markdown(f"""
    <div style="
        background: white;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        overflow: hidden;
    ">
        <div style="
            padding: 1rem 1.25rem;
            border-bottom: 1px solid #E2E8F0;
            font-weight: 600;
            color: #0F172A;
            font-size: 0.875rem;
        ">{title}</div>
    """, unsafe_allow_html=True)

    if not activities:
        st.markdown("""
        <div style="padding: 2rem; text-align: center;">
            <div style="color: #94A3B8; font-size: 1.5rem; margin-bottom: 0.5rem;">◇</div>
            <div style="color: #64748B; font-size: 0.875rem;">No recent activity</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        action_config = {
            "create": ("↑", "#10B981"),
            "update": ("◐", "#6366F1"),
            "delete": ("−", "#EF4444"),
            "run": ("▷", "#6366F1"),
            "complete": ("✓", "#10B981"),
            "fail": ("✕", "#EF4444"),
            "login": ("→", "#64748B"),
            "logout": ("←", "#64748B"),
        }

        items_html = ""
        for i, activity in enumerate(activities[:max_items]):
            action = activity.get("action", "unknown")
            user = activity.get("user", "System")
            timestamp = activity.get("timestamp", "")
            details = activity.get("details", "")

            icon, color = action_config.get(action, ("•", "#94A3B8"))

            if isinstance(timestamp, datetime):
                timestamp = timestamp.strftime("%H:%M")

            border = "" if i == len(activities[:max_items]) - 1 else "border-bottom: 1px solid #F1F5F9;"

            items_html += f"""
            <div style="
                display: flex;
                align-items: center;
                gap: 0.875rem;
                padding: 0.875rem 1.25rem;
                {border}
            ">
                <div style="
                    width: 28px;
                    height: 28px;
                    background: #F8FAFC;
                    border-radius: 6px;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    color: {color};
                    font-size: 0.875rem;
                ">{icon}</div>
                <div style="flex: 1; min-width: 0;">
                    <div style="font-size: 0.875rem; color: #0F172A; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">
                        {details or action.title()}
                    </div>
                    <div style="font-size: 0.75rem; color: #94A3B8; margin-top: 0.125rem;">
                        {user} · {timestamp}
                    </div>
                </div>
            </div>
            """

        st.markdown(items_html, unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)


def user_activity_chart(
    data: dict[str, int],
    title: str = "User Activity",
    chart_type: Literal["bar", "line"] = "bar",
) -> None:
    """Display user activity chart."""
    import pandas as pd

    st.markdown(f"""
    <div style="margin-bottom: 1rem;">
        <h3 style="
            font-size: 0.875rem;
            font-weight: 600;
            color: #0F172A;
            margin: 0;
        ">{title}</h3>
    </div>
    """, unsafe_allow_html=True)

    if not data:
        st.markdown("""
        <div style="color: #94A3B8; font-size: 0.875rem; padding: 1rem 0;">
            No activity data
        </div>
        """, unsafe_allow_html=True)
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
    """Display elegant resource usage meters."""
    st.markdown(f"""
    <div style="
        background: white;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 1.25rem;
    ">
        <div style="
            font-size: 0.875rem;
            font-weight: 600;
            color: #0F172A;
            margin-bottom: 1.25rem;
        ">{title}</div>
    """, unsafe_allow_html=True)

    items_html = ""
    for name, (used, total) in resources.items():
        pct = (used / total * 100) if total > 0 else 0

        if pct >= 90:
            color = "#EF4444"
        elif pct >= 70:
            color = "#F59E0B"
        else:
            color = "#10B981"

        items_html += f"""
        <div style="margin-bottom: 1rem;">
            <div style="display: flex; justify-content: space-between; margin-bottom: 0.375rem;">
                <span style="font-size: 0.8125rem; color: #64748B;">{name}</span>
                <span style="font-size: 0.8125rem; font-weight: 600; color: {color};">{pct:.0f}%</span>
            </div>
            <div style="height: 6px; background: #F1F5F9; border-radius: 3px; overflow: hidden;">
                <div style="width: {pct}%; height: 100%; background: {color}; border-radius: 3px;"></div>
            </div>
            <div style="font-size: 0.6875rem; color: #94A3B8; margin-top: 0.25rem;">
                {used:.1f} / {total:.1f}
            </div>
        </div>
        """

    st.markdown(f"{items_html}</div>", unsafe_allow_html=True)


def storage_breakdown_widget(
    breakdown: dict[str, float],
    title: str = "Storage",
    total: float | None = None,
) -> None:
    """Display minimal storage breakdown."""
    if not breakdown:
        return

    total_used = sum(breakdown.values())
    if total is None:
        total = total_used

    tier_colors = {
        "bronze": "#A16207",
        "silver": "#6B7280",
        "gold": "#CA8A04",
    }

    st.markdown(f"""
    <div style="
        background: white;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 1.25rem;
    ">
        <div style="
            font-size: 0.875rem;
            font-weight: 600;
            color: #0F172A;
            margin-bottom: 1.25rem;
        ">{title}</div>
    """, unsafe_allow_html=True)

    items_html = ""
    for category, size in breakdown.items():
        pct = (size / total * 100) if total > 0 else 0
        color = tier_colors.get(category.lower(), "#6366F1")

        items_html += f"""
        <div style="margin-bottom: 0.875rem;">
            <div style="display: flex; justify-content: space-between; margin-bottom: 0.375rem;">
                <span style="
                    font-size: 0.8125rem;
                    color: #0F172A;
                    font-weight: 500;
                    text-transform: capitalize;
                ">{category}</span>
                <span style="font-size: 0.75rem; color: #64748B;">{size:.2f} GB</span>
            </div>
            <div style="height: 8px; background: #F1F5F9; border-radius: 4px; overflow: hidden;">
                <div style="width: {pct}%; height: 100%; background: {color}; border-radius: 4px;"></div>
            </div>
        </div>
        """

    st.markdown(f"""
        {items_html}
        <div style="
            border-top: 1px solid #E2E8F0;
            padding-top: 0.75rem;
            margin-top: 0.5rem;
            display: flex;
            justify-content: space-between;
            font-size: 0.8125rem;
        ">
            <span style="color: #64748B;">Total</span>
            <span style="font-weight: 600; color: #0F172A;">{total_used:.2f} / {total:.2f} GB</span>
        </div>
    </div>
    """, unsafe_allow_html=True)


# ============================================================================
# Alert Widgets
# ============================================================================

def alerts_widget(
    alerts: list[dict[str, Any]],
    title: str = "Alerts",
    on_dismiss: Callable[[str], None] | None = None,
) -> None:
    """Display minimal alerts."""
    st.markdown(f"""
    <div style="margin-bottom: 1rem;">
        <h3 style="
            font-size: 0.875rem;
            font-weight: 600;
            color: #0F172A;
            margin: 0;
        ">{title}</h3>
    </div>
    """, unsafe_allow_html=True)

    if not alerts:
        st.markdown("""
        <div style="
            padding: 1rem;
            background: #ECFDF5;
            border-radius: 8px;
            color: #10B981;
            font-size: 0.875rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        ">
            <span>✓</span>
            <span>All systems operational</span>
        </div>
        """, unsafe_allow_html=True)
        return

    for alert in alerts:
        severity = alert.get("severity", "info")
        message = alert.get("message", "")
        timestamp = alert.get("timestamp", "")
        alert_id = alert.get("id", "")

        severity_config = {
            "critical": ("#EF4444", "#FEF2F2", "!"),
            "warning": ("#F59E0B", "#FFFBEB", "⚠"),
            "info": ("#3B82F6", "#EFF6FF", "i"),
        }
        color, bg, icon = severity_config.get(severity, severity_config["info"])

        if isinstance(timestamp, datetime):
            timestamp = timestamp.strftime("%H:%M")

        col1, col2 = st.columns([10, 1])

        with col1:
            st.markdown(f"""
            <div style="
                background: {bg};
                border-left: 3px solid {color};
                padding: 0.75rem 1rem;
                border-radius: 0 8px 8px 0;
                margin-bottom: 0.5rem;
            ">
                <div style="display: flex; align-items: center; gap: 0.5rem;">
                    <span style="color: {color}; font-weight: 600;">{icon}</span>
                    <span style="color: #0F172A; font-size: 0.875rem;">{message}</span>
                </div>
                <div style="font-size: 0.75rem; color: #94A3B8; margin-top: 0.25rem; margin-left: 1.25rem;">
                    {timestamp}
                </div>
            </div>
            """, unsafe_allow_html=True)

        with col2:
            if on_dismiss:
                if st.button("✕", key=f"dismiss_{alert_id}", help="Dismiss"):
                    on_dismiss(alert_id)


def system_health_widget(
    services: dict[str, str],
    title: str = "System Health",
) -> None:
    """Display minimal system health status."""
    st.markdown(f"""
    <div style="
        background: white;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        overflow: hidden;
    ">
        <div style="
            padding: 1rem 1.25rem;
            border-bottom: 1px solid #E2E8F0;
            font-weight: 600;
            color: #0F172A;
            font-size: 0.875rem;
        ">{title}</div>
    """, unsafe_allow_html=True)

    status_config = {
        "healthy": ("●", "#10B981"),
        "degraded": ("●", "#F59E0B"),
        "unhealthy": ("●", "#EF4444"),
        "unknown": ("○", "#94A3B8"),
    }

    items_html = ""
    for i, (service, status) in enumerate(services.items()):
        icon, color = status_config.get(status.lower(), status_config["unknown"])
        border = "" if i == len(services) - 1 else "border-bottom: 1px solid #F1F5F9;"

        items_html += f"""
        <div style="
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 0.75rem 1.25rem;
            {border}
        ">
            <span style="font-size: 0.875rem; color: #0F172A;">{service}</span>
            <span style="color: {color}; font-size: 0.625rem;">{icon}</span>
        </div>
        """

    st.markdown(f"""
        {items_html}
    </div>
    """, unsafe_allow_html=True)


# ============================================================================
# Helper Functions
# ============================================================================

def _render_empty_state(icon: str, title: str, description: str) -> None:
    """Render a minimal empty state."""
    st.markdown(f"""
    <div style="
        padding: 3rem 2rem;
        text-align: center;
        background: #F8FAFC;
        border-radius: 12px;
        border: 1px dashed #E2E8F0;
    ">
        <div style="font-size: 2rem; color: #CBD5E1; margin-bottom: 0.75rem;">{icon}</div>
        <div style="font-size: 0.9375rem; font-weight: 500; color: #64748B; margin-bottom: 0.25rem;">{title}</div>
        <div style="font-size: 0.8125rem; color: #94A3B8;">{description}</div>
    </div>
    """, unsafe_allow_html=True)


def section_divider(label: str | None = None) -> None:
    """Render a minimal section divider."""
    if label:
        st.markdown(f"""
        <div style="
            display: flex;
            align-items: center;
            gap: 1rem;
            margin: 1.5rem 0;
        ">
            <div style="flex: 1; height: 1px; background: #E2E8F0;"></div>
            <span style="font-size: 0.75rem; color: #94A3B8; text-transform: uppercase; letter-spacing: 0.05em;">
                {label}
            </span>
            <div style="flex: 1; height: 1px; background: #E2E8F0;"></div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style="height: 1px; background: #E2E8F0; margin: 1.5rem 0;"></div>
        """, unsafe_allow_html=True)


def stat_grid(stats: list[dict[str, Any]], columns: int = 4) -> None:
    """Render a grid of stats."""
    cols = st.columns(columns)

    for i, stat in enumerate(stats):
        with cols[i % columns]:
            label = stat.get("label", "")
            value = stat.get("value", "")
            icon = stat.get("icon", "")
            color = stat.get("color", "#6366F1")

            st.markdown(f"""
            <div style="
                background: white;
                border: 1px solid #E2E8F0;
                border-radius: 10px;
                padding: 1rem;
            ">
                {f'<div style="font-size: 1.25rem; margin-bottom: 0.5rem;">{icon}</div>' if icon else ''}
                <div style="font-size: 0.75rem; color: #64748B; margin-bottom: 0.25rem;">
                    {label}
                </div>
                <div style="font-size: 1.5rem; font-weight: 600; color: {color}; letter-spacing: -0.025em;">
                    {value}
                </div>
            </div>
            """, unsafe_allow_html=True)
