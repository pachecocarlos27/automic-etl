"""Sleek, minimal UI components for Automic ETL."""

from __future__ import annotations

from typing import Any, Callable, Literal
from datetime import datetime
import streamlit as st


# ============================================================================
# Page Layout Components
# ============================================================================

def page_header(
    title: str,
    description: str | None = None,
    actions: list[tuple[str, str, Callable]] | None = None,
) -> None:
    """
    Display a clean page header.

    Args:
        title: Page title
        description: Optional description
        actions: List of (label, type, callback) for action buttons
    """
    st.markdown(f"""
    <div class="page-header">
        <h1 style="font-size: 1.75rem; font-weight: 700; color: var(--text); margin: 0 0 0.25rem; letter-spacing: -0.02em;">{title}</h1>
        {f'<p style="font-size: 0.95rem; color: var(--text-muted); margin: 0;">{description}</p>' if description else ''}
    </div>
    """, unsafe_allow_html=True)

    if actions:
        cols = st.columns([1] * len(actions) + [max(1, 6 - len(actions))])
        for i, (label, btn_type, callback) in enumerate(actions):
            with cols[i]:
                if st.button(label, type=btn_type, use_container_width=True):
                    callback()


def section_header(
    title: str,
    description: str | None = None,
    badge: str | None = None,
) -> None:
    """Display a section header."""
    badge_html = f'<span class="badge badge-neutral" style="margin-left: 8px;">{badge}</span>' if badge else ''
    st.markdown(f"""
    <div style="margin: 1.5rem 0 1rem;">
        <h3 style="font-size: 1.125rem; font-weight: 600; color: var(--text); margin: 0; display: inline-flex; align-items: center;">
            {title}{badge_html}
        </h3>
        {f'<p style="font-size: 0.875rem; color: var(--text-muted); margin: 0.25rem 0 0;">{description}</p>' if description else ''}
    </div>
    """, unsafe_allow_html=True)


# ============================================================================
# Status & Badge Components
# ============================================================================

def status_badge(
    status: str,
    variant: Literal["success", "warning", "error", "info", "neutral"] | None = None,
    size: Literal["sm", "md"] = "md",
) -> None:
    """
    Display a minimal status badge.

    Args:
        status: Status text
        variant: Badge variant (auto-detected if not provided)
        size: Badge size
    """
    if variant is None:
        status_lower = status.lower()
        if status_lower in ("success", "completed", "active", "running", "healthy", "online", "connected"):
            variant = "success"
        elif status_lower in ("warning", "pending", "queued", "degraded", "paused"):
            variant = "warning"
        elif status_lower in ("error", "failed", "inactive", "stopped", "offline", "critical", "disconnected"):
            variant = "error"
        elif status_lower in ("info", "processing", "syncing"):
            variant = "info"
        else:
            variant = "neutral"

    padding = "3px 8px" if size == "sm" else "4px 10px"
    font_size = "0.7rem" if size == "sm" else "0.75rem"

    st.markdown(f"""
    <span class="badge badge-{variant}" style="padding: {padding}; font-size: {font_size};">{status}</span>
    """, unsafe_allow_html=True)


def tier_badge(tier: Literal["bronze", "silver", "gold"]) -> None:
    """Display a data tier badge."""
    labels = {"bronze": "Bronze", "silver": "Silver", "gold": "Gold"}
    st.markdown(f"""
    <span class="badge tier-{tier}">{labels.get(tier, tier)}</span>
    """, unsafe_allow_html=True)


def count_badge(count: int, variant: str = "neutral") -> None:
    """Display a count badge."""
    st.markdown(f"""
    <span class="badge badge-{variant}" style="min-width: 20px; text-align: center;">{count}</span>
    """, unsafe_allow_html=True)


# ============================================================================
# Card Components
# ============================================================================

def card(
    content: Callable | None = None,
    title: str | None = None,
    subtitle: str | None = None,
    footer: Callable | None = None,
    padding: str = "1.25rem",
    hover: bool = False,
) -> None:
    """
    Display a minimal card container.

    Args:
        content: Function to render card content
        title: Optional card title
        subtitle: Optional subtitle
        footer: Optional footer render function
        padding: Card padding
        hover: Enable hover effect
    """
    hover_class = "card-hover" if hover else ""

    st.markdown(f"""
    <div class="card {hover_class}" style="padding: {padding};">
    """, unsafe_allow_html=True)

    if title:
        st.markdown(f"""
        <div style="margin-bottom: {'1rem' if content else '0'};">
            <h4 style="font-size: 0.95rem; font-weight: 600; color: var(--text); margin: 0;">{title}</h4>
            {f'<p style="font-size: 0.8rem; color: var(--text-muted); margin: 0.25rem 0 0;">{subtitle}</p>' if subtitle else ''}
        </div>
        """, unsafe_allow_html=True)

    if content:
        content()

    if footer:
        st.markdown('<div style="border-top: 1px solid var(--border); margin-top: 1rem; padding-top: 1rem;">', unsafe_allow_html=True)
        footer()
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)


def metric_card(
    label: str,
    value: str | int | float,
    delta: str | float | None = None,
    delta_type: Literal["positive", "negative", "neutral"] = "neutral",
    icon: str | None = None,
    trend_data: list[float] | None = None,
) -> None:
    """
    Display a clean metric card.

    Args:
        label: Metric label
        value: Metric value
        delta: Optional change indicator
        delta_type: Type of change
        icon: Optional icon
        trend_data: Optional trend sparkline data
    """
    delta_colors = {
        "positive": "var(--success)",
        "negative": "var(--error)",
        "neutral": "var(--text-muted)",
    }
    delta_color = delta_colors.get(delta_type, "var(--text-muted)")
    delta_icon = "+" if delta_type == "positive" else ("-" if delta_type == "negative" else "")

    st.markdown(f"""
    <div class="metric-card">
        <div style="display: flex; align-items: flex-start; justify-content: space-between;">
            <div>
                <p style="font-size: 0.75rem; font-weight: 500; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.04em; margin: 0 0 0.5rem;">
                    {f'{icon} ' if icon else ''}{label}
                </p>
                <p style="font-size: 1.75rem; font-weight: 700; color: var(--text); margin: 0; letter-spacing: -0.02em;">
                    {value}
                </p>
            </div>
            {f'<span style="font-size: 0.8rem; font-weight: 500; color: {delta_color};">{delta_icon}{delta}</span>' if delta else ''}
        </div>
    </div>
    """, unsafe_allow_html=True)


def stat_row(stats: list[tuple[str, str | int, str | None]]) -> None:
    """
    Display a row of statistics.

    Args:
        stats: List of (label, value, icon) tuples
    """
    cols = st.columns(len(stats))
    for i, (label, value, icon) in enumerate(stats):
        with cols[i]:
            st.markdown(f"""
            <div style="text-align: center; padding: 1rem;">
                {f'<div style="font-size: 1.5rem; margin-bottom: 0.5rem;">{icon}</div>' if icon else ''}
                <div style="font-size: 1.5rem; font-weight: 700; color: var(--text);">{value}</div>
                <div style="font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.04em;">{label}</div>
            </div>
            """, unsafe_allow_html=True)


# ============================================================================
# Data Display Components
# ============================================================================

def data_table(
    data: list[dict],
    columns: list[str] | None = None,
    hide_index: bool = True,
) -> None:
    """
    Display a clean data table.

    Args:
        data: List of dictionaries
        columns: Columns to display
        hide_index: Hide row index
    """
    import pandas as pd

    if not data:
        empty_state("No data", "No records to display")
        return

    df = pd.DataFrame(data)
    if columns:
        df = df[[c for c in columns if c in df.columns]]

    st.dataframe(df, hide_index=hide_index, use_container_width=True)


def key_value_display(items: dict[str, Any], columns: int = 2) -> None:
    """
    Display key-value pairs in a clean layout.

    Args:
        items: Dictionary of key-value pairs
        columns: Number of columns
    """
    items_list = list(items.items())

    for i in range(0, len(items_list), columns):
        cols = st.columns(columns)
        for j, col in enumerate(cols):
            if i + j < len(items_list):
                key, value = items_list[i + j]
                with col:
                    st.markdown(f"""
                    <div style="margin-bottom: 1rem;">
                        <p style="font-size: 0.75rem; color: var(--text-muted); margin: 0 0 0.25rem; text-transform: uppercase; letter-spacing: 0.04em;">{key}</p>
                        <p style="font-size: 0.9rem; color: var(--text); margin: 0; font-weight: 500;">{_format_value(value)}</p>
                    </div>
                    """, unsafe_allow_html=True)


def _format_value(value: Any) -> str:
    """Format a value for display."""
    if value is None:
        return '<span style="color: var(--text-muted);">-</span>'
    elif isinstance(value, bool):
        return "Yes" if value else "No"
    elif isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M")
    return str(value)


def list_item(
    title: str,
    subtitle: str | None = None,
    status: str | None = None,
    meta: str | None = None,
    actions: list[tuple[str, Callable]] | None = None,
) -> None:
    """
    Display a list item with optional actions.

    Args:
        title: Item title
        subtitle: Optional subtitle
        status: Optional status badge
        meta: Optional meta information
        actions: Optional action buttons
    """
    cols = st.columns([4, 2, 2] if actions else [4, 2])

    with cols[0]:
        st.markdown(f"""
        <div>
            <p style="font-size: 0.9rem; font-weight: 500; color: var(--text); margin: 0;">{title}</p>
            {f'<p style="font-size: 0.8rem; color: var(--text-muted); margin: 0.125rem 0 0;">{subtitle}</p>' if subtitle else ''}
        </div>
        """, unsafe_allow_html=True)

    with cols[1]:
        if status:
            status_badge(status)
        if meta:
            st.caption(meta)

    if actions and len(cols) > 2:
        with cols[2]:
            action_cols = st.columns(len(actions))
            for i, (label, callback) in enumerate(actions):
                with action_cols[i]:
                    if st.button(label, key=f"action_{title}_{label}"):
                        callback()


# ============================================================================
# Empty & Loading States
# ============================================================================

def empty_state(
    title: str,
    message: str,
    icon: str = "📭",
    action_label: str | None = None,
    action_callback: Callable | None = None,
) -> None:
    """
    Display an empty state placeholder.

    Args:
        title: Title text
        message: Description message
        icon: Display icon
        action_label: Optional action button label
        action_callback: Optional action callback
    """
    st.markdown(f"""
    <div class="empty-state">
        <div class="empty-state-icon">{icon}</div>
        <h4 style="font-size: 1rem; font-weight: 600; color: var(--text); margin: 0 0 0.5rem;">{title}</h4>
        <p style="font-size: 0.875rem; color: var(--text-muted); margin: 0;">{message}</p>
    </div>
    """, unsafe_allow_html=True)

    if action_label and action_callback:
        col1, col2, col3 = st.columns([2, 1, 2])
        with col2:
            if st.button(action_label, type="primary", use_container_width=True):
                action_callback()


def loading_skeleton(rows: int = 3, height: int = 20) -> None:
    """Display loading skeleton placeholders."""
    for _ in range(rows):
        st.markdown(f"""
        <div class="skeleton" style="height: {height}px; margin-bottom: 0.75rem;"></div>
        """, unsafe_allow_html=True)


def spinner_inline(message: str = "Loading...") -> None:
    """Display an inline loading spinner."""
    st.markdown(f"""
    <div style="display: flex; align-items: center; gap: 0.75rem; padding: 0.5rem 0;">
        <div style="
            width: 16px; height: 16px;
            border: 2px solid var(--border);
            border-top-color: var(--accent);
            border-radius: 50%;
            animation: spin 0.8s linear infinite;
        "></div>
        <span style="font-size: 0.875rem; color: var(--text-secondary);">{message}</span>
    </div>
    <style>
    @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
    </style>
    """, unsafe_allow_html=True)


# ============================================================================
# Progress & Steps
# ============================================================================

def progress_bar(
    value: float,
    label: str | None = None,
    show_percentage: bool = True,
    color: str = "var(--accent)",
) -> None:
    """
    Display a minimal progress bar.

    Args:
        value: Progress value (0-100)
        label: Optional label
        show_percentage: Show percentage text
        color: Bar color
    """
    percentage = min(max(value, 0), 100)

    st.markdown(f"""
    <div style="margin: 0.5rem 0;">
        {f'<div style="display: flex; justify-content: space-between; margin-bottom: 0.375rem;"><span style="font-size: 0.8rem; color: var(--text-secondary);">{label}</span><span style="font-size: 0.8rem; color: var(--text-muted);">{percentage:.0f}%</span></div>' if label or show_percentage else ''}
        <div style="height: 6px; background: var(--surface-muted); border-radius: 3px; overflow: hidden;">
            <div style="width: {percentage}%; height: 100%; background: {color}; border-radius: 3px; transition: width 0.3s ease;"></div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def progress_steps(
    steps: list[str],
    current_step: int,
    orientation: Literal["horizontal", "vertical"] = "horizontal",
) -> None:
    """
    Display a progress stepper.

    Args:
        steps: List of step labels
        current_step: Current step (0-indexed)
        orientation: Layout orientation
    """
    if orientation == "horizontal":
        cols = st.columns(len(steps))
        for i, (col, step) in enumerate(zip(cols, steps)):
            with col:
                is_complete = i < current_step
                is_current = i == current_step
                color = "var(--success)" if is_complete else ("var(--accent)" if is_current else "var(--border)")
                bg = color if (is_complete or is_current) else "transparent"
                text_color = "white" if (is_complete or is_current) else "var(--text-muted)"

                st.markdown(f"""
                <div style="text-align: center;">
                    <div style="
                        width: 28px; height: 28px; border-radius: 50%;
                        background: {bg}; border: 2px solid {color};
                        color: {text_color}; font-size: 0.75rem; font-weight: 600;
                        display: inline-flex; align-items: center; justify-content: center;
                        margin-bottom: 0.5rem;
                    ">{'✓' if is_complete else i + 1}</div>
                    <p style="font-size: 0.75rem; color: {'var(--text)' if is_current else 'var(--text-muted)'}; margin: 0; font-weight: {'500' if is_current else '400'};">{step}</p>
                </div>
                """, unsafe_allow_html=True)


# ============================================================================
# Navigation Components
# ============================================================================

def breadcrumb(items: list[tuple[str, str | None]]) -> None:
    """
    Display breadcrumb navigation.

    Args:
        items: List of (label, url) tuples. Use None for current page.
    """
    parts = []
    for i, (label, url) in enumerate(items):
        if url and i < len(items) - 1:
            parts.append(f'<a href="{url}" style="color: var(--accent); text-decoration: none; font-weight: 500;">{label}</a>')
        else:
            parts.append(f'<span style="color: var(--text);">{label}</span>')

    separator = '<span style="color: var(--text-muted); margin: 0 0.5rem;">/</span>'
    st.markdown(f'<nav style="font-size: 0.8rem; margin-bottom: 1.5rem;">{separator.join(parts)}</nav>', unsafe_allow_html=True)


def tab_navigation(
    tabs: list[tuple[str, str]],
    selected: str,
    on_change: Callable[[str], None] | None = None,
) -> str:
    """
    Custom tab navigation.

    Args:
        tabs: List of (label, key) tuples
        selected: Currently selected tab key
        on_change: Callback when tab changes

    Returns:
        Selected tab key
    """
    tab_labels = [t[0] for t in tabs]
    tab_keys = [t[1] for t in tabs]

    selected_idx = tab_keys.index(selected) if selected in tab_keys else 0

    selected_tabs = st.tabs(tab_labels)

    for i, tab in enumerate(selected_tabs):
        with tab:
            if i != selected_idx and on_change:
                on_change(tab_keys[i])
            return tab_keys[i]

    return tab_keys[selected_idx]


# ============================================================================
# Form Components
# ============================================================================

def search_input(
    placeholder: str = "Search...",
    key: str = "search",
    icon: bool = True,
) -> str:
    """
    Display a search input.

    Args:
        placeholder: Placeholder text
        key: Session state key
        icon: Show search icon

    Returns:
        Search value
    """
    return st.text_input(
        label="Search",
        placeholder=f"🔍 {placeholder}" if icon else placeholder,
        key=key,
        label_visibility="collapsed",
    )


def filter_chips(
    options: list[str],
    selected: list[str],
    key: str = "filters",
) -> list[str]:
    """
    Display filter chip selection.

    Args:
        options: Available filter options
        selected: Currently selected options
        key: Session state key

    Returns:
        Selected options
    """
    cols = st.columns(len(options) + 1)

    with cols[0]:
        if st.button("Clear", key=f"{key}_clear"):
            return []

    new_selected = list(selected)

    for i, option in enumerate(options):
        with cols[i + 1]:
            is_selected = option in selected
            if st.button(
                option,
                key=f"{key}_{option}",
                type="primary" if is_selected else "secondary",
            ):
                if is_selected:
                    new_selected.remove(option)
                else:
                    new_selected.append(option)

    return new_selected


# ============================================================================
# Feedback Components
# ============================================================================

def toast(
    message: str,
    type: Literal["success", "error", "warning", "info"] = "info",
) -> None:
    """Display a toast notification."""
    icons = {"success": "✓", "error": "✕", "warning": "!", "info": "i"}
    st.toast(f"{icons.get(type, '')} {message}")


def inline_alert(
    message: str,
    type: Literal["success", "error", "warning", "info"] = "info",
) -> None:
    """Display an inline alert."""
    colors = {
        "success": ("var(--success)", "var(--success-light)"),
        "error": ("var(--error)", "var(--error-light)"),
        "warning": ("var(--warning)", "var(--warning-light)"),
        "info": ("var(--info)", "var(--info-light)"),
    }
    text_color, bg_color = colors.get(type, colors["info"])

    st.markdown(f"""
    <div style="
        background: {bg_color};
        border-left: 3px solid {text_color};
        padding: 0.75rem 1rem;
        border-radius: 0 var(--radius-sm) var(--radius-sm) 0;
        font-size: 0.875rem;
        color: {text_color};
    ">{message}</div>
    """, unsafe_allow_html=True)


# ============================================================================
# Layout Utilities
# ============================================================================

def divider(text: str | None = None) -> None:
    """Display a divider with optional text."""
    if text:
        st.markdown(f"""
        <div style="display: flex; align-items: center; gap: 1rem; margin: 1.5rem 0;">
            <div style="flex: 1; height: 1px; background: var(--border);"></div>
            <span style="color: var(--text-muted); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.04em;">{text}</span>
            <div style="flex: 1; height: 1px; background: var(--border);"></div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("---")


def spacer(size: Literal["xs", "sm", "md", "lg", "xl"] = "md") -> None:
    """Add vertical spacing."""
    heights = {"xs": "0.5rem", "sm": "1rem", "md": "1.5rem", "lg": "2rem", "xl": "3rem"}
    st.markdown(f'<div style="height: {heights.get(size, "1.5rem")};"></div>', unsafe_allow_html=True)


def grid_container(columns: int = 2, gap: str = "1rem") -> list:
    """Create a grid layout with custom gap."""
    return st.columns(columns)


# ============================================================================
# Icon Components
# ============================================================================

def icon_button(
    icon: str,
    tooltip: str,
    on_click: Callable,
    key: str,
    size: Literal["sm", "md", "lg"] = "md",
) -> None:
    """Display an icon-only button."""
    sizes = {"sm": "28px", "md": "36px", "lg": "44px"}
    if st.button(icon, key=key, help=tooltip):
        on_click()


def icon_with_label(
    icon: str,
    label: str,
    sublabel: str | None = None,
) -> None:
    """Display an icon with label."""
    st.markdown(f"""
    <div style="display: flex; align-items: center; gap: 0.75rem;">
        <span style="font-size: 1.5rem;">{icon}</span>
        <div>
            <p style="font-size: 0.9rem; font-weight: 500; color: var(--text); margin: 0;">{label}</p>
            {f'<p style="font-size: 0.8rem; color: var(--text-muted); margin: 0;">{sublabel}</p>' if sublabel else ''}
        </div>
    </div>
    """, unsafe_allow_html=True)
