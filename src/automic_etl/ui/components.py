"""Reusable UI components for Automic ETL."""

from __future__ import annotations

from typing import Any, Callable, Literal
from datetime import datetime
import streamlit as st


# ============================================================================
# Status and Badge Components
# ============================================================================

def status_badge(
    status: str,
    variant: Literal["success", "warning", "danger", "info"] | None = None,
) -> None:
    """
    Display a status badge.

    Args:
        status: Status text to display
        variant: Badge variant (auto-detected if not provided)
    """
    if variant is None:
        # Auto-detect variant from status text
        status_lower = status.lower()
        if status_lower in ("success", "completed", "active", "running", "healthy", "online"):
            variant = "success"
        elif status_lower in ("warning", "pending", "queued", "degraded"):
            variant = "warning"
        elif status_lower in ("error", "failed", "inactive", "stopped", "offline", "critical"):
            variant = "danger"
        else:
            variant = "info"

    st.markdown(
        f'<span class="status-badge status-{variant}">{status}</span>',
        unsafe_allow_html=True
    )


def tier_badge(tier: Literal["bronze", "silver", "gold"]) -> None:
    """Display a data tier badge."""
    tier_labels = {
        "bronze": "Bronze",
        "silver": "Silver",
        "gold": "Gold",
    }
    st.markdown(
        f'<span class="tier-badge tier-{tier}">{tier_labels.get(tier, tier)}</span>',
        unsafe_allow_html=True
    )


# ============================================================================
# Card Components
# ============================================================================

def card(
    title: str | None = None,
    subtitle: str | None = None,
    content: Callable | None = None,
    icon: str | None = None,
    footer: str | None = None,
) -> None:
    """
    Display a styled card.

    Args:
        title: Card title
        subtitle: Card subtitle
        content: Function to render card content
        icon: Optional emoji icon
        footer: Optional footer text
    """
    with st.container():
        st.markdown("""
        <div style="
            background: var(--surface);
            border: 1px solid var(--border-light);
            border-radius: var(--radius-lg);
            padding: 1.5rem;
            margin-bottom: 1rem;
            box-shadow: var(--shadow-sm);
        ">
        """, unsafe_allow_html=True)

        if title:
            header = f"{icon} {title}" if icon else title
            st.markdown(f"### {header}")
            if subtitle:
                st.caption(subtitle)
            st.markdown("---")

        if content:
            content()

        if footer:
            st.markdown("---")
            st.caption(footer)

        st.markdown("</div>", unsafe_allow_html=True)


def stat_card(
    label: str,
    value: str | int | float,
    delta: str | float | None = None,
    delta_color: Literal["normal", "inverse", "off"] = "normal",
    icon: str | None = None,
    help_text: str | None = None,
) -> None:
    """
    Display a statistics card with optional delta.

    Args:
        label: Metric label
        value: Metric value
        delta: Optional change value
        delta_color: Delta color mode
        icon: Optional emoji icon
        help_text: Optional help tooltip
    """
    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, var(--surface) 0%, var(--background) 100%);
        border: 1px solid var(--border-light);
        border-radius: var(--radius-lg);
        padding: 1.25rem;
        box-shadow: var(--shadow-sm);
    ">
        <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.5rem;">
            {f'<span style="font-size: 1.5rem;">{icon}</span>' if icon else ''}
            <span style="
                color: var(--text-secondary);
                font-size: 0.875rem;
                font-weight: 500;
                text-transform: uppercase;
                letter-spacing: 0.05em;
            ">{label}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.metric(
        label="",
        value=value,
        delta=delta,
        delta_color=delta_color,
        help=help_text,
    )


def info_card(
    title: str,
    items: dict[str, Any],
    icon: str | None = None,
) -> None:
    """
    Display an information card with key-value pairs.

    Args:
        title: Card title
        items: Dictionary of label-value pairs
        icon: Optional emoji icon
    """
    header = f"{icon} {title}" if icon else title
    st.markdown(f"#### {header}")

    for label, value in items.items():
        col1, col2 = st.columns([1, 2])
        with col1:
            st.markdown(f"**{label}:**")
        with col2:
            if isinstance(value, datetime):
                st.markdown(value.strftime("%Y-%m-%d %H:%M"))
            elif isinstance(value, bool):
                st.markdown("Yes" if value else "No")
            else:
                st.markdown(str(value))


# ============================================================================
# Navigation Components
# ============================================================================

def page_header(
    title: str,
    subtitle: str | None = None,
    icon: str | None = None,
    actions: list[tuple[str, Callable]] | None = None,
) -> None:
    """
    Display a page header with optional actions.

    Args:
        title: Page title
        subtitle: Optional subtitle
        icon: Optional emoji icon
        actions: List of (label, callback) tuples for action buttons
    """
    col1, col2 = st.columns([3, 1])

    with col1:
        header = f"{icon} {title}" if icon else title
        st.markdown(f"# {header}")
        if subtitle:
            st.markdown(f"*{subtitle}*")

    with col2:
        if actions:
            for label, callback in actions:
                if st.button(label, use_container_width=True):
                    callback()


def breadcrumb(items: list[tuple[str, str | None]]) -> None:
    """
    Display a breadcrumb navigation.

    Args:
        items: List of (label, page_key) tuples. Use None for current page.
    """
    parts = []
    for i, (label, page_key) in enumerate(items):
        if page_key and i < len(items) - 1:
            parts.append(f'<a href="#{page_key}" style="color: var(--primary); text-decoration: none;">{label}</a>')
        else:
            parts.append(f'<span style="color: var(--text-primary);">{label}</span>')

    breadcrumb_html = ' <span style="color: var(--text-muted); margin: 0 0.5rem;">/</span> '.join(parts)

    st.markdown(
        f'<nav style="margin-bottom: 1rem; font-size: 0.875rem;">{breadcrumb_html}</nav>',
        unsafe_allow_html=True
    )


def tabs_with_icons(
    tabs: list[tuple[str, str]],
    default_index: int = 0,
) -> str:
    """
    Create tabs with emoji icons.

    Args:
        tabs: List of (icon + label, key) tuples
        default_index: Default selected tab index

    Returns:
        Selected tab key
    """
    tab_labels = [t[0] for t in tabs]
    tab_keys = [t[1] for t in tabs]

    selected = st.tabs(tab_labels)

    for i, tab in enumerate(selected):
        with tab:
            if i == default_index:
                return tab_keys[i]

    return tab_keys[default_index]


# ============================================================================
# Data Display Components
# ============================================================================

def data_table(
    data: list[dict],
    columns: list[str] | None = None,
    show_index: bool = False,
    selectable: bool = False,
    on_select: Callable[[list[int]], None] | None = None,
) -> list[int] | None:
    """
    Display a styled data table.

    Args:
        data: List of dictionaries
        columns: Columns to display (all if None)
        show_index: Whether to show row index
        selectable: Whether rows are selectable
        on_select: Callback when selection changes

    Returns:
        Selected row indices if selectable
    """
    import pandas as pd

    if not data:
        st.info("No data to display")
        return []

    df = pd.DataFrame(data)

    if columns:
        df = df[columns]

    if selectable:
        selected = st.data_editor(
            df,
            hide_index=not show_index,
            use_container_width=True,
            num_rows="fixed",
        )
        return selected
    else:
        st.dataframe(
            df,
            hide_index=not show_index,
            use_container_width=True,
        )
        return None


def key_value_list(items: dict[str, Any], columns: int = 2) -> None:
    """
    Display a key-value list in columns.

    Args:
        items: Dictionary of key-value pairs
        columns: Number of columns
    """
    items_list = list(items.items())
    rows = [items_list[i:i + columns] for i in range(0, len(items_list), columns)]

    for row in rows:
        cols = st.columns(columns)
        for i, (key, value) in enumerate(row):
            with cols[i]:
                st.markdown(f"**{key}**")
                if isinstance(value, datetime):
                    st.markdown(value.strftime("%Y-%m-%d %H:%M:%S"))
                elif isinstance(value, bool):
                    status_badge("Yes" if value else "No", "success" if value else "danger")
                elif value is None:
                    st.markdown("*N/A*")
                else:
                    st.markdown(str(value))


def json_viewer(data: dict | list, expanded: bool = True) -> None:
    """
    Display JSON data in an expandable viewer.

    Args:
        data: JSON-serializable data
        expanded: Whether to expand by default
    """
    import json

    with st.expander("JSON Data", expanded=expanded):
        st.json(data)


# ============================================================================
# Form Components
# ============================================================================

def search_box(
    placeholder: str = "Search...",
    key: str = "search",
    on_change: Callable[[str], None] | None = None,
) -> str:
    """
    Display a search input box.

    Args:
        placeholder: Input placeholder text
        key: Session state key
        on_change: Callback when value changes

    Returns:
        Current search value
    """
    value = st.text_input(
        label="Search",
        placeholder=placeholder,
        key=key,
        label_visibility="collapsed",
    )

    if on_change and value:
        on_change(value)

    return value


def confirm_dialog(
    title: str,
    message: str,
    confirm_label: str = "Confirm",
    cancel_label: str = "Cancel",
    danger: bool = False,
) -> bool | None:
    """
    Display a confirmation dialog.

    Args:
        title: Dialog title
        message: Dialog message
        confirm_label: Confirm button label
        cancel_label: Cancel button label
        danger: Whether this is a dangerous action

    Returns:
        True if confirmed, False if cancelled, None if pending
    """
    st.markdown(f"### {title}")
    st.markdown(message)

    col1, col2 = st.columns(2)

    with col1:
        if st.button(cancel_label, use_container_width=True):
            return False

    with col2:
        button_type = "primary" if not danger else "primary"
        if st.button(confirm_label, use_container_width=True, type=button_type):
            return True

    return None


def form_section(title: str, description: str | None = None) -> None:
    """
    Display a form section header.

    Args:
        title: Section title
        description: Optional description
    """
    st.markdown(f"#### {title}")
    if description:
        st.caption(description)
    st.markdown("---")


# ============================================================================
# Feedback Components
# ============================================================================

def loading_spinner(message: str = "Loading...") -> None:
    """Display a loading spinner with message."""
    st.markdown(f"""
    <div style="display: flex; align-items: center; gap: 0.5rem; padding: 1rem;">
        <div class="animate-spin" style="
            width: 20px;
            height: 20px;
            border: 2px solid var(--border);
            border-top-color: var(--primary);
            border-radius: 50%;
        "></div>
        <span style="color: var(--text-secondary);">{message}</span>
    </div>
    """, unsafe_allow_html=True)


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
        title: Empty state title
        message: Description message
        icon: Emoji icon
        action_label: Optional action button label
        action_callback: Optional action callback
    """
    st.markdown(f"""
    <div style="
        text-align: center;
        padding: 3rem 2rem;
        background: var(--surface);
        border-radius: var(--radius-lg);
        border: 2px dashed var(--border);
    ">
        <div style="font-size: 3rem; margin-bottom: 1rem;">{icon}</div>
        <h3 style="color: var(--text-primary); margin-bottom: 0.5rem;">{title}</h3>
        <p style="color: var(--text-secondary);">{message}</p>
    </div>
    """, unsafe_allow_html=True)

    if action_label and action_callback:
        col1, col2, col3 = st.columns([1, 1, 1])
        with col2:
            if st.button(action_label, use_container_width=True, type="primary"):
                action_callback()


def progress_steps(
    steps: list[str],
    current_step: int,
    completed_steps: list[int] | None = None,
) -> None:
    """
    Display a progress stepper.

    Args:
        steps: List of step labels
        current_step: Current step index (0-based)
        completed_steps: List of completed step indices
    """
    completed = completed_steps or []

    cols = st.columns(len(steps))

    for i, (col, step) in enumerate(zip(cols, steps)):
        with col:
            if i in completed:
                status = "completed"
                icon = "✓"
                color = "var(--success)"
            elif i == current_step:
                status = "current"
                icon = str(i + 1)
                color = "var(--primary)"
            else:
                status = "pending"
                icon = str(i + 1)
                color = "var(--text-muted)"

            st.markdown(f"""
            <div style="text-align: center;">
                <div style="
                    width: 32px;
                    height: 32px;
                    border-radius: 50%;
                    background: {color if status != 'pending' else 'var(--surface)'};
                    border: 2px solid {color};
                    color: {'white' if status != 'pending' else color};
                    display: inline-flex;
                    align-items: center;
                    justify-content: center;
                    font-weight: 600;
                    margin-bottom: 0.5rem;
                ">{icon}</div>
                <div style="
                    font-size: 0.75rem;
                    color: {color};
                    font-weight: {'600' if status == 'current' else '400'};
                ">{step}</div>
            </div>
            """, unsafe_allow_html=True)


# ============================================================================
# Chart Components
# ============================================================================

def mini_chart(
    data: list[float],
    chart_type: Literal["line", "bar", "area"] = "line",
    color: str | None = None,
    height: int = 60,
) -> None:
    """
    Display a mini sparkline chart.

    Args:
        data: List of numeric values
        chart_type: Type of chart
        color: Chart color
        height: Chart height in pixels
    """
    import pandas as pd

    df = pd.DataFrame({"value": data})

    if chart_type == "line":
        st.line_chart(df, height=height, use_container_width=True)
    elif chart_type == "bar":
        st.bar_chart(df, height=height, use_container_width=True)
    elif chart_type == "area":
        st.area_chart(df, height=height, use_container_width=True)


# ============================================================================
# Layout Components
# ============================================================================

def grid(columns: int = 2) -> list:
    """
    Create a grid layout.

    Args:
        columns: Number of columns

    Returns:
        List of column objects
    """
    return st.columns(columns)


def spacer(size: Literal["xs", "sm", "md", "lg", "xl"] = "md") -> None:
    """Add vertical spacing."""
    sizes = {"xs": 1, "sm": 2, "md": 3, "lg": 4, "xl": 5}
    for _ in range(sizes.get(size, 3)):
        st.write("")


def divider(text: str | None = None) -> None:
    """Display a divider with optional text."""
    if text:
        st.markdown(f"""
        <div style="
            display: flex;
            align-items: center;
            gap: 1rem;
            margin: 1rem 0;
        ">
            <div style="flex: 1; height: 1px; background: var(--border);"></div>
            <span style="color: var(--text-muted); font-size: 0.875rem;">{text}</span>
            <div style="flex: 1; height: 1px; background: var(--border);"></div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("---")


# ============================================================================
# Utility Components
# ============================================================================

def copy_button(text: str, label: str = "Copy") -> None:
    """Display a copy-to-clipboard button."""
    import html as html_lib

    escaped_text = html_lib.escape(text)

    st.markdown(f"""
    <button onclick="navigator.clipboard.writeText('{escaped_text}')" style="
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: var(--radius-md);
        padding: 0.25rem 0.5rem;
        cursor: pointer;
        font-size: 0.75rem;
        color: var(--text-secondary);
    ">
        {label}
    </button>
    """, unsafe_allow_html=True)


def tooltip(text: str, content: str) -> None:
    """Display text with a tooltip."""
    st.markdown(f"""
    <span title="{content}" style="
        border-bottom: 1px dotted var(--text-muted);
        cursor: help;
    ">{text}</span>
    """, unsafe_allow_html=True)


def external_link(url: str, label: str, icon: str = "🔗") -> None:
    """Display an external link."""
    st.markdown(
        f'<a href="{url}" target="_blank" style="color: var(--primary); text-decoration: none;">'
        f'{icon} {label}</a>',
        unsafe_allow_html=True
    )
