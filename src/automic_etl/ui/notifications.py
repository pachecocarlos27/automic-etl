"""Notification toast system for Automic ETL UI."""

from __future__ import annotations

from typing import Callable
from datetime import datetime
import streamlit as st

from automic_etl.ui.state import (
    get_state,
    Notification,
    NotificationType,
)


def inject_notification_styles():
    """Inject CSS styles for notifications."""
    st.markdown("""
    <style>
    /* Toast container */
    .toast-container {
        position: fixed;
        top: 80px;
        right: 20px;
        z-index: 9999;
        display: flex;
        flex-direction: column;
        gap: 10px;
        max-width: 400px;
        pointer-events: none;
    }

    /* Individual toast */
    .toast {
        background: var(--background);
        border-radius: var(--radius-lg);
        box-shadow: var(--shadow-lg);
        padding: 1rem;
        display: flex;
        align-items: flex-start;
        gap: 0.75rem;
        animation: slideIn 0.3s ease-out;
        pointer-events: auto;
        border-left: 4px solid;
    }

    .toast-success {
        border-left-color: var(--success);
    }

    .toast-error {
        border-left-color: var(--danger);
    }

    .toast-warning {
        border-left-color: var(--warning);
    }

    .toast-info {
        border-left-color: var(--info);
    }

    .toast-icon {
        font-size: 1.25rem;
        flex-shrink: 0;
    }

    .toast-content {
        flex: 1;
        min-width: 0;
    }

    .toast-title {
        font-weight: 600;
        color: var(--text-primary);
        margin-bottom: 0.25rem;
    }

    .toast-message {
        font-size: 0.875rem;
        color: var(--text-secondary);
        line-height: 1.4;
    }

    .toast-timestamp {
        font-size: 0.75rem;
        color: var(--text-muted);
        margin-top: 0.25rem;
    }

    .toast-close {
        background: none;
        border: none;
        color: var(--text-muted);
        cursor: pointer;
        padding: 0;
        font-size: 1rem;
        line-height: 1;
        flex-shrink: 0;
    }

    .toast-close:hover {
        color: var(--text-primary);
    }

    .toast-action {
        margin-top: 0.5rem;
    }

    .toast-action button {
        background: var(--primary);
        color: white;
        border: none;
        padding: 0.25rem 0.75rem;
        border-radius: var(--radius-md);
        font-size: 0.75rem;
        cursor: pointer;
    }

    .toast-action button:hover {
        background: var(--primary-hover);
    }

    @keyframes slideIn {
        from {
            transform: translateX(100%);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }

    @keyframes slideOut {
        from {
            transform: translateX(0);
            opacity: 1;
        }
        to {
            transform: translateX(100%);
            opacity: 0;
        }
    }

    .toast.dismissing {
        animation: slideOut 0.3s ease-in forwards;
    }

    /* Notification panel */
    .notification-panel {
        position: fixed;
        top: 0;
        right: 0;
        width: 400px;
        height: 100vh;
        background: var(--background);
        box-shadow: var(--shadow-xl);
        z-index: 9998;
        transform: translateX(100%);
        transition: transform 0.3s ease;
        overflow-y: auto;
    }

    .notification-panel.open {
        transform: translateX(0);
    }

    .notification-panel-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 1rem;
        border-bottom: 1px solid var(--border);
        position: sticky;
        top: 0;
        background: var(--background);
    }

    .notification-panel-title {
        font-weight: 600;
        font-size: 1.125rem;
    }

    .notification-item {
        padding: 1rem;
        border-bottom: 1px solid var(--border-light);
        display: flex;
        gap: 0.75rem;
    }

    .notification-item:hover {
        background: var(--surface);
    }

    .notification-item.unread {
        background: var(--info-light);
    }

    .notification-badge {
        background: var(--danger);
        color: white;
        font-size: 0.75rem;
        padding: 0.125rem 0.375rem;
        border-radius: var(--radius-full);
        min-width: 18px;
        text-align: center;
    }

    /* Notification bell */
    .notification-bell {
        position: relative;
        cursor: pointer;
    }

    .notification-bell .badge {
        position: absolute;
        top: -5px;
        right: -5px;
        background: var(--danger);
        color: white;
        font-size: 0.65rem;
        padding: 0.1rem 0.3rem;
        border-radius: var(--radius-full);
        min-width: 16px;
        text-align: center;
    }
    </style>
    """, unsafe_allow_html=True)


def render_toast_container():
    """Render the toast notification container."""
    state = get_state()
    notifications = state.get("notifications", [])

    # Only show recent notifications as toasts (last 3, within 10 seconds)
    now = datetime.now()
    recent_toasts = [
        n for n in notifications[:3]
        if isinstance(n, Notification) and (now - n.timestamp).total_seconds() < 10
    ]

    if not recent_toasts:
        return

    toast_html = '<div class="toast-container">'

    for notification in recent_toasts:
        icon = {
            NotificationType.SUCCESS: "✅",
            NotificationType.ERROR: "❌",
            NotificationType.WARNING: "⚠️",
            NotificationType.INFO: "ℹ️",
        }.get(notification.type, "ℹ️")

        toast_class = f"toast toast-{notification.type.value}"

        action_html = ""
        if notification.action_label:
            action_html = f'''
            <div class="toast-action">
                <button onclick="window.location.hash='#{notification.action_callback}'">{notification.action_label}</button>
            </div>
            '''

        toast_html += f'''
        <div class="{toast_class}" data-id="{notification.id}">
            <span class="toast-icon">{icon}</span>
            <div class="toast-content">
                <div class="toast-title">{notification.title}</div>
                {"<div class='toast-message'>" + notification.message + "</div>" if notification.message else ""}
                <div class="toast-timestamp">{notification.timestamp.strftime("%H:%M")}</div>
                {action_html}
            </div>
            <button class="toast-close" onclick="this.parentElement.classList.add('dismissing'); setTimeout(() => this.parentElement.remove(), 300);">×</button>
        </div>
        '''

    toast_html += '</div>'

    # Add auto-dismiss script
    toast_html += '''
    <script>
    (function() {
        const toasts = document.querySelectorAll('.toast[data-id]');
        toasts.forEach(toast => {
            if (!toast.dataset.timerSet) {
                toast.dataset.timerSet = 'true';
                setTimeout(() => {
                    toast.classList.add('dismissing');
                    setTimeout(() => toast.remove(), 300);
                }, 5000);
            }
        });
    })();
    </script>
    '''

    st.markdown(toast_html, unsafe_allow_html=True)


def render_notification_bell():
    """Render the notification bell icon with badge."""
    state = get_state()
    unread = state.get("unread_notifications", 0)

    badge_html = f'<span class="badge">{unread}</span>' if unread > 0 else ""

    st.markdown(f"""
    <div class="notification-bell" onclick="document.querySelector('.notification-panel').classList.toggle('open')">
        <span style="font-size: 1.25rem;">🔔</span>
        {badge_html}
    </div>
    """, unsafe_allow_html=True)


def render_notification_panel():
    """Render the slide-out notification panel."""
    state = get_state()
    notifications = state.get("notifications", [])

    panel_html = '''
    <div class="notification-panel">
        <div class="notification-panel-header">
            <span class="notification-panel-title">Notifications</span>
            <div style="display: flex; gap: 0.5rem;">
                <button style="background: none; border: none; color: var(--text-secondary); cursor: pointer;"
                        onclick="/* mark all read */">Mark all read</button>
                <button class="toast-close" onclick="this.closest('.notification-panel').classList.remove('open')">×</button>
            </div>
        </div>
        <div class="notification-panel-content">
    '''

    if not notifications:
        panel_html += '''
        <div style="padding: 2rem; text-align: center; color: var(--text-muted);">
            <div style="font-size: 2rem; margin-bottom: 0.5rem;">🔕</div>
            <div>No notifications</div>
        </div>
        '''
    else:
        for notification in notifications[:20]:
            if not isinstance(notification, Notification):
                continue

            icon = {
                NotificationType.SUCCESS: "✅",
                NotificationType.ERROR: "❌",
                NotificationType.WARNING: "⚠️",
                NotificationType.INFO: "ℹ️",
            }.get(notification.type, "ℹ️")

            panel_html += f'''
            <div class="notification-item">
                <span style="font-size: 1.25rem;">{icon}</span>
                <div style="flex: 1;">
                    <div style="font-weight: 500;">{notification.title}</div>
                    {"<div style='font-size: 0.875rem; color: var(--text-secondary);'>" + notification.message + "</div>" if notification.message else ""}
                    <div style="font-size: 0.75rem; color: var(--text-muted); margin-top: 0.25rem;">
                        {notification.timestamp.strftime("%Y-%m-%d %H:%M")}
                    </div>
                </div>
            </div>
            '''

    panel_html += '''
        </div>
    </div>
    '''

    st.markdown(panel_html, unsafe_allow_html=True)


def show_inline_notification(
    type: NotificationType,
    title: str,
    message: str = "",
    dismissible: bool = True,
):
    """Show an inline notification (Streamlit native)."""
    if type == NotificationType.SUCCESS:
        st.success(f"**{title}**\n\n{message}" if message else title)
    elif type == NotificationType.ERROR:
        st.error(f"**{title}**\n\n{message}" if message else title)
    elif type == NotificationType.WARNING:
        st.warning(f"**{title}**\n\n{message}" if message else title)
    else:
        st.info(f"**{title}**\n\n{message}" if message else title)


def render_alert_banner(
    type: NotificationType,
    title: str,
    message: str = "",
    action_label: str | None = None,
    action_callback: Callable | None = None,
):
    """Render a sticky alert banner at the top of the page."""
    colors = {
        NotificationType.SUCCESS: ("var(--success-light)", "var(--success)"),
        NotificationType.ERROR: ("var(--danger-light)", "var(--danger)"),
        NotificationType.WARNING: ("var(--warning-light)", "#856404"),
        NotificationType.INFO: ("var(--info-light)", "var(--info)"),
    }

    bg_color, text_color = colors.get(type, colors[NotificationType.INFO])

    icon = {
        NotificationType.SUCCESS: "✅",
        NotificationType.ERROR: "❌",
        NotificationType.WARNING: "⚠️",
        NotificationType.INFO: "ℹ️",
    }.get(type, "ℹ️")

    st.markdown(f"""
    <div style="
        background: {bg_color};
        color: {text_color};
        padding: 0.75rem 1rem;
        border-radius: var(--radius-md);
        margin-bottom: 1rem;
        display: flex;
        align-items: center;
        gap: 0.75rem;
    ">
        <span style="font-size: 1.25rem;">{icon}</span>
        <div style="flex: 1;">
            <div style="font-weight: 600;">{title}</div>
            {"<div style='font-size: 0.875rem; margin-top: 0.25rem;'>" + message + "</div>" if message else ""}
        </div>
    </div>
    """, unsafe_allow_html=True)

    if action_label and action_callback:
        if st.button(action_label):
            action_callback()
