"""Alerts & Notifications Management Page for Automic ETL UI."""

from __future__ import annotations

import streamlit as st
from datetime import datetime, timedelta
from typing import Any


def show_alerts_page():
    """Display the alerts and notifications management page."""
    st.title("Alerts & Notifications")
    st.markdown("Configure alert rules, notification channels, and manage alerts.")

    tab1, tab2, tab3, tab4 = st.tabs([
        "Alert Dashboard",
        "Alert Rules",
        "Notification Channels",
        "Alert History",
    ])

    with tab1:
        _show_alert_dashboard()

    with tab2:
        _show_alert_rules()

    with tab3:
        _show_notification_channels()

    with tab4:
        _show_alert_history()


def _show_alert_dashboard():
    """Show alert dashboard overview."""
    st.subheader("Alert Overview")

    # Active alerts summary
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown("""
        <div style="
            background: linear-gradient(135deg, #EF4444 0%, #DC2626 100%);
            padding: 1.5rem;
            border-radius: 12px;
            color: white;
            text-align: center;
        ">
            <div style="font-size: 2.5rem; font-weight: 700;">5</div>
            <div style="font-size: 0.875rem; opacity: 0.9;">Critical Alerts</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.metric(
            label="Warning Alerts",
            value="12",
            delta="-3",
        )

    with col3:
        st.metric(
            label="Resolved Today",
            value="23",
            delta="+8",
        )

    with col4:
        st.metric(
            label="Notification Sent",
            value="45",
            delta="last 24h",
        )

    st.markdown("---")

    # Active critical alerts
    st.subheader("Active Critical Alerts")

    critical_alerts = [
        {
            "id": "alert-001",
            "title": "Pipeline Failure: customer_etl_daily",
            "message": "Pipeline failed with error: Connection timeout to source database",
            "source": "Pipeline Monitor",
            "triggered": datetime.now() - timedelta(hours=2),
            "acknowledged": False,
        },
        {
            "id": "alert-002",
            "title": "Data Quality Threshold Breached",
            "message": "bronze.raw_orders: null rate exceeded 10% (current: 15.2%)",
            "source": "Quality Monitor",
            "triggered": datetime.now() - timedelta(hours=5),
            "acknowledged": True,
        },
        {
            "id": "alert-003",
            "title": "Storage Quota Warning",
            "message": "Gold layer storage usage at 85% of quota",
            "source": "Storage Monitor",
            "triggered": datetime.now() - timedelta(hours=8),
            "acknowledged": False,
        },
    ]

    for alert in critical_alerts:
        with st.container():
            col1, col2, col3 = st.columns([4, 2, 1])

            with col1:
                ack_icon = "" if alert["acknowledged"] else ""
                st.markdown(f"{ack_icon} **{alert['title']}**")
                st.caption(alert["message"])
                st.caption(f"Source: {alert['source']} | Triggered: {alert['triggered'].strftime('%Y-%m-%d %H:%M')}")

            with col2:
                time_ago = datetime.now() - alert["triggered"]
                hours = int(time_ago.total_seconds() / 3600)
                st.markdown(f":red[CRITICAL]")
                st.caption(f"{hours} hours ago")

            with col3:
                if not alert["acknowledged"]:
                    if st.button("Acknowledge", key=f"ack_{alert['id']}"):
                        st.success("Alert acknowledged")
                else:
                    if st.button("Resolve", key=f"resolve_{alert['id']}", type="primary"):
                        st.success("Alert resolved")

            st.markdown("---")

    # Recent notifications sent
    st.subheader("Recent Notifications")

    notifications = [
        {"channel": "Slack", "message": "Pipeline customer_etl_daily failed", "sent": "10 min ago", "icon": ""},
        {"channel": "Email", "message": "Daily quality report generated", "sent": "1 hour ago", "icon": ""},
        {"channel": "PagerDuty", "message": "Critical: Storage quota warning", "sent": "2 hours ago", "icon": ""},
        {"channel": "Webhook", "message": "Job completed: orders_sync", "sent": "3 hours ago", "icon": ""},
    ]

    for notif in notifications:
        col1, col2, col3 = st.columns([1, 4, 1])

        with col1:
            st.markdown(f"### {notif['icon']}")

        with col2:
            st.markdown(f"**{notif['channel']}**")
            st.caption(notif["message"])

        with col3:
            st.caption(notif["sent"])


def _show_alert_rules():
    """Show alert rules management."""
    st.subheader("Alert Rules")

    # Create new rule
    with st.expander("Create New Alert Rule", expanded=False):
        col1, col2 = st.columns(2)

        with col1:
            rule_name = st.text_input("Rule Name", placeholder="pipeline_failure_alert")
            rule_type = st.selectbox(
                "Rule Type",
                [
                    "Pipeline Failure",
                    "Pipeline Duration Exceeded",
                    "Data Quality Threshold",
                    "Storage Quota",
                    "Job Queue Backlog",
                    "Data Freshness",
                    "Custom Metric",
                ],
            )

            if rule_type == "Pipeline Failure":
                target_pipeline = st.selectbox(
                    "Pipeline",
                    ["All Pipelines", "customer_processing", "orders_pipeline", "aggregation_pipeline"],
                )
            elif rule_type == "Pipeline Duration Exceeded":
                duration_threshold = st.number_input("Threshold (minutes)", min_value=1, value=60)
            elif rule_type == "Data Quality Threshold":
                quality_metric = st.selectbox("Metric", ["Null Rate", "Duplicate Rate", "Validation Failures"])
                threshold_value = st.number_input("Threshold (%)", min_value=0, max_value=100, value=10)
            elif rule_type == "Storage Quota":
                storage_threshold = st.slider("Usage Threshold (%)", 50, 100, 80)
            elif rule_type == "Custom Metric":
                custom_query = st.text_area("Metric Query (SQL)", placeholder="SELECT COUNT(*) FROM ...")

        with col2:
            severity = st.selectbox("Severity", ["Critical", "Warning", "Info"])
            notification_channels = st.multiselect(
                "Notification Channels",
                ["Email", "Slack", "PagerDuty", "Webhook", "SMS"],
                default=["Email", "Slack"],
            )

            st.markdown("**Notification Settings**")
            notify_on_trigger = st.checkbox("Notify on trigger", value=True)
            notify_on_resolve = st.checkbox("Notify on resolution", value=True)
            cooldown = st.number_input("Cooldown (minutes)", min_value=0, value=15,
                                       help="Minimum time between repeated alerts")

            enabled = st.checkbox("Enable Rule", value=True)

        if st.button("Create Rule", type="primary"):
            st.success(f"Alert rule '{rule_name}' created successfully!")

    st.markdown("---")

    # Existing rules
    rules = [
        {
            "id": "rule-001",
            "name": "pipeline_failure_critical",
            "type": "Pipeline Failure",
            "target": "All Pipelines",
            "severity": "Critical",
            "channels": ["Slack", "PagerDuty", "Email"],
            "enabled": True,
            "triggered_count": 12,
        },
        {
            "id": "rule-002",
            "name": "quality_threshold_warning",
            "type": "Data Quality Threshold",
            "target": "Null Rate > 10%",
            "severity": "Warning",
            "channels": ["Slack", "Email"],
            "enabled": True,
            "triggered_count": 45,
        },
        {
            "id": "rule-003",
            "name": "storage_quota_alert",
            "type": "Storage Quota",
            "target": "> 80% usage",
            "severity": "Warning",
            "channels": ["Email"],
            "enabled": True,
            "triggered_count": 3,
        },
        {
            "id": "rule-004",
            "name": "job_backlog_alert",
            "type": "Job Queue Backlog",
            "target": "> 50 pending jobs",
            "severity": "Warning",
            "channels": ["Slack"],
            "enabled": False,
            "triggered_count": 8,
        },
        {
            "id": "rule-005",
            "name": "data_freshness_alert",
            "type": "Data Freshness",
            "target": "gold.* > 6 hours stale",
            "severity": "Critical",
            "channels": ["Slack", "PagerDuty"],
            "enabled": True,
            "triggered_count": 2,
        },
    ]

    for rule in rules:
        with st.container():
            col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 1, 1])

            with col1:
                status_icon = "" if rule["enabled"] else ""
                st.markdown(f"{status_icon} **{rule['name']}**")
                st.caption(f"Type: {rule['type']}")

            with col2:
                st.caption(f"Target: {rule['target']}")
                channels_str = ", ".join(rule["channels"])
                st.caption(f"Channels: {channels_str}")

            with col3:
                severity_colors = {"Critical": "red", "Warning": "orange", "Info": "blue"}
                st.markdown(f":{severity_colors[rule['severity']]}[{rule['severity']}]")
                st.caption(f"Triggered: {rule['triggered_count']} times")

            with col4:
                st.toggle("", value=rule["enabled"], key=f"rule_enable_{rule['id']}")

            with col5:
                col_a, col_b = st.columns(2)
                with col_a:
                    st.button("", key=f"edit_{rule['id']}", help="Edit")
                with col_b:
                    st.button("", key=f"test_{rule['id']}", help="Test")

            st.markdown("---")


def _show_notification_channels():
    """Show notification channels configuration."""
    st.subheader("Notification Channels")

    # Add new channel
    with st.expander("Add New Channel", expanded=False):
        channel_type = st.selectbox(
            "Channel Type",
            ["Email", "Slack", "PagerDuty", "Webhook", "SMS", "Microsoft Teams"],
        )

        if channel_type == "Email":
            st.text_input("SMTP Server", placeholder="smtp.gmail.com")
            col1, col2 = st.columns(2)
            with col1:
                st.number_input("Port", value=587)
            with col2:
                st.checkbox("Use TLS", value=True)
            st.text_input("Username", placeholder="alerts@company.com")
            st.text_input("Password", type="password")
            st.text_input("Default Recipients", placeholder="team@company.com, alerts@company.com")

        elif channel_type == "Slack":
            st.text_input("Webhook URL", placeholder="https://hooks.slack.com/services/...")
            st.text_input("Default Channel", placeholder="#alerts")
            st.checkbox("Include @channel for critical alerts", value=True)

        elif channel_type == "PagerDuty":
            st.text_input("Integration Key", type="password")
            st.selectbox("Default Severity", ["critical", "error", "warning", "info"])

        elif channel_type == "Webhook":
            st.text_input("Webhook URL", placeholder="https://api.example.com/webhook")
            st.selectbox("Method", ["POST", "PUT"])
            st.text_area("Custom Headers (JSON)", placeholder='{"Authorization": "Bearer ..."}')

        elif channel_type == "SMS":
            st.selectbox("Provider", ["Twilio", "AWS SNS", "Nexmo"])
            st.text_input("Account SID / Access Key")
            st.text_input("Auth Token / Secret Key", type="password")
            st.text_input("From Number", placeholder="+1234567890")

        elif channel_type == "Microsoft Teams":
            st.text_input("Webhook URL", placeholder="https://outlook.office.com/webhook/...")

        channel_name = st.text_input("Channel Name", placeholder="Production Slack")

        if st.button("Add Channel", type="primary"):
            st.success(f"Channel '{channel_name}' added successfully!")

    st.markdown("---")

    # Existing channels
    channels = [
        {
            "id": "ch-001",
            "name": "Production Slack",
            "type": "Slack",
            "target": "#etl-alerts",
            "status": "connected",
            "last_used": "10 min ago",
            "icon": "",
        },
        {
            "id": "ch-002",
            "name": "Team Email",
            "type": "Email",
            "target": "team@company.com",
            "status": "connected",
            "last_used": "1 hour ago",
            "icon": "",
        },
        {
            "id": "ch-003",
            "name": "On-Call PagerDuty",
            "type": "PagerDuty",
            "target": "ETL On-Call",
            "status": "connected",
            "last_used": "2 days ago",
            "icon": "",
        },
        {
            "id": "ch-004",
            "name": "Monitoring Webhook",
            "type": "Webhook",
            "target": "https://monitor.example.com/webhook",
            "status": "error",
            "last_used": "1 week ago",
            "icon": "",
        },
        {
            "id": "ch-005",
            "name": "SMS Alerts",
            "type": "SMS",
            "target": "+1234567890",
            "status": "connected",
            "last_used": "5 days ago",
            "icon": "",
        },
    ]

    for channel in channels:
        with st.container():
            col1, col2, col3, col4 = st.columns([3, 2, 2, 1])

            with col1:
                st.markdown(f"{channel['icon']} **{channel['name']}**")
                st.caption(f"Type: {channel['type']}")

            with col2:
                st.caption(f"Target: {channel['target']}")
                st.caption(f"Last used: {channel['last_used']}")

            with col3:
                status_color = "green" if channel["status"] == "connected" else "red"
                st.markdown(f":{status_color}[{channel['status'].upper()}]")

            with col4:
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    if st.button("", key=f"test_ch_{channel['id']}", help="Test"):
                        st.info("Sending test notification...")
                with col_b:
                    st.button("", key=f"edit_ch_{channel['id']}", help="Edit")
                with col_c:
                    st.button("", key=f"delete_ch_{channel['id']}", help="Delete")

            st.markdown("---")


def _show_alert_history():
    """Show alert history."""
    st.subheader("Alert History")

    # Filters
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        h_severity = st.selectbox("Severity", ["All", "Critical", "Warning", "Info"], key="hist_sev")

    with col2:
        h_status = st.selectbox("Status", ["All", "Triggered", "Acknowledged", "Resolved"], key="hist_stat")

    with col3:
        h_source = st.selectbox("Source", ["All", "Pipeline Monitor", "Quality Monitor", "Storage Monitor"], key="hist_src")

    with col4:
        h_range = st.selectbox("Time Range", ["Last 24h", "Last 7 days", "Last 30 days"], key="hist_range")

    st.markdown("---")

    # Alert history
    history = [
        {
            "id": "hist-001",
            "title": "Pipeline Failure: customer_etl_daily",
            "severity": "Critical",
            "source": "Pipeline Monitor",
            "triggered": "2024-01-15 08:30",
            "resolved": "2024-01-15 09:15",
            "duration": "45 min",
            "status": "resolved",
            "notifications": 3,
        },
        {
            "id": "hist-002",
            "title": "Data Quality Threshold Breached",
            "severity": "Warning",
            "source": "Quality Monitor",
            "triggered": "2024-01-15 06:00",
            "resolved": "2024-01-15 07:30",
            "duration": "1h 30min",
            "status": "resolved",
            "notifications": 2,
        },
        {
            "id": "hist-003",
            "title": "Job Queue Backlog",
            "severity": "Warning",
            "source": "Job Monitor",
            "triggered": "2024-01-14 22:00",
            "resolved": "2024-01-14 22:30",
            "duration": "30 min",
            "status": "resolved",
            "notifications": 1,
        },
        {
            "id": "hist-004",
            "title": "Storage Quota Warning",
            "severity": "Warning",
            "source": "Storage Monitor",
            "triggered": "2024-01-14 18:00",
            "resolved": None,
            "duration": "Ongoing",
            "status": "acknowledged",
            "notifications": 2,
        },
        {
            "id": "hist-005",
            "title": "Pipeline Failure: orders_sync",
            "severity": "Critical",
            "source": "Pipeline Monitor",
            "triggered": "2024-01-14 15:00",
            "resolved": "2024-01-14 15:20",
            "duration": "20 min",
            "status": "resolved",
            "notifications": 4,
        },
    ]

    for alert in history:
        with st.container():
            col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 1, 1])

            with col1:
                st.markdown(f"**{alert['title']}**")
                st.caption(f"Source: {alert['source']}")

            with col2:
                st.caption(f"Triggered: {alert['triggered']}")
                if alert["resolved"]:
                    st.caption(f"Resolved: {alert['resolved']}")

            with col3:
                severity_colors = {"Critical": "red", "Warning": "orange", "Info": "blue"}
                st.markdown(f":{severity_colors[alert['severity']]}[{alert['severity']}]")
                st.caption(f"Duration: {alert['duration']}")

            with col4:
                status_colors = {"resolved": "green", "acknowledged": "orange", "triggered": "red"}
                st.markdown(f":{status_colors[alert['status']]}[{alert['status'].upper()}]")

            with col5:
                st.caption(f" {alert['notifications']} sent")
                st.button("Details", key=f"hist_details_{alert['id']}")

            st.markdown("---")

    # Pagination
    col1, col2, col3 = st.columns([2, 1, 2])
    with col2:
        st.markdown("Page 1 of 5")

    # Export options
    st.markdown("---")
    col1, col2, col3 = st.columns([2, 1, 2])
    with col2:
        st.download_button(
            "Export History",
            data="alert_history_data",
            file_name=f"alert_history_{datetime.now().strftime('%Y%m%d')}.csv",
        )

