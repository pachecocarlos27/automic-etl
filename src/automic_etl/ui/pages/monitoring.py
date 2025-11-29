"""Monitoring Page for Automic ETL UI."""

from __future__ import annotations

import streamlit as st
from datetime import datetime, timedelta


def show_monitoring_page():
    """Display the monitoring page."""
    st.title("📈 Monitoring & Jobs")
    st.markdown("Monitor pipeline runs, track jobs, and view system health.")

    # Tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Dashboard",
        "🔄 Pipeline Runs",
        "📋 Job Queue",
        "🔔 Alerts",
    ])

    with tab1:
        show_monitoring_dashboard()

    with tab2:
        show_pipeline_runs()

    with tab3:
        show_job_queue()

    with tab4:
        show_alerts()


def show_monitoring_dashboard():
    """Show monitoring dashboard."""
    st.subheader("System Health")

    # Overall status
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="System Status",
            value="Healthy",
            delta="100% uptime",
        )

    with col2:
        st.metric(
            label="Active Pipelines",
            value="3",
            delta="2 running",
        )

    with col3:
        st.metric(
            label="Jobs Today",
            value="47",
            delta="+12 vs yesterday",
        )

    with col4:
        st.metric(
            label="Errors (24h)",
            value="2",
            delta="-5 vs yesterday",
            delta_color="inverse",
        )

    st.markdown("---")

    # Resource utilization
    st.subheader("Resource Utilization")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("**CPU Usage**")
        st.progress(0.45)
        st.caption("45% - Normal")

    with col2:
        st.markdown("**Memory Usage**")
        st.progress(0.62)
        st.caption("62% - Normal")

    with col3:
        st.markdown("**Storage Usage**")
        st.progress(0.78)
        st.caption("78% - Warning")

    st.markdown("---")

    # Performance metrics
    st.subheader("Performance Metrics (Last 24h)")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Jobs Processed Over Time**")
        # Mock chart data
        chart_data = {
            "Jobs": [5, 8, 12, 15, 10, 8, 6, 4, 3, 5, 8, 12, 18, 22, 15, 12, 10, 8, 6, 5, 7, 10, 14, 12],
        }
        st.line_chart(chart_data)

    with col2:
        st.markdown("**Data Processed (GB)**")
        chart_data = {
            "GB": [0.5, 0.8, 1.2, 1.8, 1.5, 1.2, 0.8, 0.5, 0.3, 0.6, 1.0, 1.5, 2.2, 2.8, 2.0, 1.5, 1.2, 0.9, 0.7, 0.5, 0.8, 1.2, 1.8, 1.5],
        }
        st.line_chart(chart_data)

    st.markdown("---")

    # Layer statistics
    st.subheader("Medallion Layer Statistics")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("##### 🥉 Bronze Layer")
        st.metric("Tables", "12")
        st.metric("Total Size", "4.2 GB")
        st.metric("Records Ingested (24h)", "125,432")

    with col2:
        st.markdown("##### 🥈 Silver Layer")
        st.metric("Tables", "8")
        st.metric("Total Size", "2.8 GB")
        st.metric("Records Processed (24h)", "98,234")

    with col3:
        st.markdown("##### 🥇 Gold Layer")
        st.metric("Tables", "5")
        st.metric("Total Size", "450 MB")
        st.metric("Aggregations (24h)", "34")


def show_pipeline_runs():
    """Show pipeline runs."""
    st.subheader("Pipeline Runs")

    # Filters
    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    with col1:
        search = st.text_input("🔍 Search pipelines", placeholder="Search by name...")
    with col2:
        status_filter = st.selectbox("Status", ["All", "Running", "Success", "Failed", "Cancelled"])
    with col3:
        time_filter = st.selectbox("Time", ["Last Hour", "Today", "Last 7 days", "All"])
    with col4:
        if st.button("🔄 Refresh"):
            st.rerun()

    st.markdown("---")

    # Pipeline runs
    runs = [
        {
            "id": "run_001",
            "pipeline": "daily_customer_etl",
            "status": "🟢 Running",
            "progress": 65,
            "started": "10 min ago",
            "duration": "10m 23s",
            "records": "45,230 / 125,432",
        },
        {
            "id": "run_002",
            "pipeline": "incremental_orders_sync",
            "status": "🟢 Running",
            "progress": 82,
            "started": "5 min ago",
            "duration": "5m 12s",
            "records": "8,234 / 10,000",
        },
        {
            "id": "run_003",
            "pipeline": "weekly_sales_aggregation",
            "status": "✅ Success",
            "progress": 100,
            "started": "1 hour ago",
            "duration": "12m 45s",
            "records": "1,234,567",
        },
        {
            "id": "run_004",
            "pipeline": "document_processor",
            "status": "❌ Failed",
            "progress": 45,
            "started": "2 hours ago",
            "duration": "8m 30s",
            "records": "234 / 500",
        },
        {
            "id": "run_005",
            "pipeline": "gold_metrics_refresh",
            "status": "✅ Success",
            "progress": 100,
            "started": "3 hours ago",
            "duration": "3m 15s",
            "records": "15 aggregations",
        },
    ]

    for run in runs:
        with st.expander(f"{run['status'][:2]} **{run['pipeline']}** - {run['id']}"):
            col1, col2, col3 = st.columns([2, 1, 1])

            with col1:
                st.markdown(f"**Pipeline:** {run['pipeline']}")
                st.markdown(f"**Status:** {run['status']}")
                st.markdown(f"**Started:** {run['started']}")
                st.markdown(f"**Duration:** {run['duration']}")
                st.markdown(f"**Records:** {run['records']}")

                if run['progress'] < 100 and "Running" in run['status']:
                    st.progress(run['progress'] / 100)
                    st.caption(f"{run['progress']}% complete")

            with col2:
                st.markdown("**Stages:**")
                if "Success" in run['status'] or run['progress'] == 100:
                    st.markdown("✅ Extract")
                    st.markdown("✅ Transform")
                    st.markdown("✅ Load")
                    st.markdown("✅ Validate")
                elif "Running" in run['status']:
                    st.markdown("✅ Extract")
                    st.markdown("✅ Transform")
                    st.markdown("🔄 Load")
                    st.markdown("⏳ Validate")
                else:
                    st.markdown("✅ Extract")
                    st.markdown("❌ Transform")
                    st.markdown("⏳ Load")
                    st.markdown("⏳ Validate")

            with col3:
                st.markdown("**Actions:**")
                if "Running" in run['status']:
                    if st.button("⏸️ Pause", key=f"pause_{run['id']}"):
                        st.info("Pausing pipeline...")
                    if st.button("🛑 Stop", key=f"stop_{run['id']}"):
                        st.warning("Stopping pipeline...")
                else:
                    if st.button("🔄 Re-run", key=f"rerun_{run['id']}"):
                        st.info("Starting new run...")

                if st.button("📋 View Logs", key=f"logs_{run['id']}"):
                    st.code("""
[2024-01-15 10:23:45] INFO: Starting pipeline run
[2024-01-15 10:23:46] INFO: Extract stage started
[2024-01-15 10:25:12] INFO: Extract stage completed - 45,230 records
[2024-01-15 10:25:13] INFO: Transform stage started
[2024-01-15 10:28:45] INFO: Transform stage completed
[2024-01-15 10:28:46] INFO: Load stage started...
                    """)


def show_job_queue():
    """Show job queue."""
    st.subheader("Job Queue")

    # Queue stats
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Queued", "5")
    col2.metric("Running", "2")
    col3.metric("Completed (1h)", "12")
    col4.metric("Failed (1h)", "1")

    st.markdown("---")

    # Queue management
    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("⏸️ Pause Queue"):
            st.warning("Queue paused!")
        if st.button("▶️ Resume Queue"):
            st.success("Queue resumed!")

    st.markdown("---")

    # Job queue
    st.markdown("**Pending Jobs**")

    jobs = [
        {"id": "job_001", "type": "Ingestion", "source": "customers.csv", "priority": "High", "queued": "2 min ago"},
        {"id": "job_002", "type": "Transform", "source": "bronze.orders", "priority": "Normal", "queued": "5 min ago"},
        {"id": "job_003", "type": "Aggregation", "source": "silver.transactions", "priority": "Normal", "queued": "8 min ago"},
        {"id": "job_004", "type": "Entity Extraction", "source": "documents/", "priority": "Low", "queued": "15 min ago"},
        {"id": "job_005", "type": "Data Quality", "source": "silver.customers", "priority": "Normal", "queued": "20 min ago"},
    ]

    for job in jobs:
        col1, col2, col3, col4, col5, col6 = st.columns([1, 2, 2, 1, 1, 1])
        col1.write(job['id'])
        col2.write(job['type'])
        col3.write(job['source'])
        col4.write(job['priority'])
        col5.write(job['queued'])

        with col6:
            if st.button("🗑️", key=f"cancel_{job['id']}"):
                st.info(f"Cancelled {job['id']}")

    st.markdown("---")

    # Add new job
    st.markdown("**Schedule New Job**")

    col1, col2, col3 = st.columns(3)

    with col1:
        job_type = st.selectbox(
            "Job Type",
            ["Ingestion", "Transform", "Aggregation", "Data Quality", "Entity Extraction"],
        )

    with col2:
        source = st.text_input("Source", placeholder="Table or file path")

    with col3:
        priority = st.selectbox("Priority", ["Low", "Normal", "High"])

    if st.button("➕ Add to Queue", type="primary"):
        st.success("Job added to queue!")


def show_alerts():
    """Show alerts and notifications."""
    st.subheader("Alerts & Notifications")

    # Alert configuration
    with st.expander("⚙️ Configure Alerts", expanded=False):
        st.markdown("**Alert Channels**")
        col1, col2 = st.columns(2)

        with col1:
            st.checkbox("Email Notifications", value=True)
            st.text_input("Email", value="admin@company.com")

        with col2:
            st.checkbox("Slack Notifications", value=True)
            st.text_input("Slack Channel", value="#etl-alerts")

        st.markdown("---")
        st.markdown("**Alert Rules**")

        st.checkbox("Pipeline failures", value=True)
        st.checkbox("Data quality violations", value=True)
        st.checkbox("High latency (>10 min)", value=True)
        st.checkbox("Storage threshold (>80%)", value=True)
        st.checkbox("LLM rate limit warnings", value=False)

        if st.button("💾 Save Configuration"):
            st.success("Alert configuration saved!")

    st.markdown("---")

    # Active alerts
    st.markdown("**Active Alerts**")

    alerts = [
        {
            "severity": "🔴",
            "title": "Pipeline Failed: document_processor",
            "message": "Error during transform stage: Invalid JSON format in row 234",
            "time": "2 hours ago",
            "acknowledged": False,
        },
        {
            "severity": "🟡",
            "title": "Storage Warning: Silver layer at 78%",
            "message": "Consider archiving old data or expanding storage",
            "time": "1 day ago",
            "acknowledged": False,
        },
        {
            "severity": "🟡",
            "title": "Data Quality: 45 duplicate emails detected",
            "message": "silver.customers has duplicate email addresses",
            "time": "2 days ago",
            "acknowledged": True,
        },
    ]

    for alert in alerts:
        with st.expander(f"{alert['severity']} {alert['title']}"):
            st.markdown(f"**Message:** {alert['message']}")
            st.markdown(f"**Time:** {alert['time']}")
            st.markdown(f"**Status:** {'Acknowledged' if alert['acknowledged'] else 'New'}")

            col1, col2, col3 = st.columns(3)
            with col1:
                if not alert['acknowledged']:
                    if st.button("✅ Acknowledge", key=f"ack_{alert['title'][:10]}"):
                        st.success("Alert acknowledged!")
            with col2:
                if st.button("🔍 Investigate", key=f"inv_{alert['title'][:10]}"):
                    st.info("Opening investigation...")
            with col3:
                if st.button("🗑️ Dismiss", key=f"dis_{alert['title'][:10]}"):
                    st.info("Alert dismissed!")

    st.markdown("---")

    # Alert history
    st.markdown("**Alert History (Last 7 days)**")

    history_data = {
        "Day": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
        "Errors": [2, 1, 0, 3, 1, 0, 2],
        "Warnings": [5, 3, 4, 6, 2, 1, 3],
    }

    st.bar_chart(history_data, x="Day")
