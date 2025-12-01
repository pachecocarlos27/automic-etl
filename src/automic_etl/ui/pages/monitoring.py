"""Monitoring Page for Automic ETL UI."""

from __future__ import annotations

import streamlit as st
from datetime import datetime, timedelta

from automic_etl.db.pipeline_service import get_pipeline_service
from automic_etl.db.data_service import get_data_service


def show_monitoring_page():
    """Display the monitoring page with Material Design."""
    st.markdown("""
    <div style="margin-bottom: 2rem;">
        <h1 style="font-size: 1.75rem; font-weight: 700; color: #212121; margin: 0 0 0.5rem; letter-spacing: -0.03em; font-family: 'Inter', sans-serif;">Monitoring</h1>
        <p style="font-size: 1rem; color: #757575; margin: 0; font-family: 'Inter', sans-serif;">Pipeline runs, jobs, and system health</p>
    </div>
    """, unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs([
        "Dashboard",
        "Pipeline Runs",
        "Data Tables",
    ])

    with tab1:
        show_monitoring_dashboard()

    with tab2:
        show_pipeline_runs()

    with tab3:
        show_data_tables_overview()


def show_monitoring_dashboard():
    """Show monitoring dashboard."""
    st.subheader("System Health")

    pipeline_service = get_pipeline_service()
    data_service = get_data_service()

    pipelines = pipeline_service.list_pipelines()
    runs = pipeline_service.get_all_runs(limit=100)
    tables = data_service.list_tables()

    active_pipelines = len([p for p in pipelines if p.status == "active"])
    running_pipelines = len([p for p in pipelines if p.status == "running"])
    failed_runs = len([r for r in runs if r.status == "failed"])
    completed_runs = len([r for r in runs if r.status == "completed"])

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Total Pipelines",
            value=str(len(pipelines)),
            delta=f"{active_pipelines} active",
        )

    with col2:
        st.metric(
            label="Pipeline Runs",
            value=str(len(runs)),
            delta=f"{running_pipelines} running",
        )

    with col3:
        st.metric(
            label="Completed",
            value=str(completed_runs),
            delta="runs",
        )

    with col4:
        st.metric(
            label="Failed",
            value=str(failed_runs),
            delta="runs",
            delta_color="inverse" if failed_runs > 0 else "off",
        )

    st.markdown("---")

    st.subheader("Medallion Layer Statistics")

    bronze_tables = [t for t in tables if t.layer == "bronze"]
    silver_tables = [t for t in tables if t.layer == "silver"]
    gold_tables = [t for t in tables if t.layer == "gold"]

    bronze_size = sum(t.size_bytes or 0 for t in bronze_tables)
    silver_size = sum(t.size_bytes or 0 for t in silver_tables)
    gold_size = sum(t.size_bytes or 0 for t in gold_tables)

    bronze_rows = sum(t.row_count or 0 for t in bronze_tables)
    silver_rows = sum(t.row_count or 0 for t in silver_tables)
    gold_rows = sum(t.row_count or 0 for t in gold_tables)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("**Bronze Layer**")
        st.metric("Tables", len(bronze_tables))
        st.metric("Rows", f"{bronze_rows:,}")
        size_display = f"{bronze_size / 1024 / 1024:.2f} MB" if bronze_size > 0 else "0 KB"
        st.metric("Size", size_display)

    with col2:
        st.markdown("**Silver Layer**")
        st.metric("Tables", len(silver_tables))
        st.metric("Rows", f"{silver_rows:,}")
        size_display = f"{silver_size / 1024 / 1024:.2f} MB" if silver_size > 0 else "0 KB"
        st.metric("Size", size_display)

    with col3:
        st.markdown("**Gold Layer**")
        st.metric("Tables", len(gold_tables))
        st.metric("Rows", f"{gold_rows:,}")
        size_display = f"{gold_size / 1024 / 1024:.2f} MB" if gold_size > 0 else "0 KB"
        st.metric("Size", size_display)

    st.markdown("---")

    st.subheader("Recent Activity")

    recent_runs = runs[:5]
    if not recent_runs:
        st.info("No recent pipeline runs.")
    else:
        for run in recent_runs:
            status_color = {
                "completed": "green",
                "running": "blue",
                "failed": "red",
            }.get(run.status, "gray")

            col1, col2, col3, col4 = st.columns([1, 2, 2, 1])
            with col1:
                st.markdown(f"**:{status_color}[{run.status.upper()}]**")
            with col2:
                st.caption(f"Pipeline: {run.pipeline_id[:8]}...")
            with col3:
                if run.started_at:
                    st.caption(f"Started: {run.started_at.strftime('%Y-%m-%d %H:%M')}")
            with col4:
                if run.records_processed:
                    st.caption(f"{run.records_processed} records")


def show_pipeline_runs():
    """Show pipeline runs."""
    st.subheader("Pipeline Runs")

    pipeline_service = get_pipeline_service()
    runs = pipeline_service.get_all_runs(limit=50)

    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        search = st.text_input("Search", placeholder="Filter by pipeline ID...")
    with col2:
        status_filter = st.selectbox("Status", ["All", "completed", "running", "failed"])
    with col3:
        if st.button("Refresh"):
            st.rerun()

    if status_filter != "All":
        runs = [r for r in runs if r.status == status_filter]

    if search:
        runs = [r for r in runs if search.lower() in r.pipeline_id.lower()]

    if not runs:
        st.info("No pipeline runs found.")
        return

    st.markdown(f"**{len(runs)} run(s) found**")
    st.markdown("---")

    for run in runs:
        status_color = {
            "completed": "green",
            "running": "blue",
            "failed": "red",
            "pending": "gray",
        }.get(run.status, "gray")

        with st.expander(f"**{run.status.upper()}** - Run: {run.id[:8]}..."):
            col1, col2 = st.columns([2, 1])

            with col1:
                st.markdown(f"**Pipeline ID:** {run.pipeline_id[:16]}...")
                st.markdown(f"**Status:** :{status_color}[{run.status.upper()}]")
                if run.started_at:
                    st.markdown(f"**Started:** {run.started_at.strftime('%Y-%m-%d %H:%M:%S')}")
                if run.completed_at:
                    st.markdown(f"**Completed:** {run.completed_at.strftime('%Y-%m-%d %H:%M:%S')}")
                if run.duration_seconds:
                    st.markdown(f"**Duration:** {run.duration_seconds:.1f} seconds")
                st.markdown(f"**Records Processed:** {run.records_processed or 0:,}")

            with col2:
                if run.error_message:
                    st.error(f"**Error:** {run.error_message}")

                if run.logs:
                    st.markdown("**Logs:**")
                    for log in (run.logs or [])[:5]:
                        st.text(log)


def show_data_tables_overview():
    """Show data tables overview."""
    st.subheader("Data Tables Overview")

    data_service = get_data_service()
    tables = data_service.list_tables()

    col1, col2 = st.columns([2, 1])
    with col1:
        search = st.text_input("Search tables", placeholder="Filter by name...", key="mon_search")
    with col2:
        layer_filter = st.selectbox("Layer", ["All", "bronze", "silver", "gold"], key="mon_layer")

    if layer_filter != "All":
        tables = [t for t in tables if t.layer == layer_filter]

    if search:
        tables = [t for t in tables if search.lower() in t.name.lower()]

    if not tables:
        st.info("No data tables found.")
        return

    st.markdown(f"**{len(tables)} table(s) found**")

    for table in tables:
        with st.container():
            col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 1, 1])

            col1.markdown(f"**{table.name}**")
            col2.markdown(f"{table.layer}")
            col3.markdown(f"{table.row_count:,} rows")

            size_kb = (table.size_bytes or 0) / 1024
            if size_kb > 1024:
                col4.markdown(f"{size_kb/1024:.1f} MB")
            else:
                col4.markdown(f"{size_kb:.1f} KB")

            col5.markdown(table.updated_at.strftime("%m/%d %H:%M"))

            st.markdown("---")


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

