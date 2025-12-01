"""Jobs & Orchestration Page for Automic ETL UI."""

from __future__ import annotations

import streamlit as st
from datetime import datetime, timedelta
from typing import Any

from automic_etl.db.pipeline_service import get_pipeline_service


def show_jobs_page():
    """Display the jobs and orchestration management page."""
    st.title("Jobs & Orchestration")
    st.markdown("Schedule, monitor, and manage ETL jobs and workflows.")

    tab1, tab2, tab3, tab4 = st.tabs([
        "Job Dashboard",
        "Scheduled Jobs",
        "Workflows",
        "Execution History",
    ])

    with tab1:
        _show_job_dashboard()

    with tab2:
        _show_scheduled_jobs()

    with tab3:
        _show_workflows()

    with tab4:
        _show_execution_history()


def _show_job_dashboard():
    """Show job dashboard overview."""
    st.subheader("Job Overview")

    # Metrics row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Active Jobs",
            value="12",
            delta="2 running",
        )

    with col2:
        st.metric(
            label="Scheduled",
            value="8",
            delta="3 today",
        )

    with col3:
        st.metric(
            label="Completed (24h)",
            value="45",
            delta="+12",
        )

    with col4:
        st.metric(
            label="Failed (24h)",
            value="2",
            delta="-1",
            delta_color="inverse",
        )

    st.markdown("---")

    # Current running jobs
    st.subheader("Currently Running")

    running_jobs = [
        {
            "id": "job-001",
            "name": "customer_etl_daily",
            "pipeline": "customer_processing",
            "started": datetime.now() - timedelta(minutes=15),
            "progress": 65,
            "stage": "silver_transform",
        },
        {
            "id": "job-002",
            "name": "orders_sync",
            "pipeline": "orders_pipeline",
            "started": datetime.now() - timedelta(minutes=5),
            "progress": 25,
            "stage": "bronze_ingest",
        },
    ]

    for job in running_jobs:
        with st.container():
            col1, col2, col3, col4 = st.columns([3, 2, 2, 1])

            with col1:
                st.markdown(f"**{job['name']}**")
                st.caption(f"Pipeline: {job['pipeline']}")

            with col2:
                st.progress(job["progress"] / 100)
                st.caption(f"{job['progress']}% - {job['stage']}")

            with col3:
                duration = datetime.now() - job["started"]
                st.caption(f"Running for {int(duration.total_seconds() / 60)} min")

            with col4:
                if st.button("Cancel", key=f"cancel_{job['id']}"):
                    st.warning(f"Cancelling {job['name']}...")

            st.markdown("---")

    # Job queue
    st.subheader("Job Queue")

    queued_jobs = [
        {"name": "daily_aggregation", "scheduled": "In 15 min", "priority": "High"},
        {"name": "data_quality_check", "scheduled": "In 30 min", "priority": "Medium"},
        {"name": "report_generation", "scheduled": "In 1 hour", "priority": "Low"},
    ]

    for job in queued_jobs:
        col1, col2, col3, col4 = st.columns([3, 2, 2, 1])

        with col1:
            st.markdown(f"**{job['name']}**")

        with col2:
            st.caption(job["scheduled"])

        with col3:
            priority_color = {"High": "red", "Medium": "orange", "Low": "green"}
            st.markdown(f":{priority_color[job['priority']]}[{job['priority']}]")

        with col4:
            if st.button("Run Now", key=f"run_{job['name']}"):
                st.success(f"Starting {job['name']}...")


def _show_scheduled_jobs():
    """Show scheduled jobs management."""
    st.subheader("Scheduled Jobs")

    # Create new schedule
    with st.expander("Create New Schedule", expanded=False):
        col1, col2 = st.columns(2)

        with col1:
            job_name = st.text_input("Job Name", placeholder="my_etl_job")
            pipeline = st.selectbox(
                "Pipeline",
                ["customer_processing", "orders_pipeline", "daily_aggregation", "data_sync"],
            )
            schedule_type = st.selectbox(
                "Schedule Type",
                ["Cron Expression", "Interval", "One-time"],
            )

        with col2:
            if schedule_type == "Cron Expression":
                cron = st.text_input("Cron Expression", placeholder="0 0 * * *")
                st.caption("Format: minute hour day month weekday")
            elif schedule_type == "Interval":
                interval = st.number_input("Interval (minutes)", min_value=1, value=60)
            else:
                run_date = st.date_input("Run Date")
                run_time = st.time_input("Run Time")

            enabled = st.checkbox("Enabled", value=True)

        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("Create Schedule", type="primary"):
                st.success(f"Schedule '{job_name}' created successfully!")

    st.markdown("---")

    # Existing schedules
    schedules = [
        {
            "id": "sched-001",
            "name": "customer_etl_daily",
            "pipeline": "customer_processing",
            "schedule": "0 6 * * *",
            "next_run": "Tomorrow 06:00",
            "last_run": "Today 06:00",
            "status": "success",
            "enabled": True,
        },
        {
            "id": "sched-002",
            "name": "orders_hourly_sync",
            "pipeline": "orders_pipeline",
            "schedule": "0 * * * *",
            "next_run": "In 45 min",
            "last_run": "15 min ago",
            "status": "success",
            "enabled": True,
        },
        {
            "id": "sched-003",
            "name": "weekly_aggregation",
            "pipeline": "aggregation_pipeline",
            "schedule": "0 0 * * 0",
            "next_run": "Sunday 00:00",
            "last_run": "Last Sunday",
            "status": "failed",
            "enabled": True,
        },
        {
            "id": "sched-004",
            "name": "monthly_report",
            "pipeline": "report_pipeline",
            "schedule": "0 0 1 * *",
            "next_run": "1st of month",
            "last_run": "Dec 1",
            "status": "success",
            "enabled": False,
        },
    ]

    for schedule in schedules:
        with st.container():
            col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 2, 1])

            with col1:
                status_icon = "" if schedule["enabled"] else ""
                st.markdown(f"{status_icon} **{schedule['name']}**")
                st.caption(f"Pipeline: {schedule['pipeline']}")

            with col2:
                st.markdown(f"`{schedule['schedule']}`")
                st.caption(f"Next: {schedule['next_run']}")

            with col3:
                status_color = "green" if schedule["status"] == "success" else "red"
                st.markdown(f":{status_color}[{schedule['status'].upper()}]")
                st.caption(f"Last: {schedule['last_run']}")

            with col4:
                enabled = st.toggle(
                    "Enabled",
                    value=schedule["enabled"],
                    key=f"toggle_{schedule['id']}",
                    label_visibility="collapsed",
                )

            with col5:
                col_a, col_b = st.columns(2)
                with col_a:
                    if st.button("", key=f"edit_{schedule['id']}", help="Edit"):
                        st.info(f"Editing {schedule['name']}")
                with col_b:
                    if st.button("", key=f"delete_{schedule['id']}", help="Delete"):
                        st.warning(f"Delete {schedule['name']}?")

            st.markdown("---")


def _show_workflows():
    """Show workflow management."""
    st.subheader("Workflows")

    # Create workflow
    with st.expander("Create New Workflow", expanded=False):
        workflow_name = st.text_input("Workflow Name", placeholder="my_workflow")

        st.markdown("**Pipeline Steps**")
        st.caption("Add pipelines to execute in sequence")

        # Step builder
        if "workflow_steps" not in st.session_state:
            st.session_state.workflow_steps = []

        col1, col2, col3 = st.columns([3, 2, 1])
        with col1:
            new_pipeline = st.selectbox(
                "Add Pipeline",
                ["customer_processing", "orders_pipeline", "aggregation_pipeline", "validation_pipeline"],
                key="new_workflow_step",
            )
        with col2:
            depends_on = st.selectbox(
                "Depends On",
                ["None"] + [f"Step {i+1}" for i in range(len(st.session_state.workflow_steps))],
            )
        with col3:
            st.markdown("")
            st.markdown("")
            if st.button("Add Step"):
                st.session_state.workflow_steps.append({
                    "pipeline": new_pipeline,
                    "depends_on": depends_on,
                })

        # Show current steps
        if st.session_state.workflow_steps:
            for i, step in enumerate(st.session_state.workflow_steps):
                col1, col2, col3 = st.columns([1, 4, 1])
                with col1:
                    st.markdown(f"**Step {i+1}**")
                with col2:
                    st.caption(f"{step['pipeline']} (depends on: {step['depends_on']})")
                with col3:
                    if st.button("", key=f"remove_step_{i}"):
                        st.session_state.workflow_steps.pop(i)
                        st.rerun()

        if st.button("Create Workflow", type="primary"):
            st.success(f"Workflow '{workflow_name}' created!")
            st.session_state.workflow_steps = []

    st.markdown("---")

    # Existing workflows
    workflows = [
        {
            "name": "daily_etl_workflow",
            "steps": ["bronze_ingest", "silver_transform", "gold_aggregate", "quality_check"],
            "status": "idle",
            "last_run": "6 hours ago",
            "duration": "45 min",
        },
        {
            "name": "customer_360_workflow",
            "steps": ["customer_ingest", "customer_dedupe", "customer_enrich", "customer_publish"],
            "status": "running",
            "last_run": "Running",
            "duration": "~30 min",
        },
        {
            "name": "data_quality_workflow",
            "steps": ["validate_bronze", "validate_silver", "generate_report"],
            "status": "idle",
            "last_run": "1 day ago",
            "duration": "15 min",
        },
    ]

    for workflow in workflows:
        with st.container():
            col1, col2, col3 = st.columns([3, 2, 1])

            with col1:
                status_icon = "" if workflow["status"] == "running" else ""
                st.markdown(f"{status_icon} **{workflow['name']}**")

                # Show workflow steps as a flow
                steps_display = " → ".join(workflow["steps"])
                st.caption(steps_display)

            with col2:
                status_color = "orange" if workflow["status"] == "running" else "gray"
                st.markdown(f":{status_color}[{workflow['status'].upper()}]")
                st.caption(f"Last: {workflow['last_run']} | Duration: {workflow['duration']}")

            with col3:
                if workflow["status"] == "running":
                    if st.button("Stop", key=f"stop_{workflow['name']}"):
                        st.warning("Stopping workflow...")
                else:
                    if st.button("Run", key=f"run_{workflow['name']}", type="primary"):
                        st.success("Starting workflow...")

            st.markdown("---")


def _show_execution_history():
    """Show job execution history."""
    st.subheader("Execution History")

    # Filters
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        status_filter = st.selectbox(
            "Status",
            ["All", "Success", "Failed", "Running", "Cancelled"],
        )

    with col2:
        pipeline_filter = st.selectbox(
            "Pipeline",
            ["All", "customer_processing", "orders_pipeline", "aggregation_pipeline"],
        )

    with col3:
        date_range = st.selectbox(
            "Time Range",
            ["Last 24 hours", "Last 7 days", "Last 30 days", "Custom"],
        )

    with col4:
        st.markdown("")
        st.markdown("")
        if st.button("Apply Filters"):
            st.info("Filters applied")

    st.markdown("---")

    # Execution history table
    executions = [
        {
            "id": "exec-001",
            "job": "customer_etl_daily",
            "pipeline": "customer_processing",
            "status": "success",
            "started": "2024-01-15 06:00:00",
            "duration": "12m 34s",
            "rows_processed": "125,432",
        },
        {
            "id": "exec-002",
            "job": "orders_hourly_sync",
            "pipeline": "orders_pipeline",
            "status": "success",
            "started": "2024-01-15 05:00:00",
            "duration": "3m 12s",
            "rows_processed": "8,234",
        },
        {
            "id": "exec-003",
            "job": "weekly_aggregation",
            "pipeline": "aggregation_pipeline",
            "status": "failed",
            "started": "2024-01-14 00:00:00",
            "duration": "5m 45s",
            "rows_processed": "0",
            "error": "Connection timeout to source database",
        },
        {
            "id": "exec-004",
            "job": "data_quality_check",
            "pipeline": "validation_pipeline",
            "status": "success",
            "started": "2024-01-14 23:30:00",
            "duration": "8m 22s",
            "rows_processed": "450,000",
        },
        {
            "id": "exec-005",
            "job": "monthly_report",
            "pipeline": "report_pipeline",
            "status": "cancelled",
            "started": "2024-01-14 22:00:00",
            "duration": "2m 10s",
            "rows_processed": "15,000",
        },
    ]

    for execution in executions:
        with st.container():
            col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 2, 1])

            with col1:
                st.markdown(f"**{execution['job']}**")
                st.caption(f"Pipeline: {execution['pipeline']}")

            with col2:
                status_colors = {
                    "success": "green",
                    "failed": "red",
                    "running": "orange",
                    "cancelled": "gray",
                }
                st.markdown(f":{status_colors[execution['status']]}[{execution['status'].upper()}]")

            with col3:
                st.caption(f"Started: {execution['started']}")
                st.caption(f"Duration: {execution['duration']}")

            with col4:
                st.caption(f"Rows: {execution['rows_processed']}")
                if "error" in execution:
                    st.caption(f":red[Error: {execution['error'][:30]}...]")

            with col5:
                if st.button("Details", key=f"details_{execution['id']}"):
                    st.info(f"Showing details for {execution['id']}")

            st.markdown("---")

    # Pagination
    col1, col2, col3 = st.columns([2, 1, 2])
    with col2:
        st.markdown("Page 1 of 10")

