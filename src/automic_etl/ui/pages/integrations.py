"""Integrations Hub Page for Automic ETL UI."""

from __future__ import annotations

import httpx
import streamlit as st
from datetime import datetime, timedelta
from typing import Any

# API base URL
API_BASE_URL = "http://localhost:8000/api/v1"


def _get_api_client() -> httpx.Client:
    """Get configured HTTP client for API calls."""
    return httpx.Client(base_url=API_BASE_URL, timeout=30.0)


def _get_dbt_models() -> list[dict[str, Any]]:
    """Fetch dbt models from API."""
    try:
        with _get_api_client() as client:
            response = client.get("/integrations/dbt/models")
            if response.status_code == 200:
                return response.json().get("models", [])
            return []
    except Exception:
        return []


def show_integrations_page():
    """Display the integrations hub page."""
    st.title("Integrations Hub")
    st.markdown("Connect and manage third-party platform integrations.")

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Overview",
        "Apache Spark",
        "dbt",
        "Great Expectations",
        "Apache Airflow",
    ])

    with tab1:
        _show_integrations_overview()

    with tab2:
        _show_spark_integration()

    with tab3:
        _show_dbt_integration()

    with tab4:
        _show_great_expectations_integration()

    with tab5:
        _show_airflow_integration()


def _show_integrations_overview():
    """Show integrations overview."""
    st.subheader("Integration Status")

    integrations = [
        {
            "name": "Apache Spark",
            "description": "Distributed data processing",
            "status": "connected",
            "version": "3.5.0",
            "last_sync": "5 min ago",
            "icon": "",
        },
        {
            "name": "dbt",
            "description": "SQL transformations & modeling",
            "status": "connected",
            "version": "1.7.0",
            "last_sync": "10 min ago",
            "icon": "",
        },
        {
            "name": "Great Expectations",
            "description": "Data validation & profiling",
            "status": "connected",
            "version": "0.18.0",
            "last_sync": "15 min ago",
            "icon": "",
        },
        {
            "name": "Apache Airflow",
            "description": "Workflow orchestration",
            "status": "disconnected",
            "version": "-",
            "last_sync": "Never",
            "icon": "",
        },
        {
            "name": "MLflow",
            "description": "ML experiment tracking",
            "status": "not_configured",
            "version": "-",
            "last_sync": "Never",
            "icon": "",
        },
        {
            "name": "OpenMetadata",
            "description": "Data catalog & governance",
            "status": "not_configured",
            "version": "-",
            "last_sync": "Never",
            "icon": "",
        },
    ]

    # Status summary
    connected = sum(1 for i in integrations if i["status"] == "connected")
    total = len(integrations)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Connected", connected, f"of {total} integrations")

    with col2:
        st.metric("Active Jobs", "3", "via integrations")

    with col3:
        st.metric("Data Synced", "2.4 GB", "last 24h")

    st.markdown("---")

    # Integration cards
    col1, col2 = st.columns(2)

    for i, integration in enumerate(integrations):
        with col1 if i % 2 == 0 else col2:
            with st.container():
                status_colors = {
                    "connected": "#10B981",
                    "disconnected": "#EF4444",
                    "not_configured": "#9CA3AF",
                }
                status_labels = {
                    "connected": "Connected",
                    "disconnected": "Disconnected",
                    "not_configured": "Not Configured",
                }

                st.markdown(f"""
                <div style="
                    background: white;
                    border: 1px solid #E2E8F0;
                    border-radius: 12px;
                    padding: 1.25rem;
                    margin-bottom: 1rem;
                    border-left: 4px solid {status_colors[integration['status']]};
                ">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <div style="font-size: 1.25rem; font-weight: 600; color: #0F172A;">
                                {integration['icon']} {integration['name']}
                            </div>
                            <div style="font-size: 0.875rem; color: #64748B; margin-top: 0.25rem;">
                                {integration['description']}
                            </div>
                        </div>
                        <div style="
                            background: {status_colors[integration['status']]}20;
                            color: {status_colors[integration['status']]};
                            padding: 0.25rem 0.75rem;
                            border-radius: 999px;
                            font-size: 0.75rem;
                            font-weight: 600;
                        ">
                            {status_labels[integration['status']]}
                        </div>
                    </div>
                    <div style="
                        display: flex;
                        justify-content: space-between;
                        margin-top: 1rem;
                        font-size: 0.75rem;
                        color: #94A3B8;
                    ">
                        <span>Version: {integration['version']}</span>
                        <span>Last sync: {integration['last_sync']}</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                if integration["status"] == "not_configured":
                    if st.button("Configure", key=f"config_{integration['name']}", type="primary"):
                        st.info(f"Configuring {integration['name']}...")
                elif integration["status"] == "disconnected":
                    if st.button("Reconnect", key=f"reconnect_{integration['name']}"):
                        st.info(f"Reconnecting {integration['name']}...")


def _show_spark_integration():
    """Show Apache Spark integration."""
    st.subheader(" Apache Spark")

    # Connection status
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Status", "Connected")

    with col2:
        st.metric("Cluster Nodes", "5")

    with col3:
        st.metric("Active Jobs", "2")

    with col4:
        st.metric("Memory Used", "12.4 GB")

    st.markdown("---")

    # Configuration
    with st.expander("Spark Configuration", expanded=False):
        col1, col2 = st.columns(2)

        with col1:
            st.text_input("Spark Master URL", value="spark://spark-master:7077")
            st.text_input("Application Name", value="automic-etl")
            st.number_input("Executor Memory (GB)", value=4)
            st.number_input("Executor Cores", value=2)

        with col2:
            st.number_input("Driver Memory (GB)", value=2)
            st.number_input("Num Executors", value=4)
            st.selectbox("Deploy Mode", ["client", "cluster"])
            st.text_area("Extra Spark Config", placeholder="spark.sql.shuffle.partitions=200")

        if st.button("Update Spark Config", type="primary"):
            st.success("Spark configuration updated!")

    # Delta Lake settings
    with st.expander("Delta Lake Integration", expanded=False):
        st.checkbox("Enable Delta Lake support", value=True)
        st.checkbox("Use Delta Lake for all tables", value=True)
        st.text_input("Delta Log Path", value="s3://datalake/delta/_delta_log")
        st.number_input("Vacuum Retention (days)", value=7)

        if st.button("Apply Delta Settings"):
            st.success("Delta Lake settings applied!")

    # Iceberg settings
    with st.expander("Apache Iceberg Integration", expanded=False):
        st.checkbox("Enable Iceberg support", value=True)
        st.text_input("Iceberg Catalog", value="glue_catalog")
        st.text_input("Iceberg Warehouse", value="s3://datalake/iceberg/")
        st.selectbox("Iceberg File Format", ["parquet", "orc", "avro"])

        if st.button("Apply Iceberg Settings"):
            st.success("Iceberg settings applied!")

    st.markdown("---")

    # Running jobs
    st.subheader("Spark Jobs")

    spark_jobs = [
        {
            "id": "spark-001",
            "name": "bronze_to_silver_transform",
            "status": "running",
            "progress": 65,
            "started": "15 min ago",
            "stage": "Stage 3/5",
        },
        {
            "id": "spark-002",
            "name": "daily_aggregation",
            "status": "running",
            "progress": 30,
            "started": "5 min ago",
            "stage": "Stage 1/4",
        },
        {
            "id": "spark-003",
            "name": "customer_dedup",
            "status": "completed",
            "progress": 100,
            "started": "1 hour ago",
            "stage": "Completed",
        },
    ]

    for job in spark_jobs:
        col1, col2, col3, col4 = st.columns([3, 2, 2, 1])

        with col1:
            st.markdown(f"**{job['name']}**")
            st.caption(f"ID: {job['id']}")

        with col2:
            st.progress(job["progress"] / 100)
            st.caption(f"{job['stage']} ({job['progress']}%)")

        with col3:
            status_color = "green" if job["status"] == "completed" else "orange"
            st.markdown(f":{status_color}[{job['status'].upper()}]")
            st.caption(f"Started: {job['started']}")

        with col4:
            if job["status"] == "running":
                st.button("Cancel", key=f"cancel_spark_{job['id']}")
            else:
                st.button("Details", key=f"details_spark_{job['id']}")

        st.markdown("---")


def _show_dbt_integration():
    """Show dbt integration."""
    st.subheader(" dbt (Data Build Tool)")

    # Connection status
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Status", "Connected")

    with col2:
        st.metric("Models", "45")

    with col3:
        st.metric("Tests", "128")

    with col4:
        st.metric("Last Run", "2h ago")

    st.markdown("---")

    # Configuration
    with st.expander("dbt Configuration", expanded=False):
        col1, col2 = st.columns(2)

        with col1:
            st.text_input("Project Path", value="/app/dbt_project")
            st.text_input("Profiles Path", value="/app/.dbt/profiles.yml")
            st.selectbox("Target", ["dev", "staging", "prod"])

        with col2:
            st.text_input("dbt Cloud API Key", type="password")
            st.text_input("dbt Cloud Account ID", placeholder="12345")
            st.checkbox("Use dbt Cloud", value=False)

        if st.button("Update dbt Config", type="primary"):
            st.success("dbt configuration updated!")

    st.markdown("---")

    # dbt Models
    st.subheader("dbt Models")

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("dbt run", type="primary", use_container_width=True):
            st.info("Running dbt models...")

    with col2:
        if st.button("dbt test", use_container_width=True):
            st.info("Running dbt tests...")

    with col3:
        if st.button("dbt docs generate", use_container_width=True):
            st.info("Generating dbt docs...")

    st.markdown("---")

    # Fetch model list from API
    models = _get_dbt_models()

    if not models:
        st.info("No dbt models found. Configure dbt project to get started.")
        return

    for model in models:
        col1, col2, col3, col4 = st.columns([3, 2, 2, 1])

        with col1:
            st.markdown(f"**{model['name']}**")
            st.caption(f"Schema: {model['schema']}")

        with col2:
            st.markdown(f"`{model['materialized']}`")

        with col3:
            status_color = "green" if model["status"] == "success" else "red"
            st.markdown(f":{status_color}[{model['status'].upper()}]")
            st.caption(f"Run time: {model['run_time']}")

        with col4:
            st.button("Run", key=f"run_model_{model['name']}")

        st.markdown("---")


def _show_great_expectations_integration():
    """Show Great Expectations integration."""
    st.subheader(" Great Expectations")

    # Status
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Status", "Connected")

    with col2:
        st.metric("Expectation Suites", "12")

    with col3:
        st.metric("Checkpoints", "8")

    with col4:
        st.metric("Validations (24h)", "156")

    st.markdown("---")

    # Configuration
    with st.expander("GX Configuration", expanded=False):
        st.text_input("Data Context Path", value="/app/great_expectations")
        st.text_input("Data Docs Site", value="file:///app/great_expectations/uncommitted/data_docs/local_site/")
        st.checkbox("Auto-generate Data Docs", value=True)
        st.checkbox("Store validation results", value=True)

        if st.button("Update GX Config", type="primary"):
            st.success("Great Expectations configuration updated!")

    st.markdown("---")

    # Expectation Suites
    st.subheader("Expectation Suites")

    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("Create Suite", type="primary"):
            st.info("Creating new expectation suite...")

    suites = [
        {
            "name": "bronze_customers_suite",
            "datasource": "bronze.raw_customers",
            "expectations": 15,
            "last_run": "30 min ago",
            "status": "passing",
        },
        {
            "name": "bronze_orders_suite",
            "datasource": "bronze.raw_orders",
            "expectations": 22,
            "last_run": "1 hour ago",
            "status": "passing",
        },
        {
            "name": "silver_customers_suite",
            "datasource": "silver.customers",
            "expectations": 28,
            "last_run": "2 hours ago",
            "status": "failing",
        },
        {
            "name": "gold_metrics_suite",
            "datasource": "gold.sales_metrics",
            "expectations": 18,
            "last_run": "3 hours ago",
            "status": "passing",
        },
    ]

    for suite in suites:
        col1, col2, col3, col4, col5 = st.columns([3, 2, 1, 1, 1])

        with col1:
            st.markdown(f"**{suite['name']}**")
            st.caption(f"Datasource: {suite['datasource']}")

        with col2:
            st.caption(f"{suite['expectations']} expectations")
            st.caption(f"Last run: {suite['last_run']}")

        with col3:
            status_color = "green" if suite["status"] == "passing" else "red"
            st.markdown(f":{status_color}[{suite['status'].upper()}]")

        with col4:
            st.button("Run", key=f"run_suite_{suite['name']}")

        with col5:
            st.button("Edit", key=f"edit_suite_{suite['name']}")

        st.markdown("---")

    # Checkpoints
    st.subheader("Checkpoints")

    checkpoints = [
        {"name": "daily_validation", "suites": 4, "schedule": "0 6 * * *", "status": "active"},
        {"name": "pre_pipeline_check", "suites": 2, "schedule": "On pipeline trigger", "status": "active"},
        {"name": "post_load_validation", "suites": 3, "schedule": "After data load", "status": "active"},
    ]

    for cp in checkpoints:
        col1, col2, col3, col4 = st.columns([3, 2, 2, 1])

        with col1:
            st.markdown(f"**{cp['name']}**")

        with col2:
            st.caption(f"{cp['suites']} suites")
            st.caption(f"Schedule: {cp['schedule']}")

        with col3:
            st.markdown(f":green[{cp['status'].upper()}]")

        with col4:
            st.button("Run", key=f"run_cp_{cp['name']}")


def _show_airflow_integration():
    """Show Apache Airflow integration."""
    st.subheader(" Apache Airflow")

    # Status - disconnected
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Status", "Not Connected")

    with col2:
        st.metric("DAGs", "-")

    with col3:
        st.metric("Active Tasks", "-")

    with col4:
        st.metric("Last Sync", "Never")

    st.markdown("---")

    # Configuration
    st.subheader("Connect to Airflow")

    col1, col2 = st.columns(2)

    with col1:
        airflow_url = st.text_input("Airflow Web Server URL", placeholder="http://airflow-webserver:8080")
        api_endpoint = st.text_input("API Endpoint", value="/api/v1")

    with col2:
        auth_type = st.selectbox("Authentication", ["Basic Auth", "API Key", "OAuth"])

        if auth_type == "Basic Auth":
            username = st.text_input("Username", placeholder="admin")
            password = st.text_input("Password", type="password")
        elif auth_type == "API Key":
            api_key = st.text_input("API Key", type="password")

    col1, col2, col3 = st.columns([1, 1, 2])

    with col1:
        if st.button("Test Connection"):
            with st.spinner("Testing connection..."):
                import time
                time.sleep(1)
            st.error("Connection failed: Unable to reach Airflow server")

    with col2:
        if st.button("Connect", type="primary"):
            st.info("Attempting to connect...")

    st.markdown("---")

    # DAG Generation
    st.subheader("DAG Generation")

    st.markdown("""
    Generate Apache Airflow DAGs from your Automic ETL pipelines.
    Once connected, you can:
    - Export pipelines as Airflow DAGs
    - Sync job schedules with Airflow
    - Monitor DAG execution from this dashboard
    """)

    with st.expander("DAG Generation Settings", expanded=False):
        st.text_input("DAG Output Path", value="/opt/airflow/dags/automic_generated/")
        st.selectbox("Default Operator", ["PythonOperator", "BashOperator", "KubernetesPodOperator"])
        st.checkbox("Include error handling", value=True)
        st.checkbox("Add SLA monitoring", value=True)
        st.number_input("Default retries", value=3)

        st.markdown("**Select pipelines to export:**")
        st.checkbox("customer_processing", value=True)
        st.checkbox("orders_pipeline", value=True)
        st.checkbox("aggregation_pipeline", value=False)

        if st.button("Generate DAGs", type="primary"):
            st.warning("Connect to Airflow first to generate DAGs")

