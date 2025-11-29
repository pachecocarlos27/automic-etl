"""Pipeline Builder Page for Automic ETL UI."""

from __future__ import annotations

import streamlit as st
import json


def show_pipeline_builder_page():
    """Display the pipeline builder page."""
    st.title("🔧 Pipeline Builder")
    st.markdown("Create and manage ETL pipelines with a visual interface.")

    # Tabs for different views
    tab1, tab2, tab3 = st.tabs(["📝 Create Pipeline", "📋 My Pipelines", "🔄 SCD2 Manager"])

    with tab1:
        show_create_pipeline_section()

    with tab2:
        show_pipelines_list()

    with tab3:
        show_scd2_manager()


def show_create_pipeline_section():
    """Show the pipeline creation interface."""
    st.subheader("Create New Pipeline")

    # Pipeline metadata
    col1, col2 = st.columns(2)
    with col1:
        pipeline_name = st.text_input("Pipeline Name", placeholder="my_etl_pipeline")
        description = st.text_area("Description", placeholder="Describe what this pipeline does...")

    with col2:
        schedule = st.selectbox(
            "Schedule",
            ["Manual", "Every Hour", "Daily", "Weekly", "Custom Cron"],
        )
        if schedule == "Custom Cron":
            cron_expr = st.text_input("Cron Expression", value="0 0 * * *")

        tags = st.multiselect(
            "Tags",
            ["production", "development", "staging", "critical", "experimental"],
        )

    st.markdown("---")

    # Pipeline stages
    st.subheader("Pipeline Stages")

    # Initialize pipeline stages in session state
    if "pipeline_stages" not in st.session_state:
        st.session_state["pipeline_stages"] = []

    # Add stage button
    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        if st.button("➕ Add Stage"):
            st.session_state["pipeline_stages"].append({
                "type": "transform",
                "name": f"Stage {len(st.session_state['pipeline_stages']) + 1}",
                "config": {},
            })

    with col2:
        if st.button("🤖 AI Suggest"):
            st.info("AI will suggest pipeline stages based on your data...")

    # Display stages
    for i, stage in enumerate(st.session_state["pipeline_stages"]):
        with st.expander(f"Stage {i + 1}: {stage['name']}", expanded=True):
            col1, col2, col3 = st.columns([2, 2, 1])

            with col1:
                stage["name"] = st.text_input(
                    "Stage Name",
                    value=stage["name"],
                    key=f"stage_name_{i}",
                )
                stage["type"] = st.selectbox(
                    "Stage Type",
                    ["extract", "transform", "load", "validate", "enrich"],
                    key=f"stage_type_{i}",
                )

            with col2:
                if stage["type"] == "extract":
                    show_extract_config(i)
                elif stage["type"] == "transform":
                    show_transform_config(i)
                elif stage["type"] == "load":
                    show_load_config(i)
                elif stage["type"] == "validate":
                    show_validate_config(i)
                elif stage["type"] == "enrich":
                    show_enrich_config(i)

            with col3:
                st.markdown("**Actions**")
                if st.button("🗑️", key=f"delete_stage_{i}"):
                    st.session_state["pipeline_stages"].pop(i)
                    st.rerun()
                if i > 0:
                    if st.button("⬆️", key=f"move_up_{i}"):
                        st.session_state["pipeline_stages"][i], st.session_state["pipeline_stages"][i-1] = \
                            st.session_state["pipeline_stages"][i-1], st.session_state["pipeline_stages"][i]
                        st.rerun()

    # Pipeline visualization
    if st.session_state["pipeline_stages"]:
        st.markdown("---")
        st.subheader("Pipeline Flow")
        flow_text = " → ".join([f"**{s['name']}**" for s in st.session_state["pipeline_stages"]])
        st.markdown(f"📥 Source → {flow_text} → 📤 Target")

    st.markdown("---")

    # Save/Run buttons
    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        if st.button("💾 Save Pipeline", type="primary"):
            st.success(f"✅ Pipeline '{pipeline_name}' saved successfully!")
    with col2:
        if st.button("▶️ Run Now"):
            with st.spinner("Running pipeline..."):
                progress = st.progress(0)
                for i in range(100):
                    progress.progress(i + 1)
                st.success("✅ Pipeline completed!")


def show_extract_config(stage_idx: int):
    """Show extract stage configuration."""
    source_type = st.selectbox(
        "Source Type",
        ["bronze_table", "database", "file", "api"],
        key=f"extract_source_{stage_idx}",
    )

    if source_type == "bronze_table":
        st.text_input("Table Name", key=f"extract_table_{stage_idx}")
    elif source_type == "database":
        st.text_input("Connection Name", key=f"extract_conn_{stage_idx}")
        st.text_area("Query", key=f"extract_query_{stage_idx}")


def show_transform_config(stage_idx: int):
    """Show transform stage configuration."""
    transform_type = st.selectbox(
        "Transform Type",
        ["sql", "natural_language", "dedup", "filter", "join", "aggregate"],
        key=f"transform_type_{stage_idx}",
    )

    if transform_type == "sql":
        st.text_area("SQL Transform", key=f"transform_sql_{stage_idx}")
    elif transform_type == "natural_language":
        st.text_area(
            "Describe the transformation",
            placeholder="Remove duplicates, convert dates to standard format, filter out null values",
            key=f"transform_nl_{stage_idx}",
        )
        st.caption("🤖 LLM will generate the transformation code")
    elif transform_type == "dedup":
        st.text_input("Dedup Columns (comma-separated)", key=f"dedup_cols_{stage_idx}")
    elif transform_type == "filter":
        st.text_input("Filter Expression", key=f"filter_expr_{stage_idx}")


def show_load_config(stage_idx: int):
    """Show load stage configuration."""
    target = st.selectbox(
        "Target Layer",
        ["bronze", "silver", "gold"],
        key=f"load_target_{stage_idx}",
    )
    st.text_input("Table Name", key=f"load_table_{stage_idx}")
    st.selectbox(
        "Write Mode",
        ["append", "overwrite", "merge"],
        key=f"load_mode_{stage_idx}",
    )


def show_validate_config(stage_idx: int):
    """Show validation stage configuration."""
    st.multiselect(
        "Validations",
        ["not_null", "unique", "range_check", "schema_match", "custom"],
        key=f"validations_{stage_idx}",
    )
    st.number_input(
        "Fail Threshold (%)",
        min_value=0,
        max_value=100,
        value=5,
        key=f"fail_threshold_{stage_idx}",
    )


def show_enrich_config(stage_idx: int):
    """Show enrichment stage configuration."""
    st.multiselect(
        "Enrichment Type",
        ["entity_extraction", "classification", "sentiment", "geocoding", "lookup"],
        key=f"enrich_type_{stage_idx}",
    )
    st.text_input("Text Column", key=f"enrich_col_{stage_idx}")


def show_pipelines_list():
    """Show list of existing pipelines."""
    st.subheader("My Pipelines")

    # Filter/search
    col1, col2 = st.columns([3, 1])
    with col1:
        search = st.text_input("🔍 Search pipelines", placeholder="Search by name or tag...")
    with col2:
        status_filter = st.selectbox("Status", ["All", "Active", "Paused", "Failed"])

    # Mock pipeline data
    pipelines = [
        {
            "name": "daily_customer_etl",
            "description": "Daily customer data sync from CRM",
            "schedule": "Daily at 2:00 AM",
            "last_run": "2 hours ago",
            "status": "✅ Success",
            "runs": 156,
        },
        {
            "name": "weekly_sales_aggregation",
            "description": "Weekly sales metrics to Gold layer",
            "schedule": "Weekly on Monday",
            "last_run": "3 days ago",
            "status": "✅ Success",
            "runs": 23,
        },
        {
            "name": "realtime_document_processor",
            "description": "Process uploaded documents with LLM",
            "schedule": "Manual",
            "last_run": "1 hour ago",
            "status": "⚠️ Warning",
            "runs": 89,
        },
        {
            "name": "incremental_orders_sync",
            "description": "Incremental order sync from database",
            "schedule": "Every Hour",
            "last_run": "45 min ago",
            "status": "❌ Failed",
            "runs": 2345,
        },
    ]

    for pipeline in pipelines:
        with st.expander(f"{pipeline['status'][:1]} **{pipeline['name']}**"):
            col1, col2, col3 = st.columns([2, 1, 1])

            with col1:
                st.markdown(f"**Description:** {pipeline['description']}")
                st.markdown(f"**Schedule:** {pipeline['schedule']}")
                st.markdown(f"**Last Run:** {pipeline['last_run']}")
                st.markdown(f"**Total Runs:** {pipeline['runs']}")

            with col2:
                st.markdown("**Status**")
                st.markdown(pipeline['status'])

            with col3:
                st.markdown("**Actions**")
                if st.button("▶️ Run", key=f"run_{pipeline['name']}"):
                    st.info(f"Running {pipeline['name']}...")
                if st.button("✏️ Edit", key=f"edit_{pipeline['name']}"):
                    st.session_state["edit_pipeline"] = pipeline['name']
                if st.button("📊 History", key=f"history_{pipeline['name']}"):
                    st.info("Showing run history...")


def show_scd2_manager():
    """Show SCD Type 2 table manager."""
    st.subheader("SCD Type 2 Manager")

    st.markdown("""
    Manage Slowly Changing Dimension Type 2 tables for historical data tracking.
    SCD2 maintains full history by creating new rows when records change.
    """)

    # Create new SCD2 table
    with st.expander("➕ Create SCD2 Table", expanded=False):
        col1, col2 = st.columns(2)

        with col1:
            source_table = st.text_input("Source Table")
            target_table = st.text_input("Target SCD2 Table")
            namespace = st.selectbox("Namespace", ["silver", "gold"])

        with col2:
            business_keys = st.text_input(
                "Business Keys (comma-separated)",
                placeholder="customer_id, product_id",
            )
            tracked_columns = st.text_input(
                "Tracked Columns (leave empty for all)",
                placeholder="name, email, status",
            )

        if st.button("🚀 Create SCD2 Table", type="primary"):
            st.success(f"✅ SCD2 table '{target_table}' created successfully!")

    st.markdown("---")

    # Existing SCD2 tables
    st.subheader("SCD2 Tables")

    scd2_tables = [
        {
            "name": "dim_customers",
            "namespace": "silver",
            "business_keys": ["customer_id"],
            "total_records": 15420,
            "current_records": 5234,
            "versions_avg": 2.9,
        },
        {
            "name": "dim_products",
            "namespace": "silver",
            "business_keys": ["product_id"],
            "total_records": 8901,
            "current_records": 3456,
            "versions_avg": 2.6,
        },
    ]

    for table in scd2_tables:
        with st.expander(f"📋 {table['namespace']}.{table['name']}"):
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Total Records", f"{table['total_records']:,}")
            with col2:
                st.metric("Current Records", f"{table['current_records']:,}")
            with col3:
                st.metric("Avg Versions", f"{table['versions_avg']:.1f}")

            st.markdown(f"**Business Keys:** {', '.join(table['business_keys'])}")

            # Point-in-time query
            st.markdown("---")
            st.markdown("**Point-in-Time Query**")
            col1, col2 = st.columns(2)
            with col1:
                key_value = st.text_input(
                    "Business Key Value",
                    key=f"key_{table['name']}",
                )
            with col2:
                as_of_date = st.date_input(
                    "As of Date",
                    key=f"date_{table['name']}",
                )

            if st.button("🔍 Query", key=f"query_{table['name']}"):
                st.info(f"Querying {table['name']} for key={key_value} as of {as_of_date}")

            # Apply updates
            st.markdown("---")
            if st.button("🔄 Apply Updates", key=f"update_{table['name']}"):
                st.info(f"Applying SCD2 updates to {table['name']}...")
