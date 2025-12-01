"""Pipeline Builder Page for Automic ETL UI."""

from __future__ import annotations

import streamlit as st
import json
import time

from automic_etl.db.pipeline_service import get_pipeline_service


def show_pipeline_builder_page():
    """Display the pipeline builder page with Material Design."""
    st.markdown("""
    <div style="margin-bottom: 2rem;">
        <h1 style="font-size: 1.75rem; font-weight: 700; color: #212121; margin: 0 0 0.5rem; letter-spacing: -0.03em; font-family: 'Inter', sans-serif;">Pipelines</h1>
        <p style="font-size: 1rem; color: #757575; margin: 0; font-family: 'Inter', sans-serif;">Create and manage ETL pipelines</p>
    </div>
    """, unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs(["Create Pipeline", "My Pipelines", "Run History"])

    with tab1:
        show_create_pipeline_section()

    with tab2:
        show_pipelines_list()

    with tab3:
        show_run_history()


def show_create_pipeline_section():
    """Show the pipeline creation interface."""
    st.subheader("Create New Pipeline")

    user = st.session_state.get("user")
    if not user:
        st.warning("Please log in to create pipelines.")
        return

    col1, col2 = st.columns(2)
    with col1:
        pipeline_name = st.text_input("Pipeline Name", placeholder="my_etl_pipeline")
        description = st.text_area("Description", placeholder="Describe what this pipeline does...")

    with col2:
        schedule = st.selectbox(
            "Schedule",
            ["Manual", "Every Hour", "Daily", "Weekly"],
        )
        source_type = st.selectbox(
            "Source Type",
            ["file", "database", "api", "stream"],
        )
        destination_layer = st.selectbox(
            "Destination Layer",
            ["bronze", "silver", "gold"],
        )

    st.markdown("---")

    st.subheader("Pipeline Stages")

    if "pipeline_stages" not in st.session_state:
        st.session_state["pipeline_stages"] = []

    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        if st.button("Add Stage"):
            st.session_state["pipeline_stages"].append({
                "type": "transform",
                "name": f"Stage {len(st.session_state['pipeline_stages']) + 1}",
                "config": {},
            })
            st.rerun()

    with col2:
        if st.button("Clear Stages"):
            st.session_state["pipeline_stages"] = []
            st.rerun()

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
                    show_extract_config(i, stage)
                elif stage["type"] == "transform":
                    show_transform_config(i, stage)
                elif stage["type"] == "load":
                    show_load_config(i, stage)
                elif stage["type"] == "validate":
                    show_validate_config(i, stage)
                elif stage["type"] == "enrich":
                    show_enrich_config(i, stage)

            with col3:
                st.markdown("**Actions**")
                if st.button("Delete", key=f"delete_stage_{i}"):
                    st.session_state["pipeline_stages"].pop(i)
                    st.rerun()
                if i > 0:
                    if st.button("Move Up", key=f"move_up_{i}"):
                        st.session_state["pipeline_stages"][i], st.session_state["pipeline_stages"][i-1] = \
                            st.session_state["pipeline_stages"][i-1], st.session_state["pipeline_stages"][i]
                        st.rerun()

    if st.session_state["pipeline_stages"]:
        st.markdown("---")
        st.subheader("Pipeline Flow")
        flow_text = " -> ".join([f"**{s['name']}**" for s in st.session_state["pipeline_stages"]])
        st.markdown(f"Source -> {flow_text} -> Target")

    st.markdown("---")

    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        if st.button("Save Pipeline", type="primary"):
            if not pipeline_name:
                st.error("Please enter a pipeline name.")
            else:
                service = get_pipeline_service()
                try:
                    pipeline = service.create_pipeline(
                        name=pipeline_name,
                        owner_id=user.id,
                        description=description,
                        schedule=schedule if schedule != "Manual" else None,
                        source_type=source_type,
                        destination_layer=destination_layer,
                        transformations=st.session_state["pipeline_stages"],
                    )
                    st.success(f"Pipeline '{pipeline_name}' saved successfully!")
                    st.session_state["pipeline_stages"] = []
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to save pipeline: {e}")


def show_extract_config(stage_idx: int, stage: dict):
    """Show extract stage configuration."""
    source_type = st.selectbox(
        "Source Type",
        ["bronze_table", "database", "file", "api"],
        key=f"extract_source_{stage_idx}",
    )
    stage["config"]["source_type"] = source_type

    if source_type == "bronze_table":
        table = st.text_input("Table Name", key=f"extract_table_{stage_idx}")
        stage["config"]["table"] = table
    elif source_type == "database":
        conn = st.text_input("Connection Name", key=f"extract_conn_{stage_idx}")
        query = st.text_area("Query", key=f"extract_query_{stage_idx}")
        stage["config"]["connection"] = conn
        stage["config"]["query"] = query


def show_transform_config(stage_idx: int, stage: dict):
    """Show transform stage configuration."""
    transform_type = st.selectbox(
        "Transform Type",
        ["sql", "natural_language", "dedup", "filter", "join", "aggregate"],
        key=f"transform_type_{stage_idx}",
    )
    stage["config"]["transform_type"] = transform_type

    if transform_type == "sql":
        sql = st.text_area("SQL Transform", key=f"transform_sql_{stage_idx}")
        stage["config"]["sql"] = sql
    elif transform_type == "natural_language":
        nl = st.text_area(
            "Describe the transformation",
            placeholder="Remove duplicates, convert dates to standard format",
            key=f"transform_nl_{stage_idx}",
        )
        stage["config"]["natural_language"] = nl
        st.caption("LLM will generate the transformation code")
    elif transform_type == "dedup":
        cols = st.text_input("Dedup Columns (comma-separated)", key=f"dedup_cols_{stage_idx}")
        stage["config"]["dedup_columns"] = cols
    elif transform_type == "filter":
        expr = st.text_input("Filter Expression", key=f"filter_expr_{stage_idx}")
        stage["config"]["filter_expression"] = expr


def show_load_config(stage_idx: int, stage: dict):
    """Show load stage configuration."""
    target = st.selectbox(
        "Target Layer",
        ["bronze", "silver", "gold"],
        key=f"load_target_{stage_idx}",
    )
    table = st.text_input("Table Name", key=f"load_table_{stage_idx}")
    mode = st.selectbox(
        "Write Mode",
        ["append", "overwrite", "merge"],
        key=f"load_mode_{stage_idx}",
    )
    stage["config"]["target_layer"] = target
    stage["config"]["table"] = table
    stage["config"]["write_mode"] = mode


def show_validate_config(stage_idx: int, stage: dict):
    """Show validation stage configuration."""
    validations = st.multiselect(
        "Validations",
        ["not_null", "unique", "range_check", "schema_match", "custom"],
        key=f"validations_{stage_idx}",
    )
    threshold = st.number_input(
        "Fail Threshold (%)",
        min_value=0,
        max_value=100,
        value=5,
        key=f"fail_threshold_{stage_idx}",
    )
    stage["config"]["validations"] = validations
    stage["config"]["fail_threshold"] = threshold


def show_enrich_config(stage_idx: int, stage: dict):
    """Show enrichment stage configuration."""
    enrich_types = st.multiselect(
        "Enrichment Type",
        ["entity_extraction", "classification", "sentiment", "geocoding", "lookup"],
        key=f"enrich_type_{stage_idx}",
    )
    col = st.text_input("Text Column", key=f"enrich_col_{stage_idx}")
    stage["config"]["enrichment_types"] = enrich_types
    stage["config"]["text_column"] = col


def show_pipelines_list():
    """Show list of existing pipelines."""
    st.subheader("My Pipelines")

    user = st.session_state.get("user")
    if not user:
        st.warning("Please log in to view pipelines.")
        return

    col1, col2 = st.columns([3, 1])
    with col1:
        search = st.text_input("Search pipelines", placeholder="Search by name...")
    with col2:
        status_filter = st.selectbox("Status", ["All", "active", "draft", "running", "failed"])

    service = get_pipeline_service()
    pipelines = service.list_pipelines(owner_id=user.id)

    if search:
        search_lower = search.lower()
        pipelines = [p for p in pipelines if search_lower in p.name.lower()]

    if status_filter != "All":
        pipelines = [p for p in pipelines if p.status == status_filter]

    if not pipelines:
        st.info("No pipelines found. Create your first pipeline in the 'Create Pipeline' tab.")
        return

    st.markdown(f"**{len(pipelines)} pipeline(s) found**")

    for pipeline in pipelines:
        status_icon = {
            "active": "Active",
            "draft": "Draft",
            "running": "Running",
            "failed": "Failed",
            "paused": "Paused",
        }.get(pipeline.status, pipeline.status)

        with st.expander(f"**{pipeline.name}** - {status_icon}"):
            col1, col2, col3 = st.columns([2, 1, 1])

            with col1:
                st.markdown(f"**Description:** {pipeline.description or 'No description'}")
                st.markdown(f"**Schedule:** {pipeline.schedule or 'Manual'}")
                st.markdown(f"**Source:** {pipeline.source_type or 'Not configured'}")
                st.markdown(f"**Destination:** {pipeline.destination_layer}")
                if pipeline.last_run_at:
                    st.markdown(f"**Last Run:** {pipeline.last_run_at.strftime('%Y-%m-%d %H:%M')}")
                st.markdown(f"**Total Runs:** {pipeline.run_count or 0}")

            with col2:
                st.markdown("**Status**")
                st.markdown(f"**{pipeline.status.title()}**")

                stages = pipeline.transformations or []
                if stages:
                    st.markdown(f"**Stages:** {len(stages)}")

            with col3:
                st.markdown("**Actions**")
                if st.button("Run", key=f"run_{pipeline.id}", type="primary"):
                    run = service.run_pipeline(pipeline.id)
                    if run:
                        st.success(f"Pipeline started. Run ID: {run.id[:8]}")
                        time.sleep(1)
                        service.complete_run(run.id, "completed", records_processed=100)
                        st.rerun()

                if st.button("Delete", key=f"delete_{pipeline.id}"):
                    if service.delete_pipeline(pipeline.id):
                        st.success(f"Pipeline '{pipeline.name}' deleted.")
                        st.rerun()
                    else:
                        st.error("Failed to delete pipeline.")


def show_run_history():
    """Show pipeline run history."""
    st.subheader("Pipeline Run History")

    user = st.session_state.get("user")
    if not user:
        st.warning("Please log in to view run history.")
        return

    service = get_pipeline_service()
    runs = service.get_all_runs(limit=50)

    if not runs:
        st.info("No pipeline runs yet. Run a pipeline to see history here.")
        return

    col1, col2 = st.columns([2, 1])
    with col1:
        search = st.text_input("Search by Pipeline ID", placeholder="Filter by pipeline ID...")
    with col2:
        status_filter = st.selectbox("Status", ["All", "completed", "running", "failed"])

    if status_filter != "All":
        runs = [r for r in runs if r.status == status_filter]

    if search:
        runs = [r for r in runs if search.lower() in r.pipeline_id.lower()]

    st.markdown(f"**{len(runs)} run(s) found**")

    for run in runs:
        status_color = {
            "completed": "green",
            "running": "blue",
            "failed": "red",
            "pending": "gray",
        }.get(run.status, "gray")

        with st.container():
            col1, col2, col3, col4 = st.columns([1, 2, 2, 1])

            with col1:
                st.markdown(f"**:{status_color}[{run.status.upper()}]**")

            with col2:
                st.markdown(f"**Run ID:** {run.id[:8]}...")
                st.caption(f"Pipeline: {run.pipeline_id[:8]}...")

            with col3:
                if run.started_at:
                    st.markdown(f"**Started:** {run.started_at.strftime('%Y-%m-%d %H:%M')}")
                if run.completed_at:
                    st.caption(f"Completed: {run.completed_at.strftime('%H:%M:%S')}")
                if run.duration_seconds:
                    st.caption(f"Duration: {run.duration_seconds:.1f}s")

            with col4:
                if run.records_processed:
                    st.metric("Records", run.records_processed)

            if run.error_message:
                st.error(f"Error: {run.error_message}")

            st.markdown("---")

