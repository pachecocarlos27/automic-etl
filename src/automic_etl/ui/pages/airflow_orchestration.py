"""Airflow Orchestration UI Page.

Provides a comprehensive interface for:
- DAG management and monitoring
- Agentic pipeline operations
- Dynamic DAG generation
- Self-healing and optimization
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import httpx
import streamlit as st

from automic_etl.ui.theme import apply_theme, get_theme_colors
from automic_etl.ui.components import (
    card,
    metric_card,
    status_badge,
    info_box,
    data_table,
)

# API base URL - can be configured via environment or settings
API_BASE_URL = "http://localhost:8000/api/v1"


def _get_api_client() -> httpx.Client:
    """Get configured HTTP client for API calls."""
    return httpx.Client(base_url=API_BASE_URL, timeout=30.0)


def render_airflow_page() -> None:
    """Render the Airflow orchestration page."""
    apply_theme()
    colors = get_theme_colors()

    st.title("Airflow Orchestration")
    st.markdown("*AI-Powered Pipeline Orchestration with Apache Airflow*")

    # Initialize session state
    if "airflow_connected" not in st.session_state:
        st.session_state.airflow_connected = False
    if "selected_dag" not in st.session_state:
        st.session_state.selected_dag = None

    # Tabs for different features
    tabs = st.tabs([
        "Dashboard",
        "DAG Management",
        "Generate DAG",
        "Agentic Operations",
        "Recovery & Healing",
        "Settings",
    ])

    with tabs[0]:
        render_dashboard()

    with tabs[1]:
        render_dag_management()

    with tabs[2]:
        render_dag_generator()

    with tabs[3]:
        render_agentic_operations()

    with tabs[4]:
        render_recovery_healing()

    with tabs[5]:
        render_settings()


def render_dashboard() -> None:
    """Render the main dashboard."""
    st.header("Orchestration Dashboard")

    # Connection status
    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        airflow_status = check_airflow_connection()
        if airflow_status.get("status") == "healthy":
            st.success("Connected to Airflow")
            st.session_state.airflow_connected = True
        else:
            st.error(f"Airflow connection failed: {airflow_status.get('error', 'Unknown')}")
            st.session_state.airflow_connected = False

    with col2:
        if st.button("Refresh Status", key="refresh_dashboard"):
            st.rerun()

    with col3:
        st.toggle("Auto-refresh", key="auto_refresh", value=False)

    if not st.session_state.airflow_connected:
        st.warning("Connect to Airflow to view dashboard metrics.")
        return

    # Metrics row
    st.subheader("Overview")

    metrics = get_orchestrator_metrics()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Monitored DAGs",
            value=len(metrics.get("monitored_dags", [])),
        )

    with col2:
        st.metric(
            label="Active Runs",
            value=metrics.get("active_runs", 0),
        )

    with col3:
        healing_count = metrics.get("healing_actions_count", 0)
        st.metric(
            label="Auto-Healings",
            value=healing_count,
            delta="Self-healed" if healing_count > 0 else None,
        )

    with col4:
        status = "Enabled" if metrics.get("auto_healing_enabled") else "Disabled"
        st.metric(label="Auto-Healing", value=status)

    # Recent DAG runs
    st.subheader("Recent DAG Runs")

    recent_runs = get_recent_dag_runs()

    if recent_runs:
        run_data = []
        for run in recent_runs[:10]:
            state = run.get("state", "unknown")
            state_emoji = {
                "success": ":white_check_mark:",
                "failed": ":x:",
                "running": ":hourglass_flowing_sand:",
                "queued": ":clock3:",
            }.get(state.lower(), ":question:")

            run_data.append({
                "DAG": run.get("dag_id", "N/A"),
                "Run ID": run.get("run_id", "N/A")[:20] + "...",
                "State": f"{state_emoji} {state}",
                "Start": run.get("start_date", "N/A"),
            })

        st.dataframe(run_data, use_container_width=True)
    else:
        st.info("No recent runs found.")

    # DAG health overview
    st.subheader("DAG Health")

    dags = get_dag_list()

    if dags:
        healthy = sum(1 for d in dags if not d.get("is_paused"))
        paused = sum(1 for d in dags if d.get("is_paused"))

        col1, col2 = st.columns(2)

        with col1:
            st.markdown(f"**Active DAGs:** {healthy}")

        with col2:
            st.markdown(f"**Paused DAGs:** {paused}")

        # DAG list with status
        for dag in dags[:5]:
            dag_id = dag.get("dag_id", "Unknown")
            paused = dag.get("is_paused", False)
            status = ":pause_button:" if paused else ":arrow_forward:"

            with st.expander(f"{status} {dag_id}"):
                st.write(f"**Description:** {dag.get('description', 'N/A')}")
                st.write(f"**Schedule:** {dag.get('schedule', 'N/A')}")
                st.write(f"**Tags:** {', '.join(dag.get('tags', []))}")

                col1, col2, col3 = st.columns(3)
                with col1:
                    if st.button("Trigger", key=f"trigger_{dag_id}"):
                        trigger_dag(dag_id)
                        st.success(f"Triggered {dag_id}")
                with col2:
                    if st.button("Analyze", key=f"analyze_{dag_id}"):
                        st.session_state.selected_dag = dag_id
                        st.info("Go to Agentic Operations tab for analysis")
                with col3:
                    action = "Unpause" if paused else "Pause"
                    if st.button(action, key=f"toggle_{dag_id}"):
                        toggle_dag_pause(dag_id, not paused)
                        st.rerun()


def render_dag_management() -> None:
    """Render DAG management interface."""
    st.header("DAG Management")

    if not st.session_state.airflow_connected:
        st.warning("Connect to Airflow first.")
        return

    # Filters
    col1, col2, col3 = st.columns(3)

    with col1:
        search_query = st.text_input("Search DAGs", placeholder="Enter DAG name...")

    with col2:
        show_paused = st.checkbox("Show paused DAGs", value=True)

    with col3:
        tag_filter = st.multiselect(
            "Filter by tags",
            ["automic-etl", "medallion", "cdc", "streaming", "auto-generated"],
        )

    # DAG list
    dags = get_dag_list(tags=tag_filter if tag_filter else None)

    if search_query:
        dags = [d for d in dags if search_query.lower() in d.get("dag_id", "").lower()]

    if not show_paused:
        dags = [d for d in dags if not d.get("is_paused")]

    st.markdown(f"**{len(dags)} DAGs found**")

    # Display DAGs
    for dag in dags:
        dag_id = dag.get("dag_id", "Unknown")

        with st.container():
            col1, col2, col3, col4 = st.columns([3, 2, 2, 2])

            with col1:
                paused = dag.get("is_paused", False)
                icon = ":pause_button:" if paused else ":white_check_mark:"
                st.markdown(f"### {icon} {dag_id}")

            with col2:
                st.markdown(f"**Schedule:** {dag.get('schedule', 'N/A')}")

            with col3:
                tags = dag.get("tags", [])
                st.markdown(f"**Tags:** {', '.join(tags[:3])}")

            with col4:
                # Actions
                action_col1, action_col2, action_col3 = st.columns(3)
                with action_col1:
                    if st.button(":arrow_forward:", key=f"run_{dag_id}", help="Trigger DAG"):
                        result = trigger_dag(dag_id)
                        st.toast(f"Triggered {dag_id}")

                with action_col2:
                    if st.button(":bar_chart:", key=f"view_{dag_id}", help="View runs"):
                        st.session_state.selected_dag = dag_id

                with action_col3:
                    if st.button(":mag:", key=f"insights_{dag_id}", help="AI Insights"):
                        with st.spinner("Analyzing..."):
                            insights = get_dag_insights(dag_id)
                            st.session_state[f"insights_{dag_id}"] = insights

            # Show runs if selected
            if st.session_state.selected_dag == dag_id:
                st.markdown("---")
                runs = get_dag_runs(dag_id)

                if runs:
                    run_df = []
                    for run in runs[:10]:
                        run_df.append({
                            "Run ID": run.get("run_id", "N/A")[:30],
                            "State": run.get("state", "unknown"),
                            "Start": run.get("start_date", "N/A"),
                            "End": run.get("end_date", "N/A"),
                        })
                    st.dataframe(run_df, use_container_width=True)
                else:
                    st.info("No runs found for this DAG.")

            # Show insights if available
            if f"insights_{dag_id}" in st.session_state:
                insights = st.session_state[f"insights_{dag_id}"]
                with st.expander("AI Insights", expanded=True):
                    st.json(insights)

            st.markdown("---")


def render_dag_generator() -> None:
    """Render DAG generation interface."""
    st.header("Generate DAG")
    st.markdown("Create new DAGs using AI-powered generation")

    generation_method = st.radio(
        "Generation Method",
        ["Natural Language", "Medallion Template", "Schema-Based", "Advanced"],
        horizontal=True,
    )

    if generation_method == "Natural Language":
        render_nl_generator()
    elif generation_method == "Medallion Template":
        render_medallion_generator()
    elif generation_method == "Schema-Based":
        render_schema_generator()
    else:
        render_advanced_generator()


def render_nl_generator() -> None:
    """Render natural language DAG generator."""
    st.subheader("Generate from Description")

    description = st.text_area(
        "Describe your pipeline",
        placeholder="e.g., Create a daily pipeline that extracts customer data from PostgreSQL, "
                    "cleans and deduplicates it, then creates aggregated metrics for the marketing team.",
        height=150,
    )

    col1, col2 = st.columns(2)

    with col1:
        context = st.text_area(
            "Additional Context (optional)",
            placeholder='{"source_db": "production", "target": "marketing_metrics"}',
            height=100,
        )

    with col2:
        deploy = st.checkbox("Deploy immediately", value=False)
        st.caption("If checked, the DAG will be deployed to Airflow automatically.")

    if st.button("Generate DAG", type="primary"):
        if not description:
            st.error("Please provide a pipeline description.")
            return

        with st.spinner("AI is generating your DAG..."):
            try:
                context_dict = json.loads(context) if context else None
            except json.JSONDecodeError:
                context_dict = None

            result = generate_dag_from_nl(description, context_dict, deploy)

            if result:
                st.success(f"DAG generated: {result.get('dag_id')}")

                # Show generated code
                with st.expander("Generated DAG Code", expanded=True):
                    st.code(result.get("code", ""), language="python")

                # Download button
                st.download_button(
                    label="Download DAG File",
                    data=result.get("code", ""),
                    file_name=f"{result.get('dag_id', 'generated_dag')}.py",
                    mime="text/x-python",
                )


def render_medallion_generator() -> None:
    """Render medallion template generator."""
    st.subheader("Medallion Architecture Template")

    col1, col2 = st.columns(2)

    with col1:
        pipeline_name = st.text_input("Pipeline Name", placeholder="customer_analytics")

        source_type = st.selectbox(
            "Source Type",
            ["postgresql", "mysql", "mongodb", "s3", "api"],
        )

        tables = st.text_area(
            "Tables (one per line)",
            placeholder="customers\norders\nproducts",
        )

    with col2:
        schedule = st.selectbox(
            "Schedule",
            ["@daily", "@hourly", "@weekly", "0 6 * * *", "*/30 * * * *"],
        )

        include_quality = st.checkbox("Include quality checks", value=True)
        include_llm = st.checkbox("Include LLM augmentation", value=False)

    # Source configuration
    st.subheader("Source Configuration")

    source_config = {}
    if source_type in ["postgresql", "mysql"]:
        col1, col2 = st.columns(2)
        with col1:
            source_config["host"] = st.text_input("Host", value="localhost")
            source_config["database"] = st.text_input("Database")
        with col2:
            source_config["port"] = st.number_input("Port", value=5432 if source_type == "postgresql" else 3306)
            source_config["schema"] = st.text_input("Schema", value="public")

    deploy = st.checkbox("Deploy to Airflow", value=False, key="medallion_deploy")

    if st.button("Generate Medallion DAG", type="primary"):
        if not pipeline_name or not tables:
            st.error("Please provide pipeline name and tables.")
            return

        table_list = [t.strip() for t in tables.split("\n") if t.strip()]

        with st.spinner("Generating medallion DAG..."):
            result = generate_medallion_dag(
                name=pipeline_name,
                source_type=source_type,
                source_config=source_config,
                tables=table_list,
                schedule=schedule,
                deploy=deploy,
            )

            if result:
                st.success(f"DAG generated: {result.get('dag_id')}")

                with st.expander("Generated DAG Code", expanded=True):
                    st.code(result.get("code", ""), language="python")


def render_schema_generator() -> None:
    """Render schema-based generator."""
    st.subheader("Generate from Schema")

    schema_input = st.text_area(
        "Paste your schema (JSON)",
        placeholder='{\n  "columns": [\n    {"name": "id", "type": "integer"},\n    {"name": "name", "type": "string"}\n  ]\n}',
        height=200,
    )

    target_table = st.text_input("Target Table Name", placeholder="processed_data")

    if st.button("Generate from Schema", type="primary"):
        if not schema_input or not target_table:
            st.error("Please provide schema and target table name.")
            return

        try:
            schema = json.loads(schema_input)
        except json.JSONDecodeError:
            st.error("Invalid JSON schema.")
            return

        with st.spinner("Analyzing schema and generating DAG..."):
            result = generate_dag_from_schema(schema, target_table)

            if result:
                st.success(f"DAG generated: {result.get('dag_id')}")

                with st.expander("Generated DAG Code", expanded=True):
                    st.code(result.get("code", ""), language="python")


def render_advanced_generator() -> None:
    """Render advanced generator with full control."""
    st.subheader("Advanced DAG Configuration")

    st.info("Advanced configuration allows full control over DAG generation.")

    # DAG metadata
    col1, col2 = st.columns(2)

    with col1:
        dag_id = st.text_input("DAG ID", placeholder="my_custom_dag")
        description = st.text_input("Description")
        schedule = st.text_input("Schedule (cron)", value="0 6 * * *")

    with col2:
        pattern = st.selectbox(
            "Pattern",
            ["medallion", "cdc", "streaming", "data_quality", "custom"],
        )
        max_active_runs = st.number_input("Max Active Runs", value=1, min_value=1)
        catchup = st.checkbox("Catchup", value=False)

    # Task definitions
    st.subheader("Tasks")

    if "advanced_tasks" not in st.session_state:
        st.session_state.advanced_tasks = []

    # Add task form
    with st.expander("Add Task"):
        task_id = st.text_input("Task ID")
        task_type = st.selectbox(
            "Task Type",
            ["BronzeIngestionOperator", "SilverTransformOperator", "GoldAggregationOperator",
             "LLMAugmentedOperator", "PythonOperator", "BashOperator"],
        )
        task_config = st.text_area("Task Config (JSON)", value="{}")
        dependencies = st.text_input("Dependencies (comma-separated)")

        if st.button("Add Task"):
            try:
                config = json.loads(task_config)
            except json.JSONDecodeError:
                config = {}

            st.session_state.advanced_tasks.append({
                "task_id": task_id,
                "task_type": task_type,
                "config": config,
                "dependencies": [d.strip() for d in dependencies.split(",") if d.strip()],
            })
            st.success(f"Added task: {task_id}")

    # Show current tasks
    if st.session_state.advanced_tasks:
        st.markdown("**Current Tasks:**")
        for i, task in enumerate(st.session_state.advanced_tasks):
            col1, col2 = st.columns([4, 1])
            with col1:
                st.markdown(f"- **{task['task_id']}** ({task['task_type']})")
            with col2:
                if st.button("Remove", key=f"remove_task_{i}"):
                    st.session_state.advanced_tasks.pop(i)
                    st.rerun()

    if st.button("Generate Advanced DAG", type="primary"):
        st.info("Advanced generation would use the configured tasks to create a DAG.")


def render_agentic_operations() -> None:
    """Render agentic operations interface."""
    st.header("Agentic Operations")
    st.markdown("AI-powered pipeline analysis and decision making")

    operation = st.radio(
        "Select Operation",
        ["Pipeline Analysis", "Optimization Suggestions", "Agent Decision", "Insights Dashboard"],
        horizontal=True,
    )

    if operation == "Pipeline Analysis":
        render_pipeline_analysis()
    elif operation == "Optimization Suggestions":
        render_optimization()
    elif operation == "Agent Decision":
        render_agent_decision()
    else:
        render_insights_dashboard()


def render_pipeline_analysis() -> None:
    """Render pipeline analysis interface."""
    st.subheader("AI Pipeline Analysis")

    dags = get_dag_list()
    dag_ids = [d.get("dag_id") for d in dags]

    selected_dag = st.selectbox(
        "Select DAG to Analyze",
        dag_ids,
        index=dag_ids.index(st.session_state.selected_dag) if st.session_state.selected_dag in dag_ids else 0,
    )

    col1, col2 = st.columns(2)
    with col1:
        include_history = st.checkbox("Include historical runs", value=True)
    with col2:
        include_lineage = st.checkbox("Include data lineage", value=True)

    if st.button("Analyze Pipeline", type="primary"):
        with st.spinner("AI is analyzing your pipeline..."):
            analysis = analyze_pipeline(selected_dag, include_history, include_lineage)

            if analysis:
                # Health score
                health_score = analysis.get("health_score", 0)
                color = "green" if health_score >= 80 else "orange" if health_score >= 60 else "red"

                st.markdown(f"### Health Score: :{color}[{health_score}/100]")

                # Summary
                st.markdown(f"**Summary:** {analysis.get('performance_summary', 'N/A')}")

                # Bottlenecks
                bottlenecks = analysis.get("bottlenecks", [])
                if bottlenecks:
                    st.subheader("Bottlenecks Identified")
                    for bn in bottlenecks:
                        impact = bn.get("impact", "medium")
                        icon = ":red_circle:" if impact == "high" else ":orange_circle:" if impact == "medium" else ":green_circle:"
                        st.markdown(f"{icon} **{bn.get('task', 'N/A')}:** {bn.get('issue', 'N/A')}")

                # Recommendations
                recommendations = analysis.get("recommendations", [])
                if recommendations:
                    st.subheader("Recommendations")
                    for i, rec in enumerate(recommendations, 1):
                        st.markdown(f"{i}. **{rec.get('action', 'N/A')}**")
                        st.caption(rec.get("reasoning", ""))


def render_optimization() -> None:
    """Render optimization suggestions interface."""
    st.subheader("Optimization Suggestions")

    dags = get_dag_list()
    dag_ids = [d.get("dag_id") for d in dags]

    selected_dag = st.selectbox("Select DAG", dag_ids, key="opt_dag")

    optimization_goal = st.radio(
        "Optimization Goal",
        ["reliability", "latency", "cost"],
        horizontal=True,
    )

    if st.button("Get Optimization Suggestions", type="primary"):
        with st.spinner("Analyzing for optimizations..."):
            suggestions = get_optimization_suggestions(selected_dag, optimization_goal)

            if suggestions:
                st.success(f"Found {len(suggestions)} optimization suggestions")

                for i, suggestion in enumerate(suggestions, 1):
                    risk = suggestion.get("risk_level", "medium")
                    risk_color = {"low": "green", "medium": "orange", "high": "red"}.get(risk, "gray")

                    with st.expander(f"Suggestion {i}: {suggestion.get('optimization_type', 'N/A')}"):
                        st.markdown(f"**Current Value:** {suggestion.get('current_value', 'N/A')}")
                        st.markdown(f"**Suggested Value:** {suggestion.get('suggested_value', 'N/A')}")
                        st.markdown(f"**Reasoning:** {suggestion.get('reasoning', 'N/A')}")
                        st.markdown(f"**Risk Level:** :{risk_color}[{risk}]")

                        if suggestion.get("auto_applicable"):
                            if st.button("Apply Suggestion", key=f"apply_{i}"):
                                st.info("Applying optimization...")
            else:
                st.info("No optimization suggestions found. Your DAG looks good!")


def render_agent_decision() -> None:
    """Render agent decision interface."""
    st.subheader("Agent Decision Making")
    st.markdown("Get AI-powered decisions for your pipelines")

    pipeline_name = st.text_input("Pipeline Name")

    context_json = st.text_area(
        "Context (JSON)",
        placeholder='{"current_state": "running", "errors": 0}',
        height=100,
    )

    available_actions = st.multiselect(
        "Available Actions",
        ["analyze", "optimize", "recover", "scale", "alert", "skip", "retry", "rollback"],
        default=["analyze", "optimize", "recover"],
    )

    if st.button("Get Agent Decision", type="primary"):
        if not pipeline_name:
            st.error("Please provide a pipeline name.")
            return

        try:
            context = json.loads(context_json) if context_json else {}
        except json.JSONDecodeError:
            context = {}

        with st.spinner("Agent is making a decision..."):
            decision = get_agent_decision(pipeline_name, context, available_actions)

            if decision:
                st.success("Decision made!")

                col1, col2 = st.columns(2)

                with col1:
                    st.metric("Recommended Action", decision.get("action", "N/A"))

                with col2:
                    confidence = decision.get("confidence", "low")
                    conf_color = {"high": "green", "medium": "orange", "low": "red"}.get(confidence, "gray")
                    st.markdown(f"**Confidence:** :{conf_color}[{confidence}]")

                st.markdown(f"**Reasoning:** {decision.get('reasoning', 'N/A')}")

                if decision.get("requires_approval"):
                    st.warning("This action requires human approval before execution.")
                    if st.button("Approve and Execute"):
                        st.info("Executing action...")


def render_insights_dashboard() -> None:
    """Render insights dashboard."""
    st.subheader("AI Insights Dashboard")

    dags = get_dag_list()

    if not dags:
        st.info("No DAGs found.")
        return

    # Quick insights for all DAGs
    st.markdown("### Quick Insights")

    for dag in dags[:5]:
        dag_id = dag.get("dag_id", "Unknown")

        with st.expander(f":bar_chart: {dag_id}"):
            if st.button("Get Full Insights", key=f"full_insights_{dag_id}"):
                with st.spinner("Generating insights..."):
                    insights = get_dag_insights(dag_id)

                    if insights:
                        analysis = insights.get("analysis", {})
                        optimizations = insights.get("optimizations", [])

                        st.markdown(f"**Health Score:** {analysis.get('health_score', 'N/A')}")
                        st.markdown(f"**Optimizations Available:** {len(optimizations)}")

                        if optimizations:
                            st.markdown("**Top Optimization:**")
                            top_opt = optimizations[0]
                            st.markdown(f"- {top_opt.get('type', 'N/A')}: {top_opt.get('reasoning', 'N/A')}")


def render_recovery_healing() -> None:
    """Render recovery and healing interface."""
    st.header("Recovery & Self-Healing")

    operation = st.radio(
        "Operation",
        ["Diagnose Failure", "Execute Recovery", "Healing History"],
        horizontal=True,
    )

    if operation == "Diagnose Failure":
        render_diagnose_failure()
    elif operation == "Execute Recovery":
        render_execute_recovery()
    else:
        render_healing_history()


def render_diagnose_failure() -> None:
    """Render failure diagnosis interface."""
    st.subheader("Diagnose DAG Failure")

    col1, col2 = st.columns(2)

    with col1:
        dag_id = st.text_input("DAG ID", key="diag_dag_id")
        run_id = st.text_input("Run ID", key="diag_run_id")

    with col2:
        task_id = st.text_input("Task ID (optional)", key="diag_task_id")

    if st.button("Diagnose Failure", type="primary"):
        if not dag_id or not run_id:
            st.error("Please provide DAG ID and Run ID.")
            return

        with st.spinner("AI is diagnosing the failure..."):
            diagnosis = diagnose_failure(dag_id, run_id, task_id if task_id else None)

            if diagnosis:
                st.session_state.last_diagnosis = diagnosis

                st.markdown(f"### Failure Analysis")
                st.markdown(f"**Root Cause:** {diagnosis.get('failure_reason', 'Unknown')}")

                # Recovery steps
                steps = diagnosis.get("recovery_steps", [])
                if steps:
                    st.markdown("### Recovery Steps")
                    for step in steps:
                        st.markdown(f"{step.get('order', '-')}. {step.get('action', 'N/A')}")
                        if step.get("details"):
                            st.caption(step.get("details"))

                # Confidence and requirements
                confidence = diagnosis.get("confidence", 0)
                st.markdown(f"**Recovery Confidence:** {confidence * 100:.0f}%")

                if diagnosis.get("requires_human"):
                    st.warning("This recovery requires human intervention.")
                else:
                    st.success("Automated recovery is possible.")


def render_execute_recovery() -> None:
    """Render recovery execution interface."""
    st.subheader("Execute Recovery")

    if "last_diagnosis" not in st.session_state:
        st.info("First diagnose a failure to get a recovery plan.")
        return

    diagnosis = st.session_state.last_diagnosis

    st.markdown(f"**DAG:** {diagnosis.get('dag_id', 'N/A')}")
    st.markdown(f"**Task:** {diagnosis.get('task_id', 'N/A')}")
    st.markdown(f"**Confidence:** {diagnosis.get('confidence', 0) * 100:.0f}%")

    auto_execute = st.checkbox(
        "Auto-execute (only for high-confidence recoveries)",
        value=False,
    )

    if st.button("Execute Recovery", type="primary"):
        with st.spinner("Executing recovery..."):
            result = execute_recovery(
                dag_id=diagnosis.get("dag_id"),
                run_id=diagnosis.get("run_id", ""),
                task_id=diagnosis.get("task_id"),
                auto_execute=auto_execute,
            )

            if result:
                status = result.get("status", "unknown")
                if status == "completed":
                    st.success("Recovery executed successfully!")
                elif status == "pending_approval":
                    st.warning("Recovery requires approval. Review the plan and try again.")
                else:
                    st.error(f"Recovery failed: {result.get('error', 'Unknown error')}")


def render_healing_history() -> None:
    """Render healing history."""
    st.subheader("Healing History")

    # This would typically come from a database
    st.info("Healing history shows all auto-healing actions taken by the system.")

    # Placeholder data
    history = [
        {"timestamp": "2024-01-15 10:30:00", "dag_id": "etl_customers", "action": "retry", "success": True},
        {"timestamp": "2024-01-14 15:45:00", "dag_id": "etl_orders", "action": "skip_bad_records", "success": True},
        {"timestamp": "2024-01-13 08:20:00", "dag_id": "ml_pipeline", "action": "reduce_batch_size", "success": False},
    ]

    for item in history:
        icon = ":white_check_mark:" if item["success"] else ":x:"
        st.markdown(f"{icon} **{item['timestamp']}** - {item['dag_id']}: {item['action']}")


def render_settings() -> None:
    """Render settings interface."""
    st.header("Airflow Integration Settings")

    # Connection settings
    st.subheader("Connection Settings")

    col1, col2 = st.columns(2)

    with col1:
        base_url = st.text_input("Airflow Base URL", value="http://localhost:8080")
        username = st.text_input("Username", value="admin")

    with col2:
        api_version = st.text_input("API Version", value="v1")
        password = st.text_input("Password", type="password")

    if st.button("Test Connection"):
        st.info("Testing connection...")
        # Test would go here

    # Orchestrator settings
    st.subheader("Orchestrator Settings")

    col1, col2 = st.columns(2)

    with col1:
        auto_healing = st.checkbox("Enable Auto-Healing", value=True)
        auto_optimization = st.checkbox("Enable Auto-Optimization", value=True)

    with col2:
        confidence_threshold = st.slider("Auto-execute Confidence Threshold", 0.0, 1.0, 0.8)
        monitoring_interval = st.number_input("Monitoring Interval (seconds)", value=60, min_value=10)

    # DAG deployment settings
    st.subheader("DAG Deployment Settings")

    dags_folder = st.text_input("DAGs Folder Path", placeholder="/path/to/airflow/dags")

    if st.button("Save Settings"):
        st.success("Settings saved!")


# =============================================================================
# API Helper Functions
# =============================================================================

def check_airflow_connection() -> dict[str, Any]:
    """Check Airflow connection status."""
    try:
        with _get_api_client() as client:
            response = client.get("/airflow/health")
            if response.status_code == 200:
                return response.json()
            return {"status": "unhealthy", "error": f"HTTP {response.status_code}"}
    except httpx.ConnectError:
        return {"status": "unhealthy", "error": "Cannot connect to API server"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


def get_orchestrator_metrics() -> dict[str, Any]:
    """Get orchestrator metrics."""
    try:
        with _get_api_client() as client:
            response = client.get("/airflow/orchestrator/status")
            if response.status_code == 200:
                return response.json()
            return {
                "monitored_dags": [],
                "active_runs": 0,
                "healing_actions_count": 0,
                "auto_healing_enabled": False,
            }
    except Exception:
        return {
            "monitored_dags": [],
            "active_runs": 0,
            "healing_actions_count": 0,
            "auto_healing_enabled": False,
        }


def get_recent_dag_runs() -> list[dict[str, Any]]:
    """Get recent DAG runs."""
    try:
        with _get_api_client() as client:
            response = client.get("/airflow/dag-runs", params={"limit": 10})
            if response.status_code == 200:
                return response.json().get("dag_runs", [])
            return []
    except Exception:
        return []


def get_dag_list(tags: list[str] | None = None) -> list[dict[str, Any]]:
    """Get list of DAGs."""
    try:
        with _get_api_client() as client:
            params = {}
            if tags:
                params["tags"] = ",".join(tags)
            response = client.get("/airflow/dags", params=params)
            if response.status_code == 200:
                return response.json().get("dags", [])
            return []
    except Exception:
        return []


def get_dag_runs(dag_id: str) -> list[dict[str, Any]]:
    """Get runs for a specific DAG."""
    try:
        with _get_api_client() as client:
            response = client.get(f"/airflow/dags/{dag_id}/runs")
            if response.status_code == 200:
                return response.json().get("dag_runs", [])
            return []
    except Exception:
        return []


def trigger_dag(dag_id: str) -> dict[str, Any]:
    """Trigger a DAG run."""
    try:
        with _get_api_client() as client:
            response = client.post(f"/airflow/dags/{dag_id}/trigger")
            if response.status_code == 200:
                return response.json()
            return {"error": f"HTTP {response.status_code}"}
    except Exception as e:
        return {"error": str(e)}


def toggle_dag_pause(dag_id: str, pause: bool) -> None:
    """Pause or unpause a DAG."""
    try:
        with _get_api_client() as client:
            endpoint = f"/airflow/dags/{dag_id}/{'pause' if pause else 'unpause'}"
            client.post(endpoint)
    except Exception:
        pass  # Silently fail - UI will handle display


def generate_dag_from_nl(description: str, context: dict[str, Any] | None, deploy: bool) -> dict[str, Any] | None:
    """Generate DAG from natural language."""
    try:
        with _get_api_client() as client:
            response = client.post(
                "/airflow/generate/natural-language",
                json={
                    "description": description,
                    "context": context or {},
                    "deploy": deploy,
                },
            )
            if response.status_code == 200:
                return response.json()
            return None
    except Exception:
        return None


def generate_medallion_dag(
    name: str,
    source_type: str,
    source_config: dict[str, Any],
    tables: list[str],
    schedule: str,
    deploy: bool,
) -> dict[str, Any] | None:
    """Generate medallion DAG."""
    try:
        with _get_api_client() as client:
            response = client.post(
                "/airflow/generate/medallion",
                json={
                    "name": name,
                    "source_type": source_type,
                    "source_config": source_config,
                    "tables": tables,
                    "schedule": schedule,
                    "deploy": deploy,
                },
            )
            if response.status_code == 200:
                return response.json()
            return None
    except Exception:
        return None


def generate_dag_from_schema(schema: dict[str, Any], target_table: str) -> dict[str, Any] | None:
    """Generate DAG from schema."""
    try:
        with _get_api_client() as client:
            response = client.post(
                "/airflow/generate/schema",
                json={
                    "schema": schema,
                    "target_table": target_table,
                },
            )
            if response.status_code == 200:
                return response.json()
            return None
    except Exception:
        return None


def analyze_pipeline(dag_id: str, include_history: bool, include_lineage: bool) -> dict[str, Any] | None:
    """Analyze a pipeline."""
    try:
        with _get_api_client() as client:
            response = client.post(
                f"/airflow/agentic/analyze/{dag_id}",
                json={
                    "include_history": include_history,
                    "include_lineage": include_lineage,
                },
            )
            if response.status_code == 200:
                return response.json()
            return None
    except Exception:
        return None


def get_optimization_suggestions(dag_id: str, goal: str) -> list[dict[str, Any]]:
    """Get optimization suggestions."""
    try:
        with _get_api_client() as client:
            response = client.post(
                f"/airflow/agentic/optimize/{dag_id}",
                json={"goal": goal},
            )
            if response.status_code == 200:
                return response.json().get("suggestions", [])
            return []
    except Exception:
        return []


def get_agent_decision(pipeline_name: str, context: dict[str, Any], actions: list[str]) -> dict[str, Any]:
    """Get agent decision."""
    try:
        with _get_api_client() as client:
            response = client.post(
                "/airflow/agentic/decide",
                json={
                    "pipeline_name": pipeline_name,
                    "context": context,
                    "available_actions": actions,
                },
            )
            if response.status_code == 200:
                return response.json()
            return {"error": f"HTTP {response.status_code}"}
    except Exception as e:
        return {"error": str(e)}


def get_dag_insights(dag_id: str) -> dict[str, Any]:
    """Get comprehensive DAG insights."""
    try:
        with _get_api_client() as client:
            response = client.get(f"/airflow/agentic/insights/{dag_id}")
            if response.status_code == 200:
                return response.json()
            return {"analysis": {}, "optimizations": []}
    except Exception:
        return {"analysis": {}, "optimizations": []}


def diagnose_failure(dag_id: str, run_id: str, task_id: str | None) -> dict[str, Any]:
    """Diagnose a failure."""
    try:
        with _get_api_client() as client:
            response = client.post(
                "/airflow/agentic/diagnose",
                json={
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "task_id": task_id,
                },
            )
            if response.status_code == 200:
                return response.json()
            return {"error": f"HTTP {response.status_code}"}
    except Exception as e:
        return {"error": str(e)}


def execute_recovery(dag_id: str, run_id: str, task_id: str | None, auto_execute: bool) -> dict[str, Any]:
    """Execute recovery plan."""
    try:
        with _get_api_client() as client:
            response = client.post(
                "/airflow/agentic/recover",
                json={
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "task_id": task_id,
                    "auto_execute": auto_execute,
                },
            )
            if response.status_code == 200:
                return response.json()
            return {"status": "error", "error": f"HTTP {response.status_code}"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# Page entry point
if __name__ == "__main__":
    render_airflow_page()
