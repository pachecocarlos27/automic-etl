"""Data Validation & Quality Management Page for Automic ETL UI."""

from __future__ import annotations

import streamlit as st
from datetime import datetime, timedelta
from typing import Any, List, Optional

from automic_etl.db.validation_service import get_validation_service


def show_validation_page():
    """Display the data validation and quality management page."""
    st.title("Data Validation & Quality")
    st.markdown("Define validation rules, monitor data quality, and track violations.")

    tab1, tab2, tab3, tab4 = st.tabs([
        "Quality Dashboard",
        "Validation Rules",
        "Violations",
        "Quality Reports",
    ])

    with tab1:
        _show_quality_dashboard()

    with tab2:
        _show_validation_rules()

    with tab3:
        _show_violations()

    with tab4:
        _show_quality_reports()


def _show_quality_dashboard():
    """Show data quality overview dashboard."""
    st.subheader("Data Quality Overview")

    service = get_validation_service()

    # Get quality summary from database
    try:
        summary = service.get_quality_summary()
        total_rules = summary.get("total_rules", 0)
        enabled_rules = summary.get("enabled_rules", 0)
        passing_rules = summary.get("passing_rules", 0)
        failing_rules = summary.get("failing_rules", 0)
        pass_rate = summary.get("pass_rate", 0)
    except Exception:
        total_rules = 0
        enabled_rules = 0
        passing_rules = 0
        failing_rules = 0
        pass_rate = 0

    # Overall quality score
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        quality_color = "#10B981" if pass_rate >= 80 else "#F59E0B" if pass_rate >= 60 else "#EF4444"
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, {quality_color} 0%, {quality_color}99 100%);
            padding: 1.5rem;
            border-radius: 12px;
            color: white;
            text-align: center;
        ">
            <div style="font-size: 2.5rem; font-weight: 700;">{pass_rate:.0f}%</div>
            <div style="font-size: 0.875rem; opacity: 0.9;">Overall Quality Score</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.metric(
            label="Rules Defined",
            value=str(total_rules),
            delta=f"{enabled_rules} enabled",
        )

    with col3:
        st.metric(
            label="Failing Rules",
            value=str(failing_rules),
            delta=f"-{passing_rules} passing" if passing_rules else None,
            delta_color="inverse",
        )

    with col4:
        st.metric(
            label="Tables Monitored",
            value=str(len(_get_unique_tables())),
            delta=f"{summary.get('critical_failing', 0)} critical" if summary.get('critical_failing', 0) > 0 else None,
        )

    st.markdown("---")

    # Quality dimensions breakdown
    st.subheader("Quality Dimensions")

    col1, col2 = st.columns(2)

    with col1:
        # Calculate dimension scores from actual rules
        dimensions = _calculate_dimension_scores()

        for dim in dimensions:
            col_a, col_b, col_c = st.columns([2, 3, 1])
            with col_a:
                st.markdown(f"**{dim['name']}**")
            with col_b:
                st.progress(dim["score"] / 100)
            with col_c:
                color = "green" if dim["score"] >= 85 else "orange" if dim["score"] >= 70 else "red"
                st.markdown(f":{color}[{dim['score']}%]")

    with col2:
        # Quality by layer
        st.markdown("**Quality by Layer**")

        layer_stats = _calculate_layer_stats()

        for layer in layer_stats:
            with st.container():
                col_a, col_b, col_c = st.columns([2, 2, 1])
                with col_a:
                    st.markdown(f"**{layer['layer']}**")
                    st.caption(f"{layer['tables']} tables")
                with col_b:
                    st.progress(layer["score"] / 100)
                with col_c:
                    st.markdown(f"**{layer['score']}%**")

    st.markdown("---")

    # Recent quality issues
    st.subheader("Recent Quality Issues")

    failing_rules = service.get_failing_rules()

    if not failing_rules:
        st.info("No quality issues detected.")
    else:
        for rule in failing_rules[:5]:  # Show top 5
            severity_colors = {"critical": "red", "high": "orange", "medium": "blue", "low": "gray"}
            col1, col2, col3, col4 = st.columns([2, 3, 1, 1])

            with col1:
                st.markdown(f"**{rule.target_table}**")

            with col2:
                st.caption(f"{rule.name}: {rule.description or 'Rule failed'}")

            with col3:
                color = severity_colors.get(rule.severity, "gray")
                st.markdown(f":{color}[{rule.severity.capitalize()}]")

            with col4:
                if rule.last_run_at:
                    time_diff = datetime.utcnow() - rule.last_run_at
                    if time_diff.days > 0:
                        st.caption(f"{time_diff.days} days ago")
                    elif time_diff.seconds > 3600:
                        st.caption(f"{time_diff.seconds // 3600} hours ago")
                    else:
                        st.caption(f"{time_diff.seconds // 60} min ago")


def _get_unique_tables() -> List[str]:
    """Get list of unique tables being monitored."""
    service = get_validation_service()
    try:
        rules = service.list_rules()
        return list(set(r.target_table for r in rules))
    except Exception:
        return []


def _calculate_dimension_scores() -> List[dict]:
    """Calculate quality dimension scores from actual rules."""
    service = get_validation_service()

    dimensions = {
        "Completeness": {"types": ["not_null"], "rules": [], "passing": 0},
        "Accuracy": {"types": ["range", "format", "regex"], "rules": [], "passing": 0},
        "Consistency": {"types": ["referential"], "rules": [], "passing": 0},
        "Timeliness": {"types": ["freshness"], "rules": [], "passing": 0},
        "Uniqueness": {"types": ["unique"], "rules": [], "passing": 0},
        "Validity": {"types": ["custom", "custom_sql"], "rules": [], "passing": 0},
    }

    try:
        rules = service.list_rules(enabled=True)

        for rule in rules:
            for dim_name, dim_data in dimensions.items():
                if rule.rule_type.lower() in dim_data["types"]:
                    dim_data["rules"].append(rule)
                    if rule.last_status == "passed":
                        dim_data["passing"] += 1
    except Exception:
        pass

    result = []
    for dim_name, dim_data in dimensions.items():
        total = len(dim_data["rules"])
        if total > 0:
            score = int((dim_data["passing"] / total) * 100)
        else:
            score = 100  # No rules = 100% by default

        result.append({
            "name": dim_name,
            "score": score,
            "description": f"{dim_data['passing']}/{total} rules passing",
        })

    return result


def _calculate_layer_stats() -> List[dict]:
    """Calculate quality stats by data layer."""
    service = get_validation_service()

    layers = {
        "Bronze": {"prefix": "bronze", "rules": 0, "passing": 0, "tables": set()},
        "Silver": {"prefix": "silver", "rules": 0, "passing": 0, "tables": set()},
        "Gold": {"prefix": "gold", "rules": 0, "passing": 0, "tables": set()},
    }

    try:
        rules = service.list_rules(enabled=True)

        for rule in rules:
            for layer_name, layer_data in layers.items():
                if rule.target_table.lower().startswith(layer_data["prefix"]):
                    layer_data["rules"] += 1
                    layer_data["tables"].add(rule.target_table)
                    if rule.last_status == "passed":
                        layer_data["passing"] += 1
    except Exception:
        pass

    result = []
    for layer_name, layer_data in layers.items():
        total = layer_data["rules"]
        if total > 0:
            score = int((layer_data["passing"] / total) * 100)
        else:
            score = 100

        result.append({
            "layer": layer_name,
            "score": score,
            "tables": len(layer_data["tables"]),
        })

    return result


def _show_validation_rules():
    """Show validation rules management."""
    st.subheader("Validation Rules")

    service = get_validation_service()

    # Create new rule
    with st.expander("Create New Validation Rule", expanded=False):
        col1, col2 = st.columns(2)

        with col1:
            rule_name = st.text_input("Rule Name", placeholder="customer_email_valid")
            target_table = st.text_input(
                "Target Table",
                placeholder="bronze.raw_customers",
            )
            target_column = st.text_input("Target Column", placeholder="email")

        with col2:
            rule_type = st.selectbox(
                "Rule Type",
                [
                    "not_null",
                    "unique",
                    "regex",
                    "range",
                    "referential",
                    "custom_sql",
                ],
            )

            rule_config = {}

            if rule_type == "regex":
                pattern = st.text_input("Regex Pattern", placeholder=r"^[\w\.-]+@[\w\.-]+\.\w+$")
                rule_config["pattern"] = pattern
            elif rule_type == "range":
                col_a, col_b = st.columns(2)
                with col_a:
                    min_val = st.number_input("Min Value", value=0)
                with col_b:
                    max_val = st.number_input("Max Value", value=100)
                rule_config["min"] = min_val
                rule_config["max"] = max_val
            elif rule_type == "referential":
                ref_table = st.text_input("Reference Table", placeholder="silver.customers")
                ref_column = st.text_input("Reference Column", placeholder="customer_id")
                rule_config["ref_table"] = ref_table
                rule_config["ref_column"] = ref_column
            elif rule_type == "custom_sql":
                custom_sql = st.text_area("Custom SQL Expression", placeholder="column_value > 0 AND column_value < 1000")
                rule_config["sql"] = custom_sql

            severity = st.selectbox("Severity", ["critical", "high", "medium", "low"])
            description = st.text_input("Description", placeholder="Validates that email format is correct")

        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("Create Rule", type="primary"):
                if rule_name and target_table and rule_type:
                    try:
                        service.create_rule(
                            name=rule_name,
                            rule_type=rule_type,
                            target_table=target_table,
                            target_column=target_column if target_column else None,
                            description=description,
                            rule_config=rule_config,
                            severity=severity,
                        )
                        st.success(f"Rule '{rule_name}' created successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to create rule: {e}")
                else:
                    st.warning("Please fill in required fields: Name, Target Table, and Rule Type")

    st.markdown("---")

    # Filter rules
    col1, col2, col3 = st.columns(3)
    with col1:
        filter_table = st.text_input("Filter by Table", placeholder="bronze.*")
    with col2:
        filter_type = st.selectbox("Filter by Type", ["All", "not_null", "unique", "regex", "range", "referential", "custom_sql"])
    with col3:
        filter_status = st.selectbox("Filter by Status", ["All", "passing", "failing"])

    # Get rules from database
    try:
        rules = service.list_rules(
            rule_type=filter_type if filter_type != "All" else None,
        )

        # Apply additional filters
        if filter_table:
            rules = [r for r in rules if filter_table.replace("*", "") in r.target_table]
        if filter_status != "All":
            rules = [r for r in rules if r.last_status == filter_status]

    except Exception as e:
        st.error(f"Failed to load rules: {e}")
        rules = []

    if not rules:
        st.info("No validation rules found. Create one above to get started.")
    else:
        for rule in rules:
            with st.container():
                col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 1, 1])

                with col1:
                    status_icon = "" if rule.enabled else ""
                    st.markdown(f"{status_icon} **{rule.name}**")
                    st.caption(f"{rule.target_table}" + (f".{rule.target_column}" if rule.target_column else ""))

                with col2:
                    st.markdown(f"`{rule.rule_type}`")
                    severity_colors = {"critical": "red", "high": "orange", "medium": "blue", "low": "gray"}
                    st.markdown(f":{severity_colors.get(rule.severity, 'gray')}[{rule.severity.capitalize()}]")

                with col3:
                    if rule.last_status:
                        status_colors = {"passed": "green", "passing": "green", "failed": "red", "failing": "red"}
                        st.markdown(f":{status_colors.get(rule.last_status, 'gray')}[{rule.last_status.upper()}]")
                    else:
                        st.markdown(":gray[NOT RUN]")

                with col4:
                    enabled = st.toggle("", value=rule.enabled, key=f"rule_toggle_{rule.id}")
                    if enabled != rule.enabled:
                        service.update_rule(rule.id, enabled=enabled)
                        st.rerun()

                with col5:
                    col_a, col_b = st.columns(2)
                    with col_a:
                        if st.button("", key=f"edit_rule_{rule.id}", help="Edit"):
                            st.session_state[f"editing_rule"] = rule.id
                    with col_b:
                        if st.button("", key=f"delete_rule_{rule.id}", help="Delete"):
                            service.delete_rule(rule.id)
                            st.rerun()

                st.markdown("---")


def _show_violations():
    """Show validation violations."""
    st.subheader("Validation Violations")

    service = get_validation_service()

    # Get results from database
    try:
        all_results = service.get_results(status="failed", limit=100)
        total_violations = len(all_results)
        critical_count = sum(1 for r in all_results if _get_rule_severity(r.rule_id, service) == "critical")
        high_count = sum(1 for r in all_results if _get_rule_severity(r.rule_id, service) == "high")
    except Exception:
        all_results = []
        total_violations = 0
        critical_count = 0
        high_count = 0

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Violations", str(total_violations))

    with col2:
        st.metric("Critical", str(critical_count))

    with col3:
        st.metric("High", str(high_count))

    with col4:
        st.metric("Medium/Low", str(total_violations - critical_count - high_count))

    st.markdown("---")

    # Filters
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        v_severity = st.selectbox("Severity", ["All", "Critical", "High", "Medium", "Low"], key="v_severity")

    with col2:
        v_table = st.text_input("Table Filter", placeholder="bronze.*", key="v_table")

    with col3:
        v_date = st.selectbox("Date Range", ["Last 24h", "Last 7 days", "Last 30 days"], key="v_date")

    with col4:
        st.write("")  # Spacer

    # Get violations based on filters
    try:
        since = None
        if v_date == "Last 24h":
            since = datetime.utcnow() - timedelta(days=1)
        elif v_date == "Last 7 days":
            since = datetime.utcnow() - timedelta(days=7)
        elif v_date == "Last 30 days":
            since = datetime.utcnow() - timedelta(days=30)

        results = service.get_results(status="failed", limit=50)

        # Filter by date
        if since:
            results = [r for r in results if r.executed_at and r.executed_at >= since]

    except Exception as e:
        st.error(f"Failed to load violations: {e}")
        results = []

    if not results:
        st.success("No violations found!")
    else:
        for result in results:
            # Get rule info
            rule = service.get_rule(result.rule_id)
            if not rule:
                continue

            # Apply severity filter
            if v_severity != "All" and rule.severity.lower() != v_severity.lower():
                continue

            # Apply table filter
            if v_table and v_table.replace("*", "") not in rule.target_table:
                continue

            with st.container():
                col1, col2, col3, col4 = st.columns([3, 2, 2, 2])

                with col1:
                    severity_icons = {"critical": "", "high": "", "medium": "", "low": ""}
                    st.markdown(f"{severity_icons.get(rule.severity, '')} **{rule.name}**")
                    st.caption(f"Table: {rule.target_table}")

                with col2:
                    st.markdown(f"**{result.rows_failed:,}** violations")
                    st.caption(f"of {result.rows_checked:,} rows checked")

                with col3:
                    severity_colors = {"critical": "red", "high": "orange", "medium": "blue", "low": "gray"}
                    st.markdown(f":{severity_colors.get(rule.severity, 'gray')}[{rule.severity.capitalize()}]")
                    if result.executed_at:
                        st.caption(f"Detected: {result.executed_at.strftime('%Y-%m-%d %H:%M')}")

                with col4:
                    if result.failure_samples:
                        with st.expander("View Samples"):
                            for sample in result.failure_samples[:5]:
                                st.code(str(sample))

                st.markdown("---")


def _get_rule_severity(rule_id: str, service) -> str:
    """Get severity for a rule."""
    try:
        rule = service.get_rule(rule_id)
        return rule.severity if rule else "low"
    except Exception:
        return "low"


def _show_quality_reports():
    """Show data quality reports."""
    st.subheader("Quality Reports")

    service = get_validation_service()

    # Generate report
    with st.expander("Generate New Report", expanded=False):
        col1, col2 = st.columns(2)

        with col1:
            report_type = st.selectbox(
                "Report Type",
                ["Quality Summary", "Violation Details", "Trend Analysis", "Table Profiling"],
            )
            target_scope = st.multiselect(
                "Tables to Include",
                ["bronze.*", "silver.*", "gold.*", "All Tables"],
                default=["All Tables"],
            )

        with col2:
            date_range = st.selectbox("Date Range", ["Last 7 days", "Last 30 days", "Last Quarter"])
            output_format = st.selectbox("Output Format", ["PDF", "HTML", "Excel", "JSON"])

        include_options = st.multiselect(
            "Include Sections",
            ["Executive Summary", "Dimension Breakdown", "Violation Details", "Trend Charts", "Recommendations"],
            default=["Executive Summary", "Dimension Breakdown"],
        )

        if st.button("Generate Report", type="primary"):
            with st.spinner("Generating report..."):
                # Generate actual report from database
                try:
                    summary = service.get_quality_summary()
                    failing_rules = service.get_failing_rules()

                    report_content = f"""
# Data Quality Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}

## Executive Summary
- Total Rules: {summary.get('total_rules', 0)}
- Enabled Rules: {summary.get('enabled_rules', 0)}
- Pass Rate: {summary.get('pass_rate', 0):.1f}%

## Failing Rules ({summary.get('failing_rules', 0)})
"""
                    for rule in failing_rules[:10]:
                        report_content += f"- {rule.name} ({rule.target_table}): {rule.severity}\n"

                    st.success("Report generated!")
                    st.download_button(
                        "Download Report",
                        data=report_content,
                        file_name=f"quality_report_{datetime.now().strftime('%Y%m%d')}.txt",
                        mime="text/plain",
                    )
                except Exception as e:
                    st.error(f"Failed to generate report: {e}")

    st.markdown("---")

    # Current quality snapshot
    st.subheader("Current Quality Snapshot")

    try:
        summary = service.get_quality_summary()

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Rules", summary.get("total_rules", 0))
            st.metric("Enabled", summary.get("enabled_rules", 0))

        with col2:
            st.metric("Passing", summary.get("passing_rules", 0))
            st.metric("Failing", summary.get("failing_rules", 0))

        with col3:
            pass_rate = summary.get("pass_rate", 0)
            st.metric("Pass Rate", f"{pass_rate:.1f}%")
            st.metric("Critical Failing", summary.get("critical_failing", 0))

    except Exception as e:
        st.error(f"Failed to load quality snapshot: {e}")

    st.markdown("---")

    # Scheduled reports info
    st.subheader("Scheduled Reports")
    st.info("Configure scheduled reports in the Jobs & Orchestration page to automate quality reporting.")


# Run page when loaded by Streamlit
show_validation_page()
