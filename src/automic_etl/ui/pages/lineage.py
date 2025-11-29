"""Data Lineage Visualization page for Automic ETL."""

from __future__ import annotations

from typing import Any
from datetime import datetime
import streamlit as st


def show_lineage_page() -> None:
    """Display the data lineage visualization page."""
    st.title("Data Lineage")
    st.markdown("*Visualize and explore data flow and transformations*")

    # Tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs([
        "🌳 Lineage Graph",
        "📊 Table Dependencies",
        "🔄 Pipeline Flow",
        "📋 Impact Analysis"
    ])

    with tab1:
        _show_lineage_graph()

    with tab2:
        _show_table_dependencies()

    with tab3:
        _show_pipeline_flow()

    with tab4:
        _show_impact_analysis()


def _show_lineage_graph() -> None:
    """Display interactive lineage graph."""
    st.markdown("### Lineage Graph")

    # Controls
    col1, col2, col3 = st.columns([2, 2, 1])

    with col1:
        selected_table = st.selectbox(
            "Select Table",
            options=["All", "bronze.raw_customers", "bronze.raw_orders", "silver.customers",
                     "silver.orders", "gold.customer_360", "gold.sales_metrics"],
            key="lineage_table_select"
        )

    with col2:
        lineage_depth = st.slider(
            "Lineage Depth",
            min_value=1,
            max_value=5,
            value=3,
            help="Number of upstream/downstream hops to display"
        )

    with col3:
        direction = st.radio(
            "Direction",
            options=["Both", "Upstream", "Downstream"],
            horizontal=True
        )

    st.markdown("---")

    # Lineage visualization
    _render_lineage_visualization(selected_table, lineage_depth, direction)

    # Node details panel
    st.markdown("---")
    st.markdown("### Node Details")

    with st.expander("Selected Node: silver.customers", expanded=True):
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Table Information**")
            st.markdown("""
            - **Schema:** silver
            - **Table:** customers
            - **Format:** Delta Lake
            - **Rows:** 1,234,567
            - **Size:** 2.3 GB
            - **Last Updated:** 2024-01-15 10:30:00
            """)

        with col2:
            st.markdown("**Transformation Info**")
            st.markdown("""
            - **Pipeline:** customer_processing
            - **Stage:** silver_transform
            - **SCD Type:** Type 2
            - **Partitioned By:** created_date
            - **Z-Ordered By:** customer_id
            """)

        st.markdown("**Columns**")
        columns_data = [
            {"Column": "customer_id", "Type": "STRING", "Source": "bronze.raw_customers.id", "Transform": "CAST"},
            {"Column": "full_name", "Type": "STRING", "Source": "bronze.raw_customers", "Transform": "CONCAT(first_name, last_name)"},
            {"Column": "email", "Type": "STRING", "Source": "bronze.raw_customers.email", "Transform": "LOWER"},
            {"Column": "status", "Type": "STRING", "Source": "bronze.raw_customers.status", "Transform": "UPPER"},
            {"Column": "created_at", "Type": "TIMESTAMP", "Source": "bronze.raw_customers.created_at", "Transform": "TO_TIMESTAMP"},
        ]
        st.dataframe(columns_data, use_container_width=True, hide_index=True)


def _render_lineage_visualization(table: str, depth: int, direction: str) -> None:
    """Render the lineage graph visualization."""
    # Create a visual representation using HTML/CSS
    # In production, this would use a proper graph library like vis.js or D3.js

    st.markdown("""
    <style>
    .lineage-container {
        display: flex;
        justify-content: center;
        align-items: center;
        padding: 2rem;
        background: var(--surface);
        border-radius: var(--radius-lg);
        overflow-x: auto;
    }
    .lineage-column {
        display: flex;
        flex-direction: column;
        align-items: center;
        gap: 1rem;
        min-width: 150px;
    }
    .lineage-node {
        padding: 0.75rem 1rem;
        border-radius: var(--radius-md);
        text-align: center;
        font-size: 0.875rem;
        cursor: pointer;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
        min-width: 140px;
    }
    .lineage-node:hover {
        transform: scale(1.05);
        box-shadow: var(--shadow-md);
    }
    .node-bronze {
        background: linear-gradient(135deg, #cd7f32 0%, #8b5a2b 100%);
        color: white;
    }
    .node-silver {
        background: linear-gradient(135deg, #c0c0c0 0%, #a9a9a9 100%);
        color: #333;
    }
    .node-gold {
        background: linear-gradient(135deg, #ffd700 0%, #daa520 100%);
        color: #333;
    }
    .node-external {
        background: var(--surface-hover);
        border: 2px dashed var(--border);
        color: var(--text-secondary);
    }
    .lineage-arrow {
        display: flex;
        align-items: center;
        padding: 0 1rem;
        color: var(--text-muted);
        font-size: 1.5rem;
    }
    .tier-label {
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        color: var(--text-muted);
        margin-bottom: 0.5rem;
    }
    </style>

    <div class="lineage-container">
        <div class="lineage-column">
            <div class="tier-label">Sources</div>
            <div class="lineage-node node-external">Salesforce<br><small>CRM</small></div>
            <div class="lineage-node node-external">PostgreSQL<br><small>Orders DB</small></div>
            <div class="lineage-node node-external">S3 Bucket<br><small>Files</small></div>
        </div>

        <div class="lineage-arrow">→</div>

        <div class="lineage-column">
            <div class="tier-label">Bronze</div>
            <div class="lineage-node node-bronze">raw_customers</div>
            <div class="lineage-node node-bronze">raw_orders</div>
            <div class="lineage-node node-bronze">raw_products</div>
        </div>

        <div class="lineage-arrow">→</div>

        <div class="lineage-column">
            <div class="tier-label">Silver</div>
            <div class="lineage-node node-silver">customers</div>
            <div class="lineage-node node-silver">orders</div>
            <div class="lineage-node node-silver">products</div>
        </div>

        <div class="lineage-arrow">→</div>

        <div class="lineage-column">
            <div class="tier-label">Gold</div>
            <div class="lineage-node node-gold">customer_360</div>
            <div class="lineage-node node-gold">sales_metrics</div>
            <div class="lineage-node node-gold">product_analytics</div>
        </div>

        <div class="lineage-arrow">→</div>

        <div class="lineage-column">
            <div class="tier-label">Consumers</div>
            <div class="lineage-node node-external">BI Dashboard</div>
            <div class="lineage-node node-external">ML Models</div>
            <div class="lineage-node node-external">Reports</div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def _show_table_dependencies() -> None:
    """Display table dependency matrix."""
    st.markdown("### Table Dependencies")

    # Filters
    col1, col2 = st.columns(2)

    with col1:
        tier_filter = st.multiselect(
            "Filter by Tier",
            options=["Bronze", "Silver", "Gold"],
            default=["Bronze", "Silver", "Gold"]
        )

    with col2:
        dependency_type = st.selectbox(
            "Dependency Type",
            options=["All", "Direct", "Indirect"]
        )

    st.markdown("---")

    # Dependencies table
    dependencies = [
        {
            "Table": "gold.customer_360",
            "Depends On": "silver.customers, silver.orders",
            "Used By": "BI Dashboard, ML Models",
            "Tier": "Gold",
            "Dependencies Count": 2,
            "Dependents Count": 2
        },
        {
            "Table": "gold.sales_metrics",
            "Depends On": "silver.orders, silver.products",
            "Used By": "Reports, Analytics",
            "Tier": "Gold",
            "Dependencies Count": 2,
            "Dependents Count": 2
        },
        {
            "Table": "silver.customers",
            "Depends On": "bronze.raw_customers",
            "Used By": "gold.customer_360",
            "Tier": "Silver",
            "Dependencies Count": 1,
            "Dependents Count": 1
        },
        {
            "Table": "silver.orders",
            "Depends On": "bronze.raw_orders",
            "Used By": "gold.customer_360, gold.sales_metrics",
            "Tier": "Silver",
            "Dependencies Count": 1,
            "Dependents Count": 2
        },
        {
            "Table": "silver.products",
            "Depends On": "bronze.raw_products",
            "Used By": "gold.sales_metrics",
            "Tier": "Silver",
            "Dependencies Count": 1,
            "Dependents Count": 1
        },
        {
            "Table": "bronze.raw_customers",
            "Depends On": "Salesforce CRM",
            "Used By": "silver.customers",
            "Tier": "Bronze",
            "Dependencies Count": 1,
            "Dependents Count": 1
        },
        {
            "Table": "bronze.raw_orders",
            "Depends On": "PostgreSQL Orders",
            "Used By": "silver.orders",
            "Tier": "Bronze",
            "Dependencies Count": 1,
            "Dependents Count": 1
        },
        {
            "Table": "bronze.raw_products",
            "Depends On": "S3 Product Files",
            "Used By": "silver.products",
            "Tier": "Bronze",
            "Dependencies Count": 1,
            "Dependents Count": 1
        },
    ]

    # Filter by tier
    if tier_filter:
        dependencies = [d for d in dependencies if d["Tier"] in tier_filter]

    st.dataframe(dependencies, use_container_width=True, hide_index=True)

    # Dependency graph metrics
    st.markdown("---")
    st.markdown("### Dependency Metrics")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Tables", len(dependencies))
    with col2:
        st.metric("Total Dependencies", sum(d["Dependencies Count"] for d in dependencies))
    with col3:
        st.metric("Avg. Dependencies", f"{sum(d['Dependencies Count'] for d in dependencies) / len(dependencies):.1f}")
    with col4:
        st.metric("Max Depth", "4")


def _show_pipeline_flow() -> None:
    """Display pipeline data flow."""
    st.markdown("### Pipeline Data Flow")

    # Pipeline selector
    selected_pipeline = st.selectbox(
        "Select Pipeline",
        options=["customer_processing", "orders_processing", "product_analytics", "daily_aggregation"],
        key="pipeline_flow_select"
    )

    st.markdown("---")

    # Pipeline stages visualization
    st.markdown(f"#### Pipeline: {selected_pipeline}")

    _render_pipeline_stages(selected_pipeline)

    # Stage details
    st.markdown("---")
    st.markdown("### Stage Details")

    stages = _get_pipeline_stages(selected_pipeline)

    for i, stage in enumerate(stages):
        with st.expander(f"Stage {i + 1}: {stage['name']}", expanded=i == 0):
            col1, col2, col3 = st.columns(3)

            with col1:
                st.markdown("**Input**")
                for inp in stage.get("inputs", []):
                    st.markdown(f"- {inp}")

            with col2:
                st.markdown("**Transformations**")
                for transform in stage.get("transformations", []):
                    st.markdown(f"- {transform}")

            with col3:
                st.markdown("**Output**")
                for out in stage.get("outputs", []):
                    st.markdown(f"- {out}")

            if stage.get("sql"):
                st.markdown("**SQL Query**")
                st.code(stage["sql"], language="sql")


def _render_pipeline_stages(pipeline: str) -> None:
    """Render pipeline stages visualization."""
    stages = _get_pipeline_stages(pipeline)

    st.markdown("""
    <style>
    .pipeline-flow {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        padding: 1.5rem;
        background: var(--surface);
        border-radius: var(--radius-lg);
        overflow-x: auto;
    }
    .pipeline-stage {
        display: flex;
        flex-direction: column;
        align-items: center;
        min-width: 120px;
    }
    .stage-icon {
        width: 50px;
        height: 50px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 1.5rem;
        margin-bottom: 0.5rem;
    }
    .stage-extract {
        background: var(--info-light);
        border: 2px solid var(--info);
    }
    .stage-transform {
        background: var(--warning-light);
        border: 2px solid var(--warning);
    }
    .stage-load {
        background: var(--success-light);
        border: 2px solid var(--success);
    }
    .stage-validate {
        background: var(--danger-light);
        border: 2px solid var(--danger);
    }
    .stage-name {
        font-weight: 600;
        font-size: 0.875rem;
        text-align: center;
    }
    .stage-type {
        font-size: 0.75rem;
        color: var(--text-muted);
    }
    .flow-arrow {
        color: var(--text-muted);
        font-size: 1.25rem;
    }
    </style>
    """, unsafe_allow_html=True)

    stage_html = '<div class="pipeline-flow">'

    for i, stage in enumerate(stages):
        stage_type = stage.get("type", "transform").lower()
        icons = {
            "extract": "📥",
            "transform": "⚙️",
            "load": "📤",
            "validate": "✓"
        }
        icon = icons.get(stage_type, "⚙️")

        stage_html += f"""
        <div class="pipeline-stage">
            <div class="stage-icon stage-{stage_type}">{icon}</div>
            <div class="stage-name">{stage['name']}</div>
            <div class="stage-type">{stage_type.title()}</div>
        </div>
        """

        if i < len(stages) - 1:
            stage_html += '<div class="flow-arrow">→</div>'

    stage_html += '</div>'
    st.markdown(stage_html, unsafe_allow_html=True)


def _get_pipeline_stages(pipeline: str) -> list[dict[str, Any]]:
    """Get stages for a pipeline."""
    pipelines = {
        "customer_processing": [
            {
                "name": "Extract Source",
                "type": "extract",
                "inputs": ["Salesforce CRM"],
                "outputs": ["bronze.raw_customers"],
                "transformations": ["Full extract", "Schema mapping"]
            },
            {
                "name": "Validate Data",
                "type": "validate",
                "inputs": ["bronze.raw_customers"],
                "outputs": ["bronze.raw_customers_validated"],
                "transformations": ["Null checks", "Email validation", "Date format"]
            },
            {
                "name": "Clean & Dedupe",
                "type": "transform",
                "inputs": ["bronze.raw_customers_validated"],
                "outputs": ["silver.customers"],
                "transformations": ["Standardize names", "Remove duplicates", "SCD Type 2"],
                "sql": """
MERGE INTO silver.customers t
USING bronze.raw_customers_validated s
ON t.customer_id = s.id
WHEN MATCHED AND t.hash != s.hash THEN
    UPDATE SET is_current = false, end_date = current_timestamp()
WHEN NOT MATCHED THEN
    INSERT (customer_id, full_name, email, status, is_current, start_date)
    VALUES (s.id, concat(s.first_name, ' ', s.last_name), lower(s.email), upper(s.status), true, current_timestamp())
                """
            },
            {
                "name": "Build 360 View",
                "type": "transform",
                "inputs": ["silver.customers", "silver.orders"],
                "outputs": ["gold.customer_360"],
                "transformations": ["Join orders", "Calculate metrics", "Add segments"]
            },
            {
                "name": "Write Gold",
                "type": "load",
                "inputs": ["gold.customer_360"],
                "outputs": ["Delta Lake"],
                "transformations": ["Partition by date", "Optimize", "Z-Order"]
            }
        ],
        "orders_processing": [
            {
                "name": "Extract Orders",
                "type": "extract",
                "inputs": ["PostgreSQL Orders DB"],
                "outputs": ["bronze.raw_orders"],
                "transformations": ["Incremental extract", "CDC capture"]
            },
            {
                "name": "Validate Orders",
                "type": "validate",
                "inputs": ["bronze.raw_orders"],
                "outputs": ["bronze.raw_orders_validated"],
                "transformations": ["Amount checks", "Date validation", "FK checks"]
            },
            {
                "name": "Transform Orders",
                "type": "transform",
                "inputs": ["bronze.raw_orders_validated"],
                "outputs": ["silver.orders"],
                "transformations": ["Enrich data", "Calculate totals", "Apply business rules"]
            },
            {
                "name": "Load Silver",
                "type": "load",
                "inputs": ["silver.orders"],
                "outputs": ["Delta Lake"],
                "transformations": ["Merge upsert", "Update metrics"]
            }
        ],
        "product_analytics": [
            {
                "name": "Extract Products",
                "type": "extract",
                "inputs": ["S3 Product Files"],
                "outputs": ["bronze.raw_products"],
                "transformations": ["Parse CSV/JSON", "Schema inference"]
            },
            {
                "name": "Clean Products",
                "type": "transform",
                "inputs": ["bronze.raw_products"],
                "outputs": ["silver.products"],
                "transformations": ["Standardize", "Categorize", "Enrich"]
            },
            {
                "name": "Build Analytics",
                "type": "transform",
                "inputs": ["silver.products", "silver.orders"],
                "outputs": ["gold.product_analytics"],
                "transformations": ["Calculate sales", "Trend analysis", "Forecasting"]
            }
        ],
        "daily_aggregation": [
            {
                "name": "Aggregate Sales",
                "type": "transform",
                "inputs": ["silver.orders", "silver.products"],
                "outputs": ["gold.daily_sales"],
                "transformations": ["Group by date", "Sum amounts", "Count orders"]
            },
            {
                "name": "Build Metrics",
                "type": "transform",
                "inputs": ["gold.daily_sales"],
                "outputs": ["gold.sales_metrics"],
                "transformations": ["Moving averages", "YoY comparison", "KPIs"]
            }
        ]
    }

    return pipelines.get(pipeline, [])


def _show_impact_analysis() -> None:
    """Display impact analysis for changes."""
    st.markdown("### Impact Analysis")
    st.markdown("*Analyze the downstream impact of schema or data changes*")

    # Analysis type
    analysis_type = st.radio(
        "Analysis Type",
        options=["Schema Change", "Data Change", "Pipeline Change"],
        horizontal=True
    )

    st.markdown("---")

    if analysis_type == "Schema Change":
        _show_schema_impact_analysis()
    elif analysis_type == "Data Change":
        _show_data_impact_analysis()
    else:
        _show_pipeline_impact_analysis()


def _show_schema_impact_analysis() -> None:
    """Show schema change impact analysis."""
    col1, col2 = st.columns(2)

    with col1:
        source_table = st.selectbox(
            "Select Table",
            options=["bronze.raw_customers", "bronze.raw_orders", "silver.customers",
                     "silver.orders", "gold.customer_360"],
            key="schema_impact_table"
        )

    with col2:
        change_type = st.selectbox(
            "Change Type",
            options=["Add Column", "Remove Column", "Modify Column Type", "Rename Column"]
        )

    if change_type in ["Remove Column", "Modify Column Type", "Rename Column"]:
        column = st.selectbox(
            "Select Column",
            options=["customer_id", "full_name", "email", "status", "created_at"]
        )

    if st.button("Analyze Impact", type="primary"):
        st.markdown("---")
        st.markdown("### Impact Analysis Results")

        # Affected downstream tables
        st.markdown("#### Affected Downstream Tables")
        affected = [
            {"Table": "silver.customers", "Impact": "High", "Columns Affected": "customer_id, full_name"},
            {"Table": "gold.customer_360", "Impact": "High", "Columns Affected": "customer_id"},
            {"Table": "gold.sales_metrics", "Impact": "Medium", "Columns Affected": "customer_id (join)"},
        ]
        st.dataframe(affected, use_container_width=True, hide_index=True)

        # Affected pipelines
        st.markdown("#### Affected Pipelines")
        pipelines = [
            {"Pipeline": "customer_processing", "Status": "Will Break", "Action Required": "Update transformation"},
            {"Pipeline": "daily_aggregation", "Status": "May Break", "Action Required": "Verify join conditions"},
        ]
        st.dataframe(pipelines, use_container_width=True, hide_index=True)

        # Affected reports/dashboards
        st.markdown("#### Affected Consumers")
        consumers = [
            {"Consumer": "Customer Dashboard", "Type": "BI Report", "Impact": "High"},
            {"Consumer": "Churn Prediction Model", "Type": "ML Model", "Impact": "Medium"},
            {"Consumer": "Weekly Report", "Type": "Report", "Impact": "Low"},
        ]
        st.dataframe(consumers, use_container_width=True, hide_index=True)

        # Recommendations
        st.markdown("#### Recommendations")
        st.warning("""
        **High Impact Change Detected**

        This change will affect 3 downstream tables, 2 pipelines, and 3 consumers.

        **Recommended Actions:**
        1. Notify downstream data consumers before making changes
        2. Update transformation logic in `customer_processing` pipeline
        3. Schedule change during maintenance window
        4. Run data validation after change is applied
        5. Monitor pipeline executions for 24 hours post-change
        """)


def _show_data_impact_analysis() -> None:
    """Show data change impact analysis."""
    col1, col2 = st.columns(2)

    with col1:
        source_table = st.selectbox(
            "Source Table",
            options=["bronze.raw_customers", "bronze.raw_orders", "silver.customers"],
            key="data_impact_table"
        )

    with col2:
        data_change = st.selectbox(
            "Change Scenario",
            options=["Late Arriving Data", "Data Correction", "Data Deletion", "Bulk Update"]
        )

    affected_records = st.number_input(
        "Estimated Affected Records",
        min_value=1,
        max_value=10000000,
        value=1000
    )

    if st.button("Analyze Data Impact", type="primary"):
        st.markdown("---")
        st.markdown("### Data Impact Analysis")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Affected Records", f"{affected_records:,}")
        with col2:
            st.metric("Downstream Records", f"{affected_records * 5:,}")
        with col3:
            st.metric("Processing Time Est.", "~15 min")

        st.markdown("#### Downstream Propagation")
        st.info(f"""
        Changes to {affected_records:,} records in `{source_table}` will propagate to:
        - **silver.customers**: {affected_records:,} records (direct dependency)
        - **gold.customer_360**: {affected_records:,} records (aggregated view)
        - **gold.sales_metrics**: ~{affected_records * 3:,} records (joined data)
        """)


def _show_pipeline_impact_analysis() -> None:
    """Show pipeline change impact analysis."""
    pipeline = st.selectbox(
        "Select Pipeline",
        options=["customer_processing", "orders_processing", "product_analytics"],
        key="pipeline_impact_select"
    )

    change = st.text_area(
        "Describe the planned change",
        placeholder="e.g., Adding a new filter condition to remove inactive customers..."
    )

    if st.button("Analyze Pipeline Impact", type="primary") and change:
        st.markdown("---")
        st.markdown("### Pipeline Impact Analysis")

        st.success(f"""
        **Analysis for: {pipeline}**

        Based on the described change, the following impacts were identified:

        **Tables Affected:**
        - Output tables will be updated with new filtering logic
        - Historical data remains unchanged (append-only)

        **Downstream Consumers:**
        - BI dashboards will reflect changes in next refresh
        - ML models may need retraining

        **Estimated Impact:**
        - Processing time: No significant change expected
        - Data volume: May reduce by ~5-10%

        **Recommended Testing:**
        1. Run pipeline in test environment first
        2. Compare output counts before/after
        3. Validate data quality metrics
        """)
