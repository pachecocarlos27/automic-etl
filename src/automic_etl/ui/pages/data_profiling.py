"""Data Profiling Page for Automic ETL UI."""

from __future__ import annotations

import streamlit as st
import polars as pl
import json


def show_data_profiling_page():
    """Display the data profiling page."""
    st.title("📊 Data Profiling & Quality")
    st.markdown("Analyze data quality, detect anomalies, and profile your datasets.")

    # Tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs([
        "📋 Profile Data",
        "🔒 PII Detection",
        "⚠️ Data Quality",
        "📈 Statistics",
    ])

    with tab1:
        show_profile_section()

    with tab2:
        show_pii_detection_section()

    with tab3:
        show_data_quality_section()

    with tab4:
        show_statistics_section()


def show_profile_section():
    """Show data profiling section."""
    st.subheader("Data Profile")

    # Table selection
    col1, col2 = st.columns(2)
    with col1:
        namespace = st.selectbox("Namespace", ["bronze", "silver", "gold"])
    with col2:
        table = st.selectbox(
            "Table",
            ["customers", "orders", "products", "transactions"],
        )

    if st.button("🔍 Generate Profile", type="primary"):
        with st.spinner("Analyzing data..."):
            show_profile_results()


def show_profile_results():
    """Display profile results."""
    st.markdown("---")

    # Overview metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Rows", "125,432")
    col2.metric("Total Columns", "15")
    col3.metric("Missing Values", "2.3%")
    col4.metric("Duplicates", "0.1%")

    st.markdown("---")

    # Column profiling
    st.subheader("Column Analysis")

    columns_data = [
        {
            "column": "customer_id",
            "type": "String",
            "non_null": "100%",
            "unique": "100%",
            "top_value": "CUST0001",
            "pii_risk": "Low",
        },
        {
            "column": "full_name",
            "type": "String",
            "non_null": "99.8%",
            "unique": "87%",
            "top_value": "John Smith",
            "pii_risk": "High",
        },
        {
            "column": "email",
            "type": "String",
            "non_null": "98.5%",
            "unique": "99.2%",
            "top_value": "user@email.com",
            "pii_risk": "High",
        },
        {
            "column": "phone",
            "type": "String",
            "non_null": "95.2%",
            "unique": "98.5%",
            "top_value": "555-123-4567",
            "pii_risk": "High",
        },
        {
            "column": "annual_revenue",
            "type": "Float64",
            "non_null": "100%",
            "unique": "45%",
            "top_value": "$50,000",
            "pii_risk": "Low",
        },
        {
            "column": "segment",
            "type": "String",
            "non_null": "100%",
            "unique": "0.03%",
            "top_value": "Enterprise",
            "pii_risk": "None",
        },
        {
            "column": "created_at",
            "type": "Datetime",
            "non_null": "100%",
            "unique": "85%",
            "top_value": "2024-01-15",
            "pii_risk": "None",
        },
    ]

    # Display as dataframe
    st.dataframe(
        columns_data,
        use_container_width=True,
        column_config={
            "column": "Column Name",
            "type": "Data Type",
            "non_null": "Non-Null %",
            "unique": "Unique %",
            "top_value": "Most Common",
            "pii_risk": st.column_config.TextColumn(
                "PII Risk",
                help="Risk level for PII data",
            ),
        },
    )

    # Detailed column analysis
    st.markdown("---")
    st.subheader("Detailed Column Analysis")

    selected_col = st.selectbox(
        "Select column for detailed analysis",
        [c["column"] for c in columns_data],
    )

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Value Distribution**")
        # Mock distribution chart
        st.bar_chart({
            "Value": [45, 30, 15, 7, 3],
        })

    with col2:
        st.markdown("**Statistics**")
        st.json({
            "count": 125432,
            "null_count": 234,
            "unique_count": 115890,
            "min": "A001",
            "max": "Z999",
            "mean_length": 8.5,
        })


def show_pii_detection_section():
    """Show PII detection section."""
    st.subheader("PII Detection")

    st.markdown("""
    Automatically detect and classify Personally Identifiable Information (PII)
    in your datasets using AI-powered analysis.
    """)

    # Table selection
    col1, col2 = st.columns(2)
    with col1:
        namespace = st.selectbox("Namespace", ["bronze", "silver", "gold"], key="pii_ns")
    with col2:
        table = st.selectbox(
            "Table",
            ["customers", "orders", "products"],
            key="pii_table",
        )

    # Detection options
    st.markdown("**Detection Options**")
    col1, col2 = st.columns(2)
    with col1:
        pii_types = st.multiselect(
            "PII Types to Detect",
            [
                "Email", "Phone", "SSN", "Credit Card",
                "Name", "Address", "Date of Birth", "IP Address",
            ],
            default=["Email", "Phone", "Name", "Address"],
        )
    with col2:
        sensitivity = st.slider("Sensitivity", 1, 10, 7)
        use_llm = st.checkbox("Use LLM for enhanced detection", value=True)

    if st.button("🔒 Detect PII", type="primary"):
        with st.spinner("Scanning for PII..."):
            show_pii_results()


def show_pii_results():
    """Display PII detection results."""
    st.markdown("---")

    # Overall risk
    col1, col2, col3 = st.columns(3)
    col1.metric("Overall Risk Level", "HIGH", delta="3 columns")
    col2.metric("PII Columns Found", "4")
    col3.metric("Sensitive Records", "125,432")

    st.warning("⚠️ This dataset contains high-risk PII data. Consider applying masking or encryption.")

    st.markdown("---")

    # PII findings
    st.subheader("PII Findings")

    pii_findings = [
        {
            "column": "full_name",
            "pii_type": "PERSON_NAME",
            "confidence": 0.98,
            "sample": "John S***",
            "recommendation": "Apply pseudonymization",
        },
        {
            "column": "email",
            "pii_type": "EMAIL",
            "confidence": 0.99,
            "sample": "j***@email.com",
            "recommendation": "Hash or encrypt",
        },
        {
            "column": "phone",
            "pii_type": "PHONE_NUMBER",
            "confidence": 0.95,
            "sample": "555-***-****",
            "recommendation": "Mask last digits",
        },
        {
            "column": "address",
            "pii_type": "ADDRESS",
            "confidence": 0.92,
            "sample": "*** Main St, ***",
            "recommendation": "Generalize to region",
        },
    ]

    for finding in pii_findings:
        with st.expander(f"🔒 {finding['column']} - {finding['pii_type']}"):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**Confidence:** {finding['confidence']*100:.0f}%")
                st.markdown(f"**Sample (masked):** `{finding['sample']}`")
            with col2:
                st.markdown(f"**Recommendation:** {finding['recommendation']}")

                action = st.selectbox(
                    "Action",
                    ["No action", "Mask", "Encrypt", "Hash", "Remove"],
                    key=f"action_{finding['column']}",
                )

    st.markdown("---")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("📋 Generate Report"):
            st.success("✅ PII report generated!")
    with col2:
        if st.button("🔧 Apply Recommendations", type="primary"):
            st.info("Applying PII protections...")


def show_data_quality_section():
    """Show data quality section."""
    st.subheader("Data Quality Rules")

    st.markdown("""
    Define and monitor data quality rules for your datasets.
    Rules are checked automatically during pipeline runs.
    """)

    # Table selection
    col1, col2 = st.columns(2)
    with col1:
        namespace = st.selectbox("Namespace", ["bronze", "silver", "gold"], key="dq_ns")
    with col2:
        table = st.selectbox(
            "Table",
            ["customers", "orders", "products"],
            key="dq_table",
        )

    st.markdown("---")

    # Add new rule
    with st.expander("➕ Add Data Quality Rule", expanded=False):
        col1, col2 = st.columns(2)

        with col1:
            rule_name = st.text_input("Rule Name")
            rule_type = st.selectbox(
                "Rule Type",
                [
                    "not_null", "unique", "in_range", "matches_regex",
                    "foreign_key", "custom_sql", "ai_anomaly",
                ],
            )
            column = st.selectbox("Column", ["customer_id", "email", "revenue", "status"])

        with col2:
            if rule_type == "in_range":
                min_val = st.number_input("Min Value", value=0)
                max_val = st.number_input("Max Value", value=1000000)
            elif rule_type == "matches_regex":
                pattern = st.text_input("Regex Pattern", value=r"^[A-Z]{2}\d+$")
            elif rule_type == "custom_sql":
                sql = st.text_area("SQL Condition")
            elif rule_type == "ai_anomaly":
                st.info("🤖 AI will automatically detect anomalies")
                threshold = st.slider("Sensitivity", 1, 10, 5)

            severity = st.selectbox("Severity", ["error", "warning", "info"])

        if st.button("Add Rule"):
            st.success(f"✅ Rule '{rule_name}' added!")

    st.markdown("---")

    # Existing rules
    st.subheader("Active Rules")

    rules = [
        {"name": "customer_id_not_null", "type": "not_null", "column": "customer_id", "status": "✅", "violations": 0},
        {"name": "email_format", "type": "matches_regex", "column": "email", "status": "✅", "violations": 3},
        {"name": "revenue_positive", "type": "in_range", "column": "revenue", "status": "⚠️", "violations": 12},
        {"name": "unique_emails", "type": "unique", "column": "email", "status": "❌", "violations": 45},
        {"name": "anomaly_detection", "type": "ai_anomaly", "column": "revenue", "status": "✅", "violations": 2},
    ]

    for rule in rules:
        col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 1, 1])
        col1.write(rule["name"])
        col2.write(rule["type"])
        col3.write(rule["column"])
        col4.write(rule["status"])
        col5.write(f"{rule['violations']}")

    st.markdown("---")

    # Run validation
    if st.button("▶️ Run Validation", type="primary"):
        with st.spinner("Running data quality checks..."):
            progress = st.progress(0)
            for i in range(100):
                progress.progress(i + 1)

            st.success("✅ Validation complete!")
            st.markdown("**Results:** 5 rules checked, 2 warnings, 1 error")


def show_statistics_section():
    """Show statistical analysis section."""
    st.subheader("Statistical Analysis")

    # Table selection
    col1, col2 = st.columns(2)
    with col1:
        namespace = st.selectbox("Namespace", ["bronze", "silver", "gold"], key="stats_ns")
    with col2:
        table = st.selectbox(
            "Table",
            ["customers", "orders", "products"],
            key="stats_table",
        )

    if st.button("📈 Generate Statistics", type="primary"):
        with st.spinner("Calculating statistics..."):
            show_statistics_results()


def show_statistics_results():
    """Display statistical results."""
    st.markdown("---")

    # Numeric columns statistics
    st.subheader("Numeric Column Statistics")

    stats = {
        "Column": ["annual_revenue", "order_count", "days_active"],
        "Count": [125432, 125432, 125432],
        "Mean": [45230.50, 12.3, 234.5],
        "Std": [28450.20, 8.7, 156.2],
        "Min": [0, 0, 1],
        "25%": [15000, 5, 89],
        "50%": [38000, 10, 212],
        "75%": [65000, 18, 345],
        "Max": [250000, 156, 1095],
    }

    st.dataframe(stats, use_container_width=True)

    st.markdown("---")

    # Correlations
    st.subheader("Correlation Matrix")
    col1, col2 = st.columns([2, 1])

    with col1:
        # Mock correlation heatmap using table
        corr_data = {
            "": ["revenue", "orders", "days_active"],
            "revenue": [1.00, 0.75, 0.45],
            "orders": [0.75, 1.00, 0.62],
            "days_active": [0.45, 0.62, 1.00],
        }
        st.dataframe(corr_data, use_container_width=True)

    with col2:
        st.markdown("**Key Insights:**")
        st.markdown("- Strong correlation between revenue and orders (0.75)")
        st.markdown("- Moderate correlation between days active and orders (0.62)")
        st.markdown("- Weak correlation between revenue and days active (0.45)")

    st.markdown("---")

    # Distribution analysis
    st.subheader("Distribution Analysis")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Revenue Distribution**")
        st.bar_chart({"count": [2500, 4500, 3500, 2000, 1500, 800, 400, 200, 100, 50]})

    with col2:
        st.markdown("**Order Count Distribution**")
        st.bar_chart({"count": [1500, 3500, 5500, 4000, 2500, 1500, 800, 400, 200, 100]})

    # AI Insights
    st.markdown("---")
    st.subheader("🤖 AI-Generated Insights")

    with st.spinner("Generating insights..."):
        st.info("""
        **Key Findings:**
        1. Revenue distribution is right-skewed, with most customers in the $15K-$65K range
        2. Top 10% of customers contribute 45% of total revenue
        3. Order frequency increases significantly after 90 days of activity
        4. Detected 23 potential outliers in the revenue column
        5. Customer segments show distinct behavioral patterns

        **Recommendations:**
        - Consider segmenting customers by revenue tier for targeted campaigns
        - Investigate the 23 revenue outliers for data quality issues
        - Implement retention strategies for customers approaching 90-day mark
        """)


# Run page when loaded by Streamlit
show_data_profiling_page()
