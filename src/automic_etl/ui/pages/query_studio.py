"""Query Studio Page for Automic ETL UI."""

from __future__ import annotations

import streamlit as st


def show_query_studio_page():
    """Display the query studio page."""
    st.title("💬 Query Studio")
    st.markdown("Query your data using natural language or SQL.")

    # Tabs for different query modes
    tab1, tab2, tab3 = st.tabs([
        "🗣️ Natural Language",
        "📝 SQL Editor",
        "📚 Query History",
    ])

    with tab1:
        show_natural_language_query()

    with tab2:
        show_sql_editor()

    with tab3:
        show_query_history()


def show_natural_language_query():
    """Show natural language query interface."""
    st.subheader("Ask Questions in Natural Language")

    st.markdown("""
    Describe what data you want to see, and AI will generate the SQL query for you.
    """)

    # Context selection
    st.markdown("**Select Tables for Context**")
    selected_tables = st.multiselect(
        "Available Tables",
        [
            "bronze.raw_customers",
            "bronze.raw_orders",
            "silver.customers",
            "silver.orders",
            "silver.products",
            "gold.customer_metrics",
            "gold.daily_sales",
        ],
        default=["silver.customers", "silver.orders"],
        label_visibility="collapsed",
    )

    st.markdown("---")

    # Query input
    query = st.text_area(
        "Ask a question about your data",
        placeholder="Example: Show me the top 10 customers by revenue who made purchases in the last 30 days",
        height=100,
    )

    col1, col2 = st.columns([1, 5])
    with col1:
        generate_btn = st.button("🤖 Generate SQL", type="primary")

    # Example queries
    with st.expander("💡 Example Questions"):
        examples = [
            "Show me the top 10 customers by revenue",
            "What is the average order value by customer segment?",
            "List all customers from Texas who haven't ordered in 30 days",
            "Count orders by month for 2024",
            "Which products are most frequently purchased together?",
            "Show customers whose spending decreased compared to last quarter",
        ]
        for example in examples:
            if st.button(example, key=f"example_{example[:20]}"):
                query = example

    if generate_btn and query:
        with st.spinner("Generating SQL..."):
            show_generated_query(query)


def show_generated_query(question: str):
    """Show the generated SQL query."""
    st.markdown("---")

    # Mock generated SQL based on question
    generated_sql = """SELECT
    c.customer_id,
    c.full_name,
    c.email,
    c.segment,
    SUM(o.amount) as total_revenue,
    COUNT(o.order_id) as order_count
FROM silver.customers c
JOIN silver.orders o ON c.customer_id = o.customer_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY c.customer_id, c.full_name, c.email, c.segment
ORDER BY total_revenue DESC
LIMIT 10;"""

    col1, col2 = st.columns([3, 1])

    with col1:
        st.markdown("**Generated SQL:**")
        st.code(generated_sql, language="sql")

    with col2:
        st.markdown("**Confidence:** 95%")
        st.markdown("**Tables Used:**")
        st.markdown("- silver.customers")
        st.markdown("- silver.orders")

    # Explanation
    with st.expander("📖 Explanation"):
        st.markdown("""
        **Query Breakdown:**
        1. Joins the customers table with orders on customer_id
        2. Filters orders from the last 30 days
        3. Aggregates to calculate total revenue and order count per customer
        4. Sorts by revenue descending
        5. Limits to top 10 results
        """)

    st.markdown("---")

    # Actions
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if st.button("▶️ Execute Query", type="primary"):
            show_query_results()

    with col2:
        if st.button("✏️ Edit SQL"):
            st.session_state["edit_sql"] = generated_sql

    with col3:
        if st.button("💾 Save Query"):
            st.success("Query saved!")

    with col4:
        if st.button("📋 Copy SQL"):
            st.info("SQL copied to clipboard!")


def show_query_results():
    """Display query results."""
    st.markdown("---")
    st.subheader("Query Results")

    # Mock results
    results = {
        "customer_id": ["CUST001", "CUST015", "CUST042", "CUST008", "CUST023"],
        "full_name": ["John Smith", "Jane Doe", "Robert Johnson", "Emily Williams", "Michael Brown"],
        "email": ["john@email.com", "jane@email.com", "robert@email.com", "emily@email.com", "michael@email.com"],
        "segment": ["VIP", "Enterprise", "Enterprise", "VIP", "Premium"],
        "total_revenue": [125000, 98500, 87200, 76800, 65400],
        "order_count": [45, 38, 32, 28, 24],
    }

    col1, col2, col3 = st.columns(3)
    col1.metric("Rows Returned", "5")
    col2.metric("Execution Time", "0.23s")
    col3.metric("Data Scanned", "12.4 MB")

    st.dataframe(results, use_container_width=True)

    # Export options
    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        st.download_button(
            "📥 Download CSV",
            data="customer_id,full_name,email,segment,total_revenue,order_count\n",
            file_name="query_results.csv",
            mime="text/csv",
        )
    with col2:
        st.download_button(
            "📥 Download JSON",
            data="[]",
            file_name="query_results.json",
            mime="application/json",
        )


def show_sql_editor():
    """Show SQL editor interface."""
    st.subheader("SQL Editor")

    # Table browser
    col1, col2 = st.columns([1, 3])

    with col1:
        st.markdown("**Table Browser**")
        with st.expander("🥉 Bronze", expanded=True):
            st.markdown("- raw_customers")
            st.markdown("- raw_orders")
            st.markdown("- raw_products")

        with st.expander("🥈 Silver"):
            st.markdown("- customers")
            st.markdown("- orders")
            st.markdown("- products")

        with st.expander("🥇 Gold"):
            st.markdown("- customer_metrics")
            st.markdown("- daily_sales")
            st.markdown("- product_performance")

    with col2:
        # SQL editor
        sql = st.text_area(
            "SQL Query",
            value=st.session_state.get("edit_sql", "SELECT * FROM silver.customers LIMIT 10;"),
            height=200,
            label_visibility="collapsed",
        )

        # Editor toolbar
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            if st.button("▶️ Run", type="primary"):
                show_query_results()
        with col2:
            if st.button("📋 Format"):
                st.info("SQL formatted!")
        with col3:
            if st.button("✅ Validate"):
                st.success("Query is valid!")
        with col4:
            if st.button("💾 Save"):
                st.success("Query saved!")
        with col5:
            if st.button("🤖 Explain"):
                show_query_explanation()


def show_query_explanation():
    """Show AI explanation of SQL query."""
    st.markdown("---")
    st.subheader("🤖 Query Explanation")

    st.info("""
    **What this query does:**

    This query retrieves all columns from the `silver.customers` table, limiting
    the results to the first 10 rows.

    **Performance Notes:**
    - Using SELECT * retrieves all columns - consider selecting only needed columns
    - LIMIT 10 ensures quick execution regardless of table size
    - No filters applied - will scan the entire table

    **Suggested Optimizations:**
    - Specify needed columns instead of SELECT *
    - Add WHERE clause to filter relevant data
    - Consider adding ORDER BY for deterministic results
    """)


def show_query_history():
    """Show query history."""
    st.subheader("Query History")

    # Filter
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        search = st.text_input("🔍 Search queries", placeholder="Search...")
    with col2:
        date_filter = st.selectbox("Time", ["Today", "Last 7 days", "Last 30 days", "All time"])
    with col3:
        type_filter = st.selectbox("Type", ["All", "Natural Language", "SQL"])

    st.markdown("---")

    # Query history
    history = [
        {
            "time": "5 min ago",
            "query": "Show me the top 10 customers by revenue",
            "type": "NL",
            "duration": "0.23s",
            "rows": 10,
            "status": "✅",
        },
        {
            "time": "15 min ago",
            "query": "SELECT * FROM silver.customers WHERE segment = 'VIP'",
            "type": "SQL",
            "duration": "0.45s",
            "rows": 234,
            "status": "✅",
        },
        {
            "time": "1 hour ago",
            "query": "What is the total revenue by product category?",
            "type": "NL",
            "duration": "1.2s",
            "rows": 15,
            "status": "✅",
        },
        {
            "time": "2 hours ago",
            "query": "SELECT COUNT(*) FROM bronze.raw_orders GROUP BY date",
            "type": "SQL",
            "duration": "2.1s",
            "rows": 365,
            "status": "✅",
        },
        {
            "time": "3 hours ago",
            "query": "List customers who haven't ordered in 90 days",
            "type": "NL",
            "duration": "0.89s",
            "rows": 456,
            "status": "⚠️",
        },
    ]

    for item in history:
        with st.expander(f"{item['status']} {item['query'][:50]}..."):
            col1, col2 = st.columns([3, 1])

            with col1:
                st.markdown(f"**Query:**")
                st.code(item["query"], language="sql" if item["type"] == "SQL" else None)

            with col2:
                st.markdown(f"**Time:** {item['time']}")
                st.markdown(f"**Type:** {item['type']}")
                st.markdown(f"**Duration:** {item['duration']}")
                st.markdown(f"**Rows:** {item['rows']}")

            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("🔄 Re-run", key=f"rerun_{item['time']}"):
                    st.info("Re-running query...")
            with col2:
                if st.button("✏️ Edit", key=f"edit_{item['time']}"):
                    st.session_state["edit_sql"] = item["query"]
            with col3:
                if st.button("📋 Copy", key=f"copy_{item['time']}"):
                    st.info("Copied!")
