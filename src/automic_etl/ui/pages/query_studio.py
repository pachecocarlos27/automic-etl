"""Query Studio Page for Automic ETL UI - LLM-Powered SQL Interface."""

from __future__ import annotations

import json
import random
import time
import uuid
from datetime import datetime
from typing import Any

import pandas as pd
import streamlit as st

from automic_etl.ui.state import notify_success

# ============================================================================
# Input Validation Constants
# ============================================================================

MAX_QUERY_LENGTH = 5000
MIN_QUERY_LENGTH = 3
MAX_SQL_LENGTH = 10000

# Patterns that might indicate prompt injection attempts
SUSPICIOUS_PATTERNS = [
    "ignore previous",
    "ignore above",
    "disregard",
    "forget everything",
    "new instructions",
    "system prompt",
    "you are now",
    "act as",
    "pretend to be",
    "jailbreak",
]

# SQL injection patterns
SQL_INJECTION_PATTERNS = [
    "'; --",
    "' OR '1'='1",
    "'; DROP",
    "; DELETE",
    "; UPDATE",
    "; INSERT",
    "UNION SELECT",
    "/*",
    "*/",
    "xp_",
    "exec(",
    "execute(",
]


def _validate_query_input(query: str) -> tuple[bool, str | None]:
    """
    Validate natural language query input.

    Returns:
        Tuple of (is_valid, error_message)
    """
    if not query or not query.strip():
        return False, "Please enter a question"

    query = query.strip()

    # Length checks
    if len(query) < MIN_QUERY_LENGTH:
        return False, f"Question is too short (minimum {MIN_QUERY_LENGTH} characters)"

    if len(query) > MAX_QUERY_LENGTH:
        return False, f"Question is too long (maximum {MAX_QUERY_LENGTH} characters)"

    # Check for suspicious patterns (prompt injection)
    query_lower = query.lower()
    for pattern in SUSPICIOUS_PATTERNS:
        if pattern in query_lower:
            return False, "Your question contains disallowed patterns. Please rephrase."

    # Check for SQL injection patterns
    for pattern in SQL_INJECTION_PATTERNS:
        if pattern.lower() in query_lower:
            return False, "Your question contains disallowed SQL patterns. Please rephrase."

    return True, None


def _validate_sql_input(sql: str) -> tuple[bool, str | None]:
    """
    Validate SQL query input.

    Returns:
        Tuple of (is_valid, error_message)
    """
    if not sql or not sql.strip():
        return False, "Please enter a SQL query"

    sql = sql.strip()

    # Length check
    if len(sql) > MAX_SQL_LENGTH:
        return False, f"SQL query is too long (maximum {MAX_SQL_LENGTH} characters)"

    sql_upper = sql.upper()

    # Block dangerous operations
    dangerous_keywords = ["DROP", "TRUNCATE", "DELETE", "ALTER", "CREATE", "GRANT", "REVOKE"]
    for keyword in dangerous_keywords:
        # Check for keyword as a standalone word
        if f" {keyword} " in f" {sql_upper} ":
            return False, f"Dangerous SQL operation '{keyword}' is not allowed"

    # Block multiple statements
    if sql.count(';') > 1:
        return False, "Multiple SQL statements are not allowed"

    return True, None


def _sanitize_input(text: str) -> str:
    """Sanitize user input by removing potentially dangerous characters."""
    # Remove null bytes and control characters
    sanitized = ''.join(char for char in text if ord(char) >= 32 or char in '\n\t')
    return sanitized.strip()


def show_query_studio_page():
    """Display the query studio page with LLM-powered SQL interface."""
    st.markdown("""
    <div style="margin-bottom: 1.5rem;">
        <h1 style="font-size: 1.5rem; font-weight: 700; color: #111827; margin: 0 0 0.25rem; letter-spacing: -0.025em;">Query Studio</h1>
        <p style="font-size: 0.9375rem; color: #6B7280; margin: 0;">Query data using natural language or SQL</p>
    </div>
    """, unsafe_allow_html=True)

    # Initialize session state for query studio
    _init_query_studio_state()

    # Sidebar with conversation and schema info
    _show_sidebar()

    # Tabs for different query modes
    tab1, tab2, tab3, tab4 = st.tabs([
        "🗣️ Natural Language",
        "📝 SQL Editor",
        "💬 Conversation",
        "📚 Query History",
    ])

    with tab1:
        show_natural_language_query()

    with tab2:
        show_sql_editor()

    with tab3:
        show_conversation_view()

    with tab4:
        show_query_history()


def _init_query_studio_state():
    """Initialize session state for query studio."""
    if "conversation_id" not in st.session_state:
        st.session_state.conversation_id = str(uuid.uuid4())
    if "conversation_messages" not in st.session_state:
        st.session_state.conversation_messages = []
    if "query_results" not in st.session_state:
        st.session_state.query_results = None
    if "generated_sql" not in st.session_state:
        st.session_state.generated_sql = None
    if "selected_tables" not in st.session_state:
        st.session_state.selected_tables = []
    if "query_history_local" not in st.session_state:
        st.session_state.query_history_local = []


def _show_sidebar():
    """Show sidebar with schema browser and conversation controls."""
    with st.sidebar:
        st.subheader("🗂️ Schema Browser")

        # Get user's accessible tiers (mock for demo)
        user_tiers = st.session_state.get("user_tiers", ["bronze", "silver", "gold"])

        # Tier filter
        tier_filter = st.multiselect(
            "Data Tiers",
            ["bronze", "silver", "gold"],
            default=user_tiers,
            help="Filter tables by data tier"
        )

        # Schema browser by tier
        tables_by_tier = _get_available_tables(tier_filter)

        for tier, tables in tables_by_tier.items():
            if tables:
                tier_icon = {"bronze": "🥉", "silver": "🥈", "gold": "🥇"}.get(tier, "📊")
                with st.expander(f"{tier_icon} {tier.capitalize()} Layer", expanded=(tier == "gold")):
                    for table in tables:
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.markdown(f"**{table['name']}**")
                            st.caption(table.get("description", "")[:50])
                        with col2:
                            if st.button("➕", key=f"add_{tier}_{table['name']}", help="Add to context"):
                                full_name = f"{tier}.{table['name']}"
                                if full_name not in st.session_state.selected_tables:
                                    st.session_state.selected_tables.append(full_name)
                                    st.rerun()

        st.markdown("---")

        # Conversation controls
        st.subheader("💬 Conversation")
        st.caption(f"ID: {st.session_state.conversation_id[:8]}...")

        if st.button("🔄 New Conversation", use_container_width=True):
            st.session_state.conversation_id = str(uuid.uuid4())
            st.session_state.conversation_messages = []
            st.session_state.query_results = None
            st.session_state.generated_sql = None
            notify_success("Started new conversation")
            st.rerun()

        msg_count = len(st.session_state.conversation_messages)
        st.caption(f"{msg_count} messages in conversation")


def _get_available_tables(tier_filter: list[str]) -> dict[str, list[dict]]:
    """Get available tables organized by tier."""
    # In production, this would call the API
    all_tables = {
        "bronze": [
            {"name": "raw_customers", "description": "Raw customer data from CRM", "columns": 12, "rows": "50K"},
            {"name": "raw_orders", "description": "Raw order transactions", "columns": 15, "rows": "200K"},
            {"name": "raw_products", "description": "Raw product catalog", "columns": 8, "rows": "5K"},
            {"name": "raw_events", "description": "Raw clickstream events", "columns": 20, "rows": "1M+"},
        ],
        "silver": [
            {"name": "customers", "description": "Cleaned and validated customers", "columns": 10, "rows": "48K"},
            {"name": "orders", "description": "Validated orders with status", "columns": 12, "rows": "195K"},
            {"name": "products", "description": "Product master data", "columns": 10, "rows": "5K"},
            {"name": "order_items", "description": "Order line items", "columns": 8, "rows": "500K"},
        ],
        "gold": [
            {"name": "customer_summary", "description": "Customer 360 analytics view", "columns": 15, "rows": "48K"},
            {"name": "sales_metrics", "description": "Daily sales KPIs", "columns": 12, "rows": "365"},
            {"name": "product_performance", "description": "Product analytics", "columns": 10, "rows": "5K"},
            {"name": "cohort_analysis", "description": "Customer cohort metrics", "columns": 8, "rows": "1.2K"},
        ],
    }

    return {tier: tables for tier, tables in all_tables.items() if tier in tier_filter}


def show_natural_language_query():
    """Show natural language query interface with chat-like interaction."""
    st.subheader("🤖 Ask Questions in Natural Language")

    st.markdown("""
    Describe what data you want to see, and our AI will generate optimized SQL for you.
    You can refine your query through conversation.
    """)

    # Selected tables context
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("**Context Tables**")
        selected = st.multiselect(
            "Tables",
            _get_all_table_names(),
            default=st.session_state.selected_tables or ["silver.customers", "silver.orders"],
            key="nl_selected_tables",
            label_visibility="collapsed",
            help="Select tables to include in context for better query generation"
        )
        st.session_state.selected_tables = selected

    with col2:
        st.markdown("**Quick Actions**")
        if st.button("📋 Suggest Queries", use_container_width=True):
            _show_suggested_queries()

    st.markdown("---")

    # Chat-like interface
    _show_chat_interface()

    # Query input
    st.markdown("### Ask a Question")

    query = st.text_area(
        "Ask a question",
        placeholder="Example: Show me the top 10 customers by total spending in the last 90 days",
        height=100,
        key="nl_query_input",
        label_visibility="collapsed",
    )

    col1, col2, col3 = st.columns([2, 2, 4])

    with col1:
        generate_btn = st.button("🤖 Generate SQL", type="primary", use_container_width=True)

    with col2:
        if st.session_state.generated_sql:
            execute_btn = st.button("▶️ Execute", use_container_width=True)
        else:
            execute_btn = False

    with col3:
        # Confidence indicator
        if st.session_state.get("query_confidence"):
            conf = st.session_state.query_confidence
            conf_color = "green" if conf > 0.8 else "orange" if conf > 0.5 else "red"
            st.markdown(f"**Confidence:** :{conf_color}[{conf*100:.0f}%]")

    # Example queries
    _show_example_queries()

    # Handle generate
    if generate_btn:
        if query:
            # Sanitize and validate input
            sanitized_query = _sanitize_input(query)
            is_valid, error_msg = _validate_query_input(sanitized_query)

            if is_valid:
                _generate_sql_from_nl(sanitized_query)
            else:
                st.error(error_msg)
        else:
            st.warning("Please enter a question to generate SQL")

    # Handle execute
    if execute_btn and st.session_state.generated_sql:
        _execute_generated_sql()

    # Show generated SQL and results
    if st.session_state.generated_sql:
        _show_generated_sql_panel()

    if st.session_state.query_results:
        _show_query_results_panel()


def _show_chat_interface():
    """Show conversation history in chat format."""
    messages = st.session_state.conversation_messages

    if messages:
        with st.expander("💬 Conversation History", expanded=False):
            for msg in messages[-10:]:  # Show last 10 messages
                if msg["role"] == "user":
                    st.markdown(f"**You:** {msg['content']}")
                else:
                    st.markdown(f"**Assistant:** {msg['content'][:200]}...")
                    if msg.get("sql"):
                        st.code(msg["sql"][:200] + "..." if len(msg.get("sql", "")) > 200 else msg.get("sql", ""), language="sql")


def _show_example_queries():
    """Show clickable example queries."""
    with st.expander("💡 Example Questions", expanded=False):
        examples = [
            ("📊 Top Customers", "Show me the top 10 customers by total revenue"),
            ("📈 Sales Trends", "What are the monthly sales trends for 2024?"),
            ("🔍 Segment Analysis", "What is the average order value by customer segment?"),
            ("⚠️ At Risk", "Which customers haven't ordered in 90 days?"),
            ("📦 Popular Products", "What are the top selling products by quantity?"),
            ("💰 Revenue Growth", "Show customers with increasing spending month over month"),
        ]

        cols = st.columns(3)
        for idx, (label, query) in enumerate(examples):
            with cols[idx % 3]:
                if st.button(label, key=f"example_{idx}", use_container_width=True, help=query):
                    st.session_state.nl_query_input = query
                    st.rerun()


def _show_suggested_queries():
    """Show AI-suggested queries based on selected tables."""
    with st.spinner("Getting suggestions..."):
        # In production, call API endpoint /api/v1/queries/suggestions
        suggestions = [
            {
                "question": "What is the distribution of customers by segment?",
                "description": "Understand customer segmentation",
                "complexity": "simple"
            },
            {
                "question": "Show revenue by product category for each quarter",
                "description": "Quarterly product performance analysis",
                "complexity": "moderate"
            },
            {
                "question": "Identify customers with declining purchase frequency",
                "description": "Churn risk analysis",
                "complexity": "complex"
            },
        ]

        st.markdown("### 💡 Suggested Queries")
        for sug in suggestions:
            complexity_icon = {"simple": "🟢", "moderate": "🟡", "complex": "🔴"}.get(sug["complexity"], "⚪")
            col1, col2 = st.columns([4, 1])
            with col1:
                st.markdown(f"**{sug['question']}**")
                st.caption(f"{complexity_icon} {sug['description']}")
            with col2:
                if st.button("Use", key=f"sug_{sug['question'][:20]}"):
                    st.session_state.nl_query_input = sug["question"]
                    st.rerun()


def _generate_sql_from_nl(query: str):
    """Generate SQL from natural language query."""
    with st.spinner("🤖 AI is analyzing your question..."):
        # In production, call API: POST /api/v1/queries/natural
        # Simulate API response
        time.sleep(1)

        # Mock response
        generated_sql = _mock_generate_sql(query)

        st.session_state.generated_sql = generated_sql["sql"]
        st.session_state.query_confidence = generated_sql["confidence"]
        st.session_state.query_explanation = generated_sql["explanation"]
        st.session_state.query_intent = generated_sql["intent"]
        st.session_state.tables_used = generated_sql["tables_used"]
        st.session_state.query_warnings = generated_sql.get("warnings", [])
        st.session_state.follow_up_suggestions = generated_sql.get("suggestions", [])

        # Add to conversation
        st.session_state.conversation_messages.append({
            "role": "user",
            "content": query,
            "timestamp": datetime.now().isoformat()
        })
        st.session_state.conversation_messages.append({
            "role": "assistant",
            "content": generated_sql["explanation"],
            "sql": generated_sql["sql"],
            "timestamp": datetime.now().isoformat()
        })

        # Add to local history
        st.session_state.query_history_local.insert(0, {
            "query": query,
            "sql": generated_sql["sql"],
            "type": "NL",
            "timestamp": datetime.now(),
            "status": "generated"
        })

        st.rerun()


def _mock_generate_sql(query: str) -> dict[str, Any]:
    """Mock SQL generation for demo."""
    query_lower = query.lower()

    if "top" in query_lower and "customer" in query_lower:
        return {
            "sql": """SELECT
    c.customer_id,
    c.full_name,
    c.email,
    c.segment,
    SUM(o.total_amount) AS total_revenue,
    COUNT(DISTINCT o.order_id) AS order_count,
    MAX(o.order_date) AS last_order_date
FROM silver.customers c
JOIN silver.orders o ON c.customer_id = o.customer_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY c.customer_id, c.full_name, c.email, c.segment
ORDER BY total_revenue DESC
LIMIT 10;""",
            "explanation": "This query finds the top 10 customers by total spending in the last 90 days. It joins customers with orders, filters by date, aggregates revenue and order counts, and sorts by total revenue descending.",
            "intent": "aggregate",
            "confidence": 0.92,
            "tables_used": ["silver.customers", "silver.orders"],
            "columns_used": ["customer_id", "full_name", "email", "segment", "total_amount", "order_id", "order_date"],
            "warnings": [],
            "suggestions": [
                "Show breakdown by customer segment",
                "Compare with previous 90-day period",
                "Add average order value metric"
            ]
        }
    elif "trend" in query_lower or "monthly" in query_lower:
        return {
            "sql": """SELECT
    DATE_TRUNC('month', o.order_date) AS month,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.total_amount) AS total_revenue,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    AVG(o.total_amount) AS avg_order_value
FROM silver.orders o
WHERE o.order_date >= '2024-01-01'
GROUP BY DATE_TRUNC('month', o.order_date)
ORDER BY month;""",
            "explanation": "This query shows monthly sales trends for 2024, including order counts, revenue, unique customers, and average order value per month.",
            "intent": "trend",
            "confidence": 0.89,
            "tables_used": ["silver.orders"],
            "columns_used": ["order_date", "order_id", "total_amount", "customer_id"],
            "warnings": [],
            "suggestions": [
                "Compare with 2023 trends",
                "Break down by product category",
                "Add month-over-month growth percentage"
            ]
        }
    else:
        return {
            "sql": f"""-- Generated for: {query}
SELECT
    c.customer_id,
    c.full_name,
    c.segment,
    COUNT(o.order_id) AS orders
FROM silver.customers c
LEFT JOIN silver.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.full_name, c.segment
ORDER BY orders DESC
LIMIT 100;""",
            "explanation": f"Generated a query based on your request: '{query}'. This query shows customer order counts.",
            "intent": "select",
            "confidence": 0.75,
            "tables_used": ["silver.customers", "silver.orders"],
            "columns_used": ["customer_id", "full_name", "segment", "order_id"],
            "warnings": ["Low confidence - please verify the query matches your intent"],
            "suggestions": [
                "Add specific date filters",
                "Include additional metrics",
                "Specify sort order"
            ]
        }


def _show_generated_sql_panel():
    """Show the generated SQL with explanation and actions."""
    st.markdown("---")
    st.markdown("### 📝 Generated SQL")

    col1, col2 = st.columns([3, 1])

    with col1:
        st.code(st.session_state.generated_sql, language="sql")

    with col2:
        st.markdown("**Query Info**")

        # Confidence
        conf = st.session_state.get("query_confidence", 0)
        conf_color = "green" if conf > 0.8 else "orange" if conf > 0.5 else "red"
        st.markdown(f"Confidence: :{conf_color}[{conf*100:.0f}%]")

        # Intent
        intent = st.session_state.get("query_intent", "select")
        intent_icons = {
            "select": "📋", "aggregate": "📊", "join": "🔗",
            "filter": "🔍", "trend": "📈", "anomaly": "⚠️"
        }
        st.markdown(f"Intent: {intent_icons.get(intent, '📋')} {intent}")

        # Tables used
        st.markdown("**Tables:**")
        for table in st.session_state.get("tables_used", []):
            tier = table.split(".")[0] if "." in table else "silver"
            tier_icons = {"bronze": "🥉", "silver": "🥈", "gold": "🥇"}
            tier_icon = tier_icons.get(tier, "📊")
            st.markdown(f"{tier_icon} {table}")

    # Warnings
    if st.session_state.get("query_warnings"):
        for warning in st.session_state.query_warnings:
            st.warning(warning)

    # Explanation
    with st.expander("📖 Query Explanation", expanded=True):
        st.markdown(st.session_state.get("query_explanation", ""))

    # Actions
    st.markdown("**Actions:**")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if st.button("▶️ Execute Query", type="primary", use_container_width=True):
            _execute_generated_sql()

    with col2:
        if st.button("✏️ Edit SQL", use_container_width=True):
            st.session_state.edit_sql = st.session_state.generated_sql
            st.session_state.current_tab = "sql_editor"

    with col3:
        if st.button("💾 Save Query", use_container_width=True):
            notify_success("Query saved to your library")

    with col4:
        if st.button("🔄 Refine", use_container_width=True):
            st.session_state.show_refine = True

    # Refine input
    if st.session_state.get("show_refine"):
        st.markdown("---")
        st.markdown("### 🔄 Refine Your Query")
        refinement = st.text_input(
            "How would you like to modify the query?",
            placeholder="e.g., Add a filter for VIP customers only",
            key="refinement_input"
        )
        if st.button("Apply Refinement") and refinement:
            _refine_query(refinement)

    # Follow-up suggestions
    if st.session_state.get("follow_up_suggestions"):
        st.markdown("---")
        st.markdown("**💡 Follow-up Suggestions:**")
        cols = st.columns(len(st.session_state.follow_up_suggestions))
        for idx, suggestion in enumerate(st.session_state.follow_up_suggestions):
            with cols[idx]:
                if st.button(suggestion, key=f"followup_{idx}", use_container_width=True):
                    _generate_sql_from_nl(suggestion)


def _refine_query(refinement: str):
    """Refine the current query based on user feedback."""
    with st.spinner("Refining query..."):
        # In production, call API: POST /api/v1/queries/refine
        time.sleep(0.5)

        # Add refinement to conversation
        st.session_state.conversation_messages.append({
            "role": "user",
            "content": f"Refine: {refinement}",
            "timestamp": datetime.now().isoformat()
        })

        # Mock refinement (in production, call API)
        current_sql = st.session_state.generated_sql

        if "vip" in refinement.lower():
            refined_sql = current_sql.replace(
                "GROUP BY",
                "WHERE c.segment = 'VIP'\nGROUP BY"
            )
            explanation = "Added filter for VIP customers only."
        elif "limit" in refinement.lower():
            refined_sql = current_sql.replace("LIMIT 10", "LIMIT 20")
            explanation = "Increased result limit to 20."
        else:
            refined_sql = current_sql + f"\n-- Refinement: {refinement}"
            explanation = f"Applied refinement: {refinement}"

        st.session_state.generated_sql = refined_sql
        st.session_state.conversation_messages.append({
            "role": "assistant",
            "content": explanation,
            "sql": refined_sql,
            "timestamp": datetime.now().isoformat()
        })

        st.session_state.show_refine = False
        st.rerun()


def _execute_generated_sql():
    """Execute the generated SQL query."""
    with st.spinner("Executing query..."):
        # In production, call API: POST /api/v1/queries/sql
        time.sleep(0.5)

        # Mock results
        st.session_state.query_results = _mock_execute_query(st.session_state.generated_sql)

        # Update history
        if st.session_state.query_history_local:
            st.session_state.query_history_local[0]["status"] = "executed"
            st.session_state.query_history_local[0]["rows"] = st.session_state.query_results["row_count"]
            st.session_state.query_history_local[0]["duration"] = f"{random.uniform(0.1, 0.5):.2f}s"

        st.rerun()


def _mock_execute_query(sql: str) -> dict[str, Any]:
    """Mock query execution for demo."""
    if "customer" in sql.lower():
        return {
            "columns": ["customer_id", "full_name", "email", "segment", "total_revenue", "order_count"],
            "data": [
                ["CUST001", "John Smith", "john.smith@email.com", "VIP", 125000.00, 45],
                ["CUST015", "Jane Doe", "jane.doe@email.com", "Enterprise", 98500.00, 38],
                ["CUST042", "Robert Johnson", "robert.j@email.com", "Enterprise", 87200.00, 32],
                ["CUST008", "Emily Williams", "emily.w@email.com", "VIP", 76800.00, 28],
                ["CUST023", "Michael Brown", "michael.b@email.com", "Premium", 65400.00, 24],
                ["CUST031", "Sarah Davis", "sarah.d@email.com", "Premium", 54200.00, 22],
                ["CUST055", "David Wilson", "david.w@email.com", "Standard", 43100.00, 18],
                ["CUST012", "Lisa Anderson", "lisa.a@email.com", "VIP", 38900.00, 15],
                ["CUST067", "James Taylor", "james.t@email.com", "Premium", 32500.00, 14],
                ["CUST044", "Emma Martinez", "emma.m@email.com", "Standard", 28700.00, 12],
            ],
            "row_count": 10,
            "execution_time_ms": random.uniform(100, 500),
            "bytes_scanned": random.randint(10000000, 50000000),
            "summary": "Top 10 customers by revenue, dominated by VIP and Enterprise segments.",
            "visualization_type": "bar",
            "insights": [
                "VIP customers account for 45% of top 10 revenue",
                "Average revenue per top customer: $65,130",
                "Order frequency ranges from 12 to 45 orders"
            ]
        }
    else:
        return {
            "columns": ["month", "total_orders", "total_revenue", "unique_customers", "avg_order_value"],
            "data": [
                ["2024-01", 1523, 245000, 890, 160.87],
                ["2024-02", 1687, 278000, 945, 164.79],
                ["2024-03", 1845, 312000, 1020, 169.11],
                ["2024-04", 1756, 298000, 980, 169.70],
                ["2024-05", 1923, 335000, 1105, 174.21],
                ["2024-06", 2045, 365000, 1180, 178.48],
            ],
            "row_count": 6,
            "execution_time_ms": random.uniform(100, 500),
            "bytes_scanned": random.randint(10000000, 50000000),
            "summary": "Monthly sales showing consistent growth trend in 2024.",
            "visualization_type": "line",
            "insights": [
                "Revenue grew 49% from January to June",
                "Average order value increased from $160.87 to $178.48",
                "Unique customers grew 33% over 6 months"
            ]
        }


def _show_query_results_panel():
    """Show query results with visualization and export options."""
    results = st.session_state.query_results
    if not results:
        return

    st.markdown("---")
    st.markdown("### 📊 Query Results")

    # Metrics row
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Rows Returned", f"{results['row_count']:,}")
    col2.metric("Execution Time", f"{results['execution_time_ms']:.0f}ms")
    col3.metric("Data Scanned", f"{results['bytes_scanned']/1000000:.1f} MB")
    col4.metric("Columns", len(results['columns']))

    # Results summary from AI
    if results.get("summary"):
        st.info(f"**AI Summary:** {results['summary']}")

    # Insights
    if results.get("insights"):
        with st.expander("🔍 Key Insights", expanded=True):
            for insight in results["insights"]:
                st.markdown(f"• {insight}")

    # Data visualization
    viz_type = results.get("visualization_type", "table")

    tab_data, tab_chart = st.tabs(["📋 Data", "📈 Visualization"])

    with tab_data:
        df = pd.DataFrame(results["data"], columns=results["columns"])
        st.dataframe(df, use_container_width=True, height=400)

    with tab_chart:
        if viz_type == "bar" and len(results["data"]) > 0:
            df = pd.DataFrame(results["data"], columns=results["columns"])
            # Find numeric column for bar chart
            numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
            if numeric_cols:
                st.bar_chart(df.set_index(df.columns[1])[numeric_cols[0]])

        elif viz_type == "line" and len(results["data"]) > 0:
            df = pd.DataFrame(results["data"], columns=results["columns"])
            numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
            if numeric_cols and len(results["columns"]) > 1:
                st.line_chart(df.set_index(df.columns[0])[numeric_cols[:3]])

        else:
            st.info("Visualization not available for this result type")

    # Export options
    st.markdown("**Export:**")
    col1, col2, col3, col4 = st.columns(4)

    df = pd.DataFrame(results["data"], columns=results["columns"])

    with col1:
        csv = df.to_csv(index=False)
        st.download_button(
            "📥 CSV",
            data=csv,
            file_name="query_results.csv",
            mime="text/csv",
            use_container_width=True
        )

    with col2:
        json_data = df.to_json(orient="records")
        st.download_button(
            "📥 JSON",
            data=json_data,
            file_name="query_results.json",
            mime="application/json",
            use_container_width=True
        )

    with col3:
        if st.button("📊 Create Dashboard", use_container_width=True):
            notify_success("Dashboard widget created!")

    with col4:
        if st.button("📧 Share Results", use_container_width=True):
            notify_success("Share link copied!")


def _get_all_table_names() -> list[str]:
    """Get all available table names."""
    return [
        "bronze.raw_customers",
        "bronze.raw_orders",
        "bronze.raw_products",
        "bronze.raw_events",
        "silver.customers",
        "silver.orders",
        "silver.products",
        "silver.order_items",
        "gold.customer_summary",
        "gold.sales_metrics",
        "gold.product_performance",
        "gold.cohort_analysis",
    ]


def show_conversation_view():
    """Show full conversation view with chat interface."""
    st.subheader("💬 Conversation History")

    messages = st.session_state.conversation_messages

    if not messages:
        st.info("No conversation history yet. Start by asking a question in the Natural Language tab.")
        return

    # Conversation controls
    col1, col2, col3 = st.columns([2, 2, 4])
    with col1:
        if st.button("🔄 New Conversation", use_container_width=True):
            st.session_state.conversation_id = str(uuid.uuid4())
            st.session_state.conversation_messages = []
            st.session_state.query_results = None
            st.session_state.generated_sql = None
            notify_success("Started new conversation")
            st.rerun()

    with col2:
        if st.button("📥 Export Chat", use_container_width=True):
            # Export conversation as JSON
            chat_export = json.dumps(messages, indent=2, default=str)
            st.download_button(
                "Download",
                data=chat_export,
                file_name=f"conversation_{st.session_state.conversation_id[:8]}.json",
                mime="application/json"
            )

    st.markdown("---")

    # Chat messages
    for idx, msg in enumerate(messages):
        if msg["role"] == "user":
            with st.chat_message("user"):
                st.markdown(msg["content"])
                if msg.get("timestamp"):
                    st.caption(msg["timestamp"][:19] if isinstance(msg["timestamp"], str) else msg["timestamp"].strftime("%Y-%m-%d %H:%M"))
        else:
            with st.chat_message("assistant"):
                st.markdown(msg["content"])
                if msg.get("sql"):
                    with st.expander("View SQL", expanded=False):
                        st.code(msg["sql"], language="sql")
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button("▶️ Run", key=f"run_msg_{idx}"):
                                st.session_state.generated_sql = msg["sql"]
                                _execute_generated_sql()
                        with col2:
                            if st.button("✏️ Edit", key=f"edit_msg_{idx}"):
                                st.session_state.edit_sql = msg["sql"]

    # Continue conversation input
    st.markdown("---")
    continuation = st.text_input(
        "Continue the conversation...",
        placeholder="Ask a follow-up question or request a modification",
        key="conversation_continuation"
    )

    if st.button("Send", type="primary") and continuation:
        _generate_sql_from_nl(continuation)


def show_sql_editor():
    """Show SQL editor interface with syntax highlighting and validation."""
    st.subheader("📝 SQL Editor")

    col1, col2 = st.columns([1, 3])

    with col1:
        st.markdown("**Table Browser**")

        # Get user's accessible tiers
        user_tiers = st.session_state.get("user_tiers", ["bronze", "silver", "gold"])

        with st.expander("🥉 Bronze", expanded=False):
            if "bronze" in user_tiers:
                tables = ["raw_customers", "raw_orders", "raw_products", "raw_events"]
                for table in tables:
                    if st.button(f"📋 {table}", key=f"browse_bronze_{table}", use_container_width=True):
                        st.session_state.edit_sql = f"SELECT * FROM bronze.{table} LIMIT 100;"
                        st.rerun()
            else:
                st.caption("🔒 Access restricted")

        with st.expander("🥈 Silver", expanded=True):
            if "silver" in user_tiers:
                tables = ["customers", "orders", "products", "order_items"]
                for table in tables:
                    if st.button(f"📋 {table}", key=f"browse_silver_{table}", use_container_width=True):
                        st.session_state.edit_sql = f"SELECT * FROM silver.{table} LIMIT 100;"
                        st.rerun()
            else:
                st.caption("🔒 Access restricted")

        with st.expander("🥇 Gold", expanded=False):
            if "gold" in user_tiers:
                tables = ["customer_summary", "sales_metrics", "product_performance", "cohort_analysis"]
                for table in tables:
                    if st.button(f"📋 {table}", key=f"browse_gold_{table}", use_container_width=True):
                        st.session_state.edit_sql = f"SELECT * FROM gold.{table} LIMIT 100;"
                        st.rerun()
            else:
                st.caption("🔒 Access restricted")

        # Quick templates
        st.markdown("---")
        st.markdown("**Templates**")

        templates = [
            ("Count rows", "SELECT COUNT(*) FROM silver.customers;"),
            ("Group by", "SELECT segment, COUNT(*) FROM silver.customers GROUP BY segment;"),
            ("Join", "SELECT c.*, o.* FROM silver.customers c JOIN silver.orders o ON c.customer_id = o.customer_id LIMIT 100;"),
            ("Date filter", "SELECT * FROM silver.orders WHERE order_date >= CURRENT_DATE - INTERVAL '30 days';"),
        ]

        for name, template in templates:
            if st.button(f"📝 {name}", key=f"template_{name}", use_container_width=True):
                st.session_state.edit_sql = template
                st.rerun()

    with col2:
        # SQL editor
        sql = st.text_area(
            "SQL Query",
            value=st.session_state.get("edit_sql", "SELECT * FROM silver.customers LIMIT 10;"),
            height=250,
            label_visibility="collapsed",
            key="sql_editor_area"
        )

        # Editor toolbar
        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            if st.button("▶️ Run", type="primary", use_container_width=True):
                # Validate SQL before running
                sanitized_sql = _sanitize_input(sql)
                is_valid, error_msg = _validate_sql_input(sanitized_sql)

                if is_valid:
                    st.session_state.generated_sql = sanitized_sql
                    _execute_generated_sql()
                else:
                    st.error(error_msg)

        with col2:
            if st.button("📋 Format", use_container_width=True):
                # Simple SQL formatting
                formatted = _format_sql(sql)
                st.session_state.edit_sql = formatted
                notify_success("SQL formatted")
                st.rerun()

        with col3:
            if st.button("✅ Validate", use_container_width=True):
                sanitized_sql = _sanitize_input(sql)
                is_valid, message = _validate_sql_input(sanitized_sql)
                if is_valid:
                    # Also do syntax validation
                    syntax_valid, syntax_msg = _validate_sql(sanitized_sql)
                    if syntax_valid:
                        st.success(syntax_msg)
                    else:
                        st.error(syntax_msg)
                else:
                    st.error(message)

        with col4:
            if st.button("💾 Save", use_container_width=True):
                notify_success("Query saved to library")

        with col5:
            if st.button("🤖 Explain", use_container_width=True):
                _show_sql_explanation(sql)

        # Show results if available
        if st.session_state.query_results:
            _show_query_results_panel()


def _format_sql(sql: str) -> str:
    """Simple SQL formatting."""
    # Basic formatting - in production use sqlparse library
    keywords = ["SELECT", "FROM", "WHERE", "JOIN", "LEFT JOIN", "RIGHT JOIN",
                "INNER JOIN", "GROUP BY", "ORDER BY", "HAVING", "LIMIT", "AND", "OR"]

    formatted = sql.upper()
    for kw in keywords:
        formatted = formatted.replace(f" {kw} ", f"\n{kw} ")

    return formatted


def _validate_sql(sql: str) -> tuple[bool, str]:
    """Validate SQL query for security issues."""
    sql_lower = sql.lower()

    # Check for dangerous operations
    dangerous = ["drop", "truncate", "delete", "alter", "create", "insert", "update", "grant", "revoke"]
    for op in dangerous:
        if op in sql_lower:
            return False, f"Dangerous operation '{op}' not allowed"

    # Check for basic syntax
    if not sql_lower.strip().startswith("select"):
        return False, "Only SELECT queries are allowed"

    # Check for injection patterns
    if ";" in sql and sql.count(";") > 1:
        return False, "Multiple statements not allowed"

    return True, "Query is valid and safe to execute"


def _show_sql_explanation(sql: str):
    """Show AI explanation of SQL query."""
    st.markdown("---")
    st.subheader("🤖 Query Explanation")

    with st.spinner("Analyzing query..."):
        time.sleep(0.5)

        # Mock explanation - in production call API
        sql_lower = sql.lower()

        if "join" in sql_lower:
            explanation = """
**What this query does:**

This query joins multiple tables to combine related data. The JOIN operation
matches rows from different tables based on the specified conditions.

**Performance Notes:**
- Ensure join columns are indexed for optimal performance
- Consider the order of tables in the join for efficiency
- Large result sets may require LIMIT clause

**Suggested Optimizations:**
- Select only needed columns instead of SELECT *
- Add appropriate WHERE filters to reduce data scanned
            """
        elif "group by" in sql_lower:
            explanation = """
**What this query does:**

This query aggregates data by grouping rows with the same values in the
specified columns. Aggregate functions (COUNT, SUM, AVG, etc.) are applied
to each group.

**Performance Notes:**
- GROUP BY can be memory-intensive for large datasets
- Consider adding HAVING clause for filtering groups
- Indexes on GROUP BY columns improve performance

**Suggested Optimizations:**
- Filter data with WHERE before grouping
- Limit the number of groups returned
            """
        else:
            explanation = """
**What this query does:**

This query retrieves data from the specified table with optional filtering
and limiting of results.

**Performance Notes:**
- Using SELECT * retrieves all columns - consider selecting only needed columns
- LIMIT clause ensures manageable result set size
- No filters may scan the entire table

**Suggested Optimizations:**
- Specify needed columns instead of SELECT *
- Add WHERE clause to filter relevant data
- Consider adding ORDER BY for deterministic results
            """

        st.info(explanation)


def show_query_history():
    """Show query history with filtering and actions."""
    st.subheader("📚 Query History")

    # Filters
    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])

    with col1:
        search = st.text_input("🔍 Search queries", placeholder="Search by query text...", key="history_search")

    with col2:
        date_filter = st.selectbox("Time", ["All", "Today", "Last 7 days", "Last 30 days"], key="history_date")

    with col3:
        type_filter = st.selectbox("Type", ["All", "Natural Language", "SQL"], key="history_type")

    with col4:
        status_filter = st.selectbox("Status", ["All", "Executed", "Generated", "Failed"], key="history_status")

    st.markdown("---")

    # Get history (combine local and mock data)
    history = st.session_state.query_history_local + [
        {
            "query": "Show me the top 10 customers by revenue",
            "sql": "SELECT c.*, SUM(o.amount) FROM customers c JOIN orders o...",
            "type": "NL",
            "timestamp": datetime.now(),
            "status": "executed",
            "rows": 10,
            "duration": "0.23s"
        },
        {
            "query": "SELECT * FROM silver.customers WHERE segment = 'VIP'",
            "sql": "SELECT * FROM silver.customers WHERE segment = 'VIP'",
            "type": "SQL",
            "timestamp": datetime.now(),
            "status": "executed",
            "rows": 234,
            "duration": "0.45s"
        },
        {
            "query": "What is the total revenue by product category?",
            "sql": "SELECT category, SUM(revenue) FROM products GROUP BY category",
            "type": "NL",
            "timestamp": datetime.now(),
            "status": "executed",
            "rows": 15,
            "duration": "1.2s"
        },
    ]

    # Apply filters
    if search:
        search_lower = search.lower()
        history = [h for h in history if search_lower in h.get("query", "").lower()]

    if type_filter != "All":
        type_map = {"Natural Language": "NL", "SQL": "SQL"}
        history = [h for h in history if h.get("type") == type_map.get(type_filter)]

    if status_filter != "All":
        history = [h for h in history if h.get("status", "").lower() == status_filter.lower()]

    if not history:
        st.info("No queries found matching your filters.")
        return

    # Display history
    for idx, item in enumerate(history):
        status_icon = {"executed": "✅", "generated": "📝", "failed": "❌"}.get(item.get("status", ""), "⏳")
        type_icon = "🗣️" if item.get("type") == "NL" else "📝"

        with st.expander(f"{status_icon} {type_icon} {item['query'][:60]}...", expanded=False):
            col1, col2 = st.columns([3, 1])

            with col1:
                st.markdown("**Query:**")
                st.code(item.get("sql", item["query"]), language="sql")

            with col2:
                st.markdown("**Details:**")
                st.markdown(f"**Type:** {item.get('type', 'N/A')}")
                st.markdown(f"**Status:** {item.get('status', 'N/A')}")
                if item.get("rows"):
                    st.markdown(f"**Rows:** {item['rows']}")
                if item.get("duration"):
                    st.markdown(f"**Duration:** {item['duration']}")
                if item.get("timestamp"):
                    ts = item["timestamp"]
                    if isinstance(ts, str):
                        st.markdown(f"**Time:** {ts[:19]}")
                    else:
                        st.markdown(f"**Time:** {ts.strftime('%Y-%m-%d %H:%M')}")

            # Actions
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                if st.button("🔄 Re-run", key=f"rerun_{idx}", use_container_width=True):
                    st.session_state.generated_sql = item.get("sql", item["query"])
                    _execute_generated_sql()

            with col2:
                if st.button("✏️ Edit", key=f"edit_{idx}", use_container_width=True):
                    st.session_state.edit_sql = item.get("sql", item["query"])

            with col3:
                if st.button("📋 Copy", key=f"copy_{idx}", use_container_width=True):
                    notify_success("Query copied!")

            with col4:
                if st.button("🗑️ Delete", key=f"delete_{idx}", use_container_width=True):
                    if idx < len(st.session_state.query_history_local):
                        st.session_state.query_history_local.pop(idx)
                        st.rerun()

    # Pagination info
    st.markdown("---")
    st.caption(f"Showing {len(history)} queries")


def show_generated_query(question: str):
    """Show the generated SQL query - legacy function for compatibility."""
    _generate_sql_from_nl(question)

