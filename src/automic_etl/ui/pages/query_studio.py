"""Query Studio Page for Automic ETL UI - Sleek minimal design."""

from __future__ import annotations

import httpx
import json
import time
import uuid
from datetime import datetime
from typing import Any

import pandas as pd
import streamlit as st

from automic_etl.ui.state import notify_success

# API base URL
API_BASE_URL = "http://localhost:8000/api/v1"


def _get_api_client() -> httpx.Client:
    """Get configured HTTP client for API calls."""
    return httpx.Client(base_url=API_BASE_URL, timeout=30.0)


# ============================================================================
# Input Validation Constants
# ============================================================================

MAX_QUERY_LENGTH = 5000
MIN_QUERY_LENGTH = 3
MAX_SQL_LENGTH = 10000

SUSPICIOUS_PATTERNS = [
    "ignore previous", "ignore above", "disregard", "forget everything",
    "new instructions", "system prompt", "you are now", "act as",
    "pretend to be", "jailbreak",
]

SQL_INJECTION_PATTERNS = [
    "'; --", "' OR '1'='1", "'; DROP", "; DELETE", "; UPDATE",
    "; INSERT", "UNION SELECT", "/*", "*/", "xp_", "exec(", "execute(",
]


def _validate_query_input(query: str) -> tuple[bool, str | None]:
    """Validate natural language query input."""
    if not query or not query.strip():
        return False, "Please enter a question"

    query = query.strip()

    if len(query) < MIN_QUERY_LENGTH:
        return False, f"Question is too short (minimum {MIN_QUERY_LENGTH} characters)"
    if len(query) > MAX_QUERY_LENGTH:
        return False, f"Question is too long (maximum {MAX_QUERY_LENGTH} characters)"

    query_lower = query.lower()
    for pattern in SUSPICIOUS_PATTERNS:
        if pattern in query_lower:
            return False, "Your question contains disallowed patterns. Please rephrase."
    for pattern in SQL_INJECTION_PATTERNS:
        if pattern.lower() in query_lower:
            return False, "Your question contains disallowed SQL patterns. Please rephrase."

    return True, None


def _validate_sql_input(sql: str) -> tuple[bool, str | None]:
    """Validate SQL query input."""
    if not sql or not sql.strip():
        return False, "Please enter a SQL query"

    sql = sql.strip()
    if len(sql) > MAX_SQL_LENGTH:
        return False, f"SQL query is too long (maximum {MAX_SQL_LENGTH} characters)"

    sql_upper = sql.upper()
    dangerous_keywords = ["DROP", "TRUNCATE", "DELETE", "ALTER", "CREATE", "GRANT", "REVOKE"]
    for keyword in dangerous_keywords:
        if f" {keyword} " in f" {sql_upper} ":
            return False, f"Dangerous SQL operation '{keyword}' is not allowed"

    if sql.count(';') > 1:
        return False, "Multiple SQL statements are not allowed"

    return True, None


def _sanitize_input(text: str) -> str:
    """Sanitize user input."""
    sanitized = ''.join(char for char in text if ord(char) >= 32 or char in '\n\t')
    return sanitized.strip()


# ============================================================================
# Main Page
# ============================================================================

def show_query_studio_page():
    """Display the query studio page with sleek minimal design."""
    # Page header
    st.markdown("""
    <div style="margin-bottom: 2rem;">
        <h1 style="
            font-size: 1.5rem;
            font-weight: 600;
            color: #0F172A;
            margin: 0 0 0.375rem;
            letter-spacing: -0.025em;
        ">Query Studio</h1>
        <p style="font-size: 0.875rem; color: #64748B; margin: 0;">
            Query data using natural language or SQL
        </p>
    </div>
    """, unsafe_allow_html=True)

    _init_query_studio_state()
    _show_sidebar()

    # Tab styling
    st.markdown("""
    <style>
    .stTabs [data-baseweb="tab-list"] {
        gap: 0;
        background: #F8FAFC;
        border-radius: 10px;
        padding: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        border-radius: 8px;
        color: #64748B;
        font-weight: 500;
        font-size: 0.875rem;
        padding: 0.5rem 1rem;
    }
    .stTabs [aria-selected="true"] {
        background: white !important;
        color: #0F172A !important;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    }
    </style>
    """, unsafe_allow_html=True)

    tab1, tab2, tab3, tab4 = st.tabs([
        "Natural Language",
        "SQL Editor",
        "Conversation",
        "History",
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
    """Show minimal sidebar with schema browser."""
    with st.sidebar:
        st.markdown("""
        <div style="
            font-size: 0.8125rem;
            font-weight: 600;
            color: #0F172A;
            margin-bottom: 1rem;
        ">Schema Browser</div>
        """, unsafe_allow_html=True)

        user_tiers = st.session_state.get("user_tiers", ["bronze", "silver", "gold"])
        tables_by_tier = _get_available_tables(user_tiers)

        tier_config = {
            "bronze": ("#A16207", "#FFFBEB"),
            "silver": ("#6B7280", "#F8FAFC"),
            "gold": ("#CA8A04", "#FEFCE8"),
        }

        for tier, tables in tables_by_tier.items():
            if tables:
                color, bg = tier_config.get(tier, ("#64748B", "#F8FAFC"))
                with st.expander(f"{tier.capitalize()}", expanded=(tier == "gold")):
                    for table in tables:
                        st.markdown(f"""
                        <div style="
                            padding: 0.5rem;
                            background: white;
                            border: 1px solid #E2E8F0;
                            border-radius: 6px;
                            margin-bottom: 0.375rem;
                            font-size: 0.8125rem;
                        ">
                            <div style="font-weight: 500; color: #0F172A;">{table['name']}</div>
                            <div style="font-size: 0.6875rem; color: #94A3B8;">{table.get('rows', '0')} rows</div>
                        </div>
                        """, unsafe_allow_html=True)

        st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)

        st.markdown("""
        <div style="
            font-size: 0.8125rem;
            font-weight: 600;
            color: #0F172A;
            margin-bottom: 0.5rem;
        ">Session</div>
        """, unsafe_allow_html=True)

        st.markdown(f"""
        <div style="font-size: 0.75rem; color: #94A3B8; margin-bottom: 0.5rem;">
            ID: {st.session_state.conversation_id[:8]}...
        </div>
        """, unsafe_allow_html=True)

        if st.button("New Conversation", use_container_width=True, type="secondary"):
            st.session_state.conversation_id = str(uuid.uuid4())
            st.session_state.conversation_messages = []
            st.session_state.query_results = None
            st.session_state.generated_sql = None
            notify_success("Started new conversation")
            st.rerun()


def _get_available_tables(tier_filter: list[str]) -> dict[str, list[dict]]:
    """Get available tables organized by tier from API."""
    try:
        with _get_api_client() as client:
            response = client.get("/tables", params={"page_size": 100})
            if response.status_code == 200:
                data = response.json()
                tables = data.get("items", [])

                tables_by_tier: dict[str, list[dict]] = {"bronze": [], "silver": [], "gold": []}
                for table in tables:
                    tier = table.get("tier", "bronze")
                    if tier in tables_by_tier:
                        tables_by_tier[tier].append({
                            "name": table.get("name", ""),
                            "description": table.get("description", ""),
                            "columns": len(table.get("columns", [])),
                            "rows": f"{table.get('row_count', 0):,}",
                        })

                return {tier: tables for tier, tables in tables_by_tier.items() if tier in tier_filter}
    except Exception:
        pass

    return {tier: [] for tier in tier_filter}


# ============================================================================
# Natural Language Query
# ============================================================================

def show_natural_language_query():
    """Show natural language query interface."""
    _section_header("Ask in Natural Language", "Describe what data you want to see")

    # Context tables
    st.markdown("""
    <div style="font-size: 0.8125rem; font-weight: 500; color: #0F172A; margin-bottom: 0.5rem;">
        Context Tables
    </div>
    """, unsafe_allow_html=True)

    selected = st.multiselect(
        "Tables",
        _get_all_table_names(),
        default=st.session_state.selected_tables or ["silver.customers", "silver.orders"],
        key="nl_selected_tables",
        label_visibility="collapsed",
    )
    st.session_state.selected_tables = selected

    st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)

    # Chat interface
    _show_chat_interface()

    # Query input
    st.markdown("""
    <div style="font-size: 0.8125rem; font-weight: 500; color: #0F172A; margin-bottom: 0.5rem;">
        Your Question
    </div>
    """, unsafe_allow_html=True)

    query = st.text_area(
        "Ask a question",
        placeholder="Example: Show me the top 10 customers by total spending in the last 90 days",
        height=80,
        key="nl_query_input",
        label_visibility="collapsed",
    )

    col1, col2 = st.columns([1, 3])

    with col1:
        generate_btn = st.button("Generate SQL →", type="primary", use_container_width=True)

    with col2:
        if st.session_state.generated_sql:
            execute_btn = st.button("Execute", use_container_width=True)
        else:
            execute_btn = False

    # Example queries
    _show_example_queries()

    # Handle generate
    if generate_btn and query:
        sanitized_query = _sanitize_input(query)
        is_valid, error_msg = _validate_query_input(sanitized_query)
        if is_valid:
            _generate_sql_from_nl(sanitized_query)
        else:
            st.error(error_msg)

    # Handle execute
    if execute_btn and st.session_state.generated_sql:
        _execute_generated_sql()

    # Show generated SQL and results
    if st.session_state.generated_sql:
        _show_generated_sql_panel()

    if st.session_state.query_results:
        _show_query_results_panel()


def _show_chat_interface():
    """Show conversation history."""
    messages = st.session_state.conversation_messages

    if messages:
        with st.expander("Conversation History", expanded=False):
            for msg in messages[-10:]:
                if msg["role"] == "user":
                    st.markdown(f"""
                    <div style="
                        background: #F8FAFC;
                        border-radius: 8px;
                        padding: 0.75rem 1rem;
                        margin-bottom: 0.5rem;
                    ">
                        <div style="font-size: 0.6875rem; color: #94A3B8; margin-bottom: 0.25rem;">You</div>
                        <div style="font-size: 0.875rem; color: #0F172A;">{msg['content']}</div>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div style="
                        background: #EEF2FF;
                        border-radius: 8px;
                        padding: 0.75rem 1rem;
                        margin-bottom: 0.5rem;
                    ">
                        <div style="font-size: 0.6875rem; color: #6366F1; margin-bottom: 0.25rem;">Assistant</div>
                        <div style="font-size: 0.875rem; color: #0F172A;">{msg['content'][:200]}...</div>
                    </div>
                    """, unsafe_allow_html=True)


def _show_example_queries():
    """Show clickable example queries."""
    with st.expander("Example Questions", expanded=False):
        examples = [
            "Top 10 customers by revenue",
            "Monthly sales trends for 2024",
            "Average order value by segment",
            "Customers inactive for 90 days",
        ]

        cols = st.columns(2)
        for idx, query in enumerate(examples):
            with cols[idx % 2]:
                if st.button(query, key=f"example_{idx}", use_container_width=True):
                    st.session_state.nl_query_input = query
                    st.rerun()


def _generate_sql_from_nl(query: str):
    """Generate SQL from natural language query."""
    with st.spinner("Generating..."):
        try:
            with _get_api_client() as client:
                response = client.post(
                    "/queries/natural",
                    json={
                        "query": query,
                        "tables": st.session_state.get("selected_tables", []),
                        "explain_results": True,
                    },
                )
                if response.status_code == 200:
                    generated_sql = response.json()
                else:
                    st.error(f"Failed to generate SQL: HTTP {response.status_code}")
                    return
        except Exception as e:
            st.error(f"Failed to connect to API: {e}")
            return

        st.session_state.generated_sql = generated_sql.get("generated_sql", "")
        st.session_state.query_confidence = generated_sql.get("confidence", 0.0)
        st.session_state.query_explanation = generated_sql.get("explanation", "")
        st.session_state.query_intent = generated_sql.get("intent", "select")
        st.session_state.tables_used = generated_sql.get("tables_used", [])
        st.session_state.query_warnings = generated_sql.get("warnings", [])
        st.session_state.follow_up_suggestions = generated_sql.get("follow_up_questions", [])

        st.session_state.conversation_messages.append({
            "role": "user",
            "content": query,
            "timestamp": datetime.now().isoformat()
        })
        st.session_state.conversation_messages.append({
            "role": "assistant",
            "content": generated_sql.get("explanation", ""),
            "sql": generated_sql.get("generated_sql", ""),
            "timestamp": datetime.now().isoformat()
        })

        st.session_state.query_history_local.insert(0, {
            "query": query,
            "sql": generated_sql.get("generated_sql", ""),
            "type": "NL",
            "timestamp": datetime.now(),
            "status": "generated"
        })

        st.rerun()


def _show_generated_sql_panel():
    """Show the generated SQL panel."""
    st.markdown("<div style='height: 1.5rem;'></div>", unsafe_allow_html=True)

    st.markdown("""
    <div style="font-size: 0.8125rem; font-weight: 500; color: #0F172A; margin-bottom: 0.5rem;">
        Generated SQL
    </div>
    """, unsafe_allow_html=True)

    # SQL card
    st.markdown(f"""
    <div style="
        background: white;
        border: 1px solid #E2E8F0;
        border-radius: 10px;
        overflow: hidden;
    ">
        <div style="
            background: #F8FAFC;
            padding: 0.75rem 1rem;
            border-bottom: 1px solid #E2E8F0;
            display: flex;
            justify-content: space-between;
            align-items: center;
        ">
            <span style="font-size: 0.75rem; color: #64748B;">SQL Query</span>
            <span style="
                font-size: 0.6875rem;
                padding: 0.125rem 0.5rem;
                background: {'#ECFDF5' if st.session_state.get('query_confidence', 0) > 0.8 else '#FFFBEB'};
                color: {'#10B981' if st.session_state.get('query_confidence', 0) > 0.8 else '#F59E0B'};
                border-radius: 9999px;
            ">{st.session_state.get('query_confidence', 0) * 100:.0f}% confidence</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.code(st.session_state.generated_sql, language="sql")

    # Explanation
    if st.session_state.get("query_explanation"):
        st.markdown(f"""
        <div style="
            background: #F8FAFC;
            border-radius: 8px;
            padding: 0.875rem 1rem;
            font-size: 0.8125rem;
            color: #64748B;
            margin-top: 0.5rem;
        ">
            {st.session_state.query_explanation}
        </div>
        """, unsafe_allow_html=True)

    # Actions
    st.markdown("<div style='height: 0.75rem;'></div>", unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("Execute →", type="primary", use_container_width=True):
            _execute_generated_sql()

    with col2:
        if st.button("Edit SQL", use_container_width=True):
            st.session_state.edit_sql = st.session_state.generated_sql

    with col3:
        if st.button("Save Query", use_container_width=True):
            notify_success("Query saved")


def _execute_generated_sql():
    """Execute the generated SQL query."""
    with st.spinner("Executing..."):
        st.session_state.query_results = _execute_query(st.session_state.generated_sql)

        if st.session_state.query_history_local:
            st.session_state.query_history_local[0]["status"] = "executed"
            st.session_state.query_history_local[0]["rows"] = st.session_state.query_results.get("row_count", 0)
            execution_time_ms = st.session_state.query_results.get("execution_time_ms", 0)
            st.session_state.query_history_local[0]["duration"] = f"{execution_time_ms/1000:.2f}s"

        st.rerun()


def _execute_query(sql: str) -> dict[str, Any]:
    """Execute SQL query via API."""
    try:
        with _get_api_client() as client:
            response = client.post(
                "/queries/execute",
                json={"query": sql, "query_type": "sql", "limit": 1000},
            )
            if response.status_code == 200:
                return response.json()
            return {
                "columns": [], "data": [], "row_count": 0,
                "execution_time_ms": 0, "bytes_scanned": 0,
                "error": f"HTTP {response.status_code}",
            }
    except Exception as e:
        return {
            "columns": [], "data": [], "row_count": 0,
            "execution_time_ms": 0, "bytes_scanned": 0,
            "error": str(e),
        }


def _show_query_results_panel():
    """Show query results panel."""
    results = st.session_state.query_results
    if not results:
        return

    if results.get("error"):
        st.error(f"Query error: {results['error']}")
        return

    st.markdown("<div style='height: 1.5rem;'></div>", unsafe_allow_html=True)

    st.markdown("""
    <div style="font-size: 0.8125rem; font-weight: 500; color: #0F172A; margin-bottom: 0.75rem;">
        Results
    </div>
    """, unsafe_allow_html=True)

    # Stats row
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Rows", f"{results.get('row_count', 0):,}")
    col2.metric("Time", f"{results.get('execution_time_ms', 0):.0f}ms")
    bytes_scanned = results.get("bytes_scanned", 0) or 0
    col3.metric("Scanned", f"{bytes_scanned/1000000:.1f} MB")
    col4.metric("Columns", len(results.get('columns', [])))

    # Data table
    df = pd.DataFrame(results["data"], columns=results["columns"])
    st.dataframe(df, use_container_width=True, height=300)

    # Export
    st.markdown("<div style='height: 0.5rem;'></div>", unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 1, 2])

    with col1:
        csv = df.to_csv(index=False)
        st.download_button(
            "Export CSV",
            data=csv,
            file_name="query_results.csv",
            mime="text/csv",
            use_container_width=True
        )

    with col2:
        json_data = df.to_json(orient="records")
        st.download_button(
            "Export JSON",
            data=json_data,
            file_name="query_results.json",
            mime="application/json",
            use_container_width=True
        )


def _get_all_table_names() -> list[str]:
    """Get all available table names from API."""
    try:
        with _get_api_client() as client:
            response = client.get("/tables", params={"page_size": 100})
            if response.status_code == 200:
                data = response.json()
                tables = data.get("items", [])
                return [f"{t.get('tier', 'bronze')}.{t.get('name', '')}" for t in tables]
    except Exception:
        pass
    return []


# ============================================================================
# SQL Editor
# ============================================================================

def show_sql_editor():
    """Show SQL editor interface."""
    _section_header("SQL Editor", "Write and execute SQL queries")

    col1, col2 = st.columns([1, 3])

    with col1:
        st.markdown("""
        <div style="font-size: 0.8125rem; font-weight: 500; color: #0F172A; margin-bottom: 0.75rem;">
            Quick Insert
        </div>
        """, unsafe_allow_html=True)

        templates = [
            ("SELECT", "SELECT * FROM silver.customers LIMIT 10;"),
            ("COUNT", "SELECT COUNT(*) FROM silver.customers;"),
            ("GROUP BY", "SELECT segment, COUNT(*) FROM silver.customers GROUP BY segment;"),
            ("JOIN", "SELECT * FROM silver.customers c JOIN silver.orders o ON c.customer_id = o.customer_id LIMIT 100;"),
        ]

        for name, template in templates:
            if st.button(name, key=f"tpl_{name}", use_container_width=True):
                st.session_state.edit_sql = template
                st.rerun()

    with col2:
        sql = st.text_area(
            "SQL Query",
            value=st.session_state.get("edit_sql", "SELECT * FROM silver.customers LIMIT 10;"),
            height=200,
            label_visibility="collapsed",
            key="sql_editor_area"
        )

        col_a, col_b, col_c = st.columns([1, 1, 2])

        with col_a:
            if st.button("Run →", type="primary", use_container_width=True):
                sanitized_sql = _sanitize_input(sql)
                is_valid, error_msg = _validate_sql_input(sanitized_sql)
                if is_valid:
                    st.session_state.generated_sql = sanitized_sql
                    _execute_generated_sql()
                else:
                    st.error(error_msg)

        with col_b:
            if st.button("Format", use_container_width=True):
                formatted = _format_sql(sql)
                st.session_state.edit_sql = formatted
                st.rerun()

        if st.session_state.query_results:
            _show_query_results_panel()


def _format_sql(sql: str) -> str:
    """Simple SQL formatting."""
    keywords = ["SELECT", "FROM", "WHERE", "JOIN", "LEFT JOIN", "RIGHT JOIN",
                "INNER JOIN", "GROUP BY", "ORDER BY", "HAVING", "LIMIT", "AND", "OR"]

    formatted = sql.upper()
    for kw in keywords:
        formatted = formatted.replace(f" {kw} ", f"\n{kw} ")

    return formatted


# ============================================================================
# Conversation View
# ============================================================================

def show_conversation_view():
    """Show full conversation view."""
    _section_header("Conversation History", "View and continue your queries")

    messages = st.session_state.conversation_messages

    if not messages:
        _empty_state("◇", "No conversation yet", "Start by asking a question in the Natural Language tab")
        return

    col1, col2 = st.columns([1, 3])

    with col1:
        if st.button("New Conversation", use_container_width=True):
            st.session_state.conversation_id = str(uuid.uuid4())
            st.session_state.conversation_messages = []
            st.session_state.query_results = None
            st.session_state.generated_sql = None
            st.rerun()

        if st.button("Export", use_container_width=True):
            chat_export = json.dumps(messages, indent=2, default=str)
            st.download_button(
                "Download JSON",
                data=chat_export,
                file_name=f"conversation_{st.session_state.conversation_id[:8]}.json",
                mime="application/json"
            )

    with col2:
        for idx, msg in enumerate(messages):
            if msg["role"] == "user":
                with st.chat_message("user"):
                    st.markdown(msg["content"])
            else:
                with st.chat_message("assistant"):
                    st.markdown(msg["content"])
                    if msg.get("sql"):
                        with st.expander("View SQL"):
                            st.code(msg["sql"], language="sql")

        continuation = st.text_input(
            "Continue conversation",
            placeholder="Ask a follow-up question...",
            key="conversation_continuation"
        )

        if st.button("Send →", type="primary") and continuation:
            _generate_sql_from_nl(continuation)


# ============================================================================
# Query History
# ============================================================================

def show_query_history():
    """Show query history."""
    _section_header("Query History", "Browse and re-run past queries")

    col1, col2 = st.columns([3, 1])

    with col1:
        search = st.text_input(
            "Search",
            placeholder="Filter by query text...",
            label_visibility="collapsed",
        )

    with col2:
        status_filter = st.selectbox(
            "Status",
            ["All", "Executed", "Generated"],
            label_visibility="collapsed",
        )

    history = st.session_state.query_history_local.copy()

    # Fetch from API
    try:
        with _get_api_client() as client:
            response = client.get("/queries/history", params={"page_size": 50})
            if response.status_code == 200:
                data = response.json()
                api_history = data.get("items", [])
                for item in api_history:
                    history.append({
                        "query": item.get("query", ""),
                        "sql": item.get("executed_sql", item.get("query", "")),
                        "type": "NL" if item.get("query") != item.get("executed_sql") else "SQL",
                        "timestamp": item.get("executed_at", ""),
                        "status": item.get("status", "executed"),
                        "rows": item.get("row_count", 0),
                        "duration": f"{item.get('execution_time_ms', 0)/1000:.2f}s",
                    })
    except Exception:
        pass

    # Apply filters
    if search:
        search_lower = search.lower()
        history = [h for h in history if search_lower in h.get("query", "").lower()]

    if status_filter != "All":
        history = [h for h in history if h.get("status", "").lower() == status_filter.lower()]

    if not history:
        _empty_state("◇", "No queries found", "Start querying to build your history")
        return

    st.markdown(f"""
    <div style="font-size: 0.75rem; color: #64748B; margin: 1rem 0 0.75rem;">
        {len(history)} queries
    </div>
    """, unsafe_allow_html=True)

    for idx, item in enumerate(history):
        status_colors = {
            "executed": ("#10B981", "#ECFDF5"),
            "generated": ("#6366F1", "#EEF2FF"),
            "failed": ("#EF4444", "#FEF2F2"),
        }
        color, bg = status_colors.get(item.get("status", ""), ("#64748B", "#F8FAFC"))

        st.markdown(f"""
        <div style="
            background: white;
            border: 1px solid #E2E8F0;
            border-radius: 10px;
            padding: 1rem;
            margin-bottom: 0.5rem;
        ">
            <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 0.5rem;">
                <div style="flex: 1;">
                    <div style="font-size: 0.875rem; color: #0F172A; margin-bottom: 0.25rem;">
                        {item['query'][:80]}...
                    </div>
                </div>
                <div style="
                    font-size: 0.6875rem;
                    padding: 0.125rem 0.5rem;
                    background: {bg};
                    color: {color};
                    border-radius: 9999px;
                ">{item.get('status', 'unknown')}</div>
            </div>
            <div style="font-size: 0.75rem; color: #94A3B8;">
                {item.get('type', 'SQL')} · {item.get('rows', 0)} rows · {item.get('duration', '')}
            </div>
        </div>
        """, unsafe_allow_html=True)

        col1, col2 = st.columns([1, 3])
        with col1:
            if st.button("Re-run", key=f"rerun_{idx}"):
                st.session_state.generated_sql = item.get("sql", item["query"])
                _execute_generated_sql()


# ============================================================================
# Helper Components
# ============================================================================

def _section_header(title: str, subtitle: str | None = None):
    """Render a minimal section header."""
    subtitle_html = f'<div style="font-size: 0.8125rem; color: #94A3B8; margin-top: 0.25rem;">{subtitle}</div>' if subtitle else ''
    st.markdown(f"""
    <div style="margin-bottom: 1.25rem;">
        <div style="font-size: 1rem; font-weight: 600; color: #0F172A;">{title}</div>
        {subtitle_html}
    </div>
    """, unsafe_allow_html=True)


def _empty_state(icon: str, title: str, description: str):
    """Render minimal empty state."""
    st.markdown(f"""
    <div style="
        padding: 3rem 2rem;
        text-align: center;
        background: #F8FAFC;
        border-radius: 12px;
        border: 1px dashed #E2E8F0;
    ">
        <div style="font-size: 2rem; color: #CBD5E1; margin-bottom: 0.75rem;">{icon}</div>
        <div style="font-size: 0.9375rem; font-weight: 500; color: #64748B; margin-bottom: 0.25rem;">{title}</div>
        <div style="font-size: 0.8125rem; color: #94A3B8;">{description}</div>
    </div>
    """, unsafe_allow_html=True)


def show_generated_query(question: str):
    """Legacy function for compatibility."""
    _generate_sql_from_nl(question)
