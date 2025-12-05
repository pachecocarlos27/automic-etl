"""Data Ingestion Page for Automic ETL UI - Sleek minimal design."""

from __future__ import annotations

import httpx
import streamlit as st
import polars as pl
from io import BytesIO
from typing import Any
import time

from automic_etl.db.data_service import get_data_service

# API base URL
API_BASE_URL = "http://localhost:8000/api/v1"


def _get_api_client() -> httpx.Client:
    """Get configured HTTP client for API calls."""
    return httpx.Client(base_url=API_BASE_URL, timeout=30.0)


def show_ingestion_page():
    """Display the data ingestion page with sleek minimal design."""
    # Page header
    st.markdown("""
    <div style="margin-bottom: 2rem;">
        <h1 style="
            font-size: 1.5rem;
            font-weight: 600;
            color: #0F172A;
            margin: 0 0 0.375rem;
            letter-spacing: -0.025em;
        ">Data Ingestion</h1>
        <p style="font-size: 0.875rem; color: #64748B; margin: 0;">
            Upload files or connect to data sources
        </p>
    </div>
    """, unsafe_allow_html=True)

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

    tab1, tab2, tab3 = st.tabs([
        "File Upload",
        "Data Tables",
        "Database",
    ])

    with tab1:
        show_file_upload_section()

    with tab2:
        show_data_tables_section()

    with tab3:
        show_database_connection_section()


def show_file_upload_section():
    """Show file upload section with minimal styling."""
    _section_header("Upload Files", "Drag and drop or click to browse")

    user = st.session_state.get("user")
    if not user:
        _info_card("Please log in to upload files.", "info")
        return

    # Styled file uploader
    uploaded_files = st.file_uploader(
        "Choose files",
        type=["csv", "json", "parquet", "xlsx"],
        accept_multiple_files=True,
        help="Supported: CSV, JSON, Parquet, Excel",
        label_visibility="collapsed",
    )

    if uploaded_files:
        st.markdown("<div style='height: 1.5rem;'></div>", unsafe_allow_html=True)
        _section_header("Uploaded Files", f"{len(uploaded_files)} file(s) ready")

        for uploaded_file in uploaded_files:
            _render_file_card(uploaded_file)


def _render_file_card(uploaded_file):
    """Render a sleek file card for upload."""
    with st.container():
        st.markdown(f"""
        <div style="
            background: white;
            border: 1px solid #E2E8F0;
            border-radius: 12px;
            padding: 1.25rem;
            margin-bottom: 1rem;
        ">
            <div style="
                display: flex;
                align-items: center;
                gap: 0.75rem;
                margin-bottom: 1rem;
            ">
                <div style="
                    width: 36px;
                    height: 36px;
                    background: #EEF2FF;
                    border-radius: 8px;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    color: #6366F1;
                    font-size: 0.875rem;
                ">◇</div>
                <div>
                    <div style="font-weight: 500; color: #0F172A; font-size: 0.9375rem;">
                        {uploaded_file.name}
                    </div>
                    <div style="font-size: 0.75rem; color: #94A3B8;">
                        {uploaded_file.size / 1024:.1f} KB
                    </div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        try:
            if uploaded_file.name.endswith(".csv"):
                df = pl.read_csv(BytesIO(uploaded_file.getvalue()))
            elif uploaded_file.name.endswith(".json"):
                df = pl.read_json(BytesIO(uploaded_file.getvalue()))
            elif uploaded_file.name.endswith(".parquet"):
                df = pl.read_parquet(BytesIO(uploaded_file.getvalue()))
            elif uploaded_file.name.endswith(".xlsx"):
                df = pl.read_excel(BytesIO(uploaded_file.getvalue()))
            else:
                st.error(f"Unsupported file type: {uploaded_file.name}")
                return

            # Data preview
            col1, col2 = st.columns([3, 1])

            with col1:
                st.markdown(f"""
                <div style="
                    font-size: 0.75rem;
                    color: #64748B;
                    margin-bottom: 0.5rem;
                ">
                    {df.shape[0]:,} rows × {df.shape[1]} columns
                </div>
                """, unsafe_allow_html=True)
                st.dataframe(df.head(10), use_container_width=True, height=200)

            with col2:
                st.markdown("""
                <div style="font-size: 0.8125rem; font-weight: 500; color: #0F172A; margin-bottom: 0.75rem;">
                    Options
                </div>
                """, unsafe_allow_html=True)

                table_name = st.text_input(
                    "Table name",
                    value=uploaded_file.name.rsplit(".", 1)[0].lower().replace(" ", "_"),
                    key=f"table_{uploaded_file.name}",
                    label_visibility="collapsed",
                    placeholder="Table name",
                )

                target_layer = st.selectbox(
                    "Layer",
                    ["bronze", "silver"],
                    key=f"layer_{uploaded_file.name}",
                    label_visibility="collapsed",
                )

                if st.button("Ingest →", key=f"ingest_{uploaded_file.name}", type="primary", use_container_width=True):
                    _perform_ingestion(uploaded_file, df, table_name, target_layer)

            if "uploaded_dataframes" not in st.session_state:
                st.session_state["uploaded_dataframes"] = {}
            st.session_state["uploaded_dataframes"][uploaded_file.name] = df

        except Exception as e:
            _info_card(f"Error reading file: {e}", "error")


def _perform_ingestion(uploaded_file, df, table_name, target_layer):
    """Perform data ingestion with progress."""
    if not table_name:
        st.error("Please enter a table name.")
        return

    with st.spinner("Ingesting..."):
        progress = st.progress(0)

        schema_def = {
            "columns": [
                {"name": col, "dtype": str(df[col].dtype)}
                for col in df.columns
            ]
        }

        for i in range(50):
            time.sleep(0.01)
            progress.progress(i + 1)

        data_service = get_data_service()
        existing = data_service.get_table_by_name(table_name, target_layer)

        if existing:
            data_service.update_table(
                table_id=existing.id,
                row_count=df.shape[0],
                size_bytes=len(uploaded_file.getvalue()),
                schema_definition=schema_def,
            )
        else:
            data_service.create_table(
                name=table_name,
                layer=target_layer,
                schema_definition=schema_def,
                row_count=df.shape[0],
                size_bytes=len(uploaded_file.getvalue()),
            )

        for i in range(50, 100):
            time.sleep(0.01)
            progress.progress(i + 1)

        st.success(f"Ingested {df.shape[0]:,} rows → {target_layer}.{table_name}")


def show_data_tables_section():
    """Show existing data tables with minimal styling."""
    _section_header("Data Tables", "Browse and manage your tables")

    data_service = get_data_service()

    # Filters
    col1, col2 = st.columns([3, 1])
    with col1:
        search = st.text_input(
            "Search",
            placeholder="Filter by name...",
            label_visibility="collapsed",
        )
    with col2:
        layer_filter = st.selectbox(
            "Layer",
            ["All", "bronze", "silver", "gold"],
            label_visibility="collapsed",
        )

    layer = layer_filter if layer_filter != "All" else None
    tables = data_service.list_tables(layer=layer)

    if search:
        tables = [t for t in tables if search.lower() in t.name.lower()]

    if not tables:
        _empty_state("◇", "No tables found", "Upload files to create tables")
        return

    # Tables count
    st.markdown(f"""
    <div style="
        font-size: 0.75rem;
        color: #64748B;
        margin: 1rem 0 0.75rem;
    ">{len(tables)} table(s)</div>
    """, unsafe_allow_html=True)

    # Table list
    for table in tables:
        _render_table_row(table, data_service)


def _render_table_row(table, data_service):
    """Render a sleek table row."""
    size_kb = (table.size_bytes or 0) / 1024
    size_str = f"{size_kb/1024:.1f} MB" if size_kb > 1024 else f"{size_kb:.1f} KB"

    layer_colors = {
        "bronze": ("#A16207", "#FFFBEB"),
        "silver": ("#6B7280", "#F8FAFC"),
        "gold": ("#CA8A04", "#FEFCE8"),
    }
    layer_color, layer_bg = layer_colors.get(table.layer, ("#64748B", "#F8FAFC"))

    st.markdown(f"""
    <div style="
        background: white;
        border: 1px solid #E2E8F0;
        border-radius: 10px;
        padding: 1rem 1.25rem;
        margin-bottom: 0.5rem;
        display: flex;
        align-items: center;
        gap: 1rem;
    ">
        <div style="flex: 2;">
            <div style="font-weight: 500; color: #0F172A; font-size: 0.9375rem;">
                {table.name}
            </div>
            <div style="font-size: 0.75rem; color: #94A3B8; margin-top: 0.125rem;">
                {table.row_count:,} rows · {size_str}
            </div>
        </div>
        <div style="
            padding: 0.25rem 0.625rem;
            background: {layer_bg};
            color: {layer_color};
            border-radius: 9999px;
            font-size: 0.6875rem;
            font-weight: 500;
            text-transform: uppercase;
        ">{table.layer}</div>
        <div style="font-size: 0.75rem; color: #94A3B8;">
            {table.updated_at.strftime("%m/%d %H:%M")}
        </div>
    </div>
    """, unsafe_allow_html=True)

    with st.expander("Details"):
        schema = table.schema_definition or {}
        columns = schema.get("columns", [])

        if columns:
            st.markdown("""
            <div style="font-size: 0.8125rem; font-weight: 500; color: #0F172A; margin-bottom: 0.5rem;">
                Schema
            </div>
            """, unsafe_allow_html=True)

            cols_html = ""
            for col in columns:
                cols_html += f"""
                <div style="
                    display: flex;
                    justify-content: space-between;
                    padding: 0.375rem 0;
                    border-bottom: 1px solid #F1F5F9;
                    font-size: 0.8125rem;
                ">
                    <span style="color: #0F172A;">{col.get('name')}</span>
                    <span style="color: #94A3B8; font-family: monospace; font-size: 0.75rem;">{col.get('dtype')}</span>
                </div>
                """
            st.markdown(cols_html, unsafe_allow_html=True)

        col1, col2 = st.columns(2)
        with col1:
            if table.quality_score:
                st.metric("Quality", f"{table.quality_score:.0f}%")
        with col2:
            if st.button("Delete", key=f"delete_{table.id}", type="secondary"):
                data_service.delete_table(table.id)
                st.success(f"Deleted '{table.name}'")
                st.rerun()


def show_database_connection_section():
    """Show database connection section with minimal styling."""
    _section_header("Connect to Database", "Configure your data source")

    # Database type selection
    db_types = ["PostgreSQL", "MySQL", "MongoDB", "SQL Server", "Snowflake", "BigQuery"]

    st.markdown("""
    <div style="font-size: 0.8125rem; font-weight: 500; color: #0F172A; margin-bottom: 0.5rem;">
        Database Type
    </div>
    """, unsafe_allow_html=True)

    db_type = st.selectbox(
        "Database Type",
        db_types,
        label_visibility="collapsed",
    )

    st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)

    # Connection details card
    st.markdown("""
    <div style="
        background: white;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 1.25rem;
        margin-bottom: 1rem;
    ">
        <div style="font-size: 0.8125rem; font-weight: 500; color: #0F172A; margin-bottom: 1rem;">
            Connection Details
        </div>
    </div>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        host = st.text_input("Host", value="localhost", placeholder="localhost")
        port = st.number_input(
            "Port",
            value=5432 if db_type == "PostgreSQL" else 3306,
            min_value=1,
            max_value=65535,
        )
        database = st.text_input("Database", placeholder="database_name")

    with col2:
        username = st.text_input("Username", placeholder="username")
        password = st.text_input("Password", type="password", placeholder="••••••••")
        schema = st.text_input("Schema", placeholder="public (optional)")

    st.markdown("<div style='height: 1.5rem;'></div>", unsafe_allow_html=True)

    # Extraction method
    st.markdown("""
    <div style="font-size: 0.8125rem; font-weight: 500; color: #0F172A; margin-bottom: 0.75rem;">
        Extraction Method
    </div>
    """, unsafe_allow_html=True)

    extraction_method = st.radio(
        "Method",
        ["Full Table", "Custom Query", "Incremental"],
        horizontal=True,
        label_visibility="collapsed",
    )

    st.markdown("<div style='height: 0.75rem;'></div>", unsafe_allow_html=True)

    if extraction_method == "Full Table":
        table_name = st.text_input("Table Name", placeholder="customers")
        query = None
        watermark_column = None
    elif extraction_method == "Custom Query":
        query = st.text_area(
            "SQL Query",
            height=100,
            placeholder="SELECT * FROM customers WHERE created_at > '2024-01-01'",
        )
        table_name = None
        watermark_column = None
    else:
        table_name = st.text_input("Table Name", placeholder="customers")
        watermark_column = st.text_input("Watermark Column", value="updated_at")
        query = None
        st.markdown("""
        <div style="
            font-size: 0.75rem;
            color: #64748B;
            background: #F8FAFC;
            padding: 0.625rem 0.875rem;
            border-radius: 6px;
            margin-top: 0.5rem;
        ">
            Incremental extraction tracks changes using the watermark column
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='height: 1.5rem;'></div>", unsafe_allow_html=True)

    # Action buttons
    col1, col2 = st.columns([1, 2])

    with col1:
        if st.button("Test Connection", type="secondary", use_container_width=True):
            _test_database_connection(db_type, host, port, database, username, password, schema)

    with col2:
        if st.button("Start Extraction →", type="primary", use_container_width=True):
            _start_extraction(db_type, host, port, database, username, password, schema, extraction_method, table_name, query)


def _test_database_connection(db_type, host, port, database, username, password, schema):
    """Test database connection."""
    with st.spinner("Testing..."):
        try:
            with _get_api_client() as client:
                response = client.post(
                    "/connectors/test-adhoc",
                    json={
                        "connector_type": db_type.lower().replace(" ", "_"),
                        "config": {
                            "host": host,
                            "port": port,
                            "database": database,
                            "username": username,
                            "password": password,
                            "schema": schema,
                        },
                    },
                )
                if response.status_code == 200:
                    result = response.json()
                    if result.get("success"):
                        st.success(f"Connected successfully")
                    else:
                        st.error(result.get("message", "Connection failed"))
                else:
                    st.error(f"Connection failed: HTTP {response.status_code}")
        except Exception as e:
            st.error(f"Connection failed: {e}")


def _start_extraction(db_type, host, port, database, username, password, schema, extraction_method, table_name, query):
    """Start data extraction."""
    with st.spinner("Extracting..."):
        try:
            with _get_api_client() as client:
                response = client.post(
                    "/ingestion/extract",
                    json={
                        "connector_type": db_type.lower().replace(" ", "_"),
                        "config": {
                            "host": host,
                            "port": port,
                            "database": database,
                            "username": username,
                            "password": password,
                            "schema": schema,
                        },
                        "extraction_method": extraction_method,
                        "table_name": table_name if extraction_method != "Custom Query" else None,
                        "query": query if extraction_method == "Custom Query" else None,
                    },
                )
                if response.status_code == 200:
                    result = response.json()
                    st.success(result.get("message", "Data extracted successfully"))
                else:
                    st.error(f"Extraction failed: HTTP {response.status_code}")
        except Exception as e:
            st.error(f"Extraction failed: {e}")


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


def _info_card(message: str, type: str = "info"):
    """Render minimal info card."""
    colors = {
        "info": ("#3B82F6", "#EFF6FF"),
        "success": ("#10B981", "#ECFDF5"),
        "warning": ("#F59E0B", "#FFFBEB"),
        "error": ("#EF4444", "#FEF2F2"),
    }
    color, bg = colors.get(type, colors["info"])

    st.markdown(f"""
    <div style="
        background: {bg};
        border-left: 3px solid {color};
        padding: 0.875rem 1rem;
        border-radius: 0 8px 8px 0;
        font-size: 0.875rem;
        color: #0F172A;
    ">{message}</div>
    """, unsafe_allow_html=True)
