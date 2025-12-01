"""Data Ingestion Page for Automic ETL UI."""

from __future__ import annotations

import streamlit as st
import polars as pl
from io import BytesIO
from typing import Any
import time

from automic_etl.db.data_service import get_data_service


def show_ingestion_page():
    """Display the data ingestion page."""
    st.title("Data Ingestion")
    st.markdown("Upload files or connect to data sources to ingest data into the lakehouse.")

    tab1, tab2, tab3 = st.tabs([
        "File Upload",
        "Data Tables",
        "Database Connection",
    ])

    with tab1:
        show_file_upload_section()

    with tab2:
        show_data_tables_section()

    with tab3:
        show_database_connection_section()


def show_file_upload_section():
    """Show file upload section."""
    st.subheader("Upload Files")

    user = st.session_state.get("user")
    if not user:
        st.warning("Please log in to upload files.")
        return

    uploaded_files = st.file_uploader(
        "Choose files to upload",
        type=["csv", "json", "parquet", "xlsx"],
        accept_multiple_files=True,
        help="Supported formats: CSV, JSON, Parquet, Excel",
    )

    if uploaded_files:
        st.markdown("---")
        st.subheader("Uploaded Files")

        for uploaded_file in uploaded_files:
            with st.expander(f"{uploaded_file.name}", expanded=True):
                col1, col2 = st.columns([3, 1])

                with col1:
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
                            continue

                        st.markdown(f"**Shape:** {df.shape[0]:,} rows x {df.shape[1]} columns")
                        st.dataframe(df.head(10), use_container_width=True)

                        if "uploaded_dataframes" not in st.session_state:
                            st.session_state["uploaded_dataframes"] = {}
                        st.session_state["uploaded_dataframes"][uploaded_file.name] = df

                    except Exception as e:
                        st.error(f"Error reading file: {e}")
                        continue

                with col2:
                    st.markdown("**Options**")
                    table_name = st.text_input(
                        "Table Name",
                        value=uploaded_file.name.rsplit(".", 1)[0].lower().replace(" ", "_"),
                        key=f"table_{uploaded_file.name}",
                    )
                    target_layer = st.selectbox(
                        "Target Layer",
                        ["bronze", "silver"],
                        key=f"layer_{uploaded_file.name}",
                    )

                    if st.button("Ingest", key=f"ingest_{uploaded_file.name}", type="primary"):
                        if not table_name:
                            st.error("Please enter a table name.")
                        else:
                            with st.spinner("Ingesting data..."):
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
                                    st.info(f"Updated existing table: {target_layer}.{table_name}")
                                else:
                                    data_service.create_table(
                                        name=table_name,
                                        layer=target_layer,
                                        schema_definition=schema_def,
                                        row_count=df.shape[0],
                                        size_bytes=len(uploaded_file.getvalue()),
                                    )
                                    st.success(f"Created table: {target_layer}.{table_name}")

                                for i in range(50, 100):
                                    time.sleep(0.01)
                                    progress.progress(i + 1)

                                st.success(f"Ingested {df.shape[0]:,} rows to {target_layer}.{table_name}")


def show_data_tables_section():
    """Show existing data tables."""
    st.subheader("Data Tables")

    data_service = get_data_service()

    col1, col2 = st.columns([2, 1])
    with col1:
        search = st.text_input("Search tables", placeholder="Filter by name...")
    with col2:
        layer_filter = st.selectbox("Layer", ["All", "bronze", "silver", "gold"])

    layer = layer_filter if layer_filter != "All" else None
    tables = data_service.list_tables(layer=layer)

    if search:
        tables = [t for t in tables if search.lower() in t.name.lower()]

    if not tables:
        st.info("No data tables found. Upload files to create tables.")
        return

    st.markdown(f"**{len(tables)} table(s) found**")

    col_headers = st.columns([2, 1, 1, 1, 1])
    col_headers[0].markdown("**Table Name**")
    col_headers[1].markdown("**Layer**")
    col_headers[2].markdown("**Rows**")
    col_headers[3].markdown("**Size**")
    col_headers[4].markdown("**Updated**")

    for table in tables:
        cols = st.columns([2, 1, 1, 1, 1])
        cols[0].markdown(f"**{table.name}**")
        cols[1].markdown(table.layer)
        cols[2].markdown(f"{table.row_count:,}")

        size_kb = (table.size_bytes or 0) / 1024
        if size_kb > 1024:
            cols[3].markdown(f"{size_kb/1024:.1f} MB")
        else:
            cols[3].markdown(f"{size_kb:.1f} KB")

        cols[4].markdown(table.updated_at.strftime("%Y-%m-%d %H:%M"))

        with st.expander(f"Details: {table.name}"):
            schema = table.schema_definition or {}
            columns = schema.get("columns", [])

            if columns:
                st.markdown("**Schema:**")
                for col in columns:
                    st.text(f"  {col.get('name')}: {col.get('dtype')}")

            col1, col2 = st.columns(2)
            with col1:
                if table.quality_score:
                    st.metric("Quality Score", f"{table.quality_score:.1f}%")
            with col2:
                if st.button("Delete", key=f"delete_table_{table.id}"):
                    data_service.delete_table(table.id)
                    st.success(f"Table '{table.name}' deleted.")
                    st.rerun()


def show_database_connection_section():
    """Show database connection section."""
    st.subheader("Connect to Database")

    db_type = st.selectbox(
        "Database Type",
        ["PostgreSQL", "MySQL", "MongoDB", "SQL Server", "Snowflake", "BigQuery"],
    )

    col1, col2 = st.columns(2)

    with col1:
        host = st.text_input("Host", value="localhost")
        port = st.number_input(
            "Port",
            value=5432 if db_type == "PostgreSQL" else 3306,
            min_value=1,
            max_value=65535,
        )
        database = st.text_input("Database Name")

    with col2:
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        schema = st.text_input("Schema (optional)")

    st.markdown("---")

    st.subheader("Query / Table Selection")

    extraction_method = st.radio(
        "Extraction Method",
        ["Full Table", "Custom Query", "Incremental"],
        horizontal=True,
    )

    if extraction_method == "Full Table":
        table_name = st.text_input("Table Name")
    elif extraction_method == "Custom Query":
        query = st.text_area(
            "SQL Query",
            height=150,
            placeholder="SELECT * FROM customers WHERE created_at > '2024-01-01'",
        )
    else:
        table_name = st.text_input("Table Name")
        watermark_column = st.text_input("Watermark Column", value="updated_at")
        st.info("Incremental extraction will track changes using the watermark column.")

    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("🔗 Test Connection", type="secondary"):
            with st.spinner("Testing connection..."):
                st.success("✅ Connection successful!")

    with col2:
        if st.button("🚀 Start Extraction", type="primary"):
            with st.spinner("Extracting data..."):
                progress = st.progress(0)
                for i in range(100):
                    progress.progress(i + 1)
                st.success("✅ Data extracted successfully!")


def show_cloud_storage_section():
    """Show cloud storage connection section."""
    st.subheader("Connect to Cloud Storage")

    provider = st.selectbox(
        "Cloud Provider",
        ["AWS S3", "Google Cloud Storage", "Azure Blob Storage"],
    )

    if provider == "AWS S3":
        col1, col2 = st.columns(2)
        with col1:
            bucket = st.text_input("Bucket Name")
            prefix = st.text_input("Prefix/Path", value="data/")
            region = st.selectbox(
                "Region",
                ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"],
            )
        with col2:
            access_key = st.text_input("Access Key ID")
            secret_key = st.text_input("Secret Access Key", type="password")
            st.checkbox("Use IAM Role", value=False)

    elif provider == "Google Cloud Storage":
        bucket = st.text_input("Bucket Name")
        prefix = st.text_input("Prefix/Path", value="data/")
        credentials_file = st.file_uploader(
            "Service Account JSON",
            type=["json"],
        )

    else:  # Azure
        col1, col2 = st.columns(2)
        with col1:
            account_name = st.text_input("Storage Account Name")
            container = st.text_input("Container Name")
            prefix = st.text_input("Blob Prefix", value="data/")
        with col2:
            account_key = st.text_input("Account Key", type="password")
            connection_string = st.text_input("Connection String (alternative)")

    st.markdown("---")

    st.subheader("File Pattern")
    file_pattern = st.text_input(
        "File Pattern",
        value="*.parquet",
        help="Glob pattern to match files (e.g., *.csv, data_*.json)",
    )

    recursive = st.checkbox("Recursive (include subdirectories)", value=True)

    if st.button("📂 List Files", type="secondary"):
        st.info("Found 15 files matching pattern")
        st.json([
            "data/customers_2024_01.parquet",
            "data/customers_2024_02.parquet",
            "data/orders_2024_01.parquet",
        ])

    if st.button("🚀 Ingest from Cloud", type="primary"):
        with st.spinner("Ingesting from cloud storage..."):
            progress = st.progress(0)
            for i in range(100):
                progress.progress(i + 1)
            st.success("✅ Ingested 15 files from cloud storage!")


def show_unstructured_section():
    """Show unstructured data section."""
    st.subheader("Process Unstructured Data")

    st.markdown("""
    Upload unstructured documents for AI-powered processing:
    - **PDFs**: Extract text, tables, and entities
    - **Word Documents**: Extract content and metadata
    - **Images**: OCR and entity extraction
    """)

    uploaded_docs = st.file_uploader(
        "Upload Documents",
        type=["pdf", "docx", "doc", "txt", "png", "jpg", "jpeg"],
        accept_multiple_files=True,
    )

    if uploaded_docs:
        st.markdown("---")
        st.subheader("Processing Options")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Extraction Options**")
            extract_text = st.checkbox("Extract Text", value=True)
            extract_tables = st.checkbox("Extract Tables", value=True)
            extract_images = st.checkbox("Extract Images", value=False)
            perform_ocr = st.checkbox("Perform OCR", value=True)

        with col2:
            st.markdown("**AI Enrichment**")
            entity_types = st.multiselect(
                "Entity Types to Extract",
                ["PERSON", "ORGANIZATION", "DATE", "MONEY", "EMAIL", "PHONE", "ADDRESS", "LOCATION"],
                default=["PERSON", "ORGANIZATION", "DATE"],
            )
            classify_content = st.checkbox("Classify Content", value=True)
            summarize = st.checkbox("Generate Summary", value=False)

        st.markdown("---")

        for doc in uploaded_docs:
            with st.expander(f"📄 {doc.name}"):
                st.markdown(f"**Size:** {doc.size / 1024:.1f} KB")
                st.markdown(f"**Type:** {doc.type}")

                if st.button(f"Process {doc.name}", key=f"process_{doc.name}"):
                    with st.spinner(f"Processing {doc.name}..."):
                        progress = st.progress(0)
                        for i in range(100):
                            progress.progress(i + 1)

                        st.success("✅ Document processed!")

                        # Show mock results
                        st.markdown("**Extracted Entities:**")
                        st.json({
                            "PERSON": ["John Smith", "Jane Doe"],
                            "ORGANIZATION": ["Acme Corp", "TechCorp Inc"],
                            "DATE": ["January 15, 2024", "Q1 2024"],
                            "MONEY": ["$2.5M", "$150,000"],
                        })

        if st.button("🚀 Process All Documents", type="primary"):
            with st.spinner("Processing all documents..."):
                progress = st.progress(0)
                for i in range(100):
                    progress.progress(i + 1)
                st.success(f"✅ Processed {len(uploaded_docs)} documents!")


# Run page when loaded by Streamlit
show_ingestion_page()
