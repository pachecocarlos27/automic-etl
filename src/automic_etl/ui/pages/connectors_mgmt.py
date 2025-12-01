"""Connectors Management Page for Automic ETL UI."""

from __future__ import annotations

import streamlit as st
from datetime import datetime, timedelta
from typing import Any


def show_connectors_management_page():
    """Display the connectors management page."""
    st.title("Connectors")
    st.markdown("Manage data source and destination connectors.")

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Overview",
        "Databases",
        "APIs & SaaS",
        "Streaming",
        "Cloud Storage",
    ])

    with tab1:
        _show_connectors_overview()

    with tab2:
        _show_database_connectors()

    with tab3:
        _show_api_connectors()

    with tab4:
        _show_streaming_connectors()

    with tab5:
        _show_cloud_storage_connectors()


def _show_connectors_overview():
    """Show connectors overview."""
    st.subheader("Connector Overview")

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Connectors", "15", "3 active")

    with col2:
        st.metric("Data Sources", "12", "+2 this week")

    with col3:
        st.metric("Data Synced", "4.2 TB", "last 30 days")

    with col4:
        st.metric("Failed Connections", "1", "needs attention")

    st.markdown("---")

    # Quick add
    st.subheader("Quick Add Connector")

    col1, col2, col3, col4 = st.columns(4)

    connector_types = [
        {"name": "PostgreSQL", "icon": "", "category": "Database"},
        {"name": "Snowflake", "icon": "", "category": "Database"},
        {"name": "Salesforce", "icon": "", "category": "API"},
        {"name": "S3", "icon": "", "category": "Storage"},
        {"name": "Kafka", "icon": "", "category": "Streaming"},
        {"name": "REST API", "icon": "", "category": "API"},
        {"name": "MongoDB", "icon": "", "category": "Database"},
        {"name": "BigQuery", "icon": "", "category": "Database"},
    ]

    for i, conn in enumerate(connector_types):
        with [col1, col2, col3, col4][i % 4]:
            if st.button(f"{conn['icon']} {conn['name']}", key=f"quick_{conn['name']}", use_container_width=True):
                st.info(f"Adding {conn['name']} connector...")

    st.markdown("---")

    # Active connectors
    st.subheader("Active Connectors")

    connectors = [
        {
            "name": "Production PostgreSQL",
            "type": "PostgreSQL",
            "status": "connected",
            "last_sync": "5 min ago",
            "rows_synced": "1.2M",
            "icon": "",
        },
        {
            "name": "Snowflake Data Warehouse",
            "type": "Snowflake",
            "status": "connected",
            "last_sync": "10 min ago",
            "rows_synced": "5.8M",
            "icon": "",
        },
        {
            "name": "Salesforce CRM",
            "type": "Salesforce",
            "status": "connected",
            "last_sync": "1 hour ago",
            "rows_synced": "250K",
            "icon": "",
        },
        {
            "name": "AWS S3 Data Lake",
            "type": "S3",
            "status": "connected",
            "last_sync": "2 min ago",
            "rows_synced": "10M",
            "icon": "",
        },
        {
            "name": "Kafka Events",
            "type": "Kafka",
            "status": "connected",
            "last_sync": "Real-time",
            "rows_synced": "1.5M/day",
            "icon": "",
        },
        {
            "name": "MongoDB Analytics",
            "type": "MongoDB",
            "status": "error",
            "last_sync": "2 days ago",
            "rows_synced": "0",
            "icon": "",
        },
    ]

    for conn in connectors:
        with st.container():
            col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 2, 1])

            with col1:
                st.markdown(f"{conn['icon']} **{conn['name']}**")
                st.caption(f"Type: {conn['type']}")

            with col2:
                status_color = "green" if conn["status"] == "connected" else "red"
                st.markdown(f":{status_color}[{conn['status'].upper()}]")

            with col3:
                st.caption(f"Last sync: {conn['last_sync']}")

            with col4:
                st.caption(f"Rows: {conn['rows_synced']}")

            with col5:
                col_a, col_b = st.columns(2)
                with col_a:
                    st.button("", key=f"edit_conn_{conn['name']}", help="Edit")
                with col_b:
                    st.button("", key=f"sync_conn_{conn['name']}", help="Sync Now")

            st.markdown("---")


def _show_database_connectors():
    """Show database connectors."""
    st.subheader("Database Connectors")

    # Add new database
    with st.expander("Add Database Connector", expanded=False):
        db_type = st.selectbox(
            "Database Type",
            ["PostgreSQL", "MySQL", "SQL Server", "Oracle", "MongoDB", "Snowflake", "BigQuery", "Redshift"],
        )

        col1, col2 = st.columns(2)

        with col1:
            conn_name = st.text_input("Connection Name", placeholder="production_postgres")
            host = st.text_input("Host", placeholder="localhost")
            port = st.number_input("Port", value=5432 if db_type == "PostgreSQL" else 3306)
            database = st.text_input("Database", placeholder="mydb")

        with col2:
            username = st.text_input("Username", placeholder="admin")
            password = st.text_input("Password", type="password")
            ssl_mode = st.selectbox("SSL Mode", ["disable", "require", "verify-ca", "verify-full"])

            if db_type == "Snowflake":
                warehouse = st.text_input("Warehouse", placeholder="COMPUTE_WH")
                role = st.text_input("Role", placeholder="ACCOUNTADMIN")

        # Additional options
        with st.expander("Advanced Options"):
            col1, col2 = st.columns(2)
            with col1:
                connection_timeout = st.number_input("Connection Timeout (s)", value=30)
                max_connections = st.number_input("Max Connections", value=10)
            with col2:
                query_timeout = st.number_input("Query Timeout (s)", value=300)
                retry_attempts = st.number_input("Retry Attempts", value=3)

        col1, col2, col3 = st.columns([1, 1, 2])

        with col1:
            if st.button("Test Connection"):
                with st.spinner("Testing connection..."):
                    import time
                    time.sleep(1)
                st.success("Connection successful!")

        with col2:
            if st.button("Save Connector", type="primary"):
                st.success(f"Connector '{conn_name}' saved!")

    st.markdown("---")

    # Existing database connectors
    databases = [
        {
            "name": "Production PostgreSQL",
            "type": "PostgreSQL",
            "host": "db.example.com:5432",
            "database": "production",
            "status": "connected",
            "tables": 45,
        },
        {
            "name": "Snowflake DW",
            "type": "Snowflake",
            "host": "account.snowflakecomputing.com",
            "database": "ANALYTICS",
            "status": "connected",
            "tables": 120,
        },
        {
            "name": "MySQL Replica",
            "type": "MySQL",
            "host": "replica.example.com:3306",
            "database": "orders",
            "status": "connected",
            "tables": 28,
        },
        {
            "name": "MongoDB Analytics",
            "type": "MongoDB",
            "host": "mongo.example.com:27017",
            "database": "analytics",
            "status": "error",
            "tables": 0,
        },
    ]

    for db in databases:
        with st.container():
            col1, col2, col3, col4 = st.columns([3, 2, 2, 1])

            with col1:
                st.markdown(f"**{db['name']}**")
                st.caption(f"{db['type']} | {db['host']}")

            with col2:
                st.caption(f"Database: {db['database']}")
                st.caption(f"Tables: {db['tables']}")

            with col3:
                status_color = "green" if db["status"] == "connected" else "red"
                st.markdown(f":{status_color}[{db['status'].upper()}]")

            with col4:
                if st.button("Configure", key=f"config_db_{db['name']}"):
                    st.info(f"Configuring {db['name']}...")

            st.markdown("---")


def _show_api_connectors():
    """Show API connectors."""
    st.subheader("API & SaaS Connectors")

    # Add API connector
    with st.expander("Add API Connector", expanded=False):
        api_type = st.selectbox(
            "API Type",
            ["Salesforce", "HubSpot", "Stripe", "Zendesk", "REST API", "GraphQL", "Shopify", "Slack"],
        )

        conn_name = st.text_input("Connection Name", placeholder="salesforce_production", key="api_name")

        if api_type == "Salesforce":
            col1, col2 = st.columns(2)
            with col1:
                sf_username = st.text_input("Username", key="sf_user")
                sf_password = st.text_input("Password", type="password", key="sf_pass")
            with col2:
                sf_token = st.text_input("Security Token", type="password", key="sf_token")
                sf_domain = st.selectbox("Domain", ["login", "test"])

        elif api_type == "HubSpot":
            hs_api_key = st.text_input("API Key", type="password", key="hs_key")

        elif api_type == "Stripe":
            stripe_key = st.text_input("Secret Key", type="password", key="stripe_key")
            stripe_mode = st.selectbox("Mode", ["live", "test"])

        elif api_type == "REST API":
            col1, col2 = st.columns(2)
            with col1:
                rest_url = st.text_input("Base URL", placeholder="https://api.example.com")
                rest_auth = st.selectbox("Auth Type", ["None", "API Key", "Bearer Token", "Basic Auth", "OAuth2"])
            with col2:
                if rest_auth == "API Key":
                    rest_key_name = st.text_input("Header Name", value="X-API-Key")
                    rest_key_value = st.text_input("API Key", type="password")
                elif rest_auth == "Bearer Token":
                    rest_token = st.text_input("Token", type="password")
                elif rest_auth == "Basic Auth":
                    rest_user = st.text_input("Username")
                    rest_pass = st.text_input("Password", type="password")

        col1, col2 = st.columns([1, 3])
        with col1:
            if st.button("Add Connector", type="primary", key="add_api"):
                st.success(f"API connector '{conn_name}' added!")

    st.markdown("---")

    # Existing API connectors
    apis = [
        {
            "name": "Salesforce Production",
            "type": "Salesforce",
            "objects": ["Account", "Contact", "Opportunity", "Lead"],
            "status": "connected",
            "last_sync": "1 hour ago",
        },
        {
            "name": "HubSpot Marketing",
            "type": "HubSpot",
            "objects": ["Contacts", "Companies", "Deals"],
            "status": "connected",
            "last_sync": "30 min ago",
        },
        {
            "name": "Stripe Payments",
            "type": "Stripe",
            "objects": ["Charges", "Customers", "Subscriptions"],
            "status": "connected",
            "last_sync": "5 min ago",
        },
        {
            "name": "Custom REST API",
            "type": "REST API",
            "objects": ["/users", "/orders", "/products"],
            "status": "connected",
            "last_sync": "15 min ago",
        },
    ]

    for api in apis:
        with st.container():
            col1, col2, col3, col4 = st.columns([3, 2, 2, 1])

            with col1:
                st.markdown(f"**{api['name']}**")
                st.caption(f"Type: {api['type']}")

            with col2:
                objects_str = ", ".join(api["objects"][:3])
                if len(api["objects"]) > 3:
                    objects_str += f" (+{len(api['objects']) - 3} more)"
                st.caption(f"Objects: {objects_str}")

            with col3:
                st.markdown(":green[CONNECTED]")
                st.caption(f"Last sync: {api['last_sync']}")

            with col4:
                st.button("Sync", key=f"sync_api_{api['name']}")

            st.markdown("---")


def _show_streaming_connectors():
    """Show streaming connectors."""
    st.subheader("Streaming Connectors")

    # Add streaming connector
    with st.expander("Add Streaming Connector", expanded=False):
        stream_type = st.selectbox(
            "Streaming Platform",
            ["Apache Kafka", "AWS Kinesis", "Google Pub/Sub", "Azure Event Hubs", "Redis Streams"],
        )

        conn_name = st.text_input("Connection Name", placeholder="kafka_production", key="stream_name")

        if stream_type == "Apache Kafka":
            col1, col2 = st.columns(2)
            with col1:
                bootstrap_servers = st.text_input("Bootstrap Servers", placeholder="kafka1:9092,kafka2:9092")
                consumer_group = st.text_input("Consumer Group", placeholder="automic-etl-group")
            with col2:
                security_protocol = st.selectbox("Security Protocol", ["PLAINTEXT", "SSL", "SASL_SSL"])
                if security_protocol in ["SSL", "SASL_SSL"]:
                    st.text_input("SSL Certificate Path", placeholder="/path/to/cert.pem")

            st.checkbox("Enable Schema Registry", value=True)
            st.text_input("Schema Registry URL", placeholder="http://schema-registry:8081")

        elif stream_type == "AWS Kinesis":
            col1, col2 = st.columns(2)
            with col1:
                stream_name = st.text_input("Stream Name", placeholder="my-kinesis-stream")
                region = st.selectbox("AWS Region", ["us-east-1", "us-west-2", "eu-west-1"])
            with col2:
                access_key = st.text_input("Access Key", type="password")
                secret_key = st.text_input("Secret Key", type="password")

        elif stream_type == "Google Pub/Sub":
            project_id = st.text_input("Project ID", placeholder="my-gcp-project")
            subscription = st.text_input("Subscription Name", placeholder="my-subscription")
            st.file_uploader("Service Account JSON", key="pubsub_sa")

        col1, col2 = st.columns([1, 3])
        with col1:
            if st.button("Add Connector", type="primary", key="add_stream"):
                st.success(f"Streaming connector '{conn_name}' added!")

    st.markdown("---")

    # Existing streaming connectors
    streams = [
        {
            "name": "Kafka Production",
            "type": "Kafka",
            "topics": ["orders", "customers", "events"],
            "status": "connected",
            "messages_per_sec": "1,234",
            "lag": "150",
        },
        {
            "name": "AWS Kinesis Events",
            "type": "Kinesis",
            "topics": ["user-events", "click-stream"],
            "status": "connected",
            "messages_per_sec": "856",
            "lag": "50",
        },
    ]

    for stream in streams:
        with st.container():
            col1, col2, col3, col4 = st.columns([3, 2, 2, 1])

            with col1:
                st.markdown(f" **{stream['name']}**")
                st.caption(f"Type: {stream['type']}")

            with col2:
                topics_str = ", ".join(stream["topics"])
                st.caption(f"Topics: {topics_str}")
                st.caption(f"Lag: {stream['lag']} messages")

            with col3:
                st.markdown(":green[STREAMING]")
                st.caption(f"{stream['messages_per_sec']} msg/s")

            with col4:
                st.button("Monitor", key=f"monitor_stream_{stream['name']}")

            st.markdown("---")

    # Streaming metrics
    st.subheader("Streaming Metrics")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Total Messages/sec", "2,090")

    with col2:
        st.metric("Total Lag", "200 messages")

    with col3:
        st.metric("Consumer Groups", "3")


def _show_cloud_storage_connectors():
    """Show cloud storage connectors."""
    st.subheader("Cloud Storage Connectors")

    # Add storage connector
    with st.expander("Add Storage Connector", expanded=False):
        storage_type = st.selectbox(
            "Storage Type",
            ["AWS S3", "Google Cloud Storage", "Azure Blob Storage", "MinIO", "HDFS"],
        )

        conn_name = st.text_input("Connection Name", placeholder="s3_data_lake", key="storage_name")

        if storage_type == "AWS S3":
            col1, col2 = st.columns(2)
            with col1:
                bucket = st.text_input("Bucket Name", placeholder="my-data-lake")
                prefix = st.text_input("Prefix (optional)", placeholder="bronze/")
                region = st.selectbox("Region", ["us-east-1", "us-west-2", "eu-west-1"], key="s3_region")
            with col2:
                access_key = st.text_input("Access Key ID", type="password", key="s3_access")
                secret_key = st.text_input("Secret Access Key", type="password", key="s3_secret")
                st.checkbox("Use IAM Role", value=False)

        elif storage_type == "Google Cloud Storage":
            bucket = st.text_input("Bucket Name", placeholder="my-gcs-bucket", key="gcs_bucket")
            prefix = st.text_input("Prefix (optional)", placeholder="data/", key="gcs_prefix")
            st.file_uploader("Service Account JSON", key="gcs_sa")

        elif storage_type == "Azure Blob Storage":
            col1, col2 = st.columns(2)
            with col1:
                account_name = st.text_input("Storage Account Name")
                container = st.text_input("Container Name")
            with col2:
                account_key = st.text_input("Account Key", type="password")
                sas_token = st.text_input("SAS Token (optional)", type="password")

        col1, col2 = st.columns([1, 3])
        with col1:
            if st.button("Add Connector", type="primary", key="add_storage"):
                st.success(f"Storage connector '{conn_name}' added!")

    st.markdown("---")

    # Existing storage connectors
    storages = [
        {
            "name": "AWS S3 Data Lake",
            "type": "S3",
            "location": "s3://data-lake-prod/",
            "status": "connected",
            "size": "2.4 TB",
            "files": "12,456",
        },
        {
            "name": "GCS Analytics",
            "type": "GCS",
            "location": "gs://analytics-bucket/",
            "status": "connected",
            "size": "850 GB",
            "files": "5,234",
        },
        {
            "name": "Azure Archive",
            "type": "Azure",
            "location": "azure://archive-container/",
            "status": "connected",
            "size": "1.1 TB",
            "files": "8,901",
        },
    ]

    for storage in storages:
        with st.container():
            col1, col2, col3, col4 = st.columns([3, 2, 2, 1])

            with col1:
                st.markdown(f" **{storage['name']}**")
                st.caption(f"Type: {storage['type']}")

            with col2:
                st.caption(f"Location: {storage['location']}")

            with col3:
                st.markdown(":green[CONNECTED]")
                st.caption(f"Size: {storage['size']} | Files: {storage['files']}")

            with col4:
                st.button("Browse", key=f"browse_storage_{storage['name']}")

            st.markdown("---")
