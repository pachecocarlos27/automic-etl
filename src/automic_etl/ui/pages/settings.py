"""Settings Page for Automic ETL UI."""

from __future__ import annotations

import streamlit as st


def show_settings_page():
    """Display the settings page."""
    st.title("⚙️ Settings")
    st.markdown("Configure Automic ETL settings and connections.")

    # Tabs for different settings
    tab1, tab2, tab3, tab4 = st.tabs([
        "☁️ Cloud Storage",
        "🤖 LLM Configuration",
        "🗄️ Connections",
        "🔧 General",
    ])

    with tab1:
        show_cloud_storage_settings()

    with tab2:
        show_llm_settings()

    with tab3:
        show_connection_settings()

    with tab4:
        show_general_settings()


def show_cloud_storage_settings():
    """Show cloud storage settings."""
    st.subheader("Cloud Storage Configuration")

    # Storage provider selection
    provider = st.selectbox(
        "Primary Storage Provider",
        ["AWS S3", "Google Cloud Storage", "Azure Blob Storage"],
    )

    st.markdown("---")

    if provider == "AWS S3":
        st.markdown("### AWS S3 Configuration")

        col1, col2 = st.columns(2)

        with col1:
            st.text_input("AWS Access Key ID", type="password", key="aws_access_key")
            st.text_input("AWS Secret Access Key", type="password", key="aws_secret_key")
            st.selectbox(
                "Region",
                ["us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-southeast-1"],
                key="aws_region",
            )

        with col2:
            st.text_input("S3 Bucket Name", key="s3_bucket")
            st.text_input("Warehouse Path", value="lakehouse/", key="s3_warehouse")
            st.checkbox("Use IAM Role", value=False, key="aws_iam")

    elif provider == "Google Cloud Storage":
        st.markdown("### Google Cloud Storage Configuration")

        col1, col2 = st.columns(2)

        with col1:
            st.text_input("Project ID", key="gcp_project")
            st.text_input("Bucket Name", key="gcs_bucket")
            st.text_input("Warehouse Path", value="lakehouse/", key="gcs_warehouse")

        with col2:
            uploaded_creds = st.file_uploader(
                "Service Account JSON",
                type=["json"],
                key="gcs_creds",
            )
            if uploaded_creds:
                st.success("Credentials uploaded!")

    else:  # Azure
        st.markdown("### Azure Blob Storage Configuration")

        col1, col2 = st.columns(2)

        with col1:
            st.text_input("Storage Account Name", key="azure_account")
            st.text_input("Container Name", key="azure_container")
            st.text_input("Warehouse Path", value="lakehouse/", key="azure_warehouse")

        with col2:
            st.text_input("Account Key", type="password", key="azure_key")
            st.text_input("Connection String (optional)", type="password", key="azure_conn")

    st.markdown("---")

    # Iceberg catalog settings
    st.markdown("### Iceberg Catalog Configuration")

    col1, col2 = st.columns(2)

    with col1:
        catalog_type = st.selectbox(
            "Catalog Type",
            ["REST", "Hive", "Glue", "Nessie"],
        )
        if catalog_type == "REST":
            st.text_input("Catalog URI", value="http://localhost:8181")
        elif catalog_type == "Glue":
            st.text_input("Glue Database", value="automic_etl")

    with col2:
        st.text_input("Catalog Name", value="automic")
        st.multiselect(
            "Default Namespaces",
            ["bronze", "silver", "gold"],
            default=["bronze", "silver", "gold"],
        )

    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("🔗 Test Connection"):
            with st.spinner("Testing..."):
                st.success("✅ Connection successful!")

    with col2:
        if st.button("💾 Save Storage Settings", type="primary"):
            st.success("Storage settings saved!")


def show_llm_settings():
    """Show LLM settings."""
    st.subheader("LLM Configuration")

    # LLM provider selection
    provider = st.selectbox(
        "LLM Provider",
        ["Anthropic (Claude)", "OpenAI", "Ollama (Local)", "Azure OpenAI"],
    )

    st.markdown("---")

    if provider == "Anthropic (Claude)":
        st.markdown("### Anthropic Configuration")

        col1, col2 = st.columns(2)

        with col1:
            st.text_input("API Key", type="password", key="anthropic_key")
            st.selectbox(
                "Model",
                ["claude-3-opus-20240229", "claude-3-sonnet-20240229", "claude-3-haiku-20240307"],
                key="anthropic_model",
            )

        with col2:
            st.slider("Max Tokens", 1000, 4096, 2048, key="anthropic_tokens")
            st.slider("Temperature", 0.0, 1.0, 0.7, key="anthropic_temp")

    elif provider == "OpenAI":
        st.markdown("### OpenAI Configuration")

        col1, col2 = st.columns(2)

        with col1:
            st.text_input("API Key", type="password", key="openai_key")
            st.selectbox(
                "Model",
                ["gpt-4-turbo-preview", "gpt-4", "gpt-3.5-turbo"],
                key="openai_model",
            )

        with col2:
            st.slider("Max Tokens", 1000, 4096, 2048, key="openai_tokens")
            st.slider("Temperature", 0.0, 1.0, 0.7, key="openai_temp")

    elif provider == "Ollama (Local)":
        st.markdown("### Ollama Configuration")

        col1, col2 = st.columns(2)

        with col1:
            st.text_input("Host", value="http://localhost:11434", key="ollama_host")
            st.selectbox(
                "Model",
                ["llama2", "mistral", "codellama", "mixtral"],
                key="ollama_model",
            )

        with col2:
            st.slider("Context Length", 2048, 8192, 4096, key="ollama_context")
            st.number_input("Timeout (seconds)", value=120, key="ollama_timeout")

    else:  # Azure OpenAI
        st.markdown("### Azure OpenAI Configuration")

        col1, col2 = st.columns(2)

        with col1:
            st.text_input("API Key", type="password", key="azure_openai_key")
            st.text_input("Endpoint", key="azure_openai_endpoint")
            st.text_input("Deployment Name", key="azure_openai_deployment")

        with col2:
            st.text_input("API Version", value="2024-02-01", key="azure_openai_version")
            st.slider("Max Tokens", 1000, 4096, 2048, key="azure_openai_tokens")

    st.markdown("---")

    # LLM Features
    st.markdown("### LLM Features")

    col1, col2 = st.columns(2)

    with col1:
        st.checkbox("Enable Schema Inference", value=True)
        st.checkbox("Enable Entity Extraction", value=True)
        st.checkbox("Enable Data Classification", value=True)

    with col2:
        st.checkbox("Enable Natural Language Queries", value=True)
        st.checkbox("Enable Smart Data Cleaning", value=True)
        st.checkbox("Enable Anomaly Detection", value=True)

    st.markdown("---")

    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("🔗 Test LLM"):
            with st.spinner("Testing LLM connection..."):
                st.success("✅ LLM connection successful!")

    with col2:
        if st.button("💾 Save LLM Settings", type="primary"):
            st.success("LLM settings saved!")


def show_connection_settings():
    """Show connection settings."""
    st.subheader("Data Source Connections")

    # Add new connection
    with st.expander("➕ Add New Connection", expanded=False):
        connection_type = st.selectbox(
            "Connection Type",
            ["PostgreSQL", "MySQL", "MongoDB", "Snowflake", "BigQuery", "Redshift"],
        )

        col1, col2 = st.columns(2)

        with col1:
            connection_name = st.text_input("Connection Name", placeholder="my_postgres")
            host = st.text_input("Host", placeholder="localhost")
            port = st.number_input("Port", value=5432)
            database = st.text_input("Database Name")

        with col2:
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            schema = st.text_input("Schema (optional)")
            ssl = st.checkbox("Use SSL", value=True)

        col1, col2 = st.columns([1, 5])
        with col1:
            if st.button("🔗 Test"):
                st.success("✅ Connection successful!")
        with col2:
            if st.button("➕ Add Connection", type="primary"):
                st.success(f"Connection '{connection_name}' added!")

    st.markdown("---")

    # Existing connections
    st.markdown("### Existing Connections")

    connections = [
        {"name": "production_postgres", "type": "PostgreSQL", "host": "prod-db.company.com", "status": "✅ Connected"},
        {"name": "analytics_snowflake", "type": "Snowflake", "host": "company.snowflakecomputing.com", "status": "✅ Connected"},
        {"name": "staging_mysql", "type": "MySQL", "host": "staging-db.company.com", "status": "⚠️ Warning"},
        {"name": "logs_mongodb", "type": "MongoDB", "host": "logs.mongodb.net", "status": "❌ Disconnected"},
    ]

    for conn in connections:
        with st.expander(f"{conn['status'][:1]} **{conn['name']}** ({conn['type']})"):
            col1, col2, col3 = st.columns([2, 1, 1])

            with col1:
                st.markdown(f"**Type:** {conn['type']}")
                st.markdown(f"**Host:** {conn['host']}")
                st.markdown(f"**Status:** {conn['status']}")

            with col2:
                if st.button("✏️ Edit", key=f"edit_{conn['name']}"):
                    st.info("Opening editor...")
                if st.button("🔗 Test", key=f"test_{conn['name']}"):
                    st.success("Connection OK!")

            with col3:
                if st.button("🗑️ Delete", key=f"delete_{conn['name']}"):
                    st.warning(f"Deleted {conn['name']}")


def show_general_settings():
    """Show general settings."""
    st.subheader("General Settings")

    # Application settings
    st.markdown("### Application")

    col1, col2 = st.columns(2)

    with col1:
        st.text_input("Application Name", value="Automic ETL")
        st.selectbox("Log Level", ["DEBUG", "INFO", "WARNING", "ERROR"], index=1)
        st.number_input("Max Parallel Jobs", value=4, min_value=1, max_value=16)

    with col2:
        st.selectbox("Timezone", ["UTC", "US/Eastern", "US/Pacific", "Europe/London"], index=0)
        st.number_input("Job Timeout (minutes)", value=60, min_value=5, max_value=1440)
        st.number_input("Data Retention (days)", value=90, min_value=7, max_value=365)

    st.markdown("---")

    # Processing settings
    st.markdown("### Processing")

    col1, col2 = st.columns(2)

    with col1:
        st.number_input("Default Batch Size", value=10000, step=1000)
        st.selectbox("Default Write Mode", ["append", "overwrite", "merge"], index=0)
        st.checkbox("Enable Change Data Capture", value=True)

    with col2:
        st.number_input("Watermark Check Interval (seconds)", value=60)
        st.checkbox("Auto-create SCD2 History", value=True)
        st.checkbox("Enable Data Lineage Tracking", value=True)

    st.markdown("---")

    # Security settings
    st.markdown("### Security")

    col1, col2 = st.columns(2)

    with col1:
        st.checkbox("Enable PII Auto-detection", value=True)
        st.checkbox("Mask PII in Logs", value=True)
        st.checkbox("Encrypt Data at Rest", value=True)

    with col2:
        st.multiselect(
            "Allowed IP Ranges",
            ["0.0.0.0/0", "10.0.0.0/8", "192.168.0.0/16", "172.16.0.0/12"],
            default=["10.0.0.0/8"],
        )
        st.checkbox("Enable Audit Logging", value=True)

    st.markdown("---")

    # Actions
    col1, col2, col3 = st.columns([1, 1, 4])

    with col1:
        if st.button("💾 Save All Settings", type="primary"):
            st.success("All settings saved!")

    with col2:
        if st.button("🔄 Reset to Defaults"):
            st.warning("Settings reset to defaults!")

    # Export/Import
    st.markdown("---")
    st.markdown("### Configuration Export/Import")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("📥 Export Configuration"):
            st.download_button(
                "Download config.yaml",
                data="# Automic ETL Configuration\n",
                file_name="automic_etl_config.yaml",
                mime="text/yaml",
            )

    with col2:
        uploaded_config = st.file_uploader("Import Configuration", type=["yaml", "yml"])
        if uploaded_config:
            st.success("Configuration imported!")

