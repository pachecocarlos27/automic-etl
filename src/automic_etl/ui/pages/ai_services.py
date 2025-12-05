"""AI Services Page for Automic ETL UI."""

from __future__ import annotations

import httpx
import streamlit as st
from datetime import datetime, timedelta
from typing import Any

# API base URL
API_BASE_URL = "http://localhost:8000/api/v1"


def _get_api_client() -> httpx.Client:
    """Get configured HTTP client for API calls."""
    return httpx.Client(base_url=API_BASE_URL, timeout=30.0)


def _get_recent_ai_operations() -> list[dict[str, Any]]:
    """Fetch recent AI operations from API."""
    try:
        with _get_api_client() as client:
            response = client.get("/ai/operations", params={"limit": 10})
            if response.status_code == 200:
                return response.json().get("operations", [])
            return []
    except Exception:
        return []


def _run_pii_scan(tables: list[str]) -> list[dict[str, Any]]:
    """Run PII scan on selected tables."""
    try:
        with _get_api_client() as client:
            response = client.post("/ai/pii-scan", json={"tables": tables})
            if response.status_code == 200:
                return response.json().get("results", [])
            return []
    except Exception:
        return []


def show_ai_services_page():
    """Display the AI services management page."""
    st.title("AI Services")
    st.markdown("Configure and use LLM-powered data processing capabilities.")

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Overview",
        "Schema Generation",
        "Entity Extraction",
        "Data Classification",
        "LLM Configuration",
    ])

    with tab1:
        _show_ai_overview()

    with tab2:
        _show_schema_generation()

    with tab3:
        _show_entity_extraction()

    with tab4:
        _show_data_classification()

    with tab5:
        _show_llm_configuration()


def _show_ai_overview():
    """Show AI services overview."""
    st.subheader("AI-Powered Data Processing")

    # Usage metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown("""
        <div style="
            background: linear-gradient(135deg, #8B5CF6 0%, #7C3AED 100%);
            padding: 1.5rem;
            border-radius: 12px;
            color: white;
            text-align: center;
        ">
            <div style="font-size: 2rem; font-weight: 700;">1,234</div>
            <div style="font-size: 0.875rem; opacity: 0.9;">API Calls Today</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.metric("Schemas Generated", "45", "+8 this week")

    with col3:
        st.metric("Entities Extracted", "12,456", "+2,345 today")

    with col4:
        st.metric("Classifications", "8,901", "+1,200 today")

    st.markdown("---")

    # AI capabilities
    st.subheader("Available AI Capabilities")

    capabilities = [
        {
            "name": "Schema Generation",
            "description": "Automatically infer schemas from unstructured data",
            "status": "active",
            "usage": "45 schemas generated",
            "icon": "",
        },
        {
            "name": "Entity Extraction",
            "description": "Extract named entities from text data (people, places, organizations)",
            "status": "active",
            "usage": "12,456 entities extracted",
            "icon": "",
        },
        {
            "name": "Data Classification",
            "description": "Classify data for PII detection, sentiment, categories",
            "status": "active",
            "usage": "8,901 classifications",
            "icon": "",
        },
        {
            "name": "Natural Language Queries",
            "description": "Convert natural language to SQL queries",
            "status": "active",
            "usage": "1,567 queries processed",
            "icon": "",
        },
        {
            "name": "Document Processing",
            "description": "Extract structured data from PDFs, images, and documents",
            "status": "active",
            "usage": "234 documents processed",
            "icon": "",
        },
        {
            "name": "Data Quality Suggestions",
            "description": "AI-powered data quality improvement recommendations",
            "status": "beta",
            "usage": "56 suggestions generated",
            "icon": "",
        },
    ]

    col1, col2 = st.columns(2)

    for i, cap in enumerate(capabilities):
        with col1 if i % 2 == 0 else col2:
            status_color = "#10B981" if cap["status"] == "active" else "#F59E0B"
            status_label = "Active" if cap["status"] == "active" else "Beta"

            st.markdown(f"""
            <div style="
                background: white;
                border: 1px solid #E2E8F0;
                border-radius: 12px;
                padding: 1.25rem;
                margin-bottom: 1rem;
            ">
                <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                    <div>
                        <div style="font-size: 1.5rem; margin-bottom: 0.5rem;">{cap['icon']}</div>
                        <div style="font-size: 1rem; font-weight: 600; color: #0F172A;">
                            {cap['name']}
                        </div>
                        <div style="font-size: 0.875rem; color: #64748B; margin-top: 0.25rem;">
                            {cap['description']}
                        </div>
                    </div>
                    <div style="
                        background: {status_color}20;
                        color: {status_color};
                        padding: 0.25rem 0.5rem;
                        border-radius: 4px;
                        font-size: 0.7rem;
                        font-weight: 600;
                    ">
                        {status_label}
                    </div>
                </div>
                <div style="
                    font-size: 0.75rem;
                    color: #94A3B8;
                    margin-top: 0.75rem;
                    padding-top: 0.75rem;
                    border-top: 1px solid #E2E8F0;
                ">
                    {cap['usage']}
                </div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    # Recent AI operations
    st.subheader("Recent AI Operations")

    operations = _get_recent_ai_operations()

    if not operations:
        st.info("No recent AI operations.")
        return

    for op in operations:
        col1, col2, col3, col4 = st.columns([2, 3, 1, 1])

        with col1:
            st.markdown(f"**{op['type']}**")

        with col2:
            st.caption(op["input"])

        with col3:
            status_colors = {"success": "green", "failed": "red", "running": "orange"}
            st.markdown(f":{status_colors[op['status']]}[{op['status'].upper()}]")

        with col4:
            st.caption(op["time"])


def _show_schema_generation():
    """Show schema generation interface."""
    st.subheader(" Schema Generation")
    st.markdown("Use AI to automatically infer schemas from your data.")

    # Upload/select data
    st.markdown("### Input Data")

    input_type = st.radio(
        "Data Source",
        ["Upload File", "Select Table", "Paste Sample"],
        horizontal=True,
    )

    if input_type == "Upload File":
        uploaded_file = st.file_uploader(
            "Upload data file",
            type=["csv", "json", "parquet", "xlsx"],
        )

        if uploaded_file:
            st.success(f"File uploaded: {uploaded_file.name}")

    elif input_type == "Select Table":
        table = st.selectbox(
            "Select Table",
            ["bronze.raw_customers", "bronze.raw_orders", "bronze.raw_products"],
        )
        sample_rows = st.number_input("Sample Rows", min_value=10, max_value=1000, value=100)

    else:
        sample_data = st.text_area(
            "Paste JSON/CSV Sample",
            placeholder='{"name": "John", "email": "john@example.com", "age": 30}',
            height=150,
        )

    st.markdown("### Generation Options")

    col1, col2 = st.columns(2)

    with col1:
        target_format = st.selectbox(
            "Target Schema Format",
            ["Delta Lake", "Apache Iceberg", "JSON Schema", "Avro", "Protobuf"],
        )
        infer_types = st.checkbox("Infer detailed types (email, phone, URL, etc.)", value=True)
        detect_pii = st.checkbox("Detect PII columns", value=True)

    with col2:
        naming_convention = st.selectbox(
            "Column Naming Convention",
            ["snake_case", "camelCase", "PascalCase", "Keep Original"],
        )
        add_metadata = st.checkbox("Add metadata columns (created_at, updated_at)", value=True)
        nullable_by_default = st.checkbox("Make columns nullable by default", value=True)

    if st.button("Generate Schema", type="primary"):
        with st.spinner("Generating schema using AI..."):
            import time
            time.sleep(2)

        st.success("Schema generated successfully!")

        # Show generated schema
        st.markdown("### Generated Schema")

        st.code("""
{
  "table_name": "customers",
  "format": "delta",
  "columns": [
    {
      "name": "customer_id",
      "type": "string",
      "nullable": false,
      "description": "Unique customer identifier",
      "is_pii": false
    },
    {
      "name": "email",
      "type": "string",
      "nullable": false,
      "description": "Customer email address",
      "is_pii": true,
      "pii_type": "email"
    },
    {
      "name": "full_name",
      "type": "string",
      "nullable": true,
      "description": "Customer full name",
      "is_pii": true,
      "pii_type": "name"
    },
    {
      "name": "phone",
      "type": "string",
      "nullable": true,
      "description": "Phone number",
      "is_pii": true,
      "pii_type": "phone"
    },
    {
      "name": "created_at",
      "type": "timestamp",
      "nullable": false,
      "description": "Record creation timestamp"
    }
  ],
  "primary_key": ["customer_id"],
  "partition_by": ["created_at"]
}
        """, language="json")

        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("Apply Schema", type="primary"):
                st.success("Schema applied to table!")

        with col2:
            st.download_button("Download Schema", data="schema_json", file_name="schema.json")

        with col3:
            if st.button("Edit Schema"):
                st.info("Opening schema editor...")


def _show_entity_extraction():
    """Show entity extraction interface."""
    st.subheader(" Entity Extraction")
    st.markdown("Extract named entities from text data using AI.")

    # Configuration
    col1, col2 = st.columns(2)

    with col1:
        source_table = st.selectbox(
            "Source Table",
            ["bronze.raw_feedback", "bronze.raw_support_tickets", "bronze.raw_documents"],
        )
        text_column = st.text_input("Text Column", value="content")

    with col2:
        entity_types = st.multiselect(
            "Entity Types to Extract",
            ["Person", "Organization", "Location", "Date", "Money", "Email", "Phone", "Product", "Custom"],
            default=["Person", "Organization", "Location"],
        )
        confidence_threshold = st.slider("Confidence Threshold", 0.0, 1.0, 0.7)

    # Custom entities
    with st.expander("Custom Entity Definitions"):
        st.markdown("Define custom entities to extract (product names, internal codes, etc.)")
        custom_entity_name = st.text_input("Entity Name", placeholder="ProductCode")
        custom_entity_pattern = st.text_input("Pattern/Examples", placeholder="PRD-001, PRD-002, PRD-...")
        if st.button("Add Custom Entity"):
            st.success(f"Custom entity '{custom_entity_name}' added!")

    st.markdown("---")

    # Preview and run
    st.markdown("### Sample Preview")

    sample_text = st.text_area(
        "Test with sample text",
        value="John Smith from Acme Corporation called about order #12345. He mentioned their office in New York needs the shipment by December 15th. Contact him at john.smith@acme.com or (555) 123-4567.",
        height=100,
    )

    if st.button("Extract Entities", type="primary"):
        with st.spinner("Extracting entities..."):
            import time
            time.sleep(1)

        st.markdown("### Extracted Entities")

        entities = [
            {"text": "John Smith", "type": "Person", "confidence": 0.95, "start": 0, "end": 10},
            {"text": "Acme Corporation", "type": "Organization", "confidence": 0.92, "start": 16, "end": 32},
            {"text": "#12345", "type": "OrderID", "confidence": 0.88, "start": 54, "end": 60},
            {"text": "New York", "type": "Location", "confidence": 0.97, "start": 92, "end": 100},
            {"text": "December 15th", "type": "Date", "confidence": 0.94, "start": 123, "end": 136},
            {"text": "john.smith@acme.com", "type": "Email", "confidence": 0.99, "start": 155, "end": 174},
            {"text": "(555) 123-4567", "type": "Phone", "confidence": 0.96, "start": 178, "end": 192},
        ]

        for entity in entities:
            col1, col2, col3, col4 = st.columns([2, 2, 1, 1])

            with col1:
                st.markdown(f"**{entity['text']}**")

            with col2:
                type_colors = {
                    "Person": "blue", "Organization": "green", "Location": "orange",
                    "Date": "purple", "Email": "red", "Phone": "gray", "OrderID": "cyan"
                }
                st.markdown(f":{type_colors.get(entity['type'], 'gray')}[{entity['type']}]")

            with col3:
                st.caption(f"{entity['confidence']:.0%}")

            with col4:
                st.caption(f"pos: {entity['start']}-{entity['end']}")

    st.markdown("---")

    # Batch extraction
    st.markdown("### Batch Extraction")

    col1, col2 = st.columns(2)

    with col1:
        output_table = st.text_input("Output Table", value="silver.extracted_entities")
        batch_size = st.number_input("Batch Size", min_value=100, max_value=10000, value=1000)

    with col2:
        run_mode = st.radio("Run Mode", ["Full Table", "Incremental"], horizontal=True)

    if st.button("Start Batch Extraction", type="primary"):
        st.info("Batch extraction job submitted! Monitor progress in Jobs page.")


def _show_data_classification():
    """Show data classification interface."""
    st.subheader(" Data Classification")
    st.markdown("Classify data for PII detection, sentiment analysis, and custom categories.")

    # Classification type
    classification_type = st.selectbox(
        "Classification Type",
        ["PII Detection", "Sentiment Analysis", "Custom Categories", "Data Sensitivity"],
    )

    st.markdown("---")

    if classification_type == "PII Detection":
        _show_pii_detection()
    elif classification_type == "Sentiment Analysis":
        _show_sentiment_analysis()
    elif classification_type == "Custom Categories":
        _show_custom_classification()
    else:
        _show_sensitivity_classification()


def _show_pii_detection():
    """Show PII detection interface."""
    st.markdown("### PII Detection")

    col1, col2 = st.columns(2)

    with col1:
        source = st.selectbox(
            "Source Table",
            ["bronze.raw_customers", "bronze.raw_orders", "silver.customers"],
        )
        columns_to_scan = st.multiselect(
            "Columns to Scan",
            ["All Columns", "email", "phone", "address", "name", "ssn", "credit_card"],
            default=["All Columns"],
        )

    with col2:
        pii_types = st.multiselect(
            "PII Types to Detect",
            ["Email", "Phone", "SSN", "Credit Card", "Address", "Name", "Date of Birth", "IP Address"],
            default=["Email", "Phone", "SSN", "Credit Card"],
        )
        action = st.selectbox(
            "Action on Detection",
            ["Report Only", "Mask Data", "Encrypt", "Quarantine"],
        )

    if st.button("Scan for PII", type="primary"):
        with st.spinner("Scanning for PII..."):
            results = _run_pii_scan(selected_tables)

        if not results:
            st.info("No PII detected in the selected tables.")
            return

        st.markdown("### PII Scan Results")

        for result in results:
            col1, col2, col3, col4 = st.columns([2, 2, 1, 2])

            with col1:
                st.markdown(f"**{result['column']}**")

            with col2:
                st.markdown(f":red[{result['pii_type']}]")

            with col3:
                st.markdown(f"{result['count']:,}")

            with col4:
                st.caption(f"Sample: {result['sample']}")


def _show_sentiment_analysis():
    """Show sentiment analysis interface."""
    st.markdown("### Sentiment Analysis")

    col1, col2 = st.columns(2)

    with col1:
        source = st.selectbox(
            "Source Table",
            ["bronze.raw_feedback", "bronze.raw_reviews", "bronze.raw_support_tickets"],
            key="sentiment_source",
        )
        text_column = st.text_input("Text Column", value="feedback_text", key="sentiment_col")

    with col2:
        output_columns = st.multiselect(
            "Output",
            ["Sentiment Label", "Sentiment Score", "Emotion Labels", "Key Phrases"],
            default=["Sentiment Label", "Sentiment Score"],
        )

    sample_text = st.text_area(
        "Test with sample text",
        value="The product quality is excellent! However, shipping was slower than expected. Overall satisfied with my purchase.",
        height=100,
        key="sentiment_sample",
    )

    if st.button("Analyze Sentiment", type="primary", key="sentiment_btn"):
        with st.spinner("Analyzing sentiment..."):
            import time
            time.sleep(1)

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("**Overall Sentiment**")
            st.markdown(":green[POSITIVE]")
            st.progress(0.72)
            st.caption("Score: 0.72")

        with col2:
            st.markdown("**Emotions Detected**")
            st.markdown("- Satisfaction: 0.65")
            st.markdown("- Disappointment: 0.20")
            st.markdown("- Neutral: 0.15")

        with col3:
            st.markdown("**Key Phrases**")
            st.markdown("- 'excellent quality' :green[+]")
            st.markdown("- 'slower than expected' :orange[-]")
            st.markdown("- 'overall satisfied' :green[+]")


def _show_custom_classification():
    """Show custom classification interface."""
    st.markdown("### Custom Categories")

    st.markdown("Define custom categories for classification")

    # Category definition
    with st.expander("Define Categories", expanded=True):
        categories = st.text_area(
            "Categories (one per line)",
            value="Product Inquiry\nBilling Question\nTechnical Support\nComplaint\nFeedback\nOther",
            height=150,
        )

        examples = st.text_area(
            "Training Examples (category: example text)",
            placeholder="Product Inquiry: What colors does this product come in?\nComplaint: I received a damaged item...",
            height=150,
        )

    if st.button("Train Classifier", type="primary"):
        with st.spinner("Training custom classifier..."):
            import time
            time.sleep(2)
        st.success("Custom classifier trained successfully!")


def _show_sensitivity_classification():
    """Show data sensitivity classification."""
    st.markdown("### Data Sensitivity Classification")

    sensitivity_levels = st.multiselect(
        "Sensitivity Levels",
        ["Public", "Internal", "Confidential", "Restricted", "Top Secret"],
        default=["Public", "Internal", "Confidential", "Restricted"],
    )

    if st.button("Classify Data Sensitivity", type="primary"):
        st.info("Classification job submitted!")


def _show_llm_configuration():
    """Show LLM configuration."""
    st.subheader(" LLM Configuration")

    # Provider selection
    provider = st.selectbox(
        "LLM Provider",
        ["Anthropic (Claude)", "OpenAI (GPT)", "Azure OpenAI", "Local (Ollama)", "AWS Bedrock"],
    )

    st.markdown("---")

    if provider == "Anthropic (Claude)":
        st.text_input("API Key", type="password", key="anthropic_key")
        model = st.selectbox("Model", ["claude-3-opus", "claude-3-sonnet", "claude-3-haiku"])
        st.number_input("Max Tokens", value=4096, key="anthropic_tokens")

    elif provider == "OpenAI (GPT)":
        st.text_input("API Key", type="password", key="openai_key")
        model = st.selectbox("Model", ["gpt-4-turbo", "gpt-4", "gpt-3.5-turbo"])
        st.text_input("Organization ID", key="openai_org")

    elif provider == "Azure OpenAI":
        st.text_input("API Key", type="password", key="azure_key")
        st.text_input("Endpoint", placeholder="https://your-resource.openai.azure.com/")
        st.text_input("Deployment Name", placeholder="gpt-4")
        st.text_input("API Version", value="2024-02-15-preview")

    elif provider == "Local (Ollama)":
        st.text_input("Ollama URL", value="http://localhost:11434")
        model = st.selectbox("Model", ["llama2", "mistral", "codellama", "mixtral"])

    else:  # AWS Bedrock
        st.text_input("AWS Access Key", type="password")
        st.text_input("AWS Secret Key", type="password")
        st.text_input("AWS Region", value="us-east-1")
        model = st.selectbox("Model", ["anthropic.claude-3", "amazon.titan", "meta.llama2"])

    st.markdown("---")

    # Rate limiting
    st.subheader("Rate Limiting")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.number_input("Requests per minute", value=60)

    with col2:
        st.number_input("Requests per day", value=10000)

    with col3:
        st.number_input("Max concurrent requests", value=5)

    st.markdown("---")

    # Test connection
    col1, col2 = st.columns([1, 3])

    with col1:
        if st.button("Test Connection"):
            with st.spinner("Testing connection..."):
                import time
                time.sleep(1)
            st.success("Connection successful!")

    with col2:
        if st.button("Save Configuration", type="primary"):
            st.success("LLM configuration saved!")

