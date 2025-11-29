# Automic ETL

**AI-Augmented ETL Tool for Lakehouse Architecture**

Automic ETL is a comprehensive data engineering platform that builds lakehouses using the medallion architecture (Bronze/Silver/Gold) on cloud storage (AWS S3, GCS, Azure Blob) with Apache Iceberg tables. It features LLM integration for intelligent processing of unstructured, semi-structured, and structured data.

## Features

### Multi-Cloud Storage
- **AWS S3** with IAM authentication
- **Google Cloud Storage** with service account
- **Azure Blob Storage / ADLS Gen2** with SAS/managed identity
- Unified interface for all providers

### Medallion Architecture
- **Bronze Layer**: Raw data ingestion, preserves original format
- **Silver Layer**: Cleaned, validated, deduplicated data
- **Gold Layer**: Business-level aggregations, ML-ready datasets

### Apache Iceberg Integration
- ACID transactions
- Schema evolution
- Time travel queries
- Partition evolution
- Efficient incremental processing

### Data Connectors
- **Databases**: PostgreSQL, MySQL, MongoDB, Snowflake, BigQuery
- **Files**: CSV, JSON, Parquet, Excel, XML
- **APIs**: REST, GraphQL, Salesforce
- **Unstructured**: PDF, Images, Documents, Audio, Video
- **Streaming**: Kafka, Kinesis

### LLM Augmentation
- Schema inference from unstructured data
- Entity extraction from documents
- Data classification and PII detection
- Natural language to SQL queries
- Anomaly detection and data quality assessment

### Extraction Modes
- **Batch**: Full table/file extraction with parallelization
- **Incremental**: Watermark-based change data capture
- **SCD Type 2**: Slowly Changing Dimension with full history

## Installation

```bash
pip install automic-etl
```

Or install from source:

```bash
git clone https://github.com/datantllc/automic-etl.git
cd automic-etl
pip install -e .
```

## Quick Start

### Initialize Lakehouse

```python
from automic_etl import Settings
from automic_etl.medallion import Lakehouse

# Initialize with default settings
lakehouse = Lakehouse()
lakehouse.initialize()
```

### Basic Pipeline

```python
import polars as pl
from automic_etl.medallion import Lakehouse

# Create lakehouse
lakehouse = Lakehouse()

# Ingest data to Bronze
df = pl.read_csv("sales_data.csv")
lakehouse.ingest(
    table_name="sales",
    data=df,
    source="csv_import",
)

# Process to Silver
lakehouse.process_to_silver(
    bronze_table="sales",
    silver_table="sales_clean",
    dedup_columns=["order_id"],
)

# Aggregate to Gold
from automic_etl.medallion.gold import AggregationType

lakehouse.aggregate_to_gold(
    silver_table="sales_clean",
    gold_table="daily_sales",
    group_by=["date", "region"],
    aggregations={
        "total_revenue": [("amount", AggregationType.SUM)],
        "order_count": [("order_id", AggregationType.COUNT)],
    },
)
```

### Database Extraction

```python
from automic_etl.connectors import get_connector
from automic_etl.extraction import IncrementalExtractor

# Connect to PostgreSQL
connector = get_connector(
    "postgresql",
    host="localhost",
    database="mydb",
    user="user",
    password="password",
)

# Incremental extraction
with connector:
    extractor = IncrementalExtractor(settings)
    result = extractor.extract(
        connector=connector,
        source_name="postgres_orders",
        watermark_column="updated_at",
        table="orders",
    )
    print(f"Extracted {result.new_rows} new rows")
```

### LLM Augmentation

```python
from automic_etl.llm import EntityExtractor, QueryBuilder

# Extract entities from text
extractor = EntityExtractor(settings)
entities = extractor.extract(
    text="Meeting with John Smith (john@acme.com) on Jan 15, 2024 about $50K deal.",
    entity_types=["PERSON", "EMAIL", "DATE", "MONEY"],
)

# Natural language to SQL
query_builder = QueryBuilder(settings)
query_builder.register_dataframe("customers", df)

result = query_builder.build_query(
    "Show me all VIP customers with orders over $10,000"
)
print(result.sql)
```

### SCD Type 2

```python
from automic_etl.medallion import SCDType2Manager

scd2 = SCDType2Manager(settings)

# Apply SCD2 changes
result = scd2.apply_scd2(
    source_df=updated_customers,
    table_name="dim_customers",
    business_keys=["customer_id"],
)

# Query point-in-time
historical = scd2.get_record_at_time(
    table_name="dim_customers",
    business_key_values={"customer_id": "C001"},
    as_of=datetime(2024, 1, 15),
)
```

## CLI Usage

```bash
# Initialize lakehouse
automic init --provider aws

# Ingest file
automic ingest file data.csv --table sales --source csv_import

# Process to silver
automic process bronze-to-silver sales --dedup order_id

# Natural language query
automic llm nl-query "What are total sales by region?" --execute

# Check status
automic status
```

## Configuration

Create a `config/settings.yaml` file:

```yaml
lakehouse:
  name: "my_lakehouse"

storage:
  provider: "aws"
  aws:
    bucket: "my-lakehouse-bucket"
    region: "us-east-1"

iceberg:
  catalog:
    type: "glue"
    name: "lakehouse_catalog"

medallion:
  bronze:
    path: "bronze/"
    retention_days: 90
  silver:
    path: "silver/"
    retention_days: 365
  gold:
    path: "gold/"

llm:
  provider: "anthropic"
  model: "claude-sonnet-4-20250514"
  api_key: "${ANTHROPIC_API_KEY}"

extraction:
  default_mode: "incremental"
  batch:
    size: 100000
```

## Environment Variables

```bash
# LLM
export ANTHROPIC_API_KEY=your_key

# AWS
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_REGION=us-east-1

# GCP
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json

# Azure
export AZURE_STORAGE_CONNECTION_STRING=your_connection_string
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Automic ETL Tool                         │
├─────────────────────────────────────────────────────────────┤
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐ │
│  │  Ingest   │  │  Process  │  │  Storage  │  │    LLM    │ │
│  │  Layer    │──│  Layer    │──│  Layer    │──│  Engine   │ │
│  └───────────┘  └───────────┘  └───────────┘  └───────────┘ │
├─────────────────────────────────────────────────────────────┤
│                  Medallion Architecture                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   BRONZE    │  │   SILVER    │  │    GOLD     │         │
│  │  (Raw Data) │──│  (Cleaned)  │──│ (Analytics) │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
├─────────────────────────────────────────────────────────────┤
│                   Apache Iceberg Tables                      │
├─────────────────────────────────────────────────────────────┤
│  ┌───────────┐  ┌───────────┐  ┌─────────────────┐         │
│  │  AWS S3   │  │   GCS     │  │  Azure Blob     │         │
│  └───────────┘  └───────────┘  └─────────────────┘         │
└─────────────────────────────────────────────────────────────┘
```

## Examples

See the `examples/` directory for complete examples:

- `basic_pipeline.py` - Simple ETL pipeline
- `unstructured_pipeline.py` - Processing PDFs and documents
- `llm_augmented_pipeline.py` - LLM-powered data processing
- `scd2_pipeline.py` - SCD Type 2 dimension tracking

## Technology Stack

- **Python**: 3.10+
- **Data Processing**: Polars, PyArrow
- **Iceberg**: PyIceberg
- **Cloud SDKs**: boto3, google-cloud-storage, azure-storage-blob
- **LLM**: Anthropic, OpenAI, LiteLLM
- **Unstructured**: unstructured, pypdf
- **CLI**: Typer, Rich

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions welcome! Please read our contributing guidelines.

## Support

- GitHub Issues: [Report bugs or request features](https://github.com/datantllc/automic-etl/issues)
- Documentation: [Full docs](https://github.com/datantllc/automic-etl#readme)
