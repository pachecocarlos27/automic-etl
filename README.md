# Automic ETL

**AI-Augmented ETL Tool for Lakehouse Architecture**

Automic ETL is a comprehensive data engineering platform that builds lakehouses using the medallion architecture (Bronze/Silver/Gold) on cloud storage (AWS S3, GCS, Azure Blob) with Apache Iceberg and Delta Lake tables. It features LLM integration for intelligent processing of unstructured, semi-structured, and structured data.

## Features

### Core Capabilities

| Feature | Description |
|---------|-------------|
| **Medallion Architecture** | Bronze/Silver/Gold data layers with automatic transformations |
| **Multi-Cloud Storage** | AWS S3, Google Cloud Storage, Azure Blob Storage |
| **Table Formats** | Apache Iceberg and Delta Lake support |
| **LLM Augmentation** | Schema inference, entity extraction, NL-to-SQL |
| **Data Quality** | Validation rules, profiling, anomaly detection |
| **Data Lineage** | Full lineage tracking with impact analysis |
| **REST API** | Complete API for programmatic access |
| **Web UI** | Streamlit-based dashboard with theming |

### Data Connectors

#### Databases
- PostgreSQL, MySQL, MongoDB
- Snowflake, BigQuery, Redshift

#### Streaming
- Apache Kafka (with Schema Registry, Avro support)
- AWS Kinesis (with Enhanced Fan-Out)
- Google Pub/Sub

#### APIs
- REST API (generic)
- Salesforce, HubSpot, Stripe

#### Files & Storage
- CSV, JSON, Parquet, Excel
- AWS S3, Google Cloud Storage, Azure Blob
- PDF and unstructured documents

### Open Source Integrations

| Tool | Integration |
|------|-------------|
| **Apache Spark** | Distributed processing with Delta Lake/Iceberg |
| **dbt** | SQL transformations, model management |
| **Great Expectations** | Data validation and profiling |
| **Apache Airflow** | Workflow orchestration via REST API |
| **MLflow** | Experiment tracking, model registry |
| **OpenMetadata** | Data catalog and governance |

## Installation

```bash
pip install automic-etl
```

Or install from source:

```bash
git clone https://github.com/pachecocarlos27/automic-etl.git
cd automic-etl
pip install -e .
```

### Optional Dependencies

```bash
# Streaming connectors
pip install automic-etl[streaming]  # kafka, kinesis, pubsub

# Open source integrations
pip install automic-etl[spark]      # Apache Spark
pip install automic-etl[dbt]        # dbt
pip install automic-etl[ge]         # Great Expectations
pip install automic-etl[mlflow]     # MLflow

# All integrations
pip install automic-etl[all]
```

## Quick Start

### Initialize Lakehouse

```python
from automic_etl import Lakehouse

# Initialize with default settings
lakehouse = Lakehouse()
lakehouse.initialize()
```

### Basic Pipeline

```python
import polars as pl
from automic_etl import Lakehouse
from automic_etl.medallion.gold import AggregationType

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

### Streaming Ingestion (Kafka)

```python
from automic_etl.connectors import KafkaConnector, KafkaConfig

config = KafkaConfig(
    bootstrap_servers="localhost:9092",
    topic="events",
    group_id="automic-consumer",
    value_deserializer="json"
)

with KafkaConnector(config) as kafka:
    for batch in kafka.extract(batch_size=100):
        lakehouse.ingest(
            table_name="events",
            data=batch,
            source="kafka_stream",
        )
```

### LLM-Powered Queries

```python
from automic_etl.llm import QueryBuilder

query_builder = QueryBuilder(settings)
query_builder.register_dataframe("customers", df)

result = query_builder.build_query(
    "Show me all VIP customers with orders over $10,000"
)
print(result.sql)
# SELECT * FROM customers WHERE tier = 'VIP' AND total_orders > 10000
```

### Using Integrations

```python
# Apache Spark
from automic_etl import SparkIntegration, SparkConfig

spark = SparkIntegration(SparkConfig(
    app_name="my-etl",
    enable_delta=True
))
spark.start()
df = spark.read_delta("s3://bucket/bronze/sales")
spark.bronze_to_silver(
    bronze_path="s3://bucket/bronze/sales",
    silver_path="s3://bucket/silver/sales_clean",
    dedupe_columns=["order_id"]
)

# dbt
from automic_etl import DbtIntegration, DbtConfig

dbt = DbtIntegration(DbtConfig(
    project_dir="/path/to/dbt/project",
    target="prod"
))
dbt.run(models=["staging.*", "marts.orders"])

# Great Expectations
from automic_etl import GreatExpectationsIntegration, GEConfig

ge = GreatExpectationsIntegration(GEConfig(
    project_dir="/path/to/ge/project"
))
results = ge.validate(data=df, suite_name="sales_validation")

# MLflow
from automic_etl import MLflowIntegration, MLflowConfig

mlflow = MLflowIntegration(MLflowConfig(
    tracking_uri="http://mlflow:5000",
    experiment_name="sales_forecasting"
))
mlflow.log_pipeline_run(
    pipeline_name="daily_etl",
    config={"source": "postgres"},
    metrics={"rows_processed": 10000}
)
```

## REST API

Start the API server:

```bash
uvicorn automic_etl.api:app --host 0.0.0.0 --port 8000
```

### API Endpoints

| Endpoint | Methods | Description |
|----------|---------|-------------|
| `/api/v1/health` | GET | Health check and metrics |
| `/api/v1/pipelines` | GET, POST | List and create pipelines |
| `/api/v1/pipelines/{id}` | GET, PUT, DELETE | Manage pipeline |
| `/api/v1/pipelines/{id}/run` | POST | Execute pipeline |
| `/api/v1/tables` | GET, POST | List and create tables |
| `/api/v1/tables/{id}/data` | POST | Query table data |
| `/api/v1/queries/execute` | POST | Execute SQL or NL query |
| `/api/v1/connectors` | GET, POST | Manage connectors |
| `/api/v1/connectors/{id}/test` | POST | Test connection |
| `/api/v1/lineage/graph` | GET | Get lineage graph |
| `/api/v1/lineage/impact/{table}` | GET | Impact analysis |
| `/api/v1/jobs` | GET, POST | Manage scheduled jobs |

### Example: Execute Query via API

```bash
curl -X POST http://localhost:8000/api/v1/queries/execute \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Show me top 10 customers by revenue",
    "query_type": "natural_language"
  }'
```

## Web UI

Launch the Streamlit dashboard:

```bash
streamlit run -m automic_etl.ui.app
```

Features:
- Home dashboard with medallion architecture overview
- Data ingestion wizard (files, databases, APIs, streaming)
- Pipeline builder with visual stage management
- Query Studio with natural language support
- Data profiling and quality metrics
- Data lineage visualization
- Monitoring dashboard
- Settings and connector management

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

# Integrations
integrations:
  spark:
    master: "spark://localhost:7077"
    enable_delta: true

  airflow:
    base_url: "http://airflow:8080"
    username: "admin"

  mlflow:
    tracking_uri: "http://mlflow:5000"
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Automic ETL Platform                           │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │   Web UI    │  │  REST API   │  │     CLI     │  │   Python    │    │
│  │ (Streamlit) │  │  (FastAPI)  │  │   (Typer)   │  │     SDK     │    │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘    │
├─────────┴────────────────┴────────────────┴────────────────┴───────────┤
│                           Core Engine                                   │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐           │
│  │  Pipeline │  │  Lineage  │  │ Scheduler │  │    LLM    │           │
│  │  Manager  │  │  Tracker  │  │           │  │  Engine   │           │
│  └───────────┘  └───────────┘  └───────────┘  └───────────┘           │
├─────────────────────────────────────────────────────────────────────────┤
│                       Medallion Architecture                            │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐        │
│  │     BRONZE      │  │     SILVER      │  │      GOLD       │        │
│  │   (Raw Data)    │──│   (Cleaned)     │──│  (Aggregated)   │        │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘        │
├─────────────────────────────────────────────────────────────────────────┤
│                          Data Connectors                                │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐          │
│  │Databases│ │  APIs   │ │Streaming│ │  Files  │ │  Cloud  │          │
│  │ PG/MySQL│ │SF/HubSp│ │Kafka/Kin│ │CSV/JSON │ │ S3/GCS  │          │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘          │
├─────────────────────────────────────────────────────────────────────────┤
│                      Open Source Integrations                           │
│  ┌───────┐  ┌─────┐  ┌────┐  ┌─────────┐  ┌──────┐  ┌────────────┐    │
│  │ Spark │  │ dbt │  │ GE │  │ Airflow │  │MLflow│  │OpenMetadata│    │
│  └───────┘  └─────┘  └────┘  └─────────┘  └──────┘  └────────────┘    │
├─────────────────────────────────────────────────────────────────────────┤
│                    Storage (Delta Lake / Iceberg)                       │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────────┐          │
│  │    AWS S3     │  │      GCS      │  │   Azure Blob      │          │
│  └───────────────┘  └───────────────┘  └───────────────────┘          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Technology Stack

| Category | Technologies |
|----------|-------------|
| **Language** | Python 3.10+ |
| **Data Processing** | Polars, PyArrow, PySpark |
| **Table Formats** | Delta Lake, Apache Iceberg |
| **Cloud SDKs** | boto3, google-cloud-storage, azure-storage-blob |
| **LLM Providers** | Anthropic, OpenAI, LiteLLM |
| **Streaming** | confluent-kafka, boto3 (Kinesis), google-cloud-pubsub |
| **Web UI** | Streamlit |
| **REST API** | FastAPI, Pydantic |
| **CLI** | Typer, Rich |
| **Auth** | JWT, OAuth2, RBAC |

## Project Structure

```
automic-etl/
├── src/automic_etl/
│   ├── api/                 # REST API (FastAPI)
│   │   ├── routes/          # API endpoints
│   │   └── models.py        # Pydantic schemas
│   ├── auth/                # Authentication & RBAC
│   ├── connectors/          # Data source connectors
│   │   ├── databases/       # Database connectors
│   │   ├── apis/            # API connectors
│   │   └── streaming/       # Kafka, Kinesis, Pub/Sub
│   ├── core/                # Core pipeline engine
│   ├── extraction/          # Batch/incremental extraction
│   ├── integrations/        # Open source integrations
│   │   ├── spark.py         # Apache Spark
│   │   ├── dbt.py           # dbt
│   │   ├── airflow.py       # Apache Airflow
│   │   ├── mlflow.py        # MLflow
│   │   ├── great_expectations.py
│   │   └── openmetadata.py
│   ├── lineage/             # Data lineage tracking
│   ├── llm/                 # LLM integration
│   ├── medallion/           # Bronze/Silver/Gold layers
│   ├── notifications/       # Alerts (Email, Slack, etc.)
│   ├── orchestration/       # Job scheduling
│   ├── storage/             # Cloud storage & table formats
│   ├── ui/                  # Streamlit Web UI
│   │   ├── pages/           # UI pages
│   │   ├── components.py    # Reusable components
│   │   └── theme.py         # Theming system
│   └── validation/          # Data quality validation
├── config/                  # Configuration files
├── examples/                # Usage examples
└── tests/                   # Test suite
```

## Examples

See the `examples/` directory for complete examples:

- `basic_pipeline.py` - Simple ETL pipeline
- `streaming_pipeline.py` - Kafka/Kinesis streaming
- `spark_pipeline.py` - Distributed processing with Spark
- `dbt_pipeline.py` - SQL transformations with dbt
- `llm_augmented_pipeline.py` - LLM-powered data processing
- `scd2_pipeline.py` - SCD Type 2 dimension tracking

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions welcome! Please read our contributing guidelines.

## Support

- GitHub Issues: [Report bugs or request features](https://github.com/pachecocarlos27/automic-etl/issues)
- Documentation: [Full docs](https://github.com/pachecocarlos27/automic-etl#readme)
