---
description: Repository Information Overview
alwaysApply: true
---

# Automic ETL Information

## Summary

**Automic ETL** is an AI-Augmented ETL (Extract, Transform, Load) platform that builds cloud-native lakehouses using the medallion architecture (Bronze/Silver/Gold layers) on Apache Iceberg. The tool provides multi-cloud storage support (AWS S3, GCS, Azure Blob), intelligent data connectors for databases/files/APIs, and LLM-powered features for schema inference, entity extraction, and natural language query generation.

## Structure

The project is organized into modular components within `src/automic_etl/`:

- **`ui/`**: Streamlit-based web interface with pipeline builder and ingestion pages
- **`llm/`**: LLM integration modules (schema inference, entity extraction, query building, data classification)
- **`medallion/`**: Medallion architecture layers (Bronze/Silver/Gold) with SCD Type 2 support
- **`extraction/`**: Extraction modes (batch, incremental, watermark-based)
- **`connectors/`**: Database, file, and API connectors (PostgreSQL, MySQL, MongoDB, CSV, JSON, Parquet, etc.)
- **`storage/`**: Cloud storage integration with Iceberg table support
- **`cli.py`**: Command-line interface using Typer

Configuration templates are in `config/` and examples in `examples/`.

## Language & Runtime

**Language**: Python  
**Version**: 3.10+  
**Build System**: Hatchling  
**Package Manager**: pip with pyproject.toml

## Dependencies

**Core Data Processing**:
- `polars>=1.0.0` - High-performance DataFrame processing
- `pyarrow>=15.0.0` - Apache Arrow data format

**Apache Iceberg**: `pyiceberg>=0.7.0`

**Cloud Storage**:
- `boto3>=1.34.0` - AWS S3
- `google-cloud-storage>=2.14.0` - Google Cloud Storage
- `azure-storage-blob>=12.19.0` - Azure Blob Storage
- `azure-identity>=1.15.0` - Azure identity management

**Database Connectors**:
- `sqlalchemy>=2.0.0` - SQL toolkit
- `psycopg2-binary>=2.9.0` - PostgreSQL
- `pymysql>=1.1.0` - MySQL
- `pymongo>=4.6.0` - MongoDB
- `snowflake-connector-python>=3.6.0` - Snowflake
- `google-cloud-bigquery>=3.17.0` - BigQuery

**Unstructured Data**: `unstructured>=0.12.0`, `pdf2image>=1.17.0`, `pytesseract>=0.3.10`, `Pillow>=10.2.0`

**LLM Integration**: `litellm>=1.30.0`, `anthropic>=0.18.0`, `openai>=1.12.0`

**Web UI**: `streamlit>=1.31.0`, `plotly>=5.18.0`, `altair>=5.2.0`

**CLI & Configuration**: `typer>=0.9.0`, `rich>=13.7.0`, `pydantic>=2.6.0`, `pyyaml>=6.0.0`

**Development Dependencies**:
- `pytest>=8.0.0`, `pytest-asyncio>=0.23.0`, `pytest-cov>=4.1.0`
- `mypy>=1.8.0` - Type checking
- `ruff>=0.2.0` - Linting and formatting

## Build & Installation

```bash
# Install from source
git clone https://github.com/datantllc/automic-etl.git
cd automic-etl
pip install -e .

# Install with development dependencies
pip install -e ".[dev]"
```

## Main Entry Points

**CLI**: `automic` command (entry point: `automic_etl.cli:app`)
- `automic init` - Initialize lakehouse
- `automic ingest file/database` - Data ingestion
- `automic process` - Transform data through layers
- `automic llm` - LLM-powered operations (schema inference, entity extraction, NL queries)
- `automic ui` - Launch Streamlit web interface (default: http://localhost:8501)
- `automic status` - Show lakehouse status

**Configuration File**: `config/settings.yaml` - YAML-based configuration for lakehouse, storage, medallion layers, LLM provider, and extraction settings.

**Environment Variables**: `.env.example` provides templates for LLM keys (Anthropic/OpenAI), cloud credentials (AWS/GCP/Azure), database connections, and logging configuration.

## Testing

**Framework**: pytest with asyncio support  
**Test Location**: `tests/` directory (currently empty - no test files found)  
**Configuration**:
```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"
addopts = "-v --cov=automic_etl --cov-report=term-missing"
```

**Run Tests**:
```bash
pytest
# With coverage
pytest --cov=automic_etl --cov-report=html
```

## Code Quality

**Linting/Formatting**: Ruff (with isort, pyupgrade, flake8-bugbear, pycodestyle rules)  
**Type Checking**: mypy (strict mode, Python 3.10 target)

Run code quality checks:
```bash
ruff check src tests
mypy src
ruff format src tests
```

## Project Metadata

- **Version**: 0.1.0 (Beta)
- **License**: MIT
- **Repository**: https://github.com/datantllc/automic-etl
- **Keywords**: etl, lakehouse, iceberg, medallion, polars, llm, data-engineering
