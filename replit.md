# Automic ETL - Replit Setup

## Project Overview
**Automic ETL** is an AI-augmented ETL tool for building data lakehouses with medallion architecture (Bronze/Silver/Gold). It features LLM integration for intelligent data processing, supports multiple cloud storage providers, and includes both a Streamlit web UI and FastAPI backend.

## Recent Changes
- **2025-11-30**: UI/UX modernization completed
  - Redesigned theme system with modern professional color palette:
    - Primary blue (#0066FF) with teal accent (#00D4AA)
    - Clean Inter font typography
    - Dark sidebar gradient (#0F172A to #1E293B)
  - Updated login page with gradient logo icon and modern form styling
  - Redesigned dashboard home with colorful metric cards and responsive grid layout
  - Improved medallion architecture cards with colored top borders
  - Enhanced sidebar navigation with better visual hierarchy
- **2025-11-30**: Initial Replit setup completed
  - Installed Python 3.11 and all dependencies from pyproject.toml
  - Configured Streamlit UI to run on port 5000 with 0.0.0.0 host binding
  - Created workflow for Streamlit UI (frontend only)
  - Configured deployment settings for autoscale deployment
  - Updated .gitignore for Python and Replit-specific files

## Project Architecture

### Frontend (Streamlit UI)
- **Location**: `src/automic_etl/ui/`
- **Main Entry**: `src/automic_etl/ui/app.py`
- **Port**: 5000 (webview)
- **Features**:
  - Home dashboard with medallion architecture overview
  - Data ingestion wizard
  - Pipeline builder
  - Query Studio (LLM-powered SQL interface)
  - Data profiling and quality metrics
  - Data lineage visualization
  - Monitoring dashboard
  - Settings and connector management

### Backend (FastAPI - Not Currently Running)
- **Location**: `src/automic_etl/api/`
- **Main Entry**: `src/automic_etl/api/main.py`
- **Note**: Backend API is available but not currently configured as a workflow. The Streamlit UI operates independently.

### Core Components
- **Connectors**: Database, API, streaming, and file connectors (`src/automic_etl/connectors/`)
- **Medallion Architecture**: Bronze/Silver/Gold data layers (`src/automic_etl/medallion/`)
- **LLM Integration**: AI-powered features (`src/automic_etl/llm/`)
- **Data Lineage**: Tracking and visualization (`src/automic_etl/lineage/`)
- **Orchestration**: Job scheduling and workflow management (`src/automic_etl/orchestration/`)

## Configuration

### Streamlit Configuration
- **File**: `.streamlit/config.toml`
- **Settings**: Configured for Replit environment with port 5000, host 0.0.0.0, CORS disabled

### Application Settings
- **File**: `config/settings.yaml`
- **Default Storage**: SQLite catalog for local development
- **LLM Provider**: Anthropic (requires API key via secrets)
- **Default Mode**: Development mode

## Environment Variables & Secrets

The application requires various API keys and credentials for full functionality. These should be added via Replit Secrets:

### Required for LLM Features
- `ANTHROPIC_API_KEY` or `OPENAI_API_KEY`: For AI-powered features

### Optional Cloud Storage
- AWS: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_LAKEHOUSE_BUCKET`
- GCP: `GCP_PROJECT_ID`, `GCP_LAKEHOUSE_BUCKET`, `GOOGLE_APPLICATION_CREDENTIALS`
- Azure: `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_CONNECTION_STRING`, `AZURE_LAKEHOUSE_CONTAINER`

### Optional Database Connectors
- PostgreSQL, MySQL, MongoDB, Snowflake, BigQuery credentials
- See `.env.example` for complete list

## Running the Application

### Development
The Streamlit UI workflow is configured to auto-start and runs on port 5000. Access the application via the Webview tab.

### Deployment
Configured for autoscale deployment:
- Runs Streamlit on port 5000
- Auto-scales based on traffic
- No build step required (pure Python)

## Technology Stack
- **Language**: Python 3.11
- **Data Processing**: Polars, PyArrow
- **Table Formats**: Delta Lake, Apache Iceberg
- **LLM**: Anthropic Claude, OpenAI
- **Web UI**: Streamlit
- **API**: FastAPI (available but not currently active)
- **Cloud SDKs**: boto3, google-cloud-storage, azure-storage-blob

## User Preferences
*No specific user preferences documented yet*

## Notes
- The application can run in development mode without cloud credentials by using local SQLite catalog
- LLM features require API keys but the core ETL functionality works without them
- Backend API is available but not required for the Streamlit UI to function
- For production use, configure appropriate cloud storage and database credentials
