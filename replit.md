# Automic ETL - Replit Setup

## Project Overview
**Automic ETL** is an AI-augmented ETL tool for building data lakehouses with medallion architecture (Bronze/Silver/Gold). It features LLM integration for intelligent data processing, supports multiple cloud storage providers, and includes both a Streamlit web UI and FastAPI backend.

## Recent Changes
- **2025-12-01**: Material Design 3 UI Implementation
  - **Design System**: Material 3 (Material You) color palette with semantic tokens
  - **Primary Colors**: Deep Indigo (#3F51B5) - professional, trustworthy for data tools
  - **Secondary Colors**: Teal (#009688) - complementary accent for highlights
  - **Status Colors**: Material standard - Error (#D32F2F), Warning (#ED6C02), Success (#2E7D32), Info (#0288D1)
  - **Surface System**: Material surface/container hierarchy for proper elevation
  - **Medallion Layers**: Bronze (#8D6E63), Silver (#78909C), Gold (#FFA000) with container variants
  - **Typography**: Inter font with Material type scale
  - **Components**: Material-styled buttons (pill shape), tabs, inputs, cards with elevation shadows
  - **Login Page**: Gradient indigo logo, Material button styling, elevated card design
  - **Home Dashboard**: Material activity cards with status containers, updated medallion layer cards
  - **Page Headers**: Consistent Material typography across all pages
  - **Theme File**: Full Material 3 token system in theme.py with CSS custom properties
- **2025-12-01**: Previous minimal redesign (superseded by Material Design)
- **2025-12-01**: Fixed page routing and navigation
  - Disabled Streamlit's automatic multi-page navigation (conflicts with custom navigation)
  - Added missing page routes for company_admin and superadmin in app.py
  - Pages now correctly render when accessed via custom navigation
- **2025-11-30**: Full database-backed functionality implemented
  - **Authentication System**:
    - AuthService with bcrypt password hashing
    - SessionModel for persistent database-backed sessions
    - Superadmin account auto-seeding from environment secrets
  - **Pipeline Management**:
    - PipelineService with full CRUD operations (create, list, update, delete)
    - Pipeline runs tracking with status, metrics, and error handling
    - User-owned pipelines with proper filtering
  - **Data Ingestion**:
    - File upload with Polars preview (CSV, Parquet, JSON)
    - DataService for persisting table metadata (row count, size, schema)
    - Medallion layer assignment (bronze, silver, gold)
  - **Monitoring Dashboard**:
    - Real-time statistics from database (pipelines, runs, tables)
    - Medallion layer breakdown with counts and sizes
    - Recent activity feed from actual pipeline runs
  - **Admin Dashboard**:
    - User management (view, approve, suspend)
    - Session management
    - Audit log viewer
- **2025-11-30**: UI/UX modernization completed
  - Redesigned theme system with modern professional color palette
  - Clean centered login card with hidden sidebar
  - Accessibility-compliant skip link for screen readers
  - Improved navigation with better contrast and hover states
- **2025-11-30**: Initial Replit setup completed
  - PostgreSQL database configured via DATABASE_URL
  - Streamlit UI workflow on port 5000
  - Autoscale deployment configured

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

### Database Layer
- **Location**: `src/automic_etl/db/`
- **Engine**: PostgreSQL via SQLAlchemy
- **Models** (`models.py`):
  - UserModel (authentication, roles)
  - SessionModel (persistent sessions)
  - PipelineModel (ETL pipeline definitions)
  - PipelineRunModel (execution history)
  - DataTableModel (ingested data metadata)
  - AuditLogModel (activity tracking)
- **Services**:
  - AuthService: User authentication, password hashing, session management
  - PipelineService: CRUD operations for pipelines and runs
  - DataService: Data table metadata tracking

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

### Required for Superadmin Access
- `SUPERADMIN_EMAIL`: The email address for the superadmin account
- `SUPERADMIN_PASSWORD`: The password for the superadmin account

The superadmin account is automatically created/updated when these secrets are configured. Changing the secrets will update the superadmin credentials on next application restart.

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
- PostgreSQL database is automatically initialized with tables on first startup
- Superadmin account is created/updated from SUPERADMIN_EMAIL and SUPERADMIN_PASSWORD secrets
- Sessions persist in the database and survive page refreshes
- All pipeline and data operations are backed by real database storage
- LLM features require API keys but the core ETL functionality works without them
- Backend API is available but not required for the Streamlit UI to function
- For production use, configure appropriate cloud storage credentials for actual data persistence
