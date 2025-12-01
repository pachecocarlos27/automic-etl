"""UI Pages for Automic ETL."""

from automic_etl.ui.pages.ingestion import show_ingestion_page
from automic_etl.ui.pages.pipeline_builder import show_pipeline_builder_page
from automic_etl.ui.pages.data_profiling import show_data_profiling_page
from automic_etl.ui.pages.query_studio import show_query_studio_page
from automic_etl.ui.pages.monitoring import show_monitoring_page
from automic_etl.ui.pages.settings import show_settings_page
from automic_etl.ui.pages.lineage import show_lineage_page
from automic_etl.ui.pages.company_admin import show_company_admin
from automic_etl.ui.pages.superadmin import show_superadmin
# New pages
from automic_etl.ui.pages.jobs import show_jobs_page
from automic_etl.ui.pages.validation import show_validation_page
from automic_etl.ui.pages.alerts import show_alerts_page
from automic_etl.ui.pages.integrations import show_integrations_page
from automic_etl.ui.pages.ai_services import show_ai_services_page
from automic_etl.ui.pages.connectors_mgmt import show_connectors_management_page

__all__ = [
    "show_ingestion_page",
    "show_pipeline_builder_page",
    "show_data_profiling_page",
    "show_query_studio_page",
    "show_monitoring_page",
    "show_settings_page",
    "show_lineage_page",
    "show_company_admin",
    "show_superadmin",
    # New pages
    "show_jobs_page",
    "show_validation_page",
    "show_alerts_page",
    "show_integrations_page",
    "show_ai_services_page",
    "show_connectors_management_page",
]
