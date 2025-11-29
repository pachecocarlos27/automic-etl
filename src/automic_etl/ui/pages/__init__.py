"""UI Pages for Automic ETL."""

from automic_etl.ui.pages.ingestion import show_ingestion_page
from automic_etl.ui.pages.pipeline_builder import show_pipeline_builder_page
from automic_etl.ui.pages.data_profiling import show_data_profiling_page
from automic_etl.ui.pages.query_studio import show_query_studio_page
from automic_etl.ui.pages.monitoring import show_monitoring_page
from automic_etl.ui.pages.settings import show_settings_page
from automic_etl.ui.pages.lineage import show_lineage_page

__all__ = [
    "show_ingestion_page",
    "show_pipeline_builder_page",
    "show_data_profiling_page",
    "show_query_studio_page",
    "show_monitoring_page",
    "show_settings_page",
    "show_lineage_page",
]
