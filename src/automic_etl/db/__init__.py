"""Database module for Automic ETL."""

from automic_etl.db.engine import get_engine, get_session, init_db
from automic_etl.db.models import (
    Base,
    UserModel,
    SessionModel,
    PipelineModel,
    PipelineRunModel,
    AuditLogModel,
    DataTableModel,
    JobScheduleModel,
    JobRunModel,
    ValidationRuleModel,
    ValidationResultModel,
    ConnectorConfigModel,
    NotificationChannelModel,
    AlertRuleModel,
    AlertHistoryModel,
)
from automic_etl.db.auth_service import AuthService, get_auth_service, AuthenticationError
from automic_etl.db.data_service import DataService, get_data_service
from automic_etl.db.pipeline_service import PipelineService, get_pipeline_service
from automic_etl.db.job_service import JobService, get_job_service
from automic_etl.db.validation_service import ValidationService, get_validation_service
from automic_etl.db.connector_service import ConnectorService, get_connector_service
from automic_etl.db.alert_service import AlertService, get_alert_service
from automic_etl.db.table_service import TableService, get_table_service

__all__ = [
    # Engine
    "get_engine",
    "get_session",
    "init_db",
    # Base models
    "Base",
    "UserModel",
    "SessionModel",
    "PipelineModel",
    "PipelineRunModel",
    "AuditLogModel",
    "DataTableModel",
    # Job models
    "JobScheduleModel",
    "JobRunModel",
    # Validation models
    "ValidationRuleModel",
    "ValidationResultModel",
    # Connector models
    "ConnectorConfigModel",
    # Notification/Alert models
    "NotificationChannelModel",
    "AlertRuleModel",
    "AlertHistoryModel",
    # Services
    "AuthService",
    "get_auth_service",
    "AuthenticationError",
    "DataService",
    "get_data_service",
    "PipelineService",
    "get_pipeline_service",
    "JobService",
    "get_job_service",
    "ValidationService",
    "get_validation_service",
    "ConnectorService",
    "get_connector_service",
    "AlertService",
    "get_alert_service",
    "TableService",
    "get_table_service",
]
