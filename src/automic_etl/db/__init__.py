"""Database module for Automic ETL."""

from automic_etl.db.engine import get_engine, get_session, init_db
from automic_etl.db.models import Base, UserModel, SessionModel, PipelineModel, PipelineRunModel
from automic_etl.db.auth_service import AuthService, get_auth_service, AuthenticationError

__all__ = [
    "get_engine",
    "get_session", 
    "init_db",
    "Base",
    "UserModel",
    "SessionModel",
    "PipelineModel",
    "PipelineRunModel",
    "AuthService",
    "get_auth_service",
    "AuthenticationError",
]
