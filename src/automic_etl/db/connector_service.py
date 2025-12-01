"""Database-backed connector configuration service."""

from __future__ import annotations

from datetime import datetime
from typing import Optional, List
import uuid

from automic_etl.db.engine import get_session
from automic_etl.db.models import ConnectorConfigModel


class ConnectorService:
    """Service for managing connector configurations in the database."""

    def create_connector(
        self,
        name: str,
        connector_type: str,
        category: str,
        config: Optional[dict] = None,
        credentials: Optional[dict] = None,
        created_by: Optional[str] = None,
    ) -> ConnectorConfigModel:
        """Create a new connector configuration."""
        with get_session() as session:
            connector = ConnectorConfigModel(
                id=str(uuid.uuid4()),
                name=name,
                connector_type=connector_type,
                category=category,
                config=config or {},
                credentials=credentials or {},
                status="inactive",
                created_by=created_by,
            )
            session.add(connector)
            session.flush()
            session.expunge(connector)
            return connector

    def get_connector(self, connector_id: str) -> Optional[ConnectorConfigModel]:
        """Get a connector by ID."""
        with get_session() as session:
            connector = session.query(ConnectorConfigModel).filter(
                ConnectorConfigModel.id == connector_id
            ).first()
            if connector:
                session.expunge(connector)
            return connector

    def get_connector_by_name(self, name: str) -> Optional[ConnectorConfigModel]:
        """Get a connector by name."""
        with get_session() as session:
            connector = session.query(ConnectorConfigModel).filter(
                ConnectorConfigModel.name == name
            ).first()
            if connector:
                session.expunge(connector)
            return connector

    def list_connectors(
        self,
        category: Optional[str] = None,
        connector_type: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[ConnectorConfigModel]:
        """List connectors with optional filters."""
        with get_session() as session:
            query = session.query(ConnectorConfigModel)

            if category:
                query = query.filter(ConnectorConfigModel.category == category)
            if connector_type:
                query = query.filter(ConnectorConfigModel.connector_type == connector_type)
            if status:
                query = query.filter(ConnectorConfigModel.status == status)

            connectors = query.order_by(ConnectorConfigModel.name.asc()).all()
            for c in connectors:
                session.expunge(c)
            return connectors

    def update_connector(
        self,
        connector_id: str,
        name: Optional[str] = None,
        config: Optional[dict] = None,
        credentials: Optional[dict] = None,
        status: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> Optional[ConnectorConfigModel]:
        """Update a connector configuration."""
        with get_session() as session:
            connector = session.query(ConnectorConfigModel).filter(
                ConnectorConfigModel.id == connector_id
            ).first()

            if not connector:
                return None

            if name is not None:
                connector.name = name
            if config is not None:
                connector.config = config
            if credentials is not None:
                connector.credentials = credentials
            if status is not None:
                connector.status = status
            if metadata is not None:
                connector.metadata_ = metadata

            connector.updated_at = datetime.utcnow()
            session.flush()
            session.expunge(connector)
            return connector

    def delete_connector(self, connector_id: str) -> bool:
        """Delete a connector."""
        with get_session() as session:
            connector = session.query(ConnectorConfigModel).filter(
                ConnectorConfigModel.id == connector_id
            ).first()

            if not connector:
                return False

            session.delete(connector)
            return True

    def update_test_status(
        self,
        connector_id: str,
        success: bool,
        error_message: Optional[str] = None,
    ) -> None:
        """Update connector test status."""
        with get_session() as session:
            connector = session.query(ConnectorConfigModel).filter(
                ConnectorConfigModel.id == connector_id
            ).first()

            if connector:
                connector.last_tested_at = datetime.utcnow()
                connector.last_test_status = "success" if success else "failed"
                if success:
                    connector.status = "active"
                else:
                    connector.status = "error"
                    if error_message:
                        connector.metadata_ = {
                            **connector.metadata_,
                            "last_error": error_message,
                        }

    def get_active_connectors_by_type(self, connector_type: str) -> List[ConnectorConfigModel]:
        """Get all active connectors of a specific type."""
        with get_session() as session:
            connectors = session.query(ConnectorConfigModel).filter(
                ConnectorConfigModel.connector_type == connector_type,
                ConnectorConfigModel.status == "active",
            ).all()

            for c in connectors:
                session.expunge(c)
            return connectors

    def get_connector_summary(self) -> dict:
        """Get summary statistics for connectors."""
        with get_session() as session:
            total = session.query(ConnectorConfigModel).count()
            active = session.query(ConnectorConfigModel).filter(
                ConnectorConfigModel.status == "active"
            ).count()
            error = session.query(ConnectorConfigModel).filter(
                ConnectorConfigModel.status == "error"
            ).count()

            # Count by category
            by_category = {}
            for category in ["database", "api", "storage", "streaming"]:
                by_category[category] = session.query(ConnectorConfigModel).filter(
                    ConnectorConfigModel.category == category
                ).count()

            return {
                "total": total,
                "active": active,
                "inactive": total - active - error,
                "error": error,
                "by_category": by_category,
            }


# Singleton instance
_connector_service: Optional[ConnectorService] = None


def get_connector_service() -> ConnectorService:
    """Get the connector service singleton."""
    global _connector_service
    if _connector_service is None:
        _connector_service = ConnectorService()
    return _connector_service
