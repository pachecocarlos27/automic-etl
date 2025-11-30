"""Database-backed data service for managing data tables."""

from __future__ import annotations

from datetime import datetime
from typing import Optional, List
import uuid

from automic_etl.db.engine import get_session
from automic_etl.db.models import DataTableModel


class DataService:
    """Service for managing data tables in the database."""

    def create_table(
        self,
        name: str,
        layer: str,
        schema_definition: Optional[dict] = None,
        row_count: int = 0,
        size_bytes: int = 0,
        source_pipeline_id: Optional[str] = None,
    ) -> DataTableModel:
        """Register a new data table."""
        with get_session() as session:
            table = DataTableModel(
                id=str(uuid.uuid4()),
                name=name,
                layer=layer,
                schema_definition=schema_definition or {},
                row_count=row_count,
                size_bytes=size_bytes,
                source_pipeline_id=source_pipeline_id,
            )
            session.add(table)
            session.flush()
            session.expunge(table)
            return table

    def get_table(self, table_id: str) -> Optional[DataTableModel]:
        """Get a data table by ID."""
        with get_session() as session:
            table = session.query(DataTableModel).filter(
                DataTableModel.id == table_id
            ).first()
            if table:
                session.expunge(table)
            return table

    def get_table_by_name(self, name: str, layer: str) -> Optional[DataTableModel]:
        """Get a data table by name and layer."""
        with get_session() as session:
            table = session.query(DataTableModel).filter(
                DataTableModel.name == name,
                DataTableModel.layer == layer
            ).first()
            if table:
                session.expunge(table)
            return table

    def list_tables(
        self,
        layer: Optional[str] = None,
    ) -> List[DataTableModel]:
        """List data tables with optional layer filter."""
        with get_session() as session:
            query = session.query(DataTableModel)

            if layer:
                query = query.filter(DataTableModel.layer == layer)

            tables = query.order_by(DataTableModel.updated_at.desc()).all()
            for t in tables:
                session.expunge(t)
            return tables

    def update_table(
        self,
        table_id: str,
        row_count: Optional[int] = None,
        size_bytes: Optional[int] = None,
        schema_definition: Optional[dict] = None,
        quality_score: Optional[float] = None,
    ) -> Optional[DataTableModel]:
        """Update a data table."""
        with get_session() as session:
            table = session.query(DataTableModel).filter(
                DataTableModel.id == table_id
            ).first()

            if not table:
                return None

            if row_count is not None:
                table.row_count = row_count
            if size_bytes is not None:
                table.size_bytes = size_bytes
            if schema_definition is not None:
                table.schema_definition = schema_definition
            if quality_score is not None:
                table.quality_score = quality_score

            table.updated_at = datetime.utcnow()
            session.flush()
            session.expunge(table)
            return table

    def delete_table(self, table_id: str) -> bool:
        """Delete a data table."""
        with get_session() as session:
            table = session.query(DataTableModel).filter(
                DataTableModel.id == table_id
            ).first()

            if not table:
                return False

            session.delete(table)
            return True


def get_data_service() -> DataService:
    """Get the data service instance."""
    return DataService()
