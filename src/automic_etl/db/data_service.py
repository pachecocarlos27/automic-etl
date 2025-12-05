"""Database-backed data service for managing data tables."""

from __future__ import annotations

from typing import Optional, List
import uuid

from automic_etl.core.utils import utc_now
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

            table.updated_at = utc_now()
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

    def execute_sql(self, sql: str, limit: int = 100) -> dict:
        """
        Execute a SQL query against the data lakehouse.

        This is a simplified implementation that returns mock data.
        In production, this would execute against Delta Lake/Spark.
        """
        import re

        sql_lower = sql.lower().strip()

        # Extract table name from SQL
        table_match = re.search(r'from\s+(\w+)\.(\w+)', sql_lower)
        if not table_match:
            table_match = re.search(r'from\s+(\w+)', sql_lower)

        if table_match:
            if len(table_match.groups()) == 2:
                layer, table_name = table_match.groups()
            else:
                layer = "silver"
                table_name = table_match.group(1)

            # Try to get actual table metadata
            table = self.get_table_by_name(table_name, layer)
            if table and table.schema_definition:
                columns = table.schema_definition.get("columns", [])
                col_names = [c.get("name", f"col_{i}") for i, c in enumerate(columns)]

                return {
                    "columns": col_names or ["id", "name", "value"],
                    "data": [],
                    "row_count": 0,
                    "execution_time_ms": 15,
                    "bytes_scanned": 0,
                }

        # Default response for queries we can't parse
        return {
            "columns": ["result"],
            "data": [],
            "row_count": 0,
            "execution_time_ms": 10,
            "bytes_scanned": 0,
        }


def get_data_service() -> DataService:
    """Get the data service instance."""
    return DataService()
