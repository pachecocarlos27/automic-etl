"""Database-backed table service."""

from __future__ import annotations

from typing import Optional, List
import uuid

from automic_etl.core.utils import utc_now
from automic_etl.db.engine import get_session
from automic_etl.db.models import DataTableModel


class TableService:
    """Service for managing data tables in the database."""

    def create_table(
        self,
        name: str,
        layer: str,
        schema_definition: Optional[dict] = None,
        description: str = "",
        source_pipeline_id: Optional[str] = None,
        tags: Optional[list] = None,
        created_by: Optional[str] = None,
    ) -> DataTableModel:
        """Create a new table entry."""
        with get_session() as session:
            table = DataTableModel(
                id=str(uuid.uuid4()),
                name=name,
                layer=layer,
                schema_definition=schema_definition or {},
                description=description,
                source_pipeline_id=source_pipeline_id,
                tags=tags or [],
            )
            # Store created_by in schema_definition metadata
            if created_by:
                table.schema_definition = {
                    **table.schema_definition,
                    "_metadata": {"created_by": created_by}
                }
            session.add(table)
            session.flush()
            session.expunge(table)
            return table

    def get_table(self, table_id: str) -> Optional[DataTableModel]:
        """Get a table by ID."""
        with get_session() as session:
            table = session.query(DataTableModel).filter(
                DataTableModel.id == table_id
            ).first()
            if table:
                session.expunge(table)
            return table

    def get_table_by_name(self, name: str, layer: str) -> Optional[DataTableModel]:
        """Get a table by name and layer."""
        with get_session() as session:
            table = session.query(DataTableModel).filter(
                DataTableModel.name == name,
                DataTableModel.layer == layer,
            ).first()
            if table:
                session.expunge(table)
            return table

    def list_tables(
        self,
        layer: Optional[str] = None,
        tag: Optional[str] = None,
        search: Optional[str] = None,
    ) -> List[DataTableModel]:
        """List tables with optional filters."""
        with get_session() as session:
            query = session.query(DataTableModel)

            if layer:
                query = query.filter(DataTableModel.layer == layer)

            tables = query.order_by(DataTableModel.updated_at.desc()).all()

            # Apply tag and search filters in Python for JSON fields
            result = []
            for table in tables:
                if tag and tag not in (table.tags or []):
                    continue
                if search:
                    search_lower = search.lower()
                    if (search_lower not in table.name.lower() and
                        search_lower not in (table.description or "").lower()):
                        continue
                session.expunge(table)
                result.append(table)

            return result

    def update_table(
        self,
        table_id: str,
        name: Optional[str] = None,
        schema_definition: Optional[dict] = None,
        description: Optional[str] = None,
        row_count: Optional[int] = None,
        size_bytes: Optional[int] = None,
        quality_score: Optional[float] = None,
        profile_data: Optional[dict] = None,
        tags: Optional[list] = None,
    ) -> Optional[DataTableModel]:
        """Update a table."""
        with get_session() as session:
            table = session.query(DataTableModel).filter(
                DataTableModel.id == table_id
            ).first()

            if not table:
                return None

            if name is not None:
                table.name = name
            if schema_definition is not None:
                table.schema_definition = schema_definition
            if description is not None:
                table.description = description
            if row_count is not None:
                table.row_count = row_count
            if size_bytes is not None:
                table.size_bytes = size_bytes
            if quality_score is not None:
                table.quality_score = quality_score
            if profile_data is not None:
                table.profile_data = profile_data
                table.last_profiled_at = utc_now()
            if tags is not None:
                table.tags = tags

            table.updated_at = utc_now()
            session.flush()
            session.expunge(table)
            return table

    def delete_table(self, table_id: str) -> bool:
        """Delete a table."""
        with get_session() as session:
            table = session.query(DataTableModel).filter(
                DataTableModel.id == table_id
            ).first()

            if not table:
                return False

            session.delete(table)
            return True

    def add_tags(self, table_id: str, tags: list) -> Optional[DataTableModel]:
        """Add tags to a table."""
        with get_session() as session:
            table = session.query(DataTableModel).filter(
                DataTableModel.id == table_id
            ).first()

            if not table:
                return None

            existing_tags = set(table.tags or [])
            existing_tags.update(tags)
            table.tags = list(existing_tags)
            table.updated_at = utc_now()

            session.flush()
            session.expunge(table)
            return table

    def update_schema(
        self,
        table_id: str,
        columns: list,
    ) -> Optional[DataTableModel]:
        """Update table schema (columns)."""
        with get_session() as session:
            table = session.query(DataTableModel).filter(
                DataTableModel.id == table_id
            ).first()

            if not table:
                return None

            schema = table.schema_definition or {}
            schema["columns"] = columns
            table.schema_definition = schema
            table.updated_at = utc_now()

            session.flush()
            session.expunge(table)
            return table

    def add_column(
        self,
        table_id: str,
        column: dict,
    ) -> Optional[DataTableModel]:
        """Add a column to a table."""
        with get_session() as session:
            table = session.query(DataTableModel).filter(
                DataTableModel.id == table_id
            ).first()

            if not table:
                return None

            schema = table.schema_definition or {}
            columns = schema.get("columns", [])
            columns.append(column)
            schema["columns"] = columns
            table.schema_definition = schema
            table.updated_at = utc_now()

            session.flush()
            session.expunge(table)
            return table

    def drop_column(
        self,
        table_id: str,
        column_name: str,
    ) -> Optional[DataTableModel]:
        """Drop a column from a table."""
        with get_session() as session:
            table = session.query(DataTableModel).filter(
                DataTableModel.id == table_id
            ).first()

            if not table:
                return None

            schema = table.schema_definition or {}
            columns = schema.get("columns", [])
            original_count = len(columns)
            columns = [c for c in columns if c.get("name") != column_name]

            if len(columns) == original_count:
                return None  # Column not found

            schema["columns"] = columns
            table.schema_definition = schema
            table.updated_at = utc_now()

            session.flush()
            session.expunge(table)
            return table

    def get_table_stats(self) -> dict:
        """Get summary statistics for all tables."""
        with get_session() as session:
            total_tables = session.query(DataTableModel).count()

            bronze_count = session.query(DataTableModel).filter(
                DataTableModel.layer == "bronze"
            ).count()

            silver_count = session.query(DataTableModel).filter(
                DataTableModel.layer == "silver"
            ).count()

            gold_count = session.query(DataTableModel).filter(
                DataTableModel.layer == "gold"
            ).count()

            return {
                "total_tables": total_tables,
                "bronze_tables": bronze_count,
                "silver_tables": silver_count,
                "gold_tables": gold_count,
            }

    def query_table_data(
        self,
        table_id: str,
        columns: Optional[List[str]] = None,
        filters: Optional[dict] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict:
        """
        Query data from a table.

        This is a simplified implementation that returns metadata-based results.
        In production, this would query actual Delta Lake/Parquet data.
        """
        table = self.get_table(table_id)
        if not table:
            return {"columns": [], "data": [], "total_rows": 0}

        schema = table.schema_definition or {}
        all_columns = schema.get("columns", [])

        # Use requested columns or all columns
        if columns:
            col_names = columns
        else:
            col_names = [c.get("name", f"col_{i}") for i, c in enumerate(all_columns)]

        return {
            "columns": col_names,
            "data": [],
            "total_rows": table.row_count or 0,
        }


# Singleton instance
_table_service: Optional[TableService] = None


def get_table_service() -> TableService:
    """Get the table service singleton."""
    global _table_service
    if _table_service is None:
        _table_service = TableService()
    return _table_service
