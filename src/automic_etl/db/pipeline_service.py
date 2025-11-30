"""Database-backed pipeline service."""

from __future__ import annotations

from datetime import datetime
from typing import Optional, List
import uuid

from automic_etl.db.engine import get_session
from automic_etl.db.models import PipelineModel, PipelineRunModel


class PipelineService:
    """Service for managing pipelines in the database."""

    def create_pipeline(
        self,
        name: str,
        owner_id: str,
        description: str = "",
        schedule: Optional[str] = None,
        source_type: Optional[str] = None,
        source_config: Optional[dict] = None,
        destination_layer: str = "bronze",
        transformations: Optional[list] = None,
    ) -> PipelineModel:
        """Create a new pipeline."""
        with get_session() as session:
            pipeline = PipelineModel(
                id=str(uuid.uuid4()),
                name=name,
                description=description,
                owner_id=owner_id,
                status="draft",
                schedule=schedule,
                source_type=source_type,
                source_config=source_config or {},
                destination_layer=destination_layer,
                transformations=transformations or [],
            )
            session.add(pipeline)
            session.flush()
            session.expunge(pipeline)
            return pipeline

    def get_pipeline(self, pipeline_id: str) -> Optional[PipelineModel]:
        """Get a pipeline by ID."""
        with get_session() as session:
            pipeline = session.query(PipelineModel).filter(
                PipelineModel.id == pipeline_id
            ).first()
            if pipeline:
                session.expunge(pipeline)
            return pipeline

    def list_pipelines(
        self,
        owner_id: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[PipelineModel]:
        """List pipelines with optional filters."""
        with get_session() as session:
            query = session.query(PipelineModel)

            if owner_id:
                query = query.filter(PipelineModel.owner_id == owner_id)

            if status:
                query = query.filter(PipelineModel.status == status)

            pipelines = query.order_by(PipelineModel.updated_at.desc()).all()
            for p in pipelines:
                session.expunge(p)
            return pipelines

    def update_pipeline(
        self,
        pipeline_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        status: Optional[str] = None,
        schedule: Optional[str] = None,
        source_type: Optional[str] = None,
        source_config: Optional[dict] = None,
        destination_layer: Optional[str] = None,
        transformations: Optional[list] = None,
    ) -> Optional[PipelineModel]:
        """Update a pipeline."""
        with get_session() as session:
            pipeline = session.query(PipelineModel).filter(
                PipelineModel.id == pipeline_id
            ).first()

            if not pipeline:
                return None

            if name is not None:
                pipeline.name = name
            if description is not None:
                pipeline.description = description
            if status is not None:
                pipeline.status = status
            if schedule is not None:
                pipeline.schedule = schedule
            if source_type is not None:
                pipeline.source_type = source_type
            if source_config is not None:
                pipeline.source_config = source_config
            if destination_layer is not None:
                pipeline.destination_layer = destination_layer
            if transformations is not None:
                pipeline.transformations = transformations

            pipeline.updated_at = datetime.utcnow()
            session.flush()
            session.expunge(pipeline)
            return pipeline

    def delete_pipeline(self, pipeline_id: str) -> bool:
        """Delete a pipeline."""
        with get_session() as session:
            pipeline = session.query(PipelineModel).filter(
                PipelineModel.id == pipeline_id
            ).first()

            if not pipeline:
                return False

            session.delete(pipeline)
            return True

    def run_pipeline(self, pipeline_id: str) -> Optional[PipelineRunModel]:
        """Create a new run for a pipeline."""
        with get_session() as session:
            pipeline = session.query(PipelineModel).filter(
                PipelineModel.id == pipeline_id
            ).first()

            if not pipeline:
                return None

            run = PipelineRunModel(
                id=str(uuid.uuid4()),
                pipeline_id=pipeline_id,
                status="running",
                started_at=datetime.utcnow(),
            )
            session.add(run)

            pipeline.status = "running"
            pipeline.last_run_at = datetime.utcnow()
            pipeline.run_count = (pipeline.run_count or 0) + 1

            session.flush()
            session.expunge(run)
            return run

    def complete_run(
        self,
        run_id: str,
        status: str,
        records_processed: int = 0,
        error_message: Optional[str] = None,
    ) -> Optional[PipelineRunModel]:
        """Complete a pipeline run."""
        with get_session() as session:
            run = session.query(PipelineRunModel).filter(
                PipelineRunModel.id == run_id
            ).first()

            if not run:
                return None

            run.status = status
            run.completed_at = datetime.utcnow()
            run.records_processed = records_processed
            run.error_message = error_message

            if run.started_at:
                run.duration_seconds = (run.completed_at - run.started_at).total_seconds()

            pipeline = session.query(PipelineModel).filter(
                PipelineModel.id == run.pipeline_id
            ).first()

            if pipeline:
                pipeline.status = "active" if status == "completed" else "failed"

            session.flush()
            session.expunge(run)
            return run

    def get_pipeline_runs(
        self,
        pipeline_id: str,
        limit: int = 10,
    ) -> List[PipelineRunModel]:
        """Get runs for a pipeline."""
        with get_session() as session:
            runs = session.query(PipelineRunModel).filter(
                PipelineRunModel.pipeline_id == pipeline_id
            ).order_by(
                PipelineRunModel.started_at.desc()
            ).limit(limit).all()

            for r in runs:
                session.expunge(r)
            return runs

    def get_all_runs(self, limit: int = 50) -> List[PipelineRunModel]:
        """Get all recent runs."""
        with get_session() as session:
            runs = session.query(PipelineRunModel).order_by(
                PipelineRunModel.started_at.desc()
            ).limit(limit).all()

            for r in runs:
                session.expunge(r)
            return runs


def get_pipeline_service() -> PipelineService:
    """Get the pipeline service instance."""
    return PipelineService()
