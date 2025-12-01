"""Database-backed job scheduling service."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional, List
import uuid

from croniter import croniter

from automic_etl.db.engine import get_session
from automic_etl.db.models import JobScheduleModel, JobRunModel


class JobService:
    """Service for managing scheduled jobs in the database."""

    def create_schedule(
        self,
        name: str,
        job_type: str,
        schedule_type: str,
        schedule_value: str,
        target_id: Optional[str] = None,
        description: str = "",
        timezone: str = "UTC",
        config: Optional[dict] = None,
        created_by: Optional[str] = None,
    ) -> JobScheduleModel:
        """Create a new job schedule."""
        with get_session() as session:
            # Calculate next run time
            next_run = self._calculate_next_run(schedule_type, schedule_value)

            schedule = JobScheduleModel(
                id=str(uuid.uuid4()),
                name=name,
                description=description,
                job_type=job_type,
                target_id=target_id,
                schedule_type=schedule_type,
                schedule_value=schedule_value,
                timezone=timezone,
                config=config or {},
                next_run_at=next_run,
                created_by=created_by,
            )
            session.add(schedule)
            session.flush()
            session.expunge(schedule)
            return schedule

    def get_schedule(self, schedule_id: str) -> Optional[JobScheduleModel]:
        """Get a schedule by ID."""
        with get_session() as session:
            schedule = session.query(JobScheduleModel).filter(
                JobScheduleModel.id == schedule_id
            ).first()
            if schedule:
                session.expunge(schedule)
            return schedule

    def get_schedule_by_name(self, name: str) -> Optional[JobScheduleModel]:
        """Get a schedule by name."""
        with get_session() as session:
            schedule = session.query(JobScheduleModel).filter(
                JobScheduleModel.name == name
            ).first()
            if schedule:
                session.expunge(schedule)
            return schedule

    def list_schedules(
        self,
        job_type: Optional[str] = None,
        enabled: Optional[bool] = None,
    ) -> List[JobScheduleModel]:
        """List schedules with optional filters."""
        with get_session() as session:
            query = session.query(JobScheduleModel)

            if job_type:
                query = query.filter(JobScheduleModel.job_type == job_type)
            if enabled is not None:
                query = query.filter(JobScheduleModel.enabled == enabled)

            schedules = query.order_by(JobScheduleModel.next_run_at.asc()).all()
            for s in schedules:
                session.expunge(s)
            return schedules

    def get_due_schedules(self) -> List[JobScheduleModel]:
        """Get all enabled schedules that are due to run."""
        with get_session() as session:
            now = datetime.utcnow()
            schedules = session.query(JobScheduleModel).filter(
                JobScheduleModel.enabled == True,
                JobScheduleModel.next_run_at <= now,
            ).order_by(JobScheduleModel.next_run_at.asc()).all()

            for s in schedules:
                session.expunge(s)
            return schedules

    def update_schedule(
        self,
        schedule_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        schedule_type: Optional[str] = None,
        schedule_value: Optional[str] = None,
        enabled: Optional[bool] = None,
        config: Optional[dict] = None,
    ) -> Optional[JobScheduleModel]:
        """Update a schedule."""
        with get_session() as session:
            schedule = session.query(JobScheduleModel).filter(
                JobScheduleModel.id == schedule_id
            ).first()

            if not schedule:
                return None

            if name is not None:
                schedule.name = name
            if description is not None:
                schedule.description = description
            if schedule_type is not None:
                schedule.schedule_type = schedule_type
            if schedule_value is not None:
                schedule.schedule_value = schedule_value
            if enabled is not None:
                schedule.enabled = enabled
            if config is not None:
                schedule.config = config

            # Recalculate next run if schedule changed
            if schedule_type is not None or schedule_value is not None:
                schedule.next_run_at = self._calculate_next_run(
                    schedule.schedule_type, schedule.schedule_value
                )

            schedule.updated_at = datetime.utcnow()
            session.flush()
            session.expunge(schedule)
            return schedule

    def delete_schedule(self, schedule_id: str) -> bool:
        """Delete a schedule."""
        with get_session() as session:
            schedule = session.query(JobScheduleModel).filter(
                JobScheduleModel.id == schedule_id
            ).first()

            if not schedule:
                return False

            session.delete(schedule)
            return True

    def mark_schedule_run(self, schedule_id: str) -> None:
        """Mark schedule as run and update next run time."""
        with get_session() as session:
            schedule = session.query(JobScheduleModel).filter(
                JobScheduleModel.id == schedule_id
            ).first()

            if schedule:
                schedule.last_run_at = datetime.utcnow()
                schedule.run_count = (schedule.run_count or 0) + 1
                schedule.next_run_at = self._calculate_next_run(
                    schedule.schedule_type, schedule.schedule_value
                )

                # Disable one-time schedules after run
                if schedule.schedule_type == "once":
                    schedule.enabled = False

    def create_run(
        self,
        schedule_id: str,
        triggered_by: str = "scheduler",
    ) -> JobRunModel:
        """Create a new job run record."""
        with get_session() as session:
            run = JobRunModel(
                id=str(uuid.uuid4()),
                schedule_id=schedule_id,
                status="pending",
                triggered_by=triggered_by,
            )
            session.add(run)
            session.flush()
            session.expunge(run)
            return run

    def update_run(
        self,
        run_id: str,
        status: Optional[str] = None,
        result: Optional[dict] = None,
        error_message: Optional[str] = None,
        logs: Optional[list] = None,
    ) -> Optional[JobRunModel]:
        """Update a job run."""
        with get_session() as session:
            run = session.query(JobRunModel).filter(
                JobRunModel.id == run_id
            ).first()

            if not run:
                return None

            if status is not None:
                run.status = status
                if status in ("completed", "failed", "cancelled"):
                    run.completed_at = datetime.utcnow()
                    if run.started_at:
                        run.duration_seconds = (
                            run.completed_at - run.started_at
                        ).total_seconds()
                elif status == "running":
                    run.started_at = datetime.utcnow()

            if result is not None:
                run.result = result
            if error_message is not None:
                run.error_message = error_message
            if logs is not None:
                run.logs = logs

            session.flush()
            session.expunge(run)
            return run

    def get_runs(
        self,
        schedule_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[JobRunModel]:
        """Get job runs with optional filters."""
        with get_session() as session:
            query = session.query(JobRunModel)

            if schedule_id:
                query = query.filter(JobRunModel.schedule_id == schedule_id)
            if status:
                query = query.filter(JobRunModel.status == status)

            runs = query.order_by(
                JobRunModel.started_at.desc()
            ).limit(limit).all()

            for r in runs:
                session.expunge(r)
            return runs

    def _calculate_next_run(
        self,
        schedule_type: str,
        schedule_value: str,
    ) -> datetime:
        """Calculate the next run time based on schedule type."""
        now = datetime.utcnow()

        if schedule_type == "cron":
            try:
                cron = croniter(schedule_value, now)
                return cron.get_next(datetime)
            except (ValueError, KeyError):
                # Invalid cron expression, default to 1 hour
                return now + timedelta(hours=1)

        elif schedule_type == "interval":
            try:
                minutes = int(schedule_value)
                return now + timedelta(minutes=minutes)
            except ValueError:
                return now + timedelta(hours=1)

        elif schedule_type == "once":
            try:
                return datetime.fromisoformat(schedule_value)
            except ValueError:
                return now + timedelta(hours=1)

        return now + timedelta(hours=1)


# Singleton instance
_job_service: Optional[JobService] = None


def get_job_service() -> JobService:
    """Get the job service singleton."""
    global _job_service
    if _job_service is None:
        _job_service = JobService()
    return _job_service
