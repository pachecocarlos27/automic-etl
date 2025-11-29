"""Job scheduler for Automic ETL."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable
from datetime import datetime, timedelta
from enum import Enum
import threading
import time
import uuid

from croniter import croniter
import structlog

logger = structlog.get_logger()


class JobStatus(Enum):
    """Job execution status."""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


@dataclass
class Schedule:
    """Job schedule configuration."""
    schedule_type: str  # cron, interval, once
    expression: str | None = None  # cron expression
    interval_seconds: int | None = None  # for interval type
    run_at: datetime | None = None  # for once type
    timezone: str = "UTC"
    enabled: bool = True

    @classmethod
    def cron(cls, expression: str, timezone: str = "UTC") -> "Schedule":
        """Create a cron-based schedule."""
        return cls(schedule_type="cron", expression=expression, timezone=timezone)

    @classmethod
    def interval(cls, seconds: int = 0, minutes: int = 0, hours: int = 0) -> "Schedule":
        """Create an interval-based schedule."""
        total_seconds = seconds + (minutes * 60) + (hours * 3600)
        return cls(schedule_type="interval", interval_seconds=total_seconds)

    @classmethod
    def once(cls, run_at: datetime) -> "Schedule":
        """Create a one-time schedule."""
        return cls(schedule_type="once", run_at=run_at)

    @classmethod
    def daily(cls, hour: int = 0, minute: int = 0) -> "Schedule":
        """Create a daily schedule."""
        return cls.cron(f"{minute} {hour} * * *")

    @classmethod
    def hourly(cls, minute: int = 0) -> "Schedule":
        """Create an hourly schedule."""
        return cls.cron(f"{minute} * * * *")

    @classmethod
    def weekly(cls, day_of_week: int = 0, hour: int = 0) -> "Schedule":
        """Create a weekly schedule (0=Monday)."""
        return cls.cron(f"0 {hour} * * {day_of_week}")

    def get_next_run(self, after: datetime | None = None) -> datetime | None:
        """Calculate next run time."""
        after = after or datetime.utcnow()

        if self.schedule_type == "cron" and self.expression:
            cron = croniter(self.expression, after)
            return cron.get_next(datetime)
        elif self.schedule_type == "interval" and self.interval_seconds:
            return after + timedelta(seconds=self.interval_seconds)
        elif self.schedule_type == "once" and self.run_at:
            return self.run_at if self.run_at > after else None

        return None


@dataclass
class ScheduledJob:
    """A job registered with the scheduler."""
    job_id: str
    name: str
    schedule: Schedule
    func: Callable[[], Any]
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    next_run: datetime | None = None
    last_run: datetime | None = None
    last_status: JobStatus = JobStatus.PENDING
    run_count: int = 0
    error_count: int = 0
    max_retries: int = 0
    retry_delay: int = 60
    timeout: int | None = None
    tags: list[str] = field(default_factory=list)


@dataclass
class JobExecution:
    """Record of a job execution."""
    execution_id: str
    job_id: str
    job_name: str
    started_at: datetime
    ended_at: datetime | None = None
    status: JobStatus = JobStatus.RUNNING
    result: Any = None
    error: str | None = None
    duration_ms: int | None = None


class Scheduler:
    """
    Job scheduler for ETL pipelines.

    Features:
    - Cron and interval-based scheduling
    - Job dependencies
    - Retry logic
    - Execution history
    - Concurrent execution
    """

    def __init__(
        self,
        max_concurrent_jobs: int = 4,
        check_interval: int = 10,
    ) -> None:
        """
        Initialize scheduler.

        Args:
            max_concurrent_jobs: Maximum concurrent job executions
            check_interval: Seconds between schedule checks
        """
        self.max_concurrent_jobs = max_concurrent_jobs
        self.check_interval = check_interval
        self.jobs: dict[str, ScheduledJob] = {}
        self.executions: list[JobExecution] = []
        self._running = False
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._active_jobs: set[str] = set()
        self.logger = logger.bind(component="scheduler")

    def add_job(
        self,
        name: str,
        func: Callable[[], Any],
        schedule: Schedule,
        args: tuple = (),
        kwargs: dict | None = None,
        max_retries: int = 0,
        retry_delay: int = 60,
        timeout: int | None = None,
        tags: list[str] | None = None,
    ) -> str:
        """
        Add a job to the scheduler.

        Args:
            name: Job name
            func: Function to execute
            schedule: Schedule configuration
            args: Positional arguments for func
            kwargs: Keyword arguments for func
            max_retries: Maximum retry attempts
            retry_delay: Seconds between retries
            timeout: Job timeout in seconds
            tags: Tags for categorization

        Returns:
            Job ID
        """
        job_id = str(uuid.uuid4())

        job = ScheduledJob(
            job_id=job_id,
            name=name,
            schedule=schedule,
            func=func,
            args=args,
            kwargs=kwargs or {},
            next_run=schedule.get_next_run(),
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            tags=tags or [],
        )

        with self._lock:
            self.jobs[job_id] = job

        self.logger.info("Job added", job_id=job_id, name=name, next_run=job.next_run)
        return job_id

    def remove_job(self, job_id: str) -> bool:
        """Remove a job from the scheduler."""
        with self._lock:
            if job_id in self.jobs:
                del self.jobs[job_id]
                self.logger.info("Job removed", job_id=job_id)
                return True
        return False

    def pause_job(self, job_id: str) -> bool:
        """Pause a job."""
        with self._lock:
            if job_id in self.jobs:
                self.jobs[job_id].schedule.enabled = False
                self.logger.info("Job paused", job_id=job_id)
                return True
        return False

    def resume_job(self, job_id: str) -> bool:
        """Resume a paused job."""
        with self._lock:
            if job_id in self.jobs:
                job = self.jobs[job_id]
                job.schedule.enabled = True
                job.next_run = job.schedule.get_next_run()
                self.logger.info("Job resumed", job_id=job_id)
                return True
        return False

    def run_job_now(self, job_id: str) -> str | None:
        """Trigger immediate job execution."""
        with self._lock:
            if job_id not in self.jobs:
                return None

            job = self.jobs[job_id]

        return self._execute_job(job)

    def _execute_job(self, job: ScheduledJob) -> str:
        """Execute a job."""
        execution_id = str(uuid.uuid4())

        execution = JobExecution(
            execution_id=execution_id,
            job_id=job.job_id,
            job_name=job.name,
            started_at=datetime.utcnow(),
            status=JobStatus.RUNNING,
        )

        self.executions.append(execution)

        self.logger.info(
            "Job started",
            job_id=job.job_id,
            execution_id=execution_id,
            name=job.name,
        )

        try:
            with self._lock:
                self._active_jobs.add(job.job_id)

            # Execute the job function
            result = job.func(*job.args, **job.kwargs)

            execution.status = JobStatus.SUCCESS
            execution.result = result
            job.last_status = JobStatus.SUCCESS
            job.error_count = 0

            self.logger.info(
                "Job completed",
                job_id=job.job_id,
                execution_id=execution_id,
                status="success",
            )

        except Exception as e:
            execution.status = JobStatus.FAILED
            execution.error = str(e)
            job.last_status = JobStatus.FAILED
            job.error_count += 1

            self.logger.error(
                "Job failed",
                job_id=job.job_id,
                execution_id=execution_id,
                error=str(e),
            )

            # Handle retry
            if job.error_count <= job.max_retries:
                job.next_run = datetime.utcnow() + timedelta(seconds=job.retry_delay)
                self.logger.info(
                    "Job scheduled for retry",
                    job_id=job.job_id,
                    retry_count=job.error_count,
                    next_run=job.next_run,
                )

        finally:
            execution.ended_at = datetime.utcnow()
            execution.duration_ms = int(
                (execution.ended_at - execution.started_at).total_seconds() * 1000
            )

            job.last_run = execution.started_at
            job.run_count += 1

            # Schedule next run
            if job.schedule.enabled and job.schedule.schedule_type != "once":
                job.next_run = job.schedule.get_next_run(execution.ended_at)

            with self._lock:
                self._active_jobs.discard(job.job_id)

        return execution_id

    def _scheduler_loop(self) -> None:
        """Main scheduler loop."""
        while self._running:
            now = datetime.utcnow()

            jobs_to_run = []

            with self._lock:
                # Find jobs ready to run
                for job in self.jobs.values():
                    if (
                        job.schedule.enabled
                        and job.next_run
                        and job.next_run <= now
                        and job.job_id not in self._active_jobs
                        and len(self._active_jobs) < self.max_concurrent_jobs
                    ):
                        jobs_to_run.append(job)

            # Execute jobs in threads
            for job in jobs_to_run:
                thread = threading.Thread(target=self._execute_job, args=(job,))
                thread.start()

            time.sleep(self.check_interval)

    def start(self) -> None:
        """Start the scheduler."""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._thread.start()

        self.logger.info("Scheduler started")

    def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=30)

        self.logger.info("Scheduler stopped")

    def get_job(self, job_id: str) -> ScheduledJob | None:
        """Get job by ID."""
        return self.jobs.get(job_id)

    def get_jobs(
        self,
        tag: str | None = None,
        enabled: bool | None = None,
    ) -> list[ScheduledJob]:
        """Get jobs with optional filters."""
        jobs = list(self.jobs.values())

        if tag:
            jobs = [j for j in jobs if tag in j.tags]
        if enabled is not None:
            jobs = [j for j in jobs if j.schedule.enabled == enabled]

        return jobs

    def get_executions(
        self,
        job_id: str | None = None,
        status: JobStatus | None = None,
        limit: int = 100,
    ) -> list[JobExecution]:
        """Get execution history."""
        executions = self.executions[-limit:]

        if job_id:
            executions = [e for e in executions if e.job_id == job_id]
        if status:
            executions = [e for e in executions if e.status == status]

        return executions

    def get_status(self) -> dict[str, Any]:
        """Get scheduler status."""
        return {
            "running": self._running,
            "total_jobs": len(self.jobs),
            "enabled_jobs": sum(1 for j in self.jobs.values() if j.schedule.enabled),
            "active_jobs": len(self._active_jobs),
            "total_executions": len(self.executions),
            "max_concurrent": self.max_concurrent_jobs,
        }
