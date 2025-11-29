"""Job definition and execution for Automic ETL."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable
from datetime import datetime
from enum import Enum
import uuid
import traceback

import structlog

from automic_etl.orchestration.scheduler import JobStatus

logger = structlog.get_logger()


@dataclass
class JobResult:
    """Result of a job execution."""
    job_id: str
    job_name: str
    status: JobStatus
    started_at: datetime
    ended_at: datetime
    duration_seconds: float
    output: Any = None
    error: str | None = None
    error_traceback: str | None = None
    metrics: dict[str, Any] = field(default_factory=dict)

    @property
    def success(self) -> bool:
        return self.status == JobStatus.SUCCESS


@dataclass
class JobContext:
    """Context passed to job execution."""
    job_id: str
    job_name: str
    execution_id: str
    started_at: datetime
    parameters: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)

    def log(self, message: str, **kwargs: Any) -> None:
        """Log a message with job context."""
        logger.info(
            message,
            job_id=self.job_id,
            execution_id=self.execution_id,
            **kwargs,
        )


class Job:
    """
    Definition of an ETL job.

    A job encapsulates:
    - The function to execute
    - Input/output parameters
    - Retry logic
    - Dependencies
    """

    def __init__(
        self,
        name: str,
        func: Callable[[JobContext], Any],
        parameters: dict[str, Any] | None = None,
        description: str = "",
        tags: list[str] | None = None,
        max_retries: int = 0,
        retry_delay_seconds: int = 60,
        timeout_seconds: int | None = None,
        depends_on: list[str] | None = None,
    ) -> None:
        """
        Initialize job.

        Args:
            name: Job name
            func: Function to execute (receives JobContext)
            parameters: Job parameters
            description: Job description
            tags: Tags for categorization
            max_retries: Maximum retry attempts
            retry_delay_seconds: Delay between retries
            timeout_seconds: Job timeout
            depends_on: List of job names this depends on
        """
        self.job_id = str(uuid.uuid4())
        self.name = name
        self.func = func
        self.parameters = parameters or {}
        self.description = description
        self.tags = tags or []
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.timeout_seconds = timeout_seconds
        self.depends_on = depends_on or []

    def execute(self, **override_params: Any) -> JobResult:
        """
        Execute the job.

        Args:
            override_params: Parameters to override defaults

        Returns:
            JobResult with execution details
        """
        execution_id = str(uuid.uuid4())
        started_at = datetime.utcnow()

        # Merge parameters
        params = {**self.parameters, **override_params}

        context = JobContext(
            job_id=self.job_id,
            job_name=self.name,
            execution_id=execution_id,
            started_at=started_at,
            parameters=params,
        )

        logger.info(
            "Job execution started",
            job_id=self.job_id,
            job_name=self.name,
            execution_id=execution_id,
        )

        try:
            output = self.func(context)

            ended_at = datetime.utcnow()

            result = JobResult(
                job_id=self.job_id,
                job_name=self.name,
                status=JobStatus.SUCCESS,
                started_at=started_at,
                ended_at=ended_at,
                duration_seconds=(ended_at - started_at).total_seconds(),
                output=output,
                metrics=context.metadata.get("metrics", {}),
            )

            logger.info(
                "Job execution completed",
                job_id=self.job_id,
                execution_id=execution_id,
                duration=result.duration_seconds,
            )

        except Exception as e:
            ended_at = datetime.utcnow()

            result = JobResult(
                job_id=self.job_id,
                job_name=self.name,
                status=JobStatus.FAILED,
                started_at=started_at,
                ended_at=ended_at,
                duration_seconds=(ended_at - started_at).total_seconds(),
                error=str(e),
                error_traceback=traceback.format_exc(),
            )

            logger.error(
                "Job execution failed",
                job_id=self.job_id,
                execution_id=execution_id,
                error=str(e),
            )

        return result


class JobRunner:
    """
    Run jobs with retry and dependency handling.
    """

    def __init__(
        self,
        parallel: bool = False,
        max_workers: int = 4,
    ) -> None:
        self.parallel = parallel
        self.max_workers = max_workers
        self.results: dict[str, JobResult] = {}
        self.logger = logger.bind(component="job_runner")

    def run(self, job: Job, **params: Any) -> JobResult:
        """Run a single job with retry logic."""
        retries = 0

        while True:
            result = job.execute(**params)

            if result.success:
                self.results[job.name] = result
                return result

            retries += 1
            if retries > job.max_retries:
                self.results[job.name] = result
                return result

            self.logger.warning(
                "Job failed, retrying",
                job_name=job.name,
                retry=retries,
                max_retries=job.max_retries,
            )

            import time
            time.sleep(job.retry_delay_seconds)

    def run_jobs(
        self,
        jobs: list[Job],
        stop_on_failure: bool = True,
    ) -> dict[str, JobResult]:
        """
        Run multiple jobs respecting dependencies.

        Args:
            jobs: Jobs to run
            stop_on_failure: Stop if any job fails

        Returns:
            Dictionary mapping job names to results
        """
        # Build dependency graph
        job_map = {j.name: j for j in jobs}
        completed = set()
        results = {}

        def can_run(job: Job) -> bool:
            return all(dep in completed for dep in job.depends_on)

        pending = list(jobs)

        while pending:
            # Find jobs that can run
            runnable = [j for j in pending if can_run(j)]

            if not runnable:
                # Circular dependency or missing dependency
                self.logger.error("No runnable jobs found - possible circular dependency")
                break

            if self.parallel:
                # Run in parallel using thread pool
                from concurrent.futures import ThreadPoolExecutor, as_completed

                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = {executor.submit(self.run, job): job for job in runnable}

                    for future in as_completed(futures):
                        job = futures[future]
                        result = future.result()
                        results[job.name] = result
                        completed.add(job.name)
                        pending.remove(job)

                        if not result.success and stop_on_failure:
                            return results
            else:
                # Run sequentially
                for job in runnable:
                    result = self.run(job)
                    results[job.name] = result
                    completed.add(job.name)
                    pending.remove(job)

                    if not result.success and stop_on_failure:
                        return results

        self.results.update(results)
        return results

    def get_result(self, job_name: str) -> JobResult | None:
        """Get result for a job."""
        return self.results.get(job_name)

    def get_summary(self) -> dict[str, Any]:
        """Get execution summary."""
        total = len(self.results)
        success = sum(1 for r in self.results.values() if r.success)
        failed = total - success

        total_duration = sum(r.duration_seconds for r in self.results.values())

        return {
            "total_jobs": total,
            "successful": success,
            "failed": failed,
            "success_rate": (success / total * 100) if total > 0 else 0,
            "total_duration_seconds": total_duration,
        }
