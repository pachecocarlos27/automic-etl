"""Workflow orchestration for complex ETL pipelines."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable
from datetime import datetime
from enum import Enum
import uuid

import structlog

from automic_etl.orchestration.scheduler import JobStatus

logger = structlog.get_logger()


class StepType(Enum):
    """Types of workflow steps."""
    TASK = "task"
    PARALLEL = "parallel"
    CONDITIONAL = "conditional"
    LOOP = "loop"
    SUBWORKFLOW = "subworkflow"


@dataclass
class WorkflowStep:
    """A step in a workflow."""
    step_id: str
    name: str
    step_type: StepType
    func: Callable | None = None
    condition: Callable[[], bool] | None = None
    on_success: str | None = None  # Next step on success
    on_failure: str | None = None  # Next step on failure
    parallel_steps: list["WorkflowStep"] | None = None
    loop_items: Callable[[], list] | None = None
    subworkflow: "Workflow | None" = None
    timeout_seconds: int | None = None
    max_retries: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class StepResult:
    """Result of a workflow step execution."""
    step_id: str
    step_name: str
    status: JobStatus
    started_at: datetime
    ended_at: datetime
    output: Any = None
    error: str | None = None


@dataclass
class WorkflowResult:
    """Result of a complete workflow execution."""
    workflow_id: str
    workflow_name: str
    status: JobStatus
    started_at: datetime
    ended_at: datetime
    step_results: list[StepResult]
    output: dict[str, Any] = field(default_factory=dict)

    @property
    def success(self) -> bool:
        return self.status == JobStatus.SUCCESS

    @property
    def duration_seconds(self) -> float:
        return (self.ended_at - self.started_at).total_seconds()


class Workflow:
    """
    A workflow containing multiple steps.

    Features:
    - Sequential and parallel execution
    - Conditional branching
    - Loop support
    - Subworkflow composition
    - Error handling and retries
    """

    def __init__(
        self,
        name: str,
        description: str = "",
    ) -> None:
        self.workflow_id = str(uuid.uuid4())
        self.name = name
        self.description = description
        self.steps: dict[str, WorkflowStep] = {}
        self.start_step: str | None = None
        self.logger = logger.bind(workflow=name)

    def add_step(
        self,
        name: str,
        func: Callable,
        step_type: StepType = StepType.TASK,
        on_success: str | None = None,
        on_failure: str | None = None,
        timeout: int | None = None,
        max_retries: int = 0,
        **metadata: Any,
    ) -> str:
        """
        Add a task step to the workflow.

        Args:
            name: Step name
            func: Function to execute
            step_type: Type of step
            on_success: Next step on success
            on_failure: Next step on failure
            timeout: Step timeout
            max_retries: Retry attempts
            metadata: Additional metadata

        Returns:
            Step ID
        """
        step_id = str(uuid.uuid4())

        step = WorkflowStep(
            step_id=step_id,
            name=name,
            step_type=step_type,
            func=func,
            on_success=on_success,
            on_failure=on_failure,
            timeout_seconds=timeout,
            max_retries=max_retries,
            metadata=metadata,
        )

        self.steps[name] = step

        if self.start_step is None:
            self.start_step = name

        return step_id

    def add_parallel_step(
        self,
        name: str,
        steps: list[tuple[str, Callable]],
        on_success: str | None = None,
        on_failure: str | None = None,
    ) -> str:
        """Add a parallel execution step."""
        step_id = str(uuid.uuid4())

        parallel_steps = [
            WorkflowStep(
                step_id=str(uuid.uuid4()),
                name=step_name,
                step_type=StepType.TASK,
                func=step_func,
            )
            for step_name, step_func in steps
        ]

        step = WorkflowStep(
            step_id=step_id,
            name=name,
            step_type=StepType.PARALLEL,
            parallel_steps=parallel_steps,
            on_success=on_success,
            on_failure=on_failure,
        )

        self.steps[name] = step
        return step_id

    def add_conditional_step(
        self,
        name: str,
        condition: Callable[[], bool],
        on_true: str,
        on_false: str,
    ) -> str:
        """Add a conditional branching step."""
        step_id = str(uuid.uuid4())

        step = WorkflowStep(
            step_id=step_id,
            name=name,
            step_type=StepType.CONDITIONAL,
            condition=condition,
            on_success=on_true,
            on_failure=on_false,
        )

        self.steps[name] = step
        return step_id

    def add_loop_step(
        self,
        name: str,
        items_func: Callable[[], list],
        body_func: Callable[[Any], Any],
        on_complete: str | None = None,
    ) -> str:
        """Add a loop step."""
        step_id = str(uuid.uuid4())

        step = WorkflowStep(
            step_id=step_id,
            name=name,
            step_type=StepType.LOOP,
            loop_items=items_func,
            func=body_func,
            on_success=on_complete,
        )

        self.steps[name] = step
        return step_id

    def add_subworkflow_step(
        self,
        name: str,
        subworkflow: "Workflow",
        on_success: str | None = None,
        on_failure: str | None = None,
    ) -> str:
        """Add a subworkflow step."""
        step_id = str(uuid.uuid4())

        step = WorkflowStep(
            step_id=step_id,
            name=name,
            step_type=StepType.SUBWORKFLOW,
            subworkflow=subworkflow,
            on_success=on_success,
            on_failure=on_failure,
        )

        self.steps[name] = step
        return step_id

    def set_start(self, step_name: str) -> None:
        """Set the starting step."""
        if step_name not in self.steps:
            raise ValueError(f"Step '{step_name}' not found")
        self.start_step = step_name


class WorkflowRunner:
    """
    Execute workflows with proper error handling and logging.
    """

    def __init__(self) -> None:
        self.logger = logger.bind(component="workflow_runner")
        self.context: dict[str, Any] = {}

    def run(self, workflow: Workflow, context: dict[str, Any] | None = None) -> WorkflowResult:
        """
        Execute a workflow.

        Args:
            workflow: Workflow to execute
            context: Initial context data

        Returns:
            WorkflowResult with execution details
        """
        self.context = context or {}
        started_at = datetime.utcnow()
        step_results = []

        self.logger.info("Workflow started", workflow=workflow.name)

        current_step = workflow.start_step
        status = JobStatus.SUCCESS

        try:
            while current_step:
                step = workflow.steps.get(current_step)
                if not step:
                    self.logger.error(f"Step '{current_step}' not found")
                    status = JobStatus.FAILED
                    break

                result = self._execute_step(step)
                step_results.append(result)

                if result.status == JobStatus.SUCCESS:
                    current_step = step.on_success
                else:
                    status = JobStatus.FAILED
                    current_step = step.on_failure

        except Exception as e:
            self.logger.error("Workflow failed", error=str(e))
            status = JobStatus.FAILED

        ended_at = datetime.utcnow()

        result = WorkflowResult(
            workflow_id=workflow.workflow_id,
            workflow_name=workflow.name,
            status=status,
            started_at=started_at,
            ended_at=ended_at,
            step_results=step_results,
            output=self.context,
        )

        self.logger.info(
            "Workflow completed",
            workflow=workflow.name,
            status=status.value,
            duration=result.duration_seconds,
        )

        return result

    def _execute_step(self, step: WorkflowStep) -> StepResult:
        """Execute a single step."""
        started_at = datetime.utcnow()

        self.logger.debug("Executing step", step=step.name, type=step.step_type.value)

        try:
            if step.step_type == StepType.TASK:
                output = self._execute_task(step)
            elif step.step_type == StepType.PARALLEL:
                output = self._execute_parallel(step)
            elif step.step_type == StepType.CONDITIONAL:
                output = self._execute_conditional(step)
            elif step.step_type == StepType.LOOP:
                output = self._execute_loop(step)
            elif step.step_type == StepType.SUBWORKFLOW:
                output = self._execute_subworkflow(step)
            else:
                raise ValueError(f"Unknown step type: {step.step_type}")

            status = JobStatus.SUCCESS
            error = None

        except Exception as e:
            output = None
            status = JobStatus.FAILED
            error = str(e)
            self.logger.error("Step failed", step=step.name, error=error)

        ended_at = datetime.utcnow()

        return StepResult(
            step_id=step.step_id,
            step_name=step.name,
            status=status,
            started_at=started_at,
            ended_at=ended_at,
            output=output,
            error=error,
        )

    def _execute_task(self, step: WorkflowStep) -> Any:
        """Execute a task step."""
        if step.func is None:
            raise ValueError("Task step has no function")

        retries = 0
        last_error = None

        while retries <= step.max_retries:
            try:
                result = step.func(self.context)
                return result
            except Exception as e:
                last_error = e
                retries += 1
                if retries <= step.max_retries:
                    self.logger.warning(
                        "Step failed, retrying",
                        step=step.name,
                        retry=retries,
                    )
                    import time
                    time.sleep(5)

        raise last_error

    def _execute_parallel(self, step: WorkflowStep) -> list[Any]:
        """Execute parallel steps."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        if not step.parallel_steps:
            return []

        results = []

        with ThreadPoolExecutor(max_workers=len(step.parallel_steps)) as executor:
            futures = {}
            for sub_step in step.parallel_steps:
                if sub_step.func:
                    future = executor.submit(sub_step.func, self.context)
                    futures[future] = sub_step.name

            for future in as_completed(futures):
                step_name = futures[future]
                try:
                    result = future.result()
                    results.append({"step": step_name, "result": result})
                except Exception as e:
                    results.append({"step": step_name, "error": str(e)})

        return results

    def _execute_conditional(self, step: WorkflowStep) -> bool:
        """Execute conditional step."""
        if step.condition is None:
            raise ValueError("Conditional step has no condition")

        return step.condition()

    def _execute_loop(self, step: WorkflowStep) -> list[Any]:
        """Execute loop step."""
        if step.loop_items is None or step.func is None:
            raise ValueError("Loop step missing items or function")

        items = step.loop_items()
        results = []

        for item in items:
            result = step.func(item)
            results.append(result)

        return results

    def _execute_subworkflow(self, step: WorkflowStep) -> WorkflowResult:
        """Execute subworkflow."""
        if step.subworkflow is None:
            raise ValueError("Subworkflow step has no workflow")

        runner = WorkflowRunner()
        return runner.run(step.subworkflow, self.context)
