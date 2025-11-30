"""Apache Airflow integration for workflow orchestration."""

from __future__ import annotations

import logging
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal
from datetime import datetime, timedelta
import requests

logger = logging.getLogger(__name__)


@dataclass
class AirflowConfig:
    """Airflow configuration."""

    # API connection
    base_url: str = "http://localhost:8080"
    username: str = "admin"
    password: str = "admin"
    api_version: str = "v1"

    # DAG generation
    dags_folder: str | Path | None = None
    default_args: dict[str, Any] = field(default_factory=dict)

    # Default schedule
    schedule_interval: str = "@daily"
    start_date: datetime | None = None
    catchup: bool = False
    max_active_runs: int = 1

    # Retry settings
    retries: int = 3
    retry_delay_minutes: int = 5
    retry_exponential_backoff: bool = True

    # Email notifications
    email: list[str] = field(default_factory=list)
    email_on_failure: bool = True
    email_on_retry: bool = False

    # Timeout
    execution_timeout_minutes: int = 60
    dagrun_timeout_minutes: int | None = None


class AirflowIntegration:
    """
    Apache Airflow integration for orchestrating Automic ETL pipelines.

    Features:
    - Trigger and monitor DAG runs via REST API
    - Generate Airflow DAGs from Automic pipelines
    - Task state monitoring
    - XCom integration for data passing
    - Connection management

    Example:
        config = AirflowConfig(
            base_url="http://airflow.example.com:8080",
            username="admin",
            password="secret"
        )

        airflow = AirflowIntegration(config)

        # Trigger a DAG
        run_id = airflow.trigger_dag("my_etl_dag", {"date": "2024-01-01"})

        # Monitor progress
        state = airflow.get_dag_run_state("my_etl_dag", run_id)
    """

    def __init__(self, config: AirflowConfig):
        self.config = config
        self._session = None

    @property
    def api_url(self) -> str:
        """Get API base URL."""
        return f"{self.config.base_url}/api/{self.config.api_version}"

    @property
    def session(self) -> requests.Session:
        """Get authenticated session."""
        if self._session is None:
            self._session = requests.Session()
            self._session.auth = (self.config.username, self.config.password)
            self._session.headers["Content-Type"] = "application/json"
        return self._session

    def _request(
        self,
        method: str,
        endpoint: str,
        data: dict | None = None,
        params: dict | None = None,
    ) -> dict[str, Any]:
        """Make authenticated API request."""
        url = f"{self.api_url}/{endpoint}"

        response = self.session.request(
            method=method,
            url=url,
            json=data,
            params=params,
        )

        if response.status_code >= 400:
            logger.error(f"Airflow API error: {response.status_code} - {response.text}")
            response.raise_for_status()

        return response.json() if response.text else {}

    # ========================
    # DAG Operations
    # ========================

    def list_dags(
        self,
        limit: int = 100,
        offset: int = 0,
        tags: list[str] | None = None,
        only_active: bool = True,
    ) -> list[dict[str, Any]]:
        """
        List all DAGs.

        Args:
            limit: Maximum number of DAGs to return
            offset: Offset for pagination
            tags: Filter by tags
            only_active: Only return active DAGs

        Returns:
            List of DAG metadata
        """
        params = {
            "limit": limit,
            "offset": offset,
            "only_active": only_active,
        }
        if tags:
            params["tags"] = tags

        response = self._request("GET", "dags", params=params)
        return response.get("dags", [])

    def get_dag(self, dag_id: str) -> dict[str, Any]:
        """Get DAG details."""
        return self._request("GET", f"dags/{dag_id}")

    def pause_dag(self, dag_id: str) -> dict[str, Any]:
        """Pause a DAG."""
        return self._request("PATCH", f"dags/{dag_id}", data={"is_paused": True})

    def unpause_dag(self, dag_id: str) -> dict[str, Any]:
        """Unpause a DAG."""
        return self._request("PATCH", f"dags/{dag_id}", data={"is_paused": False})

    def trigger_dag(
        self,
        dag_id: str,
        conf: dict[str, Any] | None = None,
        execution_date: datetime | None = None,
        run_id: str | None = None,
    ) -> str:
        """
        Trigger a DAG run.

        Args:
            dag_id: ID of the DAG to trigger
            conf: Configuration to pass to the DAG
            execution_date: Logical date for the run
            run_id: Custom run ID

        Returns:
            Run ID of the triggered DAG run
        """
        data = {}
        if conf:
            data["conf"] = conf
        if execution_date:
            data["execution_date"] = execution_date.isoformat()
        if run_id:
            data["dag_run_id"] = run_id

        response = self._request("POST", f"dags/{dag_id}/dagRuns", data=data)
        return response.get("dag_run_id")

    def get_dag_run(self, dag_id: str, run_id: str) -> dict[str, Any]:
        """Get DAG run details."""
        return self._request("GET", f"dags/{dag_id}/dagRuns/{run_id}")

    def get_dag_run_state(self, dag_id: str, run_id: str) -> str:
        """Get the state of a DAG run."""
        run = self.get_dag_run(dag_id, run_id)
        return run.get("state", "unknown")

    def list_dag_runs(
        self,
        dag_id: str,
        limit: int = 25,
        state: str | None = None,
        start_date_gte: datetime | None = None,
        end_date_lte: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """
        List DAG runs.

        Args:
            dag_id: ID of the DAG
            limit: Maximum number of runs
            state: Filter by state
            start_date_gte: Filter by start date
            end_date_lte: Filter by end date

        Returns:
            List of DAG runs
        """
        params = {"limit": limit}
        if state:
            params["state"] = state
        if start_date_gte:
            params["start_date_gte"] = start_date_gte.isoformat()
        if end_date_lte:
            params["end_date_lte"] = end_date_lte.isoformat()

        response = self._request("GET", f"dags/{dag_id}/dagRuns", params=params)
        return response.get("dag_runs", [])

    def clear_dag_run(
        self,
        dag_id: str,
        run_id: str,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Clear (reset) a DAG run."""
        data = {"dry_run": dry_run}
        return self._request("POST", f"dags/{dag_id}/dagRuns/{run_id}/clear", data=data)

    def delete_dag_run(self, dag_id: str, run_id: str) -> None:
        """Delete a DAG run."""
        self._request("DELETE", f"dags/{dag_id}/dagRuns/{run_id}")

    # ========================
    # Task Operations
    # ========================

    def get_task_instances(
        self,
        dag_id: str,
        run_id: str,
    ) -> list[dict[str, Any]]:
        """Get all task instances for a DAG run."""
        response = self._request(
            "GET",
            f"dags/{dag_id}/dagRuns/{run_id}/taskInstances"
        )
        return response.get("task_instances", [])

    def get_task_instance(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
    ) -> dict[str, Any]:
        """Get a specific task instance."""
        return self._request(
            "GET",
            f"dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}"
        )

    def get_task_logs(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        task_try_number: int = 1,
    ) -> str:
        """Get task logs."""
        response = self.session.get(
            f"{self.api_url}/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{task_try_number}"
        )
        return response.text

    def clear_task_instances(
        self,
        dag_id: str,
        run_id: str,
        task_ids: list[str],
        include_downstream: bool = False,
        include_upstream: bool = False,
    ) -> dict[str, Any]:
        """Clear specific task instances."""
        data = {
            "dag_run_id": run_id,
            "task_ids": task_ids,
            "include_downstream": include_downstream,
            "include_upstream": include_upstream,
        }
        return self._request("POST", f"dags/{dag_id}/clearTaskInstances", data=data)

    # ========================
    # XCom Operations
    # ========================

    def get_xcom_entries(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
    ) -> list[dict[str, Any]]:
        """Get XCom entries for a task."""
        response = self._request(
            "GET",
            f"dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries"
        )
        return response.get("xcom_entries", [])

    def get_xcom_entry(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        xcom_key: str = "return_value",
    ) -> Any:
        """Get a specific XCom value."""
        response = self._request(
            "GET",
            f"dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key}"
        )
        return response.get("value")

    # ========================
    # Connection Management
    # ========================

    def list_connections(self) -> list[dict[str, Any]]:
        """List all connections."""
        response = self._request("GET", "connections")
        return response.get("connections", [])

    def get_connection(self, conn_id: str) -> dict[str, Any]:
        """Get a connection."""
        return self._request("GET", f"connections/{conn_id}")

    def create_connection(
        self,
        conn_id: str,
        conn_type: str,
        host: str | None = None,
        port: int | None = None,
        login: str | None = None,
        password: str | None = None,
        schema: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create a new connection."""
        data = {
            "connection_id": conn_id,
            "conn_type": conn_type,
        }
        if host:
            data["host"] = host
        if port:
            data["port"] = port
        if login:
            data["login"] = login
        if password:
            data["password"] = password
        if schema:
            data["schema"] = schema
        if extra:
            data["extra"] = json.dumps(extra)

        return self._request("POST", "connections", data=data)

    def delete_connection(self, conn_id: str) -> None:
        """Delete a connection."""
        self._request("DELETE", f"connections/{conn_id}")

    # ========================
    # Variable Management
    # ========================

    def list_variables(self) -> list[dict[str, Any]]:
        """List all variables."""
        response = self._request("GET", "variables")
        return response.get("variables", [])

    def get_variable(self, key: str) -> Any:
        """Get a variable value."""
        response = self._request("GET", f"variables/{key}")
        return response.get("value")

    def set_variable(self, key: str, value: Any) -> dict[str, Any]:
        """Set a variable."""
        data = {"key": key, "value": json.dumps(value) if not isinstance(value, str) else value}
        return self._request("POST", "variables", data=data)

    def delete_variable(self, key: str) -> None:
        """Delete a variable."""
        self._request("DELETE", f"variables/{key}")

    # ========================
    # Health & Monitoring
    # ========================

    def health_check(self) -> dict[str, Any]:
        """Check Airflow health."""
        return self._request("GET", "health")

    def get_import_errors(self) -> list[dict[str, Any]]:
        """Get DAG import errors."""
        response = self._request("GET", "importErrors")
        return response.get("import_errors", [])

    # ========================
    # DAG Generation
    # ========================

    def generate_dag(
        self,
        dag_id: str,
        tasks: list[dict[str, Any]],
        description: str = "",
        tags: list[str] | None = None,
        schedule: str | None = None,
    ) -> str:
        """
        Generate an Airflow DAG Python file.

        Args:
            dag_id: ID for the DAG
            tasks: List of task definitions
            description: DAG description
            tags: DAG tags
            schedule: Schedule interval

        Returns:
            Generated DAG code
        """
        schedule = schedule or self.config.schedule_interval
        start_date = self.config.start_date or datetime.now() - timedelta(days=1)

        # Build default args
        default_args = {
            "owner": "automic-etl",
            "depends_on_past": False,
            "email": self.config.email,
            "email_on_failure": self.config.email_on_failure,
            "email_on_retry": self.config.email_on_retry,
            "retries": self.config.retries,
            "retry_delay": f"timedelta(minutes={self.config.retry_delay_minutes})",
            **self.config.default_args,
        }

        default_args_str = "{\n"
        for key, value in default_args.items():
            if isinstance(value, str) and value.startswith("timedelta"):
                default_args_str += f"        '{key}': {value},\n"
            elif isinstance(value, bool):
                default_args_str += f"        '{key}': {value},\n"
            elif isinstance(value, list):
                default_args_str += f"        '{key}': {value},\n"
            else:
                default_args_str += f"        '{key}': '{value}',\n"
        default_args_str += "    }"

        # Build task code
        task_code = []
        task_ids = []

        for task in tasks:
            task_id = task["task_id"]
            task_ids.append(task_id)
            task_type = task.get("type", "python")

            if task_type == "python":
                task_code.append(f'''
    {task_id} = PythonOperator(
        task_id='{task_id}',
        python_callable={task.get('callable', 'lambda: None')},
        op_kwargs={task.get('op_kwargs', {})},
    )''')

            elif task_type == "bash":
                task_code.append(f'''
    {task_id} = BashOperator(
        task_id='{task_id}',
        bash_command='{task.get("bash_command", "echo hello")}',
    )''')

            elif task_type == "automic_etl":
                task_code.append(f'''
    {task_id} = PythonOperator(
        task_id='{task_id}',
        python_callable=run_automic_pipeline,
        op_kwargs={{
            'pipeline_name': '{task.get("pipeline_name")}',
            'config': {task.get("config", {})},
        }},
    )''')

        # Build dependencies
        deps = task.get("dependencies", [])
        dep_code = []
        for i, task in enumerate(tasks):
            task_id = task["task_id"]
            upstream = task.get("upstream", [])
            if upstream:
                for up in upstream:
                    dep_code.append(f"    {up} >> {task_id}")

        # Generate DAG file
        dag_code = f'''"""
Automic ETL Generated DAG: {dag_id}

{description}

Generated at: {datetime.now().isoformat()}
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def run_automic_pipeline(pipeline_name: str, config: dict):
    """Run an Automic ETL pipeline."""
    from automic_etl.core.pipeline import Pipeline

    pipeline = Pipeline(pipeline_name, config)
    result = pipeline.run()
    return result


default_args = {default_args_str}

with DAG(
    dag_id='{dag_id}',
    default_args=default_args,
    description='{description}',
    schedule_interval='{schedule}',
    start_date=datetime({start_date.year}, {start_date.month}, {start_date.day}),
    catchup={self.config.catchup},
    max_active_runs={self.config.max_active_runs},
    tags={tags or []},
) as dag:
{"".join(task_code)}

    # Task dependencies
{chr(10).join(dep_code) if dep_code else "    pass"}
'''

        return dag_code

    def save_dag(
        self,
        dag_id: str,
        tasks: list[dict[str, Any]],
        **kwargs,
    ) -> Path:
        """
        Generate and save a DAG file.

        Args:
            dag_id: ID for the DAG
            tasks: List of task definitions
            **kwargs: Additional arguments for generate_dag

        Returns:
            Path to saved DAG file
        """
        if not self.config.dags_folder:
            raise ValueError("dags_folder must be configured to save DAGs")

        dag_code = self.generate_dag(dag_id, tasks, **kwargs)

        dags_path = Path(self.config.dags_folder)
        dags_path.mkdir(parents=True, exist_ok=True)

        dag_file = dags_path / f"{dag_id}.py"
        with open(dag_file, "w") as f:
            f.write(dag_code)

        logger.info(f"Saved DAG to {dag_file}")
        return dag_file

    def wait_for_dag_run(
        self,
        dag_id: str,
        run_id: str,
        timeout_seconds: int = 3600,
        poll_interval: int = 10,
    ) -> dict[str, Any]:
        """
        Wait for a DAG run to complete.

        Args:
            dag_id: ID of the DAG
            run_id: ID of the run
            timeout_seconds: Maximum time to wait
            poll_interval: Seconds between status checks

        Returns:
            Final DAG run state
        """
        import time

        terminal_states = {"success", "failed", "upstream_failed"}
        start_time = time.time()

        while True:
            run = self.get_dag_run(dag_id, run_id)
            state = run.get("state", "").lower()

            if state in terminal_states:
                return run

            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                raise TimeoutError(
                    f"DAG run {run_id} did not complete within {timeout_seconds} seconds"
                )

            time.sleep(poll_interval)
