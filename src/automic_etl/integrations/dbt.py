"""dbt (Data Build Tool) integration for SQL transformations."""

from __future__ import annotations

import json
import logging
import subprocess
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal
import yaml

logger = logging.getLogger(__name__)


@dataclass
class DbtConfig:
    """dbt configuration."""

    project_dir: str | Path
    profiles_dir: str | Path | None = None
    target: str = "dev"

    # Database connection (for profile generation)
    database_type: Literal["postgres", "snowflake", "bigquery", "redshift", "databricks", "duckdb"] = "postgres"
    host: str | None = None
    port: int | None = None
    user: str | None = None
    password: str | None = None
    database: str | None = None
    schema: str = "public"

    # Snowflake specific
    account: str | None = None
    warehouse: str | None = None
    role: str | None = None

    # BigQuery specific
    project: str | None = None
    keyfile: str | None = None
    location: str = "US"

    # Databricks specific
    http_path: str | None = None
    token: str | None = None

    # Execution settings
    threads: int = 4
    full_refresh: bool = False
    fail_fast: bool = False

    # Model selection
    models: list[str] = field(default_factory=list)
    exclude: list[str] = field(default_factory=list)
    selector: str | None = None

    # Variables
    vars: dict[str, Any] = field(default_factory=dict)


class DbtIntegration:
    """
    dbt integration for SQL-based transformations.

    Features:
    - Run dbt models, tests, and snapshots
    - Generate dbt documentation
    - Parse manifest and catalog
    - Integrate with Automic ETL pipelines
    - Automatic profile generation

    Example:
        config = DbtConfig(
            project_dir="/path/to/dbt/project",
            target="prod",
            database_type="snowflake",
            account="xxx.snowflakecomputing.com",
            database="analytics"
        )

        dbt = DbtIntegration(config)

        # Run models
        result = dbt.run(models=["staging.*", "marts.orders"])

        # Run tests
        test_results = dbt.test()

        # Generate docs
        dbt.docs_generate()
    """

    def __init__(self, config: DbtConfig):
        self.config = config
        self.project_dir = Path(config.project_dir)
        self.profiles_dir = Path(config.profiles_dir) if config.profiles_dir else self.project_dir

        self._manifest: dict | None = None
        self._catalog: dict | None = None

    def setup_profile(self) -> Path:
        """
        Generate dbt profiles.yml based on configuration.

        Returns:
            Path to profiles.yml
        """
        profile_name = self._get_project_name()

        profile_config = {
            profile_name: {
                "target": self.config.target,
                "outputs": {
                    self.config.target: self._build_target_config()
                }
            }
        }

        profiles_path = self.profiles_dir / "profiles.yml"
        profiles_path.parent.mkdir(parents=True, exist_ok=True)

        with open(profiles_path, "w") as f:
            yaml.dump(profile_config, f, default_flow_style=False)

        logger.info(f"Generated dbt profile at {profiles_path}")
        return profiles_path

    def _get_project_name(self) -> str:
        """Get project name from dbt_project.yml."""
        project_file = self.project_dir / "dbt_project.yml"
        if project_file.exists():
            with open(project_file) as f:
                project = yaml.safe_load(f)
                return project.get("name", "automic_etl")
        return "automic_etl"

    def _build_target_config(self) -> dict[str, Any]:
        """Build target configuration based on database type."""
        base = {"threads": self.config.threads}

        if self.config.database_type == "postgres":
            return {
                **base,
                "type": "postgres",
                "host": self.config.host,
                "port": self.config.port or 5432,
                "user": self.config.user,
                "password": self.config.password,
                "dbname": self.config.database,
                "schema": self.config.schema,
            }

        elif self.config.database_type == "snowflake":
            return {
                **base,
                "type": "snowflake",
                "account": self.config.account,
                "user": self.config.user,
                "password": self.config.password,
                "database": self.config.database,
                "warehouse": self.config.warehouse,
                "schema": self.config.schema,
                "role": self.config.role,
            }

        elif self.config.database_type == "bigquery":
            config = {
                **base,
                "type": "bigquery",
                "method": "service-account" if self.config.keyfile else "oauth",
                "project": self.config.project,
                "dataset": self.config.schema,
                "location": self.config.location,
            }
            if self.config.keyfile:
                config["keyfile"] = self.config.keyfile
            return config

        elif self.config.database_type == "redshift":
            return {
                **base,
                "type": "redshift",
                "host": self.config.host,
                "port": self.config.port or 5439,
                "user": self.config.user,
                "password": self.config.password,
                "dbname": self.config.database,
                "schema": self.config.schema,
            }

        elif self.config.database_type == "databricks":
            return {
                **base,
                "type": "databricks",
                "host": self.config.host,
                "http_path": self.config.http_path,
                "token": self.config.token,
                "schema": self.config.schema,
            }

        elif self.config.database_type == "duckdb":
            return {
                **base,
                "type": "duckdb",
                "path": self.config.database or ":memory:",
                "schema": self.config.schema,
            }

        return base

    def _run_command(
        self,
        command: str,
        args: list[str] | None = None,
        capture_output: bool = True,
    ) -> dict[str, Any]:
        """
        Run a dbt command.

        Args:
            command: dbt command (run, test, build, etc.)
            args: Additional arguments
            capture_output: Whether to capture output

        Returns:
            Command result with status, output, and timing
        """
        import time

        cmd = ["dbt", command]

        # Add project directory
        cmd.extend(["--project-dir", str(self.project_dir)])

        # Add profiles directory
        if self.config.profiles_dir:
            cmd.extend(["--profiles-dir", str(self.profiles_dir)])

        # Add target
        cmd.extend(["--target", self.config.target])

        # Add model selection
        if self.config.models:
            cmd.extend(["--select", " ".join(self.config.models)])

        if self.config.exclude:
            cmd.extend(["--exclude", " ".join(self.config.exclude)])

        if self.config.selector:
            cmd.extend(["--selector", self.config.selector])

        # Add variables
        if self.config.vars:
            cmd.extend(["--vars", json.dumps(self.config.vars)])

        # Add flags
        if self.config.full_refresh and command in ("run", "build"):
            cmd.append("--full-refresh")

        if self.config.fail_fast:
            cmd.append("--fail-fast")

        # Add additional args
        if args:
            cmd.extend(args)

        logger.info(f"Running dbt command: {' '.join(cmd)}")

        start_time = time.time()

        try:
            result = subprocess.run(
                cmd,
                cwd=str(self.project_dir),
                capture_output=capture_output,
                text=True,
                env={**os.environ, "DBT_PROFILES_DIR": str(self.profiles_dir)},
            )

            elapsed = time.time() - start_time

            return {
                "success": result.returncode == 0,
                "return_code": result.returncode,
                "stdout": result.stdout if capture_output else None,
                "stderr": result.stderr if capture_output else None,
                "elapsed_seconds": elapsed,
                "command": " ".join(cmd),
            }

        except FileNotFoundError:
            raise RuntimeError(
                "dbt CLI not found. Install with: pip install dbt-core dbt-<adapter>"
            )

    def run(
        self,
        models: list[str] | None = None,
        full_refresh: bool = False,
    ) -> dict[str, Any]:
        """
        Run dbt models.

        Args:
            models: Models to run (overrides config)
            full_refresh: Force full refresh

        Returns:
            Run results
        """
        if models:
            self.config.models = models
        if full_refresh:
            self.config.full_refresh = full_refresh

        result = self._run_command("run")
        self._parse_run_results(result)
        return result

    def test(
        self,
        models: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Run dbt tests.

        Args:
            models: Models to test

        Returns:
            Test results
        """
        if models:
            self.config.models = models

        result = self._run_command("test")
        self._parse_test_results(result)
        return result

    def build(
        self,
        models: list[str] | None = None,
        full_refresh: bool = False,
    ) -> dict[str, Any]:
        """
        Run dbt build (run + test).

        Args:
            models: Models to build
            full_refresh: Force full refresh

        Returns:
            Build results
        """
        if models:
            self.config.models = models
        if full_refresh:
            self.config.full_refresh = full_refresh

        return self._run_command("build")

    def snapshot(self) -> dict[str, Any]:
        """Run dbt snapshots."""
        return self._run_command("snapshot")

    def seed(self, full_refresh: bool = False) -> dict[str, Any]:
        """
        Load seed data.

        Args:
            full_refresh: Force full refresh of seeds
        """
        args = ["--full-refresh"] if full_refresh else []
        return self._run_command("seed", args)

    def deps(self) -> dict[str, Any]:
        """Install dbt dependencies."""
        return self._run_command("deps")

    def compile(self, models: list[str] | None = None) -> dict[str, Any]:
        """
        Compile dbt models without running.

        Args:
            models: Models to compile

        Returns:
            Compilation results
        """
        if models:
            self.config.models = models
        return self._run_command("compile")

    def docs_generate(self) -> dict[str, Any]:
        """Generate dbt documentation."""
        return self._run_command("docs", ["generate"])

    def docs_serve(self, port: int = 8080) -> None:
        """
        Serve dbt documentation.

        Args:
            port: Port to serve on
        """
        self._run_command("docs", ["serve", "--port", str(port)], capture_output=False)

    def debug(self) -> dict[str, Any]:
        """Run dbt debug to check configuration."""
        return self._run_command("debug")

    def clean(self) -> dict[str, Any]:
        """Clean dbt artifacts."""
        return self._run_command("clean")

    def _parse_run_results(self, result: dict[str, Any]) -> None:
        """Parse run results and extract statistics."""
        run_results_path = self.project_dir / "target" / "run_results.json"
        if run_results_path.exists():
            with open(run_results_path) as f:
                run_results = json.load(f)
                result["run_results"] = {
                    "elapsed_time": run_results.get("elapsed_time"),
                    "results": [
                        {
                            "unique_id": r["unique_id"],
                            "status": r["status"],
                            "execution_time": r.get("execution_time"),
                            "rows_affected": r.get("adapter_response", {}).get("rows_affected"),
                        }
                        for r in run_results.get("results", [])
                    ]
                }

    def _parse_test_results(self, result: dict[str, Any]) -> None:
        """Parse test results."""
        run_results_path = self.project_dir / "target" / "run_results.json"
        if run_results_path.exists():
            with open(run_results_path) as f:
                run_results = json.load(f)

                passed = sum(1 for r in run_results.get("results", []) if r["status"] == "pass")
                failed = sum(1 for r in run_results.get("results", []) if r["status"] == "fail")
                warned = sum(1 for r in run_results.get("results", []) if r["status"] == "warn")
                errored = sum(1 for r in run_results.get("results", []) if r["status"] == "error")

                result["test_summary"] = {
                    "passed": passed,
                    "failed": failed,
                    "warned": warned,
                    "errored": errored,
                    "total": passed + failed + warned + errored,
                }

    def get_manifest(self) -> dict[str, Any]:
        """
        Get dbt manifest (compiled project metadata).

        Returns:
            Manifest dictionary
        """
        if self._manifest:
            return self._manifest

        manifest_path = self.project_dir / "target" / "manifest.json"
        if not manifest_path.exists():
            self.compile()

        with open(manifest_path) as f:
            self._manifest = json.load(f)

        return self._manifest

    def get_catalog(self) -> dict[str, Any]:
        """
        Get dbt catalog (database metadata).

        Returns:
            Catalog dictionary
        """
        if self._catalog:
            return self._catalog

        catalog_path = self.project_dir / "target" / "catalog.json"
        if not catalog_path.exists():
            self.docs_generate()

        if catalog_path.exists():
            with open(catalog_path) as f:
                self._catalog = json.load(f)

        return self._catalog or {}

    def get_models(self) -> list[dict[str, Any]]:
        """
        Get list of all models in the project.

        Returns:
            List of model metadata
        """
        manifest = self.get_manifest()
        models = []

        for node_id, node in manifest.get("nodes", {}).items():
            if node["resource_type"] == "model":
                models.append({
                    "unique_id": node_id,
                    "name": node["name"],
                    "schema": node["schema"],
                    "database": node.get("database"),
                    "materialization": node["config"].get("materialized"),
                    "description": node.get("description"),
                    "tags": node.get("tags", []),
                    "depends_on": node.get("depends_on", {}).get("nodes", []),
                })

        return models

    def get_sources(self) -> list[dict[str, Any]]:
        """
        Get list of all sources in the project.

        Returns:
            List of source metadata
        """
        manifest = self.get_manifest()
        sources = []

        for source_id, source in manifest.get("sources", {}).items():
            sources.append({
                "unique_id": source_id,
                "name": source["name"],
                "source_name": source["source_name"],
                "schema": source["schema"],
                "database": source.get("database"),
                "description": source.get("description"),
                "loaded_at_field": source.get("loaded_at_field"),
                "freshness": source.get("freshness"),
            })

        return sources

    def get_tests(self) -> list[dict[str, Any]]:
        """
        Get list of all tests in the project.

        Returns:
            List of test metadata
        """
        manifest = self.get_manifest()
        tests = []

        for node_id, node in manifest.get("nodes", {}).items():
            if node["resource_type"] == "test":
                tests.append({
                    "unique_id": node_id,
                    "name": node["name"],
                    "test_metadata": node.get("test_metadata"),
                    "depends_on": node.get("depends_on", {}).get("nodes", []),
                })

        return tests

    def get_lineage(self, model_name: str) -> dict[str, Any]:
        """
        Get lineage for a specific model.

        Args:
            model_name: Name of the model

        Returns:
            Lineage information (upstream and downstream)
        """
        manifest = self.get_manifest()

        # Find the model
        model_id = None
        for node_id, node in manifest.get("nodes", {}).items():
            if node["resource_type"] == "model" and node["name"] == model_name:
                model_id = node_id
                break

        if not model_id:
            return {"error": f"Model '{model_name}' not found"}

        model = manifest["nodes"][model_id]

        # Get upstream (dependencies)
        upstream = model.get("depends_on", {}).get("nodes", [])

        # Get downstream (what depends on this model)
        downstream = []
        for node_id, node in manifest.get("nodes", {}).items():
            if model_id in node.get("depends_on", {}).get("nodes", []):
                downstream.append(node_id)

        return {
            "model": model_name,
            "model_id": model_id,
            "upstream": upstream,
            "downstream": downstream,
        }

    def create_model(
        self,
        name: str,
        sql: str,
        schema_yml: dict | None = None,
        materialized: str = "view",
        tags: list[str] | None = None,
    ) -> Path:
        """
        Create a new dbt model file.

        Args:
            name: Model name
            sql: SQL for the model
            schema_yml: Optional schema documentation
            materialized: Materialization type
            tags: Tags for the model

        Returns:
            Path to created model
        """
        models_dir = self.project_dir / "models"
        models_dir.mkdir(parents=True, exist_ok=True)

        # Build config block
        config_parts = [f"materialized='{materialized}'"]
        if tags:
            config_parts.append(f"tags={tags}")

        config_block = "{{ config(" + ", ".join(config_parts) + ") }}"

        # Write model file
        model_path = models_dir / f"{name}.sql"
        with open(model_path, "w") as f:
            f.write(f"{config_block}\n\n{sql}")

        logger.info(f"Created dbt model: {model_path}")

        # Write schema if provided
        if schema_yml:
            schema_path = models_dir / "schema.yml"
            existing = {}
            if schema_path.exists():
                with open(schema_path) as f:
                    existing = yaml.safe_load(f) or {}

            if "models" not in existing:
                existing["models"] = []

            # Add or update model schema
            existing["models"] = [
                m for m in existing["models"] if m.get("name") != name
            ]
            existing["models"].append(schema_yml)

            with open(schema_path, "w") as f:
                yaml.dump(existing, f, default_flow_style=False)

        return model_path
