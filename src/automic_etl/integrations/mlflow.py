"""MLflow integration for ML model tracking and deployment."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal
import json

logger = logging.getLogger(__name__)


@dataclass
class MLflowConfig:
    """MLflow configuration."""

    tracking_uri: str = "http://localhost:5000"
    experiment_name: str = "automic-etl-ml"
    artifact_location: str | None = None

    # Registry settings
    registry_uri: str | None = None
    model_name: str | None = None

    # Authentication
    username: str | None = None
    password: str | None = None
    token: str | None = None

    # Run settings
    run_name: str | None = None
    tags: dict[str, str] = field(default_factory=dict)
    nested: bool = False

    # Autolog settings
    autolog: bool = True
    log_models: bool = True
    log_datasets: bool = True


class MLflowIntegration:
    """
    MLflow integration for tracking ML experiments and models.

    Features:
    - Experiment tracking
    - Model registry
    - Artifact logging
    - Auto-logging for popular frameworks
    - Integration with Automic ETL pipelines

    Example:
        config = MLflowConfig(
            tracking_uri="http://mlflow.example.com:5000",
            experiment_name="sales_forecasting"
        )

        mlflow_int = MLflowIntegration(config)

        # Track a run
        with mlflow_int.start_run("training_run"):
            mlflow_int.log_params({"learning_rate": 0.01})
            mlflow_int.log_metrics({"accuracy": 0.95})
            mlflow_int.log_model(model, "model")

        # Register model
        mlflow_int.register_model("runs:/run_id/model", "my_model")
    """

    def __init__(self, config: MLflowConfig):
        self.config = config
        self._client = None
        self._experiment_id = None
        self._active_run = None

    def _ensure_mlflow(self):
        """Ensure MLflow is available."""
        try:
            import mlflow
            return mlflow
        except ImportError:
            raise ImportError(
                "mlflow is required. Install with: pip install mlflow"
            )

    @property
    def client(self):
        """Get MLflow client."""
        if self._client is None:
            mlflow = self._ensure_mlflow()
            from mlflow.tracking import MlflowClient

            mlflow.set_tracking_uri(self.config.tracking_uri)

            if self.config.registry_uri:
                mlflow.set_registry_uri(self.config.registry_uri)

            self._client = MlflowClient(self.config.tracking_uri)

        return self._client

    def setup(self) -> str:
        """
        Set up MLflow tracking.

        Returns:
            Experiment ID
        """
        mlflow = self._ensure_mlflow()
        mlflow.set_tracking_uri(self.config.tracking_uri)

        # Create or get experiment
        experiment = mlflow.get_experiment_by_name(self.config.experiment_name)
        if experiment:
            self._experiment_id = experiment.experiment_id
        else:
            self._experiment_id = mlflow.create_experiment(
                self.config.experiment_name,
                artifact_location=self.config.artifact_location,
                tags=self.config.tags,
            )

        mlflow.set_experiment(self.config.experiment_name)

        # Setup autolog if enabled
        if self.config.autolog:
            mlflow.autolog(
                log_models=self.config.log_models,
                log_datasets=self.config.log_datasets,
            )

        logger.info(f"MLflow setup complete. Experiment ID: {self._experiment_id}")
        return self._experiment_id

    def start_run(
        self,
        run_name: str | None = None,
        tags: dict[str, str] | None = None,
        nested: bool | None = None,
    ):
        """
        Start an MLflow run.

        Args:
            run_name: Name for the run
            tags: Tags to add to the run
            nested: Whether this is a nested run

        Returns:
            Context manager for the run
        """
        mlflow = self._ensure_mlflow()

        if self._experiment_id is None:
            self.setup()

        run_name = run_name or self.config.run_name
        nested = nested if nested is not None else self.config.nested

        all_tags = {**self.config.tags, **(tags or {})}

        self._active_run = mlflow.start_run(
            run_name=run_name,
            experiment_id=self._experiment_id,
            tags=all_tags,
            nested=nested,
        )

        return self._active_run

    def end_run(self, status: str = "FINISHED") -> None:
        """End the current run."""
        mlflow = self._ensure_mlflow()
        mlflow.end_run(status=status)
        self._active_run = None

    def log_param(self, key: str, value: Any) -> None:
        """Log a parameter."""
        mlflow = self._ensure_mlflow()
        mlflow.log_param(key, value)

    def log_params(self, params: dict[str, Any]) -> None:
        """Log multiple parameters."""
        mlflow = self._ensure_mlflow()
        mlflow.log_params(params)

    def log_metric(self, key: str, value: float, step: int | None = None) -> None:
        """Log a metric."""
        mlflow = self._ensure_mlflow()
        mlflow.log_metric(key, value, step=step)

    def log_metrics(
        self,
        metrics: dict[str, float],
        step: int | None = None,
    ) -> None:
        """Log multiple metrics."""
        mlflow = self._ensure_mlflow()
        mlflow.log_metrics(metrics, step=step)

    def log_artifact(self, local_path: str, artifact_path: str | None = None) -> None:
        """Log an artifact file."""
        mlflow = self._ensure_mlflow()
        mlflow.log_artifact(local_path, artifact_path)

    def log_artifacts(self, local_dir: str, artifact_path: str | None = None) -> None:
        """Log all artifacts in a directory."""
        mlflow = self._ensure_mlflow()
        mlflow.log_artifacts(local_dir, artifact_path)

    def log_dict(self, dictionary: dict, artifact_file: str) -> None:
        """Log a dictionary as a JSON artifact."""
        mlflow = self._ensure_mlflow()
        mlflow.log_dict(dictionary, artifact_file)

    def log_text(self, text: str, artifact_file: str) -> None:
        """Log text as an artifact."""
        mlflow = self._ensure_mlflow()
        mlflow.log_text(text, artifact_file)

    def log_figure(self, figure: Any, artifact_file: str) -> None:
        """Log a matplotlib figure."""
        mlflow = self._ensure_mlflow()
        mlflow.log_figure(figure, artifact_file)

    def log_model(
        self,
        model: Any,
        artifact_path: str,
        flavor: str = "sklearn",
        registered_model_name: str | None = None,
        **kwargs,
    ) -> None:
        """
        Log a model.

        Args:
            model: Model to log
            artifact_path: Path within artifacts
            flavor: Model flavor (sklearn, pytorch, tensorflow, etc.)
            registered_model_name: Name to register model under
            **kwargs: Additional arguments for the flavor
        """
        mlflow = self._ensure_mlflow()

        # Get the appropriate model flavor
        if flavor == "sklearn":
            import mlflow.sklearn
            mlflow.sklearn.log_model(
                model, artifact_path,
                registered_model_name=registered_model_name,
                **kwargs
            )
        elif flavor == "pytorch":
            import mlflow.pytorch
            mlflow.pytorch.log_model(
                model, artifact_path,
                registered_model_name=registered_model_name,
                **kwargs
            )
        elif flavor == "tensorflow":
            import mlflow.tensorflow
            mlflow.tensorflow.log_model(
                model, artifact_path,
                registered_model_name=registered_model_name,
                **kwargs
            )
        elif flavor == "xgboost":
            import mlflow.xgboost
            mlflow.xgboost.log_model(
                model, artifact_path,
                registered_model_name=registered_model_name,
                **kwargs
            )
        elif flavor == "lightgbm":
            import mlflow.lightgbm
            mlflow.lightgbm.log_model(
                model, artifact_path,
                registered_model_name=registered_model_name,
                **kwargs
            )
        elif flavor == "pyfunc":
            import mlflow.pyfunc
            mlflow.pyfunc.log_model(
                artifact_path, python_model=model,
                registered_model_name=registered_model_name,
                **kwargs
            )
        else:
            raise ValueError(f"Unknown model flavor: {flavor}")

    def log_input(
        self,
        dataset: Any,
        context: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Log an input dataset."""
        mlflow = self._ensure_mlflow()
        from mlflow.data.pandas_dataset import PandasDataset

        if hasattr(dataset, "to_pandas"):
            dataset = dataset.to_pandas()

        mlflow_dataset = mlflow.data.from_pandas(dataset)
        mlflow.log_input(mlflow_dataset, context=context, tags=tags)

    def set_tag(self, key: str, value: str) -> None:
        """Set a tag on the current run."""
        mlflow = self._ensure_mlflow()
        mlflow.set_tag(key, value)

    def set_tags(self, tags: dict[str, str]) -> None:
        """Set multiple tags."""
        mlflow = self._ensure_mlflow()
        mlflow.set_tags(tags)

    # ========================
    # Model Registry
    # ========================

    def register_model(
        self,
        model_uri: str,
        name: str,
        tags: dict[str, str] | None = None,
    ) -> Any:
        """
        Register a model in the registry.

        Args:
            model_uri: URI of the model (e.g., "runs:/run_id/model")
            name: Name for the registered model
            tags: Tags to add

        Returns:
            Registered model version
        """
        mlflow = self._ensure_mlflow()
        mv = mlflow.register_model(model_uri, name, tags=tags)
        logger.info(f"Registered model {name} version {mv.version}")
        return mv

    def get_model_version(self, name: str, version: str) -> Any:
        """Get a specific model version."""
        return self.client.get_model_version(name, version)

    def get_latest_model_versions(
        self,
        name: str,
        stages: list[str] | None = None,
    ) -> list[Any]:
        """Get latest model versions."""
        return self.client.get_latest_versions(name, stages=stages)

    def transition_model_version_stage(
        self,
        name: str,
        version: str,
        stage: Literal["Staging", "Production", "Archived", "None"],
        archive_existing_versions: bool = False,
    ) -> Any:
        """
        Transition a model version to a new stage.

        Args:
            name: Model name
            version: Model version
            stage: Target stage
            archive_existing_versions: Archive existing versions in target stage

        Returns:
            Updated model version
        """
        return self.client.transition_model_version_stage(
            name, version, stage,
            archive_existing_versions=archive_existing_versions
        )

    def load_model(
        self,
        model_uri: str,
        flavor: str = "pyfunc",
    ) -> Any:
        """
        Load a model.

        Args:
            model_uri: Model URI
            flavor: Model flavor

        Returns:
            Loaded model
        """
        mlflow = self._ensure_mlflow()

        if flavor == "pyfunc":
            return mlflow.pyfunc.load_model(model_uri)
        elif flavor == "sklearn":
            import mlflow.sklearn
            return mlflow.sklearn.load_model(model_uri)
        elif flavor == "pytorch":
            import mlflow.pytorch
            return mlflow.pytorch.load_model(model_uri)
        elif flavor == "tensorflow":
            import mlflow.tensorflow
            return mlflow.tensorflow.load_model(model_uri)
        elif flavor == "xgboost":
            import mlflow.xgboost
            return mlflow.xgboost.load_model(model_uri)
        elif flavor == "lightgbm":
            import mlflow.lightgbm
            return mlflow.lightgbm.load_model(model_uri)
        else:
            raise ValueError(f"Unknown flavor: {flavor}")

    # ========================
    # Experiment Management
    # ========================

    def list_experiments(self) -> list[Any]:
        """List all experiments."""
        return self.client.search_experiments()

    def get_experiment(self, experiment_id: str) -> Any:
        """Get experiment by ID."""
        return self.client.get_experiment(experiment_id)

    def delete_experiment(self, experiment_id: str) -> None:
        """Delete an experiment."""
        self.client.delete_experiment(experiment_id)

    def search_runs(
        self,
        experiment_ids: list[str] | None = None,
        filter_string: str = "",
        max_results: int = 1000,
        order_by: list[str] | None = None,
    ) -> list[Any]:
        """
        Search for runs.

        Args:
            experiment_ids: Experiment IDs to search
            filter_string: Filter expression
            max_results: Maximum results
            order_by: Order by clauses

        Returns:
            List of matching runs
        """
        mlflow = self._ensure_mlflow()

        if experiment_ids is None and self._experiment_id:
            experiment_ids = [self._experiment_id]

        return mlflow.search_runs(
            experiment_ids=experiment_ids,
            filter_string=filter_string,
            max_results=max_results,
            order_by=order_by,
        )

    def get_run(self, run_id: str) -> Any:
        """Get a run by ID."""
        return self.client.get_run(run_id)

    def delete_run(self, run_id: str) -> None:
        """Delete a run."""
        self.client.delete_run(run_id)

    # ========================
    # ETL Pipeline Integration
    # ========================

    def log_pipeline_run(
        self,
        pipeline_name: str,
        config: dict[str, Any],
        metrics: dict[str, float],
        input_data: Any = None,
        output_data: Any = None,
        artifacts: dict[str, str] | None = None,
    ) -> str:
        """
        Log an Automic ETL pipeline run.

        Args:
            pipeline_name: Name of the pipeline
            config: Pipeline configuration
            metrics: Pipeline metrics
            input_data: Optional input data to log
            output_data: Optional output data to log
            artifacts: Optional artifacts to log

        Returns:
            Run ID
        """
        with self.start_run(run_name=pipeline_name) as run:
            # Log parameters
            self.log_params({
                "pipeline_name": pipeline_name,
                **{f"config.{k}": str(v) for k, v in config.items()},
            })

            # Log metrics
            self.log_metrics(metrics)

            # Log tags
            self.set_tags({
                "pipeline_type": "automic_etl",
                "pipeline_name": pipeline_name,
            })

            # Log input data
            if input_data is not None:
                self.log_input(input_data, context="input")

            # Log output data
            if output_data is not None:
                self.log_dict(
                    {"sample": output_data.head(100).to_dict() if hasattr(output_data, 'head') else output_data[:100]},
                    "output_sample.json"
                )

            # Log artifacts
            if artifacts:
                for name, path in artifacts.items():
                    self.log_artifact(path, name)

            return run.info.run_id

    def track_data_quality(
        self,
        quality_results: dict[str, Any],
        run_name: str = "data_quality_check",
    ) -> str:
        """
        Track data quality metrics.

        Args:
            quality_results: Results from data quality checks
            run_name: Name for the tracking run

        Returns:
            Run ID
        """
        with self.start_run(run_name=run_name) as run:
            # Log overall score
            if "score" in quality_results:
                self.log_metric("quality_score", quality_results["score"])

            # Log individual metrics
            if "metrics" in quality_results:
                for metric_name, value in quality_results["metrics"].items():
                    if isinstance(value, (int, float)):
                        self.log_metric(f"quality.{metric_name}", value)

            # Log failed checks
            if "failed_checks" in quality_results:
                self.log_dict(
                    {"failed_checks": quality_results["failed_checks"]},
                    "failed_checks.json"
                )

            # Log tags
            self.set_tags({
                "check_type": "data_quality",
                "passed": str(quality_results.get("passed", False)),
            })

            return run.info.run_id

    def compare_runs(
        self,
        run_ids: list[str],
        metrics: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Compare multiple runs.

        Args:
            run_ids: List of run IDs to compare
            metrics: Specific metrics to compare

        Returns:
            Comparison results
        """
        comparison = {"runs": {}}

        for run_id in run_ids:
            run = self.get_run(run_id)
            run_data = {
                "run_id": run_id,
                "run_name": run.info.run_name,
                "status": run.info.status,
                "start_time": run.info.start_time,
                "end_time": run.info.end_time,
                "params": dict(run.data.params),
                "metrics": dict(run.data.metrics),
            }

            if metrics:
                run_data["metrics"] = {
                    k: v for k, v in run_data["metrics"].items()
                    if k in metrics
                }

            comparison["runs"][run_id] = run_data

        # Calculate best run for each metric
        if comparison["runs"]:
            all_metrics = set()
            for run_data in comparison["runs"].values():
                all_metrics.update(run_data["metrics"].keys())

            comparison["best_by_metric"] = {}
            for metric in all_metrics:
                values = [
                    (run_id, data["metrics"].get(metric))
                    for run_id, data in comparison["runs"].items()
                    if data["metrics"].get(metric) is not None
                ]
                if values:
                    best = max(values, key=lambda x: x[1])
                    comparison["best_by_metric"][metric] = {
                        "run_id": best[0],
                        "value": best[1],
                    }

        return comparison
