"""MLflow Experiment Sensor Component.

Polls an MLflow experiment for new runs (optionally filtered by status/tags)
and fires a Dagster RunRequest per new run. Cursor-based deduplication on
run_id — only genuinely new runs trigger.

Right for teams that want a Dagster job to fire whenever a data scientist
finishes a training run (evaluation pipeline, downstream feature computation,
scheduled promotion review, etc.).
"""

import os
from typing import Optional

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
    Resolvable,
    Model,
)
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class MLflowExperimentSensorComponent(Component, Model, Resolvable):
    """Fires a RunRequest for each new MLflow experiment run.

    Cursor-based on `run_id` set: only unseen run_ids trigger the target job.
    Injects run metadata (run_id, experiment_id, status, artifact_uri, tags)
    into the triggered run's config so downstream ops can read + act on it.

    Example:

        ```yaml
        type: dagster_community_components.MLflowExperimentSensorComponent
        attributes:
          sensor_name: churn_experiment_runs
          tracking_uri_env_var: MLFLOW_TRACKING_URI
          experiment_name: churn_prediction
          filter_string: "attributes.status = 'FINISHED'"
          target_job: evaluate_new_run
          minimum_interval_seconds: 60
        ```

    Example (auth + tags filter):

        ```yaml
        type: dagster_community_components.MLflowExperimentSensorComponent
        attributes:
          sensor_name: prod_candidates
          tracking_uri_env_var: MLFLOW_TRACKING_URI
          username_env_var: MLFLOW_USERNAME
          password_env_var: MLFLOW_PASSWORD
          experiment_name: churn_prediction
          filter_string: "attributes.status = 'FINISHED' and tags.candidate = 'true'"
          target_job: evaluate_new_run
        ```
    """

    sensor_name: str = Field(description="Unique name for this sensor.")

    tracking_uri_env_var: str = Field(
        description="Env var holding the MLflow Tracking URI (e.g. MLFLOW_TRACKING_URI)."
    )

    experiment_name: Optional[str] = Field(
        default=None,
        description="MLflow experiment name to watch. Provide this OR experiment_id.",
    )

    experiment_id: Optional[str] = Field(
        default=None,
        description="MLflow experiment ID to watch. Provide this OR experiment_name.",
    )

    filter_string: Optional[str] = Field(
        default=None,
        description=(
            "MLflow search_runs filter string. E.g. \"attributes.status = 'FINISHED'\" "
            "or \"tags.candidate = 'true'\". Passed directly to MlflowClient.search_runs."
        ),
    )

    target_job: str = Field(description="Dagster job to trigger for each new run.")

    minimum_interval_seconds: int = Field(
        default=60,
        description="Minimum seconds between sensor evaluations.",
    )

    max_new_runs_per_tick: int = Field(
        default=25,
        description=(
            "Cap on RunRequests emitted per sensor tick. Guardrail against a "
            "burst backfill of historical runs on first activation."
        ),
    )

    username_env_var: Optional[str] = Field(
        default=None, description="Env var for HTTP-basic MLflow auth username (optional)."
    )
    password_env_var: Optional[str] = Field(
        default=None, description="Env var for HTTP-basic MLflow auth password (optional)."
    )

    run_config: Optional[dict] = Field(
        default=None,
        description=(
            "Extra run_config merged into the triggered job's config. Sensor injects "
            "the run metadata under `ops.config` — this field overlays anything else."
        ),
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        # Close over field values (avoid self references in the sensor body).
        sensor_name = self.sensor_name
        tracking_uri_env_var = self.tracking_uri_env_var
        experiment_name = self.experiment_name
        experiment_id = self.experiment_id
        filter_string = self.filter_string
        target_job = self.target_job
        minimum_interval_seconds = self.minimum_interval_seconds
        max_new_runs_per_tick = self.max_new_runs_per_tick
        username_env_var = self.username_env_var
        password_env_var = self.password_env_var
        extra_run_config = self.run_config or {}

        if not experiment_name and not experiment_id:
            raise ValueError(
                "MLflowExperimentSensorComponent requires either `experiment_name` or `experiment_id`."
            )

        @sensor(
            name=sensor_name,
            minimum_interval_seconds=minimum_interval_seconds,
            default_status=DefaultSensorStatus.RUNNING,
            job_name=target_job,
        )
        def mlflow_experiment_sensor(context: SensorEvaluationContext):
            try:
                import mlflow
            except ImportError:
                return SensorResult(skip_reason="mlflow is not installed. Run: pip install mlflow>=2.0.0")

            tracking_uri = os.environ.get(tracking_uri_env_var)
            if not tracking_uri:
                return SensorResult(
                    skip_reason=f"Env var {tracking_uri_env_var!r} is not set."
                )

            if username_env_var and password_env_var:
                os.environ["MLFLOW_TRACKING_USERNAME"] = os.environ.get(username_env_var, "")
                os.environ["MLFLOW_TRACKING_PASSWORD"] = os.environ.get(password_env_var, "")

            client = mlflow.tracking.MlflowClient(tracking_uri=tracking_uri)

            resolved_exp_id = experiment_id
            if not resolved_exp_id:
                try:
                    exp = client.get_experiment_by_name(experiment_name)
                except Exception as e:  # noqa: BLE001
                    return SensorResult(skip_reason=f"Failed to resolve experiment: {e}")
                if not exp:
                    return SensorResult(
                        skip_reason=f"Experiment {experiment_name!r} not found in MLflow."
                    )
                resolved_exp_id = exp.experiment_id

            # Cursor: comma-separated set of already-seen run_ids (capped size).
            seen = set((context.cursor or "").split(",")) - {""}

            try:
                runs = client.search_runs(
                    experiment_ids=[resolved_exp_id],
                    filter_string=filter_string or "",
                    max_results=max_new_runs_per_tick * 4,  # over-fetch, filter to new
                    order_by=["attributes.end_time DESC"],
                )
            except Exception as e:  # noqa: BLE001
                context.log.error(f"MLflow search_runs failed: {e}")
                return SensorResult(skip_reason=f"MLflow query failed: {e}")

            new_runs = [r for r in runs if r.info.run_id not in seen][:max_new_runs_per_tick]
            if not new_runs:
                return SensorResult(skip_reason="no new experiment runs")

            run_requests = []
            for r in new_runs:
                ops_config = {
                    "mlflow_run_id": r.info.run_id,
                    "mlflow_experiment_id": r.info.experiment_id,
                    "mlflow_status": r.info.status,
                    "mlflow_artifact_uri": r.info.artifact_uri,
                    "mlflow_start_time": r.info.start_time,
                    "mlflow_end_time": r.info.end_time,
                    "mlflow_tags": dict(r.data.tags or {}),
                }
                ops_config.update((extra_run_config.get("ops") or {}).get("config") or {})
                run_config = dict(extra_run_config)
                run_config["ops"] = {"config": ops_config}
                run_requests.append(
                    RunRequest(
                        run_key=f"{resolved_exp_id}:{r.info.run_id}",
                        run_config=run_config,
                        tags={
                            "mlflow_experiment_id": resolved_exp_id,
                            "mlflow_run_id": r.info.run_id,
                            "mlflow_status": r.info.status,
                        },
                    )
                )

            # Cap seen cursor to last 500 run_ids to avoid unbounded growth.
            all_seen = list(seen)[-500:] + [r.info.run_id for r in new_runs]
            new_cursor = ",".join(sorted(set(all_seen))[-500:])

            return SensorResult(run_requests=run_requests, cursor=new_cursor)

        return Definitions(sensors=[mlflow_experiment_sensor])
