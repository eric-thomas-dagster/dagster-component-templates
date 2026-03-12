"""MLflow Model Sensor Component.

Monitors an MLflow Model Registry for model version transitions to a specified
stage (e.g., "Production") and triggers Dagster jobs when new versions appear.
Enables model-driven pipeline triggers: when a data scientist promotes a model,
downstream scoring or inference pipelines automatically run.
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


class MLflowModelSensor(Component, Model, Resolvable):
    """Component that fires a RunRequest when an MLflow model transitions to a
    specified stage in the Model Registry.

    Uses cursor-based deduplication so only genuinely new version promotions
    trigger a run — re-evaluations of the same version are ignored.

    Example:
        ```yaml
        type: dagster_component_templates.MLflowModelSensor
        attributes:
          sensor_name: production_model_trigger
          tracking_uri_env_var: MLFLOW_TRACKING_URI
          model_name: customer_churn_model
          target_stage: Production
          target_job: run_churn_scoring_pipeline
          minimum_interval_seconds: 120
        ```
    """

    sensor_name: str = Field(
        description="Unique name for this sensor."
    )

    tracking_uri_env_var: str = Field(
        description=(
            "Name of the environment variable that holds the MLflow Tracking URI "
            "(e.g., MLFLOW_TRACKING_URI).  The variable must be set at runtime."
        )
    )

    model_name: str = Field(
        description="Registered model name to watch in the MLflow Model Registry."
    )

    target_stage: str = Field(
        default="Production",
        description=(
            "Model Registry stage transition that triggers a run. "
            "Common values: 'Production', 'Staging', 'Archived'."
        ),
    )

    target_job: str = Field(
        description="Name of the Dagster job to trigger when a new version is detected."
    )

    minimum_interval_seconds: int = Field(
        default=60,
        description="Minimum time (in seconds) between sensor evaluations.",
    )

    run_config: Optional[dict] = Field(
        default=None,
        description=(
            "Optional extra run config merged into the triggered job's run_config. "
            "Keys are merged under the top-level 'ops' key alongside the model metadata "
            "injected by the sensor."
        ),
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        # Capture all fields into local variables for the closure.
        sensor_name = self.sensor_name
        tracking_uri_env_var = self.tracking_uri_env_var
        model_name = self.model_name
        target_stage = self.target_stage
        target_job = self.target_job
        minimum_interval_seconds = self.minimum_interval_seconds
        extra_run_config = self.run_config or {}

        @sensor(
            name=sensor_name,
            minimum_interval_seconds=minimum_interval_seconds,
            default_status=DefaultSensorStatus.RUNNING,
            job_name=target_job,
        )
        def mlflow_model_sensor(context: SensorEvaluationContext):
            """Sensor that polls MLflow for new model versions in the target stage."""
            try:
                import mlflow
            except ImportError:
                return SensorResult(
                    skip_reason="mlflow is not installed. Run: pip install mlflow>=2.0.0"
                )

            # Resolve tracking URI from the environment at evaluation time.
            tracking_uri = os.environ.get(tracking_uri_env_var)
            if not tracking_uri:
                return SensorResult(
                    skip_reason=(
                        f"Environment variable '{tracking_uri_env_var}' is not set. "
                        "Cannot connect to MLflow Tracking Server."
                    )
                )

            # The cursor stores the highest model version number already processed.
            # Stored as a plain integer string, e.g. "7".
            last_seen_version = int(context.cursor or "0")

            try:
                client = mlflow.tracking.MlflowClient(tracking_uri=tracking_uri)
                versions = client.get_latest_versions(model_name, stages=[target_stage])
            except Exception as e:
                context.log.error(f"Failed to query MLflow Model Registry: {e}")
                return SensorResult(
                    skip_reason=f"MLflow query failed: {e}"
                )

            if not versions:
                return SensorResult(
                    skip_reason=(
                        f"No versions of '{model_name}' found in stage '{target_stage}'."
                    )
                )

            run_requests = []
            new_max_version = last_seen_version

            for mv in versions:
                version = int(mv.version)
                stage = mv.current_stage

                if version <= last_seen_version:
                    # Already processed this version — skip.
                    continue

                model_uri = f"models:/{model_name}/{version}"

                # Build run_config: merge sensor-injected metadata with any user config.
                merged_ops_config = {
                    "model_name": model_name,
                    "model_version": str(version),
                    "model_uri": model_uri,
                    "stage": stage,
                }
                merged_ops_config.update(
                    extra_run_config.get("ops", {}).get("config", {})
                )

                final_run_config = dict(extra_run_config)
                final_run_config["ops"] = {"config": merged_ops_config}

                run_requests.append(
                    RunRequest(
                        run_key=f"{model_name}-v{version}-{stage}",
                        run_config=final_run_config,
                        tags={
                            "mlflow_model_name": model_name,
                            "mlflow_model_version": str(version),
                            "mlflow_stage": stage,
                        },
                    )
                )

                new_max_version = max(new_max_version, version)

            if run_requests:
                context.log.info(
                    f"Found {len(run_requests)} new version(s) of '{model_name}' "
                    f"in stage '{target_stage}'. Triggering '{target_job}'."
                )
                return SensorResult(
                    run_requests=run_requests,
                    cursor=str(new_max_version),
                )

            return SensorResult(
                skip_reason=(
                    f"No new versions of '{model_name}' in stage '{target_stage}' "
                    f"since version {last_seen_version}."
                )
            )

        return Definitions(sensors=[mlflow_model_sensor])
