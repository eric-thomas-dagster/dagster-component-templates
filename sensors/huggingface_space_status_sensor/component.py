"""HuggingFace Space status sensor.

Watches a [HuggingFace Space](https://huggingface.co/spaces) and fires a
Dagster ``RunRequest`` when the Space's stage transitions to ``RUNNING``
(default) — useful for triggering downstream Dagster work after a Space
finishes rebuilding / restarting after a model push.

Polls ``huggingface_hub.HfApi().space_info()`` for the current stage.
Standard Space stages: ``NO_APP_FILE``, ``CONFIG_ERROR``, ``BUILDING``,
``BUILD_ERROR``, ``RUNNING``, ``RUNNING_BUILDING``, ``RUNTIME_ERROR``,
``DELETING``, ``STOPPED``, ``PAUSED``, ``SLEEPING``.
"""

from typing import List, Optional

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
)
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


# Reference: https://huggingface.co/docs/huggingface_hub/en/package_reference/hf_api#huggingface_hub.SpaceStage
SPACE_TERMINAL_RUNNING = {"RUNNING", "RUNNING_BUILDING"}
SPACE_TERMINAL_FAIL = {"BUILD_ERROR", "CONFIG_ERROR", "NO_APP_FILE", "RUNTIME_ERROR"}
SPACE_TRANSIENT = {"BUILDING", "STOPPED", "PAUSED", "SLEEPING", "DELETING"}


class HuggingfaceSpaceStatusSensorComponent(Component, Model, Resolvable):
    """Fire a Dagster RunRequest when a HuggingFace Space reaches a target stage.

    Example (fire on RUNNING after a rebuild):
        ```yaml
        type: dagster_community_components.HuggingfaceSpaceStatusSensorComponent
        attributes:
          sensor_name: space_rebuilt
          space_id: my-org/my-app
          target_stages: ["RUNNING"]
          job_name: downstream_eval_job
          hf_token_env_var: HF_TOKEN
          minimum_interval_seconds: 60
        ```
    """

    sensor_name: str = Field(description="Unique sensor name.")
    space_id: str = Field(
        description="HuggingFace Space id (e.g. 'gradio/hello_world' or 'my-org/my-app').",
    )
    target_stages: Optional[List[str]] = Field(
        default=None,
        description=(
            "Space stages that should trigger a RunRequest. Default: "
            "['RUNNING']. Other valid stages: BUILDING, BUILD_ERROR, "
            "RUNNING_BUILDING, RUNTIME_ERROR, STOPPED, PAUSED, SLEEPING."
        ),
    )
    job_name: str = Field(description="Dagster job to trigger when the Space hits a target stage.")
    hf_token_env_var: Optional[str] = Field(
        default=None,
        description="Env var with HF token (required for gated / private Spaces).",
        json_schema_extra={"ui:widget": "env_var"},
    )
    minimum_interval_seconds: int = Field(default=60, description="Seconds between Hub API polls.")
    default_status: str = Field(default="running", description="'running' or 'stopped'.")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _self = self
        targets = set(s.upper() for s in (self.target_stages or ["RUNNING"]))
        default_status = (
            DefaultSensorStatus.RUNNING if self.default_status == "running"
            else DefaultSensorStatus.STOPPED
        )

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.minimum_interval_seconds,
            default_status=default_status,
            job_name=_self.job_name,
        )
        def huggingface_space_status_sensor(context: SensorEvaluationContext):
            import os
            try:
                from huggingface_hub import HfApi
            except ImportError:
                return SensorResult(skip_reason="huggingface_hub not installed")

            token = os.environ.get(_self.hf_token_env_var or "", "") or None
            api = HfApi(token=token)

            try:
                info = api.space_info(_self.space_id)
            except Exception as e:
                return SensorResult(skip_reason=f"HfApi.space_info({_self.space_id!r}) failed: {e}")

            runtime = getattr(info, "runtime", None)
            stage = getattr(runtime, "stage", None) if runtime else None
            stage_str = (str(stage).upper() if stage else "(unknown)")
            last_modified = str(getattr(info, "last_modified", ""))

            # Cursor key combines stage + last_modified so a Space that stays in
            # RUNNING but gets re-pushed still fires once.
            cursor_key = f"{stage_str}|{last_modified}"
            prev_cursor = context.cursor or ""

            if stage_str in targets and cursor_key != prev_cursor:
                return SensorResult(
                    run_requests=[RunRequest(
                        run_key=cursor_key,
                        run_config={"ops": {"config": {
                            "huggingface_space_id": _self.space_id,
                            "huggingface_space_stage": stage_str,
                            "huggingface_space_last_modified": last_modified,
                        }}},
                    )],
                    cursor=cursor_key,
                )

            if stage_str in SPACE_TERMINAL_FAIL:
                return SensorResult(skip_reason=f"Space {_self.space_id} in failure stage {stage_str}")

            return SensorResult(skip_reason=f"Space {_self.space_id} stage: {stage_str} (waiting for {targets})")

        return Definitions(sensors=[huggingface_space_status_sensor])
