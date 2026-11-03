"""Coalesce Job Sensor Component.

Polls a specific Coalesce run (by runCounter) via the Scheduler API and
triggers a Dagster job when it completes successfully.

Coalesce owns the schedule; Dagster reacts to completion --
the same pattern as dbt_cloud_job_sensor and fivetran_sync_sensor.

Two real bugs fixed here (confirmed against the live OpenAPI spec at
https://docs.coalesce.io/api/coalesce-api.json and the bundled jobs spec
at https://docs.coalesce.io/api/bundled/bundled-jobs.json on 2026-11-02,
the same research `integrations/coalesce_workspace`'s own docstring is
built on):

  1. The previous `environment_id` + `job_id` fields assumed a
     `GET /scheduler/runs?environmentID=...&jobID=...&limit=1` "list latest
     runs for a job" endpoint. That endpoint does not exist -- the
     Scheduler API has exactly four endpoints (startRun, runStatus, rerun,
     cancelRun), none of which list or search runs. Every poll under the
     old field surface would 404. Fixed by switching to `run_counter`
     (Coalesce's only real per-run identifier, returned when a run is
     started -- e.g. by CoalesceResource.start_run() or Coalesce's own UI)
     polled via the real `GET /scheduler/runStatus?runCounter=`.
  2. The previous code checked `status == "succeeded"`, but Coalesce's real
     terminal `runStatus` values are `completed`, `failed`, `canceled` (one
     L) -- `"succeeded"` never arrives, so the sensor could never have
     fired even if the endpoint above existed.

resource_key, when set, genuinely drives the poll via a registered
CoalesceResourceComponent's own get_run_status(...) method -- previously
this field was declared and wired into required_resource_keys but never
actually read.
"""
from typing import Optional
import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field, model_validator

_TERMINAL_SUCCESS = "completed"
_TERMINAL_FAILURE = {"failed", "canceled"}


class CoalesceJobSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Dagster job when a specific Coalesce run (by runCounter) completes.

    Example:
        ```yaml
        type: dagster_community_components.CoalesceJobSensorComponent
        attributes:
          sensor_name: coalesce_transform_done
          run_counter: "12345"
          api_token_env_var: COALESCE_API_TOKEN
          job_name: downstream_processing_job
        ```
    """

    sensor_name: str = Field(description="Unique sensor name")
    run_counter: str = Field(
        description=(
            "The Coalesce run to poll, by its runCounter (returned when the run was started, e.g. "
            "by CoalesceResource.start_run() or Coalesce's own UI/scheduler). Coalesce's Scheduler "
            "API has no way to list or search runs, so there is no 'latest run for a job' option --"
            " the exact runCounter must already be known."
        ),
    )
    api_token_env_var: Optional[str] = Field(default=None, description="Env var containing the Coalesce API token. Required unless resource_key is set.")
    api_base_url: str = Field(default="https://app.coalescesoftware.io", description="Coalesce API base URL. Ignored when resource_key is set.")
    job_name: str = Field(description="Dagster job to trigger when the Coalesce run completes")
    minimum_interval_seconds: int = Field(default=60, description="Seconds between polls")
    default_status: str = Field(default="running", description="running or stopped")
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional resource key registered by a CoalesceResourceComponent. When set, polls run "
            "via that resource's own get_run_status(...) method instead of the inline "
            "api_token_env_var/api_base_url below."
        ),
    )

    @model_validator(mode="after")
    def _check_credentials(self):
        if not self.resource_key and not self.api_token_env_var:
            raise ValueError(
                "CoalesceJobSensorComponent: supply resource_key (a registered "
                "CoalesceResourceComponent) OR api_token_env_var."
            )
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Coalesce Job Sensor', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        resource_key = self.resource_key
        required_resource_keys = {resource_key} if resource_key else set()
        default_status = (
            DefaultSensorStatus.RUNNING if self.default_status == "running"
            else DefaultSensorStatus.STOPPED
        )

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.minimum_interval_seconds,
            default_status=default_status,
            job_name=_self.job_name,
            required_resource_keys=required_resource_keys,
        )
        def coalesce_job_sensor(context: SensorEvaluationContext, **_resources):
            if resource_key:
                _res = getattr(context.resources, resource_key, None)
                if _res is None:
                    return SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    status = _res.get_run_status(_self.run_counter)
                except Exception as e:
                    return SensorResult(skip_reason=f"Coalesce API error (via resource '{resource_key}'): {e}")
            else:
                import os
                try:
                    import requests
                except ImportError:
                    return SensorResult(skip_reason="requests not installed")

                token = os.environ.get(_self.api_token_env_var, "")
                try:
                    resp = requests.get(
                        f"{_self.api_base_url}/scheduler/runStatus",
                        headers={"Authorization": f"Bearer {token}", "accept": "application/json"},
                        params={"runCounter": _self.run_counter},
                        timeout=30,
                    )
                    resp.raise_for_status()
                    status = resp.json().get("runStatus", "")
                except Exception as e:
                    return SensorResult(skip_reason=f"Coalesce API error: {e}")

            cursor = context.cursor or ""
            if status == _TERMINAL_SUCCESS and _self.run_counter != cursor:
                return SensorResult(
                    run_requests=[RunRequest(
                        run_key=_self.run_counter,
                        run_config={"ops": {"config": {
                            "run_counter": _self.run_counter,
                        }}},
                    )],
                    cursor=_self.run_counter,
                )
            if status in _TERMINAL_FAILURE:
                return SensorResult(skip_reason=f"Run {_self.run_counter} ended with status: {status}")

            return SensorResult(skip_reason=f"Run {_self.run_counter} status: {status or 'unknown'}")

        return dg.Definitions(sensors=[coalesce_job_sensor])
