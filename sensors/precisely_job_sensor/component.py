"""Precisely Job Sensor Component.

Triggers a Dagster job when a specified Precisely Connect ETL job run hits a
terminal SUCCESS status. Precisely owns the schedule; Dagster reacts.

Two operating modes:

  1. **Watch a fixed job-run ID** (`job_run_id`). The sensor polls the
     documented Job Status endpoint (`GET /projects/{jobRunId}/status`) and
     fires a RunRequest when the run reaches `COMPLETED` or
     `COMPLETED_WITH_WARNINGS`. This mode uses Precisely's verified public
     API surface.

  2. **Watch the latest run of a job** (`job_id`). The sensor polls a
     `list_runs_path_template` (default best-guess `/api/v1/jobs/{job_id}/runs`),
     picks the newest run-id, and checks its status. The list-runs endpoint
     is NOT in Precisely's public REST docs — you'll need to validate the
     path against your install's API and override `list_runs_path_template`
     if it differs.

Job Status enum (returned as plain text):
    WAITING | RUNNING | COMPLETED | COMPLETED_WITH_WARNINGS |
    COMPLETED_WITH_ERRORS | CANCELLED | ERRORED | LOST_CONTACT

See https://help.precisely.com/r/Connect-ETL/pub/Latest/en-US/Connect-ETL-Rest-API-Reference/Job-Status
"""
from typing import Optional

import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


PRECISELY_TERMINAL_SUCCESS = {"COMPLETED", "COMPLETED_WITH_WARNINGS"}
PRECISELY_TERMINAL_FAIL = {"COMPLETED_WITH_ERRORS", "CANCELLED", "ERRORED", "LOST_CONTACT"}


class PreciselyJobSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Dagster job when a Precisely Connect ETL run reaches terminal SUCCESS.

    Example (watch a fixed run by ID — uses verified API):
        ```yaml
        type: dagster_component_templates.PreciselyJobSensorComponent
        attributes:
          sensor_name: precisely_etl_done
          job_run_id: "abc-123-...-xyz"
          host_env_var: PRECISELY_HOST
          api_token_env_var: PRECISELY_API_TOKEN
          job_name: downstream_job
        ```

    Example (watch latest run of a job — uses unverified list-runs path):
        ```yaml
        type: dagster_component_templates.PreciselyJobSensorComponent
        attributes:
          sensor_name: precisely_etl_done
          job_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
          list_runs_path_template: "/api/v1/jobs/{job_id}/runs"   # validate vs your install
          host_env_var: PRECISELY_HOST
          api_token_env_var: PRECISELY_API_TOKEN
          job_name: downstream_job
        ```
    """

    sensor_name: str = Field(description="Unique sensor name")
    job_id: Optional[str] = Field(
        default=None,
        description="Precisely Connect ETL job ID to monitor (use with list-runs mode).",
    )
    job_run_id: Optional[str] = Field(
        default=None,
        description=(
            "A specific Precisely Connect ETL job-run ID to watch. When set, the "
            "sensor uses the documented Job Status endpoint and ignores job_id."
        ),
    )
    list_runs_path_template: str = Field(
        default="/api/v1/jobs/{job_id}/runs",
        description=(
            "Path template for listing recent runs of a job (used when job_run_id "
            "is unset). Default is a best-guess RESTful shape; validate against "
            "your Connect ETL install's API."
        ),
    )
    job_name: str = Field(description="Dagster job to trigger when the Precisely run reaches terminal SUCCESS.")
    host_env_var: Optional[str] = Field(default=None, description="Env var with Precisely Connect ETL host URL")
    api_token_env_var: Optional[str] = Field(default=None, description="Env var with Precisely API token")
    resource_key: Optional[str] = Field(default=None, description="Key of a PreciselyResource")
    minimum_interval_seconds: int = Field(default=60, description="Seconds between polls")
    default_status: str = Field(default="running", description="running or stopped")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        required_resource_keys = {self.resource_key} if self.resource_key else set()
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
        def precisely_job_sensor(context: SensorEvaluationContext):
            import os
            try:
                import requests
            except ImportError:
                return SensorResult(skip_reason="requests not installed")

            if _self.resource_key:
                resource = getattr(context.resources, _self.resource_key)
                base = resource._base()
                headers = resource._headers()
            else:
                host = os.environ.get(_self.host_env_var or "", "").rstrip("/")
                token = os.environ.get(_self.api_token_env_var or "", "")
                base = host
                headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

            # Resolve which run-id to watch.
            if _self.job_run_id:
                run_id = _self.job_run_id
            elif _self.job_id:
                # Best-guess list-runs endpoint — not in Precisely's public docs.
                try:
                    resp = requests.get(
                        f"{base}{_self.list_runs_path_template.format(job_id=_self.job_id)}",
                        headers=headers,
                        params={"limit": 1, "sort": "-startTime"},
                        timeout=30,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    runs = data.get("runs", data if isinstance(data, list) else [])
                    latest = runs[0] if runs else None
                except Exception as e:
                    return SensorResult(skip_reason=f"Precisely list-runs error: {e}")

                if not latest:
                    return SensorResult(skip_reason="No runs found for this job")

                run_id = str(
                    latest.get("jobRunId")
                    or latest.get("runId")
                    or latest.get("id")
                    or ""
                )
                if not run_id:
                    return SensorResult(skip_reason="latest run had no recognizable id field")
            else:
                return SensorResult(skip_reason="set either job_run_id or job_id")

            # Poll the documented Job Status endpoint for that run-id.
            # Returns plain-text status, not JSON.
            try:
                resp = requests.get(
                    f"{base}/projects/{run_id}/status",
                    headers=headers,
                    timeout=30,
                )
                resp.raise_for_status()
                status = resp.text.strip().upper()
            except Exception as e:
                return SensorResult(skip_reason=f"Precisely status poll error: {e}")

            cursor = context.cursor or ""

            if status in PRECISELY_TERMINAL_SUCCESS and run_id != cursor:
                return SensorResult(
                    run_requests=[RunRequest(
                        run_key=run_id,
                        run_config={"ops": {"config": {
                            "precisely_job_id": _self.job_id or "",
                            "precisely_job_run_id": run_id,
                            "precisely_status": status,
                        }}},
                    )],
                    cursor=run_id,
                )

            if status in PRECISELY_TERMINAL_FAIL:
                return SensorResult(skip_reason=f"Run {run_id} terminal failure: {status}")

            return SensorResult(skip_reason=f"Run {run_id} status: {status}")

        return dg.Definitions(sensors=[precisely_job_sensor])
