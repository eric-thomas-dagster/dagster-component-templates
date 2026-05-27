"""Precisely Job Sensor Component.

Triggers a Dagster job when a specified Precisely Connect ETL job run hits a
terminal SUCCESS status. Precisely owns the schedule; Dagster reacts.

The sensor polls the documented Precisely Job Status endpoint
(``GET /projects/{jobRunId}/status``) and fires a ``RunRequest`` when the
run reaches ``COMPLETED`` or ``COMPLETED_WITH_WARNINGS``. This is the
only verified public REST surface — earlier versions of this component
shipped a second "watch the latest run of a job" mode that depended on
an unverified list-runs path, which has been removed.

Pair this sensor with the ``external_precisely_job`` component to surface
the same Precisely run in the Dagster catalog as a declare-only external
asset; on terminal SUCCESS the sensor emits ``AssetMaterialization`` for
the asset_key configured below, which lights up that external asset's
materialization history.

Job Status enum (returned as plain text):
    WAITING | RUNNING | COMPLETED | COMPLETED_WITH_WARNINGS |
    COMPLETED_WITH_ERRORS | CANCELLED | ERRORED | LOST_CONTACT

Docs: https://help.precisely.com/r/Connect-ETL/pub/Latest/en-US/Connect-ETL-Rest-API-Reference/Job-Status
"""
from typing import Dict, Optional

import dagster as dg
from dagster import (
    AssetMaterialization,
    AssetObservation,
    ConfigurableResource,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
)
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class PreciselyResource(ConfigurableResource):
    """Resource for reading status from the Precisely Connect ETL REST API.

    Only the documented ``GET /projects/{jobRunId}/status`` endpoint is
    exercised — Precisely does not publish a submit endpoint, so this
    resource only observes.

    Example:
        ```python
        PreciselyResource(
            host=EnvVar("PRECISELY_HOST"),
            api_token=EnvVar("PRECISELY_API_TOKEN"),
        )
        ```
    """

    host: str = Field(description="Precisely Connect ETL host URL (e.g. https://precisely.mycompany.com)")
    api_token: str = Field(description="Precisely API token (Bearer)")

    def _base(self) -> str:
        return self.host.rstrip("/")

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.api_token}", "Accept": "application/json"}

    def get_run_status(self, job_run_id: str) -> str:
        """Fetch the current status of a Connect ETL job run.

        Returns one of: WAITING | RUNNING | COMPLETED | COMPLETED_WITH_WARNINGS |
        COMPLETED_WITH_ERRORS | CANCELLED | ERRORED | LOST_CONTACT.
        Endpoint returns plain text, not JSON.
        """
        import requests
        resp = requests.get(
            f"{self._base()}/projects/{job_run_id}/status",
            headers=self._headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.text.strip().upper()


PRECISELY_TERMINAL_SUCCESS = {"COMPLETED", "COMPLETED_WITH_WARNINGS"}
PRECISELY_TERMINAL_FAIL = {"COMPLETED_WITH_ERRORS", "CANCELLED", "ERRORED", "LOST_CONTACT"}


class PreciselyJobSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Dagster job when a Precisely Connect ETL run reaches terminal SUCCESS.

    Example:
        ```yaml
        type: dagster_component_templates.PreciselyJobSensorComponent
        attributes:
          sensor_name: precisely_etl_done
          job_run_id: "abc-123-def-456"          # the Precisely run-id to watch
          host_env_var: PRECISELY_HOST
          api_token_env_var: PRECISELY_API_TOKEN
          job_name: downstream_processing_job
        ```
    """

    sensor_name: str = Field(description="Unique sensor name")
    job_run_id: str = Field(
        description=(
            "Precisely Connect ETL job-run ID to watch. The sensor polls the "
            "documented Job Status endpoint and fires a RunRequest when this "
            "run reaches a terminal SUCCESS state."
        ),
    )
    job_name: str = Field(description="Dagster job to trigger when the Precisely run reaches terminal SUCCESS.")
    asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional Dagster asset key. When set, the sensor also emits "
            "AssetMaterialization (or AssetObservation — see asset_event_type) "
            "for this key on terminal SUCCESS — which lights up the matching "
            "external_precisely_job asset in the catalog. Use '/' separators "
            "for nested keys, e.g. 'precisely/etl/load_customers'."
        ),
    )
    asset_event_type: str = Field(
        default="materialization",
        description=(
            "What kind of asset event to emit on terminal SUCCESS: "
            "'materialization' (default — lights up the materialization "
            "history, what you want for jobs Dagster doesn't run itself) or "
            "'observation' (emits AssetObservation instead — better when "
            "you want the materialization timeline reserved for assets "
            "Dagster materializes itself, and Precisely runs to be "
            "observability data alongside)."
        ),
    )
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

            run_id = _self.job_run_id

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
                asset_events = []
                if _self.asset_key:
                    asset_key = dg.AssetKey(_self.asset_key.split("/"))
                    description = f"Precisely run {run_id} → {status}"
                    metadata = {
                        "precisely_job_run_id": run_id,
                        "precisely_status": status,
                        "precisely_host": base,
                    }
                    if _self.asset_event_type == "observation":
                        asset_events.append(AssetObservation(
                            asset_key=asset_key,
                            description=description,
                            metadata=metadata,
                        ))
                    else:
                        asset_events.append(AssetMaterialization(
                            asset_key=asset_key,
                            description=description,
                            metadata=metadata,
                        ))
                # Pass Precisely identifiers via `tags` rather than
                # `run_config`. The old run_config={"ops": {"config":
                # {...}}} treated the literal string "config" as an op
                # name, so downstream ops' context.op_config was always
                # empty. tags are accessible to downstream ops via
                # context.run.tags and aren't tied to a specific op-name.
                return SensorResult(
                    run_requests=[RunRequest(
                        run_key=run_id,
                        tags={
                            "precisely/job_run_id": run_id,
                            "precisely/status": status,
                            "precisely/host": base,
                        },
                    )],
                    asset_events=asset_events or None,
                    cursor=run_id,
                )

            if status in PRECISELY_TERMINAL_FAIL:
                return SensorResult(skip_reason=f"Run {run_id} terminal failure: {status}")

            return SensorResult(skip_reason=f"Run {run_id} status: {status}")

        return dg.Definitions(sensors=[precisely_job_sensor])
