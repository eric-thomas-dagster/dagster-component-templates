"""TM1 Process Status Sensor Component.

Polls TM1's ActiveThreads / process execution history and triggers a Dagster
job when a TI process transitions to a target status (e.g. CompletedSuccessfully,
Aborted, HasMinorErrors).
"""
from typing import List

import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class TM1ProcessStatusSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Dagster job when a TM1 process hits a target status.

    TM1 exposes recent process executions via a queryable log; the sensor
    polls, filters to the target `process_name`, and fires on new records
    matching `target_statuses`. Cursor = last processed execution ID so
    we never re-fire on the same run.

    Common statuses:
      - CompletedSuccessfully
      - CompletedWithMessages
      - HasMinorErrors
      - Aborted
      - QuitCalled

    Example (alert on any error):

        ```yaml
        type: dagster_community_components.TM1ProcessStatusSensorComponent
        attributes:
          sensor_name: gl_load_error_alert
          process_name: LoadActualsFromGL
          target_statuses: [HasMinorErrors, Aborted, QuitCalled]
          job_name: page_oncall_tm1_error
          resource_key: tm1_resource
        ```
    """

    sensor_name: str = Field(description="Unique sensor name.")
    process_name: str = Field(description="TI process name to watch.")
    target_statuses: List[str] = Field(
        default_factory=lambda: ["CompletedSuccessfully"],
        description="Statuses that should trigger the job. Common: CompletedSuccessfully, HasMinorErrors, Aborted, QuitCalled.",
    )
    job_name: str = Field(description="Job to trigger on status match.")
    resource_key: str = Field(default="tm1_resource", description="Resource key to look up.")
    minimum_interval_seconds: int = Field(default=60, description="Seconds between polls.")
    default_status: str = Field(default="running", description="running | stopped")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        required_resource_keys = {self.resource_key}
        default_status = (
            DefaultSensorStatus.RUNNING if self.default_status == "running"
            else DefaultSensorStatus.STOPPED
        )
        target_upper = {s for s in self.target_statuses}

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.minimum_interval_seconds,
            default_status=default_status,
            job_name=_self.job_name,
            required_resource_keys=required_resource_keys,
        )
        def tm1_process_status_sensor(context: SensorEvaluationContext):
            try:
                import requests
            except ImportError:
                return SensorResult(skip_reason="requests not installed")

            resource = getattr(context.resources, _self.resource_key)
            session = requests.Session()
            session.verify = resource.verify_ssl
            headers = resource.get_auth_headers()

            # TM1 v11.4+ exposes process execution logs via TransactionLog / MessageLog.
            # We use the process detail endpoint's history sub-collection.
            url = f"{resource.process_url(_self.process_name)}?$expand=LastMessage,ExecutionCounts"
            try:
                resp = session.get(url, headers=headers, timeout=30)
                resp.raise_for_status()
                body = resp.json() or {}
            except Exception as e:
                return SensorResult(skip_reason=f"TM1 process lookup error: {e}")

            last_msg = body.get("LastMessage") or {}
            status = last_msg.get("Status") or last_msg.get("StatusCode") or ""
            execution_id = last_msg.get("ExecutionID") or last_msg.get("ID") or ""

            if status not in target_upper:
                return SensorResult(skip_reason=f"Status {status!r} not in target set {sorted(target_upper)}")

            cursor = context.cursor or ""
            fingerprint = f"{status}|{execution_id}"
            if fingerprint == cursor:
                return SensorResult(skip_reason=f"Already processed {fingerprint}")

            return SensorResult(
                run_requests=[
                    RunRequest(
                        run_key=fingerprint,
                        run_config={
                            "ops": {
                                "config": {
                                    "tm1_process": _self.process_name,
                                    "status": status,
                                    "execution_id": execution_id,
                                }
                            }
                        },
                    )
                ],
                cursor=fingerprint,
            )

        return dg.Definitions(sensors=[tm1_process_status_sensor])
