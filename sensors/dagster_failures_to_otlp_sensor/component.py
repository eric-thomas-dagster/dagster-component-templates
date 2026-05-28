"""DagsterFailuresToOtlpSensorComponent.

Listen for Dagster run failures and emit OTLP (OpenTelemetry Protocol)
log records to an OTLP/HTTP endpoint. Targets any stack that ingests
OTLP — Honeycomb, Datadog OTel-ingest, Grafana Loki via OTel
Collector, ClickHouse via OTel, AWS CloudWatch OTel, etc.

Uses OTLP/HTTP (JSON body) so we don't pull in the entire
`opentelemetry-sdk` dependency tree — just `requests` + a hand-built
JSON envelope. The OTLP log record carries:

  - severity_text:  ERROR
  - body:           run failure message + step-level error if available
  - attributes:
      - dagster.job_name        (string)
      - dagster.run_id          (string)
      - dagster.duration_seconds (double)
      - dagster.failure_step    (string, if available)
      - service.name            (resource attribute)
      - service.instance.id     (resource attribute = location label)

This is intentionally narrower than the other telemetry sensors —
failures-only, structured-log shape (not metrics). For metric-shaped
run telemetry, use `dagster_runs_to_prometheus_sensor` /
`dagster_runs_to_influxdb_sensor`.
"""
import os
import time
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class DagsterFailuresToOtlpSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Push Dagster run failure events to an OTLP/HTTP logs endpoint.

    Example (Honeycomb):

        ```yaml
        type: dagster_community_components.DagsterFailuresToOtlpSensorComponent
        attributes:
          sensor_name: dagster_failures_to_honeycomb
          otlp_endpoint_env_var: HONEYCOMB_OTLP_ENDPOINT   # e.g. https://api.honeycomb.io
          otlp_headers_env_var: HONEYCOMB_OTLP_HEADERS     # e.g. x-honeycomb-team=<key>
          service_name: dagster
          location_label: prod-east
        ```

    Example (Datadog OTel ingest):

        ```yaml
        type: dagster_community_components.DagsterFailuresToOtlpSensorComponent
        attributes:
          sensor_name: dagster_failures_to_datadog
          otlp_endpoint_env_var: DD_OTLP_ENDPOINT
          otlp_headers_env_var: DD_OTLP_HEADERS
          service_name: dagster
        ```
    """

    sensor_name: str = Field(description="Dagster sensor name.")
    otlp_endpoint_env_var: str = Field(
        description="Env var with OTLP/HTTP endpoint base URL (e.g. https://api.honeycomb.io). The /v1/logs path is appended.",
    )
    otlp_headers_env_var: Optional[str] = Field(
        default=None,
        description="Env var with comma-separated HTTP headers (e.g. `x-honeycomb-team=<key>,x-honeycomb-dataset=dagster`). Standard OTel env-var convention.",
    )
    service_name: str = Field(
        default="dagster",
        description="OTLP resource attribute `service.name`.",
    )
    location_label: str = Field(
        default="default",
        description="OTLP resource attribute `service.instance.id` — usually deployment / region.",
    )
    monitor_jobs: Optional[List[str]] = Field(
        default=None,
        description="If set, only emit OTLP records for these Dagster job names. None = all.",
    )
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    request_timeout_seconds: int = Field(default=15, ge=1)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.run_failure_sensor(
            name=self.sensor_name,
            monitored_jobs=None,
            default_status=(
                dg.DefaultSensorStatus.RUNNING
                if self.default_status.upper() == "RUNNING"
                else dg.DefaultSensorStatus.STOPPED
            ),
        )
        def _on_failure(context: dg.RunFailureSensorContext):
            _self._push_failure(context)

        return dg.Definitions(sensors=[_on_failure])

    def _push_failure(self, context: "dg.RunFailureSensorContext") -> None:
        try:
            import requests
        except ImportError:
            context.log.warning("requests not installed — skipping OTLP push.")
            return

        endpoint = os.environ.get(self.otlp_endpoint_env_var, "")
        if not endpoint:
            context.log.warning(f"{self.otlp_endpoint_env_var} not set — skipping OTLP push.")
            return
        endpoint = endpoint.rstrip("/")

        # Parse comma-separated headers, OTel convention.
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if self.otlp_headers_env_var and os.environ.get(self.otlp_headers_env_var):
            for pair in os.environ[self.otlp_headers_env_var].split(","):
                pair = pair.strip()
                if "=" not in pair:
                    continue
                k, v = pair.split("=", 1)
                headers[k.strip()] = v.strip()

        run = context.dagster_run
        if self.monitor_jobs and run.job_name not in self.monitor_jobs:
            return

        duration_seconds = 0.0
        if run.start_time and run.end_time:
            duration_seconds = float(run.end_time - run.start_time)

        # Failure detail — context.failure_event carries the first failure event.
        body_text = f"Dagster run {run.run_id} for job {run.job_name} failed."
        failure_step = None
        try:
            failure_event = context.failure_event
            if failure_event:
                failure_step = getattr(failure_event, "step_key", None)
                err_msg = (
                    getattr(failure_event, "message", None)
                    or getattr(failure_event, "logging_tags", {}).get("error", None)
                )
                if err_msg:
                    body_text = f"{body_text} step={failure_step or '?'} err={str(err_msg)[:500]}"
        except Exception:
            pass

        time_unix_nano = int((run.end_time or time.time()) * 1_000_000_000)

        # Hand-built OTLP/HTTP logs envelope.
        otlp_body = {
            "resourceLogs": [{
                "resource": {
                    "attributes": [
                        _otlp_attr("service.name", self.service_name),
                        _otlp_attr("service.instance.id", self.location_label),
                    ],
                },
                "scopeLogs": [{
                    "scope": {"name": "dagster_community_components.dagster_failures_to_otlp_sensor"},
                    "logRecords": [{
                        "timeUnixNano": str(time_unix_nano),
                        "observedTimeUnixNano": str(time_unix_nano),
                        "severityNumber": 17,    # ERROR per OTel spec
                        "severityText": "ERROR",
                        "body": {"stringValue": body_text},
                        "attributes": [
                            _otlp_attr("dagster.job_name", run.job_name or "unknown"),
                            _otlp_attr("dagster.run_id", run.run_id),
                            _otlp_attr("dagster.duration_seconds", duration_seconds, kind="double"),
                            _otlp_attr("dagster.failure_step", failure_step or ""),
                        ],
                    }],
                }],
            }],
        }

        url = f"{endpoint}/v1/logs"
        try:
            resp = requests.post(
                url,
                json=otlp_body,
                headers=headers,
                timeout=self.request_timeout_seconds,
            )
            resp.raise_for_status()
            context.log.info(f"Pushed failure → OTLP for {run.job_name} ({run.run_id})")
        except Exception as exc:
            context.log.warning(f"Failed to push failure to OTLP: {exc}")


def _otlp_attr(key: str, value, kind: str = "string") -> dict:
    """Build one OTLP KeyValue attribute. Per-spec: keys are strings, values typed."""
    if kind == "double":
        return {"key": key, "value": {"doubleValue": float(value)}}
    if kind == "int":
        return {"key": key, "value": {"intValue": int(value)}}
    return {"key": key, "value": {"stringValue": str(value)}}
