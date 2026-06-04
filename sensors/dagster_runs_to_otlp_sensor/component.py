"""DagsterRunsToOtlpSensorComponent.

Push Dagster run lifecycle events (start / success / failure) to any
OpenTelemetry-compatible logs backend via OTLP/HTTP. Parallels the
existing `dagster_runs_to_prometheus_sensor` (Push Gateway / VM) and
`dagster_runs_to_influxdb_sensor` — same `@run_status_sensor` shape,
different wire protocol.

What lands at the OTel backend per run-status event:

    LogRecord with:
      body.stringValue:  "<job_name> <success|failure> (Xs)"
      severityNumber:    9 (INFO) for success, 17 (ERROR) for failure
      severityText:      INFO / ERROR
      timeUnixNano:      end_time
      attributes:
        dagster.run_id          string
        dagster.job_name        string
        dagster.status          success | failure
        dagster.duration_seconds double
        dagster.failure_step    string (failure only)

    Resource attributes:
      service.name             "dagster"
      service.instance.id      <location_label>

This is *operational telemetry* — distinct from the data-plane
`dataframe_to_otlp_logs` sink. Use this when customers want their
Dagster run lifecycle events flowing into the same OTel stack they
already use for app traces/metrics — Splunk via Splunk OTel Collector,
Datadog OTel ingest, Honeycomb, Sumo, Grafana Loki via OTel, etc.

Pairs with the existing `dagster_failures_to_otlp_sensor` (which
covers only failures with richer error context). This sensor covers
BOTH success + failure — pick whichever shape fits your destination.
"""
import os
import time
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


# OTel severity number scheme (1-24). 9 = INFO, 17 = ERROR.
_SEVERITY_INFO = 9
_SEVERITY_ERROR = 17


class DagsterRunsToOtlpSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Push Dagster run lifecycle events as OTLP/HTTP log records.

    Example (Honeycomb):

        ```yaml
        type: dagster_community_components.DagsterRunsToOtlpSensorComponent
        attributes:
          sensor_name: dagster_runs_to_honeycomb
          otlp_endpoint_env_var: HONEYCOMB_OTLP_ENDPOINT
          otlp_headers_env_var: HONEYCOMB_OTLP_HEADERS
          service_name: dagster
          location_label: prod-east
        ```

    Example (Splunk via Splunk OTel Collector):

        ```yaml
        type: dagster_community_components.DagsterRunsToOtlpSensorComponent
        attributes:
          sensor_name: dagster_runs_to_splunk_otc
          otlp_endpoint_env_var: SPLUNK_OTC_ENDPOINT
          service_name: dagster
        ```
    """

    sensor_name: str = Field(description="Dagster sensor name.")
    otlp_endpoint_env_var: str = Field(
        description="Env var with OTLP/HTTP endpoint base URL (e.g. https://api.honeycomb.io). The /v1/logs path is appended.",
    )
    otlp_headers_env_var: Optional[str] = Field(
        default=None,
        description="Env var with comma-separated HTTP headers (OTel convention: `x-honeycomb-team=<key>,x-honeycomb-dataset=dagster`).",
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
        description="If set, only emit for these Dagster job names. None = all.",
    )
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    request_timeout_seconds: int = Field(default=15, ge=1)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.run_status_sensor(
            name=self.sensor_name,
            run_status=dg.DagsterRunStatus.SUCCESS,
            monitored_jobs=None,
            default_status=(
                dg.DefaultSensorStatus.RUNNING
                if self.default_status.upper() == "RUNNING"
                else dg.DefaultSensorStatus.STOPPED
            ),
        )
        def _on_success(context: dg.RunStatusSensorContext):
            _self._push_run(context, status="success")

        @dg.run_status_sensor(
            name=f"{self.sensor_name}_failures",
            run_status=dg.DagsterRunStatus.FAILURE,
            monitored_jobs=None,
            default_status=(
                dg.DefaultSensorStatus.RUNNING
                if self.default_status.upper() == "RUNNING"
                else dg.DefaultSensorStatus.STOPPED
            ),
        )
        def _on_failure(context: dg.RunStatusSensorContext):
            _self._push_run(context, status="failure")

        return dg.Definitions(sensors=[_on_success, _on_failure])

    # ----------------------------------------------------------------- pushing

    def _push_run(self, context: "dg.RunStatusSensorContext", status: str) -> None:
        try:
            import requests
        except ImportError:
            context.log.warning("requests not installed — skipping OTLP push.")
            return

        endpoint = os.environ.get(self.otlp_endpoint_env_var, "")
        if not endpoint:
            context.log.warning(f"{self.otlp_endpoint_env_var} not set — skipping push.")
            return
        endpoint = endpoint.rstrip("/")

        # Parse comma-separated headers, OTel convention: K=V,K=V.
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if self.otlp_headers_env_var and os.environ.get(self.otlp_headers_env_var):
            for pair in os.environ[self.otlp_headers_env_var].split(","):
                pair = pair.strip()
                if "=" not in pair:
                    continue
                k, v = pair.split("=", 1)
                headers[k.strip()] = v.strip()

        run = context.dagster_run
        job_name = run.job_name or "unknown_job"
        if self.monitor_jobs and job_name not in self.monitor_jobs:
            return

        duration_seconds = 0.0
        if run.start_time and run.end_time:
            duration_seconds = float(run.end_time - run.start_time)
        elif run.start_time:
            duration_seconds = max(0.0, time.time() - float(run.start_time))

        time_unix_nano = int((run.end_time or time.time()) * 1_000_000_000)
        severity = _SEVERITY_ERROR if status == "failure" else _SEVERITY_INFO
        severity_text = "ERROR" if status == "failure" else "INFO"

        body_text = (
            f"Dagster run {run.run_id} for job {job_name} "
            f"{status} ({duration_seconds:.2f}s)"
        )
        attrs = [
            _otlp_attr("dagster.run_id", run.run_id),
            _otlp_attr("dagster.job_name", job_name),
            _otlp_attr("dagster.status", status),
            _otlp_attr("dagster.duration_seconds", duration_seconds, kind="double"),
        ]

        # Failures get the step_key + first 500 chars of error message
        # in attrs — same shape as dagster_failures_to_otlp_sensor.
        if status == "failure":
            failure_step = None
            err_msg = None
            try:
                failure_event = getattr(context, "failure_event", None)
                if failure_event:
                    failure_step = getattr(failure_event, "step_key", None)
                    err_msg = (
                        getattr(failure_event, "message", None)
                        or getattr(failure_event, "logging_tags", {}).get("error")
                    )
            except Exception:
                pass
            attrs.append(_otlp_attr("dagster.failure_step", failure_step or ""))
            if err_msg:
                body_text = (
                    f"{body_text} step={failure_step or '?'} "
                    f"err={str(err_msg)[:500]}"
                )

        envelope = {
            "resourceLogs": [{
                "resource": {
                    "attributes": [
                        _otlp_attr("service.name", self.service_name),
                        _otlp_attr("service.instance.id", self.location_label),
                    ],
                },
                "scopeLogs": [{
                    "scope": {"name": "dagster_community_components.dagster_runs_to_otlp_sensor"},
                    "logRecords": [{
                        "timeUnixNano": str(time_unix_nano),
                        "observedTimeUnixNano": str(time_unix_nano),
                        "severityNumber": severity,
                        "severityText": severity_text,
                        "body": {"stringValue": body_text},
                        "attributes": attrs,
                    }],
                }],
            }],
        }

        try:
            resp = requests.post(
                f"{endpoint}/v1/logs",
                json=envelope,
                headers=headers,
                timeout=self.request_timeout_seconds,
            )
            resp.raise_for_status()
            context.log.info(
                f"Pushed run event → OTLP for {job_name} status={status} "
                f"duration={duration_seconds:.2f}s"
            )
        except Exception as exc:
            context.log.warning(f"Failed to push run event to OTLP: {exc}")


def _otlp_attr(key: str, value, kind: str = "string") -> dict:
    """Build one OTLP KeyValue attribute."""
    if kind == "double":
        return {"key": key, "value": {"doubleValue": float(value)}}
    if kind == "int":
        return {"key": key, "value": {"intValue": int(value)}}
    return {"key": key, "value": {"stringValue": str(value)}}
