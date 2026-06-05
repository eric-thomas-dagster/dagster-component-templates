"""DagsterRunsToOtlpMetricsSensorComponent.

Push Dagster run lifecycle events to any OTLP/HTTP **metrics** endpoint.

Distinct from `dagster_runs_to_otlp_sensor` (which emits OTel **logs**) —
this sensor emits OTel **metrics**, the right shape for the Datadog OTel
ingest, Grafana OTel collector, Honeycomb's metrics endpoint, etc.

Metrics emitted per run-status event:

  dagster.runs.total                   Sum (Counter, isMonotonic=true)
                                       attrs: dagster.status, dagster.job
  dagster.runs.duration_seconds        Gauge (point value per run)
                                       attrs: dagster.status, dagster.job
  dagster.runs.failure_step            Sum (failure only)
                                       attrs: dagster.job, dagster.step

We use OTel's per-event Sum/Gauge shape (single data point per run, with
the run's end_time as the observation timestamp). Aggregations and rate
calculations happen at the collector / backend, not in this sensor.
"""
import os
import time
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


class DagsterRunsToOtlpMetricsSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Push Dagster run lifecycle events as OTLP/HTTP metrics.

    Example (Datadog OTel ingest):

        ```yaml
        type: dagster_component_templates.DagsterRunsToOtlpMetricsSensorComponent
        attributes:
          sensor_name: dagster_runs_to_dd_otel
          otlp_endpoint_env_var: DD_OTLP_ENDPOINT
          otlp_headers_env_var: DD_OTLP_HEADERS
          service_name: dagster
          location_label: prod-east
        ```

    Example (Grafana Cloud OTel):

        ```yaml
        type: dagster_component_templates.DagsterRunsToOtlpMetricsSensorComponent
        attributes:
          sensor_name: dagster_runs_to_grafana_otel
          otlp_endpoint_env_var: GRAFANA_OTLP_ENDPOINT
          otlp_headers_env_var: GRAFANA_OTLP_HEADERS
          service_name: dagster
        ```
    """

    sensor_name: str = Field(description="Dagster sensor name.")
    otlp_endpoint_env_var: str = Field(
        description="Env var with OTLP/HTTP endpoint base URL. The /v1/metrics path is appended.",
    )
    otlp_headers_env_var: Optional[str] = Field(
        default=None,
        description="Env var with comma-separated HTTP headers (k=v,k=v).",
    )
    service_name: str = Field(
        default="dagster",
        description="OTLP resource attribute `service.name`.",
    )
    location_label: str = Field(
        default="default",
        description="OTLP resource attribute `service.instance.id`.",
    )
    monitor_jobs: Optional[List[str]] = Field(
        default=None,
        description="If set, only emit for these job names. None = all.",
    )
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    request_timeout_seconds: int = Field(default=15, ge=1)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        _status = (
            dg.DefaultSensorStatus.RUNNING
            if self.default_status.upper() == "RUNNING"
            else dg.DefaultSensorStatus.STOPPED
        )

        @dg.run_status_sensor(
            name=self.sensor_name,
            run_status=dg.DagsterRunStatus.SUCCESS,
            monitored_jobs=None,
            default_status=_status,
        )
        def _on_success(context: dg.RunStatusSensorContext):
            _self._push(context, status="success")

        @dg.run_status_sensor(
            name=f"{self.sensor_name}_failures",
            run_status=dg.DagsterRunStatus.FAILURE,
            monitored_jobs=None,
            default_status=_status,
        )
        def _on_failure(context: dg.RunStatusSensorContext):
            _self._push(context, status="failure")

        return dg.Definitions(sensors=[_on_success, _on_failure])

    # ----------------------------------------------------------------- push

    def _push(self, context: "dg.RunStatusSensorContext", status: str) -> None:
        try:
            import requests
        except ImportError:
            context.log.warning("requests not installed — skipping OTLP metrics push.")
            return

        endpoint = os.environ.get(self.otlp_endpoint_env_var, "")
        if not endpoint:
            context.log.warning(f"{self.otlp_endpoint_env_var} not set — skipping push.")
            return
        endpoint = endpoint.rstrip("/")

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

        end_time_unix_nano = int((run.end_time or time.time()) * 1_000_000_000)
        # Most OTel backends want a non-zero startTimeUnixNano on Sums.
        start_time_unix_nano = int((run.start_time or run.end_time or time.time()) * 1_000_000_000)

        common_attrs = [
            _attr("dagster.status", status),
            _attr("dagster.job", job_name),
        ]

        metrics: List[Dict] = []

        # 1. runs.total — Sum (counter, isMonotonic=true).
        metrics.append({
            "name": "dagster.runs.total",
            "unit": "1",
            "description": "Total Dagster runs that completed with this status.",
            "sum": {
                "aggregationTemporality": 2,   # 2 = CUMULATIVE
                "isMonotonic": True,
                "dataPoints": [{
                    "attributes": common_attrs,
                    "startTimeUnixNano": str(start_time_unix_nano),
                    "timeUnixNano": str(end_time_unix_nano),
                    "asInt": "1",
                }],
            },
        })

        # 2. runs.duration_seconds — Gauge (single observed value).
        metrics.append({
            "name": "dagster.runs.duration_seconds",
            "unit": "s",
            "description": "Wall-clock duration of the Dagster run.",
            "gauge": {
                "dataPoints": [{
                    "attributes": common_attrs,
                    "timeUnixNano": str(end_time_unix_nano),
                    "asDouble": float(duration_seconds),
                }],
            },
        })

        # 3. failure-only: runs.failure_step counter
        if status == "failure":
            failure_step = None
            try:
                fe = getattr(context, "failure_event", None)
                if fe:
                    failure_step = getattr(fe, "step_key", None)
            except Exception:
                pass
            metrics.append({
                "name": "dagster.runs.failure_step",
                "unit": "1",
                "description": "Failure-step counter (one data point per failed run).",
                "sum": {
                    "aggregationTemporality": 2,
                    "isMonotonic": True,
                    "dataPoints": [{
                        "attributes": [
                            _attr("dagster.job", job_name),
                            _attr("dagster.step", failure_step or "unknown"),
                        ],
                        "startTimeUnixNano": str(start_time_unix_nano),
                        "timeUnixNano": str(end_time_unix_nano),
                        "asInt": "1",
                    }],
                },
            })

        envelope = {
            "resourceMetrics": [{
                "resource": {
                    "attributes": [
                        _attr("service.name", self.service_name),
                        _attr("service.instance.id", self.location_label),
                    ],
                },
                "scopeMetrics": [{
                    "scope": {"name": "dagster_community_components.dagster_runs_to_otlp_metrics_sensor"},
                    "metrics": metrics,
                }],
            }],
        }

        try:
            resp = requests.post(
                f"{endpoint}/v1/metrics",
                json=envelope,
                headers=headers,
                timeout=self.request_timeout_seconds,
            )
            resp.raise_for_status()
            context.log.info(
                f"Pushed run metrics → OTLP for {job_name} status={status} "
                f"duration_s={duration_seconds:.2f} metrics={len(metrics)}"
            )
        except Exception as exc:
            context.log.warning(f"Failed to push run metrics to OTLP: {exc}")


def _attr(key: str, value) -> dict:
    """One OTLP KeyValue attribute (string-typed)."""
    return {"key": key, "value": {"stringValue": str(value)}}
