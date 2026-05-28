"""DagsterRunsToPrometheusSensorComponent.

Listen to Dagster run lifecycle events (started / succeeded / failed) and
push metrics to a Prometheus-compatible endpoint on each event. Works
against:

  - Prometheus Push Gateway      → `target_kind: push_gateway`  (default)
  - VictoriaMetrics import API   → `target_kind: vm_import`
  - Mimir / Cortex remote-write  → `target_kind: remote_write`

All three speak Prometheus text format on push; Mimir / Cortex additionally
accept snappy-compressed protobuf via `/api/v1/push`. This component
ships the text-format paths today; remote_write is deferred to a v2 (the
prometheus_client lib's push_to_gateway covers all three target URLs but
not the snappy protobuf encoding — for Mimir at scale, customers
typically front Mimir with a Push Gateway anyway).

Metrics pushed per run-status event:

  dagster_run_duration_seconds{job_name, status, location}  <gauge>
  dagster_run_count{job_name, status, location}             <counter>
  dagster_run_last_finished_timestamp{job_name, status}     <gauge, unix>

The sensor is **stateless** w.r.t. cumulative counters — the gateway side
aggregates. Each run-finish event publishes its own metric line; the
push-gateway / VM stores it; Prometheus / Grafana queries it.

Pairs with:
  - `prometheus_resource` / `victoriametrics_resource`
  - `dataframe_to_prometheus` (data-plane sink — distinct from this
    operational-telemetry sensor)
"""
import os
from typing import List, Optional

import dagster as dg
from pydantic import Field


class DagsterRunsToPrometheusSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Push Dagster run lifecycle metrics to Prometheus Push Gateway / VM / Mimir.

    Example (local Push Gateway):

        ```yaml
        type: dagster_community_components.DagsterRunsToPrometheusSensorComponent
        attributes:
          sensor_name: dagster_runs_to_prometheus
          base_url_env_var: PROM_PUSHGATEWAY_URL    # e.g. http://localhost:9091
          target_kind: push_gateway
          push_job: dagster_runs
          location_label: prod-east
        ```

    Example (VictoriaMetrics):

        ```yaml
        type: dagster_community_components.DagsterRunsToPrometheusSensorComponent
        attributes:
          sensor_name: dagster_runs_to_vm
          base_url_env_var: VM_BASE_URL              # e.g. http://localhost:8428
          target_kind: vm_import
          push_job: dagster_runs
          location_label: prod-east
        ```
    """

    sensor_name: str = Field(description="Dagster sensor name.")
    base_url_env_var: str = Field(
        description="Env var with the target base URL (push gateway / VM / Mimir endpoint).",
    )
    target_kind: str = Field(
        default="push_gateway",
        description="One of: push_gateway | vm_import | remote_write. Controls the URL path used.",
    )
    push_job: str = Field(
        default="dagster_runs",
        description="Prometheus Push Gateway 'job' grouping key.",
    )
    location_label: str = Field(
        default="default",
        description="Free-form label added to every pushed metric — usually the Dagster deployment / region.",
    )
    monitor_jobs: Optional[List[str]] = Field(
        default=None,
        description="If set, only emit metrics for these Dagster job names. None = all.",
    )
    bearer_token_env_var: Optional[str] = Field(
        default=None,
        description="Env var with bearer token (vmauth / Push Gateway behind ingress).",
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
            context.log.warning("requests not installed — skipping Prometheus push.")
            return

        base_url = os.environ.get(self.base_url_env_var, "")
        if not base_url:
            context.log.warning(f"{self.base_url_env_var} not set — skipping push.")
            return

        run = context.dagster_run
        job_name = run.job_name or "unknown_job"
        if self.monitor_jobs and job_name not in self.monitor_jobs:
            return

        duration_seconds = 0.0
        if run.start_time and run.end_time:
            duration_seconds = float(run.end_time - run.start_time)
        elif run.start_time:
            import time
            duration_seconds = max(0.0, time.time() - float(run.start_time))

        finished_ts = int(run.end_time or 0)

        # Prometheus text-format body — three series per push.
        body = (
            f'# HELP dagster_run_duration_seconds Run duration in seconds.\n'
            f'# TYPE dagster_run_duration_seconds gauge\n'
            f'dagster_run_duration_seconds{{job_name="{job_name}",status="{status}",location="{self.location_label}"}} {duration_seconds}\n'
            f'# HELP dagster_run_count Cumulative count of completed runs by status.\n'
            f'# TYPE dagster_run_count counter\n'
            f'dagster_run_count{{job_name="{job_name}",status="{status}",location="{self.location_label}"}} 1\n'
            f'# HELP dagster_run_last_finished_timestamp Unix timestamp of last finished run.\n'
            f'# TYPE dagster_run_last_finished_timestamp gauge\n'
            f'dagster_run_last_finished_timestamp{{job_name="{job_name}",status="{status}",location="{self.location_label}"}} {finished_ts}\n'
        )

        url, headers = self._target_url_and_headers(base_url, job_name)
        try:
            resp = requests.post(
                url,
                data=body.encode("utf-8"),
                headers=headers,
                timeout=self.request_timeout_seconds,
            )
            resp.raise_for_status()
            context.log.info(
                f"Pushed Dagster run metrics → {self.target_kind} ({url}) "
                f"for {job_name} status={status} duration={duration_seconds:.2f}s"
            )
        except Exception as exc:
            context.log.warning(f"Failed to push Dagster run metrics: {exc}")

    def _target_url_and_headers(self, base_url: str, job_name: str) -> tuple:
        base_url = base_url.rstrip("/")
        headers = {"Content-Type": "text/plain; version=0.0.4"}
        if self.bearer_token_env_var and os.environ.get(self.bearer_token_env_var):
            headers["Authorization"] = f"Bearer {os.environ[self.bearer_token_env_var]}"

        if self.target_kind == "vm_import":
            # VictoriaMetrics: text-format ingest.
            return f"{base_url}/api/v1/import/prometheus", headers
        if self.target_kind == "remote_write":
            # Mimir / Cortex / VM remote-write — but we send text-format here
            # (which works for VM but NOT Mimir). Customers on Mimir at scale
            # should front it with a Push Gateway. Documented in the README.
            return f"{base_url}/api/v1/push", headers
        # Default: Push Gateway grouping URL.
        # POST /metrics/job/<job>/dagster_job/<dagster_job>
        return (
            f"{base_url}/metrics/job/{self.push_job}/dagster_job/{job_name}",
            headers,
        )
