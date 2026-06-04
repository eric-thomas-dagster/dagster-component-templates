"""DagsterRunsToSplunkHecSensorComponent.

Push Dagster run lifecycle events (success + failure) directly to
Splunk via the HTTP Event Collector (HEC). Parallels the existing
`dagster_runs_to_prometheus_sensor` (Push Gateway / VM) and
`dagster_runs_to_influxdb_sensor` (line protocol) — same
`@run_status_sensor` shape, Splunk-native wire format.

Use this if Splunk is your observability backend and you'd rather
ship run events directly via HEC than route them through an OTel
Collector. For the OTel-Collector path (works with Splunk too via
the Splunk Distribution of OpenTelemetry Collector), use
`dagster_runs_to_otlp_sensor` instead.

What lands in Splunk per run-status event:

    {
      "event": "Dagster run <id> for job <name> success (Xs)",
      "sourcetype": "dagster:run_event",
      "source": "dagster",
      "host": "<location_label>",
      "index": "<configured>",
      "fields": {
        "dagster_run_id":          "<uuid>",
        "dagster_job_name":        "<name>",
        "dagster_status":          "success" | "failure",
        "dagster_duration_seconds": <float>,
        "dagster_failure_step":    "<step>" (failure only)
      }
    }

Query in Splunk Web:
    search index=<index> dagster_status=failure | table _time, dagster_job_name, dagster_failure_step, _raw
"""
import json
import os
import time
from typing import List, Optional

import dagster as dg
from pydantic import Field


class DagsterRunsToSplunkHecSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Push Dagster run lifecycle events to Splunk via HEC.

    Example:

        ```yaml
        type: dagster_community_components.DagsterRunsToSplunkHecSensorComponent
        attributes:
          sensor_name: dagster_runs_to_splunk
          hec_url_env_var: SPLUNK_HEC_URL
          hec_token_env_var: SPLUNK_HEC_TOKEN
          index: dagster
          sourcetype: dagster:run_event
          location_label: prod-east
        ```
    """

    sensor_name: str = Field(description="Dagster sensor name.")
    hec_url_env_var: str = Field(
        description="Env var with Splunk HEC endpoint (e.g. https://splunk.acme.com:8088/services/collector).",
    )
    hec_token_env_var: str = Field(
        description="Env var with the Splunk HEC token.",
    )
    index: Optional[str] = Field(
        default=None,
        description="Splunk index to write into. Defaults to the HEC token's default.",
    )
    sourcetype: str = Field(
        default="dagster:run_event",
        description="Splunk `sourcetype` field.",
    )
    source: str = Field(
        default="dagster",
        description="Splunk `source` field.",
    )
    location_label: str = Field(
        default="default",
        description="Splunk `host` field — usually deployment / region.",
    )
    monitor_jobs: Optional[List[str]] = Field(
        default=None,
        description="If set, only emit for these Dagster job names. None = all.",
    )
    verify_ssl: bool = Field(
        default=True,
        description="Verify Splunk's TLS cert. Set false for self-signed dev splunks.",
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
            context.log.warning("requests not installed — skipping Splunk HEC push.")
            return

        hec_url = os.environ.get(self.hec_url_env_var, "")
        hec_token = os.environ.get(self.hec_token_env_var, "")
        if not (hec_url and hec_token):
            context.log.warning(
                f"{self.hec_url_env_var} / {self.hec_token_env_var} not set — skipping push."
            )
            return

        run = context.dagster_run
        job_name = run.job_name or "unknown_job"
        if self.monitor_jobs and job_name not in self.monitor_jobs:
            return

        duration_seconds = 0.0
        if run.start_time and run.end_time:
            duration_seconds = float(run.end_time - run.start_time)
        elif run.start_time:
            duration_seconds = max(0.0, time.time() - float(run.start_time))

        fields: dict = {
            "dagster_run_id": run.run_id,
            "dagster_job_name": job_name,
            "dagster_status": status,
            "dagster_duration_seconds": duration_seconds,
        }

        event_text = (
            f"Dagster run {run.run_id} for job {job_name} "
            f"{status} ({duration_seconds:.2f}s)"
        )

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
            fields["dagster_failure_step"] = failure_step or ""
            if err_msg:
                event_text = (
                    f"{event_text} step={failure_step or '?'} "
                    f"err={str(err_msg)[:500]}"
                )

        evt: dict = {
            "event": event_text,
            "sourcetype": self.sourcetype,
            "source": self.source,
            "host": self.location_label,
            "fields": fields,
        }
        if self.index:
            evt["index"] = self.index

        # Splunk HEC accepts a single JSON object (or newline-delimited
        # JSON for batches). One run event = one POST.
        headers = {
            "Authorization": f"Splunk {hec_token}",
            "Content-Type": "application/json",
        }
        try:
            resp = requests.post(
                hec_url.rstrip("/"),
                data=json.dumps(evt).encode("utf-8"),
                headers=headers,
                timeout=self.request_timeout_seconds,
                verify=self.verify_ssl,
            )
            resp.raise_for_status()
            context.log.info(
                f"Pushed run event → Splunk HEC for {job_name} status={status} "
                f"duration={duration_seconds:.2f}s"
            )
        except Exception as exc:
            context.log.warning(f"Failed to push run event to Splunk HEC: {exc}")
