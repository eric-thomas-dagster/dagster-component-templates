"""DagsterRunsToStatsdSensorComponent.

Push Dagster run lifecycle events (SUCCESS / FAILURE) as StatsD metrics
over UDP. No external Python deps — just raw datagrams.

Targets the Datadog Agent, local statsd, etcd-statsd, the
graphiteapp/graphite-statsd Docker image, or any DogStatsD-compatible
receiver. By default emits DogStatsD-style tags (`#k:v,k:v`); set
`dogstatsd_tags: false` for plain statsd protocol (no tag block).

Metrics emitted per run:

  dagster.runs.total                   counter   +1     tags: status, job
  dagster.runs.duration_milliseconds   timer     X.Xms  tags: status, job
  dagster.runs.failures                counter   +1     tags: job, step  (failure only)

Parallels `dagster_runs_to_prometheus_sensor` (push gateway / VictoriaMetrics),
`dagster_runs_to_influxdb_sensor` (line protocol), and the existing
`dagster_runs_to_otlp_sensor` (which emits OTel **logs**, not metrics).
"""
import socket
import time
from typing import List, Optional

import dagster as dg
from pydantic import Field


class DagsterRunsToStatsdSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Emit Dagster run lifecycle events as StatsD/DogStatsD UDP datagrams.

    Example (Datadog Agent on the local host):

        ```yaml
        type: dagster_component_templates.DagsterRunsToStatsdSensorComponent
        attributes:
          sensor_name: dagster_runs_to_dd
          statsd_host: 127.0.0.1
          statsd_port: 8125
          metric_prefix: dagster
          dogstatsd_tags: true
          extra_tags:
            env: prod
            team: data-platform
        ```

    Example (plain statsd via graphite-statsd Docker):

        ```yaml
        type: dagster_component_templates.DagsterRunsToStatsdSensorComponent
        attributes:
          sensor_name: dagster_runs_to_local_statsd
          statsd_host: 127.0.0.1
          statsd_port: 8125
          metric_prefix: dagster
          dogstatsd_tags: false
        ```
    """

    sensor_name: str = Field(description="Dagster sensor name.")
    statsd_host: str = Field(
        default="127.0.0.1",
        description="StatsD server host. For Datadog Agent on the same host, 127.0.0.1.",
    )
    statsd_port: int = Field(
        default=8125,
        description="StatsD UDP port (default 8125 for both statsd and Datadog Agent).",
    )
    metric_prefix: str = Field(
        default="dagster",
        description="Prefix applied to every metric name (e.g. 'dagster.runs.total').",
    )
    dogstatsd_tags: bool = Field(
        default=True,
        description="When true, append `|#k:v,k:v` tag block (Datadog Agent, modern statsd, etcd). When false, omit tags entirely (plain statsd protocol).",
    )
    extra_tags: Optional[dict] = Field(
        default=None,
        description="Static tags applied to every metric (e.g. {env: prod, team: data}). Merged with per-event tags.",
    )
    monitor_jobs: Optional[List[str]] = Field(
        default=None,
        description="If set, only emit for these Dagster job names. None = all.",
    )
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")

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

    # --------------------------------------------------------------- push

    def _push(self, context: "dg.RunStatusSensorContext", status: str) -> None:
        run = context.dagster_run
        job_name = run.job_name or "unknown_job"
        if self.monitor_jobs and job_name not in self.monitor_jobs:
            return

        duration_seconds = 0.0
        if run.start_time and run.end_time:
            duration_seconds = float(run.end_time - run.start_time)
        elif run.start_time:
            duration_seconds = max(0.0, time.time() - float(run.start_time))
        duration_ms = duration_seconds * 1000.0

        base_tags = dict(self.extra_tags or {})
        base_tags["status"] = status
        base_tags["job"] = job_name

        prefix = self.metric_prefix.rstrip(".")
        lines: List[str] = []
        lines.append(self._format(f"{prefix}.runs.total", 1, "c", base_tags))
        lines.append(self._format(f"{prefix}.runs.duration_milliseconds", duration_ms, "ms", base_tags))

        if status == "failure":
            failure_step = None
            try:
                fe = getattr(context, "failure_event", None)
                if fe:
                    failure_step = getattr(fe, "step_key", None)
            except Exception:
                pass
            failure_tags = dict(base_tags)
            if failure_step:
                failure_tags["step"] = failure_step
            lines.append(self._format(f"{prefix}.runs.failures", 1, "c", failure_tags))

        # One UDP datagram per metric — keeps things simple and well under MTU.
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                for line in lines:
                    sock.sendto(line.encode("utf-8"), (self.statsd_host, self.statsd_port))
            finally:
                sock.close()
            context.log.info(
                f"StatsD push {self.statsd_host}:{self.statsd_port} "
                f"job={job_name} status={status} duration_ms={duration_ms:.1f} metrics={len(lines)}"
            )
        except Exception as exc:
            context.log.warning(f"Failed to push to StatsD: {exc}")

    def _format(self, name: str, value, kind: str, tags: dict) -> str:
        """One StatsD line: name:value|kind[|#k:v,k:v]."""
        if kind == "c":
            val_s = f"{int(value)}"
        else:
            val_s = f"{float(value):.4f}".rstrip("0").rstrip(".") or "0"
        line = f"{name}:{val_s}|{kind}"
        if self.dogstatsd_tags and tags:
            parts = [f"{k}:{_sanitize_tag(str(v))}" for k, v in tags.items() if v is not None]
            if parts:
                line += "|#" + ",".join(parts)
        return line


def _sanitize_tag(v: str) -> str:
    """StatsD tags can't contain commas / pipes. Replace with underscores."""
    return v.replace(",", "_").replace("|", "_").replace("#", "_")
