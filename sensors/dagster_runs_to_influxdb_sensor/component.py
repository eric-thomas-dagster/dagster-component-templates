"""DagsterRunsToInfluxdbSensorComponent.

Listen to Dagster run lifecycle events (succeeded / failed) and write
metrics to InfluxDB 2.x / 3.x via the `/api/v2/write` line-protocol
endpoint on each event. Also works against AWS Timestream for InfluxDB
— same SDK, same wire protocol.

Line-protocol shape per push:

    dagster_run,job_name=<name>,status=<success|failure>,location=<label> \
      duration_seconds=<float>,run_id="<uuid>",finished_ts=<int>i \
      <unix_nanos>

Tags: `job_name`, `status`, `location` (low-cardinality, indexed).
Fields: `duration_seconds`, `run_id`, `finished_ts`.

Pairs with:
  - `influxdb_resource` / `dataframe_to_influxdb` (data-plane sink)
"""
import os
from typing import List, Optional

import dagster as dg
from pydantic import Field


def _escape_lp(value: str) -> str:
    """InfluxDB line-protocol escape: spaces / commas / equals in tag values."""
    return value.replace(" ", r"\ ").replace(",", r"\,").replace("=", r"\=")


class DagsterRunsToInfluxdbSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Push Dagster run lifecycle metrics to InfluxDB 2.x / 3.x as line protocol.

    Example:

        ```yaml
        type: dagster_community_components.DagsterRunsToInfluxdbSensorComponent
        attributes:
          sensor_name: dagster_runs_to_influxdb
          base_url_env_var: INFLUX_URL      # e.g. http://localhost:8086
          token_env_var: INFLUX_TOKEN
          org: my-org
          bucket: dagster-runs
          measurement: dagster_run
          location_label: prod-east
        ```
    """

    sensor_name: str = Field(description="Dagster sensor name.")
    base_url_env_var: str = Field(description="Env var with InfluxDB base URL (e.g. http://localhost:8086).")
    token_env_var: str = Field(description="Env var with the InfluxDB write token.")
    org: str = Field(description="InfluxDB org name (URL query param).")
    bucket: str = Field(description="Destination bucket for the metrics.")
    measurement: str = Field(default="dagster_run", description="InfluxDB measurement name.")
    location_label: str = Field(
        default="default",
        description="Free-form low-cardinality tag — usually the Dagster deployment / region.",
    )
    monitor_jobs: Optional[List[str]] = Field(
        default=None,
        description="If set, only emit metrics for these Dagster job names. None = all.",
    )
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    request_timeout_seconds: int = Field(default=15, ge=1)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.run_status_sensor(
            name=self.sensor_name,
            run_status=dg.DagsterRunStatus.SUCCESS,
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
            default_status=(
                dg.DefaultSensorStatus.RUNNING
                if self.default_status.upper() == "RUNNING"
                else dg.DefaultSensorStatus.STOPPED
            ),
        )
        def _on_failure(context: dg.RunStatusSensorContext):
            _self._push_run(context, status="failure")

        return dg.Definitions(sensors=[_on_success, _on_failure])

    def _push_run(self, context: "dg.RunStatusSensorContext", status: str) -> None:
        try:
            import requests
        except ImportError:
            context.log.warning("requests not installed — skipping InfluxDB push.")
            return

        base_url = os.environ.get(self.base_url_env_var, "")
        token = os.environ.get(self.token_env_var, "")
        if not (base_url and token):
            context.log.warning(
                f"{self.base_url_env_var} / {self.token_env_var} not set — skipping push."
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
            import time
            duration_seconds = max(0.0, time.time() - float(run.start_time))

        finished_ts = int(run.end_time or 0)
        # Line protocol timestamp = unix nanos. Use end_time when available.
        ts_nanos = (finished_ts or int(__import__("time").time())) * 1_000_000_000

        tags = (
            f"job_name={_escape_lp(job_name)},"
            f"status={_escape_lp(status)},"
            f"location={_escape_lp(self.location_label)}"
        )
        fields = (
            f"duration_seconds={duration_seconds},"
            f'run_id="{run.run_id}",'
            f"finished_ts={finished_ts}i"
        )
        line = f"{_escape_lp(self.measurement)},{tags} {fields} {ts_nanos}\n"

        url = f"{base_url.rstrip('/')}/api/v2/write"
        headers = {
            "Authorization": f"Token {token}",
            "Content-Type": "text/plain; charset=utf-8",
        }
        params = {"org": self.org, "bucket": self.bucket, "precision": "ns"}
        try:
            resp = requests.post(
                url,
                data=line.encode("utf-8"),
                headers=headers,
                params=params,
                timeout=self.request_timeout_seconds,
            )
            resp.raise_for_status()
            context.log.info(
                f"Pushed Dagster run metrics → InfluxDB "
                f"for {job_name} status={status} duration={duration_seconds:.2f}s"
            )
        except Exception as exc:
            context.log.warning(f"Failed to push Dagster run metrics to InfluxDB: {exc}")
