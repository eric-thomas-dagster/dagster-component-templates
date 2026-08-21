"""StreamingRunHealthSensorComponent — self-heal a long-running consumer.

Companion to `StreamingConsumerComponent`. Runs on a fast poll cadence
(default 60s), queries the Dagster run store for any run of the target
job currently in RUNNING / STARTING / QUEUED / NOT_STARTED, and if none
is active, fires a `RunRequest` to launch a new one.

Gives a "24/7 always-on" asset from a chain of bounded runs — the
consumer exits cleanly at `max_seconds` (or crashes / times out), the
sensor detects no active run, launches the next. Restart gap is
bounded by `minimum_interval_seconds`.

Serverless notes:
- Set `minimum_interval_seconds` to a small value (e.g. 30-60s) so the
  restart gap is short.
- The sensor's own polling is essentially free — one `get_runs` query
  per interval.
"""
from typing import Optional

import dagster as dg
from dagster import (
    Component,
    ComponentLoadContext,
    DagsterRunStatus,
    DefaultSensorStatus,
    Definitions,
    RunRequest,
    RunsFilter,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)
from pydantic import Field


_ACTIVE_STATUSES = [
    DagsterRunStatus.QUEUED,
    DagsterRunStatus.NOT_STARTED,
    DagsterRunStatus.STARTING,
    DagsterRunStatus.STARTED,
]


class StreamingRunHealthSensorComponent(Component, dg.Model, dg.Resolvable):
    """Sensor that keeps a long-running job alive by relaunching when idle.

    Example:
        ```yaml
        type: dagster_community_components.StreamingRunHealthSensorComponent
        attributes:
          sensor_name: order_events_health
          job_name: __ASSET_JOB       # or a named job containing the streaming asset
          asset_selection: [order_events]   # optional — narrow to a subset
          minimum_interval_seconds: 60
        ```
    """

    sensor_name: str = Field(description="Unique sensor name.")
    job_name: str = Field(
        description=(
            "Job to watch + restart. For asset-based deployments this is "
            "typically the default `__ASSET_JOB`. Set explicitly if you're "
            "using a named job."
        ),
    )
    asset_selection: Optional[list] = Field(
        default=None,
        description=(
            "Optional list of asset keys to include in the launched RunRequest. "
            "Narrow the run to just the streaming asset (recommended). Omit "
            "to materialize whatever the job's default selection is."
        ),
    )
    minimum_interval_seconds: int = Field(
        default=60, ge=5,
        description="How often to check run health.",
    )
    default_status: str = Field(
        default="running",
        description="Sensor default status ('running' | 'stopped').",
    )
    description: Optional[str] = Field(
        default=None,
        description="Sensor description.",
    )

    @classmethod
    def get_description(cls) -> str:
        return "Health sensor that relaunches a long-running job whenever no run is currently active."

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        sensor_name = self.sensor_name
        job_name = self.job_name
        asset_selection = list(self.asset_selection) if self.asset_selection else None
        min_interval = self.minimum_interval_seconds
        default_status = (
            DefaultSensorStatus.RUNNING if self.default_status.lower() == "running"
            else DefaultSensorStatus.STOPPED
        )
        sensor_description = self.description or self.get_description()

        @sensor(
            name=sensor_name,
            job_name=job_name,
            minimum_interval_seconds=min_interval,
            default_status=default_status,
            description=sensor_description,
        )
        def _health_sensor(context: SensorEvaluationContext):
            # Look for any run of this job currently in flight.
            active = context.instance.get_runs(
                filters=RunsFilter(job_name=job_name, statuses=_ACTIVE_STATUSES),
                limit=1,
            )
            if active:
                run = active[0]
                return SkipReason(
                    f"job {job_name!r} already has an active run "
                    f"({run.run_id[:8]}, status={run.status.value})"
                )

            # No active run — fire one. Include a wall-clock run_key so
            # duplicate RunRequests within the same tick are deduped by
            # Dagster's run-key idempotency.
            import time as _time
            run_key = f"{sensor_name}_{int(_time.time() * 1000)}"
            kwargs = {"run_key": run_key}
            if asset_selection:
                from dagster import AssetKey
                kwargs["asset_selection"] = [AssetKey.from_user_string(k) for k in asset_selection]
            return SensorResult(run_requests=[RunRequest(**kwargs)])

        return Definitions(sensors=[_health_sensor])
