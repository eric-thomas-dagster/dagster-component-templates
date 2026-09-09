"""StuckRunTerminatorJobComponent.

Op-shaped job that finds Dagster runs stuck in STARTING/STARTED/CANCELING
longer than a configurable threshold and marks them as CANCELED (or
FAILURE if you prefer). Useful for clearing zombie runs from the UI when
a worker died mid-flight without cleaning up its run status.

Every prod Dagster deployment eventually hits this — a K8s pod OOMs,
a worker container gets evicted, network partitions, etc. The run
status stays "RUNNING" forever until an operator manually clears it.
This job automates the sweep on a schedule.
"""

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


# States that are "in progress" — anything stuck in these past the
# threshold is a candidate for force-cancel.
IN_FLIGHT_STATUSES = (
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
    dg.DagsterRunStatus.CANCELING,
)


class StuckRunTerminatorJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-shaped job that force-cancels Dagster runs stuck in
    STARTING/STARTED/CANCELING beyond a threshold.

    Scans the last `scan_limit` runs matching those statuses; any whose
    `start_time` is older than `stuck_after_hours` is force-canceled
    (or failed, per `terminal_status`).
    """

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default="*/15 * * * *", description="Cron schedule; default every 15min.")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Dagster job tags")

    stuck_after_hours: float = Field(
        default=6.0,
        gt=0,
        description="Threshold in hours. Runs in-flight longer than this are candidates.",
    )
    scan_limit: int = Field(
        default=500,
        ge=1,
        description="Max in-flight runs to inspect per tick. Older stragglers are picked up in later ticks.",
    )
    terminal_status: str = Field(
        default="CANCELED",
        description="What to mark stuck runs as. 'CANCELED' (default, cleanest) or 'FAILURE' (to trigger alerting).",
    )
    dry_run: bool = Field(
        default=False,
        description="If True, list stuck runs in the log but don't actually change their status.",
    )
    tags_filter: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional tag filter. Only inspect runs matching ALL these tag key/value pairs.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _terminate(context: dg.OpExecutionContext):
            now = time.time()
            threshold_seconds = _self.stuck_after_hours * 3600.0

            runs = context.instance.get_runs(
                filters=dg.RunsFilter(
                    statuses=list(IN_FLIGHT_STATUSES),
                    tags=dict(_self.tags_filter) if _self.tags_filter else None,
                ),
                limit=_self.scan_limit,
            )

            stuck: List[Any] = []
            for r in runs:
                # Prefer start_time; fall back to create_timestamp.
                start = r.start_time or (
                    r.create_timestamp.timestamp() if hasattr(r.create_timestamp, "timestamp") else None
                )
                if start is None:
                    continue
                age_hours = (now - start) / 3600.0
                if age_hours >= _self.stuck_after_hours:
                    stuck.append((r, age_hours))

            if not stuck:
                context.log.info(
                    f"No stuck runs found (checked {len(runs)} in-flight runs, "
                    f"threshold={_self.stuck_after_hours}h)"
                )
                return {"terminated": 0, "checked": len(runs)}

            terminated_ids: List[str] = []
            for run, age_hours in stuck:
                context.log.warning(
                    f"Stuck run {run.run_id[:8]} (job={run.job_name}, "
                    f"status={run.status.value}, age={age_hours:.1f}h)"
                )
                if _self.dry_run:
                    continue

                # Log an engine event so operators see WHY the status
                # changed when they look at the run's timeline.
                try:
                    context.instance.report_engine_event(
                        message=(
                            f"stuck_run_terminator: force-canceling "
                            f"(stuck {age_hours:.1f}h > threshold {_self.stuck_after_hours}h)"
                        ),
                        dagster_run=run,
                    )
                except Exception as exc:  # noqa: BLE001
                    context.log.warning(f"Failed to emit engine event for {run.run_id}: {exc}")

                try:
                    if _self.terminal_status.upper() == "FAILURE":
                        context.instance.report_run_failed(run)
                    else:
                        # Standard cancel path: mark canceling → canceled.
                        context.instance.report_run_canceling(run)
                        context.instance.report_run_canceled(run)
                    terminated_ids.append(run.run_id)
                except Exception as exc:  # noqa: BLE001
                    context.log.error(f"Failed to terminate {run.run_id}: {exc}")

            return {
                "checked": len(runs),
                "stuck_found": len(stuck),
                "terminated": len(terminated_ids),
                "dry_run": _self.dry_run,
                "terminated_run_ids": terminated_ids[:20],  # cap for MetadataValue
            }

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _terminate()

        defs_kwargs: Dict[str, Any] = {"jobs": [_the_job]}
        if self.schedule:
            defs_kwargs["schedules"] = [dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule,
                job=_the_job,
                default_status=(
                    dg.DefaultScheduleStatus.STOPPED
                    if self.default_status.upper() == "STOPPED"
                    else dg.DefaultScheduleStatus.RUNNING
                ),
            )]
        return dg.Definitions(**defs_kwargs)
