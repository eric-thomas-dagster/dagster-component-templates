"""DagsterRunPruneJobComponent.

Op-shaped job that prunes old Dagster runs from run + event log storage.
Every prod deployment eventually needs this — the event log grows
unbounded and sqlite/postgres storage bloats. Dagster+ has retention
policies built-in for some tables but broadly customers still need
this control.

Two knobs: `max_age_days` (delete anything older) and
`keep_last_n_per_job` (safety net — always keep the last N per job so
you retain recent history even if `max_age_days` would sweep them).
Both apply — a run is pruned only if it's beyond BOTH thresholds.

Uses `instance.delete_run(run_id)` which cascades to event log entries
for that run — no separate event log cleanup needed.
"""

import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


TERMINAL_STATUSES = (
    dg.DagsterRunStatus.SUCCESS,
    dg.DagsterRunStatus.FAILURE,
    dg.DagsterRunStatus.CANCELED,
)


class DagsterRunPruneJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-shaped job that deletes old Dagster runs (+ their event log
    entries) beyond a configurable retention window."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default="0 4 * * *", description="Cron schedule; default daily at 4am.")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Dagster job tags")

    max_age_days: int = Field(
        default=30,
        ge=1,
        description="Delete runs older than this many days (based on create_timestamp).",
    )
    keep_last_n_per_job: int = Field(
        default=10,
        ge=0,
        description=(
            "Always keep the N most recent runs per job_name, even if they'd be pruned by max_age_days. "
            "Set 0 to disable this safety net."
        ),
    )
    only_terminal_states: bool = Field(
        default=True,
        description="If True, only prune runs in SUCCESS/FAILURE/CANCELED. Safer default.",
    )
    tags_filter: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional: only prune runs matching ALL these tag key/value pairs.",
    )
    max_deletes_per_run: int = Field(
        default=1000,
        ge=1,
        description="Hard cap on runs deleted per tick — prevents runaway sweeps. Older stragglers picked up next tick.",
    )
    dry_run: bool = Field(
        default=False,
        description="If True, log what WOULD be deleted without deleting.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _prune(context: dg.OpExecutionContext):
            now = time.time()
            age_threshold = now - (_self.max_age_days * 86400.0)

            # Pull candidate old runs. Query in pages until we've either
            # exhausted candidates or hit max_deletes_per_run.
            statuses = list(TERMINAL_STATUSES) if _self.only_terminal_states else None
            filters = dg.RunsFilter(
                statuses=statuses,
                tags=dict(_self.tags_filter) if _self.tags_filter else None,
                updated_before=age_threshold,  # updated_before uses epoch seconds
            )

            candidates = context.instance.get_runs(filters=filters, limit=_self.max_deletes_per_run * 2)
            context.log.info(f"Found {len(candidates)} candidate runs older than {_self.max_age_days} days.")

            if not candidates:
                return {"deleted": 0, "kept_by_recency": 0, "checked": 0}

            # Group by job_name and sort each group by create_timestamp DESC
            # so we can preserve the last N per job.
            per_job: Dict[str, list] = defaultdict(list)
            for r in candidates:
                per_job[r.job_name or "_unknown_job_"].append(r)

            to_delete: List[Any] = []
            kept_by_recency = 0

            # For each job, first find the total count of RECENT runs
            # (those NOT in candidates — inside the retention window)
            # to determine how many we need to keep from candidates.
            # Simpler approximation: if keep_last_n_per_job > 0, keep the
            # newest N candidates per job (since anything newer than
            # cutoff isn't in candidates and is already safe).
            for job_name, group in per_job.items():
                group.sort(key=lambda r: r.create_timestamp, reverse=True)
                # How many "recent" runs already exist for this job outside candidates?
                recent_count = 0
                try:
                    recent = context.instance.get_runs(
                        filters=dg.RunsFilter(
                            job_name=job_name,
                            updated_after=age_threshold,
                        ),
                        limit=_self.keep_last_n_per_job or 1,
                    )
                    recent_count = len(recent)
                except Exception:
                    recent_count = 0

                # Number to preserve from the candidate group
                need_more_to_keep = max(0, _self.keep_last_n_per_job - recent_count)
                if need_more_to_keep:
                    to_keep = group[:need_more_to_keep]
                    kept_by_recency += len(to_keep)
                    to_delete.extend(group[need_more_to_keep:])
                else:
                    to_delete.extend(group)

            # Apply the hard cap.
            to_delete = to_delete[:_self.max_deletes_per_run]

            if _self.dry_run:
                context.log.info(
                    f"[DRY RUN] Would delete {len(to_delete)} runs; would keep {kept_by_recency} "
                    f"by keep_last_n_per_job={_self.keep_last_n_per_job} safety net."
                )
                return {
                    "checked": len(candidates),
                    "would_delete": len(to_delete),
                    "kept_by_recency": kept_by_recency,
                    "dry_run": True,
                }

            deleted = 0
            for r in to_delete:
                try:
                    context.instance.delete_run(r.run_id)
                    deleted += 1
                except Exception as exc:  # noqa: BLE001
                    context.log.warning(f"Failed to delete run {r.run_id}: {exc}")

            context.log.info(
                f"Pruned {deleted} runs (checked {len(candidates)}, "
                f"kept {kept_by_recency} by recency safety net)."
            )
            return {
                "checked": len(candidates),
                "deleted": deleted,
                "kept_by_recency": kept_by_recency,
                "max_age_days": _self.max_age_days,
                "dry_run": False,
            }

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _prune()

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
