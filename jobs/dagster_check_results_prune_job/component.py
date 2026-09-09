"""DagsterCheckResultsPruneJobComponent.

Op-shaped job that prunes old AssetCheckResult history from the event
log. Similar spirit to `dagster_asset_materialization_prune_job` but
targets asset check evaluations.

Two modes:
  - `asset_keys=[...]` — prune check results for specific assets
  - `asset_keys=[]` — prune check results across ALL assets

Filter to only prune entries older than `max_age_days`. Defaults to
dry_run=True on the first invocation.
"""

import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class DagsterCheckResultsPruneJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-shaped job that deletes old asset-check evaluation events."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default="0 5 * * 0", description="Cron schedule; default weekly Sunday at 5am.")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Dagster job tags")

    asset_keys: List[str] = Field(
        default_factory=list,
        description=(
            "Asset keys whose check results to prune. Each is a user-string like "
            "`analytics/orders`. Leave empty to prune check results for ALL assets."
        ),
    )
    max_age_days: int = Field(
        default=30,
        ge=1,
        description="Delete check results older than this many days.",
    )
    max_deletes_per_run: int = Field(
        default=5000,
        ge=1,
        description="Hard cap on events deleted per tick.",
    )
    dry_run: bool = Field(
        default=True,
        description="If True (default), log what WOULD be deleted without deleting.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _prune(context: dg.OpExecutionContext):
            age_threshold = time.time() - (_self.max_age_days * 86400.0)

            keys = (
                [dg.AssetKey.from_user_string(k) for k in _self.asset_keys]
                if _self.asset_keys else [None]  # None = "all assets"
            )
            total_deleted = 0
            per_asset: Dict[str, int] = {}

            for key in keys:
                key_str = key.to_user_string() if key else "*"

                # Query old check-evaluation events for this key (or all).
                filter_kwargs = {
                    "event_type": dg.DagsterEventType.ASSET_CHECK_EVALUATION,
                    "before_timestamp": age_threshold,
                }
                if key is not None:
                    filter_kwargs["asset_key"] = key
                try:
                    old_records = context.instance.get_event_records(
                        event_records_filter=dg.EventRecordsFilter(**filter_kwargs),
                        limit=_self.max_deletes_per_run,
                        ascending=True,
                    )
                except Exception as exc:  # noqa: BLE001
                    context.log.warning(f"{key_str}: query failed: {exc}")
                    per_asset[key_str] = 0
                    continue

                records = list(old_records)
                context.log.info(
                    f"{key_str}: {len(records)} check-evaluation events older than "
                    f"{_self.max_age_days} days"
                )

                if _self.dry_run or not records:
                    per_asset[key_str] = 0
                    continue

                # Delete by storage_id. Not all storages expose a per-event
                # delete API, so this attempts the underlying event_log_storage.
                deleted_here = 0
                storage = context.instance.event_log_storage
                for rec in records:
                    storage_id = rec.storage_id
                    try:
                        if hasattr(storage, "delete_events"):
                            storage.delete_events([storage_id])
                            deleted_here += 1
                        else:
                            context.log.warning(
                                "event_log_storage doesn't expose delete_events; "
                                "cannot prune individual check-evaluation events on this backend"
                            )
                            break
                    except Exception as exc:  # noqa: BLE001
                        context.log.warning(f"{key_str}: delete failed for storage_id={storage_id}: {exc}")

                per_asset[key_str] = deleted_here
                total_deleted += deleted_here

            return {
                "dry_run": _self.dry_run,
                "max_age_days": _self.max_age_days,
                "total_deleted": total_deleted,
                "per_asset": per_asset,
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
