"""DagsterAssetMaterializationPruneJobComponent.

Op-shaped job that wipes old asset materialization history for
configured asset keys. Complements `dagster_run_prune_job` (which
prunes RUNS + their events); this one specifically targets
materialization history for one or more assets — useful for
high-frequency assets that would otherwise accumulate huge event
histories.

Uses `instance.wipe_assets([...])` which nukes ALL materialization
history for the listed asset keys. To keep the most recent N,
this component records the N newest materialization timestamps
per asset BEFORE wiping and re-emits them as synthetic
materialization events with `_re_emitted_by_prune_job` metadata
tags so the recent history is preserved (visible but marked).

Note: this is a destructive operation. Default is dry_run=True so the
first run just logs what would happen. Explicitly flip to False when
you're ready.
"""

import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class DagsterAssetMaterializationPruneJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-shaped job that wipes asset materialization history for
    configured assets, optionally preserving the last N per asset via
    synthetic re-emission."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default="0 5 * * 0", description="Cron schedule; default weekly Sunday at 5am.")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Dagster job tags")

    asset_keys: List[str] = Field(
        description=(
            "Asset keys to prune materialization history for. Each is a "
            "user-string like `analytics/orders` or `orders`. All history "
            "for these keys is wiped (see keep_last_n_per_asset for retention)."
        ),
    )
    keep_last_n_per_asset: int = Field(
        default=100,
        ge=0,
        description=(
            "Preserve this many most-recent materialization events per asset. "
            "The event log is wiped for each asset, then the newest N events "
            "are re-emitted as synthetic materializations tagged "
            "`_re_emitted_by_prune_job=true`. Set 0 to wipe everything."
        ),
    )
    dry_run: bool = Field(
        default=True,
        description="If True (default), log what WOULD be wiped without wiping.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _prune(context: dg.OpExecutionContext):
            keys = [dg.AssetKey.from_user_string(k) for k in _self.asset_keys]
            summary: Dict[str, Any] = {"assets": {}, "dry_run": _self.dry_run}
            total_wiped = 0

            for key in keys:
                key_str = key.to_user_string()
                asset_summary: Dict[str, Any] = {"key": key_str}

                # Fetch the newest N events to preserve.
                preserved: List[Any] = []
                if _self.keep_last_n_per_asset > 0:
                    try:
                        preserved_records = context.instance.get_event_records(
                            event_records_filter=dg.EventRecordsFilter(
                                event_type=dg.DagsterEventType.ASSET_MATERIALIZATION,
                                asset_key=key,
                            ),
                            limit=_self.keep_last_n_per_asset,
                            ascending=False,
                        )
                        preserved = list(preserved_records)
                    except Exception as exc:  # noqa: BLE001
                        context.log.warning(f"Failed to fetch preserved events for {key_str}: {exc}")

                asset_summary["preserved_count"] = len(preserved)
                context.log.info(
                    f"{key_str}: will preserve {len(preserved)} most-recent materializations"
                )

                if _self.dry_run:
                    asset_summary["action"] = "dry_run_skipped"
                    summary["assets"][key_str] = asset_summary
                    continue

                # Wipe the entire asset history.
                try:
                    context.instance.wipe_assets([key])
                    context.log.info(f"{key_str}: wiped all materialization history")
                except Exception as exc:  # noqa: BLE001
                    context.log.error(f"{key_str}: wipe failed: {exc}")
                    asset_summary["action"] = "wipe_failed"
                    asset_summary["error"] = str(exc)
                    summary["assets"][key_str] = asset_summary
                    continue

                # Re-emit the preserved events as synthetic materializations
                # tagged so users know these are archival re-emissions.
                re_emitted = 0
                for record in preserved:
                    try:
                        orig = record.asset_materialization
                        if orig is None:
                            continue
                        # Preserve original metadata + mark as re-emitted.
                        new_metadata = dict(orig.metadata or {})
                        new_metadata["_re_emitted_by_prune_job"] = dg.MetadataValue.bool(True)
                        new_metadata["_original_timestamp"] = dg.MetadataValue.text(
                            str(record.event_log_entry.timestamp)
                        )
                        context.instance.report_runless_asset_event(
                            dg.AssetMaterialization(
                                asset_key=key,
                                description=orig.description,
                                metadata=new_metadata,
                            )
                        )
                        re_emitted += 1
                    except Exception as exc:  # noqa: BLE001
                        context.log.warning(f"{key_str}: failed to re-emit event: {exc}")

                asset_summary["re_emitted"] = re_emitted
                asset_summary["action"] = "wiped_and_re_emitted"
                total_wiped += 1
                summary["assets"][key_str] = asset_summary

            summary["assets_wiped"] = total_wiped
            return summary

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
