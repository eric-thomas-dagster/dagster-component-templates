"""DagsterStalePartitionCleanupJobComponent.

Op-shaped job that finds partition materialization/observation events
for partition keys that no longer exist in the asset's current
PartitionsDefinition, and reports them (with an optional wipe step for
the entire asset's history).

Motivation: when you re-shape a partition (e.g., migrate a static-set
of partitions to a new set, or rename dynamic partitions) old
materialization events remain in the event log tied to defunct
partition keys. They clutter the partition status heat map and inflate
storage. This job detects them and reports counts; the actual cleanup
today requires wiping the whole asset (Dagster doesn't expose
per-partition-event delete). Wire that carefully.
"""

from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class DagsterStalePartitionCleanupJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-shaped job that detects orphaned partition materialization
    events for assets with modified PartitionsDefinitions."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default="0 6 * * 0", description="Cron schedule; default weekly Sunday at 6am.")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Dagster job tags")

    asset_keys: List[str] = Field(
        default_factory=list,
        description=(
            "Asset keys to check. Each user-string like `analytics/orders`. "
            "Leave empty to inspect ALL partitioned assets in the code location."
        ),
    )
    wipe_on_detection: bool = Field(
        default=False,
        description=(
            "If True, wipe the entire asset's materialization history when "
            "stale partitions are detected. Default False (report-only) — "
            "Dagster does not expose per-partition-event deletion so this is "
            "the only cleanup option, and it's destructive."
        ),
    )
    max_stale_before_wipe: int = Field(
        default=100,
        ge=1,
        description=(
            "Only wipe when at least this many stale partition keys are detected. "
            "Prevents wiping just because 1-2 partition keys got renamed."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=(), csv=(), tags=('tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _cleanup(context: dg.OpExecutionContext):
            repo = getattr(context, "repository_def", None)
            if repo is None:
                raise RuntimeError("context.repository_def unavailable — job must run in a loaded code location.")

            asset_graph = repo.asset_graph
            keys_to_check: List[dg.AssetKey]
            if _self.asset_keys:
                keys_to_check = [dg.AssetKey.from_user_string(k) for k in _self.asset_keys]
            else:
                keys_to_check = [
                    k for k in asset_graph.toposorted_asset_keys
                    if asset_graph.get(k).partitions_def is not None
                ]

            context.log.info(f"Inspecting {len(keys_to_check)} partitioned asset(s) for stale partition history.")

            per_asset: Dict[str, Dict[str, Any]] = {}
            total_wiped = 0

            for key in keys_to_check:
                key_str = key.to_user_string()
                node = asset_graph.get(key)
                pd = node.partitions_def
                if pd is None:
                    per_asset[key_str] = {"skipped": "not partitioned"}
                    continue

                # Current valid partition keys.
                try:
                    valid_keys = set(pd.get_partition_keys())
                except Exception as exc:  # noqa: BLE001
                    per_asset[key_str] = {"skipped": f"failed to enumerate current partition keys: {exc}"}
                    continue

                # Historical partition keys with materialization events.
                try:
                    historical = context.instance.get_materialized_partitions(key)
                except Exception:
                    # Older Dagster versions or missing method — fall back to full event scan.
                    historical = set()
                    try:
                        for rec in context.instance.get_event_records(
                            event_records_filter=dg.EventRecordsFilter(
                                event_type=dg.DagsterEventType.ASSET_MATERIALIZATION,
                                asset_key=key,
                            ),
                            limit=10000,
                        ):
                            am = rec.asset_materialization
                            pk = getattr(am, "partition", None) if am else None
                            if pk:
                                historical.add(pk)
                    except Exception:
                        historical = set()

                stale = set(historical) - valid_keys
                per_asset[key_str] = {
                    "current_partition_keys": len(valid_keys),
                    "historical_partition_keys": len(historical),
                    "stale_partition_keys": len(stale),
                    "sample_stale": sorted(stale)[:10],
                }

                if stale and _self.wipe_on_detection and len(stale) >= _self.max_stale_before_wipe:
                    try:
                        context.instance.wipe_assets([key])
                        per_asset[key_str]["wiped"] = True
                        total_wiped += 1
                        context.log.info(
                            f"{key_str}: WIPED {len(stale)} stale partition keys "
                            f"(exceeded threshold {_self.max_stale_before_wipe})"
                        )
                    except Exception as exc:  # noqa: BLE001
                        per_asset[key_str]["wipe_error"] = str(exc)
                        context.log.error(f"{key_str}: wipe failed: {exc}")

            return {
                "assets_checked": len(keys_to_check),
                "assets_wiped": total_wiped,
                "wipe_on_detection": _self.wipe_on_detection,
                "per_asset": per_asset,
            }

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _cleanup()

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
