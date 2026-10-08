"""Aggregate Freshness Sensor Component.

Alerts when an ENTIRE selection of assets has gone silent -- not when any
one asset misses its own cadence. This is a different question than a
per-asset freshness policy (`post_processing`'s `attributes.freshness_policy`,
via `dg.FreshnessPolicy.time_window()`/`.cron()` -- see that mechanism for
per-asset SLAs). A tight aggregate window applied per-asset across a large,
mixed-cadence group would misfire constantly on its slower members; this
component instead asks one question per tick -- "has *anything at all* in
this selection produced an event recently" -- and answers it with exactly
ONE check result, not one per monitored asset.

Real motivation: a replication feed with ~250 landing tables on wildly
different cadences (hourly/daily/rare) needs a single "is the whole feed
still alive" signal distinct from each table's own SLA. Per-table policies
answer "is THIS table stale"; this component answers "did the entire feed
stop producing anything" -- the failure mode of an upstream system going
down entirely, as opposed to one table's individual schedule slipping.

Mechanism: a plain `@dg.sensor` polls `instance.get_latest_materialization_events`
for every resolved asset key in one batched call (and, optionally,
`fetch_observations` per key for assets that only ever get observed, not
materialized), takes the single most recent event across the WHOLE group,
and compares its age against `max_silence_seconds`. It reports the result as
ONE `AssetCheckEvaluation` against a single declared "rollup" asset --
confirmed directly that `SensorResult(asset_events=[...])` accepts
`AssetCheckEvaluation` objects and persists them queryably with no
pre-declared `AssetCheckSpec` required (`instance.report_runless_asset_event`).
The sensor itself never raises/fails on a stale selection -- it succeeds
every tick and reports a passed/failed check, so Dagster+'s native
"asset check failed" alert policy is the alerting mechanism, not a
sensor-tick failure (which should mean "this monitoring broke", not "the
thing it's monitoring is stale").

Selection resolution mirrors `asset_to_job_trigger_sensor`/
`enhanced_data_quality_checks`/`automation_condition_applicator`: an explicit
asset-key list, the full Dagster selection DSL via `AssetSelection.from_string()`
(tag:/group:/kind:/boolean composition), `"*"` for everything, or a bare
fnmatch glob fallback -- resolved against sibling assets in the same defs
folder.
"""
import time
from typing import Any, List, Optional, Union

import dagster as dg
from pydantic import Field


def _discover_sibling_assets(context: dg.ComponentLoadContext):
    """Returns (list_of_key_strings, sibling_defs). Same mechanism
    `asset_to_job_trigger_sensor`/`enhanced_data_quality_checks` use: load
    sibling components in the same defs folder so `sibling_defs.resolve_asset_graph()`
    can power the full Dagster selection language via `AssetSelection.from_string()`."""
    keys: List[str] = []
    sibling_defs: Optional[dg.Definitions] = None
    try:
        parent_path = context.path.parent if hasattr(context.path, "parent") else None
        if parent_path:
            sibling_defs = context.build_defs(parent_path)
            if sibling_defs and sibling_defs.assets:
                for assets_def in sibling_defs.assets:
                    for key in assets_def.keys:
                        keys.append(key.to_user_string())
    except Exception:
        pass
    return keys, sibling_defs


def _resolve_selection(
    selection: Union[str, List[str]],
    discovered_keys: List[str],
    sibling_defs: Optional[dg.Definitions],
) -> List[str]:
    """Resolve `monitored_selection` into a list of asset key strings.

    - Explicit list:       ["sap/bseg", "sap/konv"]
    - All assets:          "*"
    - Group:               "group:sap_hourly"
    - Tag:                 "tag:cadence=hourly"
    - Kind:                "kind:snowflake"
    - Boolean composition: "group:sap_landing and tag:cadence=hourly"
    - Bare fnmatch glob:   "sap/*" (backward-compat fallback)
    """
    import fnmatch

    if isinstance(selection, list):
        return selection

    if not discovered_keys:
        return []

    if selection == "*":
        return list(discovered_keys)

    if sibling_defs is not None:
        try:
            graph = sibling_defs.resolve_asset_graph()
            matched = dg.AssetSelection.from_string(selection).resolve(graph)
            if matched:
                return sorted(k.to_user_string() for k in matched)
        except Exception:
            pass

    return [k for k in discovered_keys if fnmatch.fnmatch(k, selection)]


class AggregateFreshnessSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Alert when an entire selection of assets has gone silent, as one
    check result -- not hundreds of per-asset alerts.

    Example -- alert if NOTHING across the whole hourly SAP tier has loaded
    in 10 minutes, distinct from each table's own per-table freshness policy:

        type: dagster_community_components.AggregateFreshnessSensorComponent
        attributes:
          sensor_name: sap_hourly_total_stoppage_sensor
          monitored_selection: "group:sap_hourly"
          rollup_asset_key: sap_ecc/hourly_feed_health
          max_silence_seconds: 600
          minimum_interval_seconds: 60
    """

    sensor_name: str = Field(description="Unique sensor name.")
    monitored_selection: Union[str, List[str]] = Field(
        description=(
            "Asset selection to monitor in aggregate: explicit key list, the Dagster "
            "selection DSL (tag:/group:/kind:/boolean composition), '*' for everything, "
            "or a bare fnmatch glob. Resolved against sibling assets in the same defs folder."
        )
    )
    rollup_asset_key: str = Field(
        description=(
            "Asset key for the single declared 'rollup' asset the aggregate check is "
            "reported against (e.g. 'sap_ecc/hourly_feed_health'). Declare-only -- this "
            "component never materializes it, it only exists to carry the check."
        )
    )
    max_silence_seconds: int = Field(
        description=(
            "Fail the aggregate check if the most recent materialization across the "
            "WHOLE monitored selection is older than this many seconds -- or if nothing "
            "in the selection has ever materialized at all."
        )
    )
    check_name: str = Field(
        default="total_stoppage",
        description="Name of the asset check reported against rollup_asset_key.",
    )
    include_observations: bool = Field(
        default=False,
        description=(
            "Also consider AssetObservation events (not just materializations) when "
            "finding the most recent event per monitored asset. Costs one extra query "
            "per monitored asset per tick, so it's opt-in -- leave off for large "
            "selections unless some monitored assets are observation-only."
        ),
    )
    minimum_interval_seconds: int = Field(
        default=60, description="Minimum seconds between sensor evaluations."
    )
    default_status: str = Field(
        default="stopped",
        description="'running' or 'stopped' -- initial status of the sensor.",
    )
    group_name: Optional[str] = Field(
        default=None, description="Group name for the declared rollup asset."
    )
    description: Optional[str] = Field(
        default=None, description="Description for the declared rollup asset."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.default_status not in ("running", "stopped"):
            raise ValueError(
                f"AggregateFreshnessSensorComponent: default_status must be 'running' or "
                f"'stopped', got {self.default_status!r}."
            )

        discovered_keys, sibling_defs = _discover_sibling_assets(context)
        resolved_keys = _resolve_selection(self.monitored_selection, discovered_keys, sibling_defs)
        if not resolved_keys:
            raise ValueError(
                f"AggregateFreshnessSensorComponent {self.sensor_name!r}: monitored_selection "
                f"{self.monitored_selection!r} matched no assets (discovered {len(discovered_keys)} "
                f"sibling asset(s))."
            )

        monitored_asset_keys = [dg.AssetKey(k.split("/")) for k in resolved_keys]
        rollup_key = dg.AssetKey(self.rollup_asset_key.split("/"))
        rollup_spec = dg.AssetSpec(
            key=rollup_key,
            group_name=self.group_name,
            description=self.description
            or (
                f"Aggregate freshness rollup for {len(monitored_asset_keys)} asset(s) matching "
                f"{self.monitored_selection!r}. Declare-only -- carries the "
                f"{self.check_name!r} check, never materialized directly."
            ),
            kinds={"monitor"},
        )

        sensor_name = self.sensor_name
        check_name = self.check_name
        max_silence_seconds = self.max_silence_seconds
        include_observations = self.include_observations
        default_status = (
            dg.DefaultSensorStatus.RUNNING
            if self.default_status == "running"
            else dg.DefaultSensorStatus.STOPPED
        )

        @dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=default_status,
        )
        def _aggregate_freshness_sensor(context: dg.SensorEvaluationContext):
            instance = context.instance
            latest_by_key = instance.get_latest_materialization_events(monitored_asset_keys)

            freshest_timestamp: Optional[float] = None
            freshest_key: Optional[dg.AssetKey] = None
            never_seen: List[str] = []

            for key in monitored_asset_keys:
                entry = latest_by_key.get(key)
                candidate_timestamp = entry.timestamp if entry is not None else None

                if include_observations:
                    try:
                        obs_result = instance.fetch_observations(key, limit=1, ascending=False)
                        if obs_result.records:
                            obs_timestamp = obs_result.records[0].timestamp
                            if candidate_timestamp is None or obs_timestamp > candidate_timestamp:
                                candidate_timestamp = obs_timestamp
                    except Exception:
                        pass

                if candidate_timestamp is None:
                    never_seen.append(key.to_user_string())
                elif freshest_timestamp is None or candidate_timestamp > freshest_timestamp:
                    freshest_timestamp = candidate_timestamp
                    freshest_key = key

            now = time.time()
            age_seconds = (now - freshest_timestamp) if freshest_timestamp is not None else None
            passed = age_seconds is not None and age_seconds <= max_silence_seconds

            metadata: dict[str, Any] = {
                "monitored_asset_count": dg.MetadataValue.int(len(monitored_asset_keys)),
                "max_silence_seconds": dg.MetadataValue.int(max_silence_seconds),
                "never_seen_count": dg.MetadataValue.int(len(never_seen)),
            }
            if freshest_key is not None:
                metadata["freshest_asset_key"] = dg.MetadataValue.text(freshest_key.to_user_string())
                metadata["freshest_event_age_seconds"] = dg.MetadataValue.float(age_seconds)
            if never_seen:
                metadata["never_seen_assets"] = dg.MetadataValue.json(never_seen[:50])

            if passed:
                description = (
                    f"Freshest event in selection was {age_seconds:.0f}s ago "
                    f"(threshold {max_silence_seconds}s) -- {freshest_key.to_user_string()}."
                )
            elif freshest_timestamp is None:
                description = (
                    f"No materialization (or observation) ever recorded for any of the "
                    f"{len(monitored_asset_keys)} monitored asset(s) -- entire selection "
                    f"appears silent."
                )
            else:
                description = (
                    f"Nothing in the monitored selection has produced an event in "
                    f"{age_seconds:.0f}s (threshold {max_silence_seconds}s) -- most recent "
                    f"was {freshest_key.to_user_string()}."
                )

            evaluation = dg.AssetCheckEvaluation(
                asset_key=rollup_key,
                check_name=check_name,
                passed=passed,
                severity=dg.AssetCheckSeverity.ERROR,
                description=description,
                metadata=metadata,
            )
            return dg.SensorResult(asset_events=[evaluation])

        return dg.Definitions(assets=[rollup_spec], sensors=[_aggregate_freshness_sensor])
