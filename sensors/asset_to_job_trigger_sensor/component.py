"""Asset-to-Job Trigger Sensor Component.

Bridges an upstream asset event to a job-shaped target -- the one thing
`post_processing`'s `deps:`/`automation_condition:` can't do, since those
only attach to real assets (AssetSpec/AssetsDefinition), not a bare `@dg.job`
wrapping an arbitrary op. A plain job can't be made reactive via
AutomationCondition either -- that requires an asset-backed job built via
`define_asset_job`, which a bare-op job isn't.

Real, concrete case this exists for: a component that mirrors an external
system's own pre-existing jobs (e.g. `enriched_dbt_cloud_workspace`'s
`mirror_jobs: job`, which triggers a *specific, already-configured* dbt
Cloud job by ID rather than letting Dagster decide what to build) produces
a job-shaped trigger on purpose, to preserve that external job's own
configuration. Reacting to an upstream event for a job-shaped target needs
an explicit sensor; this is that sensor, built once and configured per
case instead of hand-written each time.

Selection resolution mirrors `enhanced_data_quality_checks` /
`automation_condition_applicator` / `aggregate_freshness_sensor`: an explicit
asset-key list, the full Dagster selection DSL via
`AssetSelection.from_string()` (tag:/group:/kind:/boolean composition),
`"*"` for everything, or a bare fnmatch glob fallback -- resolved against
sibling assets in the same defs folder.
"""
from typing import Any, List, Optional, Union

import dagster as dg
from pydantic import Field


def _discover_sibling_assets(context: dg.ComponentLoadContext):
    """Returns (list_of_key_strings, sibling_defs). Same mechanism
    `enhanced_data_quality_checks`/`aggregate_freshness_sensor` use: load
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


class AssetToJobTriggerSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Monitor an asset selection; fire a `RunRequest` at a named job when
    any matched asset gets a new materialization.

    Example:

        type: dagster_component_templates.AssetToJobTriggerSensorComponent
        attributes:
          sensor_name: electricera_pipe_to_dbt_trigger_sensor
          monitored_selection: "group:electricera_snowpipes"
          job_name: fuel_and_trading_electricera_prod_job
          minimum_interval_seconds: 30
    """

    sensor_name: str = Field(description="Unique sensor name.")
    monitored_selection: Union[str, List[str]] = Field(
        description=(
            "Asset selection to monitor: explicit key list, the Dagster selection DSL "
            "(tag:/group:/kind:/boolean composition), '*' for everything, or a bare "
            "fnmatch glob. Resolved against sibling assets in the same defs folder."
        )
    )
    job_name: str = Field(description="Name of the job to run via RunRequest when a monitored asset updates.")
    minimum_interval_seconds: int = Field(
        default=30, description="Minimum seconds between sensor evaluations."
    )
    default_status: str = Field(
        default="stopped",
        description="'running' or 'stopped' -- initial status of the sensor.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.default_status not in ("running", "stopped"):
            raise ValueError(
                f"AssetToJobTriggerSensorComponent: default_status must be 'running' or "
                f"'stopped', got {self.default_status!r}."
            )

        discovered_keys, sibling_defs = _discover_sibling_assets(context)
        resolved_keys = _resolve_selection(self.monitored_selection, discovered_keys, sibling_defs)
        if not resolved_keys:
            raise ValueError(
                f"AssetToJobTriggerSensorComponent {self.sensor_name!r}: monitored_selection "
                f"{self.monitored_selection!r} matched no assets (discovered {len(discovered_keys)} "
                f"sibling asset(s))."
            )

        monitored_assets = dg.AssetSelection.assets(
            *[dg.AssetKey(k.split("/")) for k in resolved_keys]
        )
        sensor_name = self.sensor_name
        job_name = self.job_name
        default_status = (
            dg.DefaultSensorStatus.RUNNING
            if self.default_status == "running"
            else dg.DefaultSensorStatus.STOPPED
        )

        @dg.multi_asset_sensor(
            name=sensor_name,
            monitored_assets=monitored_assets,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=default_status,
        )
        def _sensor(context: dg.MultiAssetSensorEvaluationContext):
            asset_events = context.latest_materialization_records_by_key()
            new_keys = [key for key, record in asset_events.items() if record is not None]
            if not new_keys:
                return
            context.advance_all_cursors()
            yield dg.RunRequest(run_key=f"{sensor_name}_{context.cursor}", job_name=job_name)

        return dg.Definitions(sensors=[_sensor])
