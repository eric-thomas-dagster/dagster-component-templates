"""GraphFirstAssetsComponent for declaring assets with zero compute.

Lets a `defs.yaml` declare a list of real Dagster assets -- key, deps,
group_name, kinds, owners, metadata, partitions, automation_condition,
freshness -- entirely from YAML, where the materialization body is a
genuine no-op. For a "graph-first" fidelity mode: when the story is lineage,
checks, freshness, and automation, not real data, there's nothing for a real
integration component to fetch or write.

Plain-dict `assets:` entries (not `dg.ResolvedAssetSpec`) to match this
repo's own convention -- every other component here is plain pydantic
fields + inlined coercion helpers, never Dagster's `Resolved*`/`Resolver`
framework types, which also don't resolve correctly through
tools/validate_examples.py's plain `cls(**attributes)` harness (confirmed
directly: `ResolvedAssetSpec` only resolves via Dagster's real YAML-loading
pipeline, not plain Pydantic construction -- this component would have been
untestable by this repo's own tooling).
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Union  # noqa: E402,F401
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


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat


def _build_automation_condition(spec: Dict[str, Any]):
    """'eager' | {'kind': 'on_cron', 'cron_schedule': '...'} -> a real AutomationCondition.
    None (default) leaves automation unset -- the asset is never auto-materialized."""
    cond = spec.get("automation_condition")
    if not cond:
        return None
    if cond == "eager":
        return dg.AutomationCondition.eager()
    if cond == "on_missing":
        return dg.AutomationCondition.on_missing()
    if isinstance(cond, dict) and cond.get("kind") == "on_cron":
        cron = cond.get("cron_schedule")
        if not cron:
            raise ValueError("automation_condition.kind='on_cron' requires 'cron_schedule'")
        return dg.AutomationCondition.on_cron(cron)
    raise ValueError(
        f"unsupported automation_condition {cond!r}: use 'eager', 'on_missing', or "
        "{kind: on_cron, cron_schedule: '...'}"
    )


class GraphFirstAssetsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Declare a list of assets from YAML with a no-op materialization body.

    Example:
        ```yaml
        type: dagster_component_templates.GraphFirstAssetsComponent
        attributes:
          assets:
            - key: warehouse/orders
              group_name: warehouse
              kinds: [snowflake]
            - key: warehouse/customers
              group_name: warehouse
              deps: [warehouse/orders]
              metadata:
                owner: data-platform
              automation_condition: eager
              freshness_max_lag_minutes: 60
        ```

    Each list entry: `key` (required), `deps`, `group_name`, `description`,
    `kinds`, `owners`, `tags`, `metadata`, `code_version`, `skippable`,
    `partition_type`/`partition_start`/`partition_values`/`dynamic_partition_name`
    (same shape as the `ExternalXAsset` components), `automation_condition`
    (`'eager'`, `'on_missing'`, or `{kind: on_cron, cron_schedule: '...'}`),
    `freshness_max_lag_minutes` + optional `freshness_cron`.

    Materializing one of these assets logs a line and returns a
    `MaterializeResult` -- no data is read or written. Downstream components
    with a real `deps:` on one of these keys see real lineage, real checks
    can target them, and freshness/automation policies evaluate normally;
    there's just nothing behind the curtain.
    """

    assets: List[Dict[str, Any]] = Field(
        description=(
            "List of asset specs to declare. Each entry: `key` (required), plus optional "
            "`deps`, `group_name`, `description`, `kinds`, `owners`, `tags`, `metadata`, "
            "`code_version`, `skippable`, partition fields (`partition_type`/`partition_start`/"
            "`partition_values`/`dynamic_partition_name`), `automation_condition` "
            "('eager'/'on_missing'/{kind: on_cron, cron_schedule}), `freshness_max_lag_minutes` "
            "+ optional `freshness_cron`. Materializing one logs a line and returns "
            "MaterializeResult -- no data is read or written."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=(), csv=(), tags=())

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        assets_defs = []
        seen_keys = set()
        for i, entry in enumerate(self.assets):
            if not isinstance(entry, dict):
                raise ValueError(f"assets[{i}] must be a mapping, got {type(entry).__name__}")
            key_str = entry.get("key")
            if not key_str:
                raise ValueError(f"assets[{i}] is missing required field 'key'")
            key = dg.AssetKey.from_user_string(key_str)
            if key in seen_keys:
                raise ValueError(f"assets: duplicate key {key_str!r}")
            seen_keys.add(key)

            partitions_def = _build_partitions_def_entry(entry)
            freshness_policy = None
            if entry.get("freshness_max_lag_minutes") is not None:
                freshness_policy = _freshness_policy_compat(
                    entry["freshness_max_lag_minutes"], entry.get("freshness_cron"),
                )

            spec = dg.AssetSpec(
                key=key,
                deps=[dg.AssetKey.from_user_string(d) for d in (entry.get("deps") or [])],
                group_name=entry.get("group_name"),
                description=entry.get("description"),
                kinds=set(entry["kinds"]) if entry.get("kinds") else None,
                owners=entry.get("owners"),
                tags=entry.get("tags"),
                metadata=entry.get("metadata"),
                code_version=entry.get("code_version"),
                skippable=bool(entry.get("skippable", False)),
                partitions_def=partitions_def,
                automation_condition=_build_automation_condition(entry),
                freshness_policy=freshness_policy,
            )

            # A unique op name per entry, not the default (the function's own
            # name) -- every entry defines its own `_noop` function, and
            # Dagster's op-graph dependency resolution gets confused between
            # several same-named ops in the same graph (confirmed directly:
            # a lineage-only `deps:` edge between two assets whose ops were
            # both named `_noop` failed with "op _noop does not have input
            # ...", even though AssetSpec.deps is meant to need no input at
            # all -- unique names fixed it).
            op_name = "_".join(spec.key.path)

            def _make_asset(spec, op_name):
                @dg.multi_asset(specs=[spec], name=op_name)
                def _noop(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
                    context.log.info(f"graph-first no-op: {spec.key.to_user_string()}")
                    return dg.MaterializeResult(asset_key=spec.key)
                return _noop

            assets_defs.append(_make_asset(spec, op_name))

        return dg.Definitions(assets=assets_defs)


def _build_partitions_def_entry(entry: Dict[str, Any]) -> Optional[dg.PartitionsDefinition]:
    """Per-entry partitions_def from the same flat fields the ExternalXAsset components use."""
    partition_type = entry.get("partition_type")
    if not partition_type:
        return None
    from dagster import (
        DailyPartitionsDefinition, DynamicPartitionsDefinition, HourlyPartitionsDefinition,
        MonthlyPartitionsDefinition, StaticPartitionsDefinition, WeeklyPartitionsDefinition,
    )

    partition_start = entry.get("partition_start")
    partition_values = entry.get("partition_values")
    dynamic_partition_name = entry.get("dynamic_partition_name")
    if isinstance(partition_values, (list, tuple)):
        values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]

    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start (ISO date).")
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")
