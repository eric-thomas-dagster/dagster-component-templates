"""Bulk Freshness Policies Component.

Bulk sibling of `FreshnessPolicyComponent` — one config block declares
freshness policies for many assets at once, grouped by cadence tier,
instead of one component instance per asset.

Real-world motivation: a replication feed with hundreds of landing tables
on wildly different arrival cadences (some hourly, some daily, some rare)
needs every table's expected cadence declared somewhere, but hand-writing
one `FreshnessPolicyComponent` YAML block per table doesn't scale and
doesn't read as "group default" — the policy tiers ARE the real structure
(a handful of cadences), not the individual tables.

Each group's `selection` supports the same selection language this repo's
other bulk components already use (`enhanced_data_quality_checks`,
`automation_condition_applicator`): an explicit list of asset key strings,
the full Dagster selection DSL via `AssetSelection.from_string()`
(`tag:cadence=hourly`, `group:sap_landing`, `kind:snowflake`, boolean
composition), or a bare fnmatch glob as a backward-compatible fallback.
Resolution happens against SIBLING components in the same defs folder
(same discovery mechanism `enhanced_data_quality_checks` uses) — so tables
tagged or grouped elsewhere in the project are picked up with zero
per-table config here; a new table onboards by carrying the right tag,
not by being hand-listed in this component's own YAML.

Each resolved group becomes one real `build_last_update_freshness_checks`
call (the same factory `FreshnessPolicyComponent` uses) — Dagster's own
API already accepts a list of asset keys per policy, so a tier with 80
tables costs one call, not 80.
"""
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field, model_validator


def _discover_sibling_assets(context: dg.ComponentLoadContext):
    """Returns (list_of_key_strings, sibling_defs). Same mechanism
    `enhanced_data_quality_checks` uses: load sibling components in the
    same defs folder so `sibling_defs.resolve_asset_graph()` can power
    the full Dagster selection language via `AssetSelection.from_string()`."""
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
    """Resolve a group's `selection` into a list of asset key strings.

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


class BulkFreshnessPoliciesComponent(dg.Component, dg.Model, dg.Resolvable):
    """Bulk-declare freshness policies for many assets, grouped by cadence
    tier, instead of one `FreshnessPolicyComponent` instance per asset.

    Example — three cadence tiers covering a 250-table replication feed,
    selected by tag rather than hand-listed:

        type: dagster_community_components.BulkFreshnessPoliciesComponent
        attributes:
          groups:
            - name: hourly_tables
              policy_type: time_window
              fail_window_hours: 2
              selection: "tag:cadence=hourly"
            - name: daily_tables
              policy_type: time_window
              fail_window_hours: 30
              selection: "tag:cadence=daily"
            - name: rare_tables
              policy_type: cron
              deadline_cron: "0 6 * * 1"
              lower_bound_delta_hours: 24
              selection: "group:sap_rare"
    """

    groups: List[Dict[str, Any]] = Field(
        description=(
            "List of {name, policy_type, selection, ...policy fields}. Same "
            "policy_type/field shape as FreshnessPolicyComponent (time_window: "
            "fail_window_hours; cron: deadline_cron + lower_bound_delta_hours + "
            "optional timezone). `selection` is an explicit asset-key list, the "
            "full Dagster selection DSL (tag:/group:/kind:/boolean composition), "
            "'*' for everything, or a bare fnmatch glob."
        )
    )

    @model_validator(mode="after")
    def validate_groups(self):
        if not self.groups:
            raise ValueError("BulkFreshnessPoliciesComponent: groups must be non-empty.")
        seen_names = set()
        for g in self.groups:
            name = g.get("name")
            if not name:
                raise ValueError(f"BulkFreshnessPoliciesComponent: every group needs a 'name'. Got: {g}")
            if name in seen_names:
                raise ValueError(f"BulkFreshnessPoliciesComponent: duplicate group name {name!r}.")
            seen_names.add(name)

            policy_type = g.get("policy_type", "time_window")
            if policy_type not in ("time_window", "cron"):
                raise ValueError(f"group {name!r}: policy_type must be 'time_window' or 'cron', got {policy_type!r}.")
            if policy_type == "time_window" and g.get("fail_window_hours") is None:
                raise ValueError(f"group {name!r}: policy_type='time_window' requires fail_window_hours.")
            if policy_type == "cron":
                if not g.get("deadline_cron"):
                    raise ValueError(f"group {name!r}: policy_type='cron' requires deadline_cron.")
                if g.get("lower_bound_delta_hours") is None:
                    raise ValueError(f"group {name!r}: policy_type='cron' requires lower_bound_delta_hours.")

            if not g.get("selection"):
                raise ValueError(f"group {name!r}: 'selection' is required (asset key list, selection string, or '*').")
        return self

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from datetime import timedelta
        from dagster import build_last_update_freshness_checks

        discovered_keys, sibling_defs = _discover_sibling_assets(context)

        seen_keys: Dict[str, str] = {}
        all_checks = []
        for g in self.groups:
            name = g["name"]
            resolved = _resolve_selection(g["selection"], discovered_keys, sibling_defs)
            if not resolved:
                raise ValueError(
                    f"group {name!r}: selection {g['selection']!r} matched no assets "
                    f"(discovered {len(discovered_keys)} sibling asset(s))."
                )
            for k in resolved:
                if k in seen_keys:
                    raise ValueError(
                        f"asset_key {k!r} matched by both group {seen_keys[k]!r} and {name!r} -- "
                        f"each asset belongs to exactly one cadence tier."
                    )
                seen_keys[k] = name

            asset_keys = [dg.AssetKey(k.split("/")) for k in resolved]
            policy_type = g.get("policy_type", "time_window")

            if policy_type == "time_window":
                checks = build_last_update_freshness_checks(
                    assets=asset_keys,
                    lower_bound_delta=timedelta(hours=g["fail_window_hours"]),
                )
            else:
                kwargs: Dict[str, Any] = {
                    "assets": asset_keys,
                    "deadline_cron": g["deadline_cron"],
                    "lower_bound_delta": timedelta(hours=g["lower_bound_delta_hours"]),
                }
                if g.get("timezone"):
                    kwargs["timezone"] = g["timezone"]
                checks = build_last_update_freshness_checks(**kwargs)

            all_checks.extend(checks)

        return dg.Definitions(asset_checks=all_checks)
