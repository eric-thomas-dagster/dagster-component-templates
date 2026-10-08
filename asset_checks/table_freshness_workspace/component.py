"""Table Freshness Workspace Component.

Bulk sibling of `FreshnessPolicyComponent` — one config block declares
freshness policies for MANY assets at once, grouped by cadence tier, instead
of one component instance per asset.

Real-world motivation: a replication feed with hundreds of landing tables
on wildly different arrival cadences (some hourly, some daily, some rare)
needs every table's expected cadence declared somewhere, but hand-writing
one `FreshnessPolicyComponent` YAML block per table doesn't scale and
doesn't read as "group default" — the policy tiers ARE the real structure
(a handful of cadences), not the individual tables.

Each `groups:` entry becomes ONE real Dagster-native freshness-check batch
via `build_last_update_freshness_checks` (the same factory
`FreshnessPolicyComponent` uses) — Dagster's own API already accepts a list
of asset keys per policy, so tables sharing a cadence tier cost one call,
not N. New tables onboard by adding one line to the right group's
`asset_keys:` list — still just git-tracked config, no code.
"""
from typing import Any, Dict, List, Literal, Optional

import dagster as dg
from pydantic import Field, model_validator


class TableFreshnessWorkspaceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Bulk-declare freshness policies for many assets, grouped by cadence
    tier, instead of one `FreshnessPolicyComponent` instance per asset.

    Example — three cadence tiers covering a 250-table replication feed:

        type: dagster_community_components.TableFreshnessWorkspaceComponent
        attributes:
          groups:
            - name: hourly_tables
              policy_type: time_window
              fail_window_hours: 2
              asset_keys: [sap_ecc/bseg, sap_ecc/konv, sap_ecc/vbap]
            - name: daily_tables
              policy_type: time_window
              fail_window_hours: 30
              asset_keys: [sap_ecc/material_master, sap_ecc/vendor_master]
            - name: rare_tables
              policy_type: cron
              deadline_cron: "0 6 * * 1"
              lower_bound_delta_hours: 24
              asset_keys: [sap_ecc/company_codes]
    """

    groups: List[Dict[str, Any]] = Field(
        description=(
            "List of {name, policy_type, asset_keys, ...policy fields}. "
            "Same policy_type/field shape as FreshnessPolicyComponent "
            "(time_window: fail_window_hours; cron: deadline_cron + "
            "lower_bound_delta_hours + optional timezone), applied to every "
            "asset_key in the group's list."
        )
    )

    @model_validator(mode="after")
    def validate_groups(self):
        if not self.groups:
            raise ValueError("TableFreshnessWorkspaceComponent: groups must be non-empty.")
        seen_keys: Dict[str, str] = {}
        seen_names = set()
        for g in self.groups:
            name = g.get("name")
            if not name:
                raise ValueError(f"TableFreshnessWorkspaceComponent: every group needs a 'name'. Got: {g}")
            if name in seen_names:
                raise ValueError(f"TableFreshnessWorkspaceComponent: duplicate group name {name!r}.")
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

            asset_keys = g.get("asset_keys") or []
            if not asset_keys:
                raise ValueError(f"group {name!r}: asset_keys must be non-empty.")
            for k in asset_keys:
                if k in seen_keys:
                    raise ValueError(
                        f"asset_key {k!r} appears in both group {seen_keys[k]!r} and {name!r} -- "
                        f"each asset belongs to exactly one cadence tier."
                    )
                seen_keys[k] = name
        return self

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from datetime import timedelta
        from dagster import build_last_update_freshness_checks

        all_checks = []
        for g in self.groups:
            asset_keys = [dg.AssetKey(k.split("/")) for k in g["asset_keys"]]
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
