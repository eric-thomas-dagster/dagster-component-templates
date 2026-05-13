"""Automation Condition Applicator.

Broadly apply Dagster AutomationConditions to many assets at once via selection
rules — without rewriting each asset's defs.yaml. Supports fall-through
priority (CSS-specificity-like) and preserves anything already set explicitly
upstream.

Why this exists:

Dagster lets you set ONE `automation_condition` per asset. Customers with
hundreds of assets typically want to set this BROADLY ("everything tagged
`tier=gold` runs daily at 9am") but also override NARROWLY for special cases
("but `critical_metrics` runs every 5 min"). Doing this per-asset means
editing every defs.yaml; doing it via this component centralizes the rules.

Three rule shapes supported:

1. **Cron string** — `cron: "0 9 * * *"` → `on_cron(cron)`
   With optional `ignore_selection` / `allow_selection` for the daily+monthly
   mix pattern (see README).

2. **Preset** — `preset: eager` / `any_downstream_conditions` / `on_missing` —
   the named AutomationCondition factory methods.

3. **Derive from upstreams** — `derive_from_upstreams: true` + `strategy:
   most_frequent | least_frequent`. Walks each asset's dependencies, finds
   their cron schedules / freshness policies, and generates the right
   `on_cron(...)` + ignore/allow combination automatically. Handles the
   "3 daily upstreams + 1 monthly" case the Dagster skill described.

Fall-through semantics:

Rules are evaluated TOP TO BOTTOM. The first rule whose `selection` matches
an asset wins — narrow rules listed first, broad catch-alls last. Assets that
already have `automation_condition` set explicitly (by their own component)
are preserved if `preserve_existing: true` (default).

This is the same "first match wins" pattern as CSS selectors with
`preserve_existing` acting like `!important` on per-asset settings.

How to wire it in (definitions.py):

    ```python
    from pathlib import Path
    from dagster import definitions, load_from_defs_folder
    from dagster_community_components.helpers.automation_applicator import (
        apply_automation_conditions_from_defs_folder,
    )

    @definitions
    def defs():
        base = load_from_defs_folder(path_within_project=Path(__file__).parent)
        return apply_automation_conditions_from_defs_folder(base, Path(__file__).parent)
    ```

That helper scans for `AutomationConditionApplicatorComponent` instances and
applies their rules to the base Definitions. Without this wiring, the
component is a no-op — Dagster components can't post-process other components'
output via `build_defs` alone.
"""

from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


# --------------------------------------------------------------------------
# Rule application logic
# --------------------------------------------------------------------------


def _parse_selection(selection: Any):
    """Accept a string ('tag:key=value', 'group:foo', '*', etc.) OR an
    already-built AssetSelection."""
    if isinstance(selection, dg.AssetSelection):
        return selection
    if isinstance(selection, str):
        s = selection.strip()
        if s in ("*", "all"):
            return dg.AssetSelection.all()
        return dg.AssetSelection.from_string(s)
    raise ValueError(f"selection must be str or AssetSelection, got {type(selection).__name__}")


def _build_condition_for_rule(rule: dict, spec: dg.AssetSpec, defs: dg.Definitions) -> Optional[dg.AutomationCondition]:
    """Build the AutomationCondition for a rule + a specific asset spec.

    Returns None if the rule's `derive_from_upstreams` is true but the asset
    has no upstream cron schedules to derive from.
    """
    # ---- Preset shortcuts ------------------------------------------------
    preset = rule.get("preset")
    if preset:
        method = getattr(dg.AutomationCondition, preset, None)
        if method is None or not callable(method):
            raise ValueError(
                f"preset={preset!r} is not a valid AutomationCondition factory "
                f"(e.g. 'eager', 'any_downstream_conditions', 'on_missing')."
            )
        result = method()
        if not isinstance(result, dg.AutomationCondition):
            raise ValueError(
                f"preset={preset!r} did not return an AutomationCondition (got {type(result).__name__})."
            )
        return result

    # ---- Explicit cron ---------------------------------------------------
    cron = rule.get("cron")
    if cron:
        cond = dg.AutomationCondition.on_cron(cron)
        ignore_sel = rule.get("ignore_selection")
        allow_sel = rule.get("allow_selection")
        if ignore_sel:
            cond = cond.ignore(_parse_selection(ignore_sel))
        if allow_sel:
            cond = cond.allow(_parse_selection(allow_sel))
        return cond

    # ---- Derive from upstreams ------------------------------------------
    if rule.get("derive_from_upstreams"):
        return _derive_condition_from_upstreams(
            spec, defs, strategy=rule.get("strategy", "most_frequent")
        )

    raise ValueError(
        f"rule {rule.get('name', '<unnamed>')!r}: must specify one of "
        "'cron', 'preset', or 'derive_from_upstreams'."
    )


def _derive_condition_from_upstreams(
    spec: dg.AssetSpec, defs: dg.Definitions, strategy: str
) -> Optional[dg.AutomationCondition]:
    """Build an AutomationCondition by inspecting the cron schedules of upstream assets.

    strategy:
      - 'most_frequent': fire on the most frequent upstream's cron; ignore slower upstreams.
      - 'least_frequent': fire on the least frequent upstream's cron; require all deps fresh.
    """
    # Collect upstream cron schedules
    asset_graph = defs.resolve_asset_graph()
    upstream_deps = list(spec.deps) if spec.deps else []
    upstream_crons: List[tuple] = []  # (cron_str, asset_key)
    for dep in upstream_deps:
        # AssetSpec.deps yields AssetDep objects with an `.asset_key` of type AssetKey;
        # tolerate the rarer case of a bare AssetKey by checking type.
        if isinstance(dep, dg.AssetKey):
            dep_key = dep
        else:
            ak = getattr(dep, "asset_key", None)
            if not isinstance(ak, dg.AssetKey):
                continue
            dep_key = ak
        try:
            up_node = asset_graph.get(dep_key)
        except Exception:
            continue
        cron = _extract_cron_from_spec(up_node)
        if cron:
            upstream_crons.append((cron, dep_key))

    if not upstream_crons:
        return None  # nothing to derive from — caller falls through to next rule

    if strategy == "most_frequent":
        # Bucket by cron expression; ignore-list is everything NOT on the fastest cron.
        sorted_crons = sorted(upstream_crons, key=lambda x: _cron_period_seconds(x[0]))
        fastest_cron = sorted_crons[0][0]
        slow_keys = [k for c, k in sorted_crons if c != fastest_cron]
        cond = dg.AutomationCondition.on_cron(fastest_cron)
        if slow_keys:
            cond = cond.ignore(dg.AssetSelection.assets(*slow_keys))
        return cond

    if strategy == "least_frequent":
        sorted_crons = sorted(upstream_crons, key=lambda x: -_cron_period_seconds(x[0]))
        slowest_cron = sorted_crons[0][0]
        return dg.AutomationCondition.on_cron(slowest_cron)

    raise ValueError(f"unknown strategy: {strategy!r}")


def _extract_cron_from_spec(spec) -> Optional[str]:
    """Pull a cron schedule from an AssetSpec/AssetGraphNode.

    Looks (in order): automation_condition.cron_schedule, freshness_policy.cron_schedule,
    metadata['cron_schedule'].
    """
    # AutomationCondition with cron_schedule attribute
    ac = getattr(spec, "automation_condition", None)
    if ac is not None and hasattr(ac, "cron_schedule"):
        cs = getattr(ac, "cron_schedule", None)
        if cs:
            return cs
    # FreshnessPolicy
    fp = getattr(spec, "freshness_policy", None)
    if fp is not None:
        cs = getattr(fp, "cron_schedule", None)
        if cs:
            return cs
    # metadata fallback
    md = getattr(spec, "metadata", None) or {}
    if isinstance(md, dict):
        cs = md.get("cron_schedule")
        if isinstance(cs, str):
            return cs
    return None


def _cron_period_seconds(cron: str) -> int:
    """Crude cron-period estimate for sorting. Coarse heuristic — good enough
    for the typical hourly/daily/weekly/monthly cases."""
    parts = cron.split()
    if len(parts) < 5:
        return 60  # malformed — treat as frequent
    minute, hour, dom, month, dow = parts[:5]
    if minute == "*":
        return 60
    if hour == "*":
        return 60 * 60
    if dom == "*" and dow == "*":
        return 24 * 60 * 60  # daily
    if dom == "*":
        return 7 * 24 * 60 * 60  # weekly (DOW set)
    if month == "*":
        return 30 * 24 * 60 * 60  # monthly (DOM set)
    return 365 * 24 * 60 * 60  # yearly


def apply_rules(
    defs: dg.Definitions,
    rules: List[dict],
    preserve_existing: bool = True,
) -> dg.Definitions:
    """Apply automation-condition rules to a Definitions. Returns a new Definitions.

    Rules are evaluated top-to-bottom; first selection-match wins. Assets that
    already have `automation_condition` set are preserved when
    `preserve_existing=True` (default).
    """
    # Resolve selections to concrete asset-key sets up front (one pass).
    resolved_rules = []
    asset_graph = defs.resolve_asset_graph()
    for rule in rules:
        sel = _parse_selection(rule["selection"])
        try:
            matched = sel.resolve(asset_graph)
        except Exception:
            # Fall back: empty match if selection can't resolve
            matched = set()
        resolved_rules.append((rule, matched))

    def transform(spec: dg.AssetSpec) -> dg.AssetSpec:
        if preserve_existing and getattr(spec, "automation_condition", None) is not None:
            return spec
        for rule, matched_keys in resolved_rules:
            if spec.key not in matched_keys:
                continue
            condition = _build_condition_for_rule(rule, spec, defs)
            if condition is None:
                continue  # rule didn't yield a condition (e.g. derive with no upstreams) — fall through
            return spec.replace_attributes(automation_condition=condition)
        return spec

    return defs.map_asset_specs(func=transform)


# --------------------------------------------------------------------------
# Component
# --------------------------------------------------------------------------


class AutomationConditionApplicatorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Broadly apply AutomationConditions to many assets via fall-through rules.

    Without explicit wiring, this component is a NO-OP at load time (Dagster
    doesn't allow components to post-process other components' assets directly).
    To take effect, the user wires it in their `definitions.py`:

        ```python
        from pathlib import Path
        from dagster import definitions, load_from_defs_folder
        from dagster_community_components.helpers.automation_applicator import (
            apply_automation_conditions_from_defs_folder,
        )

        @definitions
        def defs():
            base = load_from_defs_folder(path_within_project=Path(__file__).parent)
            return apply_automation_conditions_from_defs_folder(base, Path(__file__).parent)
        ```

    Or for custom-Python flexibility:

        ```python
        from dagster_component_templates.assets.infrastructure.automation_condition_applicator.component import (
            apply_rules,
        )

        @definitions
        def defs():
            base = load_from_defs_folder(...)
            return apply_rules(base, rules=[
                {"selection": "tag:cadence=hourly", "cron": "0 * * * *"},
                {"selection": "*", "preset": "eager"},
            ])
        ```

    Example YAML config (fall-through priority — narrow first, broad last):

        ```yaml
        type: dagster_component_templates.AutomationConditionApplicatorComponent
        attributes:
          preserve_existing: true
          rules:
            - name: critical_hourly
              selection: "tag:cadence=hourly"
              cron: "0 * * * *"

            - name: daily_gold
              selection: "group:gold"
              cron: "0 9 * * *"
              ignore_selection: "tag:tier=archive"

            - name: derive_for_silver
              selection: "group:silver"
              derive_from_upstreams: true
              strategy: most_frequent

            - name: catchall_eager
              selection: "*"
              preset: eager
        ```
    """

    preserve_existing: bool = Field(
        default=True,
        description=(
            "If true (default): assets that already have automation_condition set "
            "by their own component are NOT overwritten. Like CSS !important — "
            "explicit per-asset wins over broad rules."
        ),
    )

    rules: List[Dict[str, Any]] = Field(
        description=(
            "Ordered list of rules. First selection-match wins (fall-through). "
            "Each rule has 'selection' + one of: 'cron' (with optional "
            "'ignore_selection'/'allow_selection'), 'preset', or "
            "'derive_from_upstreams: true' (+ optional 'strategy')."
        ),
    )

    def get_rules(self) -> List[Dict[str, Any]]:
        """Returned for use by the project-level helper."""
        return self.rules

    def get_preserve_existing(self) -> bool:
        return self.preserve_existing

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # No-op: the helper in definitions.py reads this component's rules
        # and applies them to the merged Definitions.
        return dg.Definitions(assets=[])

    def apply(self, base_defs: dg.Definitions) -> dg.Definitions:
        """Convenience method: apply this component's rules to a Definitions."""
        return apply_rules(base_defs, self.rules, preserve_existing=self.preserve_existing)
