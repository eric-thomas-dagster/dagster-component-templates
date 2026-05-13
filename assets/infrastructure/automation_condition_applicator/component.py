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


def _build_condition_for_rule(
    rule: dict,
    spec: Any,  # AssetSpec or _SpecView — duck-typed (.key / .deps / .metadata / ...)
    defs: dg.Definitions,
    effective_crons: Optional[Dict[dg.AssetKey, str]] = None,
) -> Optional[dg.AutomationCondition]:
    """Build the AutomationCondition for a rule + a specific asset spec.

    `effective_crons` (asset key → cron string) carries the result of earlier
    rule passes in topological order so derive_from_upstreams sees each
    upstream's *post-rule* cadence, not its raw metadata.

    Returns None if the rule cannot yield a condition for this asset
    (e.g., derive_from_upstreams when no upstream has a cron) — caller
    falls through to the next rule.

    Rule shape (one of `python`, `preset`, `cron`, `derive_from_upstreams`
    is required, plus optional `label`, `business_hours_only`,
    `min_interval_minutes` modifiers).
    """
    condition: Optional[dg.AutomationCondition] = None

    # ---- Python escape hatch (universal — for anything YAML can't express)
    python_ref = rule.get("python")
    if python_ref:
        condition = _resolve_python_condition(python_ref, spec, defs)

    # ---- Preset shortcuts ------------------------------------------------
    elif rule.get("preset"):
        preset = rule["preset"]
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
        condition = result

    # ---- Explicit cron ---------------------------------------------------
    elif rule.get("cron"):
        cron = rule["cron"]
        cond = dg.AutomationCondition.on_cron(cron)
        ignore_sel = rule.get("ignore_selection")
        allow_sel = rule.get("allow_selection")
        if ignore_sel:
            cond = cond.ignore(_parse_selection(ignore_sel))
        if allow_sel:
            cond = cond.allow(_parse_selection(allow_sel))
        condition = cond

    # ---- Derive from upstreams ------------------------------------------
    elif rule.get("derive_from_upstreams"):
        condition = _derive_condition_from_upstreams(
            spec,
            defs,
            strategy=rule.get("strategy", "most_frequent"),
            effective_crons=effective_crons or {},
            offset_minutes=rule.get("offset_minutes", 0),
        )

    else:
        raise ValueError(
            f"rule {rule.get('name', '<unnamed>')!r}: must specify one of "
            "'python', 'preset', 'cron', or 'derive_from_upstreams'."
        )

    if condition is None:
        return None  # rule didn't yield (e.g. derive with no upstream crons)

    # ---- Modifiers (apply to whatever condition we just built) ---------
    if rule.get("min_interval_minutes"):
        condition = _apply_min_interval(condition, int(rule["min_interval_minutes"]), rule)

    if rule.get("business_hours_only"):
        condition = _apply_business_hours(
            condition,
            hours=rule.get("business_hours", "9-17"),
            days=rule.get("business_days", "1-5"),
            rule_name=rule.get("name", "<unnamed>"),
        )

    # ---- Label override (auto-labels are descriptive but verbose) ------
    label = rule.get("label")
    if label:
        condition = condition.with_label(label)  # type: ignore[attr-defined]

    return condition


def _resolve_python_condition(ref: str, spec, defs):
    """Load an AutomationCondition from a 'module.path:function_name' reference.

    The referenced callable must return an `AutomationCondition`. It may take
    zero args, or one arg (the asset spec — useful for per-asset customization).
    """
    if ":" not in ref:
        raise ValueError(
            f"python ref must be 'module.path:function_name', got {ref!r}"
        )
    module_path, func_name = ref.rsplit(":", 1)
    module_path = module_path.strip()
    func_name = func_name.strip()

    import importlib
    import inspect

    try:
        mod = importlib.import_module(module_path)
    except ImportError as e:
        raise ValueError(
            f"python: cannot import module {module_path!r}: {e}"
        ) from e
    func = getattr(mod, func_name, None)
    if func is None:
        raise ValueError(
            f"python: module {module_path!r} has no attribute {func_name!r}"
        )
    if not callable(func):
        raise ValueError(f"python: {ref!r} is not callable")

    try:
        sig = inspect.signature(func)
        positional_params = [
            p
            for p in sig.parameters.values()
            if p.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.POSITIONAL_ONLY)
            and p.default is inspect.Parameter.empty
        ]
        if len(positional_params) == 0:
            result = func()
        else:
            result = func(spec)
    except TypeError:
        # Mismatched signature — try the other shape
        try:
            result = func()
        except TypeError:
            result = func(spec)

    if not isinstance(result, dg.AutomationCondition):
        raise ValueError(
            f"python: {ref!r} returned {type(result).__name__}, expected AutomationCondition"
        )
    return result


def _apply_min_interval(
    condition: dg.AutomationCondition,
    min_minutes: int,
    rule: dict,
) -> dg.AutomationCondition:
    """Floor the condition's effective cron to a minimum interval.

    Replaces the inner cron with a coarser one if the original is faster.
    Requires the condition to have an extractable cron — non-cron strategies
    (any_dep_updated, all_deps_updated) raise.
    """
    inner_cron = _extract_cron_from_condition(condition)
    if inner_cron is None:
        raise ValueError(
            f"rule {rule.get('name', '<unnamed>')!r}: min_interval_minutes requires a "
            "cron-producing strategy (cron / derive_from_upstreams with most_frequent / "
            "least_frequent / tiered / staggered). Got non-cron condition."
        )
    current_seconds = _cron_period_seconds(inner_cron)
    if current_seconds >= min_minutes * 60:
        return condition  # already slower than floor
    return dg.AutomationCondition.on_cron(_cron_for_interval_minutes(min_minutes)).with_label(
        f"min_interval({min_minutes}min)"
    )


def _cron_for_interval_minutes(minutes: int) -> str:
    """Return a cron expression with period >= `minutes`. Uses sane round values.

    Supports: 5/10/15/20/30 minutely; 1/2/3/4/6/8/12 hourly; daily; weekly.
    """
    if minutes <= 0:
        raise ValueError(f"min_interval_minutes must be > 0, got {minutes}")
    if minutes < 60:
        # Pick the smallest divisor of 60 that's >= minutes
        for n in (5, 10, 15, 20, 30, 60):
            if n >= minutes:
                if n == 60:
                    return "0 * * * *"
                return f"*/{n} * * * *"
    hours, rem = divmod(minutes, 60)
    if rem > 0:
        hours += 1
    if hours <= 12:
        # Pick smallest divisor of 24 that's >= hours
        for h in (1, 2, 3, 4, 6, 8, 12, 24):
            if h >= hours:
                if h == 24:
                    return "0 0 * * *"
                return f"0 */{h} * * *"
    if hours <= 24:
        return "0 0 * * *"
    if hours <= 24 * 7:
        return "0 0 * * 0"  # weekly
    return "0 0 1 * *"  # monthly floor


def _apply_business_hours(
    condition: dg.AutomationCondition,
    hours: str,
    days: str,
    rule_name: str,
) -> dg.AutomationCondition:
    """Constrain the condition's cron to business hours and days.

    Supported: simple cron strategies whose cron has a single hour value.
    For tiered conditions (multiple inner crons), this raises with a clear
    error — use the python escape hatch instead.
    """
    inner_cron = _extract_cron_from_condition(condition)
    if inner_cron is None:
        raise ValueError(
            f"rule {rule_name!r}: business_hours_only requires a cron-producing strategy."
        )

    # Refuse for tiered (multi-cron) conditions — replacing one cron breaks the
    # tier composition. Users should compose business_hours_only at the cron
    # level, or use the python escape hatch.
    if _count_distinct_crons_in_condition(condition) > 1:
        raise ValueError(
            f"rule {rule_name!r}: business_hours_only doesn't compose with tiered "
            "strategies (multiple inner crons). Use `cron:` + `business_hours_only` "
            "directly, or write a custom condition via `python:`."
        )

    parts = inner_cron.split()
    if len(parts) != 5:
        raise ValueError(
            f"rule {rule_name!r}: business_hours_only got malformed cron {inner_cron!r}."
        )
    minute, hour, dom, month, dow = parts
    # Apply business-hour bounds: replace `*` hour with the range; replace `*` dow with weekdays.
    new_hour = hours if hour == "*" else hour
    new_dow = days if dow == "*" else dow
    new_cron = f"{minute} {new_hour} {dom} {month} {new_dow}"
    return dg.AutomationCondition.on_cron(new_cron).with_label(f"business_hours({new_cron})")


def _shift_cron(cron: str, offset_minutes: int) -> str:
    """Shift a fixed-time cron by offset_minutes. Rejects crosses-day-boundary
    or non-fixed-time crons with clear errors."""
    parts = cron.split()
    if len(parts) != 5:
        raise ValueError(f"staggered: cron must have 5 fields, got {cron!r}")
    minute, hour, dom, month, dow = parts
    if minute == "*" or hour == "*":
        raise ValueError(
            f"staggered: only fixed minute+hour crons (e.g. '0 6 * * *') can be shifted; "
            f"got {cron!r}. For finer-grained shifts, write an explicit cron."
        )
    try:
        m = int(minute)
        h = int(hour)
    except ValueError:
        raise ValueError(
            f"staggered: requires integer minute+hour, got {cron!r}"
        ) from None
    total = m + h * 60 + offset_minutes
    if not (0 <= total < 24 * 60):
        raise ValueError(
            f"staggered: offset of {offset_minutes}min from {cron!r} would cross a day boundary "
            "(total time would be < 0 or ≥ 24h). For cross-day offsets, write an explicit cron "
            "or use the python escape hatch."
        )
    new_h, new_m = divmod(total, 60)
    return f"{new_m} {new_h} {dom} {month} {dow}"


def _count_distinct_crons_in_condition(condition) -> int:
    """Count distinct cron_schedule strings across a condition's tree."""
    seen: set = set()

    def walk(c):
        if c is None:
            return
        cs = getattr(c, "cron_schedule", None)
        if isinstance(cs, str):
            seen.add(cs)
        for attr in ("trigger_condition", "reset_condition", "operand"):
            walk(getattr(c, attr, None))
        for op in getattr(c, "operands", None) or []:
            walk(op)

    walk(condition)
    return len(seen)


def _derive_condition_from_upstreams(
    spec,  # AssetSpec or _SpecView — duck-typed
    defs: dg.Definitions,
    strategy: str,
    effective_crons: Dict[dg.AssetKey, str],
    offset_minutes: int = 0,
) -> Optional[dg.AutomationCondition]:
    """Build an AutomationCondition by inspecting the cron schedules of upstream assets.

    `effective_crons` (asset_key → cron) lets us see each upstream's POST-RULE
    effective cadence rather than its raw metadata. Makes propagation work
    for chains of depth ≥ 2.

    `offset_minutes` only applies to `strategy: staggered` — shifts the derived
    cron by that many minutes (positive = later). Errors if it would cross
    a day boundary.

    strategy:
      - 'most_frequent': fire on the fastest upstream's cron; ignore slower deps.
        Use when slower deps are nice-to-have but shouldn't block.
      - 'least_frequent': fire on the slowest upstream's cron; require all deps.
        Use when downstream MUST see every dep's update before firing.
      - 'tiered': fire on the fastest upstream's cron, BUT on each slower tier's
        boundary, also wait for that tier. Per-tier `allow()` gating. The
        "daily-with-monthly-boundary" pattern.
      - 'staggered': like 'most_frequent', but shifts the derived cron by
        `offset_minutes`. Use to add a buffer after upstream completion.
      - 'any_dep_updated': no cron — fire whenever ANY upstream updates since
        the last fire. Event-driven; ignores upstream cron metadata entirely.
      - 'all_deps_updated': no cron — fire only when ALL upstreams have updated
        since the last fire. Strict join semantics; ignores upstream cron metadata.
    """
    # --- No-cron strategies: don't even walk upstream cron metadata --------
    if strategy == "any_dep_updated":
        cond = (
            dg.AutomationCondition.in_latest_time_window()
            & dg.AutomationCondition.any_deps_updated().since_last_handled()
            & ~dg.AutomationCondition.any_deps_in_progress()
        )
        return cond.with_label("any_dep_updated")  # type: ignore[attr-defined]

    if strategy == "all_deps_updated":
        cond = (
            dg.AutomationCondition.in_latest_time_window()
            & dg.AutomationCondition.all_deps_match(
                dg.AutomationCondition.newly_updated()
            ).since_last_handled()
            & ~dg.AutomationCondition.any_deps_in_progress()
        )
        return cond.with_label("all_deps_updated")  # type: ignore[attr-defined]
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
        # Prefer the post-rule effective cron (propagation through chains);
        # fall back to the upstream's raw metadata/condition.
        cron = effective_crons.get(dep_key)
        if not cron:
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

    if strategy == "staggered":
        # Same as most_frequent but shifts the cron by offset_minutes.
        # Useful for pipeline pacing: fire 1 hour AFTER the upstream's daily tick.
        if offset_minutes == 0:
            raise ValueError(
                "strategy='staggered' requires offset_minutes > 0 (or use 'most_frequent')."
            )
        sorted_crons = sorted(upstream_crons, key=lambda x: _cron_period_seconds(x[0]))
        fastest_cron = sorted_crons[0][0]
        shifted_cron = _shift_cron(fastest_cron, offset_minutes)
        slow_keys = [k for c, k in sorted_crons if c != fastest_cron]
        cond = dg.AutomationCondition.on_cron(shifted_cron)
        if slow_keys:
            cond = cond.ignore(dg.AssetSelection.assets(*slow_keys))
        return cond.with_label(f"staggered({shifted_cron})")  # type: ignore[attr-defined]

    if strategy == "tiered":
        # Group upstreams by cron expression — each distinct cron is a "tier".
        # The fastest tier drives the firing cadence (cron_tick_passed).
        # Slower tiers add `allow`-gated all_deps_updated_since_cron checks —
        # they're checked on their own cron boundaries (e.g. on the 1st for
        # a monthly tier).
        from collections import defaultdict

        buckets: dict = defaultdict(list)
        for cron, key in upstream_crons:
            buckets[cron].append(key)

        # Single-tier degenerate case → fall back to plain on_cron.
        if len(buckets) == 1:
            only_cron = next(iter(buckets))
            return dg.AutomationCondition.on_cron(only_cron)

        sorted_tiers = sorted(buckets.items(), key=lambda kv: _cron_period_seconds(kv[0]))
        fastest_cron = sorted_tiers[0][0]
        slower_tiers = sorted_tiers[1:]
        slower_keys_union = [k for _, keys in slower_tiers for k in keys]

        # Build the ANDed condition matching the customer pattern:
        # in_latest_time_window
        # & cron_tick_passed(FAST).since_last_handled()
        # & all_deps_updated_since_cron(FAST).ignore(SLOW_UNION)
        # & all_deps_updated_since_cron(<each slower cron>).allow(<its keys>)
        slow_sel_union = dg.AssetSelection.assets(*slower_keys_union)
        cond = (
            dg.AutomationCondition.in_latest_time_window()
            & dg.AutomationCondition.cron_tick_passed(fastest_cron).since_last_handled()
            & dg.AutomationCondition.all_deps_updated_since_cron(fastest_cron).ignore(slow_sel_union)
        )
        for tier_cron, tier_keys in slower_tiers:
            tier_sel = dg.AssetSelection.assets(*tier_keys)
            cond = cond & dg.AutomationCondition.all_deps_updated_since_cron(tier_cron).allow(tier_sel)
        return cond.with_label(f"tiered_on_cron({fastest_cron})")

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

    Cadence propagation: assets are processed in **topological order** so
    `derive_from_upstreams` sees each upstream's POST-RULE effective cadence,
    not its raw metadata. This means chains of depth ≥ 2 work correctly —
    if asset C's rule resolves it to monthly (e.g. least_frequent of
    daily+monthly upstreams), then downstream D's derive_from_upstreams
    sees C as monthly.
    """
    # Resolve selections to concrete asset-key sets up front (one pass).
    resolved_rules = []
    asset_graph = defs.resolve_asset_graph()
    for rule in rules:
        sel = _parse_selection(rule["selection"])
        try:
            matched = sel.resolve(asset_graph)
        except Exception:
            matched = set()
        resolved_rules.append((rule, matched))

    # Topological order: roots first, leaves last. Cadence propagates downstream.
    sorted_keys = asset_graph.toposorted_asset_keys

    # Effective-cron map: asset_key → cron string. Built incrementally as we
    # process each asset in topological order, so when we process a downstream,
    # its upstreams' effective crons are already populated.
    effective_crons: Dict[dg.AssetKey, str] = {}

    # Pre-populate effective_crons for unchanged roots (their existing crons propagate).
    for key in sorted_keys:
        try:
            node = asset_graph.get(key)
        except Exception:
            continue
        cron = _extract_cron_from_spec(node)
        if cron:
            effective_crons[key] = cron

    # Compute the AutomationCondition each asset will get, in topological order.
    asset_conditions: Dict[dg.AssetKey, dg.AutomationCondition] = {}
    for key in sorted_keys:
        try:
            node = asset_graph.get(key)
        except Exception:
            continue
        # preserve_existing → asset's own explicit condition wins; don't touch.
        if preserve_existing and getattr(node, "automation_condition", None) is not None:
            continue
        # Build a synthetic spec to pass to the rule (we want spec-shape access
        # but using the AssetGraphNode's view of deps + metadata).
        spec_view = _SpecView(node)
        for rule, matched_keys in resolved_rules:
            if key not in matched_keys:
                continue
            condition = _build_condition_for_rule(rule, spec_view, defs, effective_crons)
            if condition is None:
                continue  # rule didn't yield (e.g. derive with no upstream crons) — fall through
            asset_conditions[key] = condition
            # Update effective_crons so DOWNSTREAM assets see this asset's new cadence.
            new_cron = _extract_cron_from_condition(condition)
            if new_cron:
                effective_crons[key] = new_cron
            break

    # Single map pass — apply the pre-computed conditions.
    def transform(spec: dg.AssetSpec) -> dg.AssetSpec:
        cond = asset_conditions.get(spec.key)
        if cond is None:
            return spec
        return spec.replace_attributes(automation_condition=cond)

    return defs.map_asset_specs(func=transform)


class _SpecView:
    """Thin adapter so _build_condition_for_rule can read .deps / .key / .metadata
    from an AssetGraphNode the same way it does from an AssetSpec."""

    def __init__(self, node):
        self._node = node
        self.key = node.key
        # parent_keys is a set of AssetKey — wrap as fake AssetDep-like objects
        self.deps = [_Dep(k) for k in node.parent_keys]
        self.metadata = getattr(node, "metadata", {}) or {}
        self.automation_condition = getattr(node, "automation_condition", None)
        self.freshness_policy = getattr(node, "freshness_policy", None)


class _Dep:
    def __init__(self, key):
        self.asset_key = key


def _extract_cron_from_condition(condition) -> Optional[str]:
    """Pull a cron string out of an AutomationCondition (recursing through wrappers).

    AutomationConditions nest in several shapes:
      - AndAutomationCondition / OrAutomationCondition → `.operands` list
      - SinceCondition → `.trigger_condition` + `.reset_condition`
      - AllDepsCondition / AnyDepsCondition / NotAutomationCondition → `.operand`
      - CronTickPassedCondition → has `.cron_schedule` directly
    We check the named attributes (cron-bearing leaf) first, then descend through
    every known wrapper relation.
    """
    if condition is None:
        return None
    cs = getattr(condition, "cron_schedule", None)
    if isinstance(cs, str):
        return cs

    # Descend through all known child-relation attributes
    for attr in ("trigger_condition", "reset_condition", "operand"):
        child = getattr(condition, attr, None)
        if child is not None:
            sub = _extract_cron_from_condition(child)
            if sub:
                return sub
    operands = getattr(condition, "operands", None) or []
    for op in operands:
        sub = _extract_cron_from_condition(op)
        if sub:
            return sub
    return None


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
