"""SlaAssetComponent + `@sla` — enforce wall-clock SLAs on asset compute.

Track how long each materialization takes; on breach, emit an
`AssetObservation` tagged with breach details AND optionally raise
`dg.Failure`. Cross-run breach history is queryable via the event log
so alerts can escalate after N breaches in a window.

## Why this belongs in Dagster

Every primitive here is a Dagster event:
- **Breach event** → `AssetObservation` with typed breach metadata.
- **Repeat-breach detection** → `context.instance.get_event_records`
  filtered on the breach tag within a rolling window.
- **Escalation policy** → downstream blocks via
  `AutomationCondition.eager()` on the breach observation, OR sensor
  fires on N-consecutive-breaches.

`@dg.asset(freshness_policy=...)` is about SOURCE freshness — "when
should the upstream data be refreshed." This is different: `@sla`
enforces WALL-CLOCK compute duration. Complementary, not redundant.

## Two shapes

- **`SlaAssetComponent`** (YAML)
- **`@sla` decorator** (Python)

## SLA fields

- `expected_duration_seconds` — breach threshold
- `on_breach: warn | fail` — warn = materialize + observation, fail = dg.Failure
- `escalate_after_n_breaches` — optional; count breaches in
  `escalate_window_seconds` and mark ESCALATED if >= N
- `escalate_window_seconds` — sliding window for the count

## Composes with

- `@smart_retry` — retry inside the SLA window (still counts toward budget).
- `@lifecycle` — the SLA covers write + audit + publish end-to-end.
- Any sensor — watch for `sla_breach` observations, fire notifications.
"""

import functools
import importlib
import time
from typing import Any, Callable, Dict, List, Optional

import dagster as dg
from pydantic import Field


# --------------------------------------------------------------------------
# Per-partition scoping helper
# --------------------------------------------------------------------------


def _lookup_per_partition(
    partition_key: Optional[str],
    per_partition_map: Optional[Dict[str, float]],
    default_value: float,
    matcher: str = "exact",
) -> float:
    """Return the per-partition override for `partition_key`, else `default_value`.

    Matcher modes:
    - 'exact': partition_key must equal a map key
    - 'prefix': map key is a prefix of partition_key
    - 'regex': map key is a regex pattern against partition_key
    """
    if not partition_key or not per_partition_map:
        return default_value
    if matcher == "exact":
        return per_partition_map.get(partition_key, default_value)
    if matcher == "prefix":
        for k, v in per_partition_map.items():
            if partition_key.startswith(k):
                return v
        return default_value
    if matcher == "regex":
        import re
        for pat, v in per_partition_map.items():
            if re.match(pat, partition_key):
                return v
        return default_value
    raise ValueError(f"unknown matcher: {matcher!r}")


# --------------------------------------------------------------------------
# Breach event emission + cross-run history
# --------------------------------------------------------------------------

_BREACH_TAG = "sla_breach"

# Dagster enforces `[A-Za-z0-9_.-]{,63}` on tag values. `sla_key` is
# user-controlled (can hold emails, paths, composite partition keys) so
# tag values sourced from it MUST be sanitized before emission.
_TAG_SAFE = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-")


def _sanitize_tag_value(v: str) -> str:
    """Coerce `v` into a Dagster-tag-safe string: replace disallowed chars
    with `_` and truncate to 63 chars. Preserve the raw value in metadata
    at the call site so nothing is lost.
    """
    s = str(v)
    sanitized = "".join(ch if ch in _TAG_SAFE else "_" for ch in s)
    return sanitized[:63]


def _emit_breach_observation(
    context: Any, key: str, actual_s: float, expected_s: float, escalated: bool,
):
    """Emit AssetObservation with breach metadata + tag.

    Sanitizes the user-supplied ``key`` for the tag value (Dagster rejects
    values outside ``[A-Za-z0-9_.-]{,63}``) while preserving the raw key
    in metadata. Emission failures surface via ``log.warning`` instead of
    being silently swallowed.
    """
    try:
        from dagster import AssetObservation, MetadataValue
        asset_key = getattr(context, "asset_key", None)
        if asset_key is None:
            from dagster import AssetKey
            asset_key = AssetKey(["sla_asset"])
        tags = {
            _BREACH_TAG: _sanitize_tag_value(key),
            "sla_actual_seconds": _sanitize_tag_value(str(round(actual_s, 3))),
            "sla_expected_seconds": _sanitize_tag_value(str(round(expected_s, 3))),
            "sla_escalated": _sanitize_tag_value(str(escalated).lower()),
        }
        if hasattr(context, "log_event"):
            context.log_event(AssetObservation(
                asset_key=asset_key,
                tags=tags,
                metadata={
                    "sla_key": MetadataValue.text(str(key)),
                    "sla_actual_seconds": dg.MetadataValue.float(round(actual_s, 3)),
                    "sla_expected_seconds": dg.MetadataValue.float(round(expected_s, 3)),
                    "sla_overrun_seconds": dg.MetadataValue.float(round(actual_s - expected_s, 3)),
                    "sla_overrun_pct": dg.MetadataValue.float(round((actual_s - expected_s) / expected_s * 100.0, 1)),
                    "sla_escalated": dg.MetadataValue.bool(escalated),
                },
            ))
    except Exception as e:  # noqa: BLE001
        try:
            context.log.warning(
                f"@sla: could not emit breach observation: {type(e).__name__}: {e}"
            )
        except Exception:  # noqa: BLE001
            pass


def _count_recent_breaches(context: Any, key: str, window_seconds: float) -> int:
    """Query the event log for prior `sla_breach=<key>` observations
    within `window_seconds`."""
    try:
        from dagster import EventRecordsFilter, DagsterEventType
        records = context.instance.get_event_records(
            event_records_filter=EventRecordsFilter(
                event_type=DagsterEventType.ASSET_OBSERVATION,
            ),
            limit=200, ascending=False,
        )
        cutoff = time.time() - window_seconds
        n = 0
        for r in records:
            tags = (r.asset_observation.tags if r.asset_observation else None) or {}
            if tags.get(_BREACH_TAG) != key:
                continue
            ts = r.timestamp
            if ts is not None and float(ts) >= cutoff:
                n += 1
        return n
    except Exception:  # noqa: BLE001
        return 0


# --------------------------------------------------------------------------
# Historical baseline — auto-derive expected_duration_seconds from prior runs
# --------------------------------------------------------------------------


def _statistic(values: List[float], statistic: str) -> float:
    """Compute median / mean / p95 / p99 over `values`."""
    import math
    if not values:
        raise ValueError("cannot compute statistic over empty list")
    s = sorted(values)
    n = len(s)
    stat = (statistic or "median").lower()
    if stat == "mean":
        return sum(s) / n
    if stat == "median":
        mid = n // 2
        return s[mid] if n % 2 == 1 else (s[mid - 1] + s[mid]) / 2.0
    if stat in ("p95", "p99"):
        pct = 0.95 if stat == "p95" else 0.99
        rank = pct * (n - 1)
        lo = int(math.floor(rank))
        hi = int(math.ceil(rank))
        if lo == hi:
            return s[lo]
        return s[lo] + (s[hi] - s[lo]) * (rank - lo)
    raise ValueError(f"unknown statistic {statistic!r} (allowed: median/mean/p95/p99)")


def _derive_expected_from_history(
    context: Any,
    derive_cfg: Dict[str, Any],
    asset_key_obj: Optional[Any],
    fallback_expected: float,
) -> float:
    """Query prior AssetObservations tagged with `sla_actual_seconds` for
    `asset_key_obj`; return `statistic * multiplier` over the last N.

    Falls back to `fallback_expected` if fewer than 3 usable prior runs exist.
    """
    n_runs = int(derive_cfg.get("n_runs", 10))
    statistic = str(derive_cfg.get("statistic", "median"))
    multiplier = float(derive_cfg.get("multiplier", 1.5))
    try:
        from dagster import EventRecordsFilter, DagsterEventType
        records = context.instance.get_event_records(
            event_records_filter=EventRecordsFilter(
                event_type=DagsterEventType.ASSET_OBSERVATION,
                asset_key=asset_key_obj,
            ),
            limit=max(n_runs * 4, 40),
            ascending=False,
        )
    except Exception:  # noqa: BLE001
        return fallback_expected

    durations: List[float] = []
    for r in records:
        obs = getattr(r, "asset_observation", None)
        if obs is None:
            continue
        tags = obs.tags or {}
        actual: Optional[float] = None
        try:
            meta = obs.metadata or {}
            mv = meta.get("sla_actual_seconds")
            if mv is not None:
                actual = float(getattr(mv, "value", mv))
        except Exception:  # noqa: BLE001
            actual = None
        if actual is None:
            raw = tags.get("sla_actual_seconds")
            if raw is not None:
                try:
                    actual = float(raw)
                except (TypeError, ValueError):
                    actual = None
        if actual is not None and actual > 0:
            durations.append(actual)
            if len(durations) >= n_runs:
                break

    if len(durations) < 3:
        try:
            context.log.info(
                f"[sla] derive_expected_from_history: only {len(durations)} prior runs "
                f"(need >= 3); falling back to expected_duration_seconds={fallback_expected}s"
            )
        except Exception:  # noqa: BLE001
            pass
        return fallback_expected

    baseline = _statistic(durations, statistic)
    derived = baseline * multiplier
    try:
        context.log.info(
            f"[sla] derived expected_duration_seconds={derived:.3f}s from "
            f"{len(durations)} recent runs ({statistic}={baseline:.3f}s x multiplier={multiplier})"
        )
    except Exception:  # noqa: BLE001
        pass
    return derived


def _emit_sla_actual_observation(
    context: Any, key: str, actual_s: float, expected_s: float,
) -> None:
    """Emit AssetObservation for every run (breach OR non-breach) so the
    `sla_actual_seconds` history is populated for baseline derivation.

    Sanitizes the user-supplied ``key`` for the tag value (Dagster
    rejects values outside ``[A-Za-z0-9_.-]{,63}``) while preserving the
    raw key in metadata. Emission failures surface via ``log.warning``
    instead of being silently swallowed.
    """
    try:
        from dagster import AssetObservation, MetadataValue
        asset_key = getattr(context, "asset_key", None)
        if asset_key is None:
            from dagster import AssetKey
            asset_key = AssetKey(["sla_asset"])
        if hasattr(context, "log_event"):
            context.log_event(AssetObservation(
                asset_key=asset_key,
                tags={
                    "sla_key": _sanitize_tag_value(key),
                    "sla_actual_seconds": _sanitize_tag_value(str(round(actual_s, 3))),
                    "sla_expected_seconds": _sanitize_tag_value(str(round(expected_s, 3))),
                },
                metadata={
                    "sla_key": MetadataValue.text(str(key)),
                    "sla_actual_seconds": dg.MetadataValue.float(round(actual_s, 3)),
                    "sla_expected_seconds": dg.MetadataValue.float(round(expected_s, 3)),
                },
            ))
    except Exception as e:  # noqa: BLE001
        try:
            context.log.warning(
                f"@sla: could not emit actuals observation: {type(e).__name__}: {e}"
            )
        except Exception:  # noqa: BLE001
            pass


# --------------------------------------------------------------------------
# @sla decorator
# --------------------------------------------------------------------------


def sla(
    expected_duration_seconds: float,
    *,
    on_breach: str = "warn",
    escalate_after_n_breaches: Optional[int] = None,
    escalate_window_seconds: float = 3600,
    key: Optional[str] = None,
    per_partition_expected: Optional[Dict[str, float]] = None,
    partition_matcher: str = "exact",
    derive_expected_from_history: Optional[Dict[str, Any]] = None,
) -> Callable:
    """Wrap a compute function with wall-clock SLA enforcement.

    Applied BEFORE `@dg.asset`. Timer starts before the compute call,
    stops after. On breach, emits `AssetObservation(sla_breach=<key>,
    sla_actual_seconds, sla_expected_seconds, sla_overrun_pct, ...)`.

    ```python
    from dagster_community_components import sla

    @dg.asset
    @sla(
        expected_duration_seconds=60,
        on_breach="warn",   # or "fail"
        escalate_after_n_breaches=3,
        escalate_window_seconds=3600,
    )
    def slow_report(context):
        return build_slow_report()
    ```

    `on_breach='warn'` → asset materializes with the breach observation.
    `on_breach='fail'` → raise `dg.Failure` after emitting the observation.

    Escalation: after `escalate_after_n_breaches` breaches in
    `escalate_window_seconds` (uses `context.instance.get_event_records`),
    the observation is tagged `sla_escalated=true` for sensor-driven
    alerting.
    """
    if on_breach not in ("warn", "fail"):
        raise ValueError(f"on_breach must be 'warn' or 'fail'; got {on_breach!r}")

    def _decorator(fn: Callable) -> Callable:
        _state_key = key or getattr(fn, "__qualname__", None) or fn.__name__

        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            context = None
            if args and hasattr(args[0], "log"):
                context = args[0]
            elif "context" in kwargs and hasattr(kwargs["context"], "log"):
                context = kwargs["context"]
            if context is None:
                raise RuntimeError("@sla requires a Dagster context.")

            # `context.partition_key` is a property that RAISES for un-partitioned
            # runs (not just returns None). Wrap the access itself.
            try:
                partition_key = context.partition_key
            except Exception:  # noqa: BLE001
                partition_key = None
            effective_expected = _lookup_per_partition(
                partition_key, per_partition_expected, expected_duration_seconds, partition_matcher,
            )

            # Auto-derive from history — overrides both the hardcoded and per-partition
            # values when configured. Runs BEFORE the timer starts.
            if derive_expected_from_history and getattr(context, "instance", None) is not None:
                asset_key_obj = getattr(context, "asset_key", None)
                effective_expected = _derive_expected_from_history(
                    context, derive_expected_from_history, asset_key_obj, effective_expected,
                )

            t0 = time.time()
            result = fn(*args, **kwargs)
            elapsed = time.time() - t0

            if elapsed > effective_expected:
                escalated = False
                if escalate_after_n_breaches and getattr(context, "instance", None) is not None:
                    prior = _count_recent_breaches(context, _state_key, escalate_window_seconds)
                    # +1 for this breach
                    if prior + 1 >= escalate_after_n_breaches:
                        escalated = True

                _emit_breach_observation(
                    context, _state_key, elapsed, effective_expected, escalated,
                )
                overrun_pct = (elapsed - effective_expected) / effective_expected * 100.0
                context.log.warning(
                    f"[sla] BREACH: {_state_key} took {elapsed:.1f}s "
                    f"(expected <= {effective_expected}s, "
                    f"overrun {overrun_pct:.1f}%){' [ESCALATED]' if escalated else ''}"
                )
                if on_breach == "fail":
                    raise dg.Failure(
                        description=f"SLA breach: {_state_key} took {elapsed:.1f}s > expected {effective_expected}s",
                        metadata={
                            "sla_actual_seconds": dg.MetadataValue.float(round(elapsed, 3)),
                            "sla_expected_seconds": dg.MetadataValue.float(round(effective_expected, 3)),
                            "sla_overrun_pct": dg.MetadataValue.float(round(overrun_pct, 1)),
                            "sla_escalated": dg.MetadataValue.bool(escalated),
                        },
                    )
            else:
                context.log.info(
                    f"[sla] {_state_key} completed in {elapsed:.1f}s (within {effective_expected}s SLA)"
                )
                # Emit the actuals-only observation so history stays populated for
                # baseline derivation. Breaches already emit their own observation.
                _emit_sla_actual_observation(context, _state_key, elapsed, effective_expected)
            return result

        return _wrapped

    return _decorator


# --------------------------------------------------------------------------
# SlaAssetComponent
# --------------------------------------------------------------------------


class SlaAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of the SLA. Two authoring modes:

    1. **Define a new asset from scratch** (original shape): supply
       `asset_name` + `compute: {kind: python, python: 'mod:fn'}`. The
       component builds a single asset that runs the referenced Python
       compute inside the SLA timer.

    2. **Wrap an existing DCC component** (composability): supply
       `wraps: {type: <component_class>, attributes: {...}}`. The inner
       component's assets get materialized as they would normally, but
       each compute is wrapped with the SLA timer + breach observation.
       Preserves inner asset partitions, deps, resources, kinds, tags,
       group, description. Mirrors the Python `@sla @dg.asset` decorator
       stack idiom in YAML.

    `wraps` and `compute` are mutually exclusive.
    """

    asset_name: Optional[str] = Field(
        default=None,
        description="Dagster asset name. Required when NOT using `wraps:` (inherited from inner component in wraps mode).",
    )
    upstream_asset_key: Optional[str] = Field(default=None)
    compute: Optional[Dict[str, Any]] = Field(
        default=None,
        description="`{kind: python, python: 'mod:fn'}`. Any return type. Mutually exclusive with `wraps`.",
    )
    wraps: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Wrap another DCC component's assets with SLA timing instead of defining new compute. "
            "Shape: `{type: 'dagster_community_components.<Component>', attributes: {...}}`. "
            "Mutually exclusive with `compute`."
        ),
    )

    expected_duration_seconds: float = Field(
        description="Wall-clock SLA threshold. Compute time > this = breach."
    )
    on_breach: str = Field(
        default="warn",
        description="'warn' materializes + emits AssetObservation. 'fail' raises dg.Failure.",
    )
    escalate_after_n_breaches: Optional[int] = Field(
        default=None,
        description="Count breaches in escalate_window_seconds. If >= N, tag observation ESCALATED.",
    )
    escalate_window_seconds: float = Field(
        default=3600,
        description="Sliding window for breach counting (default 1 hour).",
    )
    sla_key: Optional[str] = Field(
        default=None,
        description="Shared SLA key. Defaults to asset_name. Set explicitly to group multiple assets under one SLA budget.",
    )
    per_partition_expected: Optional[Dict[str, float]] = Field(
        default=None,
        description=(
            "Per-partition-key override. e.g. {'hourly': 30, 'daily': 300}. Falls back to "
            "expected_duration_seconds if no key matches. Only meaningful on partitioned assets."
        ),
    )
    partition_matcher: str = Field(
        default="exact",
        description=(
            "How partition_key is matched against per_partition_expected keys: "
            "'exact' | 'prefix' | 'regex'. Default exact match."
        ),
    )
    derive_expected_from_history: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Auto-derive expected_duration_seconds from prior materializations. "
            "Shape: {n_runs: 10, statistic: 'median' | 'p95' | 'mean' | 'p99', multiplier: 1.5}. "
            "If set, this OVERRIDES expected_duration_seconds. "
            "n_runs = how many recent successful runs to consider. "
            "multiplier = safety factor (1.5 = allow 50%% overhead over baseline). "
            "If fewer than 3 runs available, falls back to expected_duration_seconds."
        ),
    )

    # Catalog / governance
    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Default: ['python', 'sla', 'observability'].",
    )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="SLA Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # Route: wraps-an-inner-component  vs  builds-own-asset
        if self.wraps is not None:
            if self.compute is not None:
                raise ValueError("SlaAssetComponent: supply exactly ONE of `wraps` or `compute`, not both.")
            return self._build_wrapped(context)

        if self.compute is None:
            raise ValueError("SlaAssetComponent: supply either `compute` (build a new asset) or `wraps` (wrap an existing component).")
        if not self.asset_name:
            raise ValueError("SlaAssetComponent: `asset_name` required when using `compute:` (inferred from inner in `wraps:` mode).")

        _self = self
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        expected_s = float(self.expected_duration_seconds)
        on_breach = self.on_breach
        escalate_n = self.escalate_after_n_breaches
        escalate_window = float(self.escalate_window_seconds)
        state_key = self.sla_key or asset_name
        per_partition_map = (
            {k: float(v) for k, v in self.per_partition_expected.items()}
            if self.per_partition_expected else None
        )
        matcher = self.partition_matcher
        derive_cfg = (
            dict(self.derive_expected_from_history) if self.derive_expected_from_history else None
        )

        if on_breach not in ("warn", "fail"):
            raise ValueError(f"on_breach must be warn|fail; got {on_breach!r}")

        kinds_set = set(self.kinds or []) | {"python", "sla", "observability"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        ins = {}
        if upstream_asset_key:
            ins["upstream"] = dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"SLA-wrapped asset {asset_name} (expected <= {expected_s}s)",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            kinds=kinds_set,
            ins=ins,
        )
        def _sla_asset(context: dg.AssetExecutionContext, **kwargs):
            kind = (compute.get("kind") or "python").lower()
            if kind != "python":
                raise ValueError(f"SlaAssetComponent v1 supports compute.kind=python only; got {kind!r}")
            ref = compute.get("python")
            if not ref or ":" not in ref:
                raise ValueError("compute.python must be 'module.path:function_name'")
            mod_path, fn_name = ref.rsplit(":", 1)
            fn = getattr(importlib.import_module(mod_path.strip()), fn_name.strip(), None)
            if not callable(fn):
                raise ValueError(f"compute.python {ref!r} not callable")

            import inspect
            sig = inspect.signature(fn)
            n_positional = sum(1 for p in sig.parameters.values()
                               if p.kind in (p.POSITIONAL_OR_KEYWORD, p.POSITIONAL_ONLY))

            try:
                partition_key = getattr(context, "partition_key", None)
            except Exception:  # noqa: BLE001
                partition_key = None
            effective_expected = _lookup_per_partition(
                partition_key, per_partition_map, expected_s, matcher,
            )

            # Auto-derive from history — OVERRIDES both the hardcoded + per-partition
            # values when configured. Runs BEFORE the timer starts.
            if derive_cfg and getattr(context, "instance", None) is not None:
                asset_key_obj = getattr(context, "asset_key", None)
                effective_expected = _derive_expected_from_history(
                    context, derive_cfg, asset_key_obj, effective_expected,
                )

            t0 = time.time()
            if n_positional == 0:
                result = fn()
            elif n_positional == 1:
                result = fn(context)
            else:
                result = fn(context, kwargs.get("upstream"))
            elapsed = time.time() - t0

            breach = elapsed > effective_expected
            escalated = False
            metadata = {
                "sla_actual_seconds": dg.MetadataValue.float(round(elapsed, 3)),
                "sla_expected_seconds": dg.MetadataValue.float(round(effective_expected, 3)),
                "sla_breach": dg.MetadataValue.bool(breach),
            }

            if breach:
                if escalate_n and getattr(context, "instance", None) is not None:
                    prior = _count_recent_breaches(context, state_key, escalate_window)
                    if prior + 1 >= escalate_n:
                        escalated = True
                _emit_breach_observation(context, state_key, elapsed, effective_expected, escalated)
                overrun_pct = (elapsed - effective_expected) / effective_expected * 100.0
                metadata["sla_overrun_pct"] = dg.MetadataValue.float(round(overrun_pct, 1))
                metadata["sla_escalated"] = dg.MetadataValue.bool(escalated)
                context.log.warning(
                    f"[sla] BREACH: {state_key} took {elapsed:.1f}s "
                    f"(expected <= {effective_expected}s, overrun {overrun_pct:.1f}%)"
                    f"{' [ESCALATED]' if escalated else ''}"
                )
                if on_breach == "fail":
                    raise dg.Failure(
                        description=f"SLA breach: {state_key} took {elapsed:.1f}s > expected {effective_expected}s",
                        metadata=metadata,
                    )
            else:
                context.log.info(
                    f"[sla] {state_key} completed in {elapsed:.1f}s (within {effective_expected}s SLA)"
                )
                # Emit non-breach observation so the historical baseline has actuals to derive from.
                _emit_sla_actual_observation(context, state_key, elapsed, effective_expected)

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_sla_asset])

    # ----------------------------------------------------------------------
    # `wraps:` composability path
    # ----------------------------------------------------------------------

    def _build_wrapped(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Instantiate the inner component; rebuild each of its assets
        with SLA timing wrapped around the original compute.
        """
        inner = _resolve_inner_component(self.wraps or {})
        inner_defs = inner.build_defs(context)

        wrapped_assets = []
        skipped_multi: List[str] = []
        for asset_def in list(inner_defs.assets or []):
            if len(asset_def.keys) != 1:
                # Multi-asset AssetsDefinition not supported in v1 — carry through unwrapped.
                skipped_multi.append(str(asset_def.keys))
                wrapped_assets.append(asset_def)
                continue
            wrapped_assets.append(self._wrap_single_asset(asset_def))

        return dg.Definitions(
            assets=wrapped_assets,
            resources=inner_defs.resources,
            sensors=inner_defs.sensors,
            schedules=inner_defs.schedules,
            asset_checks=inner_defs.asset_checks,
            jobs=inner_defs.jobs,
            loggers=inner_defs.loggers,
        )

    def _wrap_single_asset(self, asset_def: "dg.AssetsDefinition") -> "dg.AssetsDefinition":
        """Rebuild one single-key AssetsDefinition with wrapped compute.
        Preserves partitions/deps/kinds/tags/group/description/metadata.
        """
        key = next(iter(asset_def.keys))
        specs_by_key = getattr(asset_def, "specs_by_key", {}) or {}
        spec = specs_by_key.get(key)

        # Extract original callable — DecoratedOpFunction wraps it.
        inner_op = asset_def.op
        inner_compute = getattr(inner_op.compute_fn, "decorated_fn", None) or inner_op.compute_fn

        # SLA config for the closure
        expected_s = float(self.expected_duration_seconds)
        on_breach = self.on_breach
        escalate_n = self.escalate_after_n_breaches
        escalate_window = float(self.escalate_window_seconds)
        state_key = self.sla_key or key.to_user_string()
        per_partition_map = (
            {k: float(v) for k, v in self.per_partition_expected.items()}
            if self.per_partition_expected else None
        )
        matcher = self.partition_matcher
        derive_cfg = (
            dict(self.derive_expected_from_history) if self.derive_expected_from_history else None
        )

        if on_breach not in ("warn", "fail"):
            raise ValueError(f"on_breach must be warn|fail; got {on_breach!r}")

        # Merge kinds — inner's + our SLA marker
        inner_kinds = set()
        if spec is not None:
            inner_kinds = set(getattr(spec, "kinds", None) or [])
        merged_kinds = inner_kinds | set(self.kinds or []) | {"sla", "observability"}

        # Merge tags similarly
        inner_tags = {}
        if spec is not None:
            inner_tags = dict(getattr(spec, "tags", None) or {})
        merged_tags = {**inner_tags, **(self.tags or {})}

        merged_owners = list((spec.owners if spec else []) or []) + (self.owners or [])
        inner_description = (spec.description if spec else None) or f"SLA-wrapped {key.to_user_string()}"
        merged_description = f"{inner_description}  [SLA: <= {expected_s}s, on_breach={on_breach}]"

        # Preserve upstream input handling by rebuilding via @dg.asset with `deps=`
        inner_deps = list(spec.deps) if (spec and getattr(spec, "deps", None)) else []

        # NOTE: we don't attempt to preserve `ins=` (typed asset inputs with dagster_type)
        # in v1 — falls back to `deps=` (ordering-only) for the wrapped asset.

        @dg.asset(
            key=key,
            partitions_def=asset_def.partitions_def,
            deps=inner_deps,
            group_name=(spec.group_name if spec else None),
            kinds=merged_kinds,
            tags=merged_tags,
            owners=merged_owners,
            description=merged_description,
            metadata=(dict(spec.metadata) if (spec and spec.metadata) else {}),
            code_version=(spec.code_version if spec else None),
        )
        def _sla_wrapped(context: dg.AssetExecutionContext, **kwargs):
            try:
                partition_key = getattr(context, "partition_key", None)
            except Exception:  # noqa: BLE001
                partition_key = None
            effective_expected = _lookup_per_partition(
                partition_key, per_partition_map, expected_s, matcher,
            )

            # Auto-derive from history — OVERRIDES hardcoded/per-partition values.
            if derive_cfg and getattr(context, "instance", None) is not None:
                asset_key_obj = getattr(context, "asset_key", None)
                effective_expected = _derive_expected_from_history(
                    context, derive_cfg, asset_key_obj, effective_expected,
                )

            t0 = time.time()
            result = inner_compute(context, **kwargs)
            elapsed = time.time() - t0

            breach = elapsed > effective_expected
            escalated = False
            extra_meta = {
                "sla_actual_seconds": dg.MetadataValue.float(round(elapsed, 3)),
                "sla_expected_seconds": dg.MetadataValue.float(round(effective_expected, 3)),
                "sla_breach": dg.MetadataValue.bool(breach),
            }

            if breach:
                if escalate_n and getattr(context, "instance", None) is not None:
                    prior = _count_recent_breaches(context, state_key, escalate_window)
                    if prior + 1 >= escalate_n:
                        escalated = True
                _emit_breach_observation(context, state_key, elapsed, effective_expected, escalated)
                overrun_pct = (elapsed - effective_expected) / effective_expected * 100.0
                extra_meta["sla_overrun_pct"] = dg.MetadataValue.float(round(overrun_pct, 1))
                extra_meta["sla_escalated"] = dg.MetadataValue.bool(escalated)
                context.log.warning(
                    f"[sla] BREACH (wrap): {state_key} took {elapsed:.1f}s "
                    f"(expected <= {effective_expected}s, overrun {overrun_pct:.1f}%)"
                    f"{' [ESCALATED]' if escalated else ''}"
                )
                if on_breach == "fail":
                    raise dg.Failure(
                        description=f"SLA breach: {state_key} took {elapsed:.1f}s > expected {effective_expected}s",
                        metadata=extra_meta,
                    )
            else:
                context.log.info(
                    f"[sla] {state_key} completed in {elapsed:.1f}s (within {effective_expected}s SLA)"
                )
                # Non-breach: emit actuals-only observation so history stays populated.
                _emit_sla_actual_observation(context, state_key, elapsed, effective_expected)

            # If the inner returned a MaterializeResult, merge SLA metadata into it.
            if isinstance(result, dg.MaterializeResult):
                merged = dict(result.metadata or {})
                merged.update(extra_meta)
                return dg.MaterializeResult(
                    asset_key=result.asset_key,
                    metadata=merged,
                    check_results=result.check_results,
                    data_version=result.data_version,
                    tags=result.tags,
                )
            # Otherwise wrap in a MaterializeResult so we can attach metadata.
            # (Dagster still surfaces `result` — MaterializeResult without value is fine
            # since the inner asset's IO manager already handled the return.)
            if result is None:
                return dg.MaterializeResult(metadata=extra_meta)
            # For non-None returns, emit observation-style metadata via log_event
            # rather than losing the return value.
            try:
                context.log_event(dg.AssetMaterialization(
                    asset_key=key, metadata=extra_meta,
                ))
            except Exception as e:  # noqa: BLE001
                try:
                    context.log.warning(
                        f"@sla: could not emit materialization event: {type(e).__name__}: {e}"
                    )
                except Exception:  # noqa: BLE001
                    pass
            return result

        return _sla_wrapped


def _resolve_inner_component(wraps: Dict[str, Any]):
    """Resolve `{type: 'mod.path.ClassName' OR 'dagster_community_components.ClassName',
    attributes: {...}}` into an instantiated component.
    """
    type_str = wraps.get("type")
    attrs = wraps.get("attributes") or {}
    if not type_str or not isinstance(type_str, str):
        raise ValueError("SlaAssetComponent.wraps requires `type: <fully-qualified-class-name>`.")

    # Accept both `mod.path.ClassName` and `mod.path:ClassName`
    if ":" in type_str:
        mod_path, cls_name = type_str.rsplit(":", 1)
    else:
        mod_path, cls_name = type_str.rsplit(".", 1)

    try:
        mod = importlib.import_module(mod_path)
    except ImportError as e:
        raise ValueError(f"SlaAssetComponent.wraps: cannot import module {mod_path!r}: {e}") from e
    cls = getattr(mod, cls_name, None)
    if cls is None:
        raise ValueError(f"SlaAssetComponent.wraps: {cls_name!r} not found in {mod_path!r}.")

    try:
        return cls(**attrs)
    except Exception as e:  # noqa: BLE001
        raise ValueError(
            f"SlaAssetComponent.wraps: constructing {type_str} failed: {type(e).__name__}: {e}"
        ) from e
