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
# Breach event emission + cross-run history
# --------------------------------------------------------------------------

_BREACH_TAG = "sla_breach"


def _emit_breach_observation(
    context: Any, key: str, actual_s: float, expected_s: float, escalated: bool,
):
    """Emit AssetObservation with breach metadata + tag."""
    try:
        from dagster import AssetObservation
        asset_key = getattr(context, "asset_key", None)
        if asset_key is None:
            from dagster import AssetKey
            asset_key = AssetKey(["sla_asset"])
        tags = {
            _BREACH_TAG: key,
            "sla_actual_seconds": str(round(actual_s, 3)),
            "sla_expected_seconds": str(round(expected_s, 3)),
            "sla_escalated": str(escalated).lower(),
        }
        if hasattr(context, "log_event"):
            context.log_event(AssetObservation(
                asset_key=asset_key,
                tags=tags,
                metadata={
                    "sla_actual_seconds": dg.MetadataValue.float(round(actual_s, 3)),
                    "sla_expected_seconds": dg.MetadataValue.float(round(expected_s, 3)),
                    "sla_overrun_seconds": dg.MetadataValue.float(round(actual_s - expected_s, 3)),
                    "sla_overrun_pct": dg.MetadataValue.float(round((actual_s - expected_s) / expected_s * 100.0, 1)),
                    "sla_escalated": dg.MetadataValue.bool(escalated),
                },
            ))
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
# @sla decorator
# --------------------------------------------------------------------------


def sla(
    expected_duration_seconds: float,
    *,
    on_breach: str = "warn",
    escalate_after_n_breaches: Optional[int] = None,
    escalate_window_seconds: float = 3600,
    key: Optional[str] = None,
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

            t0 = time.time()
            result = fn(*args, **kwargs)
            elapsed = time.time() - t0

            if elapsed > expected_duration_seconds:
                escalated = False
                if escalate_after_n_breaches and getattr(context, "instance", None) is not None:
                    prior = _count_recent_breaches(context, _state_key, escalate_window_seconds)
                    # +1 for this breach
                    if prior + 1 >= escalate_after_n_breaches:
                        escalated = True

                _emit_breach_observation(
                    context, _state_key, elapsed, expected_duration_seconds, escalated,
                )
                overrun_pct = (elapsed - expected_duration_seconds) / expected_duration_seconds * 100.0
                context.log.warning(
                    f"[sla] BREACH: {_state_key} took {elapsed:.1f}s "
                    f"(expected <= {expected_duration_seconds}s, "
                    f"overrun {overrun_pct:.1f}%){' [ESCALATED]' if escalated else ''}"
                )
                if on_breach == "fail":
                    raise dg.Failure(
                        description=f"SLA breach: {_state_key} took {elapsed:.1f}s > expected {expected_duration_seconds}s",
                        metadata={
                            "sla_actual_seconds": dg.MetadataValue.float(round(elapsed, 3)),
                            "sla_expected_seconds": dg.MetadataValue.float(round(expected_duration_seconds, 3)),
                            "sla_overrun_pct": dg.MetadataValue.float(round(overrun_pct, 1)),
                            "sla_escalated": dg.MetadataValue.bool(escalated),
                        },
                    )
            else:
                context.log.info(
                    f"[sla] {_state_key} completed in {elapsed:.1f}s (within {expected_duration_seconds}s SLA)"
                )
            return result

        return _wrapped

    return _decorator


# --------------------------------------------------------------------------
# SlaAssetComponent
# --------------------------------------------------------------------------


class SlaAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of the SLA. Defines a new asset with wall-clock SLA
    enforcement wrapping the compute.
    """

    asset_name: str = Field(description="Dagster asset name.")
    upstream_asset_key: Optional[str] = Field(default=None)
    compute: Dict[str, Any] = Field(description="`{kind: python, python: 'mod:fn'}`. Any return type.")

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
        _self = self
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        expected_s = float(self.expected_duration_seconds)
        on_breach = self.on_breach
        escalate_n = self.escalate_after_n_breaches
        escalate_window = float(self.escalate_window_seconds)
        state_key = self.sla_key or asset_name

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

            t0 = time.time()
            if n_positional == 0:
                result = fn()
            elif n_positional == 1:
                result = fn(context)
            else:
                result = fn(context, kwargs.get("upstream"))
            elapsed = time.time() - t0

            breach = elapsed > expected_s
            escalated = False
            metadata = {
                "sla_actual_seconds": dg.MetadataValue.float(round(elapsed, 3)),
                "sla_expected_seconds": dg.MetadataValue.float(round(expected_s, 3)),
                "sla_breach": dg.MetadataValue.bool(breach),
            }

            if breach:
                if escalate_n and getattr(context, "instance", None) is not None:
                    prior = _count_recent_breaches(context, state_key, escalate_window)
                    if prior + 1 >= escalate_n:
                        escalated = True
                _emit_breach_observation(context, state_key, elapsed, expected_s, escalated)
                overrun_pct = (elapsed - expected_s) / expected_s * 100.0
                metadata["sla_overrun_pct"] = dg.MetadataValue.float(round(overrun_pct, 1))
                metadata["sla_escalated"] = dg.MetadataValue.bool(escalated)
                context.log.warning(
                    f"[sla] BREACH: {state_key} took {elapsed:.1f}s "
                    f"(expected <= {expected_s}s, overrun {overrun_pct:.1f}%)"
                    f"{' [ESCALATED]' if escalated else ''}"
                )
                if on_breach == "fail":
                    raise dg.Failure(
                        description=f"SLA breach: {state_key} took {elapsed:.1f}s > expected {expected_s}s",
                        metadata=metadata,
                    )
            else:
                context.log.info(
                    f"[sla] {state_key} completed in {elapsed:.1f}s (within {expected_s}s SLA)"
                )

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_sla_asset])
