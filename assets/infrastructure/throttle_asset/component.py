"""ThrottleAssetComponent + `@throttle` — cross-run rate limiting for asset compute.

Enforce a minimum gap between materializations of the same asset. Reads the
last materialization timestamp from `context.instance.get_event_records`;
if the gap-since-last-materialization is smaller than `min_gap_seconds`,
either SKIP (return without materializing) or FAIL (`dg.Failure`).

## Why this belongs in Dagster

- **Cross-run state** — throttle state lives in the Dagster event log,
  not in Redis or an in-process dict. Instance restarts don't reset it,
  concurrent runs on different workers see the same view.
- **Skip semantics** — a throttled compute emits an `AssetObservation`
  tagged `throttle_skipped=<key>` so runs are still auditable.
- **Complementary to schedules** — a schedule says "run every 5 min";
  a throttle says "regardless of trigger source, don't materialize
  within 30 s of the previous materialization." Manual re-runs, sensor
  retries, and eager AutomationConditions all get gated the same way.

## Two shapes

- **`ThrottleAssetComponent`** (YAML)
- **`@throttle` decorator** (Python)

## Behavior

- On entry, query the most recent `ASSET_MATERIALIZATION` event for this
  asset. If `now - last_materialized_at < min_gap_seconds`, throttle.
- `on_throttle=skip`: return `None`, emit `AssetObservation` tagged
  `throttle_skipped=<key>` with `throttle_wait_seconds` metadata. The
  asset does NOT re-materialize; downstream sees no new version.
- `on_throttle=fail`: raise `dg.Failure` — surfaced in Dagit as a step
  failure, will be retried by any wrapping `RetryPolicy`.
- If no prior materialization exists (first run ever), always allowed.

## Composes with

- `@smart_retry` — retries respect the throttle window.
- `@sla` — SLA measures duration; throttle measures inter-run gap.
- `@cached` — cache-hit still counts as a materialization for throttling.
- `@lifecycle` — throttle checks before staging; no wasted WAP work.
"""

import functools
import importlib
import time
from typing import Any, Callable, Dict, List, Optional

import dagster as dg
from pydantic import Field


_THROTTLE_TAG = "throttle_skipped"


def _last_materialization_ts(context: Any, asset_key: dg.AssetKey) -> Optional[float]:
    """Read the most recent materialization timestamp for `asset_key` from event log.

    Returns UNIX seconds, or None if no prior materialization.
    """
    try:
        instance = getattr(context, "instance", None)
        if instance is None:
            return None
        from dagster import EventRecordsFilter, DagsterEventType
        records = instance.get_event_records(
            event_records_filter=EventRecordsFilter(
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_key=asset_key,
            ),
            limit=1,
            ascending=False,
        )
        if not records:
            return None
        ts = records[0].timestamp
        if ts is None:
            return None
        return float(ts)
    except Exception:  # noqa: BLE001
        return None


def _emit_throttle_observation(
    context: Any, key: str, wait_seconds: float, min_gap_s: float,
) -> None:
    try:
        from dagster import AssetObservation
        asset_key = getattr(context, "asset_key", None)
        if asset_key is None:
            asset_key = dg.AssetKey([key or "throttle_asset"])
        tags = {
            _THROTTLE_TAG: key,
            "throttle_wait_seconds": str(round(wait_seconds, 3)),
            "throttle_min_gap_seconds": str(round(min_gap_s, 3)),
        }
        if hasattr(context, "log_event"):
            context.log_event(AssetObservation(
                asset_key=asset_key,
                tags=tags,
                metadata={
                    "throttle_wait_seconds": dg.MetadataValue.float(float(round(wait_seconds, 3))),
                    "throttle_min_gap_seconds": dg.MetadataValue.float(float(round(min_gap_s, 3))),
                },
            ))
    except Exception as e:  # noqa: BLE001
        try:
            context.log.warning(f"@throttle: could not emit observation: {e}")
        except Exception:  # noqa: BLE001
            pass


def _resolve_asset_key(context: Any, fallback: str) -> dg.AssetKey:
    ak = getattr(context, "asset_key", None)
    if ak is not None:
        return ak
    return dg.AssetKey([fallback or "throttle_asset"])


def throttle(
    min_gap_seconds: float,
    *,
    key: Optional[str] = None,
    on_throttle: str = "skip",
) -> Callable:
    """Enforce a minimum gap between materializations of the wrapped asset.

    ```python
    @dg.asset
    @throttle(min_gap_seconds=30, key="expensive_report", on_throttle="skip")
    def expensive_report(context):
        return build_report()
    ```

    On invocation, reads the most recent materialization event for this
    asset from the Dagster event log. If less than `min_gap_seconds` has
    elapsed since that event, either skip (default) or fail.

    Args:
        min_gap_seconds: Minimum wall-clock gap between materializations.
        key: Optional label attached to the throttle_skipped observation
            tag. Defaults to the asset key.
        on_throttle: "skip" (return None, emit observation) or
            "fail" (raise dg.Failure).
    """
    if on_throttle not in ("skip", "fail"):
        raise ValueError(f"on_throttle must be 'skip' or 'fail'; got {on_throttle!r}")

    def _decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            context = None
            if args and hasattr(args[0], "log"):
                context = args[0]
            elif "context" in kwargs and hasattr(kwargs["context"], "log"):
                context = kwargs["context"]
            if context is None:
                raise RuntimeError("@throttle requires a Dagster context (first positional arg or 'context' kwarg).")

            asset_key = _resolve_asset_key(context, key or "")
            label = key or asset_key.to_user_string()

            last_ts = _last_materialization_ts(context, asset_key)
            now = time.time()
            if last_ts is not None:
                elapsed = now - last_ts
                if elapsed < min_gap_seconds:
                    wait = min_gap_seconds - elapsed
                    _emit_throttle_observation(context, label, wait, min_gap_seconds)
                    if on_throttle == "fail":
                        raise dg.Failure(
                            description=f"@throttle: last materialization {elapsed:.3f}s ago, "
                                        f"min_gap={min_gap_seconds}s ({wait:.3f}s early)",
                            metadata={
                                "throttle_key": dg.MetadataValue.text(label),
                                "throttle_wait_seconds": dg.MetadataValue.float(float(round(wait, 3))),
                                "throttle_min_gap_seconds": dg.MetadataValue.float(float(round(min_gap_seconds, 3))),
                            },
                        )
                    try:
                        context.log.info(
                            f"@throttle skipped: last materialization {elapsed:.3f}s ago, "
                            f"min_gap={min_gap_seconds}s"
                        )
                    except Exception:  # noqa: BLE001
                        pass
                    return None

            return fn(*args, **kwargs)

        return _wrapped

    return _decorator


class ThrottleAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of `@throttle`. Wraps a compute with cross-run rate limiting."""

    asset_name: str = Field(description="Dagster asset name.")
    upstream_asset_key: Optional[str] = Field(default=None)
    compute: Dict[str, Any] = Field(description="`{kind: python, python: 'mod:fn'}`.")

    min_gap_seconds: float = Field(
        description="Minimum wall-clock gap between materializations. Materializations closer "
                    "than this are skipped or failed depending on on_throttle."
    )
    on_throttle: str = Field(
        default="skip",
        description="'skip' (default) returns None + emits AssetObservation; 'fail' raises dg.Failure.",
    )
    key: Optional[str] = Field(
        default=None,
        description="Optional label for the throttle_skipped observation tag. Defaults to asset name.",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None, description="Default: ['python', 'throttle'].")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Throttle Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        min_gap_s = float(self.min_gap_seconds)
        on_throttle_mode = self.on_throttle
        label = self.key or asset_name

        if on_throttle_mode not in ("skip", "fail"):
            raise ValueError(f"on_throttle must be 'skip' or 'fail'; got {on_throttle_mode!r}")

        kinds_set = set(self.kinds or []) | {"python", "throttle"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        ins = {}
        if upstream_asset_key:
            ins["upstream"] = dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Throttled asset {asset_name} (min_gap={min_gap_s}s)",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            kinds=kinds_set,
            ins=ins,
        )
        def _asset(context: dg.AssetExecutionContext, **kwargs):
            asset_key = _resolve_asset_key(context, asset_name)
            last_ts = _last_materialization_ts(context, asset_key)
            now = time.time()
            if last_ts is not None:
                elapsed = now - last_ts
                if elapsed < min_gap_s:
                    wait = min_gap_s - elapsed
                    _emit_throttle_observation(context, label, wait, min_gap_s)
                    if on_throttle_mode == "fail":
                        raise dg.Failure(
                            description=f"@throttle: last materialization {elapsed:.3f}s ago, "
                                        f"min_gap={min_gap_s}s ({wait:.3f}s early)",
                            metadata={
                                "throttle_key": dg.MetadataValue.text(label),
                                "throttle_wait_seconds": dg.MetadataValue.float(float(round(wait, 3))),
                                "throttle_min_gap_seconds": dg.MetadataValue.float(float(round(min_gap_s, 3))),
                            },
                        )
                    context.log.info(
                        f"@throttle skipped: last materialization {elapsed:.3f}s ago, "
                        f"min_gap={min_gap_s}s"
                    )
                    return dg.MaterializeResult(
                        metadata={
                            "throttle_skipped": dg.MetadataValue.bool(True),
                            "throttle_wait_seconds": dg.MetadataValue.float(float(round(wait, 3))),
                            "throttle_min_gap_seconds": dg.MetadataValue.float(float(round(min_gap_s, 3))),
                        }
                    )

            kind = (compute.get("kind") or "python").lower()
            if kind != "python":
                raise ValueError(f"ThrottleAssetComponent supports compute.kind=python only; got {kind!r}")
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
            if n_positional == 0:
                _ = fn()
            elif n_positional == 1:
                _ = fn(context)
            else:
                _ = fn(context, kwargs.get("upstream"))

            return dg.MaterializeResult(
                metadata={
                    "throttle_skipped": dg.MetadataValue.bool(False),
                    "throttle_min_gap_seconds": dg.MetadataValue.float(float(round(min_gap_s, 3))),
                }
            )

        return dg.Definitions(assets=[_asset])
