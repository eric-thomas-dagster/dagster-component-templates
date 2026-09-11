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


def _last_materialization_ts(
    context: Any,
    asset_key: dg.AssetKey,
    partition_key: Optional[str] = None,
) -> Optional[float]:
    """Read the most recent materialization timestamp for `asset_key` from event log.

    If `partition_key` is given, restrict to materializations of that partition
    (scan up to 200 recent materializations for a matching partition tag).

    Returns UNIX seconds, or None if no prior materialization.
    """
    try:
        instance = getattr(context, "instance", None)
        if instance is None:
            return None
        from dagster import EventRecordsFilter, DagsterEventType
        # When filtering by partition we may have to scan back a bit further to
        # find the most recent materialization of the *same* partition.
        limit = 200 if partition_key else 1
        records = instance.get_event_records(
            event_records_filter=EventRecordsFilter(
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_key=asset_key,
            ),
            limit=limit,
            ascending=False,
        )
        if not records:
            return None
        for r in records:
            if partition_key is not None:
                # Pull the partition off the materialization event.
                event_partition = None
                dagster_event = getattr(r, "dagster_event", None)
                if dagster_event is not None:
                    mat = getattr(dagster_event, "event_specific_data", None)
                    mat = getattr(mat, "materialization", None) if mat is not None else None
                    if mat is not None:
                        event_partition = getattr(mat, "partition", None)
                if event_partition is None:
                    # Fallback: look on the raw asset_materialization attr.
                    am = getattr(r, "asset_materialization", None)
                    if am is not None:
                        event_partition = getattr(am, "partition", None)
                if event_partition != partition_key:
                    continue
            ts = r.timestamp
            if ts is None:
                continue
            return float(ts)
        return None
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
    per_partition_min_gap: Optional[Dict[str, float]] = None,
    partition_matcher: str = "exact",
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

            try:
                partition_key = getattr(context, "partition_key", None)
            except Exception:  # noqa: BLE001
                partition_key = None
            effective_gap = _lookup_per_partition(
                partition_key, per_partition_min_gap, min_gap_seconds, partition_matcher,
            )
            # In per-partition mode, throttle against last materialization of the
            # SAME partition only (isolate hourly from daily, etc.).
            filter_partition = partition_key if per_partition_min_gap else None

            last_ts = _last_materialization_ts(context, asset_key, filter_partition)
            now = time.time()
            if last_ts is not None:
                elapsed = now - last_ts
                if elapsed < effective_gap:
                    wait = effective_gap - elapsed
                    _emit_throttle_observation(context, label, wait, effective_gap)
                    if on_throttle == "fail":
                        raise dg.Failure(
                            description=f"@throttle: last materialization {elapsed:.3f}s ago, "
                                        f"min_gap={effective_gap}s ({wait:.3f}s early)",
                            metadata={
                                "throttle_key": dg.MetadataValue.text(label),
                                "throttle_wait_seconds": dg.MetadataValue.float(float(round(wait, 3))),
                                "throttle_min_gap_seconds": dg.MetadataValue.float(float(round(effective_gap, 3))),
                            },
                        )
                    try:
                        context.log.info(
                            f"@throttle skipped: last materialization {elapsed:.3f}s ago, "
                            f"min_gap={effective_gap}s"
                        )
                    except Exception:  # noqa: BLE001
                        pass
                    return None

            return fn(*args, **kwargs)

        return _wrapped

    return _decorator


class ThrottleAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of `@throttle`. Two authoring modes:

    1. **Define a new asset from scratch** (original shape): supply
       `asset_name` + `compute: {kind: python, python: 'mod:fn'}`. Builds a
       single throttled asset that calls the referenced Python compute.

    2. **Wrap an existing DCC component** (composability): supply
       `wraps: {type: <component_class>, attributes: {...}}`. The inner
       component's assets get materialized as they would normally, but
       each compute is wrapped with the throttle primitive. Preserves
       inner asset partitions, deps, resources, kinds, tags, group,
       description. Direct YAML analog of `@throttle @dg.asset` in Python.

    `wraps:` and `compute:` are mutually exclusive.
    """

    asset_name: Optional[str] = Field(
        default=None,
        description="Dagster asset name. Required when NOT using `wraps:` (inherited from inner in wraps mode).",
    )
    upstream_asset_key: Optional[str] = Field(default=None)
    compute: Optional[Dict[str, Any]] = Field(
        default=None,
        description="`{kind: python, python: 'mod:fn'}`. Mutually exclusive with `wraps`.",
    )
    wraps: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Wrap another DCC component's assets with throttle rate-limiting instead of "
            "defining new compute. Shape: `{type: 'dagster_community_components.<Component>', "
            "attributes: {...}}`. Mutually exclusive with `compute`."
        ),
    )

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
    per_partition_min_gap: Optional[Dict[str, float]] = Field(
        default=None,
        description=(
            "Per-partition-key override. e.g. {'hourly': 30, 'daily': 300}. Falls back to "
            "min_gap_seconds if no key matches. When set, throttling is per-partition — the "
            "'last materialization' check is filtered to the same partition_key so hourly "
            "and daily partitions throttle independently."
        ),
    )
    partition_matcher: str = Field(
        default="exact",
        description=(
            "How partition_key is matched against per_partition_min_gap keys: "
            "'exact' | 'prefix' | 'regex'. Default exact match."
        ),
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
        # Route: wraps-an-inner-component  vs  builds-own-asset
        if self.wraps is not None:
            if self.compute is not None:
                raise ValueError("ThrottleAssetComponent: supply exactly ONE of `wraps` or `compute`, not both.")
            return self._build_wrapped(context)

        if self.compute is None:
            raise ValueError("ThrottleAssetComponent: supply either `compute` (build a new asset) or `wraps` (wrap an existing component).")
        if not self.asset_name:
            raise ValueError("ThrottleAssetComponent: `asset_name` required when using `compute:` (inferred from inner in `wraps:` mode).")

        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        min_gap_s = float(self.min_gap_seconds)
        on_throttle_mode = self.on_throttle
        label = self.key or asset_name
        per_partition_map = (
            {k: float(v) for k, v in self.per_partition_min_gap.items()}
            if self.per_partition_min_gap else None
        )
        matcher = self.partition_matcher

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
            try:
                partition_key = getattr(context, "partition_key", None)
            except Exception:  # noqa: BLE001
                partition_key = None
            effective_gap = _lookup_per_partition(
                partition_key, per_partition_map, min_gap_s, matcher,
            )
            filter_partition = partition_key if per_partition_map else None
            last_ts = _last_materialization_ts(context, asset_key, filter_partition)
            now = time.time()
            if last_ts is not None:
                elapsed = now - last_ts
                if elapsed < effective_gap:
                    wait = effective_gap - elapsed
                    _emit_throttle_observation(context, label, wait, effective_gap)
                    if on_throttle_mode == "fail":
                        raise dg.Failure(
                            description=f"@throttle: last materialization {elapsed:.3f}s ago, "
                                        f"min_gap={effective_gap}s ({wait:.3f}s early)",
                            metadata={
                                "throttle_key": dg.MetadataValue.text(label),
                                "throttle_wait_seconds": dg.MetadataValue.float(float(round(wait, 3))),
                                "throttle_min_gap_seconds": dg.MetadataValue.float(float(round(effective_gap, 3))),
                            },
                        )
                    context.log.info(
                        f"@throttle skipped: last materialization {elapsed:.3f}s ago, "
                        f"min_gap={effective_gap}s"
                    )
                    return dg.MaterializeResult(
                        metadata={
                            "throttle_skipped": dg.MetadataValue.bool(True),
                            "throttle_wait_seconds": dg.MetadataValue.float(float(round(wait, 3))),
                            "throttle_min_gap_seconds": dg.MetadataValue.float(float(round(effective_gap, 3))),
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
                    "throttle_min_gap_seconds": dg.MetadataValue.float(float(round(effective_gap, 3))),
                }
            )

        return dg.Definitions(assets=[_asset])

    # ----------------------------------------------------------------------
    # `wraps:` composability — YAML analog of `@throttle @dg.asset` stacking
    # ----------------------------------------------------------------------

    def _build_wrapped(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Instantiate the inner component; rewrap each of its assets
        with throttle rate-limiting around the original compute.
        """
        inner = _resolve_inner_component(self.wraps or {})
        inner_defs = inner.build_defs(context)

        wrapped_assets = []
        for asset_def in list(inner_defs.assets or []):
            if len(asset_def.keys) != 1:
                # Multi-asset AssetsDefinition not supported in v1 — pass through unwrapped.
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
        """Rebuild one single-key AssetsDefinition with throttle wrapped around
        the original compute. Preserves partitions/deps/kinds/tags/group/description/metadata.
        """
        key = next(iter(asset_def.keys))
        specs_by_key = getattr(asset_def, "specs_by_key", {}) or {}
        spec = specs_by_key.get(key)

        inner_op = asset_def.op
        inner_compute = getattr(inner_op.compute_fn, "decorated_fn", None) or inner_op.compute_fn

        min_gap_s = float(self.min_gap_seconds)
        on_throttle_mode = self.on_throttle
        label = self.key or key.to_user_string()
        per_partition_map = (
            {k: float(v) for k, v in self.per_partition_min_gap.items()}
            if self.per_partition_min_gap else None
        )
        matcher = self.partition_matcher

        if on_throttle_mode not in ("skip", "fail"):
            raise ValueError(f"on_throttle must be 'skip' or 'fail'; got {on_throttle_mode!r}")

        inner_kinds = set(getattr(spec, "kinds", None) or []) if spec else set()
        merged_kinds = inner_kinds | set(self.kinds or []) | {"throttle"}
        inner_tags = dict(getattr(spec, "tags", None) or {}) if spec else {}
        merged_tags = {**inner_tags, **(self.tags or {})}
        merged_owners = list((spec.owners if spec else []) or []) + (self.owners or [])
        inner_description = (spec.description if spec else None) or f"Throttled {key.to_user_string()}"
        merged_description = f"{inner_description}  [throttle: min_gap={min_gap_s}s, on={on_throttle_mode}]"
        inner_deps = list(spec.deps) if (spec and getattr(spec, "deps", None)) else []

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
        def _throttle_wrapped(context: dg.AssetExecutionContext, **kwargs):
            # Throttle gate — check event log for last materialization.
            asset_key = _resolve_asset_key(context, label)
            try:
                partition_key = getattr(context, "partition_key", None)
            except Exception:  # noqa: BLE001
                partition_key = None
            effective_gap = _lookup_per_partition(
                partition_key, per_partition_map, min_gap_s, matcher,
            )
            filter_partition = partition_key if per_partition_map else None
            last_ts = _last_materialization_ts(context, asset_key, filter_partition)
            now = time.time()
            if last_ts is not None:
                elapsed = now - last_ts
                if elapsed < effective_gap:
                    wait = effective_gap - elapsed
                    _emit_throttle_observation(context, label, wait, effective_gap)
                    if on_throttle_mode == "fail":
                        raise dg.Failure(
                            description=f"@throttle (wrap): last materialization {elapsed:.3f}s ago, "
                                        f"min_gap={effective_gap}s ({wait:.3f}s early)",
                            metadata={
                                "throttle_key": dg.MetadataValue.text(label),
                                "throttle_wait_seconds": dg.MetadataValue.float(float(round(wait, 3))),
                                "throttle_min_gap_seconds": dg.MetadataValue.float(float(round(effective_gap, 3))),
                            },
                        )
                    context.log.info(
                        f"[throttle wrap] skipped: last materialization {elapsed:.3f}s ago, "
                        f"min_gap={effective_gap}s"
                    )
                    return dg.MaterializeResult(
                        metadata={
                            "throttle_skipped": dg.MetadataValue.bool(True),
                            "throttle_wait_seconds": dg.MetadataValue.float(float(round(wait, 3))),
                            "throttle_min_gap_seconds": dg.MetadataValue.float(float(round(effective_gap, 3))),
                        }
                    )

            # Passthrough — call the inner compute
            result = inner_compute(context, **kwargs)

            # Merge throttle metadata into the inner's MaterializeResult (if any)
            passthrough_meta = {
                "throttle_skipped": dg.MetadataValue.bool(False),
                "throttle_min_gap_seconds": dg.MetadataValue.float(float(round(effective_gap, 3))),
            }
            if isinstance(result, dg.MaterializeResult):
                merged = dict(result.metadata or {})
                merged.update(passthrough_meta)
                return dg.MaterializeResult(
                    asset_key=result.asset_key,
                    metadata=merged,
                    check_results=result.check_results,
                    data_version=result.data_version,
                    tags=result.tags,
                )
            return result

        return _throttle_wrapped


def _resolve_inner_component(wraps: Dict[str, Any]):
    """Resolve `{type: 'mod.path.ClassName' OR 'mod.path:ClassName', attributes: {...}}` → component instance."""
    type_str = wraps.get("type")
    attrs = wraps.get("attributes") or {}
    if not type_str or not isinstance(type_str, str):
        raise ValueError("ThrottleAssetComponent.wraps requires `type: <fully-qualified-class-name>`.")
    if ":" in type_str:
        mod_path, cls_name = type_str.rsplit(":", 1)
    else:
        mod_path, cls_name = type_str.rsplit(".", 1)
    try:
        mod = importlib.import_module(mod_path)
    except ImportError as e:
        raise ValueError(f"ThrottleAssetComponent.wraps: cannot import module {mod_path!r}: {e}") from e
    cls = getattr(mod, cls_name, None)
    if cls is None:
        raise ValueError(f"ThrottleAssetComponent.wraps: {cls_name!r} not found in {mod_path!r}.")
    try:
        return cls(**attrs)
    except Exception as e:  # noqa: BLE001
        raise ValueError(
            f"ThrottleAssetComponent.wraps: constructing {type_str} failed: {type(e).__name__}: {e}"
        ) from e
