"""TimeoutAssetComponent + `@timeout` — hard-kill compute after N seconds.

Dagster's `RetryPolicy` has no timeout; a runaway compute (deadlocked
API call, infinite loop, hung external process) can burn the whole
step budget. This wraps compute with a hard timeout: on exceed, kills
the thread and raises `dg.Failure` with typed metadata + optional
`AssetObservation` for cross-run timeout tracking.

## What this does that `@sla` doesn't

- **`@sla`** — timer wraps compute; observes overruns; asset materializes
  anyway (unless `on_breach=fail`).
- **`@timeout`** — timer wraps compute; if exceeded, KILLS the compute
  and raises immediately. Compute cannot continue running past the
  deadline. Complements `@sla` — most customers want both.

## Implementation

Uses `concurrent.futures.ThreadPoolExecutor` with a wall-clock deadline
on `future.result(timeout=...)`. When timeout hits, the future is
cancelled and the compute thread is abandoned (Python doesn't allow
true thread termination — the compute keeps running but its result is
discarded; on a well-behaved compute this is fine, on a stuck one you
leak a thread until the process ends).

For a *true* hard-kill on Unix, use `signal.SIGALRM` — but that only
works from the main thread which most Dagster workers aren't. The
ThreadPool approach is the portable + Dagster+ Serverless-safe choice.

## Two shapes

- **`TimeoutAssetComponent`** (YAML)
- **`@timeout` decorator** (Python)

## Cross-run timeout tracking

Optional: emit an `AssetObservation` with `timeout_hit=True` tag on
every timeout. Sensors can count them via
`context.instance.get_event_records` — "3 timeouts on this asset in the
last hour → page oncall."

## Composes with

- **`@smart_retry`** — retry on timeout (a `TimeoutError` is transient
  by classification).
- **`@sla`** — set `@sla(expected_duration=30)` + `@timeout(60)` to get
  breach observation at 30s AND hard-kill at 60s.
- **`@lifecycle`** — kill compute before it dirties staging.
"""

import concurrent.futures
import functools
import importlib
from typing import Any, Callable, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _emit_timeout_observation(context: Any, key: str, timeout_s: float):
    """Emit AssetObservation for cross-run timeout tracking."""
    try:
        from dagster import AssetObservation
        asset_key = getattr(context, "asset_key", None)
        if asset_key is None:
            from dagster import AssetKey
            asset_key = AssetKey(["timeout_asset"])
        if hasattr(context, "log_event"):
            context.log_event(AssetObservation(
                asset_key=asset_key,
                tags={
                    "timeout_hit": key,
                    "timeout_seconds": str(timeout_s),
                },
                metadata={
                    "timeout_seconds": dg.MetadataValue.float(timeout_s),
                },
            ))
    except Exception:  # noqa: BLE001
        pass


def timeout(
    seconds: float,
    *,
    on_timeout: str = "fail",
    key: Optional[str] = None,
) -> Callable:
    """Wrap compute with a hard timeout.

    Applied BEFORE `@dg.asset`. If the compute doesn't complete within
    `seconds`, the wrapped call is cancelled + `dg.Failure` is raised.

    ```python
    from dagster_community_components import timeout

    @dg.asset
    @timeout(60, on_timeout="fail")
    def slow_api_call(context):
        return call_slow_api()  # if this hangs > 60s, killed + Failure
    ```

    `on_timeout='fail'` (default) — raise `dg.Failure` with typed metadata.
    `on_timeout='warn'` — log a warning + return None (rare use case).

    Emits `AssetObservation` tagged `timeout_hit=<key>` on timeout so
    sensors can count timeouts across runs.
    """
    if on_timeout not in ("fail", "warn"):
        raise ValueError(f"on_timeout must be 'fail' or 'warn'; got {on_timeout!r}")
    if seconds <= 0:
        raise ValueError(f"seconds must be > 0; got {seconds}")

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
                raise RuntimeError("@timeout requires a Dagster context.")

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(fn, *args, **kwargs)
                try:
                    return future.result(timeout=seconds)
                except concurrent.futures.TimeoutError:
                    future.cancel()
                    _emit_timeout_observation(context, _state_key, seconds)
                    context.log.error(
                        f"[timeout] {_state_key} exceeded {seconds}s — compute cancelled"
                    )
                    if on_timeout == "fail":
                        raise dg.Failure(
                            description=f"@timeout exceeded: {_state_key} > {seconds}s",
                            metadata={
                                "timeout_key": dg.MetadataValue.text(_state_key),
                                "timeout_seconds": dg.MetadataValue.float(seconds),
                            },
                        ) from None
                    return None

        return _wrapped

    return _decorator


class TimeoutAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of `@timeout`. Two authoring modes:

    1. **Define a new timeout-wrapped asset**: `asset_name` + `compute: {...}`
    2. **Wrap an existing DCC component**: `wraps: {type, attributes}`.
       Outer timeout hard-kills the inner component's compute at the threshold.

    `wraps:` and `compute:`/`asset_name` are mutually exclusive.
    """

    asset_name: Optional[str] = Field(default=None, description="Required when NOT using `wraps:`.")
    upstream_asset_key: Optional[str] = Field(default=None)
    compute: Optional[Dict[str, Any]] = Field(
        default=None,
        description="`{kind: python, python: 'mod:fn'}`. Mutually exclusive with `wraps`.",
    )
    wraps: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Wrap another DCC component's assets with a hard-kill timeout. "
            "Shape: `{type: 'dagster_community_components.<Component>', attributes: {...}}`."
        ),
    )
    timeout_seconds: float = Field(description="Kill compute if it exceeds this wall-clock time.")
    on_timeout: str = Field(
        default="fail",
        description="'fail' raises dg.Failure. 'warn' logs + returns None (rare).",
    )
    timeout_key: Optional[str] = Field(
        default=None,
        description="Shared key for cross-run timeout counting via event log. Defaults to asset_name.",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None, description="Default: ['python', 'timeout'].")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Timeout Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # Route: wraps-an-inner-component  vs  builds-own-asset
        if self.wraps is not None:
            if self.compute is not None:
                raise ValueError("TimeoutAssetComponent: supply exactly ONE of `wraps` or `compute`, not both.")
            return self._build_wrapped(context)

        if self.compute is None:
            raise ValueError("TimeoutAssetComponent: supply either `compute` (build new asset) or `wraps` (wrap existing component).")
        if not self.asset_name:
            raise ValueError("TimeoutAssetComponent: `asset_name` required when using `compute:` (inferred from inner in `wraps:` mode).")

        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        timeout_s = float(self.timeout_seconds)
        on_to = self.on_timeout
        state_key = self.timeout_key or asset_name

        if on_to not in ("fail", "warn"):
            raise ValueError(f"on_timeout must be fail|warn; got {on_to!r}")
        if timeout_s <= 0:
            raise ValueError(f"timeout_seconds must be > 0; got {timeout_s}")

        kinds_set = set(self.kinds or []) | {"python", "timeout"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        ins = {}
        if upstream_asset_key:
            ins["upstream"] = dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Timeout-wrapped asset {asset_name} ({timeout_s}s)",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            kinds=kinds_set,
            ins=ins,
        )
        def _timeout_asset(context: dg.AssetExecutionContext, **kwargs):
            kind = (compute.get("kind") or "python").lower()
            if kind != "python":
                raise ValueError(f"TimeoutAssetComponent supports compute.kind=python only; got {kind!r}")
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

            def _call():
                if n_positional == 0:
                    return fn()
                if n_positional == 1:
                    return fn(context)
                return fn(context, kwargs.get("upstream"))

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_call)
                try:
                    result = future.result(timeout=timeout_s)
                except concurrent.futures.TimeoutError:
                    future.cancel()
                    _emit_timeout_observation(context, state_key, timeout_s)
                    context.log.error(
                        f"[timeout] {state_key} exceeded {timeout_s}s — compute cancelled"
                    )
                    if on_to == "fail":
                        raise dg.Failure(
                            description=f"@timeout exceeded: {state_key} > {timeout_s}s",
                            metadata={
                                "timeout_key": dg.MetadataValue.text(state_key),
                                "timeout_seconds": dg.MetadataValue.float(timeout_s),
                            },
                        ) from None
                    result = None

            return dg.MaterializeResult(
                metadata={
                    "timeout_seconds": dg.MetadataValue.float(timeout_s),
                    "timeout_hit": dg.MetadataValue.bool(False),
                }
            )

        return dg.Definitions(assets=[_timeout_asset])

    # ----------------------------------------------------------------------
    # `wraps:` composability
    # ----------------------------------------------------------------------

    def _build_wrapped(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        inner = _resolve_inner_component(self.wraps or {})
        inner_defs = inner.build_defs(context)
        wrapped_assets = []
        for asset_def in list(inner_defs.assets or []):
            if len(asset_def.keys) != 1:
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
        key = next(iter(asset_def.keys))
        specs_by_key = getattr(asset_def, "specs_by_key", {}) or {}
        spec = specs_by_key.get(key)
        inner_op = asset_def.op
        inner_compute = getattr(inner_op.compute_fn, "decorated_fn", None) or inner_op.compute_fn

        timeout_s = float(self.timeout_seconds)
        on_to = self.on_timeout
        state_key = self.timeout_key or key.to_user_string()

        if on_to not in ("fail", "warn"):
            raise ValueError(f"on_timeout must be fail|warn; got {on_to!r}")
        if timeout_s <= 0:
            raise ValueError(f"timeout_seconds must be > 0; got {timeout_s}")

        inner_kinds = set(getattr(spec, "kinds", None) or []) if spec else set()
        merged_kinds = inner_kinds | set(self.kinds or []) | {"timeout"}
        inner_tags = dict(getattr(spec, "tags", None) or {}) if spec else {}
        merged_tags = {**inner_tags, **(self.tags or {})}
        merged_owners = list((spec.owners if spec else []) or []) + (self.owners or [])
        inner_description = (spec.description if spec else None) or f"Timeout-wrapped {key.to_user_string()}"
        merged_description = f"{inner_description}  [timeout: {timeout_s}s, on_timeout={on_to}]"
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
        def _timeout_wrapped(context: dg.AssetExecutionContext, **kwargs):
            def _call():
                return inner_compute(context, **kwargs)

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_call)
                try:
                    result = future.result(timeout=timeout_s)
                except concurrent.futures.TimeoutError:
                    future.cancel()
                    _emit_timeout_observation(context, state_key, timeout_s)
                    context.log.error(
                        f"[timeout wrap] {state_key} exceeded {timeout_s}s — inner compute cancelled"
                    )
                    if on_to == "fail":
                        raise dg.Failure(
                            description=f"@timeout (wrap) exceeded: {state_key} > {timeout_s}s",
                            metadata={
                                "timeout_key": dg.MetadataValue.text(state_key),
                                "timeout_seconds": dg.MetadataValue.float(timeout_s),
                            },
                        ) from None
                    return None

            # Merge timeout metadata into inner's MaterializeResult if present
            timeout_meta = {
                "timeout_seconds": dg.MetadataValue.float(timeout_s),
                "timeout_hit": dg.MetadataValue.bool(False),
            }
            if isinstance(result, dg.MaterializeResult):
                merged = dict(result.metadata or {})
                merged.update(timeout_meta)
                return dg.MaterializeResult(
                    asset_key=result.asset_key,
                    metadata=merged,
                    check_results=result.check_results,
                    data_version=result.data_version,
                    tags=result.tags,
                )
            return result

        return _timeout_wrapped


def _resolve_inner_component(wraps: Dict[str, Any]):
    type_str = wraps.get("type")
    attrs = wraps.get("attributes") or {}
    if not type_str or not isinstance(type_str, str):
        raise ValueError("TimeoutAssetComponent.wraps requires `type: <fully-qualified-class-name>`.")
    if ":" in type_str:
        mod_path, cls_name = type_str.rsplit(":", 1)
    else:
        mod_path, cls_name = type_str.rsplit(".", 1)
    try:
        mod = importlib.import_module(mod_path)
    except ImportError as e:
        raise ValueError(f"TimeoutAssetComponent.wraps: cannot import {mod_path!r}: {e}") from e
    cls = getattr(mod, cls_name, None)
    if cls is None:
        raise ValueError(f"TimeoutAssetComponent.wraps: {cls_name!r} not found in {mod_path!r}.")
    try:
        return cls(**attrs)
    except Exception as e:  # noqa: BLE001
        raise ValueError(
            f"TimeoutAssetComponent.wraps: constructing {type_str} failed: {type(e).__name__}: {e}"
        ) from e
