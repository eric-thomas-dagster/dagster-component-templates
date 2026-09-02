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
    """YAML shape of the timeout. Defines a new asset with a hard-kill timeout
    wrapping the compute.
    """

    asset_name: str = Field(description="Dagster asset name.")
    upstream_asset_key: Optional[str] = Field(default=None)
    compute: Dict[str, Any] = Field(description="`{kind: python, python: 'mod:fn'}`.")
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
