"""HooksAssetComponent + `@on_hooks` — asset-scoped success/failure callbacks.

Prefect's `@task(on_completion=..., on_failure=...)` — Dagster equivalent
today is `@dg.success_hook` / `@dg.failure_hook` on jobs (not the asset).
This wraps compute with hooks that fire AFTER the wrapped function
returns (success) or raises (failure), receiving the context + the
result or exception.

## Why this shape

Dagster's built-in hooks are job-scoped: you attach them to a job's ops.
That works when you have a job with multiple ops. For asset-first
projects — where every asset is its own compute — you want the callback
right next to the asset, not off to the side in a job wiring.

`@on_hooks` gives you that. Callbacks are ordinary Python functions
referenced by `mod:fn` — same shape as `@lifecycle`'s `audit.python`
kind, `@data_contract`'s custom probes, etc.

## Two shapes

- **`HooksAssetComponent`** (YAML)
- **`@on_hooks` decorator** (Python)

## Signatures

Success callback: `fn(context, result) -> None`
Failure callback: `fn(context, exception) -> None`

The result of the wrapped compute is passed to on_success; the raised
exception (or `dg.Failure`) to on_failure. Callbacks are called
sequentially; any exception raised by a callback is LOGGED but doesn't
alter the outcome (compute success stays success; failure stays
failure). This matches Prefect's semantics.

## Composes with

- **`@smart_retry`** — hooks fire once per materialization outcome,
  not per retry attempt.
- **`@sla`** — pair with `on_failure=notify_slack` for SLA-breach alerts.
- **`@data_contract`** — pair with `on_failure=create_jira_ticket` for
  contract violations.

## What's not in v1

- **on_start / on_end** — fire before compute + after regardless of
  outcome. `finally`-style hooks.
- **Async hooks** — v1 runs callbacks synchronously.
"""

import functools
import importlib
from typing import Any, Callable, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _run_callbacks(
    callbacks: List[str], context: Any, arg: Any, kind: str,
) -> None:
    """Run each callback in `callbacks` sequentially, catching + logging any
    exceptions they raise. `kind` is 'success' or 'failure' for log tagging."""
    for ref in callbacks:
        if not ref or ":" not in ref:
            context.log.warning(
                f"[hooks] {kind}: malformed callback ref {ref!r} (expected 'mod:fn')"
            )
            continue
        try:
            mod_path, fn_name = ref.rsplit(":", 1)
            fn = getattr(importlib.import_module(mod_path.strip()), fn_name.strip(), None)
            if not callable(fn):
                context.log.warning(f"[hooks] {kind}: {ref!r} not callable")
                continue
            fn(context, arg)
        except Exception as exc:  # noqa: BLE001
            context.log.error(
                f"[hooks] {kind} callback {ref!r} raised {type(exc).__name__}: {exc}"
            )


def on_hooks(
    *,
    on_success: Optional[List[str]] = None,
    on_failure: Optional[List[str]] = None,
) -> Callable:
    """Attach on_success / on_failure callbacks to a Dagster asset compute.

    Applied BEFORE `@dg.asset`. Callbacks are ordinary Python `mod:fn`
    references. Signatures:
      - `on_success`: `fn(context, result) -> None`
      - `on_failure`: `fn(context, exception) -> None`

    ```python
    from dagster_community_components import on_hooks

    @dg.asset
    @on_hooks(
        on_success=["my_project.hooks:notify_slack_success"],
        on_failure=["my_project.hooks:create_jira_ticket",
                    "my_project.hooks:page_oncall"],
    )
    def critical_report(context):
        return build_report()
    ```

    Callback exceptions are LOGGED, not re-raised — hooks don't change
    the compute's outcome (matches Prefect's semantics).
    """
    _success = list(on_success or [])
    _failure = list(on_failure or [])

    def _decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            context = None
            if args and hasattr(args[0], "log"):
                context = args[0]
            elif "context" in kwargs and hasattr(kwargs["context"], "log"):
                context = kwargs["context"]
            if context is None:
                raise RuntimeError("@on_hooks requires a Dagster context.")

            try:
                result = fn(*args, **kwargs)
            except BaseException as exc:  # noqa: BLE001
                if _failure:
                    context.log.info(f"[hooks] running {len(_failure)} on_failure callback(s)")
                    _run_callbacks(_failure, context, exc, "failure")
                raise
            if _success:
                context.log.info(f"[hooks] running {len(_success)} on_success callback(s)")
                _run_callbacks(_success, context, result, "success")
            return result

        return _wrapped

    return _decorator


class HooksAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of `@on_hooks`. Defines a new asset with success/failure
    callbacks bound to the compute.
    """

    asset_name: str = Field(description="Dagster asset name.")
    upstream_asset_key: Optional[str] = Field(default=None)
    compute: Dict[str, Any] = Field(description="`{kind: python, python: 'mod:fn'}`.")

    on_success: Optional[List[str]] = Field(
        default=None,
        description="List of 'mod:fn' refs called with (context, result) after successful compute.",
    )
    on_failure: Optional[List[str]] = Field(
        default=None,
        description="List of 'mod:fn' refs called with (context, exception) on failure. Doesn't change the outcome.",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None, description="Default: ['python', 'hooks'].")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Hooks Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        success_cbs = list(self.on_success or [])
        failure_cbs = list(self.on_failure or [])

        kinds_set = set(self.kinds or []) | {"python", "hooks"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        ins = {}
        if upstream_asset_key:
            ins["upstream"] = dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Hooks-wrapped asset {asset_name}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            kinds=kinds_set,
            ins=ins,
        )
        def _asset(context: dg.AssetExecutionContext, **kwargs):
            kind = (compute.get("kind") or "python").lower()
            if kind != "python":
                raise ValueError(f"HooksAssetComponent supports compute.kind=python only; got {kind!r}")
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
                if n_positional == 0:
                    result = fn()
                elif n_positional == 1:
                    result = fn(context)
                else:
                    result = fn(context, kwargs.get("upstream"))
            except BaseException as exc:  # noqa: BLE001
                if failure_cbs:
                    context.log.info(f"[hooks] running {len(failure_cbs)} on_failure callback(s)")
                    _run_callbacks(failure_cbs, context, exc, "failure")
                raise
            if success_cbs:
                context.log.info(f"[hooks] running {len(success_cbs)} on_success callback(s)")
                _run_callbacks(success_cbs, context, result, "success")

            return dg.MaterializeResult(
                metadata={
                    "n_success_hooks": dg.MetadataValue.int(len(success_cbs)),
                    "n_failure_hooks": dg.MetadataValue.int(len(failure_cbs)),
                }
            )

        return dg.Definitions(assets=[_asset])
