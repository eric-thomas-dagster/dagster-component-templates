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

Start callback:   `fn(context) -> None`   (fires BEFORE compute)
Success callback: `fn(context, result) -> None`
Failure callback: `fn(context, exception) -> None`
End callback:     `fn(context, outcome, result_or_exc) -> None`
                  (fires FINALLY-STYLE after success/failure;
                   `outcome in ("success", "failure")`)

The result of the wrapped compute is passed to on_success; the raised
exception (or `dg.Failure`) to on_failure. on_end fires last regardless
of outcome. Callbacks are called sequentially; any exception raised by a
callback is LOGGED but doesn't alter the outcome (compute success stays
success; failure stays failure). This matches Prefect's semantics.

## Composes with

- **`@smart_retry`** — hooks fire once per materialization outcome,
  not per retry attempt.
- **`@sla`** — pair with `on_failure=notify_slack` for SLA-breach alerts.
- **`@data_contract`** — pair with `on_failure=create_jira_ticket` for
  contract violations.

## What's not in v1

- **Async hooks** — v1 runs callbacks synchronously.
- **Cross-asset hook sharing** — declare a hook once, reference from N assets.
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


def _run_start_callbacks(callbacks: List[str], context: Any) -> None:
    """Run on_start callbacks — signature `(context) -> None`. Each is wrapped
    in try/except so a bad hook doesn't take down the compute."""
    for ref in callbacks:
        if not ref or ":" not in ref:
            context.log.warning(
                f"[hooks] start: malformed callback ref {ref!r} (expected 'mod:fn')"
            )
            continue
        try:
            mod_path, fn_name = ref.rsplit(":", 1)
            fn = getattr(importlib.import_module(mod_path.strip()), fn_name.strip(), None)
            if not callable(fn):
                context.log.warning(f"[hooks] start: {ref!r} not callable")
                continue
            fn(context)
        except Exception as exc:  # noqa: BLE001
            context.log.error(
                f"[hooks] start callback {ref!r} raised {type(exc).__name__}: {exc}"
            )


def _run_end_callbacks(
    callbacks: List[str], context: Any, outcome: str, result_or_exc: Any,
) -> None:
    """Run on_end callbacks — signature `(context, outcome, result_or_exc) -> None`.
    `outcome` is 'success' or 'failure'. Runs finally-style AFTER on_success /
    on_failure. Each callback wrapped in try/except."""
    for ref in callbacks:
        if not ref or ":" not in ref:
            context.log.warning(
                f"[hooks] end: malformed callback ref {ref!r} (expected 'mod:fn')"
            )
            continue
        try:
            mod_path, fn_name = ref.rsplit(":", 1)
            fn = getattr(importlib.import_module(mod_path.strip()), fn_name.strip(), None)
            if not callable(fn):
                context.log.warning(f"[hooks] end: {ref!r} not callable")
                continue
            fn(context, outcome, result_or_exc)
        except Exception as exc:  # noqa: BLE001
            context.log.error(
                f"[hooks] end callback {ref!r} raised {type(exc).__name__}: {exc}"
            )


def on_hooks(
    *,
    on_success: Optional[List[str]] = None,
    on_failure: Optional[List[str]] = None,
    on_start: Optional[List[str]] = None,
    on_end: Optional[List[str]] = None,
) -> Callable:
    """Attach lifecycle callbacks to a Dagster asset compute.

    Applied BEFORE `@dg.asset`. Callbacks are ordinary Python `mod:fn`
    references. Signatures:
      - `on_start`:   `fn(context) -> None`                    (fires BEFORE compute)
      - `on_success`: `fn(context, result) -> None`            (fires on successful compute)
      - `on_failure`: `fn(context, exception) -> None`         (fires on failed compute)
      - `on_end`:     `fn(context, outcome, result_or_exc) -> None`
                      where `outcome in ("success", "failure")` — fires
                      FINALLY-STYLE after on_success/on_failure.

    ```python
    from dagster_community_components import on_hooks

    @dg.asset
    @on_hooks(
        on_start=["my_project.hooks:log_run_start"],
        on_success=["my_project.hooks:notify_slack_success"],
        on_failure=["my_project.hooks:create_jira_ticket",
                    "my_project.hooks:page_oncall"],
        on_end=["my_project.hooks:emit_lifecycle_metric"],
    )
    def critical_report(context):
        return build_report()
    ```

    Callback exceptions are LOGGED, not re-raised — hooks don't change
    the compute's outcome (matches Prefect's semantics).
    """
    _success = list(on_success or [])
    _failure = list(on_failure or [])
    _start = list(on_start or [])
    _end = list(on_end or [])

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

            if _start:
                context.log.info(f"[hooks] running {len(_start)} on_start callback(s)")
                _run_start_callbacks(_start, context)

            try:
                result = fn(*args, **kwargs)
            except BaseException as exc:  # noqa: BLE001
                if _failure:
                    context.log.info(f"[hooks] running {len(_failure)} on_failure callback(s)")
                    _run_callbacks(_failure, context, exc, "failure")
                if _end:
                    context.log.info(f"[hooks] running {len(_end)} on_end callback(s) [failure]")
                    _run_end_callbacks(_end, context, "failure", exc)
                raise
            if _success:
                context.log.info(f"[hooks] running {len(_success)} on_success callback(s)")
                _run_callbacks(_success, context, result, "success")
            if _end:
                context.log.info(f"[hooks] running {len(_end)} on_end callback(s) [success]")
                _run_end_callbacks(_end, context, "success", result)
            return result

        return _wrapped

    return _decorator


class HooksAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of `@on_hooks`. Two authoring modes:

    1. **Define a new asset from scratch** (original shape): supply
       `asset_name` + `compute: {kind: python, python: 'mod:fn'}`. Builds a
       single asset whose compute is wrapped with success/failure callbacks.

    2. **Wrap an existing DCC component** (composability): supply
       `wraps: {type: <component_class>, attributes: {...}}`. The inner
       component's assets get materialized as they would normally, but
       each compute is wrapped with on_success/on_failure callbacks.
       Preserves inner asset partitions, deps, resources, kinds, tags,
       group, description. Direct YAML analog of `@on_hooks @dg.asset` in
       Python.

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
            "Wrap another DCC component's assets with success/failure hooks instead of "
            "defining new compute. Shape: `{type: 'dagster_community_components.<Component>', "
            "attributes: {...}}`. Mutually exclusive with `compute`."
        ),
    )

    on_success: Optional[List[str]] = Field(
        default=None,
        description="List of 'mod:fn' refs called with (context, result) after successful compute.",
    )
    on_failure: Optional[List[str]] = Field(
        default=None,
        description="List of 'mod:fn' refs called with (context, exception) on failure. Doesn't change the outcome.",
    )
    on_start: Optional[List[str]] = Field(
        default=None,
        description=(
            "Callbacks fired BEFORE compute (in list order). Signature: `(context) -> None`. "
            "Each is a `mod:fn` reference."
        ),
    )
    on_end: Optional[List[str]] = Field(
        default=None,
        description=(
            "Callbacks fired AFTER compute (finally-style — regardless of success or failure, "
            "in list order). Signature: `(context, outcome: str, result_or_exc) -> None`. "
            "Runs after on_success/on_failure."
        ),
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
        # Route: wraps-an-inner-component  vs  builds-own-asset
        if self.wraps is not None:
            if self.compute is not None:
                raise ValueError("HooksAssetComponent: `wraps:` and `compute:` are mutually exclusive.")
            return self._build_wrapped(context)

        if self.compute is None:
            raise ValueError("HooksAssetComponent: supply either `compute` (build new asset) or `wraps` (wrap existing component).")
        if not self.asset_name:
            raise ValueError("HooksAssetComponent: `asset_name` required when using `compute:`.")

        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        success_cbs = list(self.on_success or [])
        failure_cbs = list(self.on_failure or [])
        start_cbs = list(self.on_start or [])
        end_cbs = list(self.on_end or [])

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

            if start_cbs:
                context.log.info(f"[hooks] running {len(start_cbs)} on_start callback(s)")
                _run_start_callbacks(start_cbs, context)

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
                if end_cbs:
                    context.log.info(f"[hooks] running {len(end_cbs)} on_end callback(s) [failure]")
                    _run_end_callbacks(end_cbs, context, "failure", exc)
                raise
            if success_cbs:
                context.log.info(f"[hooks] running {len(success_cbs)} on_success callback(s)")
                _run_callbacks(success_cbs, context, result, "success")
            if end_cbs:
                context.log.info(f"[hooks] running {len(end_cbs)} on_end callback(s) [success]")
                _run_end_callbacks(end_cbs, context, "success", result)

            return dg.MaterializeResult(
                metadata={
                    "n_success_hooks": dg.MetadataValue.int(len(success_cbs)),
                    "n_failure_hooks": dg.MetadataValue.int(len(failure_cbs)),
                    "n_start_hooks": dg.MetadataValue.int(len(start_cbs)),
                    "n_end_hooks": dg.MetadataValue.int(len(end_cbs)),
                }
            )

        return dg.Definitions(assets=[_asset])

    # ----------------------------------------------------------------------
    # `wraps:` composability — YAML analog of `@on_hooks @dg.asset` stacking
    # ----------------------------------------------------------------------

    def _build_wrapped(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Instantiate the inner component; rewrap each of its assets with
        on_success/on_failure callbacks around the original compute.
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
        """Rebuild one single-key AssetsDefinition with on_success/on_failure
        callbacks wrapping the original compute. Preserves
        partitions/deps/kinds/tags/group/description/metadata.
        """
        key = next(iter(asset_def.keys))
        specs_by_key = getattr(asset_def, "specs_by_key", {}) or {}
        spec = specs_by_key.get(key)

        inner_op = asset_def.op
        inner_compute = getattr(inner_op.compute_fn, "decorated_fn", None) or inner_op.compute_fn

        success_cbs = list(self.on_success or [])
        failure_cbs = list(self.on_failure or [])
        start_cbs = list(self.on_start or [])
        end_cbs = list(self.on_end or [])

        inner_kinds = set(getattr(spec, "kinds", None) or []) if spec else set()
        merged_kinds = inner_kinds | set(self.kinds or []) | {"hooks"}
        inner_tags = dict(getattr(spec, "tags", None) or {}) if spec else {}
        merged_tags = {**inner_tags, **(self.tags or {})}
        merged_owners = list((spec.owners if spec else []) or []) + (self.owners or [])
        inner_description = (spec.description if spec else None) or f"Hooks-wrapped {key.to_user_string()}"
        merged_description = (
            f"{inner_description}  "
            f"[hooks: on_start={len(start_cbs)}, on_success={len(success_cbs)}, "
            f"on_failure={len(failure_cbs)}, on_end={len(end_cbs)}]"
        )
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
        def _hooks_wrapped(context: dg.AssetExecutionContext, **kwargs):
            if start_cbs:
                context.log.info(
                    f"[hooks wrap] running {len(start_cbs)} on_start callback(s)"
                )
                _run_start_callbacks(start_cbs, context)

            try:
                result = inner_compute(context, **kwargs)
            except BaseException as exc:  # noqa: BLE001
                if failure_cbs:
                    context.log.info(
                        f"[hooks wrap] running {len(failure_cbs)} on_failure callback(s)"
                    )
                    _run_callbacks(failure_cbs, context, exc, "failure")
                if end_cbs:
                    context.log.info(
                        f"[hooks wrap] running {len(end_cbs)} on_end callback(s) [failure]"
                    )
                    _run_end_callbacks(end_cbs, context, "failure", exc)
                raise
            if success_cbs:
                context.log.info(
                    f"[hooks wrap] running {len(success_cbs)} on_success callback(s)"
                )
                _run_callbacks(success_cbs, context, result, "success")
            if end_cbs:
                context.log.info(
                    f"[hooks wrap] running {len(end_cbs)} on_end callback(s) [success]"
                )
                _run_end_callbacks(end_cbs, context, "success", result)

            # Merge hook counts into the inner's MaterializeResult (if any).
            passthrough_meta = {
                "n_success_hooks": dg.MetadataValue.int(len(success_cbs)),
                "n_failure_hooks": dg.MetadataValue.int(len(failure_cbs)),
                "n_start_hooks": dg.MetadataValue.int(len(start_cbs)),
                "n_end_hooks": dg.MetadataValue.int(len(end_cbs)),
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

        return _hooks_wrapped


def _resolve_inner_component(wraps: Dict[str, Any]):
    """Resolve `{type: 'mod.path.ClassName' OR 'mod.path:ClassName', attributes: {...}}` → component instance."""
    type_str = wraps.get("type")
    attrs = wraps.get("attributes") or {}
    if not type_str or not isinstance(type_str, str):
        raise ValueError("HooksAssetComponent.wraps requires `type: <fully-qualified-class-name>`.")
    if ":" in type_str:
        mod_path, cls_name = type_str.rsplit(":", 1)
    else:
        mod_path, cls_name = type_str.rsplit(".", 1)
    try:
        mod = importlib.import_module(mod_path)
    except ImportError as e:
        raise ValueError(f"HooksAssetComponent.wraps: cannot import {mod_path!r}: {e}") from e
    cls = getattr(mod, cls_name, None)
    if cls is None:
        raise ValueError(f"HooksAssetComponent.wraps: {cls_name!r} not found in {mod_path!r}.")
    try:
        return cls(**attrs)
    except Exception as e:  # noqa: BLE001
        raise ValueError(
            f"HooksAssetComponent.wraps: constructing {type_str} failed: {type(e).__name__}: {e}"
        ) from e
