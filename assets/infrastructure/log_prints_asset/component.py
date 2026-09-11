"""LogPrintsAssetComponent + `@log_prints` — capture print() into Dagster logs.

Stolen from Prefect's `@flow(log_prints=True)`. Redirects `print()` and
`sys.stdout.write()` calls inside the compute function to
`context.log.info`, so print-heavy scripts land in the Dagster event log
naturally without rewriting them.

## Why this belongs in Dagster

Every captured print line becomes a real Dagster log event — searchable
by run_id, visible in the UI's log panel, respected by the run monitor.
No monkey-patching Dagster; just a stdout redirect scoped to one
compute call.

## What it fixes

- **Porting old scripts** — legacy Python often uses `print()` for
  visibility. `@log_prints` makes them Dagster-native without a rewrite.
- **Third-party libraries that print** — pandas warnings, requests
  redirects, tqdm progress bars — captured instead of going to stderr.

## Two shapes

- **`LogPrintsAssetComponent`** (YAML)
- **`@log_prints` decorator** (Python)

## Behavior

- Wraps compute with `contextlib.redirect_stdout(sink)`.
- Each line written to stdout is emitted as `context.log.info(...)`.
- Empty lines are skipped.
- Original stdout is restored after compute (whether success or fail).

## Composes with

- **All other decorators** — `@log_prints` is orthogonal.

## What's not in v1

- **stderr capture** — v1 only redirects stdout. Add `log_stderr=True`
  in a future iteration.
- **Level per line** — v1 emits all captured lines at `info` level.
  A line-prefix like `WARN:` could route to `context.log.warning`.
"""

import contextlib
import functools
import importlib
import io
import sys
from typing import Any, Callable, Dict, List, Optional

import dagster as dg
from pydantic import Field


class _LogPrintSink(io.TextIOBase):
    """Buffer + flush-on-newline sink that routes lines to `context.log.info`."""

    def __init__(self, context: Any, prefix: str = ""):
        self._context = context
        self._prefix = prefix
        self._buffer = ""

    def write(self, s: str) -> int:  # type: ignore[override]
        if not s:
            return 0
        self._buffer += s
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            self._flush_line(line)
        return len(s)

    def flush(self) -> None:  # type: ignore[override]
        if self._buffer:
            self._flush_line(self._buffer)
            self._buffer = ""

    def _flush_line(self, line: str) -> None:
        line = line.rstrip()
        if not line:
            return
        try:
            self._context.log.info(f"{self._prefix}{line}")
        except Exception:  # noqa: BLE001
            pass


def log_prints(
    *,
    prefix: str = "[print] ",
) -> Callable:
    """Redirect `print()` inside the decorated compute to `context.log.info`.

    ```python
    @dg.asset
    @log_prints()
    def porting_script(context):
        print("Starting job")
        print(f"Processed {n} rows")
        return build()
    ```

    All `print()` output (and anything else that goes to `sys.stdout`)
    gets captured line-by-line and emitted as info-level Dagster log
    events, so they show up in the run's log panel + are searchable
    across runs. Original stdout is restored on completion (success or
    failure).
    """
    def _decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            context = None
            if args and hasattr(args[0], "log"):
                context = args[0]
            elif "context" in kwargs and hasattr(kwargs["context"], "log"):
                context = kwargs["context"]
            if context is None:
                raise RuntimeError("@log_prints requires a Dagster context.")
            sink = _LogPrintSink(context, prefix)
            with contextlib.redirect_stdout(sink):
                try:
                    return fn(*args, **kwargs)
                finally:
                    sink.flush()
        return _wrapped
    return _decorator


class LogPrintsAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of `@log_prints`. Two authoring modes:

    1. **Define a new asset from scratch** (original shape): supply
       `asset_name` + `compute: {kind: python, python: 'mod:fn'}`. Builds
       a single asset with `print()` calls captured to context.log.info.

    2. **Wrap an existing DCC component** (composability): supply
       `wraps: {type: <component_class>, attributes: {...}}`. The inner
       component's assets are materialized as they would normally, with
       stdout of each compute redirected to context.log.info. Preserves
       inner asset partitions, deps, resources, kinds, tags, group,
       description. Direct YAML analog of `@log_prints @dg.asset` in Python.

    `wraps:` and `compute:` are mutually exclusive.
    """

    asset_name: Optional[str] = Field(
        default=None,
        description="Dagster asset name. Required when NOT using `wraps:` (inherited from inner in wraps mode).",
    )
    upstream_asset_key: Optional[str] = Field(default=None)
    compute: Optional[Dict[str, Any]] = Field(
        default=None,
        description="`{kind: python, python: 'mod:fn'}`. Any return type. Mutually exclusive with `wraps`.",
    )
    wraps: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Wrap another DCC component's assets with stdout-to-context.log capture "
            "instead of defining new compute. Shape: `{type: 'dagster_community_components.<Component>', "
            "attributes: {...}}`. Mutually exclusive with `compute`."
        ),
    )
    prefix: str = Field(
        default="[print] ",
        description="Optional prefix prepended to every captured line before it lands in the log.",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None, description="Default: ['python', 'logging'].")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Log Prints Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # Route: wraps-an-inner-component  vs  builds-own-asset
        if self.wraps is not None:
            if self.compute is not None:
                raise ValueError("LogPrintsAssetComponent: `wraps:` and `compute:` are mutually exclusive.")
            return self._build_wrapped(context)
        if self.compute is None:
            raise ValueError("LogPrintsAssetComponent: supply either `compute` (build new asset) or `wraps` (wrap existing component).")
        if not self.asset_name:
            raise ValueError("LogPrintsAssetComponent: `asset_name` required when using `compute:`.")

        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        prefix = self.prefix

        kinds_set = set(self.kinds or []) | {"python", "logging"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        ins = {}
        if upstream_asset_key:
            ins["upstream"] = dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Print-capturing asset {asset_name}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            kinds=kinds_set,
            ins=ins,
        )
        def _asset(context: dg.AssetExecutionContext, **kwargs):
            kind = (compute.get("kind") or "python").lower()
            if kind != "python":
                raise ValueError(f"LogPrintsAssetComponent supports compute.kind=python only; got {kind!r}")
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

            sink = _LogPrintSink(context, prefix)
            with contextlib.redirect_stdout(sink):
                try:
                    if n_positional == 0:
                        _ = fn()
                    elif n_positional == 1:
                        _ = fn(context)
                    else:
                        _ = fn(context, kwargs.get("upstream"))
                finally:
                    sink.flush()

            return dg.MaterializeResult(
                metadata={"log_prints_prefix": dg.MetadataValue.text(prefix)}
            )

        return dg.Definitions(assets=[_asset])

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

        prefix = self.prefix

        inner_kinds = set(getattr(spec, "kinds", None) or []) if spec else set()
        merged_kinds = inner_kinds | set(self.kinds or []) | {"logging"}
        inner_tags = dict(getattr(spec, "tags", None) or {}) if spec else {}
        merged_tags = {**inner_tags, **(self.tags or {})}
        merged_owners = list((spec.owners if spec else []) or []) + (self.owners or [])
        inner_description = (spec.description if spec else None) or f"Log-prints-wrapped {key.to_user_string()}"
        merged_description = f"{inner_description}  [log_prints: prefix={prefix!r}]"
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
        def _log_prints_wrapped(context: dg.AssetExecutionContext, **kwargs):
            sink = _LogPrintSink(context, prefix)
            with contextlib.redirect_stdout(sink):
                try:
                    result = inner_compute(context, **kwargs)
                finally:
                    sink.flush()

            passthrough_meta = {
                "log_prints_prefix": dg.MetadataValue.text(prefix),
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

        return _log_prints_wrapped


def _resolve_inner_component(wraps: Dict[str, Any]):
    """Resolve `{type: '...', attributes: {...}}` → instantiated component."""
    type_str = wraps.get("type")
    attrs = wraps.get("attributes") or {}
    if not type_str or not isinstance(type_str, str):
        raise ValueError("LogPrintsAssetComponent.wraps requires `type: <fully-qualified-class-name>`.")
    if ":" in type_str:
        mod_path, cls_name = type_str.rsplit(":", 1)
    else:
        mod_path, cls_name = type_str.rsplit(".", 1)
    try:
        mod = importlib.import_module(mod_path)
    except ImportError as e:
        raise ValueError(f"LogPrintsAssetComponent.wraps: cannot import {mod_path!r}: {e}") from e
    cls = getattr(mod, cls_name, None)
    if cls is None:
        raise ValueError(f"LogPrintsAssetComponent.wraps: {cls_name!r} not found in {mod_path!r}.")
    try:
        return cls(**attrs)
    except Exception as e:  # noqa: BLE001
        raise ValueError(
            f"LogPrintsAssetComponent.wraps: constructing {type_str} failed: {type(e).__name__}: {e}"
        ) from e
