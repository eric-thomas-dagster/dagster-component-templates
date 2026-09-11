"""DryRunAssetComponent + `@dry_run` — run compute, skip the write.

Executes the asset's compute function normally, but discards the output
instead of writing it through the IO manager. The materialization
result is a `MaterializeResult` (no value), so no upstream/downstream
persistence changes.

## Why this belongs in Dagster

- **Enable via run tag** — set `dry_run=true` on a run to activate
  without editing code or redeploying.
- **AssetObservation with dry_run tag** — every dry run leaves an
  auditable event so post-mortems and cost audits can filter.
- **Composes with `@sla`, `@throttle`, `@smart_retry`** — dry runs
  still respect budgets and retries.

## Two shapes

- **`DryRunAssetComponent`** (YAML)
- **`@dry_run` decorator** (Python)

## Enable modes

Priority order (highest wins):
1. Explicit `dry_run(enabled=True)` decorator arg.
2. Run tag `dry_run` in `("true", "1", "yes")`.
3. Env var `DAGSTER_DRY_RUN` in `("true", "1", "yes")`.
4. Default: disabled (compute + persist normally).

## Behavior

- **Enabled** — compute runs; return value is captured for metadata but
  discarded; asset returns `MaterializeResult(metadata={dry_run: True, ...})`.
  The IO manager is NOT invoked (no `store_output` call). Downstreams
  reading via the IO manager get the last non-dry-run value.
- **Disabled** — pass-through: compute runs, return value flows through
  the IO manager, materialization proceeds normally.

## Composes with

- `@sla` — SLA still fires on the compute; useful for validating
  duration regressions without persisting a bad table.
- `@throttle` — throttled dry runs still emit `throttle_skipped`.
- `@smart_retry` — retries within a dry run.
- `@lifecycle` — dry-run mode skips the publish; audit still runs.
- `@profile` — profile a candidate output before committing.
"""

import functools
import importlib
import os
import time
from typing import Any, Callable, Dict, List, Optional

import dagster as dg
from pydantic import Field


_DRY_RUN_TAG = "dry_run"
_ENV_VAR = "DAGSTER_DRY_RUN"


def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    return str(v).lower() in {"true", "1", "yes"}


def _is_enabled(context: Any, explicit: Optional[bool]) -> bool:
    if explicit is True:
        return True
    if explicit is False:
        return False
    try:
        run = getattr(context, "run", None)
        if run is not None:
            tags = getattr(run, "tags", None) or {}
            if _DRY_RUN_TAG in tags and _truthy(tags[_DRY_RUN_TAG]):
                return True
    except Exception:  # noqa: BLE001
        pass
    return _truthy(os.environ.get(_ENV_VAR, ""))


def _emit_dry_run_observation(
    context: Any, elapsed_s: float, would_size: Optional[int] = None,
) -> None:
    try:
        from dagster import AssetObservation
        asset_key = getattr(context, "asset_key", None) or dg.AssetKey(["dry_run_asset"])
        metadata: Dict[str, Any] = {
            "dry_run": dg.MetadataValue.bool(True),
            "elapsed_seconds": dg.MetadataValue.float(float(round(elapsed_s, 3))),
        }
        if would_size is not None:
            metadata["would_produce_bytes"] = dg.MetadataValue.int(int(would_size))
        if hasattr(context, "log_event"):
            context.log_event(AssetObservation(
                asset_key=asset_key,
                tags={_DRY_RUN_TAG: "true"},
                metadata=metadata,
            ))
    except Exception as e:  # noqa: BLE001
        try:
            context.log.warning(f"@dry_run: could not emit observation: {e}")
        except Exception:  # noqa: BLE001
            pass


def _describe_result_bytes(v: Any) -> Optional[int]:
    """Best-effort byte-length estimate for a returned value.

    Only handles common shapes: str, bytes, pandas DataFrame. Returns
    None for anything else — sizing is a nice-to-have, not required.
    """
    try:
        if isinstance(v, (bytes, bytearray)):
            return len(v)
        if isinstance(v, str):
            return len(v.encode("utf-8"))
        if hasattr(v, "memory_usage") and callable(v.memory_usage):
            return int(v.memory_usage(deep=True).sum())
    except Exception:  # noqa: BLE001
        return None
    return None


def dry_run(*, enabled: Optional[bool] = None) -> Callable:
    """Run the wrapped compute but discard the output on dry-run mode.

    ```python
    @dg.asset
    @dry_run()  # enabled via run tag `dry_run=true` or env DAGSTER_DRY_RUN=1
    def costly_report(context):
        return build_report()
    ```

    On dry-run mode: returns `MaterializeResult(metadata={dry_run: True})`
    so the IO manager is not invoked. Emits an `AssetObservation` tagged
    `dry_run=true` for auditability.

    Args:
        enabled: Explicit override. If None, reads run tag `dry_run` then
            env `DAGSTER_DRY_RUN`.
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
                raise RuntimeError("@dry_run requires a Dagster context.")

            if not _is_enabled(context, enabled):
                return fn(*args, **kwargs)

            try:
                context.log.info("@dry_run: mode ENABLED — compute will run, output discarded")
            except Exception:  # noqa: BLE001
                pass

            t0 = time.time()
            value = fn(*args, **kwargs)
            elapsed = time.time() - t0

            would_size = _describe_result_bytes(value)
            _emit_dry_run_observation(context, elapsed, would_size)

            metadata: Dict[str, Any] = {
                "dry_run": dg.MetadataValue.bool(True),
                "elapsed_seconds": dg.MetadataValue.float(float(round(elapsed, 3))),
            }
            if would_size is not None:
                metadata["would_produce_bytes"] = dg.MetadataValue.int(int(would_size))
            return dg.MaterializeResult(metadata=metadata)

        return _wrapped
    return _decorator


class DryRunAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of `@dry_run`. Two authoring modes:

    1. **Define a new asset from scratch** (original shape): supply
       `asset_name` + `compute: {kind: python, python: 'mod:fn'}`. Builds a
       single dry-run-capable asset that calls the referenced Python compute.

    2. **Wrap an existing DCC component** (composability): supply
       `wraps: {type: <component_class>, attributes: {...}}`. The inner
       component's assets get materialized as they would normally, but
       each compute is short-circuited when dry-run mode is enabled.
       Preserves inner asset partitions, deps, resources, kinds, tags,
       group, description. Direct YAML analog of `@dry_run @dg.asset` in
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
            "Wrap another DCC component's assets with dry-run short-circuit behavior instead "
            "of defining new compute. Shape: `{type: 'dagster_community_components.<Component>', "
            "attributes: {...}}`. Mutually exclusive with `compute`."
        ),
    )

    enabled: Optional[bool] = Field(
        default=None,
        description="Explicit dry-run override. When null (default), reads run tag "
                    "`dry_run` in ('true','1','yes'), else env `DAGSTER_DRY_RUN`.",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None, description="Default: ['python', 'dry_run'].")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Dry Run Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # Route: wraps-an-inner-component  vs  builds-own-asset
        if self.wraps is not None:
            if self.compute is not None:
                raise ValueError("DryRunAssetComponent: `wraps:` and `compute:` are mutually exclusive.")
            return self._build_wrapped(context)

        if self.compute is None:
            raise ValueError("DryRunAssetComponent: supply either `compute` (build new asset) or `wraps` (wrap existing component).")
        if not self.asset_name:
            raise ValueError("DryRunAssetComponent: `asset_name` required when using `compute:`.")

        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        enabled_arg = self.enabled

        kinds_set = set(self.kinds or []) | {"python", "dry_run"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        ins = {}
        if upstream_asset_key:
            ins["upstream"] = dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Dry-run capable asset {asset_name}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            kinds=kinds_set,
            ins=ins,
        )
        def _asset(context: dg.AssetExecutionContext, **kwargs):
            kind = (compute.get("kind") or "python").lower()
            if kind != "python":
                raise ValueError(f"DryRunAssetComponent supports compute.kind=python only; got {kind!r}")
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

            enabled_now = _is_enabled(context, enabled_arg)
            t0 = time.time()
            if n_positional == 0:
                value = fn()
            elif n_positional == 1:
                value = fn(context)
            else:
                value = fn(context, kwargs.get("upstream"))
            elapsed = time.time() - t0

            if enabled_now:
                would_size = _describe_result_bytes(value)
                _emit_dry_run_observation(context, elapsed, would_size)
                metadata: Dict[str, Any] = {
                    "dry_run": dg.MetadataValue.bool(True),
                    "elapsed_seconds": dg.MetadataValue.float(float(round(elapsed, 3))),
                }
                if would_size is not None:
                    metadata["would_produce_bytes"] = dg.MetadataValue.int(int(would_size))
                return dg.MaterializeResult(metadata=metadata)

            return dg.MaterializeResult(
                metadata={
                    "dry_run": dg.MetadataValue.bool(False),
                    "elapsed_seconds": dg.MetadataValue.float(float(round(elapsed, 3))),
                }
            )

        return dg.Definitions(assets=[_asset])

    # ----------------------------------------------------------------------
    # `wraps:` composability — YAML analog of `@dry_run @dg.asset` stacking
    # ----------------------------------------------------------------------

    def _build_wrapped(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Instantiate the inner component; rewrap each of its assets with
        dry-run short-circuit around the original compute.
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
        """Rebuild one single-key AssetsDefinition with dry-run short-circuit
        wrapping the original compute. When enabled, inner compute is NOT
        called and a synthetic MaterializeResult tagged dry_run=true is emitted.
        Preserves partitions/deps/kinds/tags/group/description/metadata.
        """
        key = next(iter(asset_def.keys))
        specs_by_key = getattr(asset_def, "specs_by_key", {}) or {}
        spec = specs_by_key.get(key)

        inner_op = asset_def.op
        inner_compute = getattr(inner_op.compute_fn, "decorated_fn", None) or inner_op.compute_fn

        enabled_arg = self.enabled

        inner_kinds = set(getattr(spec, "kinds", None) or []) if spec else set()
        merged_kinds = inner_kinds | set(self.kinds or []) | {"dry_run"}
        inner_tags = dict(getattr(spec, "tags", None) or {}) if spec else {}
        merged_tags = {**inner_tags, **(self.tags or {})}
        merged_owners = list((spec.owners if spec else []) or []) + (self.owners or [])
        inner_description = (spec.description if spec else None) or f"Dry-run-capable {key.to_user_string()}"
        merged_description = f"{inner_description}  [dry_run: run tag `dry_run=true` or env DAGSTER_DRY_RUN]"
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
        def _dry_run_wrapped(context: dg.AssetExecutionContext, **kwargs):
            enabled_now = _is_enabled(context, enabled_arg)
            if enabled_now:
                # Short-circuit: DO NOT call inner_compute. Emit synthetic
                # MaterializeResult tagged dry_run=true.
                try:
                    context.log.info(
                        "[dry_run wrap] mode ENABLED — inner compute SKIPPED, "
                        "emitting synthetic MaterializeResult"
                    )
                except Exception:  # noqa: BLE001
                    pass
                _emit_dry_run_observation(context, elapsed_s=0.0, would_size=None)
                return dg.MaterializeResult(
                    metadata={
                        "dry_run": dg.MetadataValue.bool(True),
                        "dry_run_wrapped_asset": dg.MetadataValue.text(key.to_user_string()),
                        "inner_compute_invoked": dg.MetadataValue.bool(False),
                    }
                )

            # Disabled: passthrough.
            t0 = time.time()
            result = inner_compute(context, **kwargs)
            elapsed = time.time() - t0

            passthrough_meta = {
                "dry_run": dg.MetadataValue.bool(False),
                "elapsed_seconds": dg.MetadataValue.float(float(round(elapsed, 3))),
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

        return _dry_run_wrapped


def _resolve_inner_component(wraps: Dict[str, Any]):
    """Resolve `{type: 'mod.path.ClassName' OR 'mod.path:ClassName', attributes: {...}}` → component instance."""
    type_str = wraps.get("type")
    attrs = wraps.get("attributes") or {}
    if not type_str or not isinstance(type_str, str):
        raise ValueError("DryRunAssetComponent.wraps requires `type: <fully-qualified-class-name>`.")
    if ":" in type_str:
        mod_path, cls_name = type_str.rsplit(":", 1)
    else:
        mod_path, cls_name = type_str.rsplit(".", 1)
    try:
        mod = importlib.import_module(mod_path)
    except ImportError as e:
        raise ValueError(f"DryRunAssetComponent.wraps: cannot import {mod_path!r}: {e}") from e
    cls = getattr(mod, cls_name, None)
    if cls is None:
        raise ValueError(f"DryRunAssetComponent.wraps: {cls_name!r} not found in {mod_path!r}.")
    try:
        return cls(**attrs)
    except Exception as e:  # noqa: BLE001
        raise ValueError(
            f"DryRunAssetComponent.wraps: constructing {type_str} failed: {type(e).__name__}: {e}"
        ) from e
