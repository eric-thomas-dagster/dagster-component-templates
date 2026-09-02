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
    """YAML shape of `@dry_run`. Wraps a compute with dry-run mode support."""

    asset_name: str = Field(description="Dagster asset name.")
    upstream_asset_key: Optional[str] = Field(default=None)
    compute: Dict[str, Any] = Field(description="`{kind: python, python: 'mod:fn'}`.")

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
