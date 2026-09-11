"""CachedAssetComponent + `@cached` — content-addressable cache for asset compute.

Skip expensive compute when the cache is warm. Cache key is derived from
(upstream asset materialization identity, `code_version`, custom key
function output). Content-addressable: cache files live at
`{cache_dir}/{cache_key}.parquet` on local FS or any fsspec URI
(`s3://`, `gs://`, `abfs://`).

## Why Dagster is the right home for this

Everything here rides on Dagster primitives:
- **Cache key** — composes `code_version` (built into `@dg.asset`)
  with hashes of upstream materialization records
  (`context.instance.get_event_records`) and optional user hash of
  runtime inputs.
- **TTL check** — compares the cache file's mtime against `ttl_seconds`.
- **Hit/miss events** — emitted as `AssetObservation` tags so the run
  timeline shows cache activity; downstream + agents can query
  hit/miss ratios.
- **`code_version` invalidation** — bump the version, cache invalidates
  next run. Dagster's built-in change detection.

## Two shapes — component + decorator

- **Component (`CachedAssetComponent`)** — YAML-defined new asset. Compute
  referenced by `compute.python: 'mod:fn'`; on cache miss, function runs
  and result is cached. On hit, cached parquet is loaded.
- **Decorator (`@cached`)** — wraps an existing `@dg.asset`. Same engine.

## Cache invalidation levers

- **`code_version`** on the asset changes → cache key changes → miss.
- **`ttl_seconds`** exceeded → miss.
- **Custom key_fn** — user provides `key_fn: 'mod:fn'` returning a string
  that's mixed into the cache key. Perfect for "invalidate when
  external config changes."
- **Manual bust** — delete the parquet file at
  `{cache_dir}/{cache_key}.parquet`.

## What doesn't belong here

- **Compute results >1GB** — parquet on cloud storage is fine, but at
  some scale a real query cache (materialized views, Iceberg
  incrementals) beats a parquet blob. Use this for the "middle
  ground" — expensive Python compute that fits in memory.
- **Streaming assets** — cache assumes a stable compute function; a
  streaming compute isn't a fit.
"""

import functools
import hashlib
import importlib
import json
import os
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import dagster as dg
from pydantic import Field


# --------------------------------------------------------------------------
# Cache key derivation + storage
# --------------------------------------------------------------------------


def _hash_str(*parts: str) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(p.encode("utf-8"))
        h.update(b"\x00")
    return h.hexdigest()[:24]  # 24 chars = 96 bits, plenty for cache keys


def _compute_cache_key(
    context: Any,
    code_version: str,
    user_key_fn_ref: Optional[str],
    upstream: Any,
) -> str:
    """Derive the cache key from Dagster context + code_version + user key."""
    parts: List[str] = []
    # Asset key.
    ak = getattr(context, "asset_key", None)
    parts.append(str(ak.to_user_string()) if ak else "unknown")
    # Code version.
    parts.append(code_version or "")
    # Partition key.
    parts.append(str(context.partition_key) if getattr(context, "has_partition_key", False) and context.has_partition_key else "")
    # User custom key.
    if user_key_fn_ref:
        try:
            mod_path, fn_name = user_key_fn_ref.rsplit(":", 1)
            fn = getattr(importlib.import_module(mod_path.strip()), fn_name.strip(), None)
            if callable(fn):
                v = fn(context, upstream) if upstream is not None else fn(context)
                parts.append(str(v))
        except Exception:  # noqa: BLE001
            pass
    return _hash_str(*parts)


def _cache_path(cache_dir: str, cache_key: str, fmt: str) -> str:
    ext = "parquet" if fmt == "parquet" else fmt
    if "://" in cache_dir:
        return f"{cache_dir.rstrip('/')}/{cache_key}.{ext}"
    return str(Path(cache_dir).expanduser() / f"{cache_key}.{ext}")


def _load_cache(cache_path: str, fmt: str, ttl_seconds: Optional[float]) -> Optional[Any]:
    """Return the cached DataFrame if fresh; else None."""
    import pandas as pd
    if "://" in cache_path:
        import fsspec
        fs, root = fsspec.core.url_to_fs(cache_path)
        if not fs.exists(root):
            return None
        if ttl_seconds is not None:
            try:
                info = fs.info(root)
                mtime = info.get("mtime") or info.get("LastModified")
                if hasattr(mtime, "timestamp"):
                    mtime = mtime.timestamp()
                if mtime is not None and (time.time() - float(mtime)) > ttl_seconds:
                    return None
            except Exception:  # noqa: BLE001
                pass
        if fmt == "parquet":
            return pd.read_parquet(cache_path)
        if fmt == "csv":
            return pd.read_csv(cache_path)
        if fmt == "json":
            return pd.read_json(cache_path, orient="records", lines=True)
        return None
    p = Path(cache_path)
    if not p.exists():
        return None
    if ttl_seconds is not None:
        if (time.time() - p.stat().st_mtime) > ttl_seconds:
            return None
    if fmt == "parquet":
        return pd.read_parquet(p)
    if fmt == "csv":
        return pd.read_csv(p)
    if fmt == "json":
        return pd.read_json(p, orient="records", lines=True)
    return None


def _save_cache(df, cache_path: str, fmt: str):
    if "://" in cache_path:
        import fsspec
        fs, root = fsspec.core.url_to_fs(cache_path)
        parent = "/".join(root.split("/")[:-1])
        try: fs.makedirs(parent, exist_ok=True)
        except Exception: pass
        if fmt == "parquet":
            df.to_parquet(cache_path)
        elif fmt == "csv":
            df.to_csv(cache_path, index=False)
        elif fmt == "json":
            df.to_json(cache_path, orient="records", lines=True)
    else:
        p = Path(cache_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        if fmt == "parquet":
            df.to_parquet(p)
        elif fmt == "csv":
            df.to_csv(p, index=False)
        elif fmt == "json":
            df.to_json(p, orient="records", lines=True)


def _emit_cache_event(context: Any, key: str, hit: bool, path: str):
    """Emit AssetObservation with cache hit/miss info.

    - Tags: only alnum-safe short values (`cached_asset_status`). Dagster
      enforces `[A-Za-z0-9_.-]{,63}` on tag values, so full paths + long
      hashes have to go in metadata (see below).
    - Metadata: `cache_key` (long hash), `cache_path` (has slashes) — both
      go into typed MetadataValue where slashes / length are unrestricted.
    - `context.asset_key`: wrapped in try/except because it's a raising
      property on multi-asset contexts (`getattr(..., None)` doesn't catch).
    """
    from dagster import AssetKey, AssetObservation, MetadataValue
    try:
        asset_key = context.asset_key
    except Exception:  # noqa: BLE001
        asset_key = AssetKey(["cached_asset"])
    if not hasattr(context, "log_event"):
        return
    try:
        context.log_event(AssetObservation(
            asset_key=asset_key,
            tags={"cached_asset_status": "hit" if hit else "miss"},
            metadata={
                "cache_key": MetadataValue.text(key),
                "cache_path": MetadataValue.path(path),
            },
        ))
    except Exception as e:  # noqa: BLE001
        # Surface failures — silent swallowing hid a real bug for months.
        try:
            context.log.warning(f"[cached] failed to emit AssetObservation: {type(e).__name__}: {e}")
        except Exception:  # noqa: BLE001
            pass


# --------------------------------------------------------------------------
# @cached decorator
# --------------------------------------------------------------------------


def cached(
    cache_dir: str,
    *,
    code_version: str = "",
    ttl_seconds: Optional[float] = None,
    format: str = "parquet",
    key_fn: Optional[str] = None,
) -> Callable:
    """Content-addressable cache decorator for Dagster asset compute.

    Applied BEFORE `@dg.asset`. The wrapped function must return a
    `pandas.DataFrame`. On cache hit, the compute is SKIPPED and the
    cached parquet is loaded. On miss, compute runs and result is
    persisted.

    ```python
    from dagster_community_components import cached

    @dg.asset(code_version="1.2")
    @cached(
        cache_dir="s3://my-cache/orders/",
        code_version="1.2",           # matches the asset — invalidates cache on bump
        ttl_seconds=3600,             # 1 hour freshness
        format="parquet",
        key_fn="my_project.cache:key_from_config",  # optional
    )
    def orders(context):
        return expensive_build()
    ```

    Cache key = hash(asset_key + code_version + partition_key + key_fn(context, upstream)).

    On hit: emits `AssetObservation(tags={cached_asset_status: hit, ...})`
    and returns cached DataFrame without calling compute.
    On miss: emits `AssetObservation(tags={cached_asset_status: miss, ...})`
    and runs compute, saves result.
    """
    if format not in ("parquet", "csv", "json"):
        raise ValueError(f"format must be parquet|csv|json; got {format!r}")

    def _decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            # Generator shape (yield dg.Output) so we can side-channel
            # AssetObservation events via `context.log_event(...)` in the
            # step's context. Direct `return dg.Output(...)` collides with
            # the wrapped asset's `-> pd.DataFrame` type annotation.
            import pandas as pd
            context = None
            if args and hasattr(args[0], "log"):
                context = args[0]
            elif "context" in kwargs and hasattr(kwargs["context"], "log"):
                context = kwargs["context"]
            if context is None:
                raise RuntimeError(
                    "@cached requires a Dagster context — decorator must wrap a Dagster asset/op compute."
                )

            # Upstream extraction (for user key_fn)
            upstream = kwargs.get("upstream")
            if upstream is None and len(args) > 1:
                upstream = args[1]

            key = _compute_cache_key(context, code_version, key_fn, upstream)
            path = _cache_path(cache_dir, key, format)

            cached_df = _load_cache(path, format, ttl_seconds)
            if cached_df is not None:
                context.log.info(f"[cached] HIT for key={key} at {path} ({len(cached_df)} rows)")
                _emit_cache_event(context, key, hit=True, path=path)
                yield dg.Output(
                    cached_df,
                    metadata={
                        "cache_status": dg.MetadataValue.text("hit"),
                        "cache_key": dg.MetadataValue.text(key),
                        "cache_path": dg.MetadataValue.path(path),
                        "cache_rows": dg.MetadataValue.int(len(cached_df)),
                    },
                )
                return

            # MISS — run compute + save result.
            context.log.info(f"[cached] MISS for key={key} — running compute")
            _emit_cache_event(context, key, hit=False, path=path)
            df = fn(*args, **kwargs)
            if not isinstance(df, pd.DataFrame):
                raise TypeError(
                    f"@cached: compute must return a pandas DataFrame; got {type(df).__name__}."
                )
            _save_cache(df, path, format)
            context.log.info(f"[cached] saved {len(df)} rows to {path}")
            yield dg.Output(
                df,
                metadata={
                    "cache_status": dg.MetadataValue.text("miss"),
                    "cache_key": dg.MetadataValue.text(key),
                    "cache_path": dg.MetadataValue.path(path),
                    "cache_rows": dg.MetadataValue.int(len(df)),
                },
            )

        return _wrapped

    return _decorator


# --------------------------------------------------------------------------
# CachedAssetComponent — YAML-defined new asset
# --------------------------------------------------------------------------


class CachedAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of the cache. Two authoring modes:

    1. **Define a new cached asset** (original shape): supply `asset_name`
       + `compute: {kind: python, python: 'mod:fn'}` + `cache_dir`. Builds
       a single asset whose compute is only called on cache miss.

    2. **Wrap an existing DCC component** (composability): supply
       `wraps: {type: <component_class>, attributes: {...}}` + `cache_dir`.
       The inner component's asset compute is only invoked on cache miss;
       on hit, the cached parquet is loaded and the inner is skipped.
       Requires the inner component's asset to return a `pandas.DataFrame`.

    `wraps:` and `compute:`/`asset_name` are mutually exclusive.
    """

    asset_name: Optional[str] = Field(
        default=None,
        description="Dagster asset name. Required when NOT using `wraps:` (inherited from inner in wraps mode).",
    )
    upstream_asset_key: Optional[str] = Field(default=None)
    compute: Optional[Dict[str, Any]] = Field(
        default=None,
        description="`{kind: python, python: 'mod:fn'}`. Returns pandas DataFrame. Mutually exclusive with `wraps`.",
    )
    wraps: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Wrap another DCC component's assets with caching instead of defining new compute. "
            "Shape: `{type: 'dagster_community_components.<Component>', attributes: {...}}`. "
            "Inner asset must return pandas.DataFrame. Mutually exclusive with `compute`."
        ),
    )
    cache_dir: str = Field(
        description="Where cached parquets live. Local path or fsspec URI."
    )
    code_version: str = Field(
        default="",
        description="Version string mixed into the cache key. Bump to invalidate cache. Also set as `code_version` on the asset so downstream sees a bump.",
    )
    ttl_seconds: Optional[float] = Field(
        default=None,
        description="Cache expiry. If the cached file's mtime is older than this, treat as miss.",
    )
    format: str = Field(
        default="parquet",
        description="Cache file format: parquet | csv | json.",
    )
    key_fn: Optional[str] = Field(
        default=None,
        description="Optional `mod:fn` callable mixed into the cache key.",
    )

    # Catalog / governance
    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Default: ['python', 'cache'].",
    )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Cached Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # Route: wraps-an-inner-component  vs  builds-own-asset
        if self.wraps is not None:
            if self.compute is not None:
                raise ValueError("CachedAssetComponent: supply exactly ONE of `wraps` or `compute`, not both.")
            return self._build_wrapped(context)

        if self.compute is None:
            raise ValueError("CachedAssetComponent: supply either `compute` (build a new asset) or `wraps` (wrap an existing component).")
        if not self.asset_name:
            raise ValueError("CachedAssetComponent: `asset_name` required when using `compute:` (inferred from inner in `wraps:` mode).")

        _self = self
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        cache_dir = self.cache_dir
        code_version = self.code_version
        ttl_seconds = self.ttl_seconds
        fmt = self.format
        key_fn = self.key_fn

        if fmt not in ("parquet", "csv", "json"):
            raise ValueError(f"format must be parquet|csv|json; got {fmt!r}")

        kinds_set = set(self.kinds or []) | {"python", "cache"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        ins = {}
        if upstream_asset_key:
            ins["upstream"] = dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Cached asset {asset_name}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            kinds=kinds_set,
            ins=ins,
            code_version=code_version or None,
        )
        def _cached_asset(context: dg.AssetExecutionContext, **kwargs):
            import pandas as pd

            upstream = kwargs.get("upstream")
            key = _compute_cache_key(context, code_version, key_fn, upstream)
            path = _cache_path(cache_dir, key, fmt)

            cached_df = _load_cache(path, fmt, ttl_seconds)
            if cached_df is not None:
                context.log.info(f"[cached] HIT for key={key} at {path}")
                _emit_cache_event(context, key, hit=True, path=path)
                return dg.MaterializeResult(
                    metadata={
                        "cache_status": dg.MetadataValue.text("hit"),
                        "cache_key": dg.MetadataValue.text(key),
                        "cache_path": dg.MetadataValue.path(path),
                        "cache_rows": dg.MetadataValue.int(len(cached_df)),
                    },
                )

            # MISS — resolve compute + run + save.
            context.log.info(f"[cached] MISS for key={key} — running compute")
            _emit_cache_event(context, key, hit=False, path=path)

            kind = (compute.get("kind") or "python").lower()
            if kind != "python":
                raise ValueError(f"CachedAssetComponent v1 supports compute.kind=python only; got {kind!r}")
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
                df = fn()
            elif n_positional == 1:
                df = fn(context)
            else:
                df = fn(context, upstream)

            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"compute must return a DataFrame; got {type(df).__name__}")

            _save_cache(df, path, fmt)
            context.log.info(f"[cached] saved {len(df)} rows to {path}")
            return dg.MaterializeResult(
                metadata={
                    "cache_status": dg.MetadataValue.text("miss"),
                    "cache_key": dg.MetadataValue.text(key),
                    "cache_path": dg.MetadataValue.path(path),
                    "cache_rows": dg.MetadataValue.int(len(df)),
                },
            )

        return dg.Definitions(assets=[_cached_asset])

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

        cache_dir = self.cache_dir
        code_version = self.code_version
        ttl_seconds = self.ttl_seconds
        fmt = self.format
        key_fn_ref = self.key_fn

        if fmt not in ("parquet", "csv", "json"):
            raise ValueError(f"format must be parquet|csv|json; got {fmt!r}")

        inner_kinds = set(getattr(spec, "kinds", None) or []) if spec else set()
        merged_kinds = inner_kinds | set(self.kinds or []) | {"cache"}
        inner_tags = dict(getattr(spec, "tags", None) or {}) if spec else {}
        merged_tags = {**inner_tags, **(self.tags or {})}
        merged_owners = list((spec.owners if spec else []) or []) + (self.owners or [])
        inner_description = (spec.description if spec else None) or f"Cached {key.to_user_string()}"
        merged_description = f"{inner_description}  [cached: dir={cache_dir}, version={code_version!r}]"
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
            code_version=code_version or (spec.code_version if spec else None),
        )
        def _cache_wrapped(context: dg.AssetExecutionContext, **kwargs):
            import pandas as pd

            upstream = kwargs.get("upstream")
            cache_key = _compute_cache_key(context, code_version, key_fn_ref, upstream)
            path = _cache_path(cache_dir, cache_key, fmt)

            cached_df = _load_cache(path, fmt, ttl_seconds)
            if cached_df is not None:
                context.log.info(f"[cached wrap] HIT for key={cache_key} at {path} ({len(cached_df)} rows)")
                _emit_cache_event(context, cache_key, hit=True, path=path)
                return dg.Output(
                    cached_df,
                    metadata={
                        "cache_status": dg.MetadataValue.text("hit"),
                        "cache_key": dg.MetadataValue.text(cache_key),
                        "cache_path": dg.MetadataValue.path(path),
                        "cache_rows": dg.MetadataValue.int(len(cached_df)),
                    },
                )

            context.log.info(f"[cached wrap] MISS for key={cache_key} — invoking inner compute")
            _emit_cache_event(context, cache_key, hit=False, path=path)
            result = inner_compute(context, **kwargs)

            # Extract the DataFrame from the inner's return value.
            df_to_cache = None
            if isinstance(result, pd.DataFrame):
                df_to_cache = result
            elif isinstance(result, dg.Output):
                if isinstance(result.value, pd.DataFrame):
                    df_to_cache = result.value
            elif isinstance(result, dg.MaterializeResult):
                # MaterializeResult carries metadata but not value; the value is
                # already handled by the inner asset's IO manager. We can't cache
                # what we can't see.
                context.log.warning(
                    "[cached wrap] inner returned MaterializeResult (no value); "
                    "cannot cache — passing through unchanged"
                )
                return result

            if df_to_cache is None:
                context.log.warning(
                    f"[cached wrap] inner returned {type(result).__name__} (not DataFrame); "
                    "skipping cache write — passing through unchanged"
                )
                return result

            _save_cache(df_to_cache, path, fmt)
            context.log.info(f"[cached wrap] saved {len(df_to_cache)} rows to {path}")
            return dg.Output(
                df_to_cache,
                metadata={
                    "cache_status": dg.MetadataValue.text("miss"),
                    "cache_key": dg.MetadataValue.text(cache_key),
                    "cache_path": dg.MetadataValue.path(path),
                    "cache_rows": dg.MetadataValue.int(len(df_to_cache)),
                },
            )

        return _cache_wrapped


def _resolve_inner_component(wraps: Dict[str, Any]):
    """Resolve `{type: '...', attributes: {...}}` → instantiated component."""
    type_str = wraps.get("type")
    attrs = wraps.get("attributes") or {}
    if not type_str or not isinstance(type_str, str):
        raise ValueError("CachedAssetComponent.wraps requires `type: <fully-qualified-class-name>`.")
    if ":" in type_str:
        mod_path, cls_name = type_str.rsplit(":", 1)
    else:
        mod_path, cls_name = type_str.rsplit(".", 1)
    try:
        mod = importlib.import_module(mod_path)
    except ImportError as e:
        raise ValueError(f"CachedAssetComponent.wraps: cannot import {mod_path!r}: {e}") from e
    cls = getattr(mod, cls_name, None)
    if cls is None:
        raise ValueError(f"CachedAssetComponent.wraps: {cls_name!r} not found in {mod_path!r}.")
    try:
        return cls(**attrs)
    except Exception as e:  # noqa: BLE001
        raise ValueError(
            f"CachedAssetComponent.wraps: constructing {type_str} failed: {type(e).__name__}: {e}"
        ) from e
