"""SnapshotAssetComponent + `@snapshot` — point-in-time snapshots of asset outputs.

After the compute produces a value, serialize + write a snapshot to an
fsspec URI keyed by `run_id + code_version + timestamp`. Emit an
`AssetObservation` with the snapshot path + size so rollback tools can
find snapshots via event log queries.

## Why this belongs in Dagster

- **`code_version`-aware paths** — the snapshot filename embeds the
  asset's `code_version`, so rolling back "the last snapshot before we
  deployed v3" is `find snapshots WHERE version != current`.
- **AssetObservation with snapshot metadata** — every snapshot leaves
  a queryable event (`snapshot_path`, `snapshot_bytes`, `snapshot_format`).
  Rollback tools become `SELECT snapshot_path FROM observations WHERE ...`.
- **fsspec URIs** — write to any backend (local, s3://, gs://, abfs://)
  with the same code.
- **Complementary to `@cached`** — `@cached` skips compute; `@snapshot`
  always runs but saves a checkpoint. Use together for retro-cache access.

## Two shapes

- **`SnapshotAssetComponent`** (YAML)
- **`@snapshot` decorator** (Python)

## Format detection

Auto-detected from the returned value:

| Value type | Format | Extension |
|---|---|---|
| `pandas.DataFrame` | Parquet | `.parquet` |
| `dict` / `list` (JSON-safe) | JSON | `.json` |
| `bytes` / `bytearray` | raw | `.bin` |
| `str` | text | `.txt` |
| Everything else | pickle | `.pkl` |

Explicit `format` override supported (`parquet`, `json`, `pickle`, `text`, `bin`).

## Path shape

    <uri>/<asset_name>/<code_version>/<timestamp>__<run_id_short>.<ext>

Where `code_version` defaults to `unknown` if the asset didn't set one.
`timestamp` is UTC ISO-8601 with basic filename-safe chars.

## Retention

Optional `retention_days`: after a successful write, snapshots older
than N days in the asset's folder are removed. Set to `null` (default)
to keep all snapshots.

## Composes with

- `@cached` — read from the snapshot uri instead of recomputing.
- `@lifecycle` — snapshot AFTER publish, so the snapshot matches production.
- `@dry_run` — dry runs don't snapshot (skip when mode active).
- `@profile` — snapshot the profile alongside the data.
"""

import datetime as _dt
import functools
import importlib
import json
import os
import pickle
from typing import Any, Callable, Dict, List, Optional, Tuple

import dagster as dg
from pydantic import Field


_SNAPSHOT_TAG = "snapshot_asset"


def _detect_format(value: Any, explicit: Optional[str]) -> Tuple[str, str]:
    """Return (format, extension)."""
    if explicit:
        return explicit, _EXT_BY_FORMAT.get(explicit, ".bin")
    try:
        import pandas as pd
        if isinstance(value, pd.DataFrame):
            return "parquet", ".parquet"
    except ImportError:
        pass
    if isinstance(value, (dict, list)):
        return "json", ".json"
    if isinstance(value, (bytes, bytearray)):
        return "bin", ".bin"
    if isinstance(value, str):
        return "text", ".txt"
    return "pickle", ".pkl"


_EXT_BY_FORMAT = {
    "parquet": ".parquet",
    "json": ".json",
    "pickle": ".pkl",
    "text": ".txt",
    "bin": ".bin",
}


def _serialize(value: Any, fmt: str) -> bytes:
    if fmt == "parquet":
        try:
            import io
            buf = io.BytesIO()
            value.to_parquet(buf)
            return buf.getvalue()
        except Exception:
            fmt = "pickle"
    if fmt == "json":
        return json.dumps(value, default=str).encode("utf-8")
    if fmt == "pickle":
        return pickle.dumps(value)
    if fmt == "text":
        return str(value).encode("utf-8")
    if fmt == "bin":
        if isinstance(value, (bytes, bytearray)):
            return bytes(value)
        return str(value).encode("utf-8")
    raise ValueError(f"unknown snapshot format: {fmt!r}")


def _get_fs(uri: str):
    if uri.startswith(("s3://", "gs://", "abfs://", "az://", "hdfs://", "file://")):
        import fsspec
        proto = uri.split("://", 1)[0]
        return fsspec.filesystem(proto)
    return None


def _write(uri_dir: str, filename: str, data: bytes) -> str:
    fs = _get_fs(uri_dir)
    if fs is None:
        os.makedirs(uri_dir, exist_ok=True)
        full = os.path.join(uri_dir, filename)
        with open(full, "wb") as f:
            f.write(data)
        return full
    proto, path = uri_dir.split("://", 1)
    fs.makedirs(path, exist_ok=True)
    full = f"{proto}://{path.rstrip('/')}/{filename}"
    with fs.open(f"{path.rstrip('/')}/{filename}", "wb") as f:
        f.write(data)
    return full


def _prune(uri_dir: str, retention_days: Optional[int]) -> int:
    if not retention_days or retention_days <= 0:
        return 0
    cutoff = _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(days=retention_days)
    n = 0
    fs = _get_fs(uri_dir)
    try:
        if fs is None:
            if not os.path.isdir(uri_dir):
                return 0
            for name in os.listdir(uri_dir):
                fp = os.path.join(uri_dir, name)
                try:
                    mt = _dt.datetime.fromtimestamp(os.path.getmtime(fp), tz=_dt.timezone.utc)
                    if mt < cutoff:
                        os.remove(fp)
                        n += 1
                except OSError:
                    pass
        else:
            proto, path = uri_dir.split("://", 1)
            for entry in fs.ls(path.rstrip("/"), detail=True):
                mt_raw = entry.get("mtime") or entry.get("LastModified") or entry.get("modified")
                if isinstance(mt_raw, _dt.datetime):
                    mt = mt_raw if mt_raw.tzinfo else mt_raw.replace(tzinfo=_dt.timezone.utc)
                    if mt < cutoff:
                        try:
                            fs.rm(entry["name"])
                            n += 1
                        except Exception:  # noqa: BLE001
                            pass
    except Exception:  # noqa: BLE001
        pass
    return n


def _emit_snapshot_observation(
    context: Any, path: str, size_bytes: int, fmt: str, pruned: int,
) -> None:
    try:
        from dagster import AssetObservation
        asset_key = getattr(context, "asset_key", None) or dg.AssetKey(["snapshot_asset"])
        if hasattr(context, "log_event"):
            context.log_event(AssetObservation(
                asset_key=asset_key,
                tags={
                    _SNAPSHOT_TAG: "written",
                    "snapshot_format": fmt,
                },
                metadata={
                    "snapshot_path": dg.MetadataValue.path(path),
                    "snapshot_bytes": dg.MetadataValue.int(int(size_bytes)),
                    "snapshot_format": dg.MetadataValue.text(fmt),
                    "snapshot_pruned_count": dg.MetadataValue.int(int(pruned)),
                },
            ))
    except Exception:  # noqa: BLE001
        try:
            context.log.warning("@snapshot: could not emit observation")
        except Exception:  # noqa: BLE001
            pass


def _snapshot_filename(context: Any) -> str:
    ts = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = "unknown"
    try:
        run_id = str(getattr(context, "run_id", None) or "unknown")[:12]
    except Exception:  # noqa: BLE001
        pass
    return f"{ts}__{run_id}"


def _snapshot_folder(context: Any, uri: str) -> str:
    asset_key = getattr(context, "asset_key", None)
    asset_dir = "/".join(asset_key.path) if asset_key is not None else "snapshot_asset"
    code_version = "unknown"
    try:
        if hasattr(context, "assets_def"):
            defn = context.assets_def
            if defn is not None:
                cv = getattr(defn, "code_version", None) or getattr(defn, "code_versions_by_key", {}).get(asset_key)
                if cv:
                    code_version = str(cv)
    except Exception:  # noqa: BLE001
        pass
    root = uri.rstrip("/")
    return f"{root}/{asset_dir}/{code_version}"


def _dry_run_active(context: Any) -> bool:
    try:
        run = getattr(context, "run", None)
        tags = getattr(run, "tags", None) or {}
        return str(tags.get("dry_run", "")).lower() in {"true", "1", "yes"}
    except Exception:  # noqa: BLE001
        return False


def _do_snapshot(context: Any, value: Any, uri: str, fmt: Optional[str], retention_days: Optional[int]) -> None:
    if _dry_run_active(context):
        try:
            context.log.info("@snapshot: dry_run active — skipping write")
        except Exception:  # noqa: BLE001
            pass
        return
    resolved_fmt, ext = _detect_format(value, fmt)
    data = _serialize(value, resolved_fmt)
    folder = _snapshot_folder(context, uri)
    filename = _snapshot_filename(context) + ext
    full_path = _write(folder, filename, data)
    pruned = _prune(folder, retention_days)
    _emit_snapshot_observation(context, full_path, len(data), resolved_fmt, pruned)
    try:
        context.log.info(f"@snapshot: wrote {full_path} ({len(data)} bytes, format={resolved_fmt})")
    except Exception:  # noqa: BLE001
        pass


def snapshot(
    *,
    uri: str,
    format: Optional[str] = None,
    retention_days: Optional[int] = None,
) -> Callable:
    """Write a point-in-time snapshot of the wrapped asset's return value.

    ```python
    @dg.asset(code_version="2.1.0")
    @snapshot(uri="s3://backups/report_snapshots", retention_days=30)
    def daily_report(context):
        return build_report()
    ```

    Path shape:  `<uri>/<asset_name>/<code_version>/<timestamp>__<run_id>.<ext>`

    Args:
        uri: fsspec URI directory (`s3://...`, `gs://...`, `/local/path`, etc.)
        format: `parquet` | `json` | `pickle` | `text` | `bin`. If None,
            auto-detects from the returned value.
        retention_days: If set, delete snapshots older than N days from this
            asset's folder after a successful write.
    """
    if not uri:
        raise ValueError("@snapshot requires uri=<fsspec URI directory>")

    def _decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            context = None
            if args and hasattr(args[0], "log"):
                context = args[0]
            elif "context" in kwargs and hasattr(kwargs["context"], "log"):
                context = kwargs["context"]
            if context is None:
                raise RuntimeError("@snapshot requires a Dagster context.")

            value = fn(*args, **kwargs)
            try:
                _do_snapshot(context, value, uri, format, retention_days)
            except Exception as e:  # noqa: BLE001
                # Snapshot failure should not fail the primary compute.
                try:
                    context.log.warning(f"@snapshot: write failed (asset still succeeds): {type(e).__name__}: {e}")
                except Exception:  # noqa: BLE001
                    pass
            return value

        return _wrapped
    return _decorator


class SnapshotAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of `@snapshot`. Wraps a compute with point-in-time snapshot writes."""

    asset_name: str = Field(description="Dagster asset name.")
    upstream_asset_key: Optional[str] = Field(default=None)
    compute: Dict[str, Any] = Field(description="`{kind: python, python: 'mod:fn'}`.")

    uri: str = Field(
        description="fsspec URI directory for snapshots (e.g., `s3://bucket/dir`, `/local/path`)."
    )
    format: Optional[str] = Field(
        default=None,
        description="`parquet` | `json` | `pickle` | `text` | `bin`. If null, auto-detected from the returned value.",
    )
    retention_days: Optional[int] = Field(
        default=None,
        description="If set, delete snapshots older than N days from this asset's folder after a successful write.",
    )
    code_version: Optional[str] = Field(
        default=None,
        description="Optional asset code_version. Written into the snapshot path so rollbacks can filter by version.",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None, description="Default: ['python', 'snapshot'].")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Snapshot Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        uri_ = self.uri
        fmt = self.format
        retention = self.retention_days
        code_version = self.code_version

        kinds_set = set(self.kinds or []) | {"python", "snapshot"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        ins = {}
        if upstream_asset_key:
            ins["upstream"] = dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Snapshot-instrumented asset {asset_name}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            kinds=kinds_set,
            code_version=code_version,
            ins=ins,
        )
        def _asset(context: dg.AssetExecutionContext, **kwargs):
            kind = (compute.get("kind") or "python").lower()
            if kind != "python":
                raise ValueError(f"SnapshotAssetComponent supports compute.kind=python only; got {kind!r}")
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
                value = fn()
            elif n_positional == 1:
                value = fn(context)
            else:
                value = fn(context, kwargs.get("upstream"))

            try:
                _do_snapshot(context, value, uri_, fmt, retention)
            except Exception as e:  # noqa: BLE001
                context.log.warning(f"@snapshot: write failed (asset still succeeds): {type(e).__name__}: {e}")

            return value

        return dg.Definitions(assets=[_asset])
