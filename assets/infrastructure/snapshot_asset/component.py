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
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from dagster import AssetKey, DagsterInstance

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

# Codec -> suffix appended to csv/json/text/bin files (empty = no suffix).
# Parquet handles compression internally with no filename suffix.
_TEXT_COMPRESSION_SUFFIX = {
    "gzip": ".gz",
    "gz": ".gz",
    "bz2": ".bz2",
    "zstd": ".zst",
    "zst": ".zst",
    "xz": ".xz",
}

# Pandas-normalized codec names for `.to_csv` / `.to_json` compression=.
_PANDAS_TEXT_CODEC = {
    "gzip": "gzip",
    "gz": "gzip",
    "bz2": "bz2",
    "zstd": "zstd",
    "zst": "zstd",
    "xz": "xz",
}


def _apply_compression_suffix(ext: str, compression: Optional[str]) -> str:
    """For text-ish formats, append a compression suffix (.gz/.zst/.bz2) to the extension."""
    if not compression or compression.lower() == "none":
        return ext
    suffix = _TEXT_COMPRESSION_SUFFIX.get(compression.lower())
    if not suffix:
        return ext
    return ext + suffix


def _serialize(value: Any, fmt: str, compression: Optional[str] = None) -> bytes:
    """Serialize `value` to bytes.

    `compression` is passed through to pandas / stdlib codecs for supported
    formats:
      * parquet -> `df.to_parquet(compression=<codec>)` (snappy default, gzip / zstd / brotli / lz4 supported by pyarrow)
      * json / csv (via DataFrame) -> `df.to_json` / `df.to_csv` with `compression=` (produces already-compressed bytes)
      * pickle / text / bin -> compression is currently a no-op (a warning is
        emitted upstream if the caller requested one for these formats).
    """
    codec = (compression or "").lower() or None
    if codec == "none":
        codec = None

    if fmt == "parquet":
        try:
            import io as _io
            buf = _io.BytesIO()
            if codec:
                value.to_parquet(buf, compression=codec)
            else:
                value.to_parquet(buf)
            return buf.getvalue()
        except Exception:
            fmt = "pickle"

    if fmt == "json":
        # Prefer DataFrame.to_json when we have one so pandas-native compression works.
        try:
            import pandas as pd
            if isinstance(value, pd.DataFrame):
                import io as _io
                buf = _io.BytesIO()
                pd_codec = _PANDAS_TEXT_CODEC.get(codec) if codec else None
                if pd_codec:
                    value.to_json(buf, compression=pd_codec)
                else:
                    value.to_json(buf)
                return buf.getvalue()
        except ImportError:
            pass
        # Plain dict/list JSON. Apply codec via stdlib if requested.
        raw = json.dumps(value, default=str).encode("utf-8")
        return _text_compress_bytes(raw, codec) if codec else raw

    if fmt == "pickle":
        return pickle.dumps(value)
    if fmt == "text":
        raw = str(value).encode("utf-8")
        return _text_compress_bytes(raw, codec) if codec else raw
    if fmt == "bin":
        raw = bytes(value) if isinstance(value, (bytes, bytearray)) else str(value).encode("utf-8")
        return _text_compress_bytes(raw, codec) if codec else raw
    raise ValueError(f"unknown snapshot format: {fmt!r}")


def _text_compress_bytes(raw: bytes, codec: Optional[str]) -> bytes:
    """Best-effort codec-based compression of a raw byte payload."""
    if not codec:
        return raw
    codec = codec.lower()
    try:
        if codec in ("gzip", "gz"):
            import gzip
            return gzip.compress(raw)
        if codec == "bz2":
            import bz2
            return bz2.compress(raw)
        if codec == "xz":
            import lzma
            return lzma.compress(raw)
        if codec in ("zstd", "zst"):
            try:
                import zstandard as zstd
                return zstd.ZstdCompressor().compress(raw)
            except ImportError:
                return raw
    except Exception:  # noqa: BLE001
        return raw
    return raw


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


def _do_snapshot(
    context: Any,
    value: Any,
    uri: str,
    fmt: Optional[str],
    retention_days: Optional[int],
    compression: Optional[str] = None,
) -> None:
    if _dry_run_active(context):
        try:
            context.log.info("@snapshot: dry_run active — skipping write")
        except Exception:  # noqa: BLE001
            pass
        return
    resolved_fmt, ext = _detect_format(value, fmt)

    # Warn if compression is set for a format that ignores it.
    codec = (compression or "").lower() or None
    if codec == "none":
        codec = None
    if codec and resolved_fmt in {"pickle"}:
        try:
            context.log.warning(
                f"@snapshot: compression={codec!r} ignored for format={resolved_fmt!r}"
            )
        except Exception:  # noqa: BLE001
            pass
        codec = None

    data = _serialize(value, resolved_fmt, codec)
    folder = _snapshot_folder(context, uri)
    # For non-parquet formats, append a compression suffix (.gz/.zst/etc)
    # so the file reflects its codec on disk. Parquet stores codec metadata
    # inside the file, so keep `.parquet`.
    ext_with_codec = ext if resolved_fmt == "parquet" else _apply_compression_suffix(ext, codec)
    filename = _snapshot_filename(context) + ext_with_codec
    full_path = _write(folder, filename, data)
    pruned = _prune(folder, retention_days)
    _emit_snapshot_observation(context, full_path, len(data), resolved_fmt, pruned)
    try:
        codec_note = f", compression={codec}" if codec else ""
        context.log.info(
            f"@snapshot: wrote {full_path} ({len(data)} bytes, format={resolved_fmt}{codec_note})"
        )
    except Exception:  # noqa: BLE001
        pass


def snapshot(
    *,
    uri: str,
    format: Optional[str] = None,
    retention_days: Optional[int] = None,
    compression: Optional[str] = None,
) -> Callable:
    """Write a point-in-time snapshot of the wrapped asset's return value.

    ```python
    @dg.asset(code_version="2.1.0")
    @snapshot(uri="s3://backups/report_snapshots", retention_days=30, compression="zstd")
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
        compression: Codec name (`gzip` | `zstd` | `snappy` | `brotli` |
            `bz2` | `xz` | `none`). Parquet writes pass this to
            `df.to_parquet(compression=...)` (default is `snappy` if omitted).
            JSON / text / bin writes gzip/bz2/zstd via stdlib codecs — the
            file gains a `.gz` / `.zst` / `.bz2` suffix. Pickle ignores the
            flag (a warning is emitted).
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
                _do_snapshot(context, value, uri, format, retention_days, compression)
            except Exception as e:  # noqa: BLE001
                # Snapshot failure should not fail the primary compute.
                try:
                    context.log.warning(f"@snapshot: write failed (asset still succeeds): {type(e).__name__}: {e}")
                except Exception:  # noqa: BLE001
                    pass
            return value

        return _wrapped
    return _decorator


# --------------------------------------------------------------------------
# load_snapshot — first-class helper to fetch a snapshot from the event log
# --------------------------------------------------------------------------


def _parse_ts(ts: Union[str, "_dt.datetime"]) -> _dt.datetime:
    """Accept an ISO-8601 string or a datetime; return a tz-aware datetime."""
    if isinstance(ts, _dt.datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=_dt.timezone.utc)
    if isinstance(ts, str):
        s = ts.strip()
        # datetime.fromisoformat before 3.11 doesn't accept trailing "Z"
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = _dt.datetime.fromisoformat(s)
        except ValueError as e:
            raise ValueError(f"load_snapshot: cannot parse timestamp {ts!r}: {e}") from e
        return dt if dt.tzinfo else dt.replace(tzinfo=_dt.timezone.utc)
    raise TypeError(f"load_snapshot: at_or_before_ts must be str or datetime; got {type(ts).__name__}")


def _read_snapshot_file(path: str) -> Any:
    """Read a snapshot file back into its original value based on the extension.

    Returns a pandas DataFrame for `.parquet`/`.json` (if pandas is installed and the
    JSON was written from a DataFrame), a dict/list for JSON dicts, a str for `.txt`,
    bytes for `.bin`, or the unpickled object for `.pkl`. Handles `.gz`/`.bz2`/`.zst`
    compression suffixes for text formats.
    """
    fs = _get_fs(path)
    if fs is None:
        with open(path, "rb") as f:
            data = f.read()
    else:
        proto, p = path.split("://", 1)
        with fs.open(p, "rb") as f:
            data = f.read()

    lower = path.lower()
    # Strip codec suffix to determine base extension
    for suffix in (".gz", ".bz2", ".zst", ".xz"):
        if lower.endswith(suffix):
            if suffix in (".gz",):
                import gzip
                data = gzip.decompress(data)
            elif suffix == ".bz2":
                import bz2
                data = bz2.decompress(data)
            elif suffix == ".zst":
                try:
                    import zstandard as zstd
                    data = zstd.ZstdDecompressor().decompress(data)
                except ImportError as e:
                    raise ImportError(
                        "load_snapshot: zstandard package required to read .zst files"
                    ) from e
            elif suffix == ".xz":
                import lzma
                data = lzma.decompress(data)
            lower = lower[: -len(suffix)]
            break

    if lower.endswith(".parquet"):
        try:
            import io as _io
            import pandas as pd
            return pd.read_parquet(_io.BytesIO(data))
        except ImportError as e:
            raise ImportError("load_snapshot: pandas required to read parquet snapshots") from e
    if lower.endswith(".json"):
        try:
            import io as _io
            import pandas as pd
            # Try pandas first (round-trips DataFrame snapshots correctly)
            try:
                return pd.read_json(_io.BytesIO(data))
            except (ValueError, Exception):  # noqa: BLE001
                pass
        except ImportError:
            pass
        return json.loads(data.decode("utf-8"))
    if lower.endswith(".txt"):
        return data.decode("utf-8")
    if lower.endswith(".pkl"):
        return pickle.loads(data)
    if lower.endswith(".bin"):
        return data
    # Unknown extension — return raw bytes.
    return data


def load_snapshot(
    instance: "DagsterInstance",
    asset_key: Union[str, "AssetKey"],
    code_version: Optional[str] = None,
    at_or_before_ts: Optional[Union[str, "_dt.datetime"]] = None,
    latest: bool = False,
) -> Any:
    """Load a snapshot of an asset by code_version + timestamp.

    At least one of ``code_version``, ``at_or_before_ts``, or ``latest=True``
    must be supplied.

    Resolution order:

    1. Query the event log for ``AssetObservation(snapshot_asset=written)``
       events on ``asset_key``.
    2. Filter by ``code_version`` if given (matched against the parent
       folder segment in the observation's ``snapshot_path`` metadata).
    3. Filter to those with timestamp <= ``at_or_before_ts`` if given
       (or take latest if ``latest=True``).
    4. Take the most recent match.
    5. Read the parquet / json / pickle / text / bin file back via
       ``fsspec`` (auto-detecting the codec from the extension).

    Args:
        instance: DagsterInstance to query.
        asset_key: Either the user-string form (``"daily_report"`` or
            ``"reports/daily"``) or a real ``AssetKey``.
        code_version: If set, only consider snapshots whose path segment
            after ``<asset>`` matches this string.
        at_or_before_ts: ISO-8601 string or ``datetime``. Only consider
            snapshots observed at/before this timestamp.
        latest: If True, return the most recent matching snapshot
            regardless of timestamp.

    Returns:
        The rehydrated value (typically a ``pd.DataFrame``, ``dict``,
        ``list``, ``bytes``, or ``str`` depending on how it was written).

    Raises:
        ValueError: if none of ``code_version`` / ``at_or_before_ts`` /
            ``latest`` are supplied.
        FileNotFoundError: if no matching snapshot exists in the event log.
    """
    if not (code_version or at_or_before_ts or latest):
        raise ValueError(
            "load_snapshot: must supply at least one of `code_version`, "
            "`at_or_before_ts`, or `latest=True`."
        )
    from dagster import AssetKey, EventRecordsFilter, DagsterEventType

    if isinstance(asset_key, str):
        asset_key_obj = AssetKey.from_user_string(asset_key)
    else:
        asset_key_obj = asset_key

    cutoff_dt: Optional[_dt.datetime] = None
    if at_or_before_ts is not None:
        cutoff_dt = _parse_ts(at_or_before_ts)

    records = instance.get_event_records(
        event_records_filter=EventRecordsFilter(
            event_type=DagsterEventType.ASSET_OBSERVATION,
            asset_key=asset_key_obj,
        ),
        limit=500,
        ascending=False,
    )

    best_path: Optional[str] = None
    best_ts: Optional[float] = None
    for r in records:
        obs = getattr(r, "asset_observation", None)
        if obs is None:
            continue
        tags = obs.tags or {}
        if tags.get(_SNAPSHOT_TAG) != "written":
            continue
        meta = obs.metadata or {}
        path_mv = meta.get("snapshot_path")
        if path_mv is None:
            continue
        path_val = getattr(path_mv, "value", None) or getattr(path_mv, "path", None) or str(path_mv)
        if not path_val:
            continue

        # Filter by code_version — snapshot layout is <root>/<asset>/<code_version>/<file>
        if code_version:
            # Split off filename, then last dir segment is code_version.
            parent_dir = path_val.rsplit("/", 1)[0]
            observed_version = parent_dir.rsplit("/", 1)[-1] if "/" in parent_dir else ""
            if observed_version != code_version:
                continue

        ts = r.timestamp  # unix seconds (float)
        if cutoff_dt is not None:
            if ts is None:
                continue
            if _dt.datetime.fromtimestamp(float(ts), tz=_dt.timezone.utc) > cutoff_dt:
                continue

        if best_ts is None or (ts is not None and float(ts) > best_ts):
            best_ts = float(ts) if ts is not None else best_ts
            best_path = path_val

    if not best_path:
        filters: List[str] = []
        if code_version:
            filters.append(f"code_version={code_version!r}")
        if at_or_before_ts is not None:
            filters.append(f"at_or_before_ts={at_or_before_ts!r}")
        if latest and not filters:
            filters.append("latest=True")
        raise FileNotFoundError(
            f"load_snapshot: no snapshot observation found for asset_key={asset_key_obj.to_user_string()!r}"
            + (f" ({', '.join(filters)})" if filters else "")
        )

    return _read_snapshot_file(best_path)


class SnapshotAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of `@snapshot`. Two authoring modes:

    1. **Define a new asset from scratch** (original shape): supply
       `asset_name` + `compute: {kind: python, python: 'mod:fn'}`. Builds a
       single asset whose returned value is snapshotted after compute.

    2. **Wrap an existing DCC component** (composability): supply
       `wraps: {type: <component_class>, attributes: {...}}`. The inner
       component's assets are materialized as they would normally, and
       each compute's return value is snapshotted. Preserves inner asset
       partitions, deps, resources, kinds, tags, group, description.
       Direct YAML analog of `@snapshot @dg.asset` in Python.

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
            "Wrap another DCC component's assets with point-in-time snapshot writes "
            "instead of defining new compute. Shape: `{type: 'dagster_community_components.<Component>', "
            "attributes: {...}}`. Mutually exclusive with `compute`."
        ),
    )

    uri: str = Field(
        description="fsspec URI directory for snapshots (e.g., `s3://bucket/dir`, `/local/path`)."
    )
    format: Optional[str] = Field(
        default=None,
        description="`parquet` | `json` | `pickle` | `text` | `bin`. If null, auto-detected from the returned value.",
    )
    compression: Optional[str] = Field(
        default=None,
        description=(
            "Compression codec: `gzip` | `zstd` | `snappy` | `brotli` | `bz2` | `xz` | `none`. "
            "Default None = format default (parquet defaults to snappy). "
            "Parquet forwards this to `df.to_parquet(compression=...)`. "
            "JSON / text / bin writes gzip/bz2/zstd via stdlib codecs and the file gains a "
            "`.gz` / `.zst` / `.bz2` suffix. Pickle ignores the flag (warns)."
        ),
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
        # Route: wraps-an-inner-component  vs  builds-own-asset
        if self.wraps is not None:
            if self.compute is not None:
                raise ValueError("SnapshotAssetComponent: `wraps:` and `compute:` are mutually exclusive.")
            return self._build_wrapped(context)
        if self.compute is None:
            raise ValueError("SnapshotAssetComponent: supply either `compute` (build new asset) or `wraps` (wrap existing component).")
        if not self.asset_name:
            raise ValueError("SnapshotAssetComponent: `asset_name` required when using `compute:`.")

        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        uri_ = self.uri
        fmt = self.format
        compression = self.compression
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
                _do_snapshot(context, value, uri_, fmt, retention, compression)
            except Exception as e:  # noqa: BLE001
                context.log.warning(f"@snapshot: write failed (asset still succeeds): {type(e).__name__}: {e}")

            return value

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

        uri_ = self.uri
        fmt = self.format
        compression = self.compression
        retention = self.retention_days
        code_version = self.code_version

        inner_kinds = set(getattr(spec, "kinds", None) or []) if spec else set()
        merged_kinds = inner_kinds | set(self.kinds or []) | {"snapshot"}
        inner_tags = dict(getattr(spec, "tags", None) or {}) if spec else {}
        merged_tags = {**inner_tags, **(self.tags or {})}
        merged_owners = list((spec.owners if spec else []) or []) + (self.owners or [])
        inner_description = (spec.description if spec else None) or f"Snapshot-wrapped {key.to_user_string()}"
        merged_description = (
            f"{inner_description}  "
            f"[snapshot: uri={uri_}, format={fmt or 'auto'}, "
            f"compression={compression or 'default'}, retention_days={retention}]"
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
            code_version=code_version or (spec.code_version if spec else None),
        )
        def _snapshot_wrapped(context: dg.AssetExecutionContext, **kwargs):
            result = inner_compute(context, **kwargs)

            # Extract the value to snapshot. If the inner returned a plain value,
            # snapshot it directly. If it returned a MaterializeResult, we don't
            # have a value to snapshot (the IO manager has it) — fall back to the
            # MaterializeResult's metadata payload if present, else skip.
            value_to_snap = None
            if isinstance(result, dg.MaterializeResult):
                value_to_snap = None  # nothing to snapshot; inner's IO manager owns value
            elif isinstance(result, dg.Output):
                value_to_snap = result.value
            else:
                value_to_snap = result

            snapshot_meta: Dict[str, Any] = {}
            if value_to_snap is not None:
                try:
                    _do_snapshot(context, value_to_snap, uri_, fmt, retention, compression)
                    resolved_fmt, _ext = _detect_format(value_to_snap, fmt)
                    snapshot_meta = {
                        "snapshot_written": dg.MetadataValue.bool(True),
                        "snapshot_format": dg.MetadataValue.text(resolved_fmt),
                        "snapshot_compression": dg.MetadataValue.text(compression or "default"),
                    }
                except Exception as e:  # noqa: BLE001
                    context.log.warning(
                        f"[snapshot wrap] write failed (asset still succeeds): {type(e).__name__}: {e}"
                    )
                    snapshot_meta = {
                        "snapshot_written": dg.MetadataValue.bool(False),
                        "snapshot_error": dg.MetadataValue.text(f"{type(e).__name__}: {e}"),
                    }
            else:
                context.log.warning(
                    "[snapshot wrap] inner returned MaterializeResult with no value; snapshot skipped"
                )
                snapshot_meta = {
                    "snapshot_written": dg.MetadataValue.bool(False),
                    "snapshot_skip_reason": dg.MetadataValue.text("inner returned MaterializeResult (no value)"),
                }

            # Merge snapshot metadata into the inner's MaterializeResult (if any)
            if isinstance(result, dg.MaterializeResult):
                merged = dict(result.metadata or {})
                merged.update(snapshot_meta)
                return dg.MaterializeResult(
                    asset_key=result.asset_key,
                    metadata=merged,
                    check_results=result.check_results,
                    data_version=result.data_version,
                    tags=result.tags,
                )
            return result

        return _snapshot_wrapped


def _resolve_inner_component(wraps: Dict[str, Any]):
    """Resolve `{type: '...', attributes: {...}}` → instantiated component."""
    type_str = wraps.get("type")
    attrs = wraps.get("attributes") or {}
    if not type_str or not isinstance(type_str, str):
        raise ValueError("SnapshotAssetComponent.wraps requires `type: <fully-qualified-class-name>`.")
    if ":" in type_str:
        mod_path, cls_name = type_str.rsplit(":", 1)
    else:
        mod_path, cls_name = type_str.rsplit(".", 1)
    try:
        mod = importlib.import_module(mod_path)
    except ImportError as e:
        raise ValueError(f"SnapshotAssetComponent.wraps: cannot import {mod_path!r}: {e}") from e
    cls = getattr(mod, cls_name, None)
    if cls is None:
        raise ValueError(f"SnapshotAssetComponent.wraps: {cls_name!r} not found in {mod_path!r}.")
    try:
        return cls(**attrs)
    except Exception as e:  # noqa: BLE001
        raise ValueError(
            f"SnapshotAssetComponent.wraps: constructing {type_str} failed: {type(e).__name__}: {e}"
        ) from e
