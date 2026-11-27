"""CloudStorageCopyAssetComponent -- a generic "Copy activity" stand-in: moves data from one cloud-storage (or
local) path to another, or into a warehouse table, the same shape as Azure Data Factory's Copy activity's most
common pairs (file-to-file across blob/ADLS/S3/GCS, and file-to-database).

Two modes:
  copy_mode: "binary" (default) -- a raw byte-stream copy, no parsing at all. Matches ADF's default "Binary"
    dataset type, which is how most real file-to-file Copy activities actually run (preserve bytes exactly,
    no schema). Fast, format-agnostic, works for any file type.
  copy_mode: "dataframe" -- reads the source into a DataFrame (same format dispatch as file_ingestion's read
    side: pandas/pyarrow resolve fsspec's s3://, gs://, abfss://, local paths internally; only Avro needs an
    explicit fsspec.open()) and either writes it back out in a (possibly different) file format, or loads it
    into a warehouse table (the exact sink shape rest_api_ingestion's own `sinks: [{kind: table, ...}]` uses).
    Use this mode when the source and sink formats differ, or the sink is a database table.

Source and sink are both plain fsspec URIs (`s3://`, `gs://`, `abfss://...`, local paths); `{partition_key}` in
either is replaced with the running partition at materialization time, matching this repo's existing convention
(e.g. adls_to_database_asset's blob_name/table_name templating).
"""
import os
import shutil
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=(), strs=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    for k in strs:  # str (or Dict[str, str]) fields: a numeric-looking YAML scalar resolves to int/float/bool
        v = out.get(k)
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            out[k] = str(v)
        elif isinstance(v, dict):
            out[k] = {str(a): (("true" if b is True else "false") if isinstance(b, bool) else (str(b) if isinstance(b, (int, float)) else b)) for a, b in v.items()}
    return out
# END field-shape normalization


# BEGIN azure storage options (inlined per component; keep copies identical -- see
# tests/test_azure_storage_options_drift.py. fsspec storage_options built only when explicit Azure
# credential env vars are configured for an abfs://.../abfss://.../az://... URI -- ambient fsspec
# credential discovery applies otherwise, same as every other scheme already relies on.
def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    value = dg.EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value


def _azure_storage_options(uri: str, *, account_name_env_var=None, account_key_env_var=None, connection_string_env_var=None) -> Optional[Dict[str, str]]:
    """fsspec storage_options for an abfs://.../abfss://.../az://... URI, built only when Azure credential env
    vars are configured for it. Returns None for a non-Azure URI, or an Azure one with no explicit credentials
    configured (fsspec's ambient credential discovery applies then, same as the read side already relies on)."""
    if not uri.lower().startswith(("abfs://", "abfss://", "az://")):
        return None
    opts: Dict[str, str] = {}
    if account_name_env_var:
        opts["account_name"] = _require_env(account_name_env_var)
    if connection_string_env_var:
        opts["connection_string"] = _require_env(connection_string_env_var)
    elif account_key_env_var:
        opts["account_key"] = _require_env(account_key_env_var)
    return opts or None
# END azure storage options


def _write_dataframe(df, uri: str, fmt: str, *, compression: Optional[str] = None, storage_options: Optional[Dict[str, str]] = None):
    kwargs: Dict[str, Any] = {"storage_options": storage_options} if storage_options else {}
    if fmt == "csv":
        df.to_csv(uri, index=False, compression=compression, **kwargs)
    elif fmt == "jsonl":
        df.to_json(uri, orient="records", lines=True, **kwargs)
    elif fmt == "json":
        df.to_json(uri, orient="records", **kwargs)
    elif fmt == "parquet":
        df.to_parquet(uri, compression=compression, **kwargs)
    else:
        raise ValueError(f"Unsupported sink_format: {fmt!r} (supported: csv, jsonl, json, parquet)")


# ── dataframe read dispatch (mirrors file_ingestion's own read-side format dispatch) ──────────────────────────
_SOURCE_FORMATS = ("auto", "csv", "tsv", "json", "jsonl", "parquet", "excel", "avro", "orc", "feather")


def _detect_format_from_uri(uri: str) -> str:
    lower = uri.lower()
    if lower.endswith((".gz", ".bz2", ".xz", ".zip")):
        lower = lower.rsplit(".", 1)[0]
    for ext, fmt in (
        (".csv", "csv"), (".tsv", "tsv"), (".jsonl", "jsonl"), (".ndjson", "jsonl"),
        (".json", "json"), (".parquet", "parquet"), (".pq", "parquet"),
        (".xlsx", "excel"), (".xls", "excel"), (".avro", "avro"), (".orc", "orc"),
        (".feather", "feather"), (".arrow", "feather"), (".ipc", "feather"),
    ):
        if lower.endswith(ext):
            return fmt
    raise ValueError(f"Could not auto-detect format from URI {uri!r}. Set source_format explicitly to one of: {_SOURCE_FORMATS[1:]}")


def _read_avro(uri: str):
    import fastavro
    import fsspec
    import pandas as pd
    with fsspec.open(uri, "rb") as f:
        rows = list(fastavro.reader(f))
    return pd.DataFrame(rows)


def _read_dataframe(uri: str, fmt: str):
    import pandas as pd
    if fmt == "auto":
        fmt = _detect_format_from_uri(uri)
    if fmt == "csv":
        return pd.read_csv(uri)
    if fmt == "tsv":
        return pd.read_csv(uri, delimiter="\t")
    if fmt == "json":
        return pd.read_json(uri)
    if fmt == "jsonl":
        return pd.read_json(uri, lines=True)
    if fmt == "parquet":
        return pd.read_parquet(uri)
    if fmt == "excel":
        return pd.read_excel(uri)
    if fmt == "avro":
        return _read_avro(uri)
    if fmt == "orc":
        return pd.read_orc(uri)
    if fmt == "feather":
        return pd.read_feather(uri)
    raise ValueError(f"Unsupported source_format: {fmt!r} (supported: {_SOURCE_FORMATS})")


# BEGIN warehouse table sink (inlined per component; keep copies identical -- see
# tests/test_warehouse_table_sink_drift.py. Ported verbatim from rest_api_ingestion's own `sinks: [{kind: table,
# ...}]` dict-shaped sink -- DuckDB .register() fast path, SQLAlchemy to_sql fallback, mode: upsert_on_match via
# a transactional DELETE+INSERT keyed by `match`.)
def _write_table_sink(context, df, sink: Dict[str, Any]) -> Dict[str, Any]:
    from contextlib import nullcontext
    sink_df = df.copy()
    sink_resource_key = sink.get("resource_key")
    if not sink_resource_key:
        raise ValueError("sink_table requires 'resource_key'")
    sink_table = sink.get("table")
    if not sink_table:
        raise ValueError("sink_table requires 'table'")
    sink_schema = sink.get("schema")
    sink_if_exists = sink.get("if_exists", "append")
    sink_mode = (sink.get("mode") or "").lower() or None
    sink_match: List[str] = list(sink.get("match") or [])
    if sink_mode == "upsert_on_match" and not sink_match:
        raise ValueError("sink_table mode=upsert_on_match requires 'match: [col, ...]'")

    resource = getattr(context.resources, sink_resource_key)

    def _acquire():
        if hasattr(resource, "get_connection"):
            gc = resource.get_connection()
            return gc if hasattr(gc, "__enter__") else nullcontext(gc)
        if hasattr(resource, "get_engine"):
            eng = resource.get_engine()
            return eng if hasattr(eng, "__enter__") else nullcontext(eng)
        raise ValueError(f"sink resource {sink_resource_key!r} must expose .get_connection() or .get_engine()")

    qualified = f"{sink_schema}.{sink_table}" if sink_schema else sink_table
    out: Dict[str, Any] = {}
    with _acquire() as conn:
        if hasattr(conn, "register") and hasattr(conn, "execute") and hasattr(conn, "unregister"):
            conn.register("_copy_sink_batch", sink_df)
            try:
                if sink_mode == "upsert_on_match":
                    conn.execute(f"CREATE TABLE IF NOT EXISTS {qualified} AS SELECT * FROM _copy_sink_batch WHERE 1=0")
                    match_tuple = ", ".join(sink_match)
                    conn.execute("BEGIN TRANSACTION")
                    try:
                        conn.execute(f"DELETE FROM {qualified} WHERE ({match_tuple}) IN (SELECT DISTINCT {match_tuple} FROM _copy_sink_batch)")
                        conn.execute(f"INSERT INTO {qualified} SELECT * FROM _copy_sink_batch")
                        conn.execute("COMMIT")
                    except Exception:
                        conn.execute("ROLLBACK")
                        raise
                elif sink_if_exists == "replace":
                    conn.execute(f"CREATE OR REPLACE TABLE {qualified} AS SELECT * FROM _copy_sink_batch")
                else:
                    conn.execute(f"CREATE TABLE IF NOT EXISTS {qualified} AS SELECT * FROM _copy_sink_batch WHERE 1=0")
                    conn.execute("BEGIN TRANSACTION")
                    try:
                        conn.execute(f"INSERT INTO {qualified} SELECT * FROM _copy_sink_batch")
                        conn.execute("COMMIT")
                    except Exception:
                        conn.execute("ROLLBACK")
                        raise
                out["fast_path"] = "duckdb-register"
            finally:
                try:
                    conn.unregister("_copy_sink_batch")
                except Exception:  # noqa: BLE001
                    pass
        else:
            if sink_mode == "upsert_on_match":
                from sqlalchemy import text as _sa_text
                distinct = sink_df[sink_match].drop_duplicates()
                match_tuple = ", ".join(sink_match)
                if len(distinct) > 0:
                    placeholders = ", ".join("(" + ", ".join(f":v{i}_{j}" for j in range(len(sink_match))) + ")" for i in range(len(distinct)))
                    params_sql = {}
                    for i, row in enumerate(distinct.itertuples(index=False)):
                        for j, v in enumerate(row):
                            params_sql[f"v{i}_{j}"] = v
                    tx = conn.begin() if hasattr(conn, "begin") else None
                    if tx is not None:
                        with tx as _c:
                            _c.execute(_sa_text(f"DELETE FROM {qualified} WHERE ({match_tuple}) IN ({placeholders})"), params_sql)
                            sink_df.to_sql(sink_table, _c, schema=sink_schema, if_exists="append", index=False)
                    else:
                        sink_df.to_sql(sink_table, conn, schema=sink_schema, if_exists="append", index=False)
                else:
                    sink_df.to_sql(sink_table, conn, schema=sink_schema, if_exists="append", index=False)
            else:
                sink_df.to_sql(sink_table, conn, schema=sink_schema, if_exists=sink_if_exists, index=False)
            out["fast_path"] = "sqlalchemy-to_sql"
    out["rows"] = len(sink_df)
    out["mode"] = f"upsert_on_match({','.join(sink_match)})" if sink_mode == "upsert_on_match" else sink_if_exists
    out["qualified"] = qualified
    return out
# END warehouse table sink


def _copy_matching_files(
    source_dir: str, wildcard: str, recursive: bool, sink_dir: str,
    azure_opts_fn,
) -> List[Dict[str, Any]]:
    """copy_mode='binary' + wildcard only: binary-copies every file under `source_dir` matching `wildcard`
    (recursively, if `recursive`) to the same relative path under `sink_dir` -- same-format file copy across
    blob/ADLS/S3/local, the common ADF "copy everything matching *.csv in this folder" Copy-activity shape."""
    import fsspec
    fs, root = fsspec.core.url_to_fs(source_dir)
    root = root.rstrip("/")
    pattern = f"{root}/" + ("**/" if recursive else "") + wildcard
    source_scheme = source_dir.split("://", 1)[0] + "://" if "://" in source_dir else ""
    sink_dir = sink_dir.rstrip("/")
    copied: List[Dict[str, Any]] = []
    for match in sorted(fs.glob(pattern)):
        if fs.isdir(match):
            continue
        rel = match[len(root):].lstrip("/")
        src_uri = source_scheme + match if source_scheme else match
        dst_uri = f"{sink_dir}/{rel}"
        src_opts = azure_opts_fn(src_uri) or {}
        dst_opts = azure_opts_fn(dst_uri) or {}
        with fsspec.open(src_uri, "rb", **src_opts) as s, fsspec.open(dst_uri, "wb", **dst_opts) as d:
            shutil.copyfileobj(s, d)
        copied.append({"source": src_uri, "sink": dst_uri})
    return copied


# ── Partition helper (standard across this repo's components) ─────────────────────────────────────────────────

def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily": return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly": return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly": return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("static partition requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("dynamic partition requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        return MultiPartitionsDefinition({d["name"]: _build_axis(d) for d in partition_dimensions})

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily": return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly": return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly": return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError("partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only.")
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values: raise ValueError("static requires values")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("dynamic requires dynamic_partition_name")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class CloudStorageCopyAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """A generic Copy-activity stand-in: moves data from one cloud-storage (or local) path to another, or into a
    warehouse table.

    ```yaml
    type: dagster_community_components.CloudStorageCopyAssetComponent
    attributes:
      asset_key: copy/orders_blob_to_adls
      source_path: s3://raw-bucket/orders/{partition_key}.parquet
      sink_path: abfss://landing@acct.dfs.core.windows.net/orders/{partition_key}.parquet
      partition_type: daily
      partition_start: "2024-01-01"
    ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'copy/orders_to_adls').")
    source_path: str = Field(
        description=(
            "Source fsspec URI (s3://, gs://, abfss://..., or a local/plain path). '{partition_key}' is "
            "replaced with the running partition. When `wildcard` is set, this is treated as a DIRECTORY "
            "(not a single file) that `wildcard` is matched under."
        ),
    )
    wildcard: Optional[str] = Field(
        default=None,
        description=(
            "Glob pattern (e.g. '*.csv') matched under source_path, which is then treated as a directory. "
            "Every matching file is binary-copied to the same relative path under sink_path -- the common "
            "'copy everything matching *.csv in this folder' same-format file copy. Requires copy_mode='binary' "
            "and sink_path (not sink_table); leave unset for the default single-file source_path -> sink_path/"
            "sink_table copy."
        ),
    )
    recursive: bool = Field(
        default=False,
        description="wildcard only. Match files in subdirectories under source_path too (equivalent to a '**/' prefix on wildcard).",
    )
    copy_mode: Literal["binary", "dataframe"] = Field(
        default="binary",
        description=(
            "'binary' (default): a raw byte-stream copy, no parsing -- matches ADF's default Binary dataset "
            "type, the shape most real file-to-file Copy activities actually use. 'dataframe': read the source "
            "into a DataFrame and either write it out in a (possibly different) file format, or load it into a "
            "warehouse table via sink_table. Required (not binary) when sink_table is set, or when sink_format "
            "differs from the source's real format."
        ),
    )
    source_format: Literal["auto", "csv", "tsv", "json", "jsonl", "parquet", "excel", "avro", "orc", "feather"] = Field(
        default="auto",
        description="copy_mode='dataframe' only. 'auto' detects from source_path's extension.",
    )

    sink_path: Optional[str] = Field(
        default=None,
        description="File sink: fsspec URI ('{partition_key}' templated). Exactly one of sink_path / sink_table must be set.",
    )
    sink_format: Optional[Literal["csv", "jsonl", "json", "parquet"]] = Field(
        default=None,
        description="copy_mode='dataframe' + sink_path only (required then). Ignored for copy_mode='binary' -- bytes are written verbatim, no re-serialization.",
    )
    sink_compression: Optional[str] = Field(default=None, description="copy_mode='dataframe' + sink_path only. e.g. 'gzip', 'snappy' (parquet).")
    sink_table: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Warehouse-table sink (requires copy_mode='dataframe'). Keys: resource_key (required), table "
            "(required), schema, if_exists: append|replace (default append), mode: upsert_on_match (partition-"
            "rewrite idempotent DELETE-then-INSERT keyed by match), match: [col, ...] (required for "
            "upsert_on_match). Auto-detects DuckDB's .register() fast path; falls back to SQLAlchemy to_sql. "
            "Exactly one of sink_path / sink_table must be set."
        ),
    )

    azure_account_name_env_var: Optional[str] = Field(default=None, description="Only used when source_path or sink_path is an abfs://.../abfss://... URI.")
    azure_account_key_env_var: Optional[str] = Field(default=None, description="Only used when source_path or sink_path is an abfs://.../abfss://... URI (ignored if azure_connection_string_env_var is also set).")
    azure_connection_string_env_var: Optional[str] = Field(default=None, description="Only used when source_path or sink_path is an abfs://.../abfss://... URI. Takes precedence over azure_account_key_env_var.")

    dry_run: bool = Field(default=False, description="Log what would be copied and skip touching source/sink entirely.")

    group_name: Optional[str] = Field(default="ingestion", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys for lineage.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog. Auto-includes 'cloud-storage' if empty.")

    partition_type: Optional[str] = Field(default=None)
    partition_cron_schedule: Optional[str] = Field(default=None, description="Required when partition_type='cron'. e.g. '0 0 * * 1-5' for weekdays only.")
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d", description="strftime format for partition keys produced by partition_type='cron'.")
    partition_end_offset: int = Field(default=0, description="0 (default): a time-based partition set only ever holds COMPLETED windows. 1: include the current, in-progress window.")
    partition_timezone: Optional[str] = Field(default=None, description="IANA timezone for the underlying partitions definition (defaults to UTC).")
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Any] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")
    pool: Optional[str] = Field(default=None, description="Dagster's native named concurrency pool -- a cap on concurrent copies sharing the same pool name.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("deps", "owners", "kinds"), tags=("asset_tags",))

    @model_validator(mode="after")
    def _validate_sink(self):
        if bool(self.sink_path) == bool(self.sink_table):
            raise ValueError("CloudStorageCopyAssetComponent: set exactly one of sink_path or sink_table.")
        if self.sink_table and self.copy_mode != "dataframe":
            raise ValueError("CloudStorageCopyAssetComponent: sink_table requires copy_mode='dataframe'.")
        if self.sink_path and self.copy_mode == "dataframe" and not self.sink_format:
            raise ValueError("CloudStorageCopyAssetComponent: copy_mode='dataframe' with sink_path requires sink_format.")
        if self.wildcard and (self.copy_mode != "binary" or not self.sink_path):
            raise ValueError("CloudStorageCopyAssetComponent: wildcard requires copy_mode='binary' and sink_path (not sink_table).")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Cloud Storage Copy Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        kinds = set(component.kinds) if component.kinds else {"cloud-storage"}
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (component.deps or [])]

        _retry_policy = None
        if component.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=component.retry_policy_max_retries,
                delay=component.retry_policy_delay_seconds or 1,
                backoff=Backoff[component.retry_policy_backoff.upper()],
            )

        partitions_def = _build_partitions_def(
            component.partition_type, component.partition_start, component.partition_values,
            component.dynamic_partition_name, component.partition_dimensions,
            cron_schedule=component.partition_cron_schedule,
            cron_fmt=component.partition_cron_fmt or "%Y-%m-%d",
            end_offset=component.partition_end_offset, timezone=component.partition_timezone,
        )

        required_resource_keys = set()
        if component.sink_table and component.sink_table.get("resource_key"):
            required_resource_keys.add(component.sink_table["resource_key"])

        @dg.asset(
            key=dg.AssetKey(component.asset_key.split("/")),
            description=component.description or f"Copy {component.source_path} -> {component.sink_path or (component.sink_table or {}).get('table')}",
            group_name=component.group_name,
            deps=deps_keys,
            owners=component.owners or None,
            tags=component.asset_tags or None,
            kinds=kinds,
            partitions_def=partitions_def,
            required_resource_keys=required_resource_keys or None,
            retry_policy=_retry_policy,
            pool=component.pool,
        )
        def _cloud_storage_copy_asset(context: dg.AssetExecutionContext):
            raw_key = context.partition_key if context.has_partition_key else None
            source_path = component.source_path.replace("{partition_key}", raw_key) if raw_key is not None else component.source_path
            sink_path = None
            if component.sink_path:
                sink_path = component.sink_path.replace("{partition_key}", raw_key) if raw_key is not None else component.sink_path

            if component.dry_run:
                target = sink_path or (component.sink_table or {}).get("table")
                if component.wildcard:
                    context.log.info(f"[dry_run] would copy files matching {component.wildcard!r} under {source_path} -> {target}")
                else:
                    context.log.info(f"[dry_run] would copy {source_path} -> {target}")
                return dg.MaterializeResult(metadata={"dry_run": dg.MetadataValue.bool(True), "source_path": dg.MetadataValue.text(source_path)})

            if component.wildcard:
                def _azure_opts(uri):
                    return _azure_storage_options(
                        uri, account_name_env_var=component.azure_account_name_env_var,
                        account_key_env_var=component.azure_account_key_env_var,
                        connection_string_env_var=component.azure_connection_string_env_var,
                    )
                copied = _copy_matching_files(source_path, component.wildcard, component.recursive, sink_path, _azure_opts)
                context.log.info(f"Copied {len(copied)} file(s) matching {component.wildcard!r} under {source_path} -> {sink_path}")
                return dg.MaterializeResult(metadata={
                    "source_path": dg.MetadataValue.text(source_path),
                    "sink_path": dg.MetadataValue.text(sink_path),
                    "wildcard": dg.MetadataValue.text(component.wildcard),
                    "files_copied": dg.MetadataValue.int(len(copied)),
                    "copy_mode": dg.MetadataValue.text("binary"),
                    "dry_run": dg.MetadataValue.bool(False),
                })

            if component.copy_mode == "binary":
                import fsspec
                src_opts = _azure_storage_options(
                    source_path, account_name_env_var=component.azure_account_name_env_var,
                    account_key_env_var=component.azure_account_key_env_var,
                    connection_string_env_var=component.azure_connection_string_env_var,
                ) or {}
                sink_opts = _azure_storage_options(
                    sink_path, account_name_env_var=component.azure_account_name_env_var,
                    account_key_env_var=component.azure_account_key_env_var,
                    connection_string_env_var=component.azure_connection_string_env_var,
                ) or {}
                with fsspec.open(source_path, "rb", **src_opts) as src, fsspec.open(sink_path, "wb", **sink_opts) as dst:
                    bytes_copied = shutil.copyfileobj(src, dst) or 0
                    try:
                        bytes_copied = dst.tell()
                    except Exception:  # noqa: BLE001
                        bytes_copied = 0
                context.log.info(f"Copied {source_path} -> {sink_path} ({bytes_copied} bytes, binary)")
                return dg.MaterializeResult(metadata={
                    "source_path": dg.MetadataValue.text(source_path),
                    "sink_path": dg.MetadataValue.text(sink_path),
                    "bytes_copied": dg.MetadataValue.int(bytes_copied),
                    "copy_mode": dg.MetadataValue.text("binary"),
                    "dry_run": dg.MetadataValue.bool(False),
                })

            # copy_mode == "dataframe"
            df = _read_dataframe(source_path, component.source_format)
            metadata: Dict[str, Any] = {
                "source_path": dg.MetadataValue.text(source_path),
                "num_rows": dg.MetadataValue.int(len(df)),
                "num_columns": dg.MetadataValue.int(len(df.columns)),
                "copy_mode": dg.MetadataValue.text("dataframe"),
                "dry_run": dg.MetadataValue.bool(False),
            }
            if component.sink_table:
                sink_result = _write_table_sink(context, df, component.sink_table)
                context.log.info(f"sink -> {sink_result['qualified']} ({sink_result['fast_path']}, {sink_result['mode']}, {sink_result['rows']} rows)")
                metadata.update({
                    "sink_table": dg.MetadataValue.text(sink_result["qualified"]),
                    "sink_rows": dg.MetadataValue.int(sink_result["rows"]),
                    "sink_mode": dg.MetadataValue.text(sink_result["mode"]),
                })
            else:
                sink_opts = _azure_storage_options(
                    sink_path, account_name_env_var=component.azure_account_name_env_var,
                    account_key_env_var=component.azure_account_key_env_var,
                    connection_string_env_var=component.azure_connection_string_env_var,
                )
                _write_dataframe(df, sink_path, component.sink_format, compression=component.sink_compression, storage_options=sink_opts)
                context.log.info(f"Wrote {len(df)} rows to {sink_path} ({component.sink_format})")
                metadata["sink_path"] = dg.MetadataValue.text(sink_path)
            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_cloud_storage_copy_asset])
