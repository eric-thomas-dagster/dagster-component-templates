"""PandasPipelineComponent — single-asset multi-step pandas DataFrame pipeline.

The pandas-engine sibling to `polars_pipeline` — same YAML shape, same op
menu, same source/sink kinds, same partitioning, so a team already
standardized on pandas can use this pattern without picking up polars as a
new dependency. `polars_pipeline` remains the better choice when the
lazy-query-plan-fusion / parallel-execution story matters (large data,
many chained ops) — this component executes each op eagerly, one at a
time, which is simpler to reason about and debug but doesn't get that
fusion. See `assets/transforms/polars_pipeline/README.md` for that
performance-tradeoff writeup.

Two YAML shapes, both run inside a single Dagster asset:

  (a) Flat shape (one source, one ops chain) — top-level `upstream_asset_key` +
      `operations`:

      upstream_asset_key: raw_orders
      operations:
        - {op: filter, predicate: "status = 'paid'"}
        - {op: group_by, group_by: [region], aggregations: {revenue: {col: amount, agg: sum}}}

  (b) Multi-step `steps:` form — multiple sources (each from a Dagster
      upstream OR from an earlier step's output via `{kind: ref, ref: <id>}`),
      named outputs, an `op: sql` escape hatch (DuckDB over the in-memory
      pandas frames), and an optional `sinks:` list.

      steps:
        - id: paid_orders
          source: {kind: upstream, upstream_asset_key: raw_orders}
          operations:
            - {op: filter, predicate: "status = 'paid'"}
        - id: gold_customers
          source: {kind: upstream, upstream_asset_key: raw_customers}
          operations:
            - {op: filter, predicate: "tier = 'gold'"}
        - id: enriched
          source: {kind: ref, ref: paid_orders}
          operations:
            - {op: join, right: {ref: gold_customers}, on_columns: [customer_id], how: inner}
            - {op: sql, sql: "SELECT *, amount * 0.15 AS commission FROM self"}
            - {op: group_by, group_by: [region],
               aggregations: {revenue: {col: amount, agg: sum}}}
      sinks:
        - {from: enriched, kind: parquet, path: "./out/enriched.parquet"}
      # The asset's RETURN value is the last step's frame (or `primary_step`
      # if you name one explicitly).
"""
import base64
import json
import os
from datetime import date, datetime
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import dagster as dg
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    BackfillPolicy,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field, model_validator


_VALID_OPS = {"filter", "with_columns", "select", "drop", "rename",
              "group_by", "sort", "head", "tail", "head_per_group", "limit",
              "unique", "join", "union", "drop_nulls", "fill_null", "cast", "sql"}

# Ops not needing a SQL/aggregation engine at all stay pure pandas. filter,
# with_columns(expressions=), group_by, sql, and union(mode) route through a
# DuckDB connection (already a dependency elsewhere in this repo — see
# `context_engineering_pipeline`/`automl_asset`/others) because DuckDB reads
# a pandas DataFrame natively (`conn.register(name, df)`), zero-copy, and
# gives the exact same "SQL expression, not Python" ergonomics
# `polars_pipeline` already established — someone fluent in one component's
# YAML doesn't have to relearn the expression language in the other.

_PD_AGG_FUNCS = {"sum", "mean", "avg", "min", "max", "count", "median",
                 "std", "var", "first", "last", "nunique", "n_unique"}

_AGG_NAME_MAP = {"avg": "mean", "n_unique": "nunique"}


def _apply_partition_template(s: str, partition_key: Optional[str], partition: Optional[Dict[str, str]] = None) -> str:
    """Substitute `{partition_key}` and `{partition.<name>}` into a template string."""
    if not s or "{" not in s:
        return s
    out = s.replace("{partition_key}", str(partition_key or ""))
    for k, v in (partition or {}).items():
        out = out.replace("{" + f"partition.{k}" + "}", str(v))
    return out


def _is_uri(path: str) -> bool:
    """True if `path` looks like a URI (has a `scheme://` prefix)."""
    return "://" in path


def _suffix_for(fmt: str) -> str:
    return {"csv": ".csv", "parquet": ".parquet", "ndjson": ".ndjson", "json": ".json"}.get(fmt, ".dat")


def _infer_format(path: str) -> str:
    low = path.lower()
    if low.endswith(".ndjson") or low.endswith(".jsonl"):
        return "ndjson"
    if low.endswith(".json"):
        return "json"
    if low.endswith(".csv") or low.endswith(".tsv"):
        return "csv"
    if low.endswith(".parquet") or low.endswith(".pq"):
        return "parquet"
    raise ValueError(
        f"pandas_pipeline: can't infer format from {path!r} — set `format:` explicitly. "
        f"Supported: csv | json | ndjson | parquet. (polars_pipeline additionally supports "
        f"ipc/avro via polars' arrow-native readers — pandas_pipeline does not, since those "
        f"formats aren't a natural pandas fit; use polars_pipeline if you need them.)"
    )


def _validate_dtype_overrides(dtype_overrides: Optional[Dict[str, str]], label: str) -> Optional[Dict[str, str]]:
    """Validate pandas dtype-name strings in a `dtype_overrides` mapping via pandas' own
    canonical dtype resolver (`pd.api.types.pandas_dtype`) -- so any dtype name the installed
    pandas version itself recognizes (e.g. "object"/"string", "Int64", "float64", "boolean")
    works here, with no separate name->dtype table in this component to fall out of sync with
    a pandas release. Raises a clear ValueError naming the offending column + dtype on a typo."""
    if not dtype_overrides:
        return None
    for col, dtype_name in dtype_overrides.items():
        try:
            pd.api.types.pandas_dtype(dtype_name)
        except TypeError as e:
            raise ValueError(
                f"{label}: dtype_overrides[{col!r}] names an unknown pandas dtype "
                f"{dtype_name!r} ({e})"
            ) from e
    return dict(dtype_overrides)


def _read_bytes_to_df(
    data: bytes,
    fmt: str,
    delimiter: str,
    has_header: bool = True,
    quote_char: Optional[str] = None,
    dtype_overrides: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    import io
    buf = io.BytesIO(data)
    if fmt == "csv":
        csv_kwargs: Dict[str, Any] = {"sep": delimiter, "header": 0 if has_header else None}
        if quote_char is not None:
            csv_kwargs["quotechar"] = quote_char
        if dtype_overrides:
            csv_kwargs["dtype"] = dtype_overrides
        return pd.read_csv(buf, **csv_kwargs)
    if fmt == "parquet":
        return pd.read_parquet(buf)
    if fmt == "ndjson":
        return pd.read_json(buf, lines=True)
    if fmt == "json":
        return pd.read_json(buf)
    raise ValueError(f"pandas_pipeline: format {fmt!r} not supported")


def _read_file_source(src: Dict[str, Any], partition_key: Optional[str], partition_map: Optional[Dict[str, str]]) -> pd.DataFrame:
    """Read `kind: file` or `kind: url` into a pandas DataFrame.

    Supported formats: csv | json | ndjson | parquet. Auto-detects from the
    file extension when `format:` is unset. Path/url is `{partition_key}` /
    `{partition.<name>}` templated.

    CSV-only options -- matching an Azure Data Factory Copy activity's declared-type source
    read, which this component's own default schema INFERENCE cannot reproduce on its own
    (e.g. a column of zero-padded codes like "001" silently becomes the integer 1): `has_header`
    (bool, default true; pandas' own `header=0`/`header=None` convention), `quote_char` (pandas'
    own `quotechar` kwarg), `dtype_overrides` ({column: pandas dtype name as a string, e.g.
    "object"/"string"/"Int64"/"float64"} -- pins specific columns' types instead of letting
    inference guess; validated via pandas' own `pd.api.types.pandas_dtype` resolver, so any
    dtype the installed pandas version itself recognizes works here).

    Cloud URLs (s3:// / gs:// / az:// / abfs:// / http(s)://): pre-fetched via
    fsspec into a temp file, then read locally — same mechanism
    `polars_pipeline` uses, for the same reason (format-agnostic, works
    without a version-specific cloud plugin per reader).
    """
    kind = src.get("kind", "file")
    raw_path = src.get("path") if kind == "file" else src.get("url")
    if not raw_path:
        raise ValueError(f"pandas_pipeline source kind={kind!r} requires {'path' if kind == 'file' else 'url'}")
    path = _apply_partition_template(raw_path, partition_key, partition_map)
    fmt = (src.get("format") or "").lower() or _infer_format(path)
    delimiter = src.get("delimiter", "\t" if path.lower().endswith(".tsv") else ",")
    has_header = src.get("has_header", True)
    quote_char = src.get("quote_char")
    dtype_overrides = _validate_dtype_overrides(
        src.get("dtype_overrides"), f"pandas_pipeline source kind={kind!r}"
    )

    local_path = path
    if _is_uri(path):
        try:
            import fsspec
        except ImportError:
            raise ImportError(
                "pandas_pipeline: reading cloud URLs requires fsspec + a backend "
                "(pip install 's3fs' | 'gcsfs' | 'adlfs')."
            )
        import tempfile as _tempfile
        storage_options = src.get("storage_options") or {}
        _tmp = _tempfile.NamedTemporaryFile(suffix=_suffix_for(fmt), delete=False)
        try:
            with fsspec.open(path, "rb", **storage_options) as fin:
                while True:
                    chunk = fin.read(8 * 1024 * 1024)
                    if not chunk:
                        break
                    _tmp.write(chunk)
        finally:
            _tmp.close()
        local_path = _tmp.name

    if fmt == "csv":
        csv_kwargs: Dict[str, Any] = {"sep": delimiter, "header": 0 if has_header else None}
        if quote_char is not None:
            csv_kwargs["quotechar"] = quote_char
        if dtype_overrides:
            csv_kwargs["dtype"] = dtype_overrides
        return pd.read_csv(local_path, **csv_kwargs)
    if fmt == "parquet":
        return pd.read_parquet(local_path)
    if fmt == "ndjson":
        return pd.read_json(local_path, lines=True)
    if fmt == "json":
        return pd.read_json(local_path)
    raise ValueError(f"pandas_pipeline: format {fmt!r} not supported")


# BEGIN file-level AWS Encryption SDK mechanism (inlined per component; see
# transforms/field_encryption/component.py for the cell-level sibling this
# was adapted from — same library, same master-key-provider patterns,
# different granularity: this encrypts/decrypts a whole file's raw bytes,
# not individual DataFrame cells. Interop with any existing StreamSets
# "Encrypt and Decrypt Fields"-produced ciphertext is UNVERIFIED for the
# same reason documented there — do not assume compatibility without a
# real sample + stage config to check against.)
def _build_key_provider(key_source: str, raw_key_env_var: Optional[str], kms_key_ids, key_id: str):
    import aws_encryption_sdk
    from aws_encryption_sdk.identifiers import WrappingAlgorithm, EncryptionKeyType
    from aws_encryption_sdk.internal.crypto.wrapping_keys import WrappingKey
    from aws_encryption_sdk.key_providers.raw import RawMasterKeyProvider

    if key_source == "raw_key":
        raw_key_b64 = os.environ.get(raw_key_env_var or "", "")
        if not raw_key_b64:
            raise ValueError(f"raw_key_env_var {raw_key_env_var!r} is unset or empty.")
        raw_key_bytes = base64.b64decode(raw_key_b64)

        class _StaticRawMasterKeyProvider(RawMasterKeyProvider):
            provider_id = "pandas-pipeline-file-encryption-raw"

            def __new__(cls, *args, **kwargs):
                return super(RawMasterKeyProvider, cls).__new__(cls)

            def __init__(self_mk, **kwargs):
                RawMasterKeyProvider.__init__(self_mk, **kwargs)

            def _get_raw_key(self_mk, key_id):
                return WrappingKey(
                    wrapping_algorithm=WrappingAlgorithm.AES_256_GCM_IV12_TAG16_NO_PADDING,
                    wrapping_key=raw_key_bytes,
                    wrapping_key_type=EncryptionKeyType.SYMMETRIC,
                )

        provider = _StaticRawMasterKeyProvider()
        provider.add_master_key(key_id)
        return provider
    kms_ids = kms_key_ids if isinstance(kms_key_ids, list) else [kms_key_ids]
    return aws_encryption_sdk.StrictAwsKmsMasterKeyProvider(key_ids=kms_ids)


def _encrypt_file_bytes(plaintext: bytes, key_source, raw_key_env_var, kms_key_ids, key_id, encryption_context) -> bytes:
    import aws_encryption_sdk
    client = aws_encryption_sdk.EncryptionSDKClient(
        commitment_policy=aws_encryption_sdk.CommitmentPolicy.REQUIRE_ENCRYPT_REQUIRE_DECRYPT
    )
    key_provider = _build_key_provider(key_source, raw_key_env_var, kms_key_ids, key_id)
    ciphertext, _header = client.encrypt(
        source=plaintext, key_provider=key_provider, encryption_context=dict(encryption_context or {}),
    )
    return ciphertext


def _decrypt_file_bytes(ciphertext: bytes, key_source, raw_key_env_var, kms_key_ids, key_id, encryption_context) -> bytes:
    import aws_encryption_sdk
    client = aws_encryption_sdk.EncryptionSDKClient(
        commitment_policy=aws_encryption_sdk.CommitmentPolicy.REQUIRE_ENCRYPT_REQUIRE_DECRYPT
    )
    key_provider = _build_key_provider(key_source, raw_key_env_var, kms_key_ids, key_id)
    plaintext, header = client.decrypt(source=ciphertext, key_provider=key_provider)
    expected = dict(encryption_context or {})
    for k, v in expected.items():
        if header.encryption_context.get(k) != v:
            raise ValueError(
                f"encryption_context mismatch: expected {k}={v!r}, got {header.encryption_context.get(k)!r}"
            )
    return plaintext
# END file-level AWS Encryption SDK mechanism


def _build_partitions_def(
    partition_type: Optional[str], partition_start: Optional[str],
    partition_values: Optional[Any], dynamic_partition_name: Optional[str],
    partition_dimensions: Optional[List[Dict[str, Any]]],
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    """Strict partition-def builder — identical shape to polars_pipeline's
    (same field names/behavior), inlined fresh per this repo's house rule."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError(
            "pandas_pipeline: set either partition_type OR partition_dimensions, not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":  return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
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
                raise ValueError("partition dimension type='static' requires 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
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
        raise ValueError(f"partition_type={partition_type!r} requires partition_start")
    if partition_type == "daily":   return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":  return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":  return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError("partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only.")
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _duckdb_conn_with(self_df: pd.DataFrame, step_outputs: Dict[str, pd.DataFrame]):
    import duckdb
    conn = duckdb.connect(":memory:")
    conn.register("self", self_df)
    for sid, sdf in step_outputs.items():
        conn.register(sid, sdf)
    return conn


def _apply_op(df: pd.DataFrame, op: Dict[str, Any], step_outputs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Apply one op to a pandas DataFrame; returns the updated DataFrame.

    step_outputs is the map of prior-step ids → their final DataFrame, used
    by ops that reference other steps (join, union, sql) — same role as in
    polars_pipeline.
    """
    kind = op["op"].lower()
    if kind == "filter":
        predicate = op["predicate"]
        conn = _duckdb_conn_with(df, step_outputs)
        try:
            return conn.execute(f"SELECT * FROM self WHERE {predicate}").fetchdf()
        finally:
            conn.close()
    if kind == "with_columns":
        exprs = op.get("expressions") or {}
        py_exprs = op.get("python_expressions") or {}
        if not exprs and not py_exprs:
            raise ValueError(
                "with_columns needs either `expressions:` (SQL, via DuckDB) or "
                "`python_expressions:` (Python — eval'd with pd bound, vectorized "
                "over the whole DataFrame)."
            )
        if exprs:
            select_cols = ["*"] + [f"({e}) AS {name}" for name, e in exprs.items()]
            conn = _duckdb_conn_with(df, step_outputs)
            try:
                df = conn.execute(f"SELECT {', '.join(select_cols)} FROM self").fetchdf()
            except Exception as e:  # noqa: BLE001
                raise ValueError(
                    f"with_columns.expressions failed at SQL parse (DuckDB SQL, not "
                    f"the Python pandas API). Original error: {e}"
                )
            finally:
                conn.close()
        if py_exprs:
            # Restricted eval — namespace exposes pd + safe stdlib bits. Each
            # expression is evaluated ONCE and assigned to the whole column
            # (vectorized), e.g. 'pd.Timestamp.utcnow()' or
            # "df['amount'] * df['quantity']".
            from datetime import datetime as _dt, timezone as _tz, date as _date, timedelta as _td
            _ns = {"pd": pd, "df": df, "datetime": _dt, "timezone": _tz, "date": _date, "timedelta": _td}
            for name, code in py_exprs.items():
                try:
                    df[name] = eval(code, {"__builtins__": {}}, _ns)  # noqa: S307
                except Exception as e:  # noqa: BLE001
                    raise ValueError(
                        f"python_expressions[{name!r}] eval failed: {e}. "
                        f"Namespace has: pd, df, datetime, timezone, date, timedelta. "
                        f"Example: \"df['amount'] * df['quantity']\" or 'pd.Timestamp.utcnow()'"
                    )
                _ns["df"] = df
        return df
    if kind == "select":
        return df[op["columns"]]
    if kind == "drop":
        return df.drop(columns=op["columns"])
    if kind == "rename":
        return df.rename(columns=op["mapping"])
    if kind == "group_by":
        group_by = op["group_by"]
        aggregations = op["aggregations"]
        agg_kwargs = {}
        for out_col, spec in aggregations.items():
            if isinstance(spec, dict) and "col" in spec and "agg" in spec:
                src_col, func = spec["col"], spec["agg"]
            else:
                src_col, func = out_col, spec
            func = func.lower()
            if func not in _PD_AGG_FUNCS:
                raise ValueError(f"agg func {func!r} not supported. Use one of {sorted(_PD_AGG_FUNCS)}")
            agg_kwargs[out_col] = pd.NamedAgg(column=src_col, aggfunc=_AGG_NAME_MAP.get(func, func))
        # sort=False mirrors polars_pipeline's maintain_order=True — this
        # pipeline's own head/limit/sort steps can run right after a
        # group_by in the same config-driven chain, and an unsorted
        # head/limit right after group_by needs row order preserved.
        return df.groupby(group_by, sort=False, as_index=False).agg(**agg_kwargs)
    if kind == "sort":
        return df.sort_values(by=op["by"], ascending=not op.get("descending", False))
    if kind == "head" or kind == "limit":
        return df.head(op["n"])
    if kind == "tail":
        return df.tail(op["n"])
    if kind == "head_per_group":
        return df.groupby(op["group_by"], sort=False, group_keys=False).head(op["n"])
    if kind == "unique":
        return df.drop_duplicates(subset=op.get("subset"), keep=op.get("keep", "first"))
    if kind == "drop_nulls":
        return df.dropna(subset=op.get("subset"))
    if kind == "fill_null":
        return df.fillna(op.get("value", 0))
    if kind == "cast":
        mapping = op["mapping"]
        df = df.copy()
        for col, t in mapping.items():
            tl = t.lower()
            if tl in ("datetime", "timestamp", "date"):
                df[col] = pd.to_datetime(df[col])
            elif tl in ("int", "int64", "integer"):
                df[col] = pd.to_numeric(df[col], errors="raise").astype("Int64")
            elif tl in ("float", "float64", "double"):
                df[col] = pd.to_numeric(df[col], errors="raise").astype("float64")
            elif tl in ("str", "string", "utf8"):
                df[col] = df[col].astype("string")
            elif tl in ("bool", "boolean"):
                df[col] = df[col].astype("boolean")
            else:
                df[col] = df[col].astype(t)
        return df
    if kind == "join":
        right_spec = op["right"]
        if not isinstance(right_spec, dict) or "ref" not in right_spec:
            raise ValueError("pandas_pipeline join.right must be {ref: <step_id>}")
        right_id = right_spec["ref"]
        if right_id not in step_outputs:
            raise ValueError(f"join.right.ref={right_id!r} doesn't match any earlier step id")
        right_df = step_outputs[right_id]
        how = op.get("how", "inner").lower()
        on_cols = op.get("on_columns") or op.get("on")
        left_on, right_on = op.get("left_on"), op.get("right_on")
        # pandas .merge() doesn't have semi/anti joins natively — implement
        # as filters (left columns only, no right-side columns added),
        # matching the shape polars_pipeline's semi/anti produce.
        if how in ("semi", "anti"):
            key_left = on_cols or left_on
            key_right = on_cols or right_on
            if not key_left or not key_right:
                raise ValueError("join op: semi/anti require 'on_columns' OR 'left_on' + 'right_on'")
            mask = df[key_left].apply(tuple, axis=1).isin(right_df[key_right].apply(tuple, axis=1))
            return df[mask] if how == "semi" else df[~mask]
        pd_how = {"full": "outer"}.get(how, how)
        if how == "cross":
            return df.merge(right_df, how="cross")
        if on_cols:
            return df.merge(right_df, on=on_cols, how=pd_how)
        if left_on and right_on:
            return df.merge(right_df, left_on=left_on, right_on=right_on, how=pd_how)
        raise ValueError("join op: provide 'on_columns' OR 'left_on' + 'right_on'")
    if kind == "union":
        other = op.get("other")
        if not isinstance(other, dict) or "ref" not in other:
            raise ValueError("pandas_pipeline union.other must be {ref: <step_id>}")
        other_id = other["ref"]
        if other_id not in step_outputs:
            raise ValueError(f"union.other.ref={other_id!r} doesn't match any earlier step id")
        other_df = step_outputs[other_id]
        mode = (op.get("mode") or "by_name").lower()
        if mode not in ("by_name", "by_position"):
            raise ValueError(f"union op: mode must be 'by_name' or 'by_position', got {mode!r}")
        if mode == "by_name":
            # pd.concat aligns by column label already, filling missing
            # columns with NaN — matches polars_pipeline's by_name mode.
            out = pd.concat([df, other_df], ignore_index=True)
        else:
            # by_position: take column order from the left side, rename the
            # right side positionally to match, then stack.
            other_renamed = other_df.copy()
            other_renamed.columns = list(df.columns[: len(other_renamed.columns)]) + list(other_renamed.columns[len(df.columns):])
            out = pd.concat([df, other_renamed], ignore_index=True)
        if op.get("distinct", False):
            out = out.drop_duplicates()
        return out
    if kind == "sql":
        sql = op.get("sql")
        if not sql or not isinstance(sql, str):
            raise ValueError("op='sql' requires a non-empty 'sql' string")
        conn = _duckdb_conn_with(df, step_outputs)
        try:
            return conn.execute(sql).fetchdf()
        finally:
            conn.close()
    raise ValueError(f"pandas_pipeline op={kind!r} not supported. Valid: {sorted(_VALID_OPS)}")


# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
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
    return out
# END field-shape normalization


class PandasPipelineComponent(Component, Model, Resolvable):
    """Multi-step pandas DataFrame pipeline in a single Dagster asset.

    The pandas-engine sibling to `PolarsPipelineComponent` — same YAML
    shape, same op menu, same source/sink kinds, same partitioning
    (`partition_dimensions` builds a `MultiPartitionsDefinition` exactly
    like its sibling), so a `(bank, batch_day)`-style multi-dimensional
    partition works identically in either component. Pick `polars_pipeline`
    when the lazy-query-plan-fusion / parallel-execution story matters;
    pick this one when your team already standardizes on pandas, or wants
    simpler, more linear step-by-step debugging (each op runs eagerly).

    Two shapes:
      * Flat shape: `upstream_asset_key` + `operations`.
      * Multi-step: `steps:` (each with `source`/`operations`) plus optional
        `sinks:` for side-output parquet/csv/ndjson/table/encrypted_file
        writes. The asset's return value is the `primary_step`'s frame
        (default: last step).

    Supported ops: filter, with_columns, select, drop, rename, group_by,
    sort, head/limit, tail, head_per_group, unique, join, union,
    drop_nulls, fill_null, cast, sql.

    **Expression syntax is SQL, not Python** for `filter.predicate`,
    `with_columns.expressions.<col>`, and `op: sql` — evaluated via an
    in-memory DuckDB connection with the current frame registered as
    `self` and every prior step's frame registered by its step id
    (DuckDB reads a pandas DataFrame natively via `.register()`, zero-copy
    — this is the same "SQL expression, not Python" ergonomics
    `polars_pipeline` uses via its own SQLContext, just backed by DuckDB
    instead of polars' SQL engine).

    **Escape hatch: `python_expressions:` on `with_columns`.** For anything
    DuckDB's SQL dialect can't express — use the Python/pandas API instead:

        operations:
          - op: with_columns
            expressions:                        # SQL — DuckDB
              total: "amount * quantity"
            python_expressions:                 # Python — pandas API, vectorized
              ingest_ts: "pd.Timestamp.utcnow()"
              commission: "df['amount'] * 0.15"

    Namespace exposes `pd` + `df` (the frame so far) + `datetime` /
    `timezone` / `date` / `timedelta`. Both blocks may co-exist; SQL runs
    first, then Python (so Python expressions can reference SQL-created
    columns).

    **Format parity with `polars_pipeline`**: `file`/`url` source kinds
    support csv/json/ndjson/parquet — NOT ipc/avro (polars' arrow-native
    formats aren't a natural pandas fit; use `polars_pipeline` if you need
    those two). CSV reads additionally take `has_header`/`quote_char`/
    `dtype_overrides` declared-type options (pandas' own kwarg names/dtype
    strings — see `_read_file_source`'s docstring). `sinks:` additionally
    support `kind: ndjson` (pandas' native `to_json(orient="records",
    lines=True)` / `read_json(..., lines=True)`), with `mode: overwrite|
    append`.

    **`encrypted_file` source/sink**: file-level AWS Encryption SDK
    encrypt/decrypt (raw-key or KMS), same mechanism as
    `transforms/field_encryption`'s cell-level encryption but operating on
    a whole file's bytes before/after it becomes a DataFrame. See that
    component's module docstring for the same StreamSets-ciphertext-
    interop caveat — it applies here too (this component's own ciphertext
    round-trips correctly; reading ciphertext another tool already
    produced is a different, unverified claim).
    """

    asset_name: str = Field(description="Output Dagster asset name")

    # Flat-shape single-source shape ------------------------------------------
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Top-level single-source shape: Dagster upstream asset key (pandas or polars DataFrame). Mutually exclusive with `source:`.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "File / URL / encrypted-file source shape. `{kind: file, path, format?, delimiter?, "
            "has_header?, quote_char?, dtype_overrides?}` or `{kind: url, url, format?, delimiter?, "
            "has_header?, quote_char?, dtype_overrides?}` (CSV only: has_header default true, "
            "quote_char default pandas' own '\"', dtype_overrides: {column: pandas dtype name e.g. "
            "'object'/'string'/'Int64'/'float64'} -- pins a column's type instead of letting pandas' "
            "own schema inference guess, e.g. a zero-padded code column like '001' silently becoming "
            "the integer 1) or `{kind: upstream_asset, upstream_asset_key}` or "
            "`{kind: encrypted_file, path, key_source, ..., format?, has_header?, quote_char?, "
            "dtype_overrides?}` (see the encrypted_file fields below). `path`/`url` support "
            "`{partition_key}` and `{partition.<name>}` templating. "
            "Format is inferred from extension (.json / .ndjson / .csv / .parquet) when unset. "
            "Mutually exclusive with `upstream_asset_key`."
        ),
    )
    operations: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Flat shape: ordered list of ops applied to upstream_asset_key OR source. Compiles to one anonymous step.",
    )

    # Multi-step shape -----------------------------------------------------
    steps: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Named steps. Each: {id, source: {kind: upstream|ref|file|url|encrypted_file, ...}, "
            "operations: [...]}."
        ),
    )
    sinks: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Optional side-output writes. Each: {from: <step_id>, kind: parquet|csv|ndjson|table|encrypted_file, "
            "path/table: '...'}. `kind: ndjson` additionally takes `mode: overwrite|append` (append "
            "reads-concats-rewrites; single-writer, not concurrent-safe). "
            "These run after the chain finishes; the asset's return value comes from primary_step."
        ),
    )
    primary_step: Optional[str] = Field(
        default=None,
        description="Step id whose frame is returned as the asset's output (default: last step).",
    )

    # Encrypted-file config (shared by `source: {kind: encrypted_file}` and
    # `sinks: [{kind: encrypted_file}]` — field names mirror
    # `FieldEncryptionComponent`'s for registry-wide consistency) ---------
    key_source: str = Field(
        default="raw_key",
        description=(
            "'raw_key' (a user-supplied symmetric key via AWS ESDK's RawMasterKeyProvider) or "
            "'kms' (an AWS KMS CMK via AWS ESDK's StrictAwsKmsMasterKeyProvider). Only used when "
            "a source or sink kind is 'encrypted_file'."
        ),
    )
    raw_key_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var holding a base64-encoded 256-bit (32-byte) symmetric key. Required when "
            "key_source='raw_key' and an encrypted_file source/sink is used. Generate one with: "
            "python3 -c \"import os,base64; print(base64.b64encode(os.urandom(32)).decode())\""
        ),
    )
    kms_key_ids: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="KMS key ARN(s) for StrictAwsKmsMasterKeyProvider. Required when key_source='kms'.",
    )
    key_id: str = Field(
        default="pandas-pipeline-file-key",
        description="Logical wrapping-key identifier for raw_key mode. Must match between the encrypt run and the decrypt run.",
    )
    encryption_context: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional authenticated data (AAD) for the AWS Encryption SDK. Must match exactly between encrypt and decrypt.",
    )

    # Partitions ---------------------------------------------------------
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic' | None for unpartitioned.",
    )
    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only. Built on Dagster's "
            "TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'.",
    )
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron partitions "
            "definition. 0 (Dagster's default) means a time-based partition set only ever holds "
            "COMPLETED windows. Set to 1 to include the current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC).",
    )
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' materializes an "
            "entire backfill range in one run. 'multi_run' launches one run per partition "
            "(optionally capped via backfill_policy_max_partitions_per_run)."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="ISO date for time-based partition types (daily/weekly/monthly/hourly).",
    )
    partition_values: Optional[Any] = Field(
        default=None,
        description="Comma-separated string OR list — the fixed partition keys for static/multi partitioning.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec (list of {name, type, start, values, dynamic_partition_name} dicts). Set INSTEAD of partition_type for multi-dimensional partitioning — e.g. a (bank, batch_day) shape.",
    )

    # Asset metadata + execution -----------------------------------------
    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Asset-level metadata dict — passed straight through to `@asset(metadata=...)`.",
    )
    include_preview_metadata: bool = Field(default=False)
    preview_rows: int = Field(default=25, ge=1, le=500)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('kinds', 'owners', 'deps', 'kms_key_ids'), csv=(), tags=('asset_tags', 'encryption_context'))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    @classmethod
    def get_description(cls) -> str:
        return "Multi-step pandas DataFrame pipeline in a single Dagster asset — the pandas-engine sibling to polars_pipeline."

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Pandas Pipeline', editable=True)

    def _normalize(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
        """Return (steps, sinks, upstream_keys) — identical shape/logic to polars_pipeline's."""
        flat_upstream = bool(self.upstream_asset_key)
        flat_source = bool(self.source)
        flat_ops = self.operations is not None
        multi_present = bool(self.steps)

        if flat_upstream and flat_source:
            raise ValueError("pandas_pipeline: set either `upstream_asset_key` OR `source:`, not both.")
        if multi_present and (flat_upstream or flat_source or flat_ops):
            raise ValueError(
                "pandas_pipeline: choose ONE shape — top-level "
                "`upstream_asset_key`/`source:` + `operations`, OR `steps:`."
            )
        if multi_present:
            steps = list(self.steps or [])
            sinks = list(self.sinks or [])
            upstream_keys: List[str] = []
            for s in steps:
                src = s.get("source") or {}
                if src.get("kind") in ("upstream", "upstream_asset"):
                    k = src.get("upstream_asset_key")
                    if not k:
                        raise ValueError(f"step {s.get('id')!r}: source kind=upstream needs 'upstream_asset_key'")
                    if k not in upstream_keys:
                        upstream_keys.append(k)
            return steps, sinks, upstream_keys

        if not flat_ops:
            raise ValueError(
                "pandas_pipeline: provide either `steps:` OR top-level "
                "`operations:` (with `upstream_asset_key` OR `source:`)."
            )
        flat_sinks = list(self.sinks or [])
        if flat_upstream:
            flat_step = {
                "id": "_default",
                "source": {"kind": "upstream", "upstream_asset_key": self.upstream_asset_key},
                "operations": list(self.operations),
            }
            return [flat_step], flat_sinks, [self.upstream_asset_key]
        src = dict(self.source or {})
        if src.get("kind") in ("upstream", "upstream_asset") and src.get("upstream_asset_key"):
            upstream_key = src["upstream_asset_key"]
            flat_step = {
                "id": "_default",
                "source": {"kind": "upstream", "upstream_asset_key": upstream_key},
                "operations": list(self.operations),
            }
            return [flat_step], flat_sinks, [upstream_key]
        flat_step = {"id": "_default", "source": src, "operations": list(self.operations)}
        return [flat_step], flat_sinks, []

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        steps, sinks, upstream_keys = self._normalize()
        for s in steps:
            for i, op in enumerate(s.get("operations") or []):
                if not isinstance(op, dict) or "op" not in op:
                    raise ValueError(f"step {s.get('id')!r} op #{i + 1}: each op must be a dict with an 'op' key")
                if op["op"].lower() not in _VALID_OPS:
                    raise ValueError(
                        f"step {s.get('id')!r} op #{i + 1}: op={op['op']!r} not supported. "
                        f"Valid: {sorted(_VALID_OPS)}"
                    )

        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        if not steps:
            raise ValueError("pandas_pipeline: at least one step is required.")
        primary_step: str = str(self.primary_step or steps[-1].get("id") or "")
        if not primary_step:
            raise ValueError("pandas_pipeline: primary_step could not be determined; every step must have an 'id'.")

        if self.key_source not in ("raw_key", "kms"):
            raise ValueError(f"key_source must be 'raw_key' or 'kms', got {self.key_source!r}")

        kinds = list(self.kinds or []) or ["pandas"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        ins = {
            f"upstream_{j}": AssetIn(key=AssetKey.from_user_string(k))
            for j, k in enumerate(upstream_keys)
        }
        upstream_arg_names = {k: f"upstream_{j}" for j, k in enumerate(upstream_keys)}

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
            end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            backfill_policy = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            backfill_policy = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        required_resource_keys: set = set()
        for sink in sinks:
            if (sink.get("kind") or "parquet").lower() == "table":
                rk = sink.get("resource_key")
                if rk:
                    required_resource_keys.add(rk)

        key_source, raw_key_env_var, kms_key_ids, key_id, encryption_context = (
            self.key_source, self.raw_key_env_var, self.kms_key_ids, self.key_id, self.encryption_context,
        )

        @asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=all_tags,
            group_name=self.group_name,
            ins=ins,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            kinds=set(kinds),
            partitions_def=partitions_def,
            backfill_policy=backfill_policy,
            required_resource_keys=required_resource_keys or None,
            metadata=self.metadata or {},
        )
        def _pipeline_asset(context: AssetExecutionContext, **upstreams: Any) -> pd.DataFrame:
            if context.has_partition_key:
                partition_key = context.partition_key
            elif context.has_partition_key_range:
                partition_key = ",".join(str(k) for k in context.partition_keys)
            else:
                partition_key = None
            partition_map: Dict[str, str] = {}
            if context.has_partition_key:
                pk = context.partition_key
                if hasattr(pk, "keys_by_dimension"):
                    partition_map = dict(pk.keys_by_dimension)

            def _to_pandas(obj: Any) -> pd.DataFrame:
                if isinstance(obj, pd.DataFrame):
                    return obj
                # polars DataFrame/LazyFrame upstream — convert without a
                # hard polars import dependency unless actually needed.
                if hasattr(obj, "to_pandas"):
                    return obj.to_pandas()
                if hasattr(obj, "collect"):
                    return obj.collect().to_pandas()
                raise ValueError(f"pandas_pipeline: don't know how to convert upstream of type {type(obj).__name__} to a pandas DataFrame")

            step_outputs: Dict[str, pd.DataFrame] = {}

            import time as _time
            step_timings: Dict[str, float] = {}
            step_op_counts: Dict[str, int] = {}
            for s_idx, step in enumerate(steps):
                sid = step["id"]
                _t0 = _time.time()
                src = step.get("source") or {}
                src_kind = src.get("kind", "upstream")
                if src_kind in ("upstream", "upstream_asset"):
                    uk = src.get("upstream_asset_key") or ""
                    arg = upstream_arg_names[uk]
                    df = _to_pandas(upstreams[arg])
                elif src_kind in ("file", "url"):
                    df = _read_file_source(src, partition_key, partition_map)
                    context.log.info(f"step {sid} source: read {src_kind} → {(src.get('path') or src.get('url'))!r}")
                elif src_kind == "encrypted_file":
                    path = _apply_partition_template(src.get("path") or "", partition_key, partition_map)
                    if not path:
                        raise ValueError(f"step {sid!r}: source kind=encrypted_file requires 'path'")
                    fmt = (src.get("format") or "").lower() or _infer_format(path)
                    delimiter = src.get("delimiter", ",")
                    with open(path, "rb") as f:
                        ciphertext = f.read()
                    plaintext = _decrypt_file_bytes(ciphertext, key_source, raw_key_env_var, kms_key_ids, key_id, encryption_context)
                    df = _read_bytes_to_df(
                        plaintext, fmt, delimiter,
                        has_header=src.get("has_header", True),
                        quote_char=src.get("quote_char"),
                        dtype_overrides=_validate_dtype_overrides(
                            src.get("dtype_overrides"), f"step {sid!r} source kind=encrypted_file"
                        ),
                    )
                    context.log.info(f"step {sid} source: decrypted + read encrypted_file → {path!r}")
                elif src_kind == "ref":
                    ref = src.get("ref") or ""
                    if ref not in step_outputs:
                        raise ValueError(f"step {sid!r}: source ref={ref!r} not yet defined")
                    df = step_outputs[ref]
                else:
                    raise ValueError(f"step {sid!r}: source.kind={src_kind!r} not supported.")

                ops = step.get("operations") or []
                context.log.info(f"step {sid} ({s_idx + 1}/{len(steps)}): {len(ops)} op(s)")
                for op in ops:
                    df = _apply_op(df, op, step_outputs)
                step_outputs[sid] = df
                step_timings[sid] = round(_time.time() - _t0, 4)
                step_op_counts[sid] = len(ops)

            result_df = step_outputs[primary_step]

            sink_metadata: Dict[str, Any] = {}
            for sink in sinks:
                from_id = sink.get("from") or ""
                if from_id not in step_outputs:
                    raise ValueError(f"sink.from={from_id!r} doesn't match any step id")
                kind = (sink.get("kind") or "parquet").lower()
                sdf = step_outputs[from_id]

                if kind in ("parquet", "csv", "ndjson"):
                    path = sink.get("path")
                    if not path:
                        raise ValueError(f"sink from={from_id!r}: 'path' is required")
                    path = _apply_partition_template(path, partition_key, partition_map)
                    if kind == "ndjson":
                        # kind: ndjson closes a real gap: writing JSON output otherwise needed a
                        # second asset even though the encrypted_file sink already supported
                        # format: ndjson for the encrypted path. mode: overwrite|append mirrors
                        # polars_pipeline's own ndjson sink -- pandas has no native append for
                        # ndjson, so append reads the existing file, concatenates, and rewrites
                        # (single-writer assumption, not concurrent-write-safe).
                        mode = (sink.get("mode") or "overwrite").lower()
                        if mode not in ("overwrite", "append"):
                            raise ValueError(
                                f"sink from={from_id!r}: mode must be 'overwrite' or 'append', got {mode!r}"
                            )
                        if mode == "append" and os.path.exists(path):
                            existing = pd.read_json(path, orient="records", lines=True)
                            sdf = pd.concat([existing, sdf], ignore_index=True)
                        sdf.to_json(path, orient="records", lines=True)
                        if mode == "append":
                            sink_metadata[f"pandas/sink/{from_id}/mode"] = MetadataValue.text("append")
                    elif kind == "parquet":
                        sdf.to_parquet(path, index=False)
                    else:
                        sdf.to_csv(path, index=False)
                    sink_metadata[f"pandas/sink/{from_id}/path"] = MetadataValue.path(path)
                elif kind == "encrypted_file":
                    path = sink.get("path")
                    if not path:
                        raise ValueError(f"sink from={from_id!r} kind=encrypted_file: 'path' is required")
                    path = _apply_partition_template(path, partition_key, partition_map)
                    fmt = (sink.get("format") or "").lower() or _infer_format(path)
                    import io
                    buf = io.BytesIO()
                    if fmt == "csv":
                        sdf.to_csv(buf, index=False)
                    elif fmt == "parquet":
                        sdf.to_parquet(buf, index=False)
                    elif fmt in ("json", "ndjson"):
                        buf.write(sdf.to_json(orient="records", lines=(fmt == "ndjson")).encode("utf-8"))
                    else:
                        raise ValueError(f"encrypted_file sink: format {fmt!r} not supported")
                    plaintext = buf.getvalue()
                    ciphertext = _encrypt_file_bytes(plaintext, key_source, raw_key_env_var, kms_key_ids, key_id, encryption_context)
                    with open(path, "wb") as f:
                        f.write(ciphertext)
                    sink_metadata[f"pandas/sink/{from_id}/path"] = MetadataValue.path(path)
                elif kind == "table":
                    resource_key = sink.get("resource_key")
                    if not resource_key:
                        raise ValueError(f"sink from={from_id!r} kind=table: 'resource_key' is required")
                    table = _apply_partition_template(sink.get("table") or "", partition_key, partition_map)
                    if not table:
                        raise ValueError(f"sink from={from_id!r} kind=table: 'table' is required")
                    schema = sink.get("schema")
                    if_exists = sink.get("if_exists", "append")
                    partition_col = sink.get("partition_column")
                    mode = (sink.get("mode") or "").lower() or None
                    match_cols: List[str] = list(sink.get("match") or [])
                    if mode == "upsert_on_match" and not match_cols:
                        raise ValueError(f"sink from={from_id!r} mode=upsert_on_match requires 'match: [col, ...]'")

                    if partition_key and partition_col:
                        sdf = sdf.copy()
                        sdf[partition_col] = str(partition_key)

                    resource = getattr(context.resources, resource_key)
                    from contextlib import nullcontext

                    def _acquire():
                        if hasattr(resource, "get_connection"):
                            gc = resource.get_connection()
                            return gc if hasattr(gc, "__enter__") else nullcontext(gc)
                        if hasattr(resource, "get_engine"):
                            eng = resource.get_engine()
                            return eng if hasattr(eng, "__enter__") else nullcontext(eng)
                        raise ValueError(f"resource {resource_key!r} must expose .get_connection() or .get_engine()")

                    qualified = f"{schema}.{table}" if schema else table
                    with _acquire() as conn:
                        if hasattr(conn, "register") and hasattr(conn, "execute") and hasattr(conn, "unregister"):
                            conn.register("_pandas_sink_batch", sdf)
                            try:
                                if mode == "upsert_on_match":
                                    conn.execute(f"CREATE TABLE IF NOT EXISTS {qualified} AS SELECT * FROM _pandas_sink_batch WHERE 1=0")
                                    match_tuple = ", ".join(match_cols)
                                    conn.execute("BEGIN TRANSACTION")
                                    try:
                                        conn.execute(f"DELETE FROM {qualified} WHERE ({match_tuple}) IN (SELECT DISTINCT {match_tuple} FROM _pandas_sink_batch)")
                                        conn.execute(f"INSERT INTO {qualified} SELECT * FROM _pandas_sink_batch")
                                        conn.execute("COMMIT")
                                    except Exception:
                                        conn.execute("ROLLBACK")
                                        raise
                                    sink_metadata[f"pandas/sink/{from_id}/mode"] = MetadataValue.text(f"upsert_on_match({','.join(match_cols)})")
                                elif if_exists == "replace":
                                    conn.execute(f"CREATE OR REPLACE TABLE {qualified} AS SELECT * FROM _pandas_sink_batch")
                                else:
                                    conn.execute(f"CREATE TABLE IF NOT EXISTS {qualified} AS SELECT * FROM _pandas_sink_batch WHERE 1=0")
                                    conn.execute("BEGIN TRANSACTION")
                                    try:
                                        conn.execute(f"INSERT INTO {qualified} SELECT * FROM _pandas_sink_batch")
                                        conn.execute("COMMIT")
                                    except Exception:
                                        conn.execute("ROLLBACK")
                                        raise
                                sink_metadata[f"pandas/sink/{from_id}/fast_path"] = MetadataValue.text("duckdb-register")
                            finally:
                                try:
                                    conn.unregister("_pandas_sink_batch")
                                except Exception:  # noqa: BLE001
                                    pass
                        else:
                            if mode == "upsert_on_match":
                                match_tuple = ", ".join(match_cols)
                                distinct = sdf[match_cols].drop_duplicates()
                                if len(distinct) > 0:
                                    from sqlalchemy import text as _sa_text
                                    placeholders = ", ".join(
                                        "(" + ", ".join(f":v{i}_{j}" for j in range(len(match_cols))) + ")"
                                        for i in range(len(distinct))
                                    )
                                    params = {}
                                    for i, row in enumerate(distinct.itertuples(index=False)):
                                        for j, v in enumerate(row):
                                            params[f"v{i}_{j}"] = v
                                    tx = conn.begin() if hasattr(conn, "begin") else nullcontext()
                                    with tx:
                                        conn.execute(_sa_text(f"DELETE FROM {qualified} WHERE ({match_tuple}) IN ({placeholders})"), params)
                                        sdf.to_sql(table, conn, schema=schema, if_exists="append", index=False)
                                else:
                                    sdf.to_sql(table, conn, schema=schema, if_exists="append", index=False)
                                sink_metadata[f"pandas/sink/{from_id}/mode"] = MetadataValue.text(f"upsert_on_match({','.join(match_cols)})")
                            else:
                                sdf.to_sql(table, conn, schema=schema, if_exists=if_exists, index=False)
                            sink_metadata[f"pandas/sink/{from_id}/fast_path"] = MetadataValue.text("sqlalchemy-to_sql")

                    sink_metadata[f"pandas/sink/{from_id}/table"] = MetadataValue.text(qualified)
                    sink_metadata[f"pandas/sink/{from_id}/resource_key"] = MetadataValue.text(resource_key)
                    context.log.info(f"table sink {from_id!r} → {qualified} (via {resource_key}, {if_exists}, {len(sdf)} rows)")
                else:
                    raise ValueError(f"sink.kind={kind!r} not supported. Use 'parquet' | 'csv' | 'ndjson' | 'table' | 'encrypted_file'.")
                sink_metadata[f"pandas/sink/{from_id}/row_count"] = MetadataValue.int(len(sdf))

            row_count = len(result_df)
            from dagster import TableSchema, TableColumn
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(result_df.dtypes[col]))
                for col in result_df.columns
            ])
            try:
                mem_bytes = int(result_df.memory_usage(deep=True).sum())
            except Exception:  # noqa: BLE001
                mem_bytes = 0
            metadata: Dict[str, Any] = {
                "dagster/row_count": MetadataValue.int(row_count),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                "pandas/step_count": MetadataValue.int(len(steps)),
                "pandas/primary_step": MetadataValue.text(primary_step or ""),
                "pandas/estimated_bytes": MetadataValue.int(mem_bytes),
                "pandas/estimated_mb": MetadataValue.float(round(mem_bytes / (1024 * 1024), 3)),
                "pandas/step_timings_seconds": MetadataValue.json(step_timings),
                "pandas/step_op_counts": MetadataValue.json(step_op_counts),
            }
            metadata.update(sink_metadata)
            if include_preview and row_count > 0:
                try:
                    _prev = result_df.sample(min(preview_rows, row_count)) if row_count > preview_rows * 10 else result_df.head(preview_rows)
                    metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")
            context.add_output_metadata(metadata)

            return result_df

        return Definitions(assets=[_pipeline_asset])
