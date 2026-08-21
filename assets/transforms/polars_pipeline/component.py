"""PolarsPipelineComponent — single-asset multi-step polars LazyFrame pipeline.

Two YAML shapes are supported, both run inside a single Dagster asset and a
single polars query graph (so the optimizer can fuse + parallelize across
ALL ops, including across steps):

  (a) Flat shape (one source, one ops chain, one sink) — top-level `upstream_asset_key` +
      `operations`. One LazyFrame, one chain of ops:

      upstream_asset_key: raw_orders
      operations:
        - {op: filter, predicate: "status = 'paid'"}
        - {op: group_by, group_by: [region], aggregations: {revenue: {col: amount, agg: sum}}}

  (b) Multi-step `steps:` form — multiple sources (each from a Dagster
      upstream OR from an earlier step's output via `{kind: ref, ref: <id>}`),
      named outputs, an `op: sql` escape hatch, and an optional `sinks:`
      list (writes side-outputs to disk as parquet/csv).

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
from typing import Any, Dict, List, Optional, Tuple

import dagster as dg
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


_VALID_OPS = {"filter", "with_columns", "select", "drop", "rename",
              "group_by", "sort", "head", "tail", "head_per_group", "limit",
              "unique", "join", "drop_nulls", "fill_null", "cast", "sql"}


def _apply_partition_template(s: str, partition_key: Optional[str], partition: Optional[Dict[str, str]] = None) -> str:
    """Substitute `{partition_key}` and `{partition.<name>}` into a template string."""
    if not s or "{" not in s:
        return s
    out = s.replace("{partition_key}", str(partition_key or ""))
    for k, v in (partition or {}).items():
        out = out.replace("{" + f"partition.{k}" + "}", str(v))
    return out


_CLOUD_URL_SCHEMES = ("s3://", "gs://", "gcs://", "az://", "abfs://", "abfss://", "http://", "https://")


def _read_file_source(pl_module, src: Dict[str, Any], partition_key: Optional[str], partition_map: Optional[Dict[str, str]]):
    """Read `kind: file` or `kind: url` into a polars LazyFrame.

    Supported formats: json | ndjson | csv | parquet | ipc | avro.
    Auto-detects from the file extension when `format:` is unset.
    Path/url is `{partition_key}` / `{partition.<name>}` templated.

    Cloud URLs (s3:// / gs:// / az:// / abfs:// / http(s)://): pre-fetched
    via fsspec into a temp file, then read locally. This is format-agnostic
    and works for every polars reader without depending on version-specific
    cloud plugins. For very large files consider using
    `pl.scan_parquet(s3://...)` directly in a separate asset instead.
    """
    kind = src.get("kind", "file")
    raw_path = src.get("path") if kind == "file" else src.get("url")
    if not raw_path:
        raise ValueError(f"polars_pipeline source kind={kind!r} requires {'path' if kind == 'file' else 'url'}")
    path = _apply_partition_template(raw_path, partition_key, partition_map)
    fmt = (src.get("format") or "").lower()
    if not fmt:
        low = path.lower()
        if low.endswith(".ndjson") or low.endswith(".jsonl"):
            fmt = "ndjson"
        elif low.endswith(".json"):
            fmt = "json"
        elif low.endswith(".csv") or low.endswith(".tsv"):
            fmt = "csv"
        elif low.endswith(".parquet") or low.endswith(".pq"):
            fmt = "parquet"
        elif low.endswith(".ipc") or low.endswith(".arrow"):
            fmt = "ipc"
        elif low.endswith(".avro"):
            fmt = "avro"
        else:
            raise ValueError(
                f"polars_pipeline: can't infer format from {path!r} — set `format:` explicitly."
            )
    delimiter = src.get("delimiter", "\t" if path.lower().endswith(".tsv") else ",")

    # Cloud URL pre-fetch — universal for any format. `storage_options:`
    # in the source spec is forwarded to fsspec.open (e.g. anon: true,
    # endpoint_url: '...', key/secret pairs).
    is_cloud = any(path.lower().startswith(scheme) for scheme in _CLOUD_URL_SCHEMES)
    local_path = path
    if is_cloud:
        try:
            import fsspec
        except ImportError:
            raise ImportError(
                "polars_pipeline: reading cloud URLs requires fsspec + a backend "
                "(pip install 's3fs' | 'gcsfs' | 'adlfs')."
            )
        import tempfile as _tempfile
        storage_options = src.get("storage_options") or {}
        _tmp = _tempfile.NamedTemporaryFile(suffix=_suffix_for(fmt), delete=False)
        try:
            with fsspec.open(path, "rb", **storage_options) as fin:
                # Stream in chunks — file could be large.
                while True:
                    chunk = fin.read(8 * 1024 * 1024)  # 8 MiB
                    if not chunk:
                        break
                    _tmp.write(chunk)
        finally:
            _tmp.close()
        local_path = _tmp.name

    # scan_* returns LazyFrame (streaming-friendly); read_json is eager only.
    if fmt == "csv":
        return pl_module.scan_csv(local_path, separator=delimiter)
    if fmt == "parquet":
        return pl_module.scan_parquet(local_path)
    if fmt == "ndjson":
        return pl_module.scan_ndjson(local_path)
    if fmt == "json":
        return pl_module.read_json(local_path).lazy()
    if fmt == "ipc":
        return pl_module.scan_ipc(local_path)
    if fmt == "avro":
        return pl_module.read_avro(local_path).lazy()
    raise ValueError(f"polars_pipeline: format {fmt!r} not supported")


def _suffix_for(fmt: str) -> str:
    return {
        "csv": ".csv", "parquet": ".parquet", "ndjson": ".ndjson",
        "json": ".json", "ipc": ".ipc", "avro": ".avro",
    }.get(fmt, ".dat")


def _build_partitions_def(
    partition_type: Optional[str], partition_start: Optional[str],
    partition_values: Optional[Any], dynamic_partition_name: Optional[str],
    partition_dimensions: Optional[List[Dict[str, Any]]],
):
    """Strict partition-def builder — matches ml_pipeline's shape."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError(
            "polars_pipeline: set either partition_type OR partition_dimensions, not both."
        )
    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":  return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly": return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly": return HourlyPartitionsDefinition(start_date=spec["start"])
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
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start")
    if partition_type == "daily":   return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":  return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":  return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")

_PL_AGG_FUNCS = {"sum", "mean", "avg", "min", "max", "count", "median",
                 "std", "var", "first", "last", "nunique", "n_unique"}


def _pl_agg(pl_module, src_col: str, out_col: str, func: str):
    col = pl_module.col(src_col)
    f = func.lower()
    if f == "sum":     expr = col.sum()
    elif f in ("mean", "avg"): expr = col.mean()
    elif f == "min":   expr = col.min()
    elif f == "max":   expr = col.max()
    elif f == "count": expr = col.count()
    elif f == "median": expr = col.median()
    elif f == "std":   expr = col.std()
    elif f == "var":   expr = col.var()
    elif f == "first": expr = col.first()
    elif f == "last":  expr = col.last()
    elif f in ("nunique", "n_unique"): expr = col.n_unique()
    else:
        raise ValueError(f"agg func {func!r} not supported. Use one of {sorted(_PL_AGG_FUNCS)}")
    return expr.alias(out_col)


def _apply_op(pl, lf, op: Dict[str, Any], step_outputs: Dict[str, Any]):
    """Apply one op to a polars LazyFrame; returns the updated LazyFrame.

    step_outputs is the map of prior-step ids → their final LazyFrame, used
    by ops that reference other steps (join, sql).
    """
    kind = op["op"].lower()
    if kind == "filter":
        ctx = pl.SQLContext({"self": lf, **step_outputs})
        return ctx.execute(f"SELECT * FROM self WHERE {op['predicate']}", eager=False)
    if kind == "with_columns":
        # Two escape hatches:
        #   expressions: {col: "SQL EXPR"}      # via pl.sql_expr — SQL subset
        #   python_expressions: {col: "PY EXPR"} # via eval — full pl.* API
        # Both may appear in the same op; SQL first, then Python (Python can
        # reference columns that SQL just created).
        exprs = op.get("expressions") or {}
        py_exprs = op.get("python_expressions") or {}
        if not exprs and not py_exprs:
            raise ValueError(
                "with_columns needs either `expressions:` (SQL — parsed by "
                "polars.sql_expr) or `python_expressions:` (Python — eval'd "
                "with pl bound). See the polars SQL reference at "
                "https://docs.pola.rs/api/python/stable/reference/sql.html"
            )
        try:
            if exprs:
                lf = lf.with_columns([pl.sql_expr(e).alias(name) for name, e in exprs.items()])
        except Exception as e:  # noqa: BLE001
            raise ValueError(
                f"with_columns.expressions failed at SQL parse. Expressions "
                f"are polars-SQL (NOT the Python pl.* API). Common gotchas: "
                f"NOW() isn't in polars-SQL — use `python_expressions:` with "
                f"pl.lit(datetime.utcnow()) instead. Original error: {e}"
            )
        if py_exprs:
            # Restricted eval — namespace exposes polars + safe stdlib bits.
            # Users write pl.lit(datetime.utcnow()) style; no builtins.
            from datetime import datetime as _dt, timezone as _tz, date as _date, timedelta as _td
            _ns = {"pl": pl, "datetime": _dt, "timezone": _tz, "date": _date, "timedelta": _td}
            built_exprs = []
            for name, code in py_exprs.items():
                try:
                    e_obj = eval(code, {"__builtins__": {}}, _ns)  # noqa: S307
                except Exception as e:  # noqa: BLE001
                    raise ValueError(
                        f"python_expressions[{name!r}] eval failed: {e}. "
                        f"Namespace has: pl, datetime, timezone, date, timedelta. "
                        f"Example: 'pl.lit(datetime.utcnow())'"
                    )
                built_exprs.append(e_obj.alias(name))
            lf = lf.with_columns(built_exprs)
        return lf
    if kind == "select":
        return lf.select(op["columns"])
    if kind == "drop":
        return lf.drop(op["columns"])
    if kind == "rename":
        return lf.rename(op["mapping"])
    if kind == "group_by":
        group_by = op["group_by"]
        aggregations = op["aggregations"]
        aggs = []
        for out_col, spec in aggregations.items():
            if isinstance(spec, dict) and "col" in spec and "agg" in spec:
                aggs.append(_pl_agg(pl, spec["col"], out_col, spec["agg"]))
            else:
                aggs.append(_pl_agg(pl, out_col, out_col, spec))
        return lf.group_by(group_by).agg(aggs)
    if kind == "sort":
        return lf.sort(by=op["by"], descending=op.get("descending", False))
    if kind == "head" or kind == "limit":
        return lf.head(op["n"])
    if kind == "tail":
        return lf.tail(op["n"])
    if kind == "head_per_group":
        return lf.group_by(op["group_by"], maintain_order=True).head(op["n"])
    if kind == "unique":
        return lf.unique(
            subset=op.get("subset"),
            keep=op.get("keep", "first"),
            maintain_order=op.get("maintain_order", True),
        )
    if kind == "drop_nulls":
        return lf.drop_nulls(subset=op.get("subset"))
    if kind == "fill_null":
        return lf.fill_null(op.get("value", 0))
    if kind == "cast":
        mapping = op["mapping"]
        type_map = {name: getattr(pl, t) for name, t in mapping.items()}
        return lf.cast(type_map)
    if kind == "join":
        # Right side can be: {ref: <step_id>} (preferred) or a column name string isn't meaningful here.
        right_spec = op["right"]
        if not isinstance(right_spec, dict) or "ref" not in right_spec:
            raise ValueError("polars_pipeline join.right must be {ref: <step_id>}")
        right_id = right_spec["ref"]
        if right_id not in step_outputs:
            raise ValueError(f"join.right.ref={right_id!r} doesn't match any earlier step id")
        right_lf = step_outputs[right_id]
        how = op.get("how", "inner").lower()
        on_cols = op.get("on_columns") or op.get("on")
        if on_cols:
            return lf.join(right_lf, on=on_cols, how=how)
        left_on, right_on = op.get("left_on"), op.get("right_on")
        if left_on and right_on:
            return lf.join(right_lf, left_on=left_on, right_on=right_on, how=how)
        if how == "cross":
            return lf.join(right_lf, how="cross")
        raise ValueError("join op: provide 'on_columns' OR 'left_on' + 'right_on'")
    if kind == "sql":
        sql = op.get("sql")
        if not sql or not isinstance(sql, str):
            raise ValueError("op='sql' requires a non-empty 'sql' string")
        # `self` is the current chain; prior step ids are usable by name.
        ctx = pl.SQLContext({"self": lf, **step_outputs})
        return ctx.execute(sql, eager=False)
    raise ValueError(f"polars_pipeline op={kind!r} not supported. Valid: {sorted(_VALID_OPS)}")


class PolarsPipelineComponent(Component, Model, Resolvable):
    """Multi-step polars LazyFrame pipeline in a single Dagster asset.

    Two shapes:
      * Flat shape: `upstream_asset_key` + `operations`.
      * Multi-step: `steps:` (each with `source`/`operations`) plus optional
        `sinks:` for side-output parquet/csv writes. The asset's return
        value is the `primary_step`'s frame (default: last step).

    Supported ops: filter, with_columns, select, drop, rename, group_by,
    sort, head/limit, tail, head_per_group, unique, drop_nulls, fill_null,
    cast, join, sql.

    **Expression syntax is SQL, not Python.** Every string value under
    `filter.predicate`, `with_columns.expressions.<col>`, and `sort.by`
    is parsed via `polars.sql_expr()` — you write `"amount - tax"` or
    `"COALESCE(status, 'unknown')"`, NOT `pl.col('amount') - pl.col('tax')`.
    See the polars SQL reference:
      https://docs.pola.rs/api/python/stable/reference/sql.html
    for the supported function catalog. `CAST(x AS INT)`, `CASE WHEN`,
    `LIKE`, string / substring / concat / regex functions all work.

    **Escape hatch: `python_expressions:` on `with_columns`.** When the
    polars-SQL subset is too narrow — e.g. `NOW()` isn't in polars-SQL —
    use the Python API instead:

        operations:
          - op: with_columns
            expressions:                        # SQL — polars.sql_expr
              total: "amount * quantity"
            python_expressions:                 # Python — pl.* API
              ingest_ts: "pl.lit(datetime.utcnow())"
              partition_col: "pl.lit(context.partition_key)"

    Namespace exposes `pl` + `datetime` / `timezone` / `date` /
    `timedelta`. Both blocks may co-exist; SQL runs first, then Python
    (so Python expressions can reference SQL-created columns).

    `op: sql` uses polars's SQLContext where the current chain is available
    as `self` and earlier step outputs are available by id.
    """

    asset_name: str = Field(description="Output Dagster asset name")

    # Flat-shape single-source shape ------------------------------------------
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Top-level single-source shape: Dagster upstream asset key (pandas or polars DataFrame). Mutually exclusive with `source:`.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "File / URL source shape. `{kind: file, path, format?, delimiter?}` or "
            "`{kind: url, url, format?, delimiter?}` or `{kind: upstream_asset, "
            "upstream_asset_key}`. `path`/`url` support `{partition_key}` and "
            "`{partition.<name>}` templating. Format is inferred from extension "
            "(.json / .ndjson / .csv / .parquet / .ipc / .avro) when unset. "
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
            "Named steps. Each: {id, source: {kind: upstream|ref, upstream_asset_key|ref}, "
            "operations: [...]}."
        ),
    )
    sinks: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Optional side-output writes. Each: {from: <step_id>, kind: parquet|csv, path: '...'}. "
            "These run after the chain finishes; the asset's return value comes from primary_step."
        ),
    )
    primary_step: Optional[str] = Field(
        default=None,
        description="Step id whose frame is returned as the asset's output (default: last step).",
    )

    # Partitions ---------------------------------------------------------
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic' | 'multi' | None for unpartitioned.",
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
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'. Must match the sensor's `dynamic_partitions_name`.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec (list of {name, type, start, values, dynamic_partition_name} dicts). Set INSTEAD of partition_type for multi-dimensional partitioning.",
    )

    # Asset metadata + execution -----------------------------------------
    output_type: str = Field(
        default="polars",
        description="'polars' (default) or 'pandas' — what the asset returns.",
    )
    streaming: bool = Field(
        default=False,
        description="Use polars streaming engine for the final collect.",
    )
    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    include_preview_metadata: bool = Field(default=False)
    preview_rows: int = Field(default=25, ge=1, le=500)

    @classmethod
    def get_description(cls) -> str:
        return "Multi-step polars LazyFrame pipeline in a single Dagster asset (query-plan fusion across all steps)."

    def _normalize(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
        """Return (steps, sinks, upstream_keys).

        upstream_keys is the deduplicated list of Dagster upstream asset
        keys this asset depends on (collected from `upstream_asset_key`
        flat-shape field + any kind=upstream / kind=upstream_asset sources
        across steps). File / url source kinds don't produce upstream keys.
        """
        flat_upstream = bool(self.upstream_asset_key)
        flat_source = bool(self.source)
        flat_ops = self.operations is not None
        multi_present = bool(self.steps)

        if flat_upstream and flat_source:
            raise ValueError("polars_pipeline: set either `upstream_asset_key` OR `source:`, not both.")
        if multi_present and (flat_upstream or flat_source or flat_ops):
            raise ValueError(
                "polars_pipeline: choose ONE shape — top-level "
                "`upstream_asset_key`/`source:` + `operations`, OR `steps:`."
            )
        if multi_present:
            steps = list(self.steps or [])
            sinks = list(self.sinks or [])
            upstream_keys: List[str] = []
            for s in steps:
                src = s.get("source") or {}
                # Accept both 'upstream' (legacy) and 'upstream_asset' (matches ml_pipeline).
                if src.get("kind") in ("upstream", "upstream_asset"):
                    k = src.get("upstream_asset_key")
                    if not k:
                        raise ValueError(f"step {s.get('id')!r}: source kind=upstream needs 'upstream_asset_key'")
                    if k not in upstream_keys:
                        upstream_keys.append(k)
            return steps, sinks, upstream_keys

        if not flat_ops:
            raise ValueError(
                "polars_pipeline: provide either `steps:` OR top-level "
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
        # flat_source path (file / url / upstream_asset)
        src = dict(self.source or {})
        if src.get("kind") in ("upstream", "upstream_asset") and src.get("upstream_asset_key"):
            upstream_key = src["upstream_asset_key"]
            flat_step = {
                "id": "_default",
                "source": {"kind": "upstream", "upstream_asset_key": upstream_key},
                "operations": list(self.operations),
            }
            return [flat_step], flat_sinks, [upstream_key]
        # file / url — no upstream dependency
        flat_step = {
            "id": "_default",
            "source": src,
            "operations": list(self.operations),
        }
        return [flat_step], flat_sinks, []

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        steps, sinks, upstream_keys = self._normalize()
        # Validate each step's ops up front.
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
        output_type = self.output_type.lower()
        if output_type not in ("polars", "pandas"):
            raise ValueError(f"output_type must be 'polars' or 'pandas', got {self.output_type!r}")
        streaming = self.streaming
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        if not steps:
            raise ValueError("polars_pipeline: at least one step is required.")
        primary_step: str = str(self.primary_step or steps[-1].get("id") or "")
        if not primary_step:
            raise ValueError("polars_pipeline: primary_step could not be determined; every step must have an 'id'.")

        kinds = list(self.kinds or []) or ["polars"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        # One AssetIn per distinct upstream — input arg names get sanitized.
        ins = {
            f"upstream_{j}": AssetIn(key=AssetKey.from_user_string(k))
            for j, k in enumerate(upstream_keys)
        }
        upstream_arg_names = {k: f"upstream_{j}" for j, k in enumerate(upstream_keys)}

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )

        # Auto-detect required Dagster resource keys from table-kind sinks.
        required_resource_keys: set = set()
        for sink in sinks:
            if (sink.get("kind") or "parquet").lower() == "table":
                rk = sink.get("resource_key")
                if rk:
                    required_resource_keys.add(rk)

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
            required_resource_keys=required_resource_keys or None,
        )
        def _pipeline_asset(context: AssetExecutionContext, **upstreams: Any) -> Any:
            import polars as pl

            # Partition-key substitution values for file/url path templating.
            partition_key = context.partition_key if context.has_partition_key else None
            partition_map: Dict[str, str] = {}
            if partition_key is not None:
                pk = context.partition_key
                if hasattr(pk, "keys_by_dimension"):
                    partition_map = dict(pk.keys_by_dimension)

            def _to_lazy(obj: Any):
                if isinstance(obj, pl.LazyFrame):
                    return obj
                if isinstance(obj, pl.DataFrame):
                    return obj.lazy()
                return pl.from_pandas(obj).lazy()

            # step_outputs accumulates the LazyFrame at the END of each step.
            step_outputs: Dict[str, Any] = {}

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
                    lf = _to_lazy(upstreams[arg])
                elif src_kind in ("file", "url"):
                    lf = _read_file_source(pl, src, partition_key, partition_map)
                    context.log.info(
                        f"step {sid} source: read {src_kind} → {(src.get('path') or src.get('url'))!r}"
                    )
                elif src_kind == "ref":
                    ref = src.get("ref") or ""
                    if ref not in step_outputs:
                        raise ValueError(f"step {sid!r}: source ref={ref!r} not yet defined")
                    lf = step_outputs[ref]
                else:
                    raise ValueError(f"step {sid!r}: source.kind={src_kind!r} not supported. Use 'upstream' or 'ref'.")

                ops = step.get("operations") or []
                context.log.info(f"step {sid} ({s_idx + 1}/{len(steps)}): {len(ops)} op(s)")
                for op in ops:
                    lf = _apply_op(pl, lf, op, step_outputs)
                step_outputs[sid] = lf
                # Lazy-plan build time — the actual work happens at .collect()
                # on the primary step. This measures "how long did it take to
                # construct the query graph up through this step".
                step_timings[sid] = round(_time.time() - _t0, 4)
                step_op_counts[sid] = len(ops)

            # Collect the primary step. The polars planner fuses everything
            # touched by that final LazyFrame — including ops in earlier
            # steps it referenced.
            primary_lf = step_outputs[primary_step]
            # Capture the optimized plan BEFORE collect so we can surface it
            # as metadata even if collect fails. .explain() is cheap.
            try:
                explain_plan = str(primary_lf.explain(optimized=True))
            except Exception:  # noqa: BLE001
                try:
                    explain_plan = str(primary_lf.explain())
                except Exception:  # noqa: BLE001
                    explain_plan = "(explain unavailable)"
            _collect_t0 = _time.time()
            try:
                result_pl = primary_lf.collect(engine="streaming" if streaming else "auto")
            except TypeError:
                result_pl = primary_lf.collect(streaming=streaming) if streaming else primary_lf.collect()
            collect_seconds = round(_time.time() - _collect_t0, 4)

            # Run any side-output sinks.
            sink_metadata: Dict[str, Any] = {}
            for sink in sinks:
                from_id = sink.get("from") or ""
                if from_id not in step_outputs:
                    raise ValueError(f"sink.from={from_id!r} doesn't match any step id")
                kind = (sink.get("kind") or "parquet").lower()
                df = step_outputs[from_id].collect()  # materialize side-output

                if kind in ("parquet", "csv"):
                    path = sink.get("path")
                    if not path:
                        raise ValueError(f"sink from={from_id!r}: 'path' is required")
                    path = _apply_partition_template(path, partition_key, partition_map)
                    if kind == "parquet":
                        df.write_parquet(path)
                    else:
                        df.write_csv(path)
                    sink_metadata[f"polars/sink/{from_id}/path"] = MetadataValue.path(path)
                elif kind == "table":
                    # Write to any Dagster resource that exposes .get_engine() /
                    # .get_connection() (e.g. postgres_resource, duckdb_resource,
                    # snowflake_resource, ...). Mirrors ml_pipeline.table_sinks.
                    resource_key = sink.get("resource_key")
                    if not resource_key:
                        raise ValueError(f"sink from={from_id!r} kind=table: 'resource_key' is required")
                    table = _apply_partition_template(sink.get("table") or "", partition_key, partition_map)
                    if not table:
                        raise ValueError(f"sink from={from_id!r} kind=table: 'table' is required")
                    schema = sink.get("schema")
                    if_exists = sink.get("if_exists", "append")
                    partition_col = sink.get("partition_column")

                    # Add partition_column with the partition_key value — analytics-friendly
                    # single-table pattern (WHERE partition_date = ...).
                    if partition_key and partition_col:
                        df = df.with_columns(pl.lit(str(partition_key)).alias(partition_col))

                    resource = getattr(context.resources, resource_key)
                    if hasattr(resource, "get_engine"):
                        engine = resource.get_engine()
                    elif hasattr(resource, "get_connection"):
                        engine = resource.get_connection()
                    else:
                        raise ValueError(
                            f"resource {resource_key!r} must expose .get_engine() or .get_connection()"
                        )
                    df.to_pandas().to_sql(
                        table, engine, schema=schema, if_exists=if_exists, index=False,
                    )
                    sink_metadata[f"polars/sink/{from_id}/table"] = MetadataValue.text(
                        f"{schema+'.' if schema else ''}{table}"
                    )
                    sink_metadata[f"polars/sink/{from_id}/resource_key"] = MetadataValue.text(resource_key)
                    context.log.info(
                        f"table sink {from_id!r} → {schema+'.' if schema else ''}{table} "
                        f"(via {resource_key}, {if_exists}, {df.height} rows)"
                    )
                else:
                    raise ValueError(
                        f"sink.kind={kind!r} not supported. Use 'parquet' | 'csv' | 'table'."
                    )
                sink_metadata[f"polars/sink/{from_id}/row_count"] = MetadataValue.int(df.height)

            row_count = result_pl.height
            _meta_df = result_pl.to_pandas()
            from dagster import TableSchema, TableColumn
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(_meta_df.dtypes[col]))
                for col in _meta_df.columns
            ])
            # Estimated memory footprint of the materialized DataFrame.
            try:
                mem_bytes = int(result_pl.estimated_size())
            except Exception:  # noqa: BLE001
                mem_bytes = 0
            metadata: Dict[str, Any] = {
                "dagster/row_count": MetadataValue.int(row_count),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                "polars/step_count": MetadataValue.int(len(steps)),
                "polars/primary_step": MetadataValue.text(primary_step or ""),
                "polars/streaming": MetadataValue.bool(streaming),
                "polars/collect_seconds": MetadataValue.float(collect_seconds),
                "polars/estimated_bytes": MetadataValue.int(mem_bytes),
                "polars/estimated_mb": MetadataValue.float(round(mem_bytes / (1024 * 1024), 3)),
                "polars/step_timings_seconds": MetadataValue.json(step_timings),
                "polars/step_op_counts": MetadataValue.json(step_op_counts),
                "polars/explain": MetadataValue.md(f"```\n{explain_plan}\n```"),
            }
            metadata.update(sink_metadata)
            if include_preview and row_count > 0:
                try:
                    _prev = _meta_df.sample(min(preview_rows, row_count)) if row_count > preview_rows * 10 else _meta_df.head(preview_rows)
                    metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")
            context.add_output_metadata(metadata)

            return result_pl if output_type == "polars" else result_pl.to_pandas()

        return Definitions(assets=[_pipeline_asset])
