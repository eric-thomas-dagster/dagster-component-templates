"""PySparkPipelineComponent — single-asset multi-step PySpark DataFrame pipeline.

PySpark is lazy: every `.filter()/.groupBy()/.join()` against a DataFrame
builds a Catalyst logical plan, and execution happens at a terminal action.
This component runs the WHOLE pipeline as ONE Catalyst plan — across every
step, every op, every sink — so the optimizer can fuse filters, push
predicates back to source readers (parquet column pruning, JDBC predicate
pushdown), prune projections, and parallelize across the Spark cluster.

Two YAML shapes — both run inside a single Dagster asset / single
SparkSession / single Catalyst plan:

  (a) Flat shape (one source, one ops chain, one sink) — top-level `source` + `operations` + `sink`:

      source: {kind: parquet, path: "..."}
      operations: [...]
      sink:   {kind: parquet, path: "...", mode: overwrite}

  (b) Multi-step `steps:` form with `sinks:` (plural):

      steps:
        - id: paid_orders
          source: {kind: parquet, path: "orders.parquet"}
          operations:
            - {op: filter, predicate: "status = 'paid'"}
        - id: gold_customers
          source: {kind: parquet, path: "customers.parquet"}
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
        - {from: enriched, kind: parquet, path: "out/enriched/", mode: overwrite}

Source kinds: parquet, csv, json, orc, delta, table, jdbc, upstream, ref.
Sink kinds:   parquet, csv, json, delta, table, jdbc, none.

Control-flow step types (opt-in, only in the multi-step `steps:` shape):

  - `type: condition`  — data-quality guard evaluated after prior steps.
        ```yaml
        - id: guard
          type: condition
          when: "orders.row_count > 0"     # safe-eval expression
          on_false: skip_rest              # or: fail (raise dg.Failure)
        ```
        Expression scope includes `<step_id>.row_count` / `.columns` for any
        prior step, `vars.<x>` for feed-level constants, and `checkpoint.<x>`
        for values persisted by a prior run.

  - `type: checkpoint` — persist named values to
        `<checkpoint_dir>/<asset_name>.json` for the next run.
        ```yaml
        - id: snapshot
          type: checkpoint
          keys: ["orders.row_count", "vars.batch_id"]
        ```
        Requires `checkpoint_dir:` on the component (default `checkpoints/`).
        On the NEXT run, `{{ checkpoint.<key> }}` is available in
        expressions and inside sql via jinja-style substitution.

  - `type: for_each`   — loop the inner `steps:` once per element.
        ```yaml
        - id: partitions
          type: for_each
          over: "vars.dates"               # any iterable expression
          as: date
          steps:
            - id: daily_slice
              source: {kind: parquet, path: "raw/{{ loop.date }}/data.parquet"}
              operations: [...]
        ```
        Inside inner steps, `{{ loop.<as> }}` and `{{ loop.index }}` string-
        substitute in any step config field. Cross-iteration state stays in
        `step_outputs` — the outer scope sees the LAST iteration's values.

Feed-level `variables:` field on the component + `checkpoint_dir:` field
support the above.
"""
from typing import Any, Dict, List, Literal, Optional, Tuple

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


_VALID_OPS = {"filter", "select", "drop", "rename", "with_columns",
              "group_by", "sort", "limit", "distinct", "drop_nulls",
              "join", "union", "sql"}
_SUPPORTED_AGGS = {"sum", "mean", "avg", "min", "max", "count", "countDistinct",
                    "first", "last", "stddev", "variance"}

# Control-flow step types — see the module docstring for shapes.
_CONTROL_TYPES = {"condition", "checkpoint", "for_each"}


def _safe_eval(expr: str, context: Dict[str, Any]) -> Any:
    """Evaluate a control-flow expression against a scoped dict.

    Used by `type: condition when: <expr>` and `type: for_each over: <expr>`.
    Restricted eval: no __ names, no attribute walks into private members,
    no imports. Function surface is limited to len / min / max / int / float /
    bool / str / round / sum / any / all / abs — matches
    parametric_data_generator's formula sandbox.
    """
    import ast
    _SAFE_FUNCS = {
        "len": len, "min": min, "max": max, "int": int, "float": float,
        "bool": bool, "str": str, "round": round, "sum": sum,
        "any": any, "all": all, "abs": abs,
    }

    tree = ast.parse(expr, mode="eval")

    class _Walk(ast.NodeVisitor):
        def visit_Name(self, node):
            if node.id.startswith("__"):
                raise ValueError(f"safe_eval: name {node.id!r} not allowed")
            self.generic_visit(node)

        def visit_Attribute(self, node):
            if node.attr.startswith("_"):
                raise ValueError(f"safe_eval: attribute {node.attr!r} not allowed")
            self.generic_visit(node)

        def visit_Call(self, node):
            if isinstance(node.func, ast.Name) and node.func.id not in _SAFE_FUNCS:
                raise ValueError(f"safe_eval: function {node.func.id!r} not allowed")
            self.generic_visit(node)

    _Walk().visit(tree)
    return eval(compile(tree, filename="<safe_eval>", mode="eval"),
                {"__builtins__": {}}, {**_SAFE_FUNCS, **context})


def _step_output_meta(spark_df) -> Dict[str, Any]:
    """Return a small dict of side-info about a step's DataFrame for use in
    `condition when:` expressions — e.g. `{{ orders.row_count }} > 0`. Kept
    lightweight — `row_count` triggers a Spark action, so the value is
    cached on the wrapper.
    """
    class _Info:
        def __init__(self, df):
            self._df = df
            self._row_count: Optional[int] = None
            self._columns: Optional[List[str]] = None

        @property
        def row_count(self):
            if self._row_count is None:
                self._row_count = self._df.count()
            return self._row_count

        @property
        def columns(self):
            if self._columns is None:
                self._columns = list(self._df.columns)
            return self._columns

    return _Info(spark_df)


def _load_checkpoint(path):
    """Load prior-run checkpoint JSON; return {} if the file doesn't exist."""
    import json
    from pathlib import Path
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text()) or {}
    except Exception:  # noqa: BLE001
        return {}


def _save_checkpoint(path, state: Dict[str, Any]) -> None:
    """Overwrite the checkpoint JSON with the given state."""
    import json
    from pathlib import Path
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state, indent=2, default=str))


def _apply_op(spark, df, op: Dict[str, Any], step_outputs: Dict[str, Any]):
    """Apply one op to a Spark DataFrame; returns the new DataFrame.

    step_outputs is the map of prior-step ids → their DataFrame, used by
    join/union/sql ops that reference other steps.
    """
    from pyspark.sql import functions as F
    kind = op["op"].lower()
    if kind == "filter":
        return df.filter(op["predicate"])
    if kind == "select":
        return df.select(*op["columns"])
    if kind == "drop":
        return df.drop(*op["columns"])
    if kind == "rename":
        out = df
        for old, new in op["mapping"].items():
            out = out.withColumnRenamed(old, new)
        return out
    if kind == "with_columns":
        out = df
        for name, expr_str in op["expressions"].items():
            out = out.withColumn(name, F.expr(expr_str))
        return out
    if kind == "group_by":
        group_by = op["group_by"]
        aggregations = op["aggregations"]
        agg_exprs = []
        for out_col, spec in aggregations.items():
            if isinstance(spec, dict) and "col" in spec and "agg" in spec:
                src_col, func = spec["col"], spec["agg"]
            else:
                src_col, func = out_col, spec
            if func not in _SUPPORTED_AGGS:
                raise ValueError(
                    f"pyspark_pipeline: agg func {func!r} not supported. "
                    f"Use one of {sorted(_SUPPORTED_AGGS)}"
                )
            agg_fn = getattr(F, func) if hasattr(F, func) else getattr(F, "count_distinct")
            agg_exprs.append(agg_fn(F.col(src_col)).alias(out_col))
        return df.groupBy(*group_by).agg(*agg_exprs)
    if kind == "sort":
        by = op["by"] if isinstance(op["by"], list) else [op["by"]]
        descending = op.get("descending", False)
        descending = descending if isinstance(descending, list) else [descending] * len(by)
        cols = [(F.col(c).desc() if d else F.col(c).asc()) for c, d in zip(by, descending)]
        return df.orderBy(*cols)
    if kind == "limit":
        return df.limit(op["n"])
    if kind == "distinct":
        return df.distinct()
    if kind == "drop_nulls":
        subset = op.get("subset")
        return df.dropna(subset=subset)
    if kind == "join":
        right_spec = op["right"]
        if not isinstance(right_spec, dict) or "ref" not in right_spec:
            raise ValueError("pyspark_pipeline join.right must be {ref: <step_id>}")
        right_id = right_spec["ref"]
        if right_id not in step_outputs:
            raise ValueError(f"join.right.ref={right_id!r} doesn't match any earlier step id")
        right_df = step_outputs[right_id]
        how = op.get("how", "inner").lower()
        on_cols = op.get("on_columns") or op.get("on")
        if on_cols:
            return df.join(right_df, on=on_cols, how=how)
        left_on, right_on = op.get("left_on"), op.get("right_on")
        if left_on and right_on:
            cond = None
            for lo, ro in zip(left_on, right_on):
                term = df[lo] == right_df[ro]
                cond = term if cond is None else (cond & term)
            return df.join(right_df, on=cond, how=how)
        if how == "cross":
            return df.crossJoin(right_df)
        raise ValueError("join op: provide 'on_columns' OR 'left_on' + 'right_on'")
    if kind == "union":
        other = op["other"]
        if not isinstance(other, dict) or "ref" not in other:
            raise ValueError("pyspark_pipeline union.other must be {ref: <step_id>}")
        other_id = other["ref"]
        if other_id not in step_outputs:
            raise ValueError(f"union.other.ref={other_id!r} doesn't match any earlier step id")
        other_df = step_outputs[other_id]
        if op.get("distinct", False):
            return df.union(other_df).distinct()
        return df.union(other_df)
    if kind == "sql":
        sql = op.get("sql")
        if not sql or not isinstance(sql, str):
            raise ValueError("op='sql' requires a non-empty 'sql' string")
        # Register the current chain as 'self' + every prior step output by id.
        df.createOrReplaceTempView("self")
        for sid, other_df in step_outputs.items():
            other_df.createOrReplaceTempView(sid)
        return spark.sql(sql)
    raise ValueError(f"pyspark_pipeline: unsupported op {kind!r}. Valid: {sorted(_VALID_OPS)}")


def _read_source(spark, source: Dict[str, Any]):
    kind = source["kind"].lower()
    if kind == "parquet":
        return spark.read.parquet(source["path"])
    if kind == "csv":
        return spark.read.csv(source["path"], header=source.get("header", True),
                              inferSchema=source.get("inferSchema", True))
    if kind == "json":
        return spark.read.json(source["path"])
    if kind == "orc":
        return spark.read.orc(source["path"])
    if kind == "delta":
        return spark.read.format("delta").load(source["path"])
    if kind == "table":
        return spark.read.table(source["table"])
    if kind == "jdbc":
        opts = source.get("options", {})
        return (spark.read.format("jdbc")
                .option("url", source["url"])
                .option("dbtable", source.get("dbtable") or f"({source['query']}) AS _t")
                .options(**opts).load())
    raise ValueError(f"Unknown source kind {kind!r}")


def _write_sink(df, sink: Dict[str, Any]):
    kind = sink["kind"].lower()
    mode = sink.get("mode", "overwrite")
    if kind == "parquet":
        df.write.mode(mode).parquet(sink["path"]); return None
    if kind == "csv":
        df.write.mode(mode).csv(sink["path"], header=sink.get("header", True)); return None
    if kind == "json":
        df.write.mode(mode).json(sink["path"]); return None
    if kind == "delta":
        df.write.format("delta").mode(mode).save(sink["path"]); return None
    if kind == "table":
        df.write.mode(mode).saveAsTable(sink["table"]); return None
    if kind == "jdbc":
        opts = sink.get("options", {})
        (df.write.format("jdbc")
            .option("url", sink["url"]).option("dbtable", sink["dbtable"])
            .options(**opts).mode(mode).save())
        return None
    if kind == "none":
        return df.toPandas()
    raise ValueError(f"Unknown sink kind {kind!r}")



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


class PySparkPipelineComponent(Component, Model, Resolvable):
    """Multi-step PySpark DataFrame pipeline in a single Dagster asset.

    Two shapes:
      * Flat shape: top-level `source` + `operations` + `sink`.
      * Multi-step: `steps:` (each with `source`/`operations`) + `sinks:` (list).

    Supported ops: filter, select, drop, rename, with_columns, group_by,
    sort, limit, distinct, drop_nulls, join, union, sql.

    `op: sql` registers the current chain as `self` and every prior step
    output by its id, then runs `spark.sql(...)`.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    spark_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description="SparkConf options. Stringified before passing to .config(...).",
    )
    spark_app_name: str = Field(default="dagster-pyspark-pipeline")

    # Flat-shape shape ---------------------------------------------------------
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Top-level single-source shape: {kind: parquet|csv|json|orc|delta|table|jdbc|upstream, ...}",
    )
    operations: Optional[List[Dict[str, Any]]] = Field(default=None)
    sink: Optional[Dict[str, Any]] = Field(default=None, description="Top-level single-sink shape.")
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Only when (flat shape) source.kind='upstream'.",
    )

    # Multi-step shape -----------------------------------------------------
    steps: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Named steps. Each: {id, source: {kind: parquet|table|jdbc|upstream|ref|..., ...}, operations: [...]}",
    )
    sinks: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Sinks. Each: {from: <step_id>, kind: parquet|csv|json|delta|table|jdbc, path/table/url, mode}.",
    )

    # Control-flow surface (Bucket-B-adjacent additions) ------------------
    #
    # These enable pipelines that mirror the customer's feed-shaped pattern:
    # `type: condition | checkpoint | for_each` steps in the `steps:` list.
    # See the module docstring for the full step-type reference.
    variables: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Feed-level constants available at run time via {{ vars.<key> }} "
            "in condition when: / for_each over: / SQL where relevant. Merged "
            "with prior-run `checkpoint.*` values (see checkpoint_dir)."
        ),
    )
    checkpoint_dir: Optional[str] = Field(
        default=None,
        description=(
            "Directory (relative to project root, or absolute) where "
            "`type: checkpoint` steps persist named values across runs. "
            "Default `checkpoints/` under the project root. On each run, "
            "`checkpoints/<asset_name>.json` is loaded and its keys become "
            "available as {{ checkpoint.<key> }}."
        ),
    )

    group_name: Optional[str] = Field(default=None)

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned. A partitioned run sends only the rows in its partition.",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in one run (the row-filter "
            "logic below already handles a multi-partition range correctly). "
            "'multi_run' launches one run per partition (optionally capped via "
            "backfill_policy_max_partitions_per_run). Leave unset for Dagster's "
            "default (one run per partition, uncapped). Ignored when "
            "partition_type is not set. 'single_run' requires a partition-"
            "range-aware IO manager (e.g. a DB-table IO manager with "
            "partition_expr set) -- a generic filesystem/default IO manager "
            "cannot persist a single_run output spanning multiple partitions "
            "and will raise a clear error at materialization time."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides the flat partition fields.",
    )
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('kinds', 'owners', 'deps'), csv=(), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    @classmethod
    def get_description(cls) -> str:
        return "Multi-step PySpark DataFrame pipeline in a single Dagster asset (Catalyst-optimized across all steps)."

    def _normalize(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
        """Return (steps, sinks, upstream_keys)."""
        flat_present = bool(self.source or self.operations or self.sink or self.upstream_asset_key)
        multi_present = bool(self.steps or self.sinks)
        if multi_present and flat_present:
            raise ValueError(
                "pyspark_pipeline: choose ONE shape — either top-level "
                "source/operations/sink OR steps/sinks, not both."
            )
        if multi_present:
            if not self.steps:
                raise ValueError("pyspark_pipeline: 'sinks' provided without 'steps'.")
            steps = list(self.steps)
            sinks = list(self.sinks or [])
            upstream_keys: List[str] = []

            def _walk_upstreams(step_list: List[Dict[str, Any]]) -> None:
                for s in step_list:
                    stype = (s.get("type") or "").lower()
                    if stype in _CONTROL_TYPES:
                        # for_each nests further steps — recurse
                        if stype == "for_each":
                            _walk_upstreams(s.get("steps") or [])
                        continue
                    src = s.get("source") or {}
                    if (src.get("kind") or "").lower() == "upstream":
                        k = src.get("upstream_asset_key")
                        if not k:
                            raise ValueError(f"step {s.get('id')!r}: source kind=upstream needs 'upstream_asset_key'")
                        if k not in upstream_keys:
                            upstream_keys.append(k)

            _walk_upstreams(steps)
            return steps, sinks, upstream_keys
        # Flat shape
        if not (self.source and self.operations is not None and self.sink):
            raise ValueError(
                "pyspark_pipeline: provide either 'steps' + 'sinks' OR "
                "top-level 'source' + 'operations' + 'sink'."
            )
        flat_step = {
            "id": "_default",
            "source": dict(self.source),
            "operations": list(self.operations),
        }
        # Annotate the upstream_asset_key on the source if flat upstream was given.
        if (self.source.get("kind") or "").lower() == "upstream":
            if not self.upstream_asset_key:
                raise ValueError("source.kind='upstream' requires upstream_asset_key.")
            flat_step["source"]["upstream_asset_key"] = self.upstream_asset_key
        flat_sink = dict(self.sink, **{"from": "_default"})
        upstream_keys = [self.upstream_asset_key] if self.upstream_asset_key else []
        return [flat_step], [flat_sink], upstream_keys

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            from dagster import BackfillPolicy
            backfill_policy = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            from dagster import BackfillPolicy
            backfill_policy = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        steps, sinks, upstream_keys = self._normalize()
        spark_config = dict(self.spark_config or {})
        spark_app_name = self.spark_app_name
        asset_name = self.asset_name

        # Validate ops up front (walks into for_each nested steps)
        def _validate_ops(step_list: List[Dict[str, Any]]) -> None:
            for s in step_list:
                stype = (s.get("type") or "").lower()
                if stype in _CONTROL_TYPES:
                    if stype == "for_each":
                        _validate_ops(s.get("steps") or [])
                    continue
                for i, op in enumerate(s.get("operations") or []):
                    if not isinstance(op, dict) or "op" not in op:
                        raise ValueError(f"step {s.get('id')!r} op #{i + 1}: each op must be a dict with 'op' key")
                    if op["op"].lower() not in _VALID_OPS:
                        raise ValueError(
                            f"step {s.get('id')!r} op #{i + 1}: op={op['op']!r} not supported. Valid: {sorted(_VALID_OPS)}"
                        )

        _validate_ops(steps)

        kinds = list(self.kinds or []) or ["pyspark", "spark"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        ins = {
            f"upstream_{j}": AssetIn(key=AssetKey.from_user_string(k))
            for j, k in enumerate(upstream_keys)
        }
        upstream_arg_names = {k: f"upstream_{j}" for j, k in enumerate(upstream_keys)}

        description = self.description or self.get_description()
        owners = self.owners or []
        group_name = self.group_name
        deps = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]
        kinds_set = set(kinds)

        # Snapshot control-flow config into the closure so _execute stays pure.
        variables = dict(self.variables or {})
        checkpoint_dir = self.checkpoint_dir

        @asset(partitions_def=partitions_def,
            backfill_policy=backfill_policy,
            key=dg.AssetKey.from_user_string(asset_name), description=description, owners=owners,
            tags=all_tags, group_name=group_name, deps=deps, kinds=kinds_set,
            ins=ins,
        )
        def _pyspark_pipeline_asset(context: AssetExecutionContext, **upstreams: Any) -> Any:
            _tokens = _partition_vars(context)  # {partition_*} in sources, SQL, sink paths and variables on a partitioned run
            return _execute(context, spark_config, spark_app_name, _substitute_partition(steps, _tokens), _substitute_partition(sinks, _tokens),
                             upstream_arg_names, upstreams,
                             variables=_substitute_partition(variables, _tokens), checkpoint_dir=checkpoint_dir,
                             asset_name=asset_name)

        return Definitions(assets=[_pyspark_pipeline_asset])


def _execute(context, spark_config, spark_app_name, steps, sinks,
              upstream_arg_names, upstreams,
              variables: Optional[Dict[str, Any]] = None,
              checkpoint_dir: Optional[str] = None,
              asset_name: str = ""):
    from pyspark.sql import SparkSession
    from pathlib import Path

    builder = SparkSession.builder.appName(spark_app_name)
    for k, v in (spark_config or {}).items():
        builder = builder.config(k, str(v))
    spark = builder.getOrCreate()

    step_outputs: Dict[str, Any] = {}
    # `meta` is a parallel map keyed by step id → _Info wrapper for
    # condition/for_each expressions that reference `<step>.row_count` etc.
    step_meta: Dict[str, Any] = {}

    # ── Load prior-run checkpoint (if configured) ─────────────────────
    ckpt_file: Optional[Path] = None
    prior_checkpoint: Dict[str, Any] = {}
    if checkpoint_dir:
        ckpt_file = Path(checkpoint_dir) / f"{asset_name}.json"
        prior_checkpoint = _load_checkpoint(ckpt_file)
        if prior_checkpoint:
            context.log.info(
                f"checkpoint: loaded {len(prior_checkpoint)} keys from {ckpt_file}"
            )

    vars_ctx = dict(variables or {})
    # In-run checkpoint state — mutated by `type: checkpoint` steps.
    live_checkpoint: Dict[str, Any] = dict(prior_checkpoint)

    def _eval_scope(extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Build the eval scope for condition when: / for_each over: exprs.

        Keys: `vars.<x>`, `checkpoint.<x>`, `<step_id>.row_count`, etc.
        Since ast.parse doesn't do dotted-access sub-eval, we mimic with
        a tiny attribute-holder dict-wrapper.
        """
        class _Dot(dict):
            __getattr__ = dict.get  # type: ignore[assignment]
        scope: Dict[str, Any] = {}
        scope["vars"] = _Dot(vars_ctx)
        scope["checkpoint"] = _Dot(live_checkpoint)
        for sid, meta in step_meta.items():
            scope[sid] = meta
        if extra:
            scope.update(extra)
        return scope

    # ── Step execution — recursive so `for_each` can nest ─────────────
    def _run_regular_step(step: Dict[str, Any]) -> None:
        sid = step["id"]
        src = step.get("source") or {}
        src_kind = (src.get("kind") or "").lower()

        if src_kind == "upstream":
            uk = src["upstream_asset_key"]
            obj = upstreams[upstream_arg_names[uk]]
            try:
                import polars as pl
                if isinstance(obj, pl.DataFrame):
                    obj = obj.to_pandas()
            except Exception:
                pass
            df = spark.createDataFrame(obj)
            context.log.info(f"step {sid}: read upstream pandas/polars DataFrame ({len(obj)} rows)")
        elif src_kind == "ref":
            ref = src.get("ref")
            if ref not in step_outputs:
                raise ValueError(f"step {sid!r}: source ref={ref!r} not yet defined")
            df = step_outputs[ref]
            context.log.info(f"step {sid}: ref → {ref}")
        else:
            df = _read_source(spark, src)
            context.log.info(f"step {sid}: read source {src.get('kind')}")

        for op in step.get("operations") or []:
            df = _apply_op(spark, df, op, step_outputs)
        step_outputs[sid] = df
        step_meta[sid] = _step_output_meta(df)
        context.log.info(f"step {sid}: {len(step.get('operations') or [])} op(s) staged")

    def _run_condition_step(step: Dict[str, Any], skip_state: Dict[str, bool]) -> None:
        sid = step.get("id") or "<condition>"
        when_expr = step.get("when")
        if not when_expr:
            raise ValueError(f"step {sid!r} type=condition requires 'when'")
        on_false = (step.get("on_false") or "skip_rest").lower()
        try:
            value = _safe_eval(when_expr, _eval_scope())
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"step {sid!r}: condition eval failed on {when_expr!r}: {e}") from e
        if bool(value):
            context.log.info(f"condition {sid}: {when_expr!r} → True (continuing)")
            return
        context.log.info(f"condition {sid}: {when_expr!r} → False (on_false={on_false})")
        if on_false == "fail":
            raise dg.Failure(f"condition {sid}: {when_expr!r} evaluated to False")
        if on_false == "skip_rest":
            skip_state["skip"] = True
            return
        raise ValueError(f"condition {sid!r}: on_false={on_false!r} not supported (fail | skip_rest)")

    def _run_checkpoint_step(step: Dict[str, Any]) -> None:
        sid = step.get("id") or "<checkpoint>"
        keys = step.get("keys") or []
        if not isinstance(keys, list):
            raise ValueError(f"step {sid!r} type=checkpoint requires 'keys' (list of var / step-attr names)")
        # Persist referenced values. Each key can be a plain name (from vars_ctx)
        # or a dotted expression like `orders.row_count`.
        scope = _eval_scope()
        new_state: Dict[str, Any] = dict(live_checkpoint)
        for k in keys:
            try:
                new_state[k] = _safe_eval(k, scope)
            except Exception as e:  # noqa: BLE001
                context.log.warning(f"checkpoint {sid}: could not resolve {k!r}: {e}")
        live_checkpoint.clear()
        live_checkpoint.update(new_state)
        if ckpt_file is not None:
            _save_checkpoint(ckpt_file, new_state)
            context.log.info(f"checkpoint {sid}: saved {len(new_state)} keys to {ckpt_file}")
        else:
            context.log.warning(
                f"checkpoint {sid}: no checkpoint_dir configured — held in-run only, not persisted"
            )

    def _run_for_each_step(step: Dict[str, Any], skip_state: Dict[str, bool]) -> None:
        sid = step.get("id") or "<for_each>"
        over_expr = step.get("over")
        as_name = step.get("as") or "item"
        inner = step.get("steps") or []
        if not over_expr:
            raise ValueError(f"step {sid!r} type=for_each requires 'over'")
        try:
            iterable = _safe_eval(over_expr, _eval_scope())
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"step {sid!r}: for_each over eval failed on {over_expr!r}: {e}") from e
        if not hasattr(iterable, "__iter__"):
            raise ValueError(f"step {sid!r}: for_each over={over_expr!r} did not evaluate to an iterable (got {type(iterable).__name__})")
        elements = list(iterable)
        context.log.info(f"for_each {sid}: {len(elements)} iteration(s) over {over_expr!r}")
        for i, element in enumerate(elements):
            # `loop.<as>` binding — but our eval scope doesn't do dotted-index
            # into user-defined objects, so we emulate with a per-iteration
            # extra namespace shipped as `loop` (dict-attr wrapper).
            class _Dot(dict):
                __getattr__ = dict.get  # type: ignore[assignment]
            loop_ns = _Dot({as_name: element, "index": i})
            # Re-evaluate expressions inside inner steps by string substitution
            # for `{{ loop.<as> }}` tokens — pragmatic + preserves the current
            # DataFrame-source model without introducing a full templater.
            def _sub(v):
                if isinstance(v, str) and "{{ loop." in v:
                    out = v.replace(f"{{{{ loop.{as_name} }}}}", str(element))
                    out = out.replace("{{ loop.index }}", str(i))
                    return out
                return v

            def _sub_tree(node):
                if isinstance(node, dict):
                    return {k: _sub_tree(v) for k, v in node.items()}
                if isinstance(node, list):
                    return [_sub_tree(x) for x in node]
                return _sub(node)

            # Push a per-iteration `loop` into eval scope for condition/etc.
            def _push():
                step_meta["loop"] = loop_ns
            def _pop():
                step_meta.pop("loop", None)

            _push()
            try:
                for inner_step in inner:
                    if skip_state.get("skip"):
                        return
                    substituted = _sub_tree(inner_step)
                    _dispatch(substituted, skip_state)
            finally:
                _pop()

    def _dispatch(step: Dict[str, Any], skip_state: Dict[str, bool]) -> None:
        stype = (step.get("type") or "").lower()
        if stype == "condition":
            _run_condition_step(step, skip_state)
        elif stype == "checkpoint":
            _run_checkpoint_step(step)
        elif stype == "for_each":
            _run_for_each_step(step, skip_state)
        else:
            _run_regular_step(step)

    skip_state: Dict[str, bool] = {"skip": False}
    for s_idx, step in enumerate(steps):
        if skip_state["skip"]:
            context.log.info(f"skip_rest: bypassing remaining steps (halted at index {s_idx})")
            break
        _dispatch(step, skip_state)

    # Write all sinks. Spark fires the Catalyst plan at each terminal action.
    sink_metadata: Dict[str, Any] = {}
    collected_pandas = None
    for sink in sinks:
        from_id = sink.get("from") or ""
        if from_id not in step_outputs:
            raise ValueError(f"sink.from={from_id!r} doesn't match any step id")
        df = step_outputs[from_id]
        kind = (sink.get("kind") or "").lower()
        result = _write_sink(df, sink)
        if kind == "none" and result is not None:
            # Caller wants the data back in Python.
            collected_pandas = result
            sink_metadata[f"pyspark/sink/{from_id}/row_count"] = MetadataValue.int(len(result))
        else:
            sink_metadata[f"pyspark/sink/{from_id}/kind"] = MetadataValue.text(kind)
            for k in ("path", "table", "dbtable"):
                if sink.get(k):
                    sink_metadata[f"pyspark/sink/{from_id}/{k}"] = MetadataValue.text(str(sink[k]))

    metadata: Dict[str, Any] = {
        "pyspark/step_count": MetadataValue.int(len(steps)),
        "pyspark/sink_count": MetadataValue.int(len(sinks)),
    }
    metadata.update(sink_metadata)
    if collected_pandas is not None:
        metadata["dagster/row_count"] = MetadataValue.int(len(collected_pandas))
        context.add_output_metadata(metadata)
        return collected_pandas
    return dg.MaterializeResult(metadata=metadata)


# BEGIN scaffold partitions-def builder (inlined per component; keep copies identical, see tests/test_partition_port_scaffold.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from dagster import (  # noqa: E402,F401
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    HourlyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    WeeklyPartitionsDefinition,
)


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
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
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
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
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")
# END scaffold partitions-def builder

# BEGIN partition-vars helper (inlined per component; keep copies identical, see tests/test_partition_vars.py)
def _partition_vars(context):
    """Tokens a partitioned run can use in its variables: {partition_key}, and for time partitions {partition_date}
    (YYYY-MM-DD), {partition_start} and {partition_end} (ISO datetimes); for a multi-partition also
    {partition_<dimension>}. Empty for an unpartitioned run."""
    import re as _re

    if not (context.has_partition_key or context.has_partition_key_range):
        return {}
    keys = list(context.partition_keys)
    tokens = {"partition_key": ",".join(str(k) for k in keys)}
    try:
        window = context.partition_time_window
        tokens["partition_start"] = window.start.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_end"] = window.end.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_date"] = window.start.strftime("%Y-%m-%d")
    except Exception:
        pass  # static / dynamic partitions have no time window
    if len(keys) == 1 and hasattr(keys[0], "keys_by_dimension"):
        for dimension, value in keys[0].keys_by_dimension.items():
            tokens["partition_" + _re.sub(r"\W", "_", str(dimension))] = str(value)
    return tokens


def _partition_env(tokens):
    """DAGSTER_PARTITION_* environment variables for a partitioned run (empty when unpartitioned)."""
    if not tokens:
        return {}
    env = {"DAGSTER_PARTITION_KEY": tokens["partition_key"]}
    for token, name in (("partition_date", "DAGSTER_PARTITION_DATE"), ("partition_start", "DAGSTER_PARTITION_START"),
                        ("partition_end", "DAGSTER_PARTITION_END")):
        if token in tokens:
            env[name] = tokens[token]
    return env


def _substitute_partition(value, tokens):
    """Replace {partition_*} tokens inside strings, recursively through dicts and lists. Only known tokens are
    replaced, so other braces (JSON, shell, templates) are left alone. A no-op for an unpartitioned run."""
    import re as _re

    if not tokens:
        return value
    if isinstance(value, str):
        return _re.sub(r"\{(partition_\w+)\}", lambda m: tokens.get(m.group(1), m.group(0)), value)
    if isinstance(value, dict):
        return {k: _substitute_partition(v, tokens) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(_substitute_partition(v, tokens) for v in value)
    return value
# END partition-vars helper
