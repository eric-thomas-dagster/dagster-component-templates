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


_VALID_OPS = {"filter", "select", "drop", "rename", "with_columns",
              "group_by", "sort", "limit", "distinct", "drop_nulls",
              "join", "union", "sql"}
_SUPPORTED_AGGS = {"sum", "mean", "avg", "min", "max", "count", "countDistinct",
                    "first", "last", "stddev", "variance"}


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
    upstream_asset_key: Optional[str] = Field(
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

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

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
            for s in steps:
                src = s.get("source") or {}
                if (src.get("kind") or "").lower() == "upstream":
                    k = src.get("upstream_asset_key")
                    if not k:
                        raise ValueError(f"step {s.get('id')!r}: source kind=upstream needs 'upstream_asset_key'")
                    if k not in upstream_keys:
                        upstream_keys.append(k)
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
        steps, sinks, upstream_keys = self._normalize()
        spark_config = dict(self.spark_config or {})
        spark_app_name = self.spark_app_name
        asset_name = self.asset_name

        # Validate ops up front
        for s in steps:
            for i, op in enumerate(s.get("operations") or []):
                if not isinstance(op, dict) or "op" not in op:
                    raise ValueError(f"step {s.get('id')!r} op #{i + 1}: each op must be a dict with 'op' key")
                if op["op"].lower() not in _VALID_OPS:
                    raise ValueError(
                        f"step {s.get('id')!r} op #{i + 1}: op={op['op']!r} not supported. Valid: {sorted(_VALID_OPS)}"
                    )

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

        @asset(
            key=dg.AssetKey.from_user_string(asset_name), description=description, owners=owners,
            tags=all_tags, group_name=group_name, deps=deps, kinds=kinds_set,
            ins=ins,
        )
        def _pyspark_pipeline_asset(context: AssetExecutionContext, **upstreams: Any) -> Any:
            return _execute(context, spark_config, spark_app_name, steps, sinks,
                             upstream_arg_names, upstreams)

        return Definitions(assets=[_pyspark_pipeline_asset])


def _execute(context, spark_config, spark_app_name, steps, sinks,
              upstream_arg_names, upstreams):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(spark_app_name)
    for k, v in (spark_config or {}).items():
        builder = builder.config(k, str(v))
    spark = builder.getOrCreate()

    step_outputs: Dict[str, Any] = {}

    for s_idx, step in enumerate(steps):
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
        context.log.info(f"step {sid}: {len(step.get('operations') or [])} op(s) staged ({s_idx + 1}/{len(steps)})")

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
