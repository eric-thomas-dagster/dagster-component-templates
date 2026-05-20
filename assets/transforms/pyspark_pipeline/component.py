"""PySparkPipelineComponent — single-asset multi-step PySpark DataFrame chain.

PySpark already has lazy evaluation: every `.filter() / .groupBy() / .join() /
.agg()` against a DataFrame builds a Catalyst logical plan, and execution
happens at the terminal action (write / count / collect / show). Inside this
component, that whole chain is built and executed in one Catalyst plan —
so the optimizer can fuse filters, push predicates back to data sources,
prune projections, and parallelize across the Spark cluster.

The source step (`source.kind`) chooses where the DataFrame is read from:
- `parquet` / `csv` / `json` / `orc` / `delta` — read a file/path
- `table` — read a Spark catalog table (e.g. Hive metastore, Iceberg, Delta)
- `jdbc` — read from JDBC with the predicate pushed to the database
- `upstream` — accept an upstream Dagster asset (pandas DataFrame, converted to Spark at the boundary)

The sink step (`sink.kind`) chooses where the final result lands:
- `parquet` / `csv` / `json` / `delta` — write a file/path
- `table` — write a Spark catalog table
- `jdbc` — write to a JDBC target
- `none` — collect to Python (becomes a pandas DataFrame, then returned as the asset's output)

Example:
    type: dagster_component_templates.PySparkPipelineComponent
    attributes:
      asset_name: paid_orders_summary
      spark_config:
        spark.app.name: dagster-orders-summary
        spark.master: "local[*]"
      source:
        kind: parquet
        path: s3a://bucket/orders/*.parquet
      operations:
        - op: filter
          predicate: "status = 'paid' AND amount > 100"
        - op: group_by
          group_by: [region, customer_id]
          aggregations:
            total:       {col: amount,   agg: sum}
            order_count: {col: order_id, agg: count}
        - op: sort
          by: [total]
          descending: true
        - op: limit
          n: 100
      sink:
        kind: parquet
        path: s3a://bucket/analytics/paid_orders_summary/
        mode: overwrite
"""
from typing import Any, Dict, List, Optional

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
              "group_by", "sort", "limit", "distinct", "drop_nulls"}
_SUPPORTED_AGGS = {"sum", "mean", "avg", "min", "max", "count", "countDistinct",
                    "first", "last", "stddev", "variance"}


def _apply_op(df, op: Dict[str, Any]):
    """Apply a single op to a Spark DataFrame, returning the new DataFrame."""
    from pyspark.sql import functions as F
    kind = op["op"]
    if kind == "filter":
        # SparkSQL expression — predicate pushdown applies to source readers.
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
        # SQL expression strings as values: {new_col_name: "SQL expr"}
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
            # F.sum, F.mean, F.min, ...; F.countDistinct is camelCase.
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
        df.write.mode(mode).parquet(sink["path"])
        return None
    if kind == "csv":
        df.write.mode(mode).csv(sink["path"], header=sink.get("header", True))
        return None
    if kind == "json":
        df.write.mode(mode).json(sink["path"])
        return None
    if kind == "delta":
        df.write.format("delta").mode(mode).save(sink["path"])
        return None
    if kind == "table":
        df.write.mode(mode).saveAsTable(sink["table"])
        return None
    if kind == "jdbc":
        opts = sink.get("options", {})
        (df.write.format("jdbc")
            .option("url", sink["url"]).option("dbtable", sink["dbtable"])
            .options(**opts).mode(mode).save())
        return None
    if kind == "none":
        # Collect into pandas for the asset's return value.
        return df.toPandas()
    raise ValueError(f"Unknown sink kind {kind!r}")


class PySparkPipelineComponent(Component, Model, Resolvable):
    """Single-asset PySpark DataFrame chain with Catalyst-optimized execution.

    Supported source kinds: parquet, csv, json, orc, delta, table, jdbc, upstream.
    Supported sink kinds:   parquet, csv, json, delta, table, jdbc, none.
    Supported ops: filter, select, drop, rename, with_columns, group_by,
                   sort, limit, distinct, drop_nulls.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    spark_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "SparkConf options (values stringified before passing to .config(...)). "
            "Set `spark.master` to 'local[*]' for local execution."
        ),
    )
    spark_app_name: str = Field(default="dagster-pyspark-pipeline", description="Spark application name")
    source: Dict[str, Any] = Field(description="Source spec: {kind: parquet|csv|json|orc|delta|table|jdbc|upstream, ...}")
    operations: List[Dict[str, Any]] = Field(
        description="Ordered list of PySpark DataFrame ops. Executes as one Catalyst plan.",
    )
    sink: Dict[str, Any] = Field(description="Sink spec: {kind: parquet|csv|json|delta|table|jdbc|none, ...}")
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Only set when source.kind = 'upstream'. The asset key providing a pandas/polars DataFrame.",
    )
    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    @classmethod
    def get_description(cls) -> str:
        return "Multi-step PySpark DataFrame chain in a single Dagster asset (Catalyst-optimized)."

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        spark_config = dict(self.spark_config or {})
        spark_app_name = self.spark_app_name
        source = dict(self.source)
        operations = list(self.operations)
        sink = dict(self.sink)
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key

        if source.get("kind", "").lower() == "upstream" and not upstream_asset_key:
            raise ValueError("source.kind='upstream' requires upstream_asset_key to be set.")

        kinds = list(self.kinds or []) or ["pyspark", "spark"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        description = self.description or self.get_description()
        owners = self.owners or []
        group_name = self.group_name
        deps = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]
        kinds_set = set(kinds)

        if upstream_asset_key:
            @asset(
                name=asset_name, description=description, owners=owners,
                tags=all_tags, group_name=group_name, deps=deps, kinds=kinds_set,
                ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            )
            def _pyspark_pipeline_asset(context: AssetExecutionContext, upstream: Any) -> Any:
                return _execute(context, spark_config, spark_app_name, source, operations, sink, upstream=upstream)
            return Definitions(assets=[_pyspark_pipeline_asset])

        @asset(
            name=asset_name, description=description, owners=owners,
            tags=all_tags, group_name=group_name, deps=deps, kinds=kinds_set,
        )
        def _pyspark_pipeline_asset_no_upstream(context: AssetExecutionContext) -> Any:
            return _execute(context, spark_config, spark_app_name, source, operations, sink)
        return Definitions(assets=[_pyspark_pipeline_asset_no_upstream])


def _execute(context, spark_config, spark_app_name, source, operations, sink, upstream=None):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(spark_app_name)
    for k, v in (spark_config or {}).items():
        builder = builder.config(k, str(v))  # Spark's .config accepts str, stringify whatever YAML gave us
    spark = builder.getOrCreate()

    try:
        # Read source
        if source["kind"].lower() == "upstream":
            try:
                import polars as pl
                if isinstance(upstream, pl.DataFrame):
                    upstream = upstream.to_pandas()
            except Exception:
                pass
            df = spark.createDataFrame(upstream)
            context.log.info(f"pyspark_pipeline: read upstream pandas/polars DataFrame ({len(upstream)} rows)")
        else:
            df = _read_source(spark, source)
            context.log.info(f"pyspark_pipeline: read source {source['kind']}")

        # Apply ops — entire chain becomes ONE Catalyst plan when executed at the sink.
        for i, op in enumerate(operations):
            df = _apply_op(df, op)
            context.log.info(f"  step {i + 1}/{len(operations)}: {op['op']}")

        # Sink — terminal action; Spark builds + runs the Catalyst-optimized plan here.
        result = _write_sink(df, sink)
        context.log.info(f"pyspark_pipeline: wrote sink {sink['kind']}")

        metadata = {
            "pyspark/source_kind": MetadataValue.text(source["kind"]),
            "pyspark/sink_kind": MetadataValue.text(sink["kind"]),
            "pyspark/operation_count": MetadataValue.int(len(operations)),
        }
        if isinstance(result, type(None)):
            # Materialized externally (parquet/table/jdbc/...). Return MaterializeResult.
            return dg.MaterializeResult(metadata=metadata)
        # sink.kind = 'none' → pandas DataFrame returned to Dagster IO.
        metadata["dagster/row_count"] = MetadataValue.int(len(result))
        context.add_output_metadata(metadata)
        return result
    finally:
        # Note: don't stop spark explicitly — getOrCreate is process-shared.
        # In a tight test loop you can spark.stop() explicitly.
        pass
