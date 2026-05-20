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
        exprs = op["expressions"]
        return lf.with_columns([pl.sql_expr(e).alias(name) for name, e in exprs.items()])
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

    `op: sql` uses polars's SQLContext where the current chain is available
    as `self` and earlier step outputs are available by id.
    """

    asset_name: str = Field(description="Output Dagster asset name")

    # Flat-shape single-source shape ------------------------------------------
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Top-level single-source shape: Dagster upstream asset key (pandas or polars DataFrame)",
    )
    operations: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Flat shape: ordered list of ops applied to upstream_asset_key. Compiles to one anonymous step.",
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
        flat-shape field + any kind=upstream sources across steps).
        """
        flat_present = bool(self.upstream_asset_key or self.operations)
        multi_present = bool(self.steps)
        if multi_present and flat_present:
            raise ValueError(
                "polars_pipeline: choose ONE shape — either top-level "
                "upstream_asset_key + operations OR steps, not both."
            )
        if multi_present:
            steps = list(self.steps or [])
            sinks = list(self.sinks or [])
            upstream_keys: List[str] = []
            for s in steps:
                src = s.get("source") or {}
                if src.get("kind") == "upstream":
                    k = src.get("upstream_asset_key")
                    if not k:
                        raise ValueError(f"step {s.get('id')!r}: source kind=upstream needs 'upstream_asset_key'")
                    if k not in upstream_keys:
                        upstream_keys.append(k)
            return steps, sinks, upstream_keys
        if not (self.upstream_asset_key and self.operations is not None):
            raise ValueError(
                "polars_pipeline: provide either 'steps' OR top-level "
                "'upstream_asset_key' + 'operations'."
            )
        flat_step = {
            "id": "_default",
            "source": {"kind": "upstream", "upstream_asset_key": self.upstream_asset_key},
            "operations": list(self.operations),
        }
        return [flat_step], [], [self.upstream_asset_key]

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

        @asset(
            name=asset_name,
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=all_tags,
            group_name=self.group_name,
            ins=ins,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            kinds=set(kinds),
        )
        def _pipeline_asset(context: AssetExecutionContext, **upstreams: Any) -> Any:
            import polars as pl

            def _to_lazy(obj: Any):
                if isinstance(obj, pl.LazyFrame):
                    return obj
                if isinstance(obj, pl.DataFrame):
                    return obj.lazy()
                return pl.from_pandas(obj).lazy()

            # step_outputs accumulates the LazyFrame at the END of each step.
            step_outputs: Dict[str, Any] = {}

            for s_idx, step in enumerate(steps):
                sid = step["id"]
                src = step.get("source") or {}
                src_kind = src.get("kind", "upstream")
                if src_kind == "upstream":
                    uk = src.get("upstream_asset_key") or ""
                    arg = upstream_arg_names[uk]
                    lf = _to_lazy(upstreams[arg])
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

            # Collect the primary step. The polars planner fuses everything
            # touched by that final LazyFrame — including ops in earlier
            # steps it referenced.
            primary_lf = step_outputs[primary_step]
            try:
                result_pl = primary_lf.collect(engine="streaming" if streaming else "auto")
            except TypeError:
                result_pl = primary_lf.collect(streaming=streaming) if streaming else primary_lf.collect()

            # Run any side-output sinks.
            sink_metadata: Dict[str, Any] = {}
            for sink in sinks:
                from_id = sink.get("from") or ""
                if from_id not in step_outputs:
                    raise ValueError(f"sink.from={from_id!r} doesn't match any step id")
                kind = (sink.get("kind") or "parquet").lower()
                path = sink.get("path")
                if not path:
                    raise ValueError(f"sink from={from_id!r}: 'path' is required")
                df = step_outputs[from_id].collect()  # materialize side-output
                if kind == "parquet":
                    df.write_parquet(path)
                elif kind == "csv":
                    df.write_csv(path)
                else:
                    raise ValueError(f"sink.kind={kind!r} not supported in polars_pipeline. Use 'parquet' or 'csv'.")
                sink_metadata[f"polars/sink/{from_id}/path"] = MetadataValue.path(path)
                sink_metadata[f"polars/sink/{from_id}/row_count"] = MetadataValue.int(df.height)

            row_count = result_pl.height
            _meta_df = result_pl.to_pandas()
            from dagster import TableSchema, TableColumn
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(_meta_df.dtypes[col]))
                for col in _meta_df.columns
            ])
            metadata: Dict[str, Any] = {
                "dagster/row_count": MetadataValue.int(row_count),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                "polars/step_count": MetadataValue.int(len(steps)),
                "polars/primary_step": MetadataValue.text(primary_step or ""),
                "polars/streaming": MetadataValue.bool(streaming),
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
