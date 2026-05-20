"""PolarsPipelineComponent — single-asset, multi-step polars LazyFrame chain.

Where `summarize(backend=polars)` / `filter(backend=polars)` / etc. run ONE
polars op per Dagster asset, this component runs MANY polars ops as a single
LazyFrame chain inside a single asset.

Why: polars's query planner only optimizes within a single LazyFrame graph.
Spread across separate Dagster assets, each asset materializes — the lazy
chain is broken at every boundary. Inside one asset, the optimizer can fuse
filters, push predicates back to source-style scans, eliminate intermediate
columns, and run the whole pipeline in one parallelized sweep.

Tradeoff: coarser lineage. You get one asset for the whole sequence, not
one-per-step. Use this when the steps are tightly coupled and you care
about throughput more than per-step lineage. Use the per-asset
`backend: polars` transforms when each step deserves its own catalog entry.

Example:
    type: dagster_component_templates.PolarsPipelineComponent
    attributes:
      asset_name: top_customers_by_region
      upstream_asset_key: raw_orders
      operations:
        - op: filter
          predicate: "status = 'paid'"
        - op: with_columns
          expressions:
            net_amount: "amount - tax"
        - op: group_by
          group_by: [region, customer_id]
          aggregations:
            total: {col: net_amount, agg: sum}
            order_count: {col: order_id, agg: count}
        - op: sort
          by: [region, total]
          descending: [false, true]
        - op: head_per_group
          group_by: [region]
          n: 5
      group_name: analytics
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


_VALID_OPS = {"filter", "with_columns", "select", "drop", "rename",
              "group_by", "sort", "head", "tail", "head_per_group",
              "unique", "join", "drop_nulls", "fill_null", "cast"}


_PL_AGG_FUNCS = {"sum", "mean", "avg", "min", "max", "count", "median",
                 "std", "var", "first", "last", "nunique", "n_unique"}


def _validate_ops(ops: List[Dict[str, Any]]) -> None:
    for i, op in enumerate(ops):
        if not isinstance(op, dict) or "op" not in op:
            raise ValueError(f"operation {i}: each step must be a dict with an 'op' key")
        kind = op["op"]
        if kind not in _VALID_OPS:
            raise ValueError(
                f"operation {i}: op={kind!r} not supported. "
                f"Valid: {sorted(_VALID_OPS)}"
            )


def _pl_agg(pl_module, src_col: str, out_col: str, func: str):
    """Map agg func name → polars expression with alias."""
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


def _apply_op(pl, lf, op: Dict[str, Any]):
    """Apply a single operation to a polars LazyFrame, returning the updated LazyFrame."""
    kind = op["op"]
    if kind == "filter":
        predicate = op["predicate"]
        # Use polars's SQLContext for the predicate — same dialect as the per-asset filter component.
        ctx = pl.SQLContext({"_lf": lf})
        return ctx.execute(f"SELECT * FROM _lf WHERE {predicate}", eager=False)
    if kind == "with_columns":
        # expressions: {col_name: <polars expression string>}
        # We support both literal SQL fragments and raw polars expressions via pl.sql_expr().
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
    if kind == "head":
        return lf.head(op["n"])
    if kind == "tail":
        return lf.tail(op["n"])
    if kind == "head_per_group":
        # Sort + group_by + head per group (preserves the lazy chain).
        group_by = op["group_by"]
        n = op["n"]
        # If the input was already sorted by the user in a prior op, polars
        # preserves that order; if not, take input order.
        return lf.group_by(group_by, maintain_order=True).head(n)
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
        # mapping: {col_name: 'Int64' | 'Float64' | 'Utf8' | 'Date' | ...}
        mapping = op["mapping"]
        type_map = {name: getattr(pl, t) for name, t in mapping.items()}
        return lf.cast(type_map)
    if kind == "join":
        # Joining within a pipeline is supported when the right side is also
        # passed in as a sibling upstream — see additional_upstream_keys.
        raise NotImplementedError(
            "join inside polars_pipeline: pass the right-side frame via "
            "additional_upstream_keys and use a separate _apply_join step "
            "(future enhancement)."
        )
    raise ValueError(f"Unhandled op {kind!r}")


class PolarsPipelineComponent(Component, Model, Resolvable):
    """Run a multi-step polars LazyFrame chain in a single Dagster asset.

    Operations are applied in order. The entire chain is lazy until the
    final `.collect()` — polars's query planner fuses filters, eliminates
    intermediate columns, and parallelizes the execution.

    Supported operations: filter, with_columns, select, drop, rename,
    group_by, sort, head, tail, head_per_group, unique, drop_nulls,
    fill_null, cast.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key (pandas or polars DataFrame)")
    operations: List[Dict[str, Any]] = Field(
        description=(
            "Ordered list of polars operations to apply as a single lazy chain. "
            "Each is a dict with an 'op' key plus op-specific parameters. "
            "See class docstring for examples."
        ),
    )
    output_type: str = Field(
        default="polars",
        description="'polars' (default) or 'pandas'. Polars output preserves the polars frame for downstream polars chains; pandas auto-converts at the boundary.",
    )
    streaming: bool = Field(
        default=False,
        description=(
            "Use polars streaming engine for the final .collect() (out-of-core "
            "execution for frames larger than memory). Default off."
        ),
    )
    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    include_preview_metadata: bool = Field(
        default=False,
        description="Emit a small preview of the output as asset metadata.",
    )
    preview_rows: int = Field(default=25, ge=1, le=500)

    @classmethod
    def get_description(cls) -> str:
        return "Multi-step polars LazyFrame chain in a single Dagster asset (query-plan fusion + parallelism)."

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _validate_ops(self.operations)

        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        operations = list(self.operations)
        output_type = self.output_type.lower()
        if output_type not in ("polars", "pandas"):
            raise ValueError(f"output_type must be 'polars' or 'pandas', got {self.output_type!r}")
        streaming = self.streaming
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        kinds = list(self.kinds or []) or ["polars"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=asset_name,
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=all_tags,
            group_name=self.group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            kinds=set(kinds),
        )
        def _pipeline_asset(context: AssetExecutionContext, upstream: Any) -> Any:
            import polars as pl

            # Accept pandas OR polars input. Convert pandas → polars LazyFrame.
            if isinstance(upstream, pl.DataFrame):
                lf = upstream.lazy()
            else:
                # Assume pandas DataFrame.
                lf = pl.from_pandas(upstream).lazy()

            context.log.info(f"polars_pipeline: applying {len(operations)} operation(s) as one lazy chain")

            for i, op in enumerate(operations):
                lf = _apply_op(pl, lf, op)
                context.log.info(f"  step {i + 1}/{len(operations)}: {op['op']}")

            # Collect — this is where polars fuses + executes the whole chain.
            try:
                result_pl = lf.collect(engine="streaming" if streaming else "auto")
            except TypeError:
                # older polars versions: collect(streaming=True) keyword
                result_pl = lf.collect(streaming=streaming) if streaming else lf.collect()
            row_count = result_pl.height
            context.log.info(f"polars_pipeline: produced {row_count} rows × {result_pl.width} columns")

            _meta_df = result_pl.to_pandas()
            from dagster import TableSchema, TableColumn
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(_meta_df.dtypes[col]))
                for col in _meta_df.columns
            ])
            metadata = {
                "dagster/row_count": MetadataValue.int(row_count),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                "polars/operation_count": MetadataValue.int(len(operations)),
                "polars/streaming": MetadataValue.bool(streaming),
            }
            if include_preview and row_count > 0:
                try:
                    _prev = _meta_df.sample(min(preview_rows, row_count)) if row_count > preview_rows * 10 else _meta_df.head(preview_rows)
                    metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")
            context.add_output_metadata(metadata)

            return result_pl if output_type == "polars" else result_pl.to_pandas()

        return Definitions(assets=[_pipeline_asset])
