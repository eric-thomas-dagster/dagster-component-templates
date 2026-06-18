"""PolarsScanParquetComponent — predicate + projection pushdown to parquet.

Reads parquet files via `pl.scan_parquet()` (lazy). When you supply
`predicate:` and/or `columns:`, polars's query planner pushes them down
to the parquet reader — only matching row groups + selected columns are
read off disk/object-store. That's true predicate pushdown, the thing
the in-memory `filter(backend=polars)` component CAN'T give you.

Use when:
- Source data is parquet (local, S3, GCS, ADLS)
- You want to read only a subset of rows / columns
- The frame is too large to load fully into memory

Example:
    type: dagster_component_templates.PolarsScanParquetComponent
    attributes:
      asset_name: paid_orders_recent
      path: s3://bucket/orders/year=2026/*.parquet
      columns: [order_id, customer_id, amount, status, created_at]
      predicate: "status = 'paid' AND created_at >= '2026-01-01'"
      group_name: ingestion

Glob patterns, local paths, and cloud URIs (s3://, gs://, az://) all
work — polars's `scan_parquet` handles them natively. For cloud paths,
pass storage_options the way polars expects.
"""
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from dagster import (
    AssetExecutionContext,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


class PolarsScanParquetComponent(Component, Model, Resolvable):
    """Read parquet via polars's lazy scanner with predicate + column pushdown."""

    asset_name: str = Field(description="Output Dagster asset name")
    path: str = Field(
        description=(
            "Path or glob pattern. Local: '/data/orders/*.parquet'. "
            "S3: 's3://bucket/orders/**/*.parquet'. "
            "GCS: 'gs://bucket/...'. ADLS: 'az://container@account.dfs.core.windows.net/...'."
        ),
    )
    columns: Optional[List[Union[str, int]]] = Field(
        default=None,
        description="Subset of columns to read. None = read all. Pushed down: unread columns never come off disk.",
    )
    predicate: Optional[str] = Field(
        default=None,
        description=(
            "SQL predicate to push down to the parquet reader. Polars's query "
            "planner can skip whole row groups whose stats don't match. "
            "Example: \"status = 'paid' AND amount > 100\"."
        ),
    )
    storage_options: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Cloud auth options passed through to polars/object_store. "
            "Example for S3: {aws_access_key_id: ..., aws_secret_access_key: ..., region: us-east-1}."
        ),
    )
    output_type: str = Field(
        default="polars",
        description="'polars' (default) or 'pandas'. Auto-converts at boundary if pandas.",
    )
    n_rows: Optional[int] = Field(
        default=None,
        description="Hard row limit (LIMIT). Pushed down — reader stops after this many.",
    )
    streaming: bool = Field(
        default=False,
        description="Use polars streaming engine for out-of-core execution (frames larger than memory).",
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
        return "Read parquet via polars's lazy scanner with predicate + column pushdown."

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        output_type = self.output_type.lower()
        if output_type not in ("polars", "pandas"):
            raise ValueError(f"output_type must be 'polars' or 'pandas', got {self.output_type!r}")

        path = self.path
        columns = list(self.columns) if self.columns else None
        predicate = self.predicate
        storage_options = dict(self.storage_options) if self.storage_options else None
        n_rows = self.n_rows
        streaming = self.streaming
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        asset_name = self.asset_name

        kinds = list(self.kinds or []) or ["polars", "parquet"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        @asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=all_tags,
            group_name=self.group_name,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            kinds=set(kinds),
        )
        def _polars_scan_parquet_asset(context: AssetExecutionContext) -> Any:
            import polars as pl

            scan_kwargs: Dict[str, Any] = {}
            if storage_options:
                scan_kwargs["storage_options"] = storage_options
            if n_rows is not None:
                scan_kwargs["n_rows"] = n_rows

            lf = pl.scan_parquet(path, **scan_kwargs)
            context.log.info(f"polars_scan_parquet: scanning {path}")

            if columns:
                lf = lf.select(columns)
                context.log.info(f"  projection pushdown: {len(columns)} columns")
            if predicate:
                ctx = pl.SQLContext({"_lf": lf})
                lf = ctx.execute(f"SELECT * FROM _lf WHERE {predicate}", eager=False)
                context.log.info(f"  predicate pushdown: {predicate}")

            try:
                result_pl = lf.collect(engine="streaming" if streaming else "auto")
            except TypeError:
                result_pl = lf.collect(streaming=streaming) if streaming else lf.collect()

            row_count = result_pl.height
            context.log.info(f"polars_scan_parquet: read {row_count} rows × {result_pl.width} columns")

            _meta_df = result_pl.to_pandas()
            from dagster import TableSchema, TableColumn
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(_meta_df.dtypes[col]))
                for col in _meta_df.columns
            ])
            metadata = {
                "dagster/row_count": MetadataValue.int(row_count),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                "polars/path": MetadataValue.text(path),
                "polars/streaming": MetadataValue.bool(streaming),
            }
            if predicate:
                metadata["polars/predicate_pushdown"] = MetadataValue.text(predicate)
            if columns:
                metadata["polars/columns_pushdown"] = MetadataValue.text(", ".join(columns))
            if include_preview and row_count > 0:
                try:
                    _prev = _meta_df.head(preview_rows)
                    metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")
            context.add_output_metadata(metadata)

            return result_pl if output_type == "polars" else result_pl.to_pandas()

        return Definitions(assets=[_polars_scan_parquet_asset])
