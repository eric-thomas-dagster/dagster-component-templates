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
    type: dagster_community_components.PolarsScanParquetComponent
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
from typing import Any, Dict, List, Literal, Optional, Union

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


class PolarsScanParquetComponent(Component, Model, Resolvable):
    """Read parquet via polars's lazy scanner with predicate + column pushdown."""

    asset_name: str = Field(description="Output Dagster asset name")
    path: str = Field(
        description=(
            "Path or glob pattern. Local: '/data/orders/*.parquet'. S3: 's3://bucket/orders/**/*.parquet'. GCS: 'gs://bucket/...'. ADLS: 'az://container@account.dfs.core.windows.net/...'. On a partitioned run {partition_key}, {partition_date}, {partition_start} and {partition_end} (and {partition_<dimension>} for multi-partitions) are substituted."
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
    include_preview_metadata: bool = Field(default=False)
    preview_rows: int = Field(default=25, ge=1, le=500)
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "scans one path covering the whole backfill range (the "
            "partition_key/partition_date/partition_start/partition_end tokens "
            "reflect the full range, comma-joined for partition_key) rather than "
            "one scan per partition. 'multi_run' launches one run per partition "
            "(optionally capped via backfill_policy_max_partitions_per_run). Leave "
            "unset for Dagster's default (one run per partition, uncapped). Ignored "
            "when partition_type is not set. 'single_run' requires a partition-"
            "range-aware IO manager (e.g. a DB-table IO manager with partition_expr "
            "set) -- a generic filesystem/default IO manager cannot persist a "
            "single_run output spanning multiple partitions and will raise a clear "
            "error at materialization time."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('kinds', 'owners', 'deps'), csv=(), tags=('asset_tags',))

    @classmethod
    def get_description(cls) -> str:
        return "Read parquet via polars's lazy scanner with predicate + column pushdown."

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)

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

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            backfill_policy = dg.BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            backfill_policy = dg.BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        @asset(partitions_def=partitions_def,
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=all_tags,
            group_name=self.group_name,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            kinds=set(kinds),
            backfill_policy=backfill_policy,
        )
        def _polars_scan_parquet_asset(context: AssetExecutionContext) -> Any:
            import polars as pl

            scan_kwargs: Dict[str, Any] = {}
            if storage_options:
                scan_kwargs["storage_options"] = storage_options
            if n_rows is not None:
                scan_kwargs["n_rows"] = n_rows

            lf = pl.scan_parquet(_substitute_partition(path, _partition_vars(context)), **scan_kwargs)
            context.log.info(f"polars_scan_parquet: scanning {_substitute_partition(path, _partition_vars(context))}")

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
                "polars/path": MetadataValue.text(_substitute_partition(path, _partition_vars(context))),
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
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(
                start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"],
            )
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
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule)
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
            "date": DailyPartitionsDefinition(start_date=partition_start),
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
