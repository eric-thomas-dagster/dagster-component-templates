"""Product Recommendations Component.

Generate product recommendations using collaborative filtering (customers who bought X also bought Y)
and popularity-based algorithms.
"""

from typing import Any, Dict, List, Optional, Union
import pandas as pd
import numpy as np
from datetime import datetime
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    asset,
    Output,
)
from pydantic import Field


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

    # Both shapes set: ambiguous. Pick one.
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


# --- backfill_policy support -------------------------------------------------
# A single_run backfill hands the asset a whole partition RANGE, on which
# `context.partition_key` raises. The compute below was written against one
# key at a time, so a range is run key-by-key through a thin context proxy and
# the per-partition frames are concatenated.
from typing import Literal  # noqa: E402,F401


class _SinglePartitionContext:
    """Presents ONE partition key of a range run to compute code that reads
    ``context.partition_key``; everything else delegates to the real context.
    ``add_output_metadata`` is captured (the real context only accepts one call
    per output) and merged by ``_run_per_partition``."""

    def __init__(self, context, partition_key):
        self._ctx = context
        self._key = partition_key
        self.captured_metadata = {}

    @property
    def has_partition_key(self):
        return self._key is not None

    @property
    def has_partition_key_range(self):
        return False

    @property
    def partition_key(self):
        if self._key is None:
            raise AttributeError("no partition key on this run")
        return self._key

    @property
    def partition_keys(self):
        return [] if self._key is None else [self._key]

    def add_output_metadata(self, metadata, output_name=None, mapping_key=None):
        self.captured_metadata.update(metadata or {})

    def __getattr__(self, name):
        return getattr(self._ctx, name)


def _run_per_partition(context, compute, partitions_def, loop, args, kwargs):
    """Run ``compute(context, *args, **kwargs)``; for a multi-partition range
    run it once per key (when ``loop``) and concatenate the DataFrames."""
    import pandas as pd
    from dagster import MetadataValue, Output

    if not (context.has_partition_key or context.has_partition_key_range):
        return compute(context, *args, **kwargs)
    keys = list(context.partition_keys)
    if len(keys) <= 1 and context.has_partition_key:
        return compute(context, *args, **kwargs)  # ordinary single-partition run

    if not loop:
        # Partition key does not change the computation: run once.
        proxy = _SinglePartitionContext(context, None)
        result = compute(proxy, *args, **kwargs)
        if isinstance(result, Output):
            proxy.captured_metadata.update(result.metadata or {})
            result = result.value
        if proxy.captured_metadata:
            context.add_output_metadata(proxy.captured_metadata)
        return result

    frames, merged = [], {}
    for key in keys:
        if hasattr(partitions_def, "partitions_defs") and not hasattr(key, "keys_by_dimension"):
            key = partitions_def.get_partition_key_from_str(key)
        proxy = _SinglePartitionContext(context, key)
        result = compute(proxy, *args, **kwargs)
        merged.update(proxy.captured_metadata)
        if isinstance(result, Output):
            merged.update(result.metadata or {})
            result = result.value
        if not isinstance(result, pd.DataFrame):
            raise TypeError(
                f"backfill_policy='single_run' needs a DataFrame result per partition; "
                f"got {type(result).__name__} for partition {key!r}"
            )
        result = result.copy()
        if "_partition_key" not in result.columns:
            result["_partition_key"] = str(key)
        frames.append(result)
    merged["partition_keys"] = MetadataValue.json([str(k) for k in keys])
    context.add_output_metadata(merged)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


class ProductRecommendationsComponent(Component, Model, Resolvable):
    """Component for generating product recommendations.

    This component creates product recommendations using proven approaches:
    - **Collaborative Filtering**: "Customers who bought X also bought Y"
    - **Popular Products**: Trending and bestselling items
    - **Frequently Bought Together**: Product bundles and cross-sells
    - **Category-Based**: Similar products in same category

    The output can be used for:
    - Product page recommendations
    - Shopping cart upsells
    - Email campaigns
    - Homepage personalization

    Example:
        ```yaml
        type: dagster_community_components.ProductRecommendationsComponent
        attributes:
          asset_name: product_recommendations
          upstream_asset_key: order_items
          recommendation_type: collaborative
          num_recommendations: 10
          min_co_occurrence: 3
          description: "Product recommendations"
          group_name: product_analytics
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    upstream_asset_key: Union[str, List[str]] = Field(
        description="Upstream asset key providing a DataFrame with order/transaction data"
    )

    recommendation_type: str = Field(
        default="collaborative",
        description="Type: collaborative, popular, frequently_bought_together"
    )

    num_recommendations: int = Field(
        default=10,
        description="Number of recommendations to generate per product"
    )

    min_co_occurrence: int = Field(
        default=3,
        description="Minimum times products must be purchased together (collaborative filtering)"
    )

    lookback_days: Optional[int] = Field(
        default=None,
        description="Only consider recent purchases (optional, use all data if not set)"
    )

    customer_id_field: Optional[str] = Field(
        default=None,
        description="Customer ID column (auto-detected if not specified)"
    )

    product_id_field: Optional[str] = Field(
        default=None,
        description="Product ID column (auto-detected if not specified)"
    )

    order_id_field: Optional[str] = Field(
        default=None,
        description="Order ID column (auto-detected if not specified)"
    )

    date_field: Optional[str] = Field(
        default=None,
        description="Transaction date column (auto-detected if not specified)"
    )

    quantity_field: Optional[str] = Field(
        default=None,
        description="Quantity purchased column (auto-detected if not specified)"
    )

    product_name_field: Optional[str] = Field(
        default=None,
        description="Product name column (optional, for readable output)"
    )

    category_field: Optional[str] = Field(
        default=None,
        description="Product category column (optional, for category-based recommendations)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="product_analytics",
        description="Asset group for organization"
    )
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
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
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
    )

    include_preview_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata when "
            "`include_preview_metadata` is True. For long DataFrames "
            "(>10x preview_rows), a random sample is used so the preview "
            "reflects the data distribution; otherwise head() is used."
        ),
    )

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' materializes an entire backfill range in one run: the analysis runs once per partition in the range, each on the upstream rows filtered to that partition (via partition_date_column / partition_static_column), and the per-partition outputs are concatenated with a '_partition_key' column added. 'multi_run' launches one run per partition (optionally capped via backfill_policy_max_partitions_per_run). Leave unset for Dagster's default (one run per partition, uncapped). Ignored when partition_type is not set. 'single_run' requires a partition-range-aware IO manager (e.g. a DB-table IO manager with partition_expr set) -- a generic filesystem/default IO manager cannot persist a single_run output spanning multiple partitions and will raise a clear error at materialization time. If neither partition_date_column nor partition_static_column is set the partition key does not change the computation, so a range runs the analysis once. Components that persist side effects (e.g. model files) write them once per partition, so the last partition in the range wins."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )

    retry_policy_max_retries: Optional[int] = Field(

        default=None,

        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",

    )

    retry_policy_delay_seconds: Optional[int] = Field(

        default=None,

        description="Seconds between retries (default 1).",

    )

    retry_policy_backoff: str = Field(

        default="exponential",

        description="Backoff strategy: 'linear' or 'exponential'.",

    )



    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        rec_type = self.recommendation_type
        num_recs = self.num_recommendations
        min_co_occur = self.min_co_occurrence
        lookback_days = self.lookback_days
        customer_id_field = self.customer_id_field
        product_id_field = self.product_id_field
        order_id_field = self.order_id_field
        date_field = self.date_field
        quantity_field = self.quantity_field
        product_name_field = self.product_name_field
        category_field = self.category_field
        description = self.description or f"Product recommendations ({rec_type})"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "product_recommendations"  # component directory name
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            from dagster import BackfillPolicy
            _backfill_policy_obj = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            from dagster import BackfillPolicy
            _backfill_policy_obj = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )
        _deco_product_recommendations_asset = asset(retry_policy=_retry_policy, 
            key=AssetKey.from_user_string(asset_name),
            description=description,
            partitions_def=partitions_def,
            backfill_policy=_backfill_policy_obj,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def product_recommendations_asset_compute(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            # Filter to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    _col_dates = pd.to_datetime(upstream[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                    upstream = upstream[_col_dates == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
            """Asset that generates product recommendations."""

            df = upstream
            if not isinstance(df, pd.DataFrame):
                context.log.error("Source data is not a DataFrame")
                return pd.DataFrame()

            context.log.info(f"Processing {len(df)} transaction records for recommendations")

            # Auto-detect required columns
            def find_column(possible_names, custom_name=None):
                if custom_name and custom_name in df.columns:
                    return custom_name
                for name in possible_names:
                    if name in df.columns:
                        return name
                return None

            product_col = find_column(
                ['product_id', 'item_id', 'sku', 'productId', 'product'],
                product_id_field
            )
            order_col = find_column(
                ['order_id', 'transaction_id', 'orderId', 'transactionId'],
                order_id_field
            )

            # Validate required columns
            missing = []
            if not product_col:
                missing.append("product_id")
            if not order_col:
                missing.append("order_id")

            if missing:
                context.log.error(f"Missing required columns: {', '.join(missing)}")
                context.log.info(f"Available columns: {', '.join(df.columns)}")
                return pd.DataFrame()

            # Optional columns
            customer_col = find_column(
                ['customer_id', 'user_id', 'customerId', 'userId'],
                customer_id_field
            )
            date_col = find_column(
                ['date', 'order_date', 'transaction_date', 'created_at'],
                date_field
            )
            quantity_col = find_column(
                ['quantity', 'qty', 'amount', 'count'],
                quantity_field
            )
            name_col = find_column(
                ['product_name', 'name', 'title', 'item_name'],
                product_name_field
            )
            category_col = find_column(
                ['category', 'product_category', 'item_category'],
                category_field
            )

            context.log.info(f"Using columns - Product: {product_col}, Order: {order_col}")

            # Prepare data
            cols_to_use = [product_col, order_col]
            col_names = ['product_id', 'order_id']

            if customer_col:
                cols_to_use.append(customer_col)
                col_names.append('customer_id')
            if date_col:
                cols_to_use.append(date_col)
                col_names.append('date')
            if quantity_col:
                cols_to_use.append(quantity_col)
                col_names.append('quantity')
            if name_col:
                cols_to_use.append(name_col)
                col_names.append('product_name')
            if category_col:
                cols_to_use.append(category_col)
                col_names.append('category')

            orders_df = df[cols_to_use].copy()
            orders_df.columns = col_names

            # Apply lookback window if specified
            if lookback_days and 'date' in orders_df.columns:
                orders_df['date'] = pd.to_datetime(orders_df['date'], errors='coerce')
                cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=lookback_days)
                orders_df = orders_df[orders_df['date'] >= cutoff_date]
                context.log.info(f"Applied {lookback_days}-day lookback window: {len(orders_df)} records")

            if 'quantity' not in orders_df.columns:
                orders_df['quantity'] = 1

            # Remove duplicates
            orders_df = orders_df.drop_duplicates()

            context.log.info(f"Generating {rec_type} recommendations for {orders_df['product_id'].nunique()} products")

            recommendations = []

            if rec_type == 'collaborative':
                # Collaborative filtering: "Customers who bought X also bought Y"

                # Get products purchased together in same order
                product_pairs = []

                for order_id in orders_df['order_id'].unique():
                    products = orders_df[orders_df['order_id'] == order_id]['product_id'].unique()

                    # Generate all pairs of products in this order
                    if len(products) > 1:
                        for i, prod1 in enumerate(products):
                            for prod2 in products[i+1:]:
                                product_pairs.append((prod1, prod2))
                                product_pairs.append((prod2, prod1))  # Both directions

                if not product_pairs:
                    context.log.warning("No product pairs found for collaborative filtering")
                    return pd.DataFrame()

                # Count co-occurrences
                pairs_df = pd.DataFrame(product_pairs, columns=['product_id', 'recommended_product_id'])
                pair_counts = pairs_df.groupby(['product_id', 'recommended_product_id']).size().reset_index(name='co_occurrence_count')

                # Filter by minimum co-occurrence
                pair_counts = pair_counts[pair_counts['co_occurrence_count'] >= min_co_occur]

                # Calculate support (percentage of orders containing product A that also contain product B)
                product_order_counts = orders_df.groupby('product_id')['order_id'].nunique().reset_index(name='product_order_count')
                pair_counts = pair_counts.merge(product_order_counts, on='product_id', how='left')
                pair_counts['support'] = (pair_counts['co_occurrence_count'] / pair_counts['product_order_count'] * 100).round(2)

                # Rank recommendations by co-occurrence count
                pair_counts['recommendation_rank'] = pair_counts.groupby('product_id')['co_occurrence_count'].rank(ascending=False, method='first').astype(int)

                # Keep top N recommendations per product
                recommendations = pair_counts[pair_counts['recommendation_rank'] <= num_recs].copy()

                context.log.info(f"Generated {len(recommendations)} collaborative recommendations")

            elif rec_type == 'popular':
                # Popular products: Most frequently purchased items

                # Calculate product popularity
                product_stats = orders_df.groupby('product_id').agg({
                    'order_id': 'nunique',
                    'quantity': 'sum'
                }).reset_index()
                product_stats.columns = ['product_id', 'num_orders', 'total_quantity']

                # Rank by number of orders
                product_stats['popularity_rank'] = product_stats['num_orders'].rank(ascending=False, method='first').astype(int)
                product_stats = product_stats.sort_values('popularity_rank')

                # For each product, recommend top popular products (excluding itself)
                all_products = product_stats['product_id'].unique()
                top_products = product_stats[product_stats['popularity_rank'] <= num_recs]['product_id'].tolist()

                for product_id in all_products:
                    # Recommend top products except the product itself
                    recs_for_product = [p for p in top_products if p != product_id][:num_recs]

                    for rank, rec_product in enumerate(recs_for_product, 1):
                        rec_data = product_stats[product_stats['product_id'] == rec_product].iloc[0]
                        recommendations.append({
                            'product_id': product_id,
                            'recommended_product_id': rec_product,
                            'recommendation_rank': rank,
                            'num_orders': rec_data['num_orders'],
                            'total_quantity': rec_data['total_quantity'],
                            'popularity_rank': rec_data['popularity_rank']
                        })

                recommendations = pd.DataFrame(recommendations)
                context.log.info(f"Generated {len(recommendations)} popularity-based recommendations")

            elif rec_type == 'frequently_bought_together':
                # Same as collaborative but with stricter filtering for bundles

                # Get products purchased together in same order
                product_pairs = []

                for order_id in orders_df['order_id'].unique():
                    products = orders_df[orders_df['order_id'] == order_id]['product_id'].unique()

                    if len(products) >= 2:  # Only consider orders with multiple items
                        for i, prod1 in enumerate(products):
                            for prod2 in products[i+1:]:
                                product_pairs.append((prod1, prod2))
                                product_pairs.append((prod2, prod1))

                if not product_pairs:
                    context.log.warning("No product pairs found")
                    return pd.DataFrame()

                pairs_df = pd.DataFrame(product_pairs, columns=['product_id', 'recommended_product_id'])
                pair_counts = pairs_df.groupby(['product_id', 'recommended_product_id']).size().reset_index(name='bundle_frequency')

                # Higher threshold for bundles
                bundle_threshold = max(min_co_occur * 2, 5)
                pair_counts = pair_counts[pair_counts['bundle_frequency'] >= bundle_threshold]

                # Calculate lift (how much more likely to buy together than random)
                total_orders = orders_df['order_id'].nunique()
                product_order_counts = orders_df.groupby('product_id')['order_id'].nunique()

                pair_counts = pair_counts.merge(
                    product_order_counts.rename('product_a_orders'),
                    left_on='product_id',
                    right_index=True
                )
                pair_counts = pair_counts.merge(
                    product_order_counts.rename('product_b_orders'),
                    left_on='recommended_product_id',
                    right_index=True
                )

                pair_counts['expected_co_occurrence'] = (
                    pair_counts['product_a_orders'] * pair_counts['product_b_orders'] / total_orders
                )
                pair_counts['lift'] = (pair_counts['bundle_frequency'] / pair_counts['expected_co_occurrence']).round(2)

                # Rank by lift (preference for strong associations)
                pair_counts['recommendation_rank'] = pair_counts.groupby('product_id')['lift'].rank(ascending=False, method='first').astype(int)

                recommendations = pair_counts[pair_counts['recommendation_rank'] <= num_recs].copy()

                context.log.info(f"Generated {len(recommendations)} bundle recommendations")

            else:
                context.log.error(f"Unknown recommendation type: {rec_type}")
                return pd.DataFrame()

            # Add product names if available
            if 'product_name' in orders_df.columns:
                product_names = orders_df[['product_id', 'product_name']].drop_duplicates()

                recommendations = recommendations.merge(
                    product_names.rename(columns={'product_name': 'source_product_name'}),
                    on='product_id',
                    how='left'
                )
                recommendations = recommendations.merge(
                    product_names.rename(columns={'product_id': 'recommended_product_id', 'product_name': 'recommended_product_name'}),
                    on='recommended_product_id',
                    how='left'
                )

            # Add category if available
            if 'category' in orders_df.columns:
                product_categories = orders_df[['product_id', 'category']].drop_duplicates()
                recommendations = recommendations.merge(
                    product_categories.rename(columns={'category': 'recommended_product_category'}),
                    left_on='recommended_product_id',
                    right_on='product_id',
                    how='left',
                    suffixes=('', '_drop')
                )
                # Clean up duplicate product_id column from merge
                recommendations = recommendations.drop(columns=[col for col in recommendations.columns if col.endswith('_drop')])

            # Sort by product and rank
            recommendations = recommendations.sort_values(['product_id', 'recommendation_rank'])

            context.log.info(f"Recommendation generation complete: {len(recommendations)} recommendations for {recommendations['product_id'].nunique()} products")

            # Log sample recommendations
            if len(recommendations) > 0:
                sample_product = recommendations['product_id'].iloc[0]
                sample_recs = recommendations[recommendations['product_id'] == sample_product].head(5)
                context.log.info(f"\nSample recommendations for product {sample_product}:")
                for _, row in sample_recs.iterrows():
                    rec_prod = row['recommended_product_id']
                    if 'recommended_product_name' in row:
                        rec_prod = f"{rec_prod} ({row['recommended_product_name']})"
                    context.log.info(f"  #{row['recommendation_rank']}: {rec_prod}")

            # Add metadata — cast pandas scalars to Python natives
            metadata = {
                "row_count": len(recommendations),
                "num_products_with_recommendations": int(recommendations['product_id'].nunique()),
                "recommendation_type": rec_type,
                "num_recommendations_per_product": int(num_recs),
                "min_co_occurrence": int(min_co_occur),
            }

            if lookback_days:
                metadata['lookback_days'] = lookback_days

            # Return with metadata
            if include_preview and len(recommendations) > 0:
                try:
                    _prev = recommendations.sample(min(preview_rows, len(recommendations))) if len(recommendations) > preview_rows * 10 else recommendations.head(preview_rows)
                    metadata['preview'] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            context.add_output_metadata(metadata)
            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(recommendations.dtypes[col]))
                for col in recommendations.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(recommendations)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            # Use explicit lineage, or auto-infer passthrough columns at runtime
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(upstream.columns)
                    _effective_lineage = {
                        col.name: [col.name] for col in _col_schema.columns
                        if col.name in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
                        _lineage_deps[str(out_col)] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=str(ic))
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)
            return recommendations
        @_deco_product_recommendations_asset
        def product_recommendations_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            return _run_per_partition(context, product_recommendations_asset_compute, partitions_def, bool(partition_date_column or partition_static_column), [upstream], {})


        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[product_recommendations_asset])


        return Definitions(assets=[product_recommendations_asset], asset_checks=list(_schema_checks))


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat
