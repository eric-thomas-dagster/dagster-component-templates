"""Lifetime Value (LTV) Prediction Component.

Predict customer lifetime value using historical purchase patterns and cohort analysis.
Calculate both historical LTV and projected future value.
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
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
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


class LTVPredictionComponent(Component, Model, Resolvable):
    """Component for predicting customer lifetime value.

    LTV prediction calculates the total revenue a customer is expected to generate
    over their lifetime with your business. This component uses historical patterns
    to calculate:
    - Historical LTV: Actual revenue to date
    - Average Order Value (AOV)
    - Purchase Frequency
    - Customer Lifespan
    - Predicted LTV: Projected future value

    The prediction uses a simple but effective formula:
    Predicted LTV = AOV × Purchase Frequency × Customer Lifespan

    Example:
        ```yaml
        type: dagster_component_templates.LTVPredictionComponent
        attributes:
          asset_name: customer_ltv
          upstream_asset_key: transaction_data
          prediction_period_months: 24
          cohort_analysis: true
          description: "Customer lifetime value prediction"
          group_name: customer_analytics
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    upstream_asset_key: Union[str, List[str]] = Field(
        description="Upstream asset key providing a DataFrame with transaction/order data"
    )

    prediction_period_months: int = Field(
        default=24,
        description="Months to project forward for LTV prediction"
    )

    cohort_analysis: bool = Field(
        default=True,
        description="Include cohort-based analysis (by customer signup month)"
    )

    include_confidence_intervals: bool = Field(
        default=True,
        description="Include confidence intervals for predictions"
    )

    min_transactions_required: int = Field(
        default=2,
        description="Minimum transactions required to calculate LTV (filters out one-time buyers)"
    )

    customer_id_field: Optional[str] = Field(
        default=None,
        description="Customer ID column name (auto-detected if not specified)"
    )

    transaction_date_field: Optional[str] = Field(
        default=None,
        description="Transaction date column (auto-detected if not specified)"
    )

    amount_field: Optional[str] = Field(
        default=None,
        description="Transaction amount column (auto-detected if not specified)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="customer_analytics",
        description="Asset group for organization"
    )
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
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
        prediction_period_months = self.prediction_period_months
        cohort_analysis = self.cohort_analysis
        include_confidence = self.include_confidence_intervals
        min_transactions = self.min_transactions_required
        customer_id_field = self.customer_id_field
        transaction_date_field = self.transaction_date_field
        amount_field = self.amount_field
        description = self.description or "Customer lifetime value prediction"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "ltv_prediction"  # component directory name
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
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

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
        _deco_ltv_prediction_asset = asset(retry_policy=_retry_policy, 
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
        def ltv_prediction_asset_compute(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            """Asset that predicts customer lifetime value."""

            df = upstream
            if not isinstance(df, pd.DataFrame):
                context.log.error("Source data is not a DataFrame")
                return pd.DataFrame()

            context.log.info(f"Processing {len(df)} transactions for LTV prediction")

            # Auto-detect required columns
            def find_column(possible_names, custom_name=None):
                if custom_name and custom_name in df.columns:
                    return custom_name
                for name in possible_names:
                    if name in df.columns:
                        return name
                return None

            customer_col = find_column(
                ['customer_id', 'user_id', 'customerId', 'userId', 'id'],
                customer_id_field
            )
            date_col = find_column(
                ['date', 'transaction_date', 'order_date', 'purchase_date', 'created_at'],
                transaction_date_field
            )
            amount_col = find_column(
                ['amount', 'total', 'revenue', 'value', 'order_total', 'transaction_amount'],
                amount_field
            )

            # Validate required columns
            missing = []
            if not customer_col:
                missing.append("customer_id")
            if not date_col:
                missing.append("date/transaction_date")
            if not amount_col:
                missing.append("amount/revenue")

            if missing:
                context.log.error(f"Missing required columns: {', '.join(missing)}")
                context.log.info(f"Available columns: {', '.join(df.columns)}")
                return pd.DataFrame()

            context.log.info(f"Using columns - Customer: {customer_col}, Date: {date_col}, Amount: {amount_col}")

            # Prepare data
            ltv_df = df[[customer_col, date_col, amount_col]].copy()
            ltv_df.columns = ['customer_id', 'date', 'amount']

            # Parse dates and amounts
            ltv_df['date'] = pd.to_datetime(ltv_df['date'], errors='coerce')
            ltv_df = ltv_df.dropna(subset=['date'])
            ltv_df['amount'] = pd.to_numeric(ltv_df['amount'], errors='coerce').fillna(0)

            # Remove negative amounts (returns/refunds for this analysis)
            ltv_df = ltv_df[ltv_df['amount'] > 0]

            if len(ltv_df) == 0:
                context.log.warning("No valid transactions after data cleaning")
                return pd.DataFrame()

            context.log.info(f"Calculating LTV for {len(ltv_df)} valid transactions")

            # Calculate per-customer metrics
            customer_metrics = ltv_df.groupby('customer_id').agg(
                first_purchase_date=('date', 'min'),
                last_purchase_date=('date', 'max'),
                total_transactions=('date', 'count'),
                total_revenue=('amount', 'sum'),
                avg_order_value=('amount', 'mean')
            ).reset_index()

            # Filter by minimum transactions
            customer_metrics = customer_metrics[
                customer_metrics['total_transactions'] >= min_transactions
            ]

            if len(customer_metrics) == 0:
                context.log.warning(f"No customers with >= {min_transactions} transactions")
                return pd.DataFrame()

            context.log.info(f"Analyzing {len(customer_metrics)} customers with >= {min_transactions} transactions")

            # Calculate customer lifespan in days
            customer_metrics['lifespan_days'] = (
                customer_metrics['last_purchase_date'] - customer_metrics['first_purchase_date']
            ).dt.days

            # Calculate purchase frequency (purchases per month)
            # Add 1 to avoid division by zero for customers who made all purchases in one day
            customer_metrics['purchase_frequency_monthly'] = (
                customer_metrics['total_transactions'] /
                ((customer_metrics['lifespan_days'] + 1) / 30)
            )

            # Historical LTV (actual revenue to date)
            customer_metrics['historical_ltv'] = customer_metrics['total_revenue']

            # Calculate average customer lifespan from data
            # For active customers, we don't know their full lifespan yet
            # Use the median lifespan of all customers as an estimate
            median_lifespan_days = customer_metrics['lifespan_days'].median()

            # Predict future lifespan for each customer
            current_date = pd.Timestamp.now()
            customer_metrics['days_since_last_purchase'] = (
                current_date - customer_metrics['last_purchase_date']
            ).dt.days

            # Estimate remaining lifespan (months)
            customer_metrics['estimated_remaining_months'] = (
                (median_lifespan_days - customer_metrics['lifespan_days']).clip(lower=0) / 30
            ).clip(upper=prediction_period_months)

            # Predicted LTV = Historical LTV + (AOV × Purchase Frequency × Remaining Months)
            customer_metrics['predicted_additional_ltv'] = (
                customer_metrics['avg_order_value'] *
                customer_metrics['purchase_frequency_monthly'] *
                customer_metrics['estimated_remaining_months']
            )

            customer_metrics['predicted_total_ltv'] = (
                customer_metrics['historical_ltv'] +
                customer_metrics['predicted_additional_ltv']
            )

            # Confidence intervals based on purchase consistency
            if include_confidence:
                # Calculate coefficient of variation for each customer's purchases
                customer_variance = ltv_df.groupby('customer_id')['amount'].std().fillna(0)
                customer_metrics = customer_metrics.merge(
                    customer_variance.rename('order_value_std'),
                    on='customer_id',
                    how='left'
                )

                # Lower variance = higher confidence
                customer_metrics['prediction_confidence'] = (
                    1 - (customer_metrics['order_value_std'] / customer_metrics['avg_order_value'])
                ).clip(lower=0.3, upper=0.95)

                # Calculate confidence intervals (±20% scaled by confidence)
                margin = customer_metrics['predicted_total_ltv'] * 0.20 * (1 - customer_metrics['prediction_confidence'])
                customer_metrics['ltv_lower_bound'] = (customer_metrics['predicted_total_ltv'] - margin).clip(lower=0)
                customer_metrics['ltv_upper_bound'] = customer_metrics['predicted_total_ltv'] + margin

            # Cohort analysis
            if cohort_analysis:
                customer_metrics['cohort_month'] = customer_metrics['first_purchase_date'].dt.to_period('M')

                # Calculate cohort-level metrics
                cohort_stats = customer_metrics.groupby('cohort_month').agg({
                    'customer_id': 'count',
                    'historical_ltv': 'mean',
                    'predicted_total_ltv': 'mean',
                    'avg_order_value': 'mean',
                    'purchase_frequency_monthly': 'mean'
                }).round(2)

                context.log.info(f"\nCohort Analysis ({len(cohort_stats)} cohorts):")
                for cohort, row in cohort_stats.head(10).iterrows():
                    context.log.info(
                        f"  {cohort}: {row['customer_id']} customers, "
                        f"${row['predicted_total_ltv']:.2f} avg predicted LTV"
                    )

            # Calculate customer value segments
            customer_metrics['ltv_percentile'] = customer_metrics['predicted_total_ltv'].rank(pct=True) * 100

            def segment_by_ltv(percentile):
                if percentile >= 90:
                    return 'Platinum (Top 10%)'
                elif percentile >= 75:
                    return 'Gold (Top 25%)'
                elif percentile >= 50:
                    return 'Silver (Top 50%)'
                else:
                    return 'Bronze (Bottom 50%)'

            customer_metrics['value_segment'] = customer_metrics['ltv_percentile'].apply(segment_by_ltv)

            # Select output columns
            output_cols = [
                'customer_id',
                'first_purchase_date',
                'last_purchase_date',
                'total_transactions',
                'avg_order_value',
                'purchase_frequency_monthly',
                'historical_ltv',
                'predicted_total_ltv',
                'value_segment',
                'ltv_percentile'
            ]

            if include_confidence:
                output_cols.extend(['prediction_confidence', 'ltv_lower_bound', 'ltv_upper_bound'])

            if cohort_analysis:
                output_cols.insert(3, 'cohort_month')
                # Convert period to string for output
                customer_metrics['cohort_month'] = customer_metrics['cohort_month'].astype(str)

            result_df = customer_metrics[output_cols].copy()

            # Round numeric columns
            numeric_cols = ['avg_order_value', 'purchase_frequency_monthly', 'historical_ltv',
                          'predicted_total_ltv', 'ltv_percentile']
            if include_confidence:
                numeric_cols.extend(['prediction_confidence', 'ltv_lower_bound', 'ltv_upper_bound'])

            for col in numeric_cols:
                if col in result_df.columns:
                    result_df[col] = result_df[col].round(2)

            context.log.info(f"LTV prediction complete: {len(result_df)} customers analyzed")

            # Log value segments
            segment_dist = result_df['value_segment'].value_counts()
            context.log.info("\nValue Segment Distribution:")
            for segment, count in segment_dist.items():
                avg_ltv = result_df[result_df['value_segment'] == segment]['predicted_total_ltv'].mean()
                context.log.info(f"  {segment}: {count} customers (${avg_ltv:,.2f} avg LTV)")

            # Calculate summary statistics — cast to native floats so the
            # metadata serializer can handle them.
            total_predicted_ltv = float(result_df['predicted_total_ltv'].sum())
            avg_predicted_ltv = float(result_df['predicted_total_ltv'].mean())
            median_predicted_ltv = float(result_df['predicted_total_ltv'].median())

            # Add metadata
            metadata = {
                "row_count": len(result_df),
                "total_customers": len(result_df),
                "total_predicted_ltv": round(total_predicted_ltv, 2),
                "avg_predicted_ltv": round(avg_predicted_ltv, 2),
                "median_predicted_ltv": round(median_predicted_ltv, 2),
                "prediction_period_months": int(prediction_period_months),
                "min_transactions_required": int(min_transactions),
                "value_segments": MetadataValue.md(segment_dist.to_frame('count').to_markdown())
            }

            # Return with metadata
            if include_preview and len(result_df) > 0:
                # Sort by predicted LTV descending to show highest value customers first
                result_sorted = result_df.sort_values('predicted_total_ltv', ascending=False)

                _prev = result_sorted.sample(preview_rows) if len(result_sorted) > preview_rows * 10 else result_sorted.head(preview_rows)
                metadata['preview'] = MetadataValue.md(_prev.to_markdown(index=False))
            context.add_output_metadata(metadata)
            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(result_df.dtypes[col]))
                for col in result_df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(result_df)),
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
            return result_df
        @_deco_ltv_prediction_asset
        def ltv_prediction_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            return _run_per_partition(context, ltv_prediction_asset_compute, partitions_def, bool(partition_date_column or partition_static_column), [upstream], {})


        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[ltv_prediction_asset])


        return Definitions(assets=[ltv_prediction_asset], asset_checks=list(_schema_checks))
