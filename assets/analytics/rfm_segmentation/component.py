"""RFM Segmentation Component.

Segment customers by Recency, Frequency, and Monetary value using heuristic scoring.
Analyzes transaction data to identify Champions, Loyal Customers, At Risk, and Lost segments.
"""

from typing import Any, Dict, List, Literal, Optional
import pandas as pd
import numpy as np
from datetime import datetime
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
    asset,
    Output,
    MetadataValue,
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


class RFMSegmentationComponent(Component, Model, Resolvable):
    """Component for RFM (Recency, Frequency, Monetary) customer segmentation.

    RFM analysis is a customer segmentation technique that scores customers based on:
    - Recency: How recently they made a purchase
    - Frequency: How often they make purchases
    - Monetary: How much money they spend

    This component accepts transaction data and produces customer segments with
    actionable labels (Champions, Loyal Customers, At Risk, Lost, etc.).

    Example:
        ```yaml
        type: dagster_component_templates.RFMSegmentationComponent
        attributes:
          asset_name: customer_rfm_segments
          source_asset: transactions_data
          scoring_method: quintile
          lookback_days: 365
          description: "RFM customer segmentation"
          group_name: customer_analytics
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Source asset with transaction data (set via lineage in Dagster Designer)",
    )

    scoring_method: Literal["quintile", "quartile"] = Field(
        default="quintile",
        description="Scoring method: quintile (1-5) or quartile (1-4)"
    )

    lookback_days: int = Field(
        default=365,
        description="Number of days to look back for transaction data"
    )

    customer_id_field: Optional[str] = Field(
        default=None,
        description="Customer ID column name (auto-detected if not specified)"
    )

    order_date_field: Optional[str] = Field(
        default=None,
        description="Order date column name (auto-detected if not specified)"
    )

    order_id_field: Optional[str] = Field(
        default=None,
        description="Order ID column name (auto-detected if not specified)"
    )

    revenue_field: Optional[str] = Field(
        default=None,
        description="Revenue/amount column name (auto-detected if not specified)"
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
    partition_date_column: Optional[str] = Field(
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

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        source_asset = self.upstream_asset_key
        scoring_method = self.scoring_method
        lookback_days = self.lookback_days
        customer_id_field = self.customer_id_field
        order_date_field = self.order_date_field
        order_id_field = self.order_id_field
        revenue_field = self.revenue_field
        description = self.description or "RFM customer segmentation"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        # Set up dependencies
        upstream_keys = []
        if source_asset:
            upstream_keys.append(source_asset)

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
        _comp_name = "rfm_segmentation"  # component directory name
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



        @asset(retry_policy=_retry_policy, 
            name=asset_name,
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            deps=upstream_keys if upstream_keys else None,
        )
        def rfm_segmentation_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            # Filter to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    upstream = upstream[upstream[partition_date_column].astype(str) == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
            """Asset that performs RFM customer segmentation."""

            # Load upstream data
            upstream_data = {}
            if upstream_keys and hasattr(context, 'load_asset_value'):
                for key in upstream_keys:
                    try:
                        value = context.load_asset_value(AssetKey(key))
                        upstream_data[key] = value
                        context.log.info(f"Loaded {len(value)} rows from {key}")
                    except Exception as e:
                        context.log.warning(f"Could not load {key}: {e}")
            else:
                upstream_data = kwargs

            if not upstream_data:
                context.log.warning("No upstream data available")
                return pd.DataFrame()

            # Get the source DataFrame
            df = list(upstream_data.values())[0]
            if not isinstance(df, pd.DataFrame):
                context.log.error("Source data is not a DataFrame")
                return pd.DataFrame()

            context.log.info(f"Processing {len(df)} transaction records for RFM segmentation")

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
                ['order_date', 'date', 'transaction_date', 'created_at', 'timestamp', 'order_time'],
                order_date_field
            )
            order_col = find_column(
                ['order_id', 'transaction_id', 'orderId', 'transactionId', 'invoice_id'],
                order_id_field
            )
            revenue_col = find_column(
                ['revenue', 'amount', 'total', 'price', 'value', 'order_amount', 'spend'],
                revenue_field
            )

            # Validate required columns
            missing = []
            if not customer_col:
                missing.append("customer_id")
            if not date_col:
                missing.append("order_date")
            if not order_col:
                missing.append("order_id")
            if not revenue_col:
                missing.append("revenue")

            if missing:
                context.log.error(f"Missing required columns: {', '.join(missing)}")
                context.log.info(f"Available columns: {', '.join(df.columns)}")
                return pd.DataFrame()

            context.log.info(f"Using columns - Customer: {customer_col}, Date: {date_col}, Order: {order_col}, Revenue: {revenue_col}")

            # Prepare data
            rfm_df = df[[customer_col, date_col, order_col, revenue_col]].copy()
            rfm_df.columns = ['customer_id', 'order_date', 'order_id', 'revenue']

            # Parse dates
            rfm_df['order_date'] = pd.to_datetime(rfm_df['order_date'], errors='coerce')
            rfm_df = rfm_df.dropna(subset=['order_date'])

            # Convert revenue to numeric
            rfm_df['revenue'] = pd.to_numeric(rfm_df['revenue'], errors='coerce')
            rfm_df = rfm_df.dropna(subset=['revenue'])

            # Filter to lookback window
            current_date = pd.Timestamp.now()
            lookback_date = current_date - pd.Timedelta(days=lookback_days)
            rfm_df = rfm_df[rfm_df['order_date'] >= lookback_date]

            if len(rfm_df) == 0:
                context.log.warning(f"No transactions found in the last {lookback_days} days")
                return pd.DataFrame()

            context.log.info(f"Analyzing {len(rfm_df)} transactions from {len(rfm_df['customer_id'].unique())} customers")

            # Calculate RFM metrics
            # Recency: Days since last purchase
            recency = current_date - rfm_df.groupby('customer_id')['order_date'].max()
            recency_days = recency.dt.days

            # Frequency: Number of unique orders
            frequency = rfm_df.groupby('customer_id')['order_id'].nunique()

            # Monetary: Total revenue
            monetary = rfm_df.groupby('customer_id')['revenue'].sum()

            # Combine into RFM dataframe
            rfm = pd.DataFrame({
                'customer_id': recency_days.index,
                'recency_days': recency_days.values,
                'frequency': frequency.values,
                'monetary': monetary.values
            })

            # Check if we have enough customers for meaningful segmentation
            num_customers = len(rfm)
            if num_customers < 10:
                context.log.warning(f"Only {num_customers} customers found. RFM segmentation works best with 10+ customers.")

            # Score RFM metrics
            num_bins = 5 if scoring_method == "quintile" else 4
            score_labels = list(range(1, num_bins + 1))

            def calculate_score(series, ascending=True, reverse_labels=False):
                """Calculate score using quantiles with fallback for small datasets."""
                try:
                    if reverse_labels:
                        labels = score_labels[::-1]
                    else:
                        labels = score_labels

                    return pd.qcut(
                        series,
                        q=num_bins,
                        labels=labels,
                        duplicates='drop'
                    )
                except ValueError:
                    # Fallback for duplicate bin edges or too few values
                    context.log.info(f"Using rank-based scoring due to data distribution")
                    pct_rank = series.rank(pct=True, ascending=ascending)
                    score = (pct_rank * (num_bins - 1) + 1).astype(int)
                    return score.clip(1, num_bins)

            # Score: Lower recency = higher score (more recent is better)
            rfm['r_score'] = calculate_score(rfm['recency_days'], ascending=True, reverse_labels=True)

            # Score: Higher frequency = higher score
            rfm['f_score'] = calculate_score(rfm['frequency'], ascending=True)

            # Score: Higher monetary = higher score
            rfm['m_score'] = calculate_score(rfm['monetary'], ascending=True)

            # Combine RFM scores
            rfm['rfm_score'] = (
                rfm['r_score'].astype(str) +
                rfm['f_score'].astype(str) +
                rfm['m_score'].astype(str)
            )

            # Assign segments based on scores
            def assign_segment(row):
                r, f, m = row['r_score'], row['f_score'], row['m_score']

                if r >= 4 and f >= 4 and m >= 4:
                    return 'Champions', 'Best customers who buy often and spend the most'
                elif r >= 3 and f >= 3 and m >= 3:
                    return 'Loyal Customers', 'Consistent customers with good spending habits'
                elif r >= 4 and f <= 2:
                    return 'New Customers', 'Recent buyers with low frequency'
                elif r >= 3 and f <= 2 and m >= 3:
                    return 'Promising', 'Recent buyers with high spend potential'
                elif r >= 3 and m <= 2:
                    return 'Need Attention', 'Recent but low-value customers'
                elif r <= 2 and f >= 4:
                    return 'At Risk', 'Previously frequent buyers who haven\'t purchased recently'
                elif r <= 2 and f >= 2 and m >= 3:
                    return 'Cant Lose Them', 'High-value customers at risk of churning'
                elif r <= 2 and f <= 2 and m >= 3:
                    return 'Hibernating High Value', 'Inactive but previously high-value'
                elif r <= 2 and f <= 2:
                    return 'Lost', 'Inactive low-value customers'
                else:
                    return 'About to Sleep', 'Customers showing signs of reduced activity'

            rfm[['rfm_segment', 'segment_label']] = rfm.apply(
                assign_segment, axis=1, result_type='expand'
            )

            # Calculate segment statistics
            segment_stats = rfm.groupby('rfm_segment').agg({
                'customer_id': 'count',
                'recency_days': 'mean',
                'frequency': 'mean',
                'monetary': 'mean'
            }).round(2)

            context.log.info(f"RFM segmentation complete: {len(rfm)} customers across {len(rfm['rfm_segment'].unique())} segments")

            # Log segment distribution
            for segment, count in rfm['rfm_segment'].value_counts().items():
                pct = (count / len(rfm)) * 100
                context.log.info(f"  {segment}: {count} customers ({pct:.1f}%)")

            # Add metadata
            metadata = {
                "row_count": len(rfm),
                "total_customers": len(rfm),
                "num_segments": len(rfm['rfm_segment'].unique()),
                "scoring_method": scoring_method,
                "lookback_days": lookback_days,
                "segment_distribution": MetadataValue.md(
                    rfm['rfm_segment'].value_counts().to_frame('count').to_markdown()
                )
            }

            # Return with metadata
            if include_preview and len(rfm) > 0:
                # Sort by RFM score descending for better preview
                rfm_sorted = rfm.sort_values(['r_score', 'f_score', 'm_score'], ascending=False)

                _prev = rfm_sorted.sample(preview_rows) if len(rfm_sorted) > preview_rows * 10 else rfm_sorted.head(preview_rows)
                metadata['preview'] = MetadataValue.md(_prev.to_markdown(index=False))
            context.add_output_metadata(metadata)
            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(rfm.dtypes[col]))
                for col in rfm.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(rfm)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            # Use explicit lineage, or auto-infer passthrough columns at runtime
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(upstream.columns)
                    _effective_lineage = {
                        col: [col] for col in _col_schema.columns_by_name
                        if col in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
                        _lineage_deps[out_col] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=ic)
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)
            return rfm

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[rfm_segmentation_asset])


        return Definitions(assets=[rfm_segmentation_asset], asset_checks=list(_schema_checks))
