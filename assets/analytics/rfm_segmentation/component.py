"""RFM Segmentation Component.

Segment customers by Recency, Frequency, and Monetary value using heuristic scoring.
Analyzes transaction data to identify Champions, Loyal Customers, At Risk, and Lost segments.
"""

from typing import Dict, List, Literal, Optional
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

    source_asset: Optional[str] = Field(
        default=None,
        description="Source asset with transaction data (set via lineage in Dagster Designer)"
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

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        source_asset = self.source_asset
        scoring_method = self.scoring_method
        lookback_days = self.lookback_days
        customer_id_field = self.customer_id_field
        order_date_field = self.order_date_field
        order_id_field = self.order_id_field
        revenue_field = self.revenue_field
        description = self.description or "RFM customer segmentation"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        # Set up dependencies
        upstream_keys = []
        if source_asset:
            upstream_keys.append(source_asset)

        # Build partition definition
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, MultiPartitionsDefinition,
            )
            _start = self.partition_start or "2020-01-01"
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if self.partition_type == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "static":
                partitions_def = StaticPartitionsDefinition(_values)
            elif self.partition_type == "multi":
                _dim = self.partition_static_dim or "segment"
                partitions_def = MultiPartitionsDefinition({
                    "date": DailyPartitionsDefinition(start_date=_start),
                    _dim: StaticPartitionsDefinition(_values),
                })
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


        @asset(
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
            if include_sample and len(rfm) > 0:
                # Sort by RFM score descending for better preview
                rfm_sorted = rfm.sort_values(['r_score', 'f_score', 'm_score'], ascending=False)

                return Output(
                    value=rfm,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(rfm_sorted.head(10).to_markdown(index=False)),
                        "preview": MetadataValue.dataframe(rfm_sorted.head(10))
                    }
                )
            else:
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
                    _metadata["dagster/column_lineage"] = MetadataValue.table_column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)
                return rfm

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[rfm_segmentation_asset])


        return Definitions(assets=[rfm_segmentation_asset], asset_checks=list(_schema_checks))
