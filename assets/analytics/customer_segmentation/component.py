"""Customer Segmentation Component.

Segments customers using RFM (Recency, Frequency, Monetary) analysis to identify
Champions, Loyal Customers, At-Risk customers, and other actionable segments.
"""

from typing import Any, Dict, List, Optional

import pandas as pd
import numpy as np
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    OpExecutionContext,
    asset,
    MetadataValue,
)
from dagster._core.definitions.definitions_class import Definitions
from dagster_components import Component, ComponentLoadContext, component_type
from dagster_components.core.component_defs_builder import build_defs_from_component
from pydantic import Field


@component_type(name="customer_segmentation")
class CustomerSegmentationComponent(Component):
    """Component that segments customers using RFM analysis."""

    asset_name: str = Field(
        ...,
        description="Name of the customer segmentation asset to create",
    )

    # Input asset references (set via lineage)
    transaction_data_asset: Optional[str] = Field(
        default="",
        description="Transaction/order data with customer_id, date, and amount",
    )

    customer_data_asset: Optional[str] = Field(
        default="",
        description="Customer data for additional attributes (optional)",
    )

    # RFM Configuration
    recency_weight: float = Field(
        default=1.0,
        description="Weight for recency in RFM score",
    )

    frequency_weight: float = Field(
        default=1.0,
        description="Weight for frequency in RFM score",
    )

    monetary_weight: float = Field(
        default=1.0,
        description="Weight for monetary value in RFM score",
    )

    # Scoring method
    scoring_method: str = Field(
        default="quintiles",
        description="Scoring method: quintiles, quartiles, or custom",
    )

    # Analysis period
    analysis_period_days: int = Field(
        default=365,
        description="Number of days to analyze for RFM calculation",
    )

    # Segment definitions
    use_predefined_segments: bool = Field(
        default=True,
        description="Use predefined RFM segments (Champions, Loyal, At Risk, etc.)",
    )

    # Output options
    include_recommendations: bool = Field(
        default=True,
        description="Include action recommendations for each segment",
    )

    calculate_segment_value: bool = Field(
        default=True,
        description="Calculate total and average value per segment",
    )

    # Asset properties
    description: str = Field(
        default="",
        description="Asset description",
    )

    group_name: str = Field(
        default="analytics",
        description="Asset group name",
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

    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")

    def _calculate_rfm_scores(self, transaction_data: pd.DataFrame) -> pd.DataFrame:
        """Calculate RFM scores for each customer."""
        if transaction_data is None or transaction_data.empty:
            return pd.DataFrame()

        # Standardize column names
        df = transaction_data.copy()

        # Map common column variations
        if 'customer_id' not in df.columns:
            for col in ['user_id', 'id', 'customerid']:
                if col in df.columns:
                    df['customer_id'] = df[col]
                    break

        if 'date' not in df.columns:
            for col in ['order_date', 'transaction_date', 'created_at']:
                if col in df.columns:
                    df['date'] = df[col]
                    break

        if 'amount' not in df.columns:
            for col in ['total', 'revenue', 'value', 'order_total']:
                if col in df.columns:
                    df['amount'] = df[col]
                    break

        # Validate required columns
        if 'customer_id' not in df.columns:
            raise ValueError("Transaction data must have customer_id column")
        if 'date' not in df.columns:
            raise ValueError("Transaction data must have date column")
        if 'amount' not in df.columns:
            raise ValueError("Transaction data must have amount column")

        # Ensure date is datetime
        df['date'] = pd.to_datetime(df['date'])

        # Filter to analysis period
        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=self.analysis_period_days)
        df = df[df['date'] >= cutoff_date]

        if df.empty:
            return pd.DataFrame()

        # Calculate RFM metrics
        current_date = df['date'].max() + pd.Timedelta(days=1)

        rfm = df.groupby('customer_id').agg({
            'date': lambda x: (current_date - x.max()).days,  # Recency
            'customer_id': 'count',  # Frequency
            'amount': 'sum'  # Monetary
        }).rename(columns={
            'date': 'recency',
            'customer_id': 'frequency',
            'amount': 'monetary'
        })

        # Calculate RFM scores (1-5, where 5 is best)
        if self.scoring_method == 'quintiles':
            rfm['R_score'] = pd.qcut(rfm['recency'], 5, labels=[5,4,3,2,1], duplicates='drop')
            rfm['F_score'] = pd.qcut(rfm['frequency'], 5, labels=[1,2,3,4,5], duplicates='drop')
            rfm['M_score'] = pd.qcut(rfm['monetary'], 5, labels=[1,2,3,4,5], duplicates='drop')
        elif self.scoring_method == 'quartiles':
            rfm['R_score'] = pd.qcut(rfm['recency'], 4, labels=[4,3,2,1], duplicates='drop')
            rfm['F_score'] = pd.qcut(rfm['frequency'], 4, labels=[1,2,3,4], duplicates='drop')
            rfm['M_score'] = pd.qcut(rfm['monetary'], 4, labels=[1,2,3,4], duplicates='drop')

        # Convert to numeric
        rfm['R_score'] = pd.to_numeric(rfm['R_score'])
        rfm['F_score'] = pd.to_numeric(rfm['F_score'])
        rfm['M_score'] = pd.to_numeric(rfm['M_score'])

        # Calculate weighted RFM score
        rfm['RFM_score'] = (
            rfm['R_score'] * self.recency_weight +
            rfm['F_score'] * self.frequency_weight +
            rfm['M_score'] * self.monetary_weight
        ) / (self.recency_weight + self.frequency_weight + self.monetary_weight)

        # Reset index to make customer_id a column
        rfm = rfm.reset_index()

        return rfm

    def _assign_segments(self, rfm: pd.DataFrame) -> pd.DataFrame:
        """Assign customers to predefined RFM segments."""
        if not self.use_predefined_segments:
            return rfm

        def segment_customer(row):
            r, f, m = row['R_score'], row['F_score'], row['M_score']

            if r >= 4 and f >= 4 and m >= 4:
                return 'Champions'
            elif r >= 4 and f >= 3:
                return 'Loyal Customers'
            elif r >= 4 and f < 3:
                return 'Potential Loyalists'
            elif r >= 3 and f >= 3 and m >= 3:
                return 'Promising'
            elif r >= 3 and f < 3:
                return 'Need Attention'
            elif r < 3 and f >= 4:
                return 'At Risk'
            elif r < 3 and f >= 3:
                return 'About to Sleep'
            elif r < 2 and f < 2 and m >= 4:
                return 'Cant Lose Them'
            elif r < 2:
                return 'Hibernating'
            else:
                return 'Lost'

        rfm['segment'] = rfm.apply(segment_customer, axis=1)

        if self.include_recommendations:
            recommendations = {
                'Champions': 'Reward them. Can be early adopters. Promote brand advocates.',
                'Loyal Customers': 'Upsell higher value products. Ask for reviews. Engage them.',
                'Potential Loyalists': 'Offer membership / loyalty program. Recommend products.',
                'Promising': 'Offer free shipping, Add benefits to build long-term relationship.',
                'Need Attention': 'Make limited time offers. Recommend based on past purchases. Reactivate them.',
                'At Risk': 'Send personalized emails. Offer renewals. Provide helpful resources.',
                'About to Sleep': 'Share valuable resources. Recommend popular products. Reconnect with them.',
                'Cant Lose Them': 'Win them back via renewals or newer products. Survey them. Reach out proactively.',
                'Hibernating': 'Offer other relevant products. Use special offers to revive interest.',
                'Lost': 'Revive interest with a reach out campaign. Ignore otherwise.'
            }
            rfm['recommendation'] = rfm['segment'].map(recommendations)

        return rfm

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build asset definitions."""
        asset_name = self.asset_name

        # Require transaction data
        if not self.transaction_data_asset:
            raise ValueError("Transaction data asset is required for customer segmentation")

        asset_ins = {
            "transaction_data": AssetIn(key=AssetKey.from_user_string(self.transaction_data_asset))
        }

        # Optional customer data
        if self.customer_data_asset:
            asset_ins["customer_data"] = AssetIn(key=AssetKey.from_user_string(self.customer_data_asset))

        component = self

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
        _comp_name = "customer_segmentation"  # component directory name
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
            ins=asset_ins,
            description=self.description or "Customer segmentation using RFM analysis",
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=self.group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def customer_segmentation_asset(context: AssetExecutionContext, **inputs) -> pd.DataFrame:
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
            """Segment customers using RFM analysis."""

            context.log.info("Calculating RFM scores...")

            # Get transaction data
            transaction_data = inputs.get('transaction_data')

            # Calculate RFM scores
            rfm = component._calculate_rfm_scores(transaction_data)

            if rfm.empty:
                context.log.warning("No RFM scores calculated")
                return pd.DataFrame()

            context.log.info(f"Calculated RFM scores for {len(rfm)} customers")

            # Assign segments
            if component.use_predefined_segments:
                context.log.info("Assigning RFM segments...")
                rfm = component._assign_segments(rfm)

                # Log segment distribution
                segment_counts = rfm['segment'].value_counts()
                context.log.info("Segment distribution:")
                for segment, count in segment_counts.items():
                    pct = count / len(rfm) * 100
                    context.log.info(f"  {segment}: {count} ({pct:.1f}%)")

                # Calculate segment value if enabled
                if component.calculate_segment_value:
                    segment_value = rfm.groupby('segment').agg({
                        'monetary': ['sum', 'mean', 'count']
                    }).round(2)
                    context.log.info("\nSegment value:")
                    for segment in segment_value.index:
                        total = segment_value.loc[segment, ('monetary', 'sum')]
                        avg = segment_value.loc[segment, ('monetary', 'mean')]
                        context.log.info(f"  {segment}: ${total:,.0f} total, ${avg:,.0f} avg")

            # Add timestamp
            rfm['segmented_at'] = pd.Timestamp.now()

            return rfm

        return build_defs_from_component(
            context=context,
            component=self,
            asset_defs=[customer_segmentation_asset],
        )
