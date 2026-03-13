"""Lifetime Value (LTV) Prediction Component.

Predict customer lifetime value using historical purchase patterns and cohort analysis.
Calculate both historical LTV and projected future value.
"""

from typing import Dict, List, Optional
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

    upstream_asset_key: str = Field(
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
        include_sample = self.include_sample_metadata

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


        @asset(
            name=asset_name,
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
        )
        def ltv_prediction_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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

            # Calculate summary statistics
            total_predicted_ltv = result_df['predicted_total_ltv'].sum()
            avg_predicted_ltv = result_df['predicted_total_ltv'].mean()
            median_predicted_ltv = result_df['predicted_total_ltv'].median()

            # Add metadata
            metadata = {
                "row_count": len(result_df),
                "total_customers": len(result_df),
                "total_predicted_ltv": round(total_predicted_ltv, 2),
                "avg_predicted_ltv": round(avg_predicted_ltv, 2),
                "median_predicted_ltv": round(median_predicted_ltv, 2),
                "prediction_period_months": prediction_period_months,
                "min_transactions_required": min_transactions,
                "value_segments": MetadataValue.md(segment_dist.to_frame('count').to_markdown())
            }

            # Return with metadata
            if include_sample and len(result_df) > 0:
                # Sort by predicted LTV descending to show highest value customers first
                result_sorted = result_df.sort_values('predicted_total_ltv', ascending=False)

                return Output(
                    value=result_df,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(result_sorted.head(10).to_markdown(index=False)),
                        "preview": MetadataValue.dataframe(result_sorted.head(10))
                    }
                )
            else:
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
                return result_df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[ltv_prediction_asset])


        return Definitions(assets=[ltv_prediction_asset], asset_checks=list(_schema_checks))
