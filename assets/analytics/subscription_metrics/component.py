"""Subscription Metrics Component.

Calculate key SaaS metrics from Stripe subscription data including MRR, ARR,
churn rate, and customer lifetime value (LTV).
"""

from typing import Dict, List, Literal, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dagster import (
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
    Output,
    MetadataValue,
)
from pydantic import Field


class SubscriptionMetricsComponent(Component, Model, Resolvable):
    """Component for calculating subscription and revenue metrics.

    Analyzes Stripe subscription data to calculate key SaaS metrics including:
    - MRR (Monthly Recurring Revenue): Current monthly recurring revenue
    - ARR (Annual Recurring Revenue): MRR * 12
    - Churn Rate: Percentage of customers who cancelled
    - Customer Lifetime Value (LTV): Average revenue per customer over lifetime
    - New MRR: Revenue from new subscriptions
    - Expansion MRR: Revenue from upgrades
    - Contraction MRR: Revenue lost from downgrades
    - Net MRR Growth Rate: Overall MRR growth percentage
    - ARPU (Average Revenue Per User): MRR / Active Customers

    The component expects Stripe data with subscriptions and optionally customers.
    It can also accept revenue_data for more comprehensive LTV calculations.

    Output columns:
    - metric_date: Date of the metric snapshot
    - mrr: Monthly recurring revenue
    - arr: Annual recurring revenue
    - active_subscriptions: Number of active subscriptions
    - new_subscriptions: New subscriptions this period
    - churned_subscriptions: Cancelled subscriptions
    - churn_rate: Percentage of customers who cancelled
    - new_mrr: MRR from new subscriptions
    - expansion_mrr: MRR from upgrades
    - contraction_mrr: MRR from downgrades
    - net_mrr_growth_rate: Overall MRR growth percentage
    - arpu: Average revenue per user
    - ltv: Customer lifetime value

    Example:
        ```yaml
        type: dagster_component_templates.SubscriptionMetricsComponent
        attributes:
          asset_name: subscription_metrics
          stripe_data_asset: "stripe_data"
          calculation_period: "monthly"
          ltv_method: "historical"
        ```
    """

    asset_name: str = Field(
        description="Name of the subscription metrics output asset"
    )

    stripe_data_asset: Optional[str] = Field(
        default=None,
        description="Stripe data asset with subscriptions (automatically set via lineage)"
    )

    revenue_data_asset: Optional[str] = Field(
        default=None,
        description="Revenue data for enhanced LTV calculations (optional)"
    )

    calculation_period: Literal["daily", "weekly", "monthly"] = Field(
        default="monthly",
        description="Time period for metrics calculation"
    )

    ltv_method: Literal["historical", "predictive"] = Field(
        default="historical",
        description="Method for calculating LTV (historical average or predictive)"
    )

    lookback_months: int = Field(
        default=12,
        description="Months to look back for historical calculations"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="subscription_analytics",
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
        stripe_asset = self.stripe_data_asset
        revenue_asset = self.revenue_data_asset
        calculation_period = self.calculation_period
        ltv_method = self.ltv_method
        lookback_months = self.lookback_months
        description = self.description or "Subscription and revenue metrics (MRR, ARR, churn, LTV)"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        upstream_keys = []
        if stripe_asset:
            upstream_keys.append(stripe_asset)
        if revenue_asset:
            upstream_keys.append(revenue_asset)

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
        _comp_name = "subscription_metrics"  # component directory name
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
        def subscription_metrics_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
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
            """Asset that calculates subscription metrics from Stripe data."""

            context.log.info(f"Calculating subscription metrics with {calculation_period} granularity")

            # Load upstream data
            upstream_data = {}
            if upstream_keys and hasattr(context, 'load_asset_value'):
                for key in upstream_keys:
                    try:
                        value = context.load_asset_value(AssetKey(key))
                        upstream_data[key] = value
                        context.log.info(f"Loaded {key}: {len(value)} rows")
                    except Exception as e:
                        context.log.warning(f"Could not load {key}: {e}")
            else:
                upstream_data = kwargs

            # Get Stripe data
            stripe_data = upstream_data.get(stripe_asset)

            if stripe_data is None or len(stripe_data) == 0:
                raise ValueError("Stripe data is required for subscription metrics")

            # Filter for subscriptions
            subscriptions_df = stripe_data.copy()
            if '_resource_type' in subscriptions_df.columns:
                subscriptions_df = subscriptions_df[subscriptions_df['_resource_type'] == 'subscriptions']

            if len(subscriptions_df) == 0:
                context.log.warning("No subscription data found in Stripe data")
                return pd.DataFrame()

            context.log.info(f"Analyzing {len(subscriptions_df)} subscriptions")

            # Prepare datetime columns
            date_columns = ['created', 'current_period_start', 'current_period_end', 'canceled_at', 'ended_at']
            for col in date_columns:
                if col in subscriptions_df.columns:
                    subscriptions_df[col] = pd.to_datetime(subscriptions_df[col], unit='s', errors='coerce')

            # Get current active subscriptions
            current_date = pd.Timestamp.now()
            active_subs = subscriptions_df[
                (subscriptions_df['status'].isin(['active', 'trialing'])) |
                ((subscriptions_df['status'] == 'canceled') &
                 (subscriptions_df['current_period_end'] > current_date))
            ]

            # Calculate MRR from active subscriptions
            # Note: Stripe stores amounts in cents
            if 'plan_amount' in subscriptions_df.columns:
                amount_col = 'plan_amount'
            elif 'items_data_0_price_unit_amount' in subscriptions_df.columns:
                amount_col = 'items_data_0_price_unit_amount'
            else:
                context.log.warning("No amount column found, using default")
                active_subs['monthly_amount'] = 0

            if amount_col in active_subs.columns:
                # Convert cents to dollars
                active_subs['monthly_amount'] = active_subs[amount_col] / 100

                # Normalize to monthly (handle annual plans)
                if 'plan_interval' in active_subs.columns:
                    active_subs['monthly_amount'] = active_subs.apply(
                        lambda row: row['monthly_amount'] / 12 if row.get('plan_interval') == 'year'
                        else row['monthly_amount'],
                        axis=1
                    )

            # Calculate current metrics
            current_mrr = active_subs['monthly_amount'].sum()
            current_arr = current_mrr * 12
            active_subscription_count = len(active_subs)

            # Calculate churn metrics
            lookback_date = current_date - pd.DateOffset(months=lookback_months)

            # Subscriptions that were active at start of period
            active_at_start = subscriptions_df[
                (subscriptions_df['created'] < lookback_date) &
                ((subscriptions_df['canceled_at'].isna()) | (subscriptions_df['canceled_at'] > lookback_date))
            ]

            # Subscriptions that churned during period
            churned = subscriptions_df[
                (subscriptions_df['canceled_at'] >= lookback_date) &
                (subscriptions_df['canceled_at'] <= current_date)
            ]

            # New subscriptions in period
            new_subs = subscriptions_df[
                (subscriptions_df['created'] >= lookback_date) &
                (subscriptions_df['created'] <= current_date)
            ]

            churn_count = len(churned)
            new_count = len(new_subs)

            # Calculate churn rate
            if len(active_at_start) > 0:
                churn_rate = (churn_count / len(active_at_start)) * 100
            else:
                churn_rate = 0

            # Calculate MRR changes
            if 'monthly_amount' in new_subs.columns:
                new_mrr = new_subs['monthly_amount'].sum()
            else:
                new_mrr = 0

            # For expansion/contraction, we'd need historical subscription changes
            # This is simplified - in production you'd track plan changes
            expansion_mrr = 0
            contraction_mrr = 0

            # Calculate net MRR growth
            churned_mrr = 0
            if 'monthly_amount' in churned.columns:
                churned_mrr = churned['monthly_amount'].sum()

            net_mrr_change = new_mrr + expansion_mrr - contraction_mrr - churned_mrr
            if current_mrr > 0:
                net_mrr_growth_rate = (net_mrr_change / current_mrr) * 100
            else:
                net_mrr_growth_rate = 0

            # Calculate ARPU
            if active_subscription_count > 0:
                arpu = current_mrr / active_subscription_count
            else:
                arpu = 0

            # Calculate LTV
            if ltv_method == "historical":
                # LTV = ARPU / Churn Rate (monthly)
                monthly_churn_rate = churn_rate / lookback_months / 100
                if monthly_churn_rate > 0:
                    ltv = arpu / monthly_churn_rate
                else:
                    ltv = 0
            else:
                # Predictive LTV could use more sophisticated models
                # For now, use simple average customer lifetime
                avg_lifetime_months = 1 / (monthly_churn_rate if monthly_churn_rate > 0 else 0.01)
                ltv = arpu * avg_lifetime_months

            # Create metrics DataFrame
            metrics_df = pd.DataFrame([{
                'metric_date': current_date.date(),
                'mrr': float(current_mrr),
                'arr': float(current_arr),
                'active_subscriptions': int(active_subscription_count),
                'new_subscriptions': int(new_count),
                'churned_subscriptions': int(churn_count),
                'churn_rate': float(churn_rate),
                'new_mrr': float(new_mrr),
                'expansion_mrr': float(expansion_mrr),
                'contraction_mrr': float(contraction_mrr),
                'net_mrr_growth_rate': float(net_mrr_growth_rate),
                'arpu': float(arpu),
                'ltv': float(ltv),
            }])

            context.log.info(
                f"Metrics calculated - MRR: ${current_mrr:,.2f}, "
                f"Active subs: {active_subscription_count}, "
                f"Churn rate: {churn_rate:.2f}%"
            )

            # Add output metadata
            metadata = {
                "mrr": f"${current_mrr:,.2f}",
                "arr": f"${current_arr:,.2f}",
                "active_subscriptions": active_subscription_count,
                "churn_rate": f"{churn_rate:.2f}%",
                "ltv": f"${ltv:,.2f}",
                "calculation_period": calculation_period,
                "lookback_months": lookback_months,
            }

            context.add_output_metadata(metadata)

            # Return DataFrame
            if include_sample:
                return Output(
                    value=metrics_df,
                    metadata={
                        "row_count": len(metrics_df),
                        "mrr": f"${current_mrr:,.2f}",
                        "arr": f"${current_arr:,.2f}",
                        "preview": MetadataValue.dataframe(metrics_df)
                    }
                )
            else:
                # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(metrics_df.dtypes[col]))
                for col in metrics_df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(metrics_df)),
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
                return metrics_df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[subscription_metrics_asset])


        return Definitions(assets=[subscription_metrics_asset], asset_checks=list(_schema_checks))
