"""Subscription Metrics Component.

Calculate key SaaS metrics from Stripe subscription data including MRR, ARR,
churn rate, and customer lifetime value (LTV).
"""

from typing import Optional, Literal
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

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
            deps=upstream_keys if upstream_keys else None,
        )
        def subscription_metrics_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
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
                return metrics_df

        return Definitions(assets=[subscription_metrics_asset])
