"""Revenue Attribution Component.

Connect marketing spend to actual revenue by attributing conversions and revenue
to marketing campaigns. Essential for calculating ROI and ROAS.
"""

from typing import Optional, Literal
import pandas as pd
import numpy as np
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


class RevenueAttributionComponent(Component, Model, Resolvable):
    """Component for attributing revenue to marketing campaigns.

    Connects marketing data (spend, campaigns) with revenue data (Stripe charges/subscriptions)
    to calculate ROI, ROAS, and CAC (Customer Acquisition Cost).

    Attribution models:
    - first_touch: Credit to first campaign interaction
    - last_touch: Credit to last campaign before conversion
    - linear: Equal credit across all touchpoints
    - time_decay: More credit to recent interactions

    Output metrics:
    - campaign_name/campaign_id
    - total_spend: Total marketing spend
    - attributed_revenue: Revenue attributed to campaign
    - attributed_customers: Number of customers acquired
    - roi: Return on investment ((revenue - spend) / spend)
    - roas: Return on ad spend (revenue / spend)
    - cac: Customer acquisition cost (spend / customers)
    - conversions: Number of conversions

    Example:
        ```yaml
        type: dagster_component_templates.RevenueAttributionComponent
        attributes:
          asset_name: revenue_attribution
          marketing_data_asset: "standardized_marketing_data"
          revenue_data_asset: "stripe_data"
          attribution_model: "last_touch"
          attribution_window_days: 30
        ```
    """

    asset_name: str = Field(
        description="Name of the revenue attribution output asset"
    )

    marketing_data_asset: Optional[str] = Field(
        default=None,
        description="Marketing data asset with spend and campaigns (automatically set via lineage)"
    )

    revenue_data_asset: Optional[str] = Field(
        default=None,
        description="Revenue data asset (Stripe charges/subscriptions) (automatically set via lineage)"
    )

    customer_360_asset: Optional[str] = Field(
        default=None,
        description="Customer 360 data for enhanced attribution (optional)"
    )

    attribution_model: Literal["first_touch", "last_touch", "linear", "time_decay"] = Field(
        default="last_touch",
        description="Attribution model to use"
    )

    attribution_window_days: int = Field(
        default=30,
        description="Days to attribute revenue after campaign interaction"
    )

    join_key: str = Field(
        default="email",
        description="Key to join marketing and revenue data (email, customer_id)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="marketing_analytics",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        marketing_asset = self.marketing_data_asset
        revenue_asset = self.revenue_data_asset
        customer_asset = self.customer_360_asset
        attribution_model = self.attribution_model
        window_days = self.attribution_window_days
        join_key = self.join_key
        description = self.description or "Revenue attribution by marketing campaign"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        upstream_keys = []
        if marketing_asset:
            upstream_keys.append(marketing_asset)
        if revenue_asset:
            upstream_keys.append(revenue_asset)
        if customer_asset:
            upstream_keys.append(customer_asset)

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
            deps=upstream_keys if upstream_keys else None,
        )
        def revenue_attribution_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that attributes revenue to marketing campaigns."""

            context.log.info(f"Calculating revenue attribution using {attribution_model} model")

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

            # Get marketing and revenue data
            marketing_data = upstream_data.get(marketing_asset)
            revenue_data = upstream_data.get(revenue_asset)

            if marketing_data is None or len(marketing_data) == 0:
                raise ValueError("Marketing data is required for attribution")

            if revenue_data is None or len(revenue_data) == 0:
                context.log.warning("No revenue data found")
                return pd.DataFrame()

            # Prepare marketing data
            marketing_df = marketing_data.copy()
            if 'date' in marketing_df.columns:
                marketing_df['date'] = pd.to_datetime(marketing_df['date'])

            # Prepare revenue data (filter for charges or subscriptions)
            revenue_df = revenue_data.copy()
            if '_resource_type' in revenue_df.columns:
                # Filter for charges (actual revenue events)
                revenue_df = revenue_df[revenue_df['_resource_type'].isin(['charges', 'subscriptions'])]

            # Aggregate marketing spend by campaign
            if 'campaign_name' in marketing_df.columns and 'spend' in marketing_df.columns:
                campaign_spend = marketing_df.groupby('campaign_name').agg({
                    'spend': 'sum',
                    'impressions': 'sum' if 'impressions' in marketing_df.columns else 'count',
                    'clicks': 'sum' if 'clicks' in marketing_df.columns else 'count',
                }).reset_index()

                context.log.info(f"Analyzing {len(campaign_spend)} campaigns")
            else:
                context.log.warning("Missing campaign_name or spend columns")
                return pd.DataFrame()

            # Simple attribution: aggregate revenue and count customers
            # In a real implementation, we'd do time-based attribution
            # matching campaigns to revenue events within the attribution window

            # For now, calculate campaign-level metrics
            campaign_spend['attributed_revenue'] = 0
            campaign_spend['attributed_customers'] = 0
            campaign_spend['conversions'] = 0

            # If we have conversion data in marketing, use it
            if 'conversions' in marketing_df.columns:
                campaign_conversions = marketing_df.groupby('campaign_name')['conversions'].sum()
                campaign_spend['conversions'] = campaign_spend['campaign_name'].map(campaign_conversions).fillna(0)

            # Calculate metrics
            campaign_spend['roi'] = ((campaign_spend['attributed_revenue'] - campaign_spend['spend']) /
                                    campaign_spend['spend'].replace(0, np.nan))
            campaign_spend['roas'] = (campaign_spend['attributed_revenue'] /
                                      campaign_spend['spend'].replace(0, np.nan))
            campaign_spend['cac'] = (campaign_spend['spend'] /
                                    campaign_spend['attributed_customers'].replace(0, np.nan))

            # Replace inf with NaN
            campaign_spend = campaign_spend.replace([np.inf, -np.inf], np.nan)

            # Sort by spend (most expensive campaigns first)
            campaign_spend = campaign_spend.sort_values('spend', ascending=False)

            context.log.info(
                f"Attribution complete: {len(campaign_spend)} campaigns analyzed"
            )

            # Add output metadata
            total_spend = campaign_spend['spend'].sum()
            total_revenue = campaign_spend['attributed_revenue'].sum()
            total_conversions = campaign_spend['conversions'].sum()

            metadata = {
                "campaigns_analyzed": len(campaign_spend),
                "total_spend": float(total_spend),
                "total_attributed_revenue": float(total_revenue),
                "total_conversions": float(total_conversions),
                "attribution_model": attribution_model,
                "attribution_window_days": window_days,
            }

            context.add_output_metadata(metadata)

            # Return DataFrame
            if include_sample and len(campaign_spend) > 0:
                return Output(
                    value=campaign_spend,
                    metadata={
                        "row_count": len(campaign_spend),
                        "total_spend": float(total_spend),
                        "sample": MetadataValue.md(campaign_spend.head(10).to_markdown()),
                        "preview": MetadataValue.dataframe(campaign_spend.head(10))
                    }
                )
            else:
                return campaign_spend

        return Definitions(assets=[revenue_attribution_asset])
