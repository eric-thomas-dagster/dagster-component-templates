"""Marketing Data Standardizer Component.

Transform platform-specific marketing data (Facebook Ads, Google Ads, etc.) into a
standardized common schema for cross-platform analysis. Similar to Supermetrics and
Funnel.io data model standardization.
"""

from typing import Optional, Literal
import pandas as pd
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


class MarketingDataStandardizerComponent(Component, Model, Resolvable):
    """Component for standardizing marketing data across platforms.

    Transforms platform-specific schemas (Facebook Ads, Google Ads, etc.) into a
    unified marketing data model with consistent field names and metrics.

    Standard Schema Output:
    - date: Date of the metrics
    - platform: Source platform (facebook_ads, google_ads, etc.)
    - campaign_id: Campaign identifier
    - campaign_name: Campaign name
    - ad_id: Ad identifier (if applicable)
    - ad_name: Ad name (if applicable)
    - impressions: Number of times ads were shown
    - clicks: Number of clicks
    - spend: Amount spent (in account currency)
    - conversions: Number of conversions
    - conversion_value: Total conversion value
    - reach: Unique users reached (if available)
    - ctr: Click-through rate (%)
    - cpc: Cost per click
    - cpm: Cost per 1000 impressions
    - cpa: Cost per acquisition/conversion
    - roas: Return on ad spend

    Example:
        ```yaml
        type: dagster_component_templates.MarketingDataStandardizerComponent
        attributes:
          asset_name: standardized_marketing_data
          platform: "facebook_ads"
          source_table: "facebook_ads_insights"
        ```
    """

    asset_name: str = Field(
        description="Name of the standardized output asset"
    )

    platform: Literal["facebook_ads", "google_ads", "linkedin_ads", "tiktok_ads", "twitter_ads"] = Field(
        description="Source platform to standardize"
    )

    source_asset: Optional[str] = Field(
        default=None,
        description="Upstream asset containing raw platform data (automatically set via lineage)"
    )

    campaign_id_field: Optional[str] = Field(
        default=None,
        description="Field name for campaign ID (auto-detected if not provided)"
    )

    campaign_name_field: Optional[str] = Field(
        default=None,
        description="Field name for campaign name (auto-detected if not provided)"
    )

    date_field: Optional[str] = Field(
        default=None,
        description="Field name for date (auto-detected if not provided)"
    )

    # Cost/spend field configuration
    spend_field: Optional[str] = Field(
        default=None,
        description="Field name for spend/cost (auto-detected if not provided)"
    )

    spend_multiplier: float = Field(
        default=1.0,
        description="Multiplier to convert spend to currency units (e.g., 0.000001 for micros)"
    )

    # Optional filters
    filter_date_from: Optional[str] = Field(
        default=None,
        description="Filter data from this date (YYYY-MM-DD)"
    )

    filter_date_to: Optional[str] = Field(
        default=None,
        description="Filter data to this date (YYYY-MM-DD)"
    )

    filter_campaign_status: Optional[str] = Field(
        default=None,
        description="Filter by campaign status (e.g., 'ACTIVE,PAUSED')"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="marketing_standardized",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        platform = self.platform
        source_asset = self.source_asset
        campaign_id_field = self.campaign_id_field
        campaign_name_field = self.campaign_name_field
        date_field = self.date_field
        spend_field = self.spend_field
        spend_multiplier = self.spend_multiplier
        filter_date_from = self.filter_date_from
        filter_date_to = self.filter_date_to
        filter_campaign_status = self.filter_campaign_status
        description = self.description or f"Standardized {platform} marketing data"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        # Parse upstream asset keys
        upstream_keys = []
        if source_asset:
            upstream_keys = [source_asset]

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
            deps=upstream_keys if upstream_keys else None,
        )
        def marketing_standardizer_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that standardizes platform-specific marketing data."""

            context.log.info(f"Standardizing {platform} marketing data")

            # Load upstream data
            if upstream_keys and hasattr(context, 'load_asset_value'):
                context.log.info(f"Loading data from upstream asset: {source_asset}")
                raw_data = context.load_asset_value(AssetKey(source_asset))
            elif kwargs:
                raw_data = list(kwargs.values())[0]
            else:
                raise ValueError(
                    f"Marketing Standardizer '{asset_name}' requires upstream data. "
                    f"Connect to a platform ingestion component (Facebook Ads, Google Ads, etc.)"
                )

            # Convert to DataFrame if needed
            if isinstance(raw_data, dict):
                if 'data' in raw_data:
                    df = pd.DataFrame(raw_data['data'])
                elif 'rows' in raw_data:
                    df = pd.DataFrame(raw_data['rows'])
                else:
                    # Try to use the dict directly
                    df = pd.DataFrame([raw_data])
            elif isinstance(raw_data, pd.DataFrame):
                df = raw_data
            else:
                raise TypeError(f"Unexpected data type: {type(raw_data)}")

            context.log.info(f"Raw data: {len(df)} rows, {len(df.columns)} columns")
            original_rows = len(df)

            # Platform-specific field mappings
            field_mappings = {
                "facebook_ads": {
                    "date": ["date_start", "date", "created_time"],
                    "campaign_id": ["campaign_id"],
                    "campaign_name": ["campaign_name"],
                    "ad_id": ["ad_id"],
                    "ad_name": ["ad_name"],
                    "impressions": ["impressions"],
                    "clicks": ["clicks"],
                    "spend": ["spend"],
                    "conversions": ["conversions", "actions"],
                    "conversion_value": ["conversion_values", "action_values"],
                    "reach": ["reach"],
                    "ctr": ["ctr"],
                    "cpc": ["cpc"],
                    "cpm": ["cpm"],
                },
                "google_ads": {
                    "date": ["date", "day"],
                    "campaign_id": ["campaign_id", "campaignId"],
                    "campaign_name": ["campaign_name", "campaignName", "name"],
                    "ad_id": ["ad_id", "adId"],
                    "ad_name": ["ad_name", "adName"],
                    "impressions": ["impressions", "metrics_impressions"],
                    "clicks": ["clicks", "metrics_clicks"],
                    "spend": ["cost_micros", "cost", "metrics_cost_micros"],
                    "conversions": ["conversions", "metrics_conversions"],
                    "conversion_value": ["conversions_value", "metrics_conversions_value"],
                    "reach": ["reach", "unique_users"],
                    "ctr": ["ctr", "metrics_ctr"],
                    "cpc": ["average_cpc", "metrics_average_cpc"],
                    "cpm": ["average_cpm", "metrics_average_cpm"],
                },
                "linkedin_ads": {
                    "date": ["start_at", "date", "day"],
                    "campaign_id": ["campaign_id", "campaignId"],
                    "campaign_name": ["campaign_name", "campaignName"],
                    "ad_id": ["creative_id", "creativeId"],
                    "ad_name": ["creative_name", "creativeName"],
                    "impressions": ["impressions"],
                    "clicks": ["clicks"],
                    "spend": ["cost_in_local_currency", "spend"],
                    "conversions": ["conversions", "external_website_conversions"],
                    "conversion_value": ["conversion_value_in_local_currency"],
                },
                "tiktok_ads": {
                    "date": ["stat_time_day", "date"],
                    "campaign_id": ["campaign_id"],
                    "campaign_name": ["campaign_name"],
                    "ad_id": ["ad_id"],
                    "ad_name": ["ad_name"],
                    "impressions": ["impressions"],
                    "clicks": ["clicks"],
                    "spend": ["spend"],
                    "conversions": ["conversions", "complete_payment"],
                    "conversion_value": ["conversion_value"],
                },
                "twitter_ads": {
                    "date": ["date"],
                    "campaign_id": ["campaign_id"],
                    "campaign_name": ["campaign_name"],
                    "ad_id": ["promoted_tweet_id"],
                    "ad_name": ["promoted_tweet_name"],
                    "impressions": ["impressions"],
                    "clicks": ["url_clicks"],
                    "spend": ["billed_charge_local_micro", "spend"],
                    "conversions": ["conversions"],
                },
            }

            mapping = field_mappings.get(platform)
            if not mapping:
                raise ValueError(f"Unsupported platform: {platform}")

            # Helper function to find field in DataFrame
            def find_field(possible_names, custom_field=None):
                if custom_field and custom_field in df.columns:
                    return custom_field
                for name in possible_names:
                    if name in df.columns:
                        return name
                return None

            # Build standardized DataFrame
            standardized_data = {}

            # Platform identifier
            standardized_data['platform'] = platform

            # Date field
            date_col = find_field(mapping['date'], date_field)
            if date_col:
                standardized_data['date'] = pd.to_datetime(df[date_col]).dt.date
            else:
                context.log.warning("Date field not found")

            # Campaign fields
            campaign_id_col = find_field(mapping['campaign_id'], campaign_id_field)
            if campaign_id_col:
                standardized_data['campaign_id'] = df[campaign_id_col].astype(str)

            campaign_name_col = find_field(mapping['campaign_name'], campaign_name_field)
            if campaign_name_col:
                standardized_data['campaign_name'] = df[campaign_name_col]

            # Ad fields
            ad_id_col = find_field(mapping.get('ad_id', []))
            if ad_id_col:
                standardized_data['ad_id'] = df[ad_id_col].astype(str)

            ad_name_col = find_field(mapping.get('ad_name', []))
            if ad_name_col:
                standardized_data['ad_name'] = df[ad_name_col]

            # Metrics
            impressions_col = find_field(mapping['impressions'])
            if impressions_col:
                standardized_data['impressions'] = pd.to_numeric(df[impressions_col], errors='coerce')

            clicks_col = find_field(mapping['clicks'])
            if clicks_col:
                standardized_data['clicks'] = pd.to_numeric(df[clicks_col], errors='coerce')

            # Spend (handle micros conversion)
            spend_col = find_field(mapping['spend'], spend_field)
            if spend_col:
                standardized_data['spend'] = pd.to_numeric(df[spend_col], errors='coerce') * spend_multiplier

            # Conversions
            conversions_col = find_field(mapping['conversions'])
            if conversions_col:
                standardized_data['conversions'] = pd.to_numeric(df[conversions_col], errors='coerce')

            conversion_value_col = find_field(mapping.get('conversion_value', []))
            if conversion_value_col:
                standardized_data['conversion_value'] = pd.to_numeric(df[conversion_value_col], errors='coerce')

            # Reach
            reach_col = find_field(mapping.get('reach', []))
            if reach_col:
                standardized_data['reach'] = pd.to_numeric(df[reach_col], errors='coerce')

            # Create standardized DataFrame
            std_df = pd.DataFrame(standardized_data)

            # Calculate derived metrics
            if 'clicks' in std_df.columns and 'impressions' in std_df.columns:
                std_df['ctr'] = (std_df['clicks'] / std_df['impressions'] * 100).round(2)

            if 'spend' in std_df.columns and 'clicks' in std_df.columns:
                std_df['cpc'] = (std_df['spend'] / std_df['clicks']).round(2)

            if 'spend' in std_df.columns and 'impressions' in std_df.columns:
                std_df['cpm'] = (std_df['spend'] / std_df['impressions'] * 1000).round(2)

            if 'spend' in std_df.columns and 'conversions' in std_df.columns:
                std_df['cpa'] = (std_df['spend'] / std_df['conversions']).round(2)

            if 'conversion_value' in std_df.columns and 'spend' in std_df.columns:
                std_df['roas'] = (std_df['conversion_value'] / std_df['spend']).round(2)

            # Apply filters
            if filter_date_from and 'date' in std_df.columns:
                std_df = std_df[std_df['date'] >= pd.to_datetime(filter_date_from).date()]
                context.log.info(f"Filtered from date: {filter_date_from}")

            if filter_date_to and 'date' in std_df.columns:
                std_df = std_df[std_df['date'] <= pd.to_datetime(filter_date_to).date()]
                context.log.info(f"Filtered to date: {filter_date_to}")

            # Replace inf and -inf with NaN
            std_df = std_df.replace([float('inf'), float('-inf')], pd.NA)

            final_rows = len(std_df)
            context.log.info(
                f"Standardization complete: {original_rows} → {final_rows} rows, "
                f"{len(std_df.columns)} columns"
            )

            # Add metadata
            context.add_output_metadata({
                "platform": platform,
                "original_rows": original_rows,
                "final_rows": final_rows,
                "columns": list(std_df.columns),
                "date_range": f"{std_df['date'].min()} to {std_df['date'].max()}" if 'date' in std_df.columns else "N/A",
                "total_spend": float(std_df['spend'].sum()) if 'spend' in std_df.columns else 0,
                "total_impressions": int(std_df['impressions'].sum()) if 'impressions' in std_df.columns else 0,
                "total_clicks": int(std_df['clicks'].sum()) if 'clicks' in std_df.columns else 0,
                "total_conversions": float(std_df['conversions'].sum()) if 'conversions' in std_df.columns else 0,
            })

            # Return DataFrame
            if include_sample and len(std_df) > 0:
                return Output(
                    value=std_df,
                    metadata={
                        "row_count": len(std_df),
                        "columns": std_df.columns.tolist(),
                        "sample": MetadataValue.md(std_df.head(10).to_markdown()),
                        "preview": MetadataValue.dataframe(std_df.head(10))
                    }
                )
            else:
                return std_df

        return Definitions(assets=[marketing_standardizer_asset])
