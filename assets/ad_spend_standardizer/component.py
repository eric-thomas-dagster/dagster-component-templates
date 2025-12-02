"""Ad Spend Standardizer Component.

Unifies advertising spend data from multiple platforms (Google Ads, Facebook Ads, etc.)
into a standardized schema for cross-platform analysis and attribution.
"""

from typing import Optional
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


class AdSpendStandardizerComponent(Component, Model, Resolvable):
    """Component for standardizing advertising spend data across platforms.

    Unifies ad campaign data from Google Ads, Facebook Ads, and other platforms
    into a common schema. Essential for cross-platform campaign comparison and
    marketing attribution analysis.

    Standard output schema:
    - campaign_id: Unique campaign identifier
    - campaign_name: Campaign display name
    - platform: Advertising platform (google_ads, facebook_ads, etc.)
    - campaign_type: Campaign type (search, display, shopping, social)
    - date: Campaign date
    - impressions: Number of ad impressions
    - clicks: Number of clicks
    - spend: Amount spent (normalized currency)
    - conversions: Number of conversions
    - conversion_value: Value of conversions
    - ctr: Click-through rate (clicks/impressions)
    - cpc: Cost per click
    - cpa: Cost per acquisition
    - roas: Return on ad spend
    - ad_group_id: Ad group identifier (if available)
    - ad_group_name: Ad group name (if available)
    - keyword: Target keyword (for search campaigns)
    - device_type: Device type (mobile, desktop, tablet)
    - location: Geographic location/region

    Example:
        ```yaml
        type: dagster_component_templates.AdSpendStandardizerComponent
        attributes:
          asset_name: standardized_ad_spend
          google_ads_asset: "google_ads_data"
          facebook_ads_asset: "facebook_ads_data"
          currency_normalization: "USD"
        ```
    """

    asset_name: str = Field(
        description="Name of the standardized ad spend output asset"
    )

    # Input assets (set via visual lineage)
    google_ads_asset: Optional[str] = Field(
        default=None,
        description="Google Ads data asset (automatically set via lineage)"
    )

    facebook_ads_asset: Optional[str] = Field(
        default=None,
        description="Facebook Ads data asset (automatically set via lineage)"
    )

    other_ad_platform_asset: Optional[str] = Field(
        default=None,
        description="Other ad platform data asset (automatically set via lineage)"
    )

    # Configuration
    currency_normalization: str = Field(
        default="USD",
        description="Normalize all spend to this currency (USD, EUR, GBP)"
    )

    date_column_name: str = Field(
        default="date",
        description="Name of the date column in source data"
    )

    aggregate_by_day: bool = Field(
        default=True,
        description="Aggregate metrics by day (recommended for consistency)"
    )

    include_zero_spend_days: bool = Field(
        default=False,
        description="Include days with zero spend in output"
    )

    calculate_derived_metrics: bool = Field(
        default=True,
        description="Calculate CTR, CPC, CPA, ROAS automatically"
    )

    description: Optional[str] = Field(
        default="",
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="standardizers",
        description="Asset group name"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions for ad spend standardization."""

        # Capture configuration
        asset_name = self.asset_name
        google_ads_asset = self.google_ads_asset
        facebook_ads_asset = self.facebook_ads_asset
        other_ad_platform_asset = self.other_ad_platform_asset
        currency_normalization = self.currency_normalization
        date_column_name = self.date_column_name
        aggregate_by_day = self.aggregate_by_day
        include_zero_spend_days = self.include_zero_spend_days
        calculate_derived_metrics = self.calculate_derived_metrics
        description = self.description or "Standardized advertising spend data"
        group_name = self.group_name or "standardizers"

        # Build dependency list
        deps = []
        if google_ads_asset:
            deps.append(AssetKey(google_ads_asset))
        if facebook_ads_asset:
            deps.append(AssetKey(facebook_ads_asset))
        if other_ad_platform_asset:
            deps.append(AssetKey(other_ad_platform_asset))

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
            deps=deps if deps else None,
        )
        def ad_spend_standardizer_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Standardize ad spend data from multiple platforms."""

            standardized_datasets = []

            # Process Google Ads data
            if google_ads_asset:
                try:
                    google_ads_df = context.load_asset_value(AssetKey(google_ads_asset))
                    context.log.info(f"Loaded Google Ads data: {len(google_ads_df)} rows")

                    standardized_google = self._standardize_google_ads(
                        google_ads_df,
                        date_column_name,
                        currency_normalization
                    )
                    standardized_datasets.append(standardized_google)
                    context.log.info(f"Standardized Google Ads: {len(standardized_google)} rows")
                except Exception as e:
                    context.log.warning(f"Could not process Google Ads data: {e}")

            # Process Facebook Ads data
            if facebook_ads_asset:
                try:
                    facebook_ads_df = context.load_asset_value(AssetKey(facebook_ads_asset))
                    context.log.info(f"Loaded Facebook Ads data: {len(facebook_ads_df)} rows")

                    standardized_facebook = self._standardize_facebook_ads(
                        facebook_ads_df,
                        date_column_name,
                        currency_normalization
                    )
                    standardized_datasets.append(standardized_facebook)
                    context.log.info(f"Standardized Facebook Ads: {len(standardized_facebook)} rows")
                except Exception as e:
                    context.log.warning(f"Could not process Facebook Ads data: {e}")

            # Process other platform data
            if other_ad_platform_asset:
                try:
                    other_ads_df = context.load_asset_value(AssetKey(other_ad_platform_asset))
                    context.log.info(f"Loaded other ad platform data: {len(other_ads_df)} rows")

                    standardized_other = self._standardize_generic_ads(
                        other_ads_df,
                        date_column_name,
                        currency_normalization
                    )
                    standardized_datasets.append(standardized_other)
                    context.log.info(f"Standardized other ads: {len(standardized_other)} rows")
                except Exception as e:
                    context.log.warning(f"Could not process other ad platform data: {e}")

            if not standardized_datasets:
                context.log.warning("No ad platform data available, returning empty DataFrame")
                return pd.DataFrame(columns=self._get_standard_columns())

            # Combine all standardized data
            combined_df = pd.concat(standardized_datasets, ignore_index=True)
            context.log.info(f"Combined data: {len(combined_df)} rows from {len(standardized_datasets)} platforms")

            # Aggregate by day if requested
            if aggregate_by_day:
                combined_df = self._aggregate_by_day(combined_df)
                context.log.info(f"Aggregated by day: {len(combined_df)} rows")

            # Calculate derived metrics
            if calculate_derived_metrics:
                combined_df = self._calculate_derived_metrics(combined_df)

            # Filter zero spend days if requested
            if not include_zero_spend_days:
                original_count = len(combined_df)
                combined_df = combined_df[combined_df['spend'] > 0]
                context.log.info(f"Filtered zero spend days: {original_count} → {len(combined_df)} rows")

            # Sort by date and platform
            combined_df = combined_df.sort_values(['date', 'platform', 'campaign_name'])

            context.log.info(f"✅ Standardized ad spend data: {len(combined_df)} rows")
            context.log.info(f"Platforms: {combined_df['platform'].unique().tolist()}")
            context.log.info(f"Date range: {combined_df['date'].min()} to {combined_df['date'].max()}")
            context.log.info(f"Total spend: {combined_df['spend'].sum():.2f} {currency_normalization}")

            return Output(
                combined_df,
                metadata={
                    "row_count": len(combined_df),
                    "platforms": MetadataValue.text(", ".join(combined_df['platform'].unique())),
                    "total_spend": float(combined_df['spend'].sum()),
                    "total_clicks": int(combined_df['clicks'].sum()),
                    "total_impressions": int(combined_df['impressions'].sum()),
                    "avg_ctr": float(combined_df['ctr'].mean()),
                    "date_range": f"{combined_df['date'].min()} to {combined_df['date'].max()}",
                    "preview": MetadataValue.md(combined_df.head(10).to_markdown()),
                }
            )

        return Definitions(assets=[ad_spend_standardizer_asset])

    def _get_standard_columns(self) -> list[str]:
        """Get list of standard column names."""
        return [
            'campaign_id', 'campaign_name', 'platform', 'campaign_type', 'date',
            'impressions', 'clicks', 'spend', 'conversions', 'conversion_value',
            'ctr', 'cpc', 'cpa', 'roas', 'ad_group_id', 'ad_group_name',
            'keyword', 'device_type', 'location'
        ]

    def _standardize_google_ads(
        self,
        df: pd.DataFrame,
        date_col: str,
        currency: str
    ) -> pd.DataFrame:
        """Standardize Google Ads data format."""

        # Common Google Ads field mappings
        field_mappings = {
            # Campaign fields
            'campaign_id': ['campaign_id', 'campaignId', 'id'],
            'campaign_name': ['campaign_name', 'campaignName', 'campaign', 'name'],
            'campaign_type': ['campaign_type', 'campaignType', 'advertising_channel_type'],

            # Date
            'date': [date_col, 'date', 'day', 'segments_date'],

            # Metrics
            'impressions': ['impressions', 'metrics_impressions'],
            'clicks': ['clicks', 'metrics_clicks'],
            'spend': ['cost', 'spend', 'cost_micros', 'metrics_cost_micros'],
            'conversions': ['conversions', 'metrics_conversions', 'all_conversions'],
            'conversion_value': ['conversion_value', 'metrics_conversions_value', 'conversions_value'],

            # Additional fields
            'ad_group_id': ['ad_group_id', 'adGroupId', 'adgroup_id'],
            'ad_group_name': ['ad_group_name', 'adGroupName', 'adgroup_name', 'adgroup'],
            'keyword': ['keyword_text', 'keyword', 'criteria', 'search_term'],
            'device_type': ['device', 'device_type', 'segments_device'],
            'location': ['location', 'geographic_location', 'country', 'region'],
        }

        standardized = pd.DataFrame()

        # Map fields
        for std_field, possible_names in field_mappings.items():
            for name in possible_names:
                if name in df.columns:
                    standardized[std_field] = df[name]
                    break

            # Set default if not found
            if std_field not in standardized.columns:
                if std_field in ['impressions', 'clicks', 'conversions']:
                    standardized[std_field] = 0
                elif std_field == 'spend':
                    standardized[std_field] = 0.0
                else:
                    standardized[std_field] = None

        # Convert Google Ads micros to currency units
        if 'cost_micros' in df.columns or 'metrics_cost_micros' in df.columns:
            standardized['spend'] = standardized['spend'] / 1_000_000

        # Add platform identifier
        standardized['platform'] = 'google_ads'

        # Ensure date is datetime
        standardized['date'] = pd.to_datetime(standardized['date'])

        # Set defaults for calculated metrics (will be calculated later)
        standardized['ctr'] = None
        standardized['cpc'] = None
        standardized['cpa'] = None
        standardized['roas'] = None

        return standardized

    def _standardize_facebook_ads(
        self,
        df: pd.DataFrame,
        date_col: str,
        currency: str
    ) -> pd.DataFrame:
        """Standardize Facebook Ads data format."""

        field_mappings = {
            'campaign_id': ['campaign_id', 'campaignId', 'id'],
            'campaign_name': ['campaign_name', 'campaignName', 'campaign', 'name'],
            'campaign_type': ['objective', 'campaign_objective', 'buying_type'],
            'date': [date_col, 'date', 'date_start'],
            'impressions': ['impressions', 'reach'],
            'clicks': ['clicks', 'link_clicks', 'inline_link_clicks'],
            'spend': ['spend', 'amount_spent', 'cost'],
            'conversions': ['conversions', 'actions', 'purchases'],
            'conversion_value': ['conversion_value', 'action_values', 'purchase_value'],
            'ad_group_id': ['adset_id', 'adsetId', 'ad_set_id'],
            'ad_group_name': ['adset_name', 'adsetName', 'ad_set_name'],
            'device_type': ['platform_position', 'device_platform'],
            'location': ['region', 'country', 'location'],
        }

        standardized = pd.DataFrame()

        for std_field, possible_names in field_mappings.items():
            for name in possible_names:
                if name in df.columns:
                    standardized[std_field] = df[name]
                    break

            if std_field not in standardized.columns:
                if std_field in ['impressions', 'clicks', 'conversions']:
                    standardized[std_field] = 0
                elif std_field == 'spend':
                    standardized[std_field] = 0.0
                else:
                    standardized[std_field] = None

        standardized['platform'] = 'facebook_ads'
        standardized['date'] = pd.to_datetime(standardized['date'])
        standardized['keyword'] = None  # Facebook doesn't use keywords
        standardized['ctr'] = None
        standardized['cpc'] = None
        standardized['cpa'] = None
        standardized['roas'] = None

        return standardized

    def _standardize_generic_ads(
        self,
        df: pd.DataFrame,
        date_col: str,
        currency: str
    ) -> pd.DataFrame:
        """Standardize generic ad platform data."""

        # Attempt to map common fields
        standardized = pd.DataFrame()

        # Try to identify standard fields
        for col in df.columns:
            col_lower = col.lower()
            if 'campaign' in col_lower and 'id' in col_lower:
                standardized['campaign_id'] = df[col]
            elif 'campaign' in col_lower and 'name' in col_lower:
                standardized['campaign_name'] = df[col]
            elif col_lower == 'date' or date_col.lower() in col_lower:
                standardized['date'] = df[col]
            elif 'impression' in col_lower:
                standardized['impressions'] = df[col]
            elif 'click' in col_lower:
                standardized['clicks'] = df[col]
            elif 'spend' in col_lower or 'cost' in col_lower:
                standardized['spend'] = df[col]
            elif 'conversion' in col_lower and 'value' in col_lower:
                standardized['conversion_value'] = df[col]
            elif 'conversion' in col_lower:
                standardized['conversions'] = df[col]

        # Fill missing columns
        for col in self._get_standard_columns():
            if col not in standardized.columns:
                if col in ['impressions', 'clicks', 'conversions']:
                    standardized[col] = 0
                elif col == 'spend':
                    standardized[col] = 0.0
                else:
                    standardized[col] = None

        standardized['platform'] = 'other'
        standardized['date'] = pd.to_datetime(standardized['date'])

        return standardized

    def _aggregate_by_day(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate metrics by day, platform, and campaign."""

        group_cols = ['date', 'platform', 'campaign_id', 'campaign_name', 'campaign_type']
        group_cols = [col for col in group_cols if col in df.columns and df[col].notna().any()]

        agg_dict = {
            'impressions': 'sum',
            'clicks': 'sum',
            'spend': 'sum',
            'conversions': 'sum',
            'conversion_value': 'sum',
        }

        aggregated = df.groupby(group_cols).agg(agg_dict).reset_index()

        # Preserve non-aggregated fields from first row of each group
        for col in ['ad_group_id', 'ad_group_name', 'keyword', 'device_type', 'location']:
            if col in df.columns:
                aggregated[col] = df.groupby(group_cols)[col].first().values

        return aggregated

    def _calculate_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate CTR, CPC, CPA, ROAS."""

        # CTR = (clicks / impressions) * 100
        df['ctr'] = np.where(
            df['impressions'] > 0,
            (df['clicks'] / df['impressions']) * 100,
            0
        )

        # CPC = spend / clicks
        df['cpc'] = np.where(
            df['clicks'] > 0,
            df['spend'] / df['clicks'],
            0
        )

        # CPA = spend / conversions
        df['cpa'] = np.where(
            df['conversions'] > 0,
            df['spend'] / df['conversions'],
            0
        )

        # ROAS = conversion_value / spend
        df['roas'] = np.where(
            df['spend'] > 0,
            df['conversion_value'] / df['spend'],
            0
        )

        return df
