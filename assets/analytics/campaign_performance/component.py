"""Campaign Performance Analytics Component.

Analyze marketing campaign performance across channels, calculating ROI, conversion rates,
and cost-per-acquisition metrics.
"""

from typing import Optional
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


class CampaignPerformanceComponent(Component, Model, Resolvable):
    """Component for analyzing marketing campaign performance.

    Calculates key campaign metrics including:
    - Return on Ad Spend (ROAS)
    - Cost Per Acquisition (CPA)
    - Conversion Rate
    - Click-Through Rate (CTR)
    - Cost Per Click (CPC)
    - Revenue attribution by campaign

    Example:
        ```yaml
        type: dagster_component_templates.CampaignPerformanceComponent
        attributes:
          asset_name: campaign_performance
          upstream_asset_key: campaign_metrics
          target_roas: 3.0
          description: "Campaign performance analysis"
          group_name: marketing_analytics
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame with campaign metrics (impressions, clicks, spend)"
    )

    target_roas: Optional[float] = Field(
        default=None,
        description="Target ROAS for performance comparison (optional)"
    )

    target_cpa: Optional[float] = Field(
        default=None,
        description="Target CPA for performance comparison (optional)"
    )

    include_benchmarks: bool = Field(
        default=True,
        description="Include industry benchmark comparisons"
    )

    campaign_id_field: Optional[str] = Field(
        default=None,
        description="Campaign ID column (auto-detected)"
    )

    campaign_name_field: Optional[str] = Field(
        default=None,
        description="Campaign name column (auto-detected)"
    )

    channel_field: Optional[str] = Field(
        default=None,
        description="Marketing channel column (auto-detected)"
    )

    spend_field: Optional[str] = Field(
        default=None,
        description="Campaign spend column (auto-detected)"
    )

    impressions_field: Optional[str] = Field(
        default=None,
        description="Impressions column (auto-detected)"
    )

    clicks_field: Optional[str] = Field(
        default=None,
        description="Clicks column (auto-detected)"
    )

    conversions_field: Optional[str] = Field(
        default=None,
        description="Conversions column (auto-detected)"
    )

    revenue_field: Optional[str] = Field(
        default=None,
        description="Revenue column (auto-detected)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="marketing_analytics",
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

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        target_roas = self.target_roas
        target_cpa = self.target_cpa
        include_benchmarks = self.include_benchmarks
        campaign_id_field = self.campaign_id_field
        campaign_name_field = self.campaign_name_field
        channel_field = self.channel_field
        spend_field = self.spend_field
        impressions_field = self.impressions_field
        clicks_field = self.clicks_field
        conversions_field = self.conversions_field
        revenue_field = self.revenue_field
        description = self.description or "Campaign performance analytics"
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

        @asset(
            name=asset_name,
            description=description,
            partitions_def=partitions_def,
            group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
        )
        def campaign_performance_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            """Asset that analyzes campaign performance."""

            campaign_df = upstream
            if not isinstance(campaign_df, pd.DataFrame):
                context.log.error("Campaign data is not a DataFrame")
                return pd.DataFrame()

            context.log.info(f"Processing {len(campaign_df)} campaign records")

            # Auto-detect required columns
            def find_column(possible_names, custom_name=None):
                if custom_name and custom_name in campaign_df.columns:
                    return custom_name
                for name in possible_names:
                    if name in campaign_df.columns:
                        return name
                return None

            campaign_id_col = find_column(
                ['campaign_id', 'campaignId', 'id', 'campaign'],
                campaign_id_field
            )
            campaign_name_col = find_column(
                ['campaign_name', 'name', 'campaignName', 'campaign'],
                campaign_name_field
            )
            channel_col = find_column(
                ['channel', 'source', 'marketing_channel', 'platform'],
                channel_field
            )
            spend_col = find_column(
                ['spend', 'cost', 'budget', 'amount'],
                spend_field
            )
            impressions_col = find_column(
                ['impressions', 'views', 'reach'],
                impressions_field
            )
            clicks_col = find_column(
                ['clicks', 'link_clicks', 'clicks_total'],
                clicks_field
            )
            conversions_col = find_column(
                ['conversions', 'purchases', 'leads', 'conversion_count'],
                conversions_field
            )
            revenue_col = find_column(
                ['revenue', 'sales', 'conversion_value', 'total_revenue'],
                revenue_field
            )

            # Validate required columns
            missing = []
            if not campaign_id_col and not campaign_name_col:
                missing.append("campaign_id or campaign_name")
            if not spend_col:
                missing.append("spend/cost")

            if missing:
                context.log.error(f"Missing required columns: {', '.join(missing)}")
                context.log.info(f"Available columns: {', '.join(campaign_df.columns)}")
                return pd.DataFrame()

            # Use campaign_id or campaign_name as identifier
            campaign_key = campaign_id_col if campaign_id_col else campaign_name_col
            context.log.info(f"Using campaign key: {campaign_key}")

            # Prepare data
            cols_to_use = [campaign_key, spend_col]
            col_names = ['campaign_id', 'spend']

            optional_cols = [
                (campaign_name_col, 'campaign_name'),
                (channel_col, 'channel'),
                (impressions_col, 'impressions'),
                (clicks_col, 'clicks'),
                (conversions_col, 'conversions'),
                (revenue_col, 'revenue')
            ]

            for col, name in optional_cols:
                if col:
                    cols_to_use.append(col)
                    col_names.append(name)

            performance_df = campaign_df[cols_to_use].copy()
            performance_df.columns = col_names

            # Convert numeric columns
            numeric_cols = ['spend', 'impressions', 'clicks', 'conversions', 'revenue']
            for col in numeric_cols:
                if col in performance_df.columns:
                    performance_df[col] = pd.to_numeric(performance_df[col], errors='coerce').fillna(0)

            # Calculate metrics
            context.log.info("Calculating performance metrics...")

            # Click-Through Rate (CTR)
            if 'impressions' in performance_df.columns and 'clicks' in performance_df.columns:
                performance_df['ctr'] = (performance_df['clicks'] / performance_df['impressions'] * 100).round(2)
                performance_df['ctr'] = performance_df['ctr'].replace([np.inf, -np.inf], np.nan).fillna(0)

            # Cost Per Click (CPC)
            if 'clicks' in performance_df.columns:
                performance_df['cpc'] = (performance_df['spend'] / performance_df['clicks']).round(2)
                performance_df['cpc'] = performance_df['cpc'].replace([np.inf, -np.inf], np.nan).fillna(0)

            # Conversion Rate
            if 'clicks' in performance_df.columns and 'conversions' in performance_df.columns:
                performance_df['conversion_rate'] = (performance_df['conversions'] / performance_df['clicks'] * 100).round(2)
                performance_df['conversion_rate'] = performance_df['conversion_rate'].replace([np.inf, -np.inf], np.nan).fillna(0)

            # Cost Per Acquisition (CPA)
            if 'conversions' in performance_df.columns:
                performance_df['cpa'] = (performance_df['spend'] / performance_df['conversions']).round(2)
                performance_df['cpa'] = performance_df['cpa'].replace([np.inf, -np.inf], np.nan).fillna(0)

            # Return on Ad Spend (ROAS)
            if 'revenue' in performance_df.columns:
                performance_df['roas'] = (performance_df['revenue'] / performance_df['spend']).round(2)
                performance_df['roas'] = performance_df['roas'].replace([np.inf, -np.inf], np.nan).fillna(0)

                # ROI (percentage)
                performance_df['roi'] = ((performance_df['revenue'] - performance_df['spend']) / performance_df['spend'] * 100).round(2)
                performance_df['roi'] = performance_df['roi'].replace([np.inf, -np.inf], np.nan).fillna(0)

            # Performance scoring
            if target_roas and 'roas' in performance_df.columns:
                performance_df['roas_vs_target'] = ((performance_df['roas'] / target_roas - 1) * 100).round(2)
                performance_df['roas_performance'] = performance_df['roas'].apply(
                    lambda x: 'Excellent' if x >= target_roas * 1.2 else (
                        'Good' if x >= target_roas else (
                            'Below Target' if x >= target_roas * 0.8 else 'Poor'
                        )
                    )
                )

            if target_cpa and 'cpa' in performance_df.columns:
                performance_df['cpa_vs_target'] = ((performance_df['cpa'] / target_cpa - 1) * 100).round(2)
                performance_df['cpa_performance'] = performance_df['cpa'].apply(
                    lambda x: 'Excellent' if x <= target_cpa * 0.8 else (
                        'Good' if x <= target_cpa else (
                            'Above Target' if x <= target_cpa * 1.2 else 'Poor'
                        )
                    )
                )

            # Sort by performance (ROAS or ROI if available, otherwise by spend)
            if 'roas' in performance_df.columns:
                performance_df = performance_df.sort_values('roas', ascending=False)
            elif 'roi' in performance_df.columns:
                performance_df = performance_df.sort_values('roi', ascending=False)
            else:
                performance_df = performance_df.sort_values('spend', ascending=False)

            context.log.info(f"Performance analysis complete: {len(performance_df)} campaigns analyzed")

            # Log summary statistics
            total_spend = performance_df['spend'].sum()
            context.log.info(f"\nCampaign Summary:")
            context.log.info(f"  Total Campaigns: {len(performance_df)}")
            context.log.info(f"  Total Spend: ${total_spend:,.2f}")

            if 'revenue' in performance_df.columns:
                total_revenue = performance_df['revenue'].sum()
                overall_roas = total_revenue / total_spend if total_spend > 0 else 0
                context.log.info(f"  Total Revenue: ${total_revenue:,.2f}")
                context.log.info(f"  Overall ROAS: {overall_roas:.2f}x")

            if 'conversions' in performance_df.columns:
                total_conversions = performance_df['conversions'].sum()
                overall_cpa = total_spend / total_conversions if total_conversions > 0 else 0
                context.log.info(f"  Total Conversions: {total_conversions:,.0f}")
                context.log.info(f"  Overall CPA: ${overall_cpa:.2f}")

            # Log top/bottom performers
            if len(performance_df) >= 3:
                context.log.info(f"\nTop 3 Campaigns:")
                for idx, row in performance_df.head(3).iterrows():
                    campaign_name = row.get('campaign_name', row['campaign_id'])
                    if 'roas' in row:
                        context.log.info(f"  {campaign_name}: ROAS {row['roas']}x, Spend ${row['spend']:,.2f}")
                    else:
                        context.log.info(f"  {campaign_name}: Spend ${row['spend']:,.2f}")

            # Add metadata
            metadata = {
                "row_count": len(performance_df),
                "total_campaigns": len(performance_df),
                "total_spend": round(total_spend, 2),
            }

            if 'revenue' in performance_df.columns:
                metadata['total_revenue'] = round(total_revenue, 2)
                metadata['overall_roas'] = round(overall_roas, 2)

            if 'conversions' in performance_df.columns:
                metadata['total_conversions'] = int(total_conversions)
                metadata['overall_cpa'] = round(overall_cpa, 2)

            if target_roas:
                metadata['target_roas'] = target_roas

            if target_cpa:
                metadata['target_cpa'] = target_cpa

            # Return with metadata
            if include_sample and len(performance_df) > 0:
                return Output(
                    value=performance_df,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(performance_df.head(10).to_markdown(index=False)),
                        "preview": MetadataValue.dataframe(performance_df.head(10))
                    }
                )
            else:
                context.add_output_metadata(metadata)
                return performance_df

        return Definitions(assets=[campaign_performance_asset])
