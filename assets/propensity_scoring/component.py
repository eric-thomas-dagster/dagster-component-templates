"""Propensity Scoring Component.

Calculate propensity scores for various customer actions (purchase, upgrade, churn, refer)
using heuristic scoring based on behavior patterns.
"""

from typing import Optional
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


class PropensityScoringComponent(Component, Model, Resolvable):
    """Component for calculating customer propensity scores.

    Calculate likelihood scores for customer actions:
    - **Purchase Propensity**: Likelihood to make next purchase
    - **Upgrade Propensity**: Likelihood to upgrade tier/plan
    - **Referral Propensity**: Likelihood to refer others
    - **Engagement Propensity**: Likelihood to engage with content

    Uses heuristic scoring based on:
    - Recent activity levels
    - Historical behavior patterns
    - Engagement metrics
    - RFM characteristics

    Example:
        ```yaml
        type: dagster_component_templates.PropensityScoringComponent
        attributes:
          asset_name: customer_propensity_scores
          source_asset: customer_behavior
          propensity_type: purchase
          scoring_window_days: 90
          description: "Customer purchase propensity scores"
          group_name: customer_analytics
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    source_asset: Optional[str] = Field(
        default=None,
        description="Source asset with customer behavior data (set via lineage)"
    )

    propensity_type: str = Field(
        default="purchase",
        description="Type of propensity: purchase, upgrade, referral, engagement"
    )

    scoring_window_days: int = Field(
        default=90,
        description="Days of historical data to use for scoring"
    )

    score_threshold_high: float = Field(
        default=70.0,
        description="Score threshold for 'high propensity' classification"
    )

    score_threshold_medium: float = Field(
        default=40.0,
        description="Score threshold for 'medium propensity' classification"
    )

    customer_id_field: Optional[str] = Field(
        default=None,
        description="Customer ID column (auto-detected)"
    )

    last_activity_field: Optional[str] = Field(
        default=None,
        description="Last activity date column (auto-detected)"
    )

    activity_count_field: Optional[str] = Field(
        default=None,
        description="Activity count column (auto-detected)"
    )

    engagement_score_field: Optional[str] = Field(
        default=None,
        description="Engagement score column (optional)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="customer_analytics",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        source_asset = self.source_asset
        propensity_type = self.propensity_type
        scoring_window = self.scoring_window_days
        threshold_high = self.score_threshold_high
        threshold_medium = self.score_threshold_medium
        customer_id_field = self.customer_id_field
        last_activity_field = self.last_activity_field
        activity_count_field = self.activity_count_field
        engagement_score_field = self.engagement_score_field
        description = self.description or f"Customer {propensity_type} propensity scores"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        # Set up dependencies
        upstream_keys = []
        if source_asset:
            upstream_keys.append(source_asset)

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
            deps=upstream_keys if upstream_keys else None,
        )
        def propensity_scoring_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that calculates customer propensity scores."""

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

            context.log.info(f"Processing {len(df)} customer records for propensity scoring")

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
            last_activity_col = find_column(
                ['last_activity_date', 'last_activity', 'last_seen', 'last_purchase_date'],
                last_activity_field
            )
            activity_count_col = find_column(
                ['activity_count', 'total_activities', 'event_count', 'interactions'],
                activity_count_field
            )
            engagement_col = find_column(
                ['engagement_score', 'engagement', 'activity_score'],
                engagement_score_field
            )

            # Validate required columns
            missing = []
            if not customer_col:
                missing.append("customer_id")
            if not last_activity_col:
                missing.append("last_activity_date")

            if missing:
                context.log.error(f"Missing required columns: {', '.join(missing)}")
                context.log.info(f"Available columns: {', '.join(df.columns)}")
                return pd.DataFrame()

            context.log.info(f"Using columns - Customer: {customer_col}, Last Activity: {last_activity_col}")

            # Prepare data
            cols_to_use = [customer_col, last_activity_col]
            col_names = ['customer_id', 'last_activity_date']

            if activity_count_col:
                cols_to_use.append(activity_count_col)
                col_names.append('activity_count')

            if engagement_col:
                cols_to_use.append(engagement_col)
                col_names.append('engagement_score')

            propensity_df = df[cols_to_use].copy()
            propensity_df.columns = col_names

            # Parse dates
            propensity_df['last_activity_date'] = pd.to_datetime(propensity_df['last_activity_date'], errors='coerce')
            propensity_df = propensity_df.dropna(subset=['last_activity_date'])

            # Calculate days since last activity
            current_date = pd.Timestamp.now()
            propensity_df['days_since_activity'] = (current_date - propensity_df['last_activity_date']).dt.days

            context.log.info(f"Calculating {propensity_type} propensity for {len(propensity_df)} customers")

            # Calculate propensity score based on type
            if propensity_type == 'purchase':
                # Purchase propensity based on recency and frequency
                # Recency score (0-40 points): Recent activity = high score
                propensity_df['recency_score'] = propensity_df['days_since_activity'].apply(
                    lambda days: max(0, 40 - (days / 7) * 5)  # Decay 5 points per week
                ).clip(0, 40)

                # Frequency score (0-40 points)
                if 'activity_count' in propensity_df.columns:
                    max_activities = propensity_df['activity_count'].quantile(0.95)
                    propensity_df['frequency_score'] = (
                        propensity_df['activity_count'] / max_activities * 40
                    ).clip(0, 40)
                else:
                    propensity_df['frequency_score'] = 20  # Default mid-range

                # Engagement score (0-20 points)
                if 'engagement_score' in propensity_df.columns:
                    max_engagement = propensity_df['engagement_score'].max()
                    if max_engagement > 0:
                        propensity_df['engagement_component'] = (
                            propensity_df['engagement_score'] / max_engagement * 20
                        ).clip(0, 20)
                    else:
                        propensity_df['engagement_component'] = 10
                else:
                    propensity_df['engagement_component'] = 10  # Default mid-range

                # Total propensity score (0-100)
                propensity_df['propensity_score'] = (
                    propensity_df['recency_score'] +
                    propensity_df['frequency_score'] +
                    propensity_df['engagement_component']
                ).round(2)

            elif propensity_type == 'upgrade':
                # Upgrade propensity based on engagement and activity growth
                # High engagement = high upgrade propensity
                if 'engagement_score' in propensity_df.columns:
                    max_engagement = propensity_df['engagement_score'].max()
                    if max_engagement > 0:
                        propensity_df['propensity_score'] = (
                            propensity_df['engagement_score'] / max_engagement * 70
                        ).clip(0, 70)
                    else:
                        propensity_df['propensity_score'] = 35
                else:
                    propensity_df['propensity_score'] = 35

                # Boost for recent activity
                propensity_df['propensity_score'] += propensity_df['days_since_activity'].apply(
                    lambda days: max(0, 30 - days)  # Up to 30 points for very recent
                ).clip(0, 30)

                propensity_df['propensity_score'] = propensity_df['propensity_score'].round(2).clip(0, 100)

            elif propensity_type == 'referral':
                # Referral propensity based on engagement and satisfaction indicators
                # Highly engaged customers are more likely to refer
                if 'engagement_score' in propensity_df.columns:
                    max_engagement = propensity_df['engagement_score'].max()
                    if max_engagement > 0:
                        propensity_df['propensity_score'] = (
                            propensity_df['engagement_score'] / max_engagement * 60
                        ).clip(0, 60)
                    else:
                        propensity_df['propensity_score'] = 30
                else:
                    propensity_df['propensity_score'] = 30

                # Boost for moderate recency (not too new, not too old)
                propensity_df['propensity_score'] += propensity_df['days_since_activity'].apply(
                    lambda days: 40 if 7 <= days <= 60 else (20 if days < 7 else max(0, 40 - (days - 60) / 10))
                ).clip(0, 40)

                propensity_df['propensity_score'] = propensity_df['propensity_score'].round(2).clip(0, 100)

            elif propensity_type == 'engagement':
                # Engagement propensity based on recent activity patterns
                # Recent and frequent activity = high engagement propensity
                propensity_df['recency_score'] = propensity_df['days_since_activity'].apply(
                    lambda days: max(0, 50 - days)  # 50 points max, decays daily
                ).clip(0, 50)

                if 'activity_count' in propensity_df.columns:
                    max_activities = propensity_df['activity_count'].quantile(0.95)
                    propensity_df['frequency_score'] = (
                        propensity_df['activity_count'] / max_activities * 50
                    ).clip(0, 50)
                else:
                    propensity_df['frequency_score'] = 25

                propensity_df['propensity_score'] = (
                    propensity_df['recency_score'] + propensity_df['frequency_score']
                ).round(2).clip(0, 100)

            else:
                context.log.error(f"Unknown propensity type: {propensity_type}")
                return pd.DataFrame()

            # Classify propensity level
            def classify_propensity(score):
                if score >= threshold_high:
                    return 'High'
                elif score >= threshold_medium:
                    return 'Medium'
                else:
                    return 'Low'

            propensity_df['propensity_level'] = propensity_df['propensity_score'].apply(classify_propensity)

            # Select output columns
            output_cols = [
                'customer_id',
                'propensity_score',
                'propensity_level',
                'days_since_activity'
            ]

            result_df = propensity_df[output_cols].copy()

            context.log.info(f"Propensity scoring complete: {len(result_df)} customers scored")

            # Log propensity distribution
            level_dist = result_df['propensity_level'].value_counts()
            context.log.info("\nPropensity Level Distribution:")
            for level, count in level_dist.items():
                pct = (count / len(result_df) * 100).round(1)
                avg_score = result_df[result_df['propensity_level'] == level]['propensity_score'].mean().round(2)
                context.log.info(f"  {level}: {count} customers ({pct}%), avg score: {avg_score}")

            # Add metadata
            metadata = {
                "row_count": len(result_df),
                "total_customers": len(result_df),
                "propensity_type": propensity_type,
                "avg_propensity_score": round(result_df['propensity_score'].mean(), 2),
                "high_propensity_count": int(level_dist.get('High', 0)),
                "medium_propensity_count": int(level_dist.get('Medium', 0)),
                "low_propensity_count": int(level_dist.get('Low', 0)),
                "scoring_window_days": scoring_window,
            }

            # Return with metadata
            if include_sample and len(result_df) > 0:
                # Sort by propensity score descending
                result_sorted = result_df.sort_values('propensity_score', ascending=False)

                return Output(
                    value=result_df,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(result_sorted.head(20).to_markdown(index=False)),
                        "preview": MetadataValue.dataframe(result_sorted.head(20))
                    }
                )
            else:
                context.add_output_metadata(metadata)
                return result_df

        return Definitions(assets=[propensity_scoring_asset])
