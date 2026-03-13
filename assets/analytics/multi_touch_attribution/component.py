"""Multi-Touch Attribution Component.

Attribute revenue and conversions across multiple marketing touchpoints using various
attribution models (first-touch, last-touch, linear, time-decay, U-shaped, W-shaped).
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


class MultiTouchAttributionComponent(Component, Model, Resolvable):
    """Component for multi-touch attribution analysis.

    Multi-touch attribution distributes credit for conversions across all marketing
    touchpoints in a customer's journey. This component supports multiple attribution
    models to help you understand which channels drive conversions:

    - **First Touch**: 100% credit to first interaction
    - **Last Touch**: 100% credit to last interaction before conversion
    - **Linear**: Equal credit to all touchpoints
    - **Time Decay**: More credit to recent touchpoints (exponential decay)
    - **U-Shaped (Position-Based)**: 40% first, 40% last, 20% middle touchpoints
    - **W-Shaped**: 30% first, 30% middle (lead conversion), 30% last, 10% others

    Example:
        ```yaml
        type: dagster_component_templates.MultiTouchAttributionComponent
        attributes:
          asset_name: channel_attribution
          upstream_asset_key: marketing_touchpoints
          attribution_model: time_decay
          lookback_window_days: 30
          description: "Multi-touch attribution analysis"
          group_name: marketing_analytics
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame with marketing touchpoint data"
    )

    attribution_model: str = Field(
        default="linear",
        description="Attribution model: first_touch, last_touch, linear, time_decay, u_shaped, w_shaped"
    )

    lookback_window_days: int = Field(
        default=30,
        description="Days to look back for touchpoints before conversion"
    )

    time_decay_half_life_days: float = Field(
        default=7.0,
        description="Half-life for time decay model (days)"
    )

    include_channel_performance: bool = Field(
        default=True,
        description="Include aggregated channel performance metrics"
    )

    include_journey_details: bool = Field(
        default=True,
        description="Include detailed customer journey paths"
    )

    customer_id_field: Optional[str] = Field(
        default=None,
        description="Customer ID column name (auto-detected if not specified)"
    )

    touchpoint_date_field: Optional[str] = Field(
        default=None,
        description="Touchpoint timestamp column (auto-detected if not specified)"
    )

    channel_field: Optional[str] = Field(
        default=None,
        description="Marketing channel column (auto-detected if not specified)"
    )

    conversion_date_field: Optional[str] = Field(
        default=None,
        description="Conversion timestamp column (auto-detected if not specified)"
    )

    conversion_value_field: Optional[str] = Field(
        default=None,
        description="Conversion value column (auto-detected if not specified)"
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
        attribution_model = self.attribution_model
        lookback_days = self.lookback_window_days
        time_decay_half_life = self.time_decay_half_life_days
        include_performance = self.include_channel_performance
        include_journeys = self.include_journey_details
        customer_id_field = self.customer_id_field
        touchpoint_date_field = self.touchpoint_date_field
        channel_field = self.channel_field
        conversion_date_field = self.conversion_date_field
        conversion_value_field = self.conversion_value_field
        description = self.description or f"Multi-touch attribution analysis ({attribution_model})"
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
        def multi_touch_attribution_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            """Asset that performs multi-touch attribution analysis."""

            touchpoint_df = upstream
            if not isinstance(touchpoint_df, pd.DataFrame):
                context.log.error("Touchpoint data is not a DataFrame")
                return pd.DataFrame()

            context.log.info(f"Processing {len(touchpoint_df)} touchpoint records")

            # Auto-detect required columns
            def find_column(df, possible_names, custom_name=None):
                if custom_name and custom_name in df.columns:
                    return custom_name
                for name in possible_names:
                    if name in df.columns:
                        return name
                return None

            # Detect columns in touchpoint data
            customer_col = find_column(
                touchpoint_df,
                ['customer_id', 'user_id', 'customerId', 'userId', 'id'],
                customer_id_field
            )
            date_col = find_column(
                touchpoint_df,
                ['date', 'timestamp', 'touchpoint_date', 'interaction_date', 'created_at'],
                touchpoint_date_field
            )
            channel_col = find_column(
                touchpoint_df,
                ['channel', 'source', 'marketing_channel', 'utm_source', 'campaign_source'],
                channel_field
            )

            # Check if conversions are in the same dataset or separate
            conversion_col = find_column(
                touchpoint_df,
                ['is_conversion', 'converted', 'conversion'],
                None
            )
            conversion_value_col = find_column(
                touchpoint_df,
                ['conversion_value', 'revenue', 'value', 'amount'],
                conversion_value_field
            )

            # Validate required columns
            missing = []
            if not customer_col:
                missing.append("customer_id")
            if not date_col:
                missing.append("date/timestamp")
            if not channel_col:
                missing.append("channel")

            if missing:
                context.log.error(f"Missing required columns: {', '.join(missing)}")
                context.log.info(f"Available columns: {', '.join(touchpoint_df.columns)}")
                return pd.DataFrame()

            context.log.info(f"Using columns - Customer: {customer_col}, Date: {date_col}, Channel: {channel_col}")

            # Prepare touchpoint data
            touchpoints = touchpoint_df[[customer_col, date_col, channel_col]].copy()

            # Add conversion columns if they exist
            if conversion_col:
                touchpoints['is_conversion'] = touchpoint_df[conversion_col]
            if conversion_value_col:
                touchpoints['conversion_value'] = touchpoint_df[conversion_value_col]

            touchpoints.columns = ['customer_id', 'date', 'channel'] + \
                                 (['is_conversion'] if conversion_col else []) + \
                                 (['conversion_value'] if conversion_value_col else [])

            # Parse dates
            touchpoints['date'] = pd.to_datetime(touchpoints['date'], errors='coerce')
            touchpoints = touchpoints.dropna(subset=['date'])

            # If no conversion column, mark last touchpoint per customer as conversion
            if 'is_conversion' not in touchpoints.columns:
                context.log.info("No conversion column found, using last touchpoint per customer as conversion")
                touchpoints = touchpoints.sort_values(['customer_id', 'date'])
                touchpoints['is_conversion'] = False
                last_touchpoints = touchpoints.groupby('customer_id').tail(1).index
                touchpoints.loc[last_touchpoints, 'is_conversion'] = True

            # If no conversion value, default to 1
            if 'conversion_value' not in touchpoints.columns:
                touchpoints['conversion_value'] = 1

            # Convert conversion flag to boolean
            touchpoints['is_conversion'] = touchpoints['is_conversion'].fillna(False).astype(bool)
            touchpoints['conversion_value'] = pd.to_numeric(touchpoints['conversion_value'], errors='coerce').fillna(0)

            context.log.info(f"Found {touchpoints['is_conversion'].sum()} conversions")

            if touchpoints['is_conversion'].sum() == 0:
                context.log.warning("No conversions found in data")
                return pd.DataFrame()

            # Sort by customer and date
            touchpoints = touchpoints.sort_values(['customer_id', 'date'])

            # For each conversion, find touchpoints within lookback window
            conversions = touchpoints[touchpoints['is_conversion']].copy()
            attribution_results = []

            context.log.info(f"Analyzing {len(conversions)} conversions with {attribution_model} model")

            for idx, conversion in conversions.iterrows():
                customer_id = conversion['customer_id']
                conversion_date = conversion['date']
                conversion_value = conversion['conversion_value']

                # Get all touchpoints for this customer before conversion
                customer_touchpoints = touchpoints[
                    (touchpoints['customer_id'] == customer_id) &
                    (touchpoints['date'] <= conversion_date) &
                    (touchpoints['date'] >= conversion_date - pd.Timedelta(days=lookback_days))
                ].copy()

                if len(customer_touchpoints) == 0:
                    continue

                # Calculate attribution weights based on model
                num_touchpoints = len(customer_touchpoints)

                if attribution_model == 'first_touch':
                    weights = [0.0] * num_touchpoints
                    weights[0] = 1.0

                elif attribution_model == 'last_touch':
                    weights = [0.0] * num_touchpoints
                    weights[-1] = 1.0

                elif attribution_model == 'linear':
                    weights = [1.0 / num_touchpoints] * num_touchpoints

                elif attribution_model == 'time_decay':
                    # Exponential decay: weight = 2^(-days_ago / half_life)
                    days_before_conversion = (conversion_date - customer_touchpoints['date']).dt.days.values
                    weights = np.power(2, -days_before_conversion / time_decay_half_life)
                    weights = weights / weights.sum()  # Normalize

                elif attribution_model == 'u_shaped':
                    # 40% first, 40% last, 20% divided among middle
                    if num_touchpoints == 1:
                        weights = [1.0]
                    elif num_touchpoints == 2:
                        weights = [0.5, 0.5]
                    else:
                        weights = [0.0] * num_touchpoints
                        weights[0] = 0.4
                        weights[-1] = 0.4
                        middle_weight = 0.2 / (num_touchpoints - 2)
                        for i in range(1, num_touchpoints - 1):
                            weights[i] = middle_weight

                elif attribution_model == 'w_shaped':
                    # 30% first, 30% middle (key conversion point), 30% last, 10% others
                    if num_touchpoints == 1:
                        weights = [1.0]
                    elif num_touchpoints == 2:
                        weights = [0.5, 0.5]
                    elif num_touchpoints == 3:
                        weights = [0.4, 0.2, 0.4]
                    else:
                        weights = [0.0] * num_touchpoints
                        weights[0] = 0.3
                        middle_idx = len(weights) // 2
                        weights[middle_idx] = 0.3
                        weights[-1] = 0.3
                        remaining_weight = 0.1 / (num_touchpoints - 3)
                        for i in range(len(weights)):
                            if i not in [0, middle_idx, len(weights) - 1]:
                                weights[i] = remaining_weight

                else:
                    context.log.warning(f"Unknown attribution model: {attribution_model}, using linear")
                    weights = [1.0 / num_touchpoints] * num_touchpoints

                # Assign attributed value to each touchpoint
                customer_touchpoints['attribution_weight'] = weights
                customer_touchpoints['attributed_value'] = weights * conversion_value
                customer_touchpoints['conversion_id'] = f"{customer_id}_{conversion_date}"
                customer_touchpoints['conversion_date'] = conversion_date
                customer_touchpoints['total_conversion_value'] = conversion_value

                attribution_results.append(customer_touchpoints)

            if not attribution_results:
                context.log.warning("No attribution results generated")
                return pd.DataFrame()

            # Combine all results
            result_df = pd.concat(attribution_results, ignore_index=True)

            context.log.info(f"Attribution complete: {len(result_df)} attributed touchpoints")

            # Remove the is_conversion column from output
            result_df = result_df.drop(columns=['is_conversion'], errors='ignore')

            # Aggregate by channel for performance summary
            if include_performance:
                channel_performance = result_df.groupby('channel').agg({
                    'attributed_value': 'sum',
                    'conversion_id': 'nunique',
                    'customer_id': 'nunique'
                }).round(2)

                channel_performance.columns = ['total_attributed_value', 'conversions', 'unique_customers']
                channel_performance['avg_value_per_conversion'] = (
                    channel_performance['total_attributed_value'] / channel_performance['conversions']
                ).round(2)

                # Calculate contribution percentage
                total_value = channel_performance['total_attributed_value'].sum()
                channel_performance['contribution_pct'] = (
                    channel_performance['total_attributed_value'] / total_value * 100
                ).round(2)

                channel_performance = channel_performance.sort_values('total_attributed_value', ascending=False)

                context.log.info(f"\nChannel Performance ({attribution_model} model):")
                for channel, row in channel_performance.head(10).iterrows():
                    context.log.info(
                        f"  {channel}: ${row['total_attributed_value']:,.2f} "
                        f"({row['contribution_pct']}%), "
                        f"{row['conversions']} conversions"
                    )

            # Journey path analysis
            if include_journeys:
                # Create journey paths (sequence of channels)
                journey_paths = result_df.groupby('conversion_id').agg({
                    'channel': lambda x: ' → '.join(x),
                    'attributed_value': 'first',  # Total conversion value
                    'customer_id': 'first'
                })
                journey_paths.columns = ['journey_path', 'conversion_value', 'customer_id']

                # Count most common paths
                path_frequency = journey_paths['journey_path'].value_counts().head(20)

                context.log.info(f"\nTop 10 Conversion Paths:")
                for path, count in path_frequency.head(10).items():
                    context.log.info(f"  {path}: {count} conversions")

            # Add metadata
            metadata = {
                "row_count": len(result_df),
                "total_conversions": result_df['conversion_id'].nunique(),
                "total_customers": result_df['customer_id'].nunique(),
                "total_attributed_value": round(result_df['attributed_value'].sum(), 2),
                "attribution_model": attribution_model,
                "lookback_window_days": lookback_days,
                "unique_channels": result_df['channel'].nunique(),
            }

            if include_performance:
                metadata['channel_performance'] = MetadataValue.md(
                    channel_performance.to_markdown()
                )

            # Return with metadata
            if include_sample and len(result_df) > 0:
                return Output(
                    value=result_df,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(result_df.head(20).to_markdown(index=False)),
                        "preview": MetadataValue.dataframe(result_df.head(20))
                    }
                )
            else:
                context.add_output_metadata(metadata)
                return result_df

        return Definitions(assets=[multi_touch_attribution_asset])
