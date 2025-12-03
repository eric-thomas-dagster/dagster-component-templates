"""Customer Journey Mapping Component.

Map and analyze customer journeys from first touch to conversion, identifying common paths,
drop-off points, and journey performance metrics.
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


class CustomerJourneyMappingComponent(Component, Model, Resolvable):
    """Component for mapping and analyzing customer journeys.

    This component traces customer interactions from first touch to conversion,
    identifying:
    - Common journey paths and sequences
    - Average journey length and duration
    - Drop-off points and bottlenecks
    - Conversion rates by journey stage
    - Journey performance by segment

    Example:
        ```yaml
        type: dagster_component_templates.CustomerJourneyMappingComponent
        attributes:
          asset_name: customer_journeys
          source_asset: customer_events
          journey_stages: ["awareness", "consideration", "purchase"]
          max_journey_length: 20
          description: "Customer journey analysis"
          group_name: customer_analytics
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    source_asset: Optional[str] = Field(
        default=None,
        description="Source asset with customer event/touchpoint data (set via lineage)"
    )

    journey_stages: Optional[list] = Field(
        default=None,
        description="Ordered list of journey stages to track (optional)"
    )

    max_journey_length: int = Field(
        default=20,
        description="Maximum events to include in a journey path"
    )

    conversion_event: Optional[str] = Field(
        default=None,
        description="Event name that indicates conversion (e.g., 'purchase', 'signup')"
    )

    time_window_hours: int = Field(
        default=720,  # 30 days
        description="Maximum time between first and last event in a journey (hours)"
    )

    include_path_analysis: bool = Field(
        default=True,
        description="Include detailed path analysis and visualization"
    )

    customer_id_field: Optional[str] = Field(
        default=None,
        description="Customer ID column (auto-detected if not specified)"
    )

    event_field: Optional[str] = Field(
        default=None,
        description="Event name/type column (auto-detected if not specified)"
    )

    timestamp_field: Optional[str] = Field(
        default=None,
        description="Event timestamp column (auto-detected if not specified)"
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
        journey_stages = self.journey_stages
        max_length = self.max_journey_length
        conversion_event = self.conversion_event
        time_window = self.time_window_hours
        include_paths = self.include_path_analysis
        customer_id_field = self.customer_id_field
        event_field = self.event_field
        timestamp_field = self.timestamp_field
        description = self.description or "Customer journey mapping and analysis"
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
        def customer_journey_mapping_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that maps customer journeys."""

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

            context.log.info(f"Processing {len(df)} events for journey mapping")

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
            event_col = find_column(
                ['event', 'event_name', 'event_type', 'action', 'activity'],
                event_field
            )
            timestamp_col = find_column(
                ['timestamp', 'date', 'created_at', 'event_time', 'time'],
                timestamp_field
            )

            # Validate required columns
            missing = []
            if not customer_col:
                missing.append("customer_id")
            if not event_col:
                missing.append("event/event_name")
            if not timestamp_col:
                missing.append("timestamp/date")

            if missing:
                context.log.error(f"Missing required columns: {', '.join(missing)}")
                context.log.info(f"Available columns: {', '.join(df.columns)}")
                return pd.DataFrame()

            context.log.info(f"Using columns - Customer: {customer_col}, Event: {event_col}, Timestamp: {timestamp_col}")

            # Prepare data
            events_df = df[[customer_col, event_col, timestamp_col]].copy()
            events_df.columns = ['customer_id', 'event', 'timestamp']

            # Parse timestamps
            events_df['timestamp'] = pd.to_datetime(events_df['timestamp'], errors='coerce')
            events_df = events_df.dropna(subset=['timestamp'])

            # Sort by customer and timestamp
            events_df = events_df.sort_values(['customer_id', 'timestamp'])

            context.log.info(f"Analyzing journeys for {events_df['customer_id'].nunique()} customers")

            # Build journeys for each customer
            journeys = []

            for customer_id, customer_events in events_df.groupby('customer_id'):
                # Get events for this customer
                events = customer_events.sort_values('timestamp')

                # Check if journey is within time window
                first_event = events.iloc[0]
                last_event = events.iloc[-1]
                journey_duration_hours = (last_event['timestamp'] - first_event['timestamp']).total_seconds() / 3600

                if journey_duration_hours > time_window:
                    # Split into multiple journeys if exceeds time window
                    # For simplicity, we'll take the most recent journey
                    cutoff_time = last_event['timestamp'] - pd.Timedelta(hours=time_window)
                    events = events[events['timestamp'] >= cutoff_time]

                # Limit journey length
                if len(events) > max_length:
                    events = events.tail(max_length)

                # Build journey path
                journey_path = ' → '.join(events['event'].tolist())
                journey_length = len(events)

                # Calculate journey duration
                if journey_length > 1:
                    journey_duration = (events.iloc[-1]['timestamp'] - events.iloc[0]['timestamp']).total_seconds() / 3600
                else:
                    journey_duration = 0

                # Check if journey resulted in conversion
                has_conversion = False
                if conversion_event:
                    has_conversion = conversion_event in events['event'].values
                else:
                    # If no conversion event specified, assume last event is outcome
                    has_conversion = True

                # Get first and last event
                first_event_name = events.iloc[0]['event']
                last_event_name = events.iloc[-1]['event']

                # Count unique event types
                unique_events = events['event'].nunique()

                journeys.append({
                    'customer_id': customer_id,
                    'journey_path': journey_path,
                    'journey_length': journey_length,
                    'journey_duration_hours': round(journey_duration, 2),
                    'first_event': first_event_name,
                    'last_event': last_event_name,
                    'unique_events': unique_events,
                    'converted': has_conversion,
                    'first_timestamp': events.iloc[0]['timestamp'],
                    'last_timestamp': events.iloc[-1]['timestamp']
                })

            result_df = pd.DataFrame(journeys)

            if len(result_df) == 0:
                context.log.warning("No customer journeys found")
                return pd.DataFrame()

            context.log.info(f"Mapped {len(result_df)} customer journeys")

            # Calculate conversion rate
            conversion_rate = (result_df['converted'].sum() / len(result_df) * 100).round(2)
            context.log.info(f"Overall conversion rate: {conversion_rate}%")

            # Analyze path patterns
            if include_paths:
                # Most common paths
                path_counts = result_df['journey_path'].value_counts().head(20)
                context.log.info(f"\nTop 10 Journey Paths:")
                for path, count in path_counts.head(10).items():
                    pct = (count / len(result_df) * 100).round(1)
                    context.log.info(f"  ({count} customers, {pct}%): {path}")

                # Conversion rate by journey length
                length_conv_rate = result_df.groupby('journey_length')['converted'].mean() * 100
                context.log.info(f"\nConversion Rate by Journey Length:")
                for length, rate in length_conv_rate.head(10).items():
                    count = len(result_df[result_df['journey_length'] == length])
                    context.log.info(f"  {length} events: {rate:.1f}% ({count} journeys)")

            # Add metadata
            metadata = {
                "row_count": len(result_df),
                "total_customers": len(result_df),
                "avg_journey_length": round(result_df['journey_length'].mean(), 2),
                "avg_journey_duration_hours": round(result_df['journey_duration_hours'].mean(), 2),
                "conversion_rate": conversion_rate,
                "conversions": result_df['converted'].sum(),
                "max_journey_length": max_length,
                "time_window_hours": time_window,
            }

            # Return with metadata
            if include_sample and len(result_df) > 0:
                # Show sample of converted and non-converted journeys
                converted_sample = result_df[result_df['converted']].head(5)
                non_converted_sample = result_df[~result_df['converted']].head(5)
                sample_df = pd.concat([converted_sample, non_converted_sample])

                return Output(
                    value=result_df,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(sample_df.to_markdown(index=False)),
                        "preview": MetadataValue.dataframe(sample_df)
                    }
                )
            else:
                context.add_output_metadata(metadata)
                return result_df

        return Definitions(assets=[customer_journey_mapping_asset])
