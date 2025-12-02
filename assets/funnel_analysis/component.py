"""Funnel Analysis Component.

Track conversion rates through multi-step user journeys and funnels.
Analyzes drop-off rates and conversion optimization opportunities.
"""

from typing import Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
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


class FunnelAnalysisComponent(Component, Model, Resolvable):
    """Component for analyzing conversion funnels.

    Funnel analysis tracks how users progress through ordered steps in a journey
    (e.g., Landing → Product → Cart → Purchase). This helps identify:
    - Conversion bottlenecks
    - Drop-off points
    - Time between steps
    - Overall funnel efficiency

    This component accepts event/session data and produces funnel metrics including
    conversion rates, drop-off rates, and average time to next step.

    Example:
        ```yaml
        type: dagster_component_templates.FunnelAnalysisComponent
        attributes:
          asset_name: checkout_funnel
          source_asset: user_events
          funnel_steps: "Landing,Product View,Add to Cart,Checkout,Purchase"
          conversion_window_hours: 24
          require_sequential: true
          description: "E-commerce checkout funnel analysis"
          group_name: product_analytics
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    source_asset: Optional[str] = Field(
        default=None,
        description="Source asset with event/session data (set via lineage in Dagster Designer)"
    )

    funnel_steps: str = Field(
        description="Comma-separated ordered funnel steps (e.g., 'Landing,Product,Cart,Purchase')"
    )

    conversion_window_hours: int = Field(
        default=24,
        description="Maximum hours allowed between funnel steps"
    )

    require_sequential: bool = Field(
        default=True,
        description="Require steps to occur in exact order (no skipping)"
    )

    allow_skips: bool = Field(
        default=False,
        description="Allow users to skip intermediate steps"
    )

    user_id_field: Optional[str] = Field(
        default=None,
        description="User ID column name (auto-detected if not specified)"
    )

    event_name_field: Optional[str] = Field(
        default=None,
        description="Event name column (auto-detected if not specified)"
    )

    timestamp_field: Optional[str] = Field(
        default=None,
        description="Timestamp column (auto-detected if not specified)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="product_analytics",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        source_asset = self.source_asset
        funnel_steps_str = self.funnel_steps
        conversion_window_hours = self.conversion_window_hours
        require_sequential = self.require_sequential
        allow_skips = self.allow_skips
        user_id_field = self.user_id_field
        event_name_field = self.event_name_field
        timestamp_field = self.timestamp_field
        description = self.description or "Funnel conversion analysis"
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
        def funnel_analysis_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that performs funnel conversion analysis."""

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

            context.log.info(f"Processing {len(df)} event records for funnel analysis")

            # Parse funnel steps
            steps = [s.strip() for s in funnel_steps_str.split(',')]
            if len(steps) < 2:
                context.log.error("Funnel must have at least 2 steps")
                return pd.DataFrame()

            context.log.info(f"Analyzing funnel with {len(steps)} steps: {' → '.join(steps)}")

            # Auto-detect required columns
            def find_column(possible_names, custom_name=None):
                if custom_name and custom_name in df.columns:
                    return custom_name
                for name in possible_names:
                    if name in df.columns:
                        return name
                return None

            user_col = find_column(
                ['user_id', 'customer_id', 'userId', 'customerId', 'visitor_id', 'session_id'],
                user_id_field
            )
            event_col = find_column(
                ['event_name', 'event', 'event_type', 'action', 'page_path'],
                event_name_field
            )
            timestamp_col = find_column(
                ['timestamp', 'event_time', 'created_at', 'date', 'time'],
                timestamp_field
            )

            # Validate required columns
            missing = []
            if not user_col:
                missing.append("user_id")
            if not event_col:
                missing.append("event_name")
            if not timestamp_col:
                missing.append("timestamp")

            if missing:
                context.log.error(f"Missing required columns: {', '.join(missing)}")
                context.log.info(f"Available columns: {', '.join(df.columns)}")
                return pd.DataFrame()

            context.log.info(f"Using columns - User: {user_col}, Event: {event_col}, Timestamp: {timestamp_col}")

            # Prepare data
            funnel_df = df[[user_col, event_col, timestamp_col]].copy()
            funnel_df.columns = ['user_id', 'event_name', 'timestamp']

            # Parse timestamps
            funnel_df['timestamp'] = pd.to_datetime(funnel_df['timestamp'], errors='coerce')
            funnel_df = funnel_df.dropna(subset=['timestamp'])

            # Filter to funnel events only
            funnel_df = funnel_df[funnel_df['event_name'].isin(steps)]

            if len(funnel_df) == 0:
                context.log.warning(f"No events found matching funnel steps: {steps}")
                return pd.DataFrame()

            # Sort by user and timestamp
            funnel_df = funnel_df.sort_values(['user_id', 'timestamp'])

            context.log.info(f"Analyzing {len(funnel_df)} funnel events from {len(funnel_df['user_id'].unique())} users")

            # Track user progression through funnel
            user_progression = {}
            conversion_window = timedelta(hours=conversion_window_hours)

            for user_id, group in funnel_df.groupby('user_id'):
                events = group['event_name'].tolist()
                timestamps = group['timestamp'].tolist()

                reached_steps = {}

                for step_idx, step_name in enumerate(steps):
                    # Find if user reached this step
                    if step_name in events:
                        # Get first occurrence of this step
                        event_idx = events.index(step_name)
                        step_timestamp = timestamps[event_idx]

                        # Check prerequisites
                        can_count = True

                        if require_sequential and step_idx > 0:
                            # Previous step must be completed
                            if (step_idx - 1) not in reached_steps:
                                can_count = False

                        if can_count and step_idx > 0 and (step_idx - 1) in reached_steps:
                            # Check conversion window
                            prev_timestamp = reached_steps[step_idx - 1]
                            time_diff = step_timestamp - prev_timestamp
                            if time_diff > conversion_window:
                                can_count = False

                        if allow_skips and not require_sequential:
                            # User can skip steps, just check they eventually reached it
                            can_count = True

                        if can_count:
                            reached_steps[step_idx] = step_timestamp

                user_progression[user_id] = reached_steps

            # Calculate metrics per step
            total_users = len(user_progression)
            funnel_metrics = []

            users_at_prev_step = total_users

            for step_idx, step_name in enumerate(steps):
                # Count users who reached this step
                users_reached = sum(1 for progression in user_progression.values() if step_idx in progression)

                # Calculate conversion rates
                conversion_overall = (users_reached / total_users * 100) if total_users > 0 else 0

                if step_idx > 0:
                    conversion_from_prev = (users_reached / users_at_prev_step * 100) if users_at_prev_step > 0 else 0
                    drop_off = ((users_at_prev_step - users_reached) / users_at_prev_step * 100) if users_at_prev_step > 0 else 0
                else:
                    conversion_from_prev = 100.0
                    drop_off = 0.0

                # Calculate average time to next step
                if step_idx < len(steps) - 1:
                    time_diffs = []
                    for progression in user_progression.values():
                        if step_idx in progression and (step_idx + 1) in progression:
                            time_diff = progression[step_idx + 1] - progression[step_idx]
                            time_diffs.append(time_diff.total_seconds() / 3600)  # Convert to hours

                    avg_time_to_next = np.mean(time_diffs) if time_diffs else None
                else:
                    avg_time_to_next = None

                funnel_metrics.append({
                    'step_number': step_idx + 1,
                    'step_name': step_name,
                    'users_entered': users_reached,
                    'conversion_rate_overall': round(conversion_overall, 2),
                    'conversion_rate_from_previous': round(conversion_from_prev, 2),
                    'drop_off_rate': round(drop_off, 2),
                    'avg_time_to_next_hours': round(avg_time_to_next, 2) if avg_time_to_next is not None else None
                })

                users_at_prev_step = users_reached

            result_df = pd.DataFrame(funnel_metrics)

            context.log.info(f"Funnel analysis complete")
            context.log.info(f"  Total users entering funnel: {total_users}")
            context.log.info(f"  Users completing funnel: {users_at_prev_step} ({result_df.iloc[-1]['conversion_rate_overall']:.1f}%)")

            # Log step-by-step metrics
            for idx, row in result_df.iterrows():
                context.log.info(
                    f"  Step {row['step_number']} ({row['step_name']}): "
                    f"{row['users_entered']} users ({row['conversion_rate_from_previous']:.1f}% from prev)"
                )

            # Add metadata
            metadata = {
                "row_count": len(result_df),
                "total_steps": len(steps),
                "total_users_entering": total_users,
                "users_completing_funnel": users_at_prev_step,
                "overall_conversion_rate": round(result_df.iloc[-1]['conversion_rate_overall'], 2) if len(result_df) > 0 else 0,
                "conversion_window_hours": conversion_window_hours,
                "require_sequential": require_sequential
            }

            # Identify biggest drop-off step
            if len(result_df) > 1:
                max_dropoff_idx = result_df['drop_off_rate'].idxmax()
                max_dropoff_step = result_df.iloc[max_dropoff_idx]
                metadata['biggest_dropoff_step'] = f"{max_dropoff_step['step_name']} ({max_dropoff_step['drop_off_rate']:.1f}%)"

            # Return with metadata
            if include_sample and len(result_df) > 0:
                return Output(
                    value=result_df,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(result_df.to_markdown(index=False)),
                        "preview": MetadataValue.dataframe(result_df)
                    }
                )
            else:
                context.add_output_metadata(metadata)
                return result_df

        return Definitions(assets=[funnel_analysis_asset])
