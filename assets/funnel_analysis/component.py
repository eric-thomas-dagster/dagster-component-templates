"""Funnel Analysis Component.

Analyzes user progression through defined funnel stages to identify conversion rates,
drop-off points, and optimization opportunities across the customer journey.
"""

from typing import Any, Optional, List, Dict

import pandas as pd
import numpy as np
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    OpExecutionContext,
    asset,
)
from dagster._core.definitions.definitions_class import Definitions
from dagster_components import Component, ComponentLoadContext, component_type
from dagster_components.core.component_defs_builder import build_defs_from_component
from pydantic import Field


@component_type(name="funnel_analysis")
class FunnelAnalysisComponent(Component):
    """Component that analyzes user progression through conversion funnels."""

    asset_name: str = Field(
        ...,
        description="Name of the funnel analysis asset to create",
    )

    # Input asset references (set via lineage)
    event_data_asset: Optional[str] = Field(
        default="",
        description="Event/activity data with user actions and timestamps",
    )

    user_data_asset: Optional[str] = Field(
        default="",
        description="User/customer data for segmentation (optional)",
    )

    # Funnel configuration
    funnel_type: str = Field(
        default="linear",
        description="Funnel type: linear (sequential) or flexible (any order)",
    )

    stage_1_event: str = Field(
        default="page_view",
        description="Event name for first funnel stage",
    )

    stage_1_name: str = Field(
        default="Awareness",
        description="Display name for first stage",
    )

    stage_2_event: str = Field(
        default="signup",
        description="Event name for second funnel stage",
    )

    stage_2_name: str = Field(
        default="Signup",
        description="Display name for second stage",
    )

    stage_3_event: str = Field(
        default="",
        description="Event name for third funnel stage (optional)",
    )

    stage_3_name: str = Field(
        default="",
        description="Display name for third stage",
    )

    stage_4_event: str = Field(
        default="",
        description="Event name for fourth funnel stage (optional)",
    )

    stage_4_name: str = Field(
        default="",
        description="Display name for fourth stage",
    )

    stage_5_event: str = Field(
        default="",
        description="Event name for fifth funnel stage (optional)",
    )

    stage_5_name: str = Field(
        default="",
        description="Display name for fifth stage",
    )

    # Time window configuration
    funnel_window_days: int = Field(
        default=30,
        description="Maximum days between first and last stage to count as conversion",
    )

    analysis_period_days: int = Field(
        default=90,
        description="Number of days of data to analyze",
    )

    # Cohort analysis
    group_by_cohort: bool = Field(
        default=True,
        description="Group results by weekly/monthly cohorts",
    )

    cohort_period: str = Field(
        default="weekly",
        description="Cohort period: daily, weekly, or monthly",
    )

    # Segmentation
    segment_by_source: bool = Field(
        default=True,
        description="Segment funnel by traffic source/campaign",
    )

    segment_by_attribute: str = Field(
        default="",
        description="Additional attribute to segment by (e.g., plan_type, country)",
    )

    # Output options
    calculate_time_to_convert: bool = Field(
        default=True,
        description="Calculate median time between stages",
    )

    identify_drop_offs: bool = Field(
        default=True,
        description="Flag high drop-off stages",
    )

    drop_off_threshold: float = Field(
        default=0.5,
        description="Drop-off rate threshold to flag (e.g., 0.5 = 50% drop)",
    )

    include_user_level_data: bool = Field(
        default=False,
        description="Include individual user progression (large output)",
    )

    # Asset properties
    description: str = Field(
        default="",
        description="Asset description",
    )

    group_name: str = Field(
        default="analytics",
        description="Asset group name",
    )

    def _get_funnel_stages(self) -> List[tuple]:
        """Get configured funnel stages as (event, name) tuples."""
        stages = []

        if self.stage_1_event:
            stages.append((self.stage_1_event, self.stage_1_name or "Stage 1"))

        if self.stage_2_event:
            stages.append((self.stage_2_event, self.stage_2_name or "Stage 2"))

        if self.stage_3_event:
            stages.append((self.stage_3_event, self.stage_3_name or "Stage 3"))

        if self.stage_4_event:
            stages.append((self.stage_4_event, self.stage_4_name or "Stage 4"))

        if self.stage_5_event:
            stages.append((self.stage_5_event, self.stage_5_name or "Stage 5"))

        return stages

    def _prepare_event_data(self, event_data: pd.DataFrame) -> pd.DataFrame:
        """Prepare and validate event data."""
        if event_data is None or event_data.empty:
            return pd.DataFrame()

        # Standardize column names
        df = event_data.copy()

        # Map common column name variations
        column_mappings = {
            'user_id': ['user_id', 'customer_id', 'id', 'userid'],
            'event_name': ['event_name', 'event', 'event_type', 'action'],
            'timestamp': ['timestamp', 'event_timestamp', 'created_at', 'event_time'],
        }

        for target_col, possible_names in column_mappings.items():
            for col in possible_names:
                if col in df.columns and target_col not in df.columns:
                    df[target_col] = df[col]
                    break

        # Validate required columns
        if 'user_id' not in df.columns:
            raise ValueError("Event data must have user_id column")
        if 'event_name' not in df.columns:
            raise ValueError("Event data must have event_name column")
        if 'timestamp' not in df.columns:
            raise ValueError("Event data must have timestamp column")

        # Ensure timestamp is datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Filter to analysis period
        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=self.analysis_period_days)
        df = df[df['timestamp'] >= cutoff_date]

        # Add optional fields if not present
        if 'source' not in df.columns:
            df['source'] = 'unknown'

        return df

    def _calculate_linear_funnel(self, event_data: pd.DataFrame, stages: List[tuple]) -> pd.DataFrame:
        """Calculate conversion rates for linear (sequential) funnel."""
        results = []

        # Get all users who entered the funnel
        stage_events = [event for event, name in stages]
        funnel_data = event_data[event_data['event_name'].isin(stage_events)].copy()

        if funnel_data.empty:
            return pd.DataFrame()

        # Sort by user and timestamp
        funnel_data = funnel_data.sort_values(['user_id', 'timestamp'])

        # For each user, find their progression through stages
        user_progressions = []

        for user_id, user_events in funnel_data.groupby('user_id'):
            progression = {}
            progression['user_id'] = user_id

            # Find first occurrence of each stage
            for i, (event, stage_name) in enumerate(stages):
                stage_key = f"stage_{i+1}"
                stage_events_df = user_events[user_events['event_name'] == event]

                if not stage_events_df.empty:
                    first_event = stage_events_df.iloc[0]
                    progression[f"{stage_key}_timestamp"] = first_event['timestamp']
                    progression[f"{stage_key}_completed"] = True

                    # Capture source from first stage
                    if i == 0 and 'source' in first_event:
                        progression['source'] = first_event.get('source', 'unknown')
                else:
                    progression[f"{stage_key}_completed"] = False

            user_progressions.append(progression)

        if not user_progressions:
            return pd.DataFrame()

        df = pd.DataFrame(user_progressions)

        # Validate sequential progression within time window
        for i in range(len(stages) - 1):
            current_stage = f"stage_{i+1}"
            next_stage = f"stage_{i+2}"

            if f"{current_stage}_timestamp" in df.columns and f"{next_stage}_timestamp" in df.columns:
                # Check if next stage happened after current stage
                valid_sequence = df[f"{next_stage}_timestamp"] > df[f"{current_stage}_timestamp"]

                # Check if within time window
                time_diff = (df[f"{next_stage}_timestamp"] - df[f"{current_stage}_timestamp"]).dt.days
                within_window = time_diff <= self.funnel_window_days

                # Mark next stage as incomplete if invalid
                df.loc[~(valid_sequence & within_window), f"{next_stage}_completed"] = False

        return df

    def _aggregate_funnel_metrics(self, user_progressions: pd.DataFrame, stages: List[tuple]) -> pd.DataFrame:
        """Aggregate user-level progressions into funnel metrics."""
        if user_progressions.empty:
            return pd.DataFrame()

        metrics = []

        # Overall funnel metrics
        overall = {'segment': 'overall', 'cohort': 'all'}

        for i, (event, stage_name) in enumerate(stages):
            stage_key = f"stage_{i+1}"
            completed_col = f"{stage_key}_completed"

            if completed_col in user_progressions.columns:
                reached = user_progressions[completed_col].sum()
                overall[f"{stage_name}_count"] = reached

                if i > 0:
                    prev_stage = stages[i-1][1]
                    prev_count = overall[f"{prev_stage}_count"]

                    if prev_count > 0:
                        conversion_rate = reached / prev_count
                        drop_off_rate = 1 - conversion_rate
                        overall[f"{stage_name}_conversion_rate"] = conversion_rate
                        overall[f"{stage_name}_drop_off_rate"] = drop_off_rate

                        # Calculate time to convert if enabled
                        if self.calculate_time_to_convert:
                            prev_stage_key = f"stage_{i}"
                            if f"{prev_stage_key}_timestamp" in user_progressions.columns and f"{stage_key}_timestamp" in user_progressions.columns:
                                completed_users = user_progressions[user_progressions[completed_col] == True]
                                if not completed_users.empty:
                                    time_diffs = (
                                        completed_users[f"{stage_key}_timestamp"] -
                                        completed_users[f"{prev_stage_key}_timestamp"]
                                    ).dt.total_seconds() / 3600  # Convert to hours

                                    overall[f"{stage_name}_median_hours"] = time_diffs.median()

        metrics.append(overall)

        # Segment by source if enabled
        if self.segment_by_source and 'source' in user_progressions.columns:
            for source in user_progressions['source'].unique():
                if pd.isna(source):
                    continue

                source_data = user_progressions[user_progressions['source'] == source]
                segment = {'segment': source, 'cohort': 'all'}

                for i, (event, stage_name) in enumerate(stages):
                    stage_key = f"stage_{i+1}"
                    completed_col = f"{stage_key}_completed"

                    if completed_col in source_data.columns:
                        reached = source_data[completed_col].sum()
                        segment[f"{stage_name}_count"] = reached

                        if i > 0:
                            prev_stage = stages[i-1][1]
                            prev_count = segment.get(f"{prev_stage}_count", 0)

                            if prev_count > 0:
                                conversion_rate = reached / prev_count
                                segment[f"{stage_name}_conversion_rate"] = conversion_rate
                                segment[f"{stage_name}_drop_off_rate"] = 1 - conversion_rate

                metrics.append(segment)

        # Cohort analysis if enabled
        if self.group_by_cohort and 'stage_1_timestamp' in user_progressions.columns:
            user_progressions['cohort_date'] = user_progressions['stage_1_timestamp']

            if self.cohort_period == 'daily':
                user_progressions['cohort'] = user_progressions['cohort_date'].dt.date
            elif self.cohort_period == 'weekly':
                user_progressions['cohort'] = user_progressions['cohort_date'].dt.to_period('W').dt.start_time
            else:  # monthly
                user_progressions['cohort'] = user_progressions['cohort_date'].dt.to_period('M').dt.start_time

            for cohort in user_progressions['cohort'].unique():
                if pd.isna(cohort):
                    continue

                cohort_data = user_progressions[user_progressions['cohort'] == cohort]
                cohort_metric = {'segment': 'overall', 'cohort': str(cohort)}

                for i, (event, stage_name) in enumerate(stages):
                    stage_key = f"stage_{i+1}"
                    completed_col = f"{stage_key}_completed"

                    if completed_col in cohort_data.columns:
                        reached = cohort_data[completed_col].sum()
                        cohort_metric[f"{stage_name}_count"] = reached

                        if i > 0:
                            prev_stage = stages[i-1][1]
                            prev_count = cohort_metric.get(f"{prev_stage}_count", 0)

                            if prev_count > 0:
                                conversion_rate = reached / prev_count
                                cohort_metric[f"{stage_name}_conversion_rate"] = conversion_rate

                metrics.append(cohort_metric)

        result_df = pd.DataFrame(metrics)

        # Identify drop-off stages if enabled
        if self.identify_drop_offs:
            for i in range(1, len(stages)):
                stage_name = stages[i][1]
                drop_off_col = f"{stage_name}_drop_off_rate"

                if drop_off_col in result_df.columns:
                    result_df[f"{stage_name}_high_drop_off"] = result_df[drop_off_col] > self.drop_off_threshold

        return result_df

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build asset definitions."""
        asset_name = self.asset_name

        # Require event data
        if not self.event_data_asset:
            raise ValueError("Event data asset is required for funnel analysis")

        asset_ins = {
            "event_data": AssetIn(key=AssetKey.from_user_string(self.event_data_asset))
        }

        # Optional user data for segmentation
        if self.user_data_asset:
            asset_ins["user_data"] = AssetIn(key=AssetKey.from_user_string(self.user_data_asset))

        component = self

        @asset(
            name=asset_name,
            ins=asset_ins,
            description=self.description or "Funnel analysis with conversion rates and drop-off identification",
            group_name=self.group_name,
        )
        def funnel_analysis_asset(context: AssetExecutionContext, **inputs) -> pd.DataFrame:
            """Analyze user progression through conversion funnel."""

            stages = component._get_funnel_stages()

            if len(stages) < 2:
                raise ValueError("Funnel must have at least 2 stages configured")

            context.log.info(f"Analyzing {len(stages)}-stage funnel: {' → '.join([name for _, name in stages])}")

            # Prepare event data
            event_data = inputs.get('event_data')
            prepared_data = component._prepare_event_data(event_data)

            if prepared_data.empty:
                context.log.warning("No event data available for analysis")
                return pd.DataFrame()

            context.log.info(f"Processing {len(prepared_data)} events from {prepared_data['user_id'].nunique()} users")

            # Calculate funnel based on type
            if component.funnel_type == 'linear':
                context.log.info("Calculating linear (sequential) funnel...")
                user_progressions = component._calculate_linear_funnel(prepared_data, stages)
            else:
                # Flexible funnel not implemented in this version
                context.log.warning("Flexible funnel not yet implemented, using linear")
                user_progressions = component._calculate_linear_funnel(prepared_data, stages)

            if user_progressions.empty:
                context.log.warning("No user progressions found")
                return pd.DataFrame()

            # Aggregate into metrics
            context.log.info("Aggregating funnel metrics...")
            funnel_metrics = component._aggregate_funnel_metrics(user_progressions, stages)

            # Log summary
            overall = funnel_metrics[funnel_metrics['segment'] == 'overall'].iloc[0]
            for i, (event, stage_name) in enumerate(stages):
                count = overall.get(f"{stage_name}_count", 0)
                context.log.info(f"  {stage_name}: {count} users")

                if i > 0:
                    conversion = overall.get(f"{stage_name}_conversion_rate", 0)
                    context.log.info(f"    Conversion: {conversion*100:.1f}%")

            return funnel_metrics

        return build_defs_from_component(
            context=context,
            component=self,
            asset_defs=[funnel_analysis_asset],
        )
