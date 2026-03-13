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
    MetadataValue,
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
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
    )

    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")

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

        # Infer kinds from component name if not explicitly set
        _comp_name = "funnel_analysis"  # component directory name
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None


        @asset(
            name=asset_name,
            ins=asset_ins,
            description=self.description or "Funnel analysis with conversion rates and drop-off identification",
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=self.group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def funnel_analysis_asset(context: AssetExecutionContext, **inputs) -> pd.DataFrame:
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
