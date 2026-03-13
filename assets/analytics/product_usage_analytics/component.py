"""Product Usage Analytics Component.

Analyzes product feature usage to measure adoption, engagement, and identify
power users vs. inactive users for product-led growth strategies.
"""

from typing import Any, Optional

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


@component_type(name="product_usage_analytics")
class ProductUsageAnalyticsComponent(Component):
    """Component that analyzes product feature usage and user engagement."""

    asset_name: str = Field(
        ...,
        description="Name of the product usage analytics asset to create",
    )

    # Input asset references
    event_data_asset: Optional[str] = Field(
        default="",
        description="Product usage events with user_id, event_name, timestamp",
    )

    user_data_asset: Optional[str] = Field(
        default="",
        description="User data for additional context (optional)",
    )

    # Analysis configuration
    analysis_period_days: int = Field(
        default=30,
        description="Number of days to analyze",
    )

    core_feature_events: str = Field(
        default="",
        description="Comma-separated list of core feature event names",
    )

    calculate_dau_mau: bool = Field(
        default=True,
        description="Calculate DAU/MAU ratio (stickiness)",
    )

    calculate_feature_adoption: bool = Field(
        default=True,
        description="Calculate feature adoption rates",
    )

    identify_power_users: bool = Field(
        default=True,
        description="Identify power users based on usage",
    )

    power_user_threshold: int = Field(
        default=20,
        description="Number of actions to qualify as power user",
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

    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")

    def _prepare_event_data(self, event_data: pd.DataFrame) -> pd.DataFrame:
        """Prepare and validate event data."""
        if event_data is None or event_data.empty:
            return pd.DataFrame()

        df = event_data.copy()

        # Standardize columns
        if 'user_id' not in df.columns:
            for col in ['customer_id', 'id', 'userid']:
                if col in df.columns:
                    df['user_id'] = df[col]
                    break

        if 'timestamp' not in df.columns:
            for col in ['event_timestamp', 'created_at', 'event_time']:
                if col in df.columns:
                    df['timestamp'] = df[col]
                    break

        # Validate
        if 'user_id' not in df.columns:
            raise ValueError("Event data must have user_id column")
        if 'timestamp' not in df.columns:
            raise ValueError("Event data must have timestamp column")

        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Filter to analysis period
        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=self.analysis_period_days)
        df = df[df['timestamp'] >= cutoff_date]

        return df

    def _calculate_engagement_metrics(self, events: pd.DataFrame) -> pd.DataFrame:
        """Calculate user engagement metrics."""
        metrics = events.groupby('user_id').agg({
            'timestamp': ['count', 'min', 'max'],
            'user_id': 'first'
        })

        metrics.columns = ['total_actions', 'first_seen', 'last_seen', 'user_id']
        metrics = metrics.reset_index(drop=True)

        # Calculate days active
        metrics['days_active'] = (
            (metrics['last_seen'] - metrics['first_seen']).dt.days + 1
        )

        # Calculate actions per day
        metrics['actions_per_day'] = metrics['total_actions'] / metrics['days_active']

        return metrics

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build asset definitions."""
        asset_name = self.asset_name

        if not self.event_data_asset:
            raise ValueError("Event data asset is required")

        asset_ins = {
            "event_data": AssetIn(key=AssetKey.from_user_string(self.event_data_asset))
        }

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

        @asset(
            name=asset_name,
            ins=asset_ins,
            description=self.description or "Product usage analytics with engagement metrics and power user identification",
            partitions_def=partitions_def,
            group_name=self.group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def product_usage_asset(context: AssetExecutionContext, **inputs) -> pd.DataFrame:
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
            """Analyze product usage patterns."""

            context.log.info(f"Analyzing product usage for past {component.analysis_period_days} days...")

            event_data = inputs.get('event_data')
            events = component._prepare_event_data(event_data)

            if events.empty:
                context.log.warning("No event data available")
                return pd.DataFrame()

            context.log.info(f"Processing {len(events)} events from {events['user_id'].nunique()} users")

            # Calculate engagement metrics
            metrics = component._calculate_engagement_metrics(events)

            # Identify power users
            if component.identify_power_users:
                metrics['is_power_user'] = metrics['total_actions'] >= component.power_user_threshold

                power_user_count = metrics['is_power_user'].sum()
                power_user_pct = power_user_count / len(metrics) * 100
                context.log.info(f"Power users: {power_user_count} ({power_user_pct:.1f}%)")

            context.log.info(f"Average actions per user: {metrics['total_actions'].mean():.1f}")
            context.log.info(f"Average days active: {metrics['days_active'].mean():.1f}")

            return metrics

        return build_defs_from_component(
            context=context,
            component=self,
            asset_defs=[product_usage_asset],
        )
