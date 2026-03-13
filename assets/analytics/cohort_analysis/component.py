"""Cohort Analysis Component.

Track customer retention by acquisition cohort over time periods.
Analyzes how different customer cohorts retain and engage over their lifecycle.
"""

from typing import Optional, Literal
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


class CohortAnalysisComponent(Component, Model, Resolvable):
    """Component for cohort retention analysis.

    Cohort analysis tracks how groups of customers (cohorts) acquired at the same time
    behave over subsequent time periods. This is essential for understanding:
    - Customer retention rates
    - Lifetime value patterns
    - Product-market fit
    - Impact of product changes on different cohorts

    This component accepts customer activity data and produces a cohort retention matrix
    showing the percentage of customers active in each period after acquisition.

    Example:
        ```yaml
        type: dagster_component_templates.CohortAnalysisComponent
        attributes:
          asset_name: customer_cohort_retention
          upstream_asset_key: customer_activity
          cohort_period: monthly
          retention_periods: 12
          include_revenue: true
          description: "Monthly cohort retention analysis"
          group_name: customer_analytics
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame with customer activity data"
    )

    cohort_period: Literal["daily", "weekly", "monthly"] = Field(
        default="monthly",
        description="Time period for cohort grouping"
    )

    retention_periods: int = Field(
        default=12,
        description="Number of periods to track retention"
    )

    include_revenue: bool = Field(
        default=False,
        description="Include average revenue per user in analysis"
    )

    customer_id_field: Optional[str] = Field(
        default=None,
        description="Customer ID column name (auto-detected if not specified)"
    )

    first_date_field: Optional[str] = Field(
        default=None,
        description="First activity date column (auto-detected if not specified)"
    )

    activity_date_field: Optional[str] = Field(
        default=None,
        description="Activity date column (auto-detected if not specified)"
    )

    revenue_field: Optional[str] = Field(
        default=None,
        description="Revenue column name (required if include_revenue=True)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="customer_analytics",
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
        cohort_period = self.cohort_period
        retention_periods = self.retention_periods
        include_revenue = self.include_revenue
        customer_id_field = self.customer_id_field
        first_date_field = self.first_date_field
        activity_date_field = self.activity_date_field
        revenue_field = self.revenue_field
        description = self.description or "Cohort retention analysis"
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
        def cohort_analysis_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            """Asset that performs cohort retention analysis."""

            df = upstream
            if not isinstance(df, pd.DataFrame):
                context.log.error("Source data is not a DataFrame")
                return pd.DataFrame()

            context.log.info(f"Processing {len(df)} activity records for cohort analysis")

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
            first_date_col = find_column(
                ['first_order_date', 'first_activity_date', 'signup_date', 'created_at', 'first_date'],
                first_date_field
            )
            activity_date_col = find_column(
                ['activity_date', 'order_date', 'date', 'transaction_date', 'timestamp'],
                activity_date_field
            )

            # Validate required columns
            missing = []
            if not customer_col:
                missing.append("customer_id")
            if not first_date_col:
                missing.append("first_order_date/first_activity_date")
            if not activity_date_col:
                missing.append("activity_date")

            if include_revenue:
                revenue_col = find_column(
                    ['revenue', 'amount', 'total', 'price', 'value'],
                    revenue_field
                )
                if not revenue_col:
                    missing.append("revenue (required when include_revenue=True)")

            if missing:
                context.log.error(f"Missing required columns: {', '.join(missing)}")
                context.log.info(f"Available columns: {', '.join(df.columns)}")
                return pd.DataFrame()

            context.log.info(f"Using columns - Customer: {customer_col}, First Date: {first_date_col}, Activity Date: {activity_date_col}")

            # Prepare data
            cohort_df = df[[customer_col, first_date_col, activity_date_col]].copy()
            if include_revenue and revenue_col:
                cohort_df[revenue_col] = df[revenue_col]

            cohort_df.columns = ['customer_id', 'first_date', 'activity_date'] + (['revenue'] if include_revenue else [])

            # Parse dates
            cohort_df['first_date'] = pd.to_datetime(cohort_df['first_date'], errors='coerce')
            cohort_df['activity_date'] = pd.to_datetime(cohort_df['activity_date'], errors='coerce')
            cohort_df = cohort_df.dropna(subset=['first_date', 'activity_date'])

            if include_revenue:
                cohort_df['revenue'] = pd.to_numeric(cohort_df['revenue'], errors='coerce').fillna(0)

            if len(cohort_df) == 0:
                context.log.warning("No valid activity records after date parsing")
                return pd.DataFrame()

            # Determine period frequency
            period_freq = {
                'daily': 'D',
                'weekly': 'W',
                'monthly': 'M'
            }[cohort_period]

            # Assign cohort period based on first activity
            cohort_df['cohort_period'] = cohort_df['first_date'].dt.to_period(period_freq)

            # Assign activity period
            cohort_df['activity_period'] = cohort_df['activity_date'].dt.to_period(period_freq)

            # Calculate period number (offset from cohort period)
            cohort_df['period_number'] = (
                cohort_df['activity_period'].astype('int64') -
                cohort_df['cohort_period'].astype('int64')
            )

            # Filter to valid periods (>= 0 and within retention window)
            cohort_df = cohort_df[
                (cohort_df['period_number'] >= 0) &
                (cohort_df['period_number'] <= retention_periods)
            ]

            if len(cohort_df) == 0:
                context.log.warning("No activity records within retention period window")
                return pd.DataFrame()

            # Filter cohorts that have had enough time to mature
            current_period = pd.Timestamp.now().to_period(period_freq)
            max_cohort_period = current_period - retention_periods
            cohort_df = cohort_df[cohort_df['cohort_period'] <= max_cohort_period]

            if len(cohort_df) == 0:
                context.log.warning(f"No cohorts old enough to track {retention_periods} {cohort_period} periods")
                return pd.DataFrame()

            context.log.info(f"Analyzing {len(cohort_df['cohort_period'].unique())} cohorts")

            # Calculate cohort sizes
            cohort_sizes = cohort_df.groupby('cohort_period')['customer_id'].nunique()

            # Count unique active customers per cohort/period
            retention_counts = cohort_df.groupby(['cohort_period', 'period_number'])['customer_id'].nunique()

            # Calculate retention percentages
            retention_df = retention_counts.reset_index()
            retention_df.columns = ['cohort_period', 'period_number', 'active_customers']

            # Add cohort sizes
            retention_df['cohort_size'] = retention_df['cohort_period'].map(cohort_sizes)

            # Calculate retention percentage
            retention_df['retention_pct'] = (
                (retention_df['active_customers'] / retention_df['cohort_size'].replace(0, np.nan)) * 100
            ).round(2)

            # Include revenue if requested
            if include_revenue:
                revenue_per_period = cohort_df.groupby(['cohort_period', 'period_number'])['revenue'].sum()
                retention_df['total_revenue'] = retention_df.apply(
                    lambda row: revenue_per_period.get((row['cohort_period'], row['period_number']), 0),
                    axis=1
                )
                retention_df['avg_revenue_per_user'] = (
                    retention_df['total_revenue'] / retention_df['active_customers'].replace(0, np.nan)
                ).round(2)

            # Pivot to wide format for easier analysis
            pivot_retention = retention_df.pivot(
                index='cohort_period',
                columns='period_number',
                values='retention_pct'
            )

            # Add cohort size as first column
            pivot_retention.insert(0, 'cohort_size', cohort_sizes)

            # Rename period columns
            pivot_retention.columns = ['cohort_size'] + [f'period_{i}' for i in range(retention_periods + 1)]

            # Reset index to make cohort_period a column
            result_df = pivot_retention.reset_index()
            result_df['cohort_period'] = result_df['cohort_period'].astype(str)

            context.log.info(f"Cohort analysis complete: {len(result_df)} cohorts tracked over {retention_periods} periods")

            # Calculate average retention rates across all cohorts
            avg_retention = {}
            for i in range(retention_periods + 1):
                col = f'period_{i}'
                if col in result_df.columns:
                    avg_retention[col] = result_df[col].mean()

            context.log.info(f"Average retention - Period 0: {avg_retention.get('period_0', 0):.1f}%, Period {retention_periods}: {avg_retention.get(f'period_{retention_periods}', 0):.1f}%")

            # Add metadata
            metadata = {
                "row_count": len(result_df),
                "total_cohorts": len(result_df),
                "cohort_period": cohort_period,
                "retention_periods": retention_periods,
                "avg_period_0_retention": round(avg_retention.get('period_0', 0), 2),
                "avg_final_period_retention": round(avg_retention.get(f'period_{retention_periods}', 0), 2)
            }

            # Return with metadata
            if include_sample and len(result_df) > 0:
                # Show most recent cohorts first
                result_sorted = result_df.sort_values('cohort_period', ascending=False)

                return Output(
                    value=result_df,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(result_sorted.head(10).to_markdown(index=False)),
                        "preview": MetadataValue.dataframe(result_sorted.head(10))
                    }
                )
            else:
                context.add_output_metadata(metadata)
                return result_df

        return Definitions(assets=[cohort_analysis_asset])
