"""WindowCalculationComponent.

Window functions — PARTITION BY + ORDER BY + (lag, lead, rank, dense_rank, row_number, cumulative sum, moving avg). The full SQL window-function shape, in pandas.
"""

import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class WindowCalculationComponent(dg.Component, dg.Model, dg.Resolvable):
    """Window functions — PARTITION BY + ORDER BY + (lag, lead, rank, dense_rank, row_number, cumulative sum, moving avg). The full SQL window-function shape, in pandas."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key")

    partition_by: Optional[list] = Field(default=None, description="Columns to partition by (None = single global partition)")
    order_by: Optional[list] = Field(default=None, description="Columns to sort within each partition (with optional 'desc' suffix per col)")
    operations: list = Field(description="List of {output: name, func: ..., column: ..., periods: ..., window: ...} dicts")

    description: Optional[str] = Field(default=None)
    group_name: str = Field(default="transforms")
    deps: Optional[list[str]] = Field(default=None)
    owners: Optional[list[str]] = Field(default=None)
    asset_tags: Optional[dict] = Field(default=None)
    kinds: Optional[list[str]] = Field(default=None)
    include_preview_metadata: bool = Field(default=True)
    preview_rows: int = Field(default=20)

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _values:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_values)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        _self = self

        @dg.asset(
            key=dg.AssetKey.from_user_string(self.asset_name),
            description=self.description or "Window functions — PARTITION BY + ORDER BY + (lag, lead, rank, dense_rank, row_number, cumulative sum, moving avg). The full SQL window-function shape, in pandas.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['window', 'analytic']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)] + [dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame) -> pd.DataFrame:
            self = _self
            df = df.copy()
            # Sort once if order_by provided
            if self.order_by:
                sort_cols, sort_asc = [], []
                for col in self.order_by:
                    if col.lower().endswith(" desc"):
                        sort_cols.append(col[:-5].strip()); sort_asc.append(False)
                    else:
                        sort_cols.append(col); sort_asc.append(True)
                df = df.sort_values(by=sort_cols, ascending=sort_asc).reset_index(drop=True)

            groups = df.groupby(self.partition_by, sort=False) if self.partition_by else None

            for op in self.operations:
                out_col = op["output"]
                func = op["func"]
                src_col = op.get("column")
                if func == "row_number":
                    df[out_col] = (groups.cumcount() if groups is not None else range(len(df))) + 1
                elif func == "rank":
                    df[out_col] = (groups[src_col].rank(method="min") if groups is not None else df[src_col].rank(method="min"))
                elif func == "dense_rank":
                    df[out_col] = (groups[src_col].rank(method="dense") if groups is not None else df[src_col].rank(method="dense"))
                elif func == "lag":
                    periods = op.get("periods", 1)
                    df[out_col] = (groups[src_col].shift(periods) if groups is not None else df[src_col].shift(periods))
                elif func == "lead":
                    periods = op.get("periods", 1)
                    df[out_col] = (groups[src_col].shift(-periods) if groups is not None else df[src_col].shift(-periods))
                elif func == "cumsum":
                    df[out_col] = (groups[src_col].cumsum() if groups is not None else df[src_col].cumsum())
                elif func == "cummax":
                    df[out_col] = (groups[src_col].cummax() if groups is not None else df[src_col].cummax())
                elif func == "cummin":
                    df[out_col] = (groups[src_col].cummin() if groups is not None else df[src_col].cummin())
                elif func == "moving_avg":
                    window = op.get("window", 3)
                    if groups is not None:
                        df[out_col] = groups[src_col].transform(lambda s: s.rolling(window, min_periods=1).mean())
                    else:
                        df[out_col] = df[src_col].rolling(window, min_periods=1).mean()
                elif func == "moving_sum":
                    window = op.get("window", 3)
                    if groups is not None:
                        df[out_col] = groups[src_col].transform(lambda s: s.rolling(window, min_periods=1).sum())
                    else:
                        df[out_col] = df[src_col].rolling(window, min_periods=1).sum()
                else:
                    raise ValueError(f"unknown window function: {func}")
            context.add_output_metadata({
                "dagster/row_count": dg.MetadataValue.int(len(df)),
            })
            if _self.include_preview_metadata and len(df) > 0:
                try:
                    sample = df.sample(min(_self.preview_rows, len(df))) if len(df) > _self.preview_rows * 10 else df.head(_self.preview_rows)
                    context.add_output_metadata({"preview": dg.MetadataValue.md(sample.to_markdown(index=False))})
                except Exception:
                    pass
            return df

        return dg.Definitions(assets=[_asset])
