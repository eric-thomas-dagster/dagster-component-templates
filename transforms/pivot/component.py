"""PivotComponent.

Pivot a DataFrame from long to wide — rotate row values into column headers with a chosen aggregation.
"""

import os
from typing import Any, Optional, Union

import dagster as dg
import pandas as pd
from pydantic import Field


class PivotComponent(dg.Component, dg.Model, dg.Resolvable):
    """Pivot a DataFrame from long to wide — rotate row values into column headers with a chosen aggregation."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key")

    index_columns: list = Field(description="Columns that stay as rows (group keys)")
    pivot_column: Union[str, int] = Field(description="Column whose values become new column headers")
    value_column: Union[str, int] = Field(description="Column whose values fill the pivoted cells")
    agg_func: str = Field(default="sum", description="Aggregation when (index, pivot) collides: sum | mean | count | min | max | first | last")
    fill_value: Optional[float] = Field(default=None, description="Fill NaN cells with this value")
    column_prefix: Optional[Union[str, int]] = Field(
        default=None,
        description=(
            "Optional prefix prepended to each pivoted column name. Useful when "
            "the pivot's aggregation context should be visible in the column "
            "(e.g. prefix='Avg_' → pivoted 'Speed'/'Acceleration' become "
            "'Avg_Speed'/'Avg_Acceleration')."
        ),
    )

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
            description=self.description or "Pivot a DataFrame from long to wide — rotate row values into column headers with a chosen aggregation.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['pivot']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)] + [dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: dg.AssetExecutionContext, df: Any) -> pd.DataFrame:
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(df, dict):
                _frames = [v for v in df.values() if isinstance(v, pd.DataFrame)]
                df = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            self = _self
            _required = list(self.index_columns) + [self.pivot_column, self.value_column]
            _missing = [c for c in _required if c not in df.columns]
            if _missing:
                context.log.warning(
                    f"pivot: columns {_missing} not present in upstream "
                    f"(have {list(df.columns)[:10]}). Returning upstream unchanged."
                )
                return df.copy()
            # Stability guards — pivot output is only as predictable as
            # its `pivot_column` values. Surface the surprises loudly so
            # downstream KeyErrors on missing pivoted columns are
            # diagnosable from the run logs.
            if len(df) == 0:
                context.log.warning(
                    f"pivot: upstream has 0 rows — output will have no "
                    f"pivoted columns. Downstream tools referencing "
                    f"`<column_prefix><value>` columns (e.g. Avg_Speed) "
                    "will hit KeyError. Check upstream chain for empty "
                    "filters / failed joins."
                )
            else:
                _pivot_values = df[self.pivot_column].dropna().unique()
                if len(_pivot_values) == 0:
                    context.log.warning(
                        f"pivot: `{self.pivot_column}` column has 0 distinct "
                        "non-null values — no pivoted columns will be emitted."
                    )
                elif len(_pivot_values) <= 1:
                    context.log.info(
                        f"pivot: `{self.pivot_column}` has only "
                        f"{len(_pivot_values)} distinct value(s) "
                        f"({list(_pivot_values)}); output will have "
                        "just that one pivoted column. Downstream tools "
                        "expecting multiple pivoted columns will fail."
                    )
                else:
                    context.log.info(
                        f"pivot: pivoting on `{self.pivot_column}` with "
                        f"{len(_pivot_values)} distinct values: "
                        f"{list(_pivot_values)[:10]}"
                        + ("..." if len(_pivot_values) > 10 else "")
                    )
            # Translate SQL-ish agg-func names → pandas vocabulary
            # (otherwise pandas raises 'avg is not a valid function').
            _AGG_ALIASES = {
                "avg": "mean", "average": "mean", "stddev": "std", "variance": "var",
                "countdistinct": "nunique", "distinct_count": "nunique",
                "concat": "first",  # pandas pivot_table can't concat; best-effort.
            }
            _agg = _AGG_ALIASES.get(str(self.agg_func).lower(), self.agg_func)
            out = df.pivot_table(
                index=self.index_columns, columns=self.pivot_column,
                values=self.value_column, aggfunc=_agg,
                fill_value=self.fill_value,
            ).reset_index()
            out.columns = [str(c) for c in out.columns]
            # Apply column_prefix to every pivoted (non-index) column so
            # downstream tools that reference `<prefix><pivot_value>` (e.g.
            # `Avg_Speed`) find the right column.
            if self.column_prefix:
                _idx_set = set(self.index_columns)
                out.columns = [
                    (c if c in _idx_set else f"{self.column_prefix}{c}")
                    for c in out.columns
                ]
            df = out
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
