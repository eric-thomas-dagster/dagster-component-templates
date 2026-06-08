"""UnpivotComponent.

Unpivot a DataFrame from wide to long — melt N value columns into 2 columns (variable, value).
"""

import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class UnpivotComponent(dg.Component, dg.Model, dg.Resolvable):
    """Unpivot a DataFrame from wide to long — melt N value columns into 2 columns (variable, value)."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key")

    id_columns: list = Field(description="Columns to keep as identifiers (not melted)")
    value_columns: Optional[list] = Field(default=None, description="Columns to melt (None = all non-id columns)")
    var_name: str = Field(default="variable", description="Name for the new 'variable' column")
    value_name: str = Field(default="value", description="Name for the new 'value' column")
    drop_null_values: bool = Field(default=False, description="Drop rows where the melted value is NaN")

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
            name=self.asset_name,
            description=self.description or "Unpivot a DataFrame from wide to long — melt N value columns into 2 columns (variable, value).",
            group_name=self.group_name,
            kinds=set(self.kinds or ['unpivot', 'melt']),
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
            # pd.melt() raises if any id_vars / value_vars column is missing
            # from the input. Drop missing columns from each list and log
            # a warning — preferable to crashing on a partial-shape upstream.
            id_present = [c for c in (self.id_columns or []) if c in df.columns]
            id_missing = [c for c in (self.id_columns or []) if c not in df.columns]
            val_present = None
            val_missing: list = []
            if self.value_columns is not None:
                val_present = [c for c in self.value_columns if c in df.columns]
                val_missing = [c for c in self.value_columns if c not in df.columns]
            if id_missing or val_missing:
                context.log.warning(
                    f"Unpivot upstream is missing columns; melting what's available. "
                    f"Missing id_columns: {id_missing!r}; missing value_columns: {val_missing!r}"
                )
            # pd.melt rejects var_name / value_name that match ANY existing
            # column (even ones being melted). When the colliding column is
            # being melted (not in id_columns / not a survivor), pre-rename
            # it to a unique sentinel before melt so the user-supplied var/
            # value name stays. After melt, the sentinel is gone anyway
            # (it was a melted column). When the colliding column IS a
            # survivor (in id_columns), auto-bump var/value name with _1, _2
            # to avoid stomping it.
            _surviving = set(id_present)
            if val_present is not None:
                _surviving = set(df.columns) - set(val_present)
            _melt_cols = set(df.columns) - _surviving

            for _name in (self.var_name, self.value_name):
                if _name in _melt_cols:
                    # Pre-rename to a sentinel so melt accepts; the sentinel
                    # column gets melted into the value column anyway.
                    i = 1
                    sentinel = f"{_name}__melted_{i}"
                    while sentinel in df.columns:
                        i += 1
                        sentinel = f"{_name}__melted_{i}"
                    df = df.rename(columns={_name: sentinel})
                    _melt_cols.discard(_name)
                    _melt_cols.add(sentinel)
                    context.log.info(
                        f"Unpivot: pre-renamed colliding melted column "
                        f"{_name!r} → {sentinel!r} so var_name/value_name can "
                        f"keep their configured value."
                    )

            def _unique_name(base: str) -> str:
                if base not in df.columns:
                    return base
                # Match the common BI-tool convention: `<name>2`, `<name>3`, …
                # (rather than `<name>_1` which doesn't survive round-trip
                # workflows that reference auto-generated duplicate-column
                # names by index suffix).
                i = 2
                while f"{base}{i}" in df.columns:
                    i += 1
                return f"{base}{i}"
            var_name_final = _unique_name(self.var_name)
            value_name_final = _unique_name(self.value_name)
            if var_name_final != self.var_name or value_name_final != self.value_name:
                context.log.warning(
                    f"Unpivot output column-name collision with surviving "
                    f"upstream column — renamed var_name {self.var_name!r}->"
                    f"{var_name_final!r}, value_name {self.value_name!r}->"
                    f"{value_name_final!r}."
                )
            # Also refresh id_present / val_present after the pre-rename to
            # avoid passing a stale column name to melt.
            id_present = [c for c in (self.id_columns or []) if c in df.columns]
            if val_present is not None:
                val_present = [c for c in self.value_columns if c in df.columns]
            out = df.melt(
                id_vars=id_present,
                value_vars=val_present,
                var_name=var_name_final,
                value_name=value_name_final,
            )
            if self.drop_null_values:
                out = out.dropna(subset=[self.value_name])
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
