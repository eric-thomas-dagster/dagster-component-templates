"""Generate Rows.

Expand a DataFrame by repeating rows, appending new rows, or cross-joining with new rows.
"""
from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


@dataclass
class GenerateRowsComponent(Component, Model, Resolvable):
    """Expand a DataFrame by repeating, appending, or cross-joining rows."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    mode: str = Field(
        default="repeat",
        description=(
            "Expansion mode: 'repeat' (duplicate each row N times), "
            "'cross_join' (cross product with new_rows), "
            "'append' (append fixed new rows)"
        ),
    )
    n: int = Field(
        default=1,
        description="For mode='repeat': number of times to repeat each row",
    )
    new_rows: Optional[List[Dict]] = Field(
        default=None,
        description="For mode='append' or 'cross_join': list of row dicts to use",
    )
    reset_index: bool = Field(default=True, description="Reset the index after expansion")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
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

    @classmethod
    def get_description(cls) -> str:
        return "Expand a DataFrame by repeating, appending, or cross-joining rows."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        mode = self.mode
        n = self.n
        new_rows = self.new_rows
        do_reset_index = self.reset_index
        group_name = self.group_name

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
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
            group_name=group_name,
            description=GenerateRowsComponent.get_description(),
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            df = upstream.copy()

            if mode == "repeat":
                result = pd.concat([df] * n, ignore_index=do_reset_index)
                context.log.info(f"Repeated {len(df)} rows x{n} = {len(result)} rows")

            elif mode == "append" and new_rows:
                new_df = pd.DataFrame(new_rows)
                result = pd.concat([df, new_df], ignore_index=do_reset_index)
                context.log.info(f"Appended {len(new_rows)} rows; total {len(result)}")

            elif mode == "cross_join" and new_rows:
                new_df = pd.DataFrame(new_rows)
                df["_key"] = 1
                new_df["_key"] = 1
                result = df.merge(new_df, on="_key").drop(columns=["_key"])
                if do_reset_index:
                    result = result.reset_index(drop=True)
                context.log.info(
                    f"Cross-joined {len(df)} x {len(new_df)} = {len(result)} rows"
                )

            else:
                context.log.warning(
                    f"No operation performed: mode='{mode}', new_rows={'set' if new_rows else 'None'}"
                )
                result = df

            return result

        return Definitions(assets=[_asset])
