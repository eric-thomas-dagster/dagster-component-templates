from dataclasses import dataclass
from typing import Optional, List, Union
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


@dataclass
class DocumentMergerComponent(Component, Model, Resolvable):
    """Merge two document DataFrames on a key column using pandas.merge()."""

    asset_name: str = Field(description="Output Dagster asset name")
    left_asset_key: str = Field(description="Asset key for the left DataFrame")
    right_asset_key: str = Field(description="Asset key for the right DataFrame")
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
    on: Union[str, List[str]] = Field(description="Join key column(s)")
    how: str = Field(
        default="left",
        description="Join type: 'inner', 'left', 'right', or 'outer'.",
    )
    suffixes: List[str] = Field(
        default_factory=lambda: ["_left", "_right"],
        description="Two-element list of suffixes for overlapping column names.",
    )
    flatten_result: bool = Field(
        default=False,
        description="Auto-flatten any nested dict columns after the merge.",
    )

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        left_asset_key = self.left_asset_key
        right_asset_key = self.right_asset_key
        group_name = self.group_name
        on = self.on
        how = self.how
        suffixes = self.suffixes
        flatten_result = self.flatten_result

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
            ins={
                "left": AssetIn(key=AssetKey.from_user_string(left_asset_key)),
                "right": AssetIn(key=AssetKey.from_user_string(right_asset_key)),
            },
            partitions_def=partitions_def,
            group_name=group_name,
        )
        def _asset(
            context: AssetExecutionContext,
            # Filter to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                for _side in [left, right]:
                    if partition_date_column and partition_date_column in _side.columns and _date_key:
                        _side = _side[_side[partition_date_column].astype(str) == _date_key]
                    if partition_static_column and partition_static_column in _side.columns and _static_key:
                        _side = _side[_side[partition_static_column].astype(str) == _static_key]
            left: pd.DataFrame,
            right: pd.DataFrame,
        ) -> pd.DataFrame:
            if how not in ("inner", "left", "right", "outer"):
                raise ValueError(f"how must be 'inner', 'left', 'right', or 'outer', got: {how}")
            if len(suffixes) != 2:
                raise ValueError(f"suffixes must be a list of exactly 2 strings, got: {suffixes}")

            result = pd.merge(
                left,
                right,
                on=on,
                how=how,
                suffixes=tuple(suffixes),
            )

            if flatten_result:
                try:
                    from pandas import json_normalize
                except ImportError:
                    from pandas.io.json import json_normalize

                object_cols = [c for c in result.columns if result[c].dtype == object]
                for col in object_cols:
                    sample = result[col].dropna()
                    if len(sample) > 0 and isinstance(sample.iloc[0], dict):
                        try:
                            flat = json_normalize(result[col].tolist(), sep=".")
                            flat.index = result.index
                            flat.columns = [f"{col}.{c}" for c in flat.columns]
                            result = pd.concat([result.drop(columns=[col]), flat], axis=1)
                        except Exception as e:
                            context.log.warning(f"Could not flatten column '{col}': {e}")

            context.add_output_metadata({
                "row_count": MetadataValue.int(len(result)),
                "columns": MetadataValue.int(len(result.columns)),
                "left_rows": MetadataValue.int(len(left)),
                "right_rows": MetadataValue.int(len(right)),
            })
            return result

        return Definitions(assets=[_asset])
