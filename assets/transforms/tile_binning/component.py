"""Tile Binning Asset Component.

Assign records to bins or buckets based on a numeric column's value range,
using equal-width intervals, equal-frequency quantiles, or custom bin edges.
"""
from dataclasses import dataclass
from typing import Optional, List

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
class TileBinningComponent(Component, Model, Resolvable):
    """Assign records to bins based on a numeric column's value range.

    Supports three binning methods: equal-width intervals (pd.cut), equal-frequency
    quantile bins (pd.qcut), and custom bin edges. Optionally adds a numeric bin
    index column alongside the labeled bin column.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    column: str = Field(description="Numeric column to bin")
    n_bins: int = Field(default=10, description="Number of bins")
    method: str = Field(
        default="equal_width",
        description="Binning method: 'equal_width' (pd.cut), 'equal_freq' (pd.qcut), or 'custom' (use bin_edges)",
    )
    bin_edges: Optional[List[float]] = Field(
        default=None,
        description="Custom bin edges for method='custom'",
    )
    labels: Optional[List[str]] = Field(
        default=None,
        description="Labels for bins (length must equal n_bins or len(bin_edges) - 1)",
    )
    output_column: Optional[str] = Field(
        default=None,
        description="Output column name (defaults to '{column}_bin')",
    )
    include_numeric_label: bool = Field(
        default=False,
        description="Also add '{column}_bin_num' column with 0-indexed bin number",
    )
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
        return "Assign records to bins/buckets based on a numeric column's value range."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        column = self.column
        n_bins = self.n_bins
        method = self.method
        bin_edges = self.bin_edges
        labels = self.labels
        output_column = self.output_column
        include_numeric_label = self.include_numeric_label
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
            description=TileBinningComponent.get_description(),
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
            out_col = output_column or f"{column}_bin"

            if method == "equal_width":
                df[out_col] = pd.cut(df[column], bins=n_bins, labels=labels)
            elif method == "equal_freq":
                df[out_col] = pd.qcut(df[column], q=n_bins, labels=labels, duplicates="drop")
            elif method == "custom":
                if bin_edges is None:
                    raise ValueError("bin_edges must be provided when method='custom'")
                df[out_col] = pd.cut(df[column], bins=bin_edges, labels=labels)
            else:
                raise ValueError(f"Unknown method '{method}'. Use 'equal_width', 'equal_freq', or 'custom'.")

            if include_numeric_label:
                df[f"{column}_bin_num"] = pd.cut(df[column], bins=n_bins, labels=False)

            context.log.info(
                f"Binned '{column}' into '{out_col}' using method='{method}'. "
                f"Value counts: {df[out_col].value_counts().to_dict()}"
            )
            return df

        return Definitions(assets=[_asset])
