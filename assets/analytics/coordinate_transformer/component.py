"""Coordinate Transformer Component.

Reproject coordinate columns in a DataFrame from one CRS (coordinate reference
system) to another using pyproj. Supports any EPSG or PROJ string.
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
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


@dataclass
class CoordinateTransformerComponent(Component, Model, Resolvable):
    """Reproject coordinate columns from one CRS to another using pyproj.

    Transforms X/Y (or lng/lat) columns from a source coordinate reference
    system to a target CRS and writes the results to new output columns.
    Supports any EPSG code or PROJ string.

    Example:
        ```yaml
        type: dagster_component_templates.CoordinateTransformerComponent
        attributes:
          asset_name: events_web_mercator
          upstream_asset_key: events
          x_column: longitude
          y_column: latitude
          source_crs: EPSG:4326
          target_crs: EPSG:3857
          output_x_column: x_mercator
          output_y_column: y_mercator
          group_name: geospatial
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame with coordinate columns")
    x_column: str = Field(description="Input column name for X values (longitude in geographic CRS)")
    y_column: str = Field(description="Input column name for Y values (latitude in geographic CRS)")
    source_crs: str = Field(default="EPSG:4326", description="Source CRS as EPSG code or PROJ string (e.g. EPSG:4326)")
    target_crs: str = Field(
        default="EPSG:3857",
        description="Target CRS as EPSG code or PROJ string (e.g. EPSG:3857 for Web Mercator)"
    )
    output_x_column: str = Field(default="x_transformed", description="Output column name for transformed X values")
    output_y_column: str = Field(default="y_transformed", description="Output column name for transformed Y values")
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
        return "Reproject coordinate columns from one CRS to another using pyproj."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        x_column = self.x_column
        y_column = self.y_column
        source_crs = self.source_crs
        target_crs = self.target_crs
        output_x_column = self.output_x_column
        output_y_column = self.output_y_column
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
            try:
                from pyproj import Transformer
            except ImportError:
                raise ImportError("pyproj is required: pip install pyproj")

            transformer = Transformer.from_crs(source_crs, target_crs, always_xy=True)
            df = upstream.copy()
            x_vals, y_vals = transformer.transform(df[x_column].values, df[y_column].values)
            df[output_x_column] = x_vals
            df[output_y_column] = y_vals

            context.log.info(
                f"Transformed {len(df)} coordinates from {source_crs} to {target_crs}"
            )
            context.add_output_metadata({
                "rows": MetadataValue.int(len(df)),
                "source_crs": MetadataValue.text(source_crs),
                "target_crs": MetadataValue.text(target_crs),
                "output_x_column": MetadataValue.text(output_x_column),
                "output_y_column": MetadataValue.text(output_y_column),
            })
            return df

        return Definitions(assets=[_asset])
