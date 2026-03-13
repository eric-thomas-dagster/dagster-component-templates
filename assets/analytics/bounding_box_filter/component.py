"""Bounding Box Filter Component.

Filter a DataFrame of geographic points to keep only those inside (or outside)
a rectangular bounding box defined by min/max latitude and longitude bounds.
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
class BoundingBoxFilterComponent(Component, Model, Resolvable):
    """Filter geographic points to those inside (or outside) a bounding box.

    Filters a DataFrame to keep rows whose lat/lng coordinates fall within
    the rectangular bounding box defined by min/max lat and lng values. Set
    `keep_outside=true` to invert the filter and keep points outside the box.

    Example:
        ```yaml
        type: dagster_component_templates.BoundingBoxFilterComponent
        attributes:
          asset_name: us_events
          upstream_asset_key: all_events
          lat_column: latitude
          lng_column: longitude
          min_lat: 24.396308
          max_lat: 49.384358
          min_lng: -125.0
          max_lng: -66.93457
          keep_outside: false
          group_name: geospatial
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame with lat/lng columns")
    lat_column: str = Field(default="latitude", description="Column name containing latitude values")
    lng_column: str = Field(default="longitude", description="Column name containing longitude values")
    min_lat: float = Field(description="Southern boundary (minimum latitude)")
    max_lat: float = Field(description="Northern boundary (maximum latitude)")
    min_lng: float = Field(description="Western boundary (minimum longitude)")
    max_lng: float = Field(description="Eastern boundary (maximum longitude)")
    keep_outside: bool = Field(
        default=False,
        description="If True, keep points OUTSIDE the bounding box instead of inside"
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
        return "Filter geographic points to those inside (or outside) a bounding box."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        lat_column = self.lat_column
        lng_column = self.lng_column
        min_lat = self.min_lat
        max_lat = self.max_lat
        min_lng = self.min_lng
        max_lng = self.max_lng
        keep_outside = self.keep_outside
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
            df = upstream.copy()
            inside = (
                (df[lat_column] >= min_lat) & (df[lat_column] <= max_lat) &
                (df[lng_column] >= min_lng) & (df[lng_column] <= max_lng)
            )
            result = df[~inside if keep_outside else inside].reset_index(drop=True)

            context.log.info(
                f"BoundingBox {'excluded' if keep_outside else 'kept'} "
                f"{len(result)}/{len(df)} points "
                f"(lat [{min_lat}, {max_lat}], lng [{min_lng}, {max_lng}])"
            )
            context.add_output_metadata({
                "input_rows": MetadataValue.int(len(df)),
                "output_rows": MetadataValue.int(len(result)),
                "filtered_rows": MetadataValue.int(len(df) - len(result)),
                "keep_outside": MetadataValue.bool(keep_outside),
                "bounding_box": MetadataValue.text(
                    f"lat [{min_lat}, {max_lat}], lng [{min_lng}, {max_lng}]"
                ),
            })
            return result

        return Definitions(assets=[_asset])
