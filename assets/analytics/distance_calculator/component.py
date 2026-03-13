"""Distance Calculator Component.

Calculate the geographic distance between two lat/lng coordinate pairs per row
using the Haversine formula (fast) or Vincenty/geodesic formula (precise).
"""

import math
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
class DistanceCalculatorComponent(Component, Model, Resolvable):
    """Calculate geographic distance between two coordinate pairs per row.

    Computes the distance between origin and destination lat/lng columns using
    either the Haversine formula (fast approximation) or geodesic/Vincenty
    (precise). Supports km, miles, and meters output.

    Example:
        ```yaml
        type: dagster_component_templates.DistanceCalculatorComponent
        attributes:
          asset_name: deliveries_with_distance
          upstream_asset_key: deliveries
          lat1_column: pickup_lat
          lng1_column: pickup_lng
          lat2_column: dropoff_lat
          lng2_column: dropoff_lng
          output_column: distance_km
          unit: km
          formula: haversine
          group_name: geospatial
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    lat1_column: str = Field(description="Column name containing the origin latitude")
    lng1_column: str = Field(description="Column name containing the origin longitude")
    lat2_column: str = Field(description="Column name containing the destination latitude")
    lng2_column: str = Field(description="Column name containing the destination longitude")
    output_column: str = Field(default="distance_km", description="Column name for the computed distance")
    unit: str = Field(default="km", description="Distance unit: 'km', 'miles', or 'meters'")
    formula: str = Field(
        default="haversine",
        description="Distance formula: 'haversine' (fast, less accurate) or 'vincenty' (precise geodesic)"
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
        return "Calculate geographic distance between two coordinate pairs per row."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        lat1_column = self.lat1_column
        lng1_column = self.lng1_column
        lat2_column = self.lat2_column
        lng2_column = self.lng2_column
        output_column = self.output_column
        unit = self.unit
        formula = self.formula
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
            def haversine(lat1, lon1, lat2, lon2):
                R = 6371.0  # Earth radius in km
                lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
                dlat = lat2 - lat1
                dlon = lon2 - lon1
                a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
                return R * 2 * math.asin(math.sqrt(a))

            df = upstream.copy()

            if formula == "vincenty":
                try:
                    from geopy.distance import geodesic
                except ImportError:
                    raise ImportError("geopy is required for vincenty formula: pip install geopy")
                distances = df.apply(
                    lambda r: geodesic(
                        (r[lat1_column], r[lng1_column]),
                        (r[lat2_column], r[lng2_column])
                    ).km,
                    axis=1,
                )
            else:
                distances = df.apply(
                    lambda r: haversine(r[lat1_column], r[lng1_column], r[lat2_column], r[lng2_column]),
                    axis=1,
                )

            if unit == "miles":
                distances = distances * 0.621371
            elif unit == "meters":
                distances = distances * 1000

            df[output_column] = distances

            context.log.info(f"Computed {formula} distances for {len(df)} rows (unit: {unit})")
            context.add_output_metadata({
                "rows": MetadataValue.int(len(df)),
                "formula": MetadataValue.text(formula),
                "unit": MetadataValue.text(unit),
                "mean_distance": MetadataValue.float(round(float(df[output_column].mean()), 4)),
                "max_distance": MetadataValue.float(round(float(df[output_column].max()), 4)),
            })
            return df

        return Definitions(assets=[_asset])
