"""Spatial Join Component.

Spatially join a points DataFrame against a regions DataFrame using GeoPandas,
enriching each point with attributes from the region it falls within.
"""

import json
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
class SpatialJoinComponent(Component, Model, Resolvable):
    """Spatially join a points DataFrame against a regions DataFrame.

    Uses GeoPandas to perform a spatial join between a points asset (with
    lat/lng columns) and a regions asset (with a GeoJSON geometry column).
    Each point is enriched with the attributes of the region it falls within.

    Example:
        ```yaml
        type: dagster_component_templates.SpatialJoinComponent
        attributes:
          asset_name: events_joined_regions
          upstream_asset_key: events
          regions_asset_key: admin_regions
          lat_column: latitude
          lng_column: longitude
          geometry_column: geometry
          how: left
          group_name: geospatial
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Points asset key providing a DataFrame with lat/lng columns")
    regions_asset_key: str = Field(description="Regions asset key providing a DataFrame with a GeoJSON geometry column")
    lat_column: str = Field(default="latitude", description="Column name in the points DataFrame for latitude")
    lng_column: str = Field(default="longitude", description="Column name in the points DataFrame for longitude")
    geometry_column: str = Field(
        default="geometry",
        description="Column in the regions DataFrame containing GeoJSON geometry dicts or strings"
    )
    how: str = Field(
        default="left",
        description="Join type: 'left' (keep all points) or 'inner' (keep only matched points)"
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
        return "Spatially join a points DataFrame against a regions DataFrame using GeoPandas."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        regions_asset_key = self.regions_asset_key
        lat_column = self.lat_column
        lng_column = self.lng_column
        geometry_column = self.geometry_column
        how = self.how
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
            ins={
                "upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key)),
                "regions_df": AssetIn(key=AssetKey.from_user_string(regions_asset_key)),
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
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    upstream = upstream[upstream[partition_date_column].astype(str) == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
            upstream: pd.DataFrame,
            regions_df: pd.DataFrame,
        ) -> pd.DataFrame:
            try:
                import geopandas as gpd
                from shapely.geometry import Point, shape
            except ImportError:
                raise ImportError("geopandas and shapely are required: pip install geopandas shapely")

            context.log.info(
                f"Spatial join: {len(upstream)} points against {len(regions_df)} regions (how={how})"
            )

            gdf_points = gpd.GeoDataFrame(
                upstream,
                geometry=gpd.points_from_xy(upstream[lng_column], upstream[lat_column]),
                crs="EPSG:4326",
            )

            regions = regions_df.copy()
            regions["geometry"] = regions[geometry_column].apply(
                lambda g: shape(g) if isinstance(g, dict) else shape(json.loads(g))
            )
            gdf_regions = gpd.GeoDataFrame(regions, geometry="geometry", crs="EPSG:4326")

            result = gpd.sjoin(gdf_points, gdf_regions, how=how, predicate="within")
            result_df = pd.DataFrame(result.drop(columns=["geometry"]))

            context.log.info(f"Spatial join complete: {len(result_df)} rows in result")
            context.add_output_metadata({
                "input_points": MetadataValue.int(len(upstream)),
                "input_regions": MetadataValue.int(len(regions_df)),
                "output_rows": MetadataValue.int(len(result_df)),
                "join_type": MetadataValue.text(how),
            })
            return result_df

        return Definitions(assets=[_asset])
