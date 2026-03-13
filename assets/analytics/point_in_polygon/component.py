"""Point-in-Polygon Component.

Test whether geographic points fall inside GeoJSON polygons and annotate each
row with the matching polygon's name (or null if no match).
"""

import json
import os
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
class PointInPolygonComponent(Component, Model, Resolvable):
    """Annotate each row with the GeoJSON polygon region it falls inside.

    Loads GeoJSON polygons from a local file or URL, then tests each lat/lng
    row against all polygons and writes the matching polygon name to an output
    column. Optionally adds a boolean column indicating whether the point is
    inside any polygon.

    Example:
        ```yaml
        type: dagster_component_templates.PointInPolygonComponent
        attributes:
          asset_name: events_with_region
          upstream_asset_key: events
          lat_column: latitude
          lng_column: longitude
          geojson_path: /data/regions/us_states.geojson
          polygon_name_field: NAME
          output_column: state
          output_inside_column: in_us
          group_name: geospatial
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame with lat/lng columns")
    lat_column: str = Field(default="latitude", description="Column name containing latitude values")
    lng_column: str = Field(default="longitude", description="Column name containing longitude values")
    geojson_path: Optional[str] = Field(default=None, description="Path to a local GeoJSON file with polygon features")
    geojson_url: Optional[str] = Field(default=None, description="URL to a GeoJSON file (alternative to geojson_path)")
    polygon_name_field: Optional[str] = Field(
        default=None,
        description="GeoJSON feature property key to use as the polygon name (e.g. 'NAME')"
    )
    output_column: str = Field(default="region", description="Column name to add with the matched polygon name")
    output_inside_column: Optional[str] = Field(
        default=None,
        description="Optional boolean column name: True if the point is inside any polygon"
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
        return "Annotate each row with the GeoJSON polygon region it falls inside."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        lat_column = self.lat_column
        lng_column = self.lng_column
        geojson_path = self.geojson_path
        geojson_url = self.geojson_url
        polygon_name_field = self.polygon_name_field
        output_column = self.output_column
        output_inside_column = self.output_inside_column
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
                from shapely.geometry import Point, shape
            except ImportError:
                raise ImportError("shapely is required: pip install shapely")

            if not geojson_path and not geojson_url:
                raise ValueError("One of geojson_path or geojson_url must be provided")

            if geojson_path:
                with open(os.path.expandvars(geojson_path)) as f:
                    geojson = json.load(f)
                context.log.info(f"Loaded GeoJSON from file: {geojson_path}")
            else:
                import urllib.request
                with urllib.request.urlopen(geojson_url) as r:
                    geojson = json.load(r)
                context.log.info(f"Loaded GeoJSON from URL: {geojson_url}")

            polygons = []
            for i, feature in enumerate(geojson.get("features", [])):
                poly = shape(feature["geometry"])
                if polygon_name_field:
                    name = feature.get("properties", {}).get(polygon_name_field, str(i))
                else:
                    name = str(i)
                polygons.append((poly, name))

            context.log.info(f"Loaded {len(polygons)} polygons from GeoJSON")

            def find_region(row):
                pt = Point(row[lng_column], row[lat_column])
                for poly, name in polygons:
                    if poly.contains(pt):
                        return name
                return None

            df = upstream.copy()
            df[output_column] = df.apply(find_region, axis=1)
            if output_inside_column:
                df[output_inside_column] = df[output_column].notna()

            matched = df[output_column].notna().sum()
            context.log.info(f"Point-in-polygon: {matched}/{len(df)} points matched a polygon")
            context.add_output_metadata({
                "rows": MetadataValue.int(len(df)),
                "matched_count": MetadataValue.int(int(matched)),
                "polygon_count": MetadataValue.int(len(polygons)),
                "match_rate": MetadataValue.float(round(matched / len(df) * 100, 2) if len(df) > 0 else 0.0),
            })
            return df

        return Definitions(assets=[_asset])
