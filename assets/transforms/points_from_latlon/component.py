"""PointsFromLatLon.

Build Shapely Point geometries from a DataFrame's latitude / longitude
columns and add them as a `geometry` column. Drop-in for Alteryx's
**Create Points** tool. Output is a GeoDataFrame-compatible pandas
DataFrame (downstream geo components accept either).
"""
from typing import Dict, List, Optional

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


class PointsFromLatLonComponent(Component, Model, Resolvable):
    """Build Shapely Point geometries from lat/lon columns."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    longitude_column: str = Field(description="Column with longitude values (X / east-west).")
    latitude_column: str = Field(description="Column with latitude values (Y / north-south).")
    geometry_column: str = Field(
        default="geometry",
        description="Output column name for the Shapely Point geometries.",
    )
    crs: str = Field(
        default="EPSG:4326",
        description=(
            "Coordinate reference system the lat/lon values are in. Defaults "
            "to WGS84 (EPSG:4326) — what virtually every GPS / web-map source uses."
        ),
    )
    drop_invalid: bool = Field(
        default=True,
        description=(
            "Drop rows where lat or lon is NaN / non-numeric. Otherwise rows "
            "with bad coords get a `None` geometry."
        ),
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        longitude_column = self.longitude_column
        latitude_column = self.latitude_column
        geometry_column = self.geometry_column
        crs = self.crs
        drop_invalid = self.drop_invalid

        tags = dict(self.asset_tags or {})
        for k in (self.kinds or ["python", "spatial"]):
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or "Build Shapely Point geometries from lat/lon columns.",
            tags=tags,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            try:
                import geopandas as gpd
            except ImportError as e:
                raise ImportError(
                    "geopandas is required for points_from_latlon: pip install geopandas"
                ) from e

            df = upstream.copy()
            if drop_invalid:
                df = df.dropna(subset=[longitude_column, latitude_column])
                # also drop rows where coords can't coerce to float
                df = df[
                    pd.to_numeric(df[longitude_column], errors="coerce").notna()
                    & pd.to_numeric(df[latitude_column], errors="coerce").notna()
                ]
            lon = pd.to_numeric(df[longitude_column], errors="coerce")
            lat = pd.to_numeric(df[latitude_column], errors="coerce")
            gdf = gpd.GeoDataFrame(
                df,
                geometry=gpd.points_from_xy(lon, lat, crs=crs),
            )
            if geometry_column != "geometry":
                gdf = gdf.rename_geometry(geometry_column)
            context.add_output_metadata({
                "dagster/row_count": MetadataValue.int(len(gdf)),
                "crs": MetadataValue.text(crs),
                "geometry_column": MetadataValue.text(geometry_column),
            })
            return gdf

        return Definitions(assets=[_asset])
