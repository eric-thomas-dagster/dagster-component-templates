"""CreatePointsComponent.

Takes a DataFrame with lat/lng columns and returns a GeoDataFrame with a `geometry` column of shapely Points (in EPSG:4326 by default). The starting block of any spatial pipeline — once you have geometry, buffer / spatial_join / make_grid all work.
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


class CreatePointsComponent(Component, Model, Resolvable):
    """Build a `geometry` column of shapely Points from lat/lng columns."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    lat_column: str = Field(default="lat", description="Latitude column name.")
    lng_column: str = Field(default="lng", description="Longitude column name.")
    crs: str = Field(default="EPSG:4326", description="Coordinate reference system.")
    output_column: str = Field(default="geometry", description="Output geometry column name.")
    drop_invalid: bool = Field(default=True, description="Drop rows where lat/lng is NaN/invalid.")

    include_preview_metadata: bool = Field(
        default=False,
        description="Include a preview of the output DataFrame in metadata (for builder UIs).",
    )
    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description="Rows in the preview when include_preview_metadata=True.",
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds.")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        group_name = self.group_name
        _self = self

        ins = {"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))}
        tags_dict = dict(self.asset_tags or {})
        for k in (self.kinds or ["python"]):
            tags_dict[f"dagster/kind/{k}"] = ""

        @asset(
            name=asset_name,
            ins=ins,
            group_name=group_name,
            description=self.description or "Build a `geometry` column of shapely Points from lat/lng columns.",
            tags=tags_dict,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            df = upstream
            import geopandas as gpd
            from shapely.geometry import Point
            df = df.copy()
            if _self.drop_invalid:
                df = df.dropna(subset=[_self.lat_column, _self.lng_column]).copy()
            df[_self.output_column] = [Point(lng, lat) for lat, lng in zip(df[_self.lat_column], df[_self.lng_column])]
            gdf = gpd.GeoDataFrame(df, geometry=_self.output_column, crs=_self.crs)
            # Convert back to a DataFrame with WKT for downstream pandas-only consumers.
            df = pd.DataFrame(gdf.drop(columns=_self.output_column))
            df[_self.output_column] = gdf[_self.output_column].apply(lambda g: g.wkt)
            out_df = df

            if include_preview and len(out_df) > 0:
                try:
                    _prev = out_df.sample(min(preview_rows, len(out_df))) if len(out_df) > preview_rows * 10 else out_df.head(preview_rows)
                    context.add_output_metadata({"preview": MetadataValue.md(_prev.to_markdown(index=False))})
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return out_df

        return Definitions(assets=[_asset])
