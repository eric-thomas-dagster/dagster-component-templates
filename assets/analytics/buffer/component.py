"""BufferComponent.

Reads a WKT `geometry` column and replaces each geometry with its buffer at the given radius — e.g. a 5km circle around each store. Useful for service-area / catchment analyses paired with `spatial_join`.
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


class BufferComponent(Component, Model, Resolvable):
    """Buffer (expand) point or polygon geometries by a fixed radius."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    geometry_column: str = Field(default="geometry", description="WKT geometry column name.")
    radius_meters: float = Field(description="Buffer radius in meters. Geometries are reprojected to a metric CRS for the buffer.")
    metric_crs: str = Field(default="EPSG:3857", description="Metric projection used for the buffer (Web Mercator by default).")
    src_crs: str = Field(default="EPSG:4326", description="Source CRS of the input geometries.")

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
            description=self.description or "Buffer (expand) point or polygon geometries by a fixed radius.",
            tags=tags_dict,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            df = upstream
            import geopandas as gpd
            from shapely import wkt
            df = df.copy()
            df[_self.geometry_column] = df[_self.geometry_column].apply(wkt.loads)
            gdf = gpd.GeoDataFrame(df, geometry=_self.geometry_column, crs=_self.src_crs)
            buffered = gdf.to_crs(_self.metric_crs).buffer(_self.radius_meters).to_crs(_self.src_crs)
            df = pd.DataFrame(gdf.drop(columns=_self.geometry_column))
            df[_self.geometry_column] = buffered.apply(lambda g: g.wkt)
            out_df = df

            if include_preview and len(out_df) > 0:
                try:
                    _prev = out_df.sample(min(preview_rows, len(out_df))) if len(out_df) > preview_rows * 10 else out_df.head(preview_rows)
                    context.add_output_metadata({"preview": MetadataValue.md(_prev.to_markdown(index=False))})
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return out_df

        return Definitions(assets=[_asset])
