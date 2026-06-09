"""GeoSimplify.

Reduce the vertex count of each geometry using Shapely's
Douglas-Peucker simplification.

Use case: lighten complex polygons for plotting, shrink tile sizes,
or speed up downstream spatial joins. Higher `tolerance` removes more
vertices (smoother shape, less fidelity to the original).
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


class GeoSimplifyComponent(Component, Model, Resolvable):
    """Apply Douglas-Peucker simplification to a geometry column."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a (Geo)DataFrame with a geometry column")
    tolerance: float = Field(
        description=(
            "Simplification tolerance, in the CRS's units. For EPSG:4326 "
            "(lat/lon degrees), 0.001 ≈ 100 meters at the equator. For "
            "EPSG:3857 (Web Mercator meters), use the desired vertex-spacing "
            "in meters directly."
        ),
    )
    preserve_topology: bool = Field(
        default=True,
        description=(
            "When true (default), the simplified geometry is guaranteed to "
            "stay valid (no self-intersections). When false, simplification "
            "is faster but can produce invalid geometries."
        ),
    )
    geometry_column: str = Field(default="geometry")

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        tolerance = self.tolerance
        preserve_topology = self.preserve_topology
        geometry_column = self.geometry_column

        tags = dict(self.asset_tags or {})
        for k in (self.kinds or ["python", "spatial"]):
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            key=AssetKey.from_user_string(asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"Simplify geometries (tolerance={tolerance}).",
            tags=tags,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            try:
                import geopandas as gpd
            except ImportError as e:
                raise ImportError("geopandas required: pip install geopandas") from e

            if not isinstance(upstream, gpd.GeoDataFrame):
                if geometry_column not in upstream.columns:
                    raise ValueError(f"Input has no {geometry_column!r} column.")
                gdf = gpd.GeoDataFrame(upstream, geometry=geometry_column)
            else:
                gdf = upstream.copy()

            # Count vertices before / after for metadata.
            try:
                vc_before = int(gdf.geometry.apply(lambda g: 0 if g is None else len(g.coords) if hasattr(g, "coords") else sum(len(p.coords) for p in getattr(g, "geoms", []))).sum())
            except Exception:
                vc_before = -1

            gdf["geometry"] = gdf.geometry.simplify(
                tolerance, preserve_topology=preserve_topology
            )

            try:
                vc_after = int(gdf.geometry.apply(lambda g: 0 if g is None else len(g.coords) if hasattr(g, "coords") else sum(len(p.coords) for p in getattr(g, "geoms", []))).sum())
            except Exception:
                vc_after = -1

            md = {
                "dagster/row_count": MetadataValue.int(len(gdf)),
                "tolerance": MetadataValue.float(float(tolerance)),
            }
            if vc_before > 0 and vc_after > 0:
                md["vertices_before"] = MetadataValue.int(vc_before)
                md["vertices_after"] = MetadataValue.int(vc_after)
                md["reduction_pct"] = MetadataValue.float(
                    100.0 * (1 - vc_after / vc_before)
                )
            context.add_output_metadata(md)
            return gdf

        return Definitions(assets=[_asset])
