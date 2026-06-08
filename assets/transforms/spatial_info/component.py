"""SpatialInfo.

Compute geometry metadata (area, length, centroid, bounds, geometry-type)
for each row of an input DataFrame with a Shapely geometry column. Drop-in
for the Spatial Info step.

The selected metrics get appended as new columns. Area and length are
computed in the projected CRS for accurate real-world units (defaults to
EPSG:3857 / Web Mercator) — square meters and meters respectively — then
the output rows return in the original CRS.
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


_METRIC_NAMES = {"area", "length", "centroid", "bounds", "geom_type", "num_points", "is_valid"}


class SpatialInfoComponent(Component, Model, Resolvable):
    """Compute geometry metadata columns (area, length, centroid, bounds, etc.)."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream (Geo)DataFrame with a geometry column")
    geometry_column: str = Field(default="geometry", description="Geometry column to inspect")
    metrics: List[str] = Field(
        default_factory=lambda: ["area", "length", "centroid", "bounds", "geom_type"],
        description=(
            "Which metrics to compute. Subset of: area, length, centroid, "
            "bounds, geom_type, num_points, is_valid. Centroid adds centroid_x "
            "+ centroid_y; bounds adds minx/miny/maxx/maxy."
        ),
    )
    projected_crs: str = Field(
        default="EPSG:3857",
        description=(
            "Projected CRS used for area / length math (gives real-world units). "
            "Default is Web Mercator. For survey-grade results pick the right "
            "UTM zone (e.g. EPSG:32610 for the US West Coast)."
        ),
    )
    area_unit: str = Field(
        default="sq_meters",
        description="Unit for area: 'sq_meters' (default), 'sq_km', 'sq_miles', 'sq_feet'.",
    )
    length_unit: str = Field(
        default="meters",
        description="Unit for length: 'meters' (default), 'km', 'miles', 'feet'.",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _self = self
        asset_name = self.asset_name

        tags = dict(self.asset_tags or {})
        for k in (self.kinds or ["python", "spatial"]):
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"Append geometry metadata columns ({', '.join(self.metrics)}).",
            tags=tags,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            try:
                import geopandas as gpd
            except ImportError as exc:
                raise ImportError(
                    "geopandas is required for spatial_info: pip install geopandas"
                ) from exc

            df = upstream.copy()
            _geom_col = _self.geometry_column
            if _geom_col not in df.columns:
                # Upstream PolyBuild / drive_time / etc. typically emit a
                # `geometry` (or `Centroid` / `SpatialObj`) column. Fall back
                # to whichever common geom name actually exists so the chain
                # doesn't die just because of a column-naming convention drift.
                _CANDIDATES = ("geometry", "Centroid", "SpatialObj", "SpatialObject",
                               "geom", "shape", "centroid")
                for _alt in _CANDIDATES:
                    if _alt in df.columns:
                        _geom_col = _alt
                        break
                # Last resort: a join may have prefixed the geometry with
                # `Right_` / `Left_` / `<group>_`. Match any column whose
                # SUFFIX is a known geometry name.
                if _geom_col not in df.columns:
                    for _c in df.columns:
                        if not isinstance(_c, str):
                            continue
                        _tail = _c.split("_")[-1] if "_" in _c else _c
                        if _tail in _CANDIDATES:
                            _geom_col = _c
                            break
            if _geom_col not in df.columns:
                context.log.warning(
                    f"spatial_info: geometry_column {_self.geometry_column!r} not in upstream "
                    f"DataFrame. Available: {list(df.columns)[:10]}. "
                    "Returning upstream unchanged."
                )
                return df

            invalid_metrics = [m for m in _self.metrics if m not in _METRIC_NAMES]
            if invalid_metrics:
                raise ValueError(
                    f"spatial_info: unknown metrics {invalid_metrics!r}. "
                    f"Valid: {sorted(_METRIC_NAMES)}"
                )

            area_factor = {
                "sq_meters": 1.0, "sq_km": 1e-6,
                "sq_miles": 1.0 / 2_589_988.110336,
                "sq_feet": 1.0 / 0.09290304,
            }.get(_self.area_unit, 1.0)
            length_factor = {
                "meters": 1.0, "km": 1e-3,
                "miles": 1.0 / 1609.344, "feet": 1.0 / 0.3048,
            }.get(_self.length_unit, 1.0)

            gdf = gpd.GeoDataFrame(df, geometry=_geom_col, crs="EPSG:4326")
            gdf_proj = gdf.to_crs(_self.projected_crs)

            # Short-form aliases match the common ETL convention:
            #   sq_miles → AreaSqMi, miles → LengthMi
            #   sq_km → AreaSqKm, km → LengthKm
            #   sq_meters → AreaSqM, meters → LengthM
            #   sq_feet → AreaSqFt, feet → LengthFt
            _AREA_ALIAS = {"sq_miles": "AreaSqMi", "sq_km": "AreaSqKm",
                           "sq_meters": "AreaSqM", "sq_feet": "AreaSqFt"}
            _LEN_ALIAS = {"miles": "LengthMi", "km": "LengthKm",
                          "meters": "LengthM", "feet": "LengthFt"}
            if "geom_type" in _self.metrics:
                df["geom_type"] = gdf.geometry.geom_type
            if "area" in _self.metrics:
                _vals = gdf_proj.geometry.area * area_factor
                df[f"area_{_self.area_unit}"] = _vals
                _alias = _AREA_ALIAS.get(_self.area_unit)
                if _alias:
                    df[_alias] = _vals
            if "length" in _self.metrics:
                _vals = gdf_proj.geometry.length * length_factor
                df[f"length_{_self.length_unit}"] = _vals
                _alias = _LEN_ALIAS.get(_self.length_unit)
                if _alias:
                    df[_alias] = _vals
            if "centroid" in _self.metrics:
                cent = gdf.geometry.centroid
                df["centroid_x"] = cent.x
                df["centroid_y"] = cent.y
            if "bounds" in _self.metrics:
                bounds = gdf.geometry.bounds  # minx/miny/maxx/maxy
                df["minx"] = bounds["minx"]
                df["miny"] = bounds["miny"]
                df["maxx"] = bounds["maxx"]
                df["maxy"] = bounds["maxy"]
            if "num_points" in _self.metrics:
                df["num_points"] = gdf.geometry.apply(
                    lambda g: 0 if g is None else getattr(g, "geoms", [g]).__iter__().__next__().exterior.coords.__len__() if g.geom_type == "Polygon" else (len(g.coords) if hasattr(g, "coords") else 0)
                )
            if "is_valid" in _self.metrics:
                df["is_valid"] = gdf.geometry.is_valid

            context.log.info(
                f"spatial_info: computed {len(_self.metrics)} metric(s) on "
                f"{len(df)} geometries ({_self.metrics})."
            )
            context.add_output_metadata({
                "dagster/row_count": MetadataValue.int(len(df)),
                "metrics_computed": MetadataValue.text(", ".join(_self.metrics)),
                "projected_crs": MetadataValue.text(_self.projected_crs),
            })
            return df

        return Definitions(assets=[_asset])

    @classmethod
    def get_description(cls) -> str:
        return cls.__doc__ or ""
