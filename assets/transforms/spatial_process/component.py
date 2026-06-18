"""SpatialProcess.

Apply a single geometry operation to a column of Shapely geometries.
Covers the common grab-bag of conversions / extractions:

  - centroid           polygon → point
  - boundary           polygon → linestring (its outer ring)
  - polygon_to_lines   polygon → multilinestring (boundary)
  - polygon_to_points  polygon → multipoint (its exterior vertices)
  - line_to_polygon    closed polyline → polygon
  - convex_hull        any geom → its convex hull
  - envelope           any geom → its axis-aligned bounding box
  - simplify           any geom → simplified (Douglas-Peucker; needs tolerance)
  - buffer             any geom → buffered polygon (needs distance)
  - set_precision      round coordinates to N decimal places (compression)

The output geometry replaces the input column (in place) by default, or
lands in `output_column` when set. Accepts both Shapely objects and
WKT / GeoJSON strings as inputs.
"""
from typing import Any, Dict, List, Optional, Union

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


_VALID_METHODS = (
    "centroid",
    "boundary",
    "polygon_to_lines",
    "polygon_to_points",
    "line_to_polygon",
    "convex_hull",
    "envelope",
    "simplify",
    "buffer",
    "set_precision",
)


class SpatialProcessComponent(Component, Model, Resolvable):
    """Apply a single geometry op (centroid, boundary, convex_hull, buffer, …) to each row."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame with a geometry column")
    method: str = Field(
        default="centroid",
        description=f"Operation to apply per row. One of: {sorted(_VALID_METHODS)}.",
    )
    geometry_column: Union[str, int] = Field(
        default="geometry",
        description="Input geometry column. Accepts Shapely objects or WKT/GeoJSON strings.",
    )
    output_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Output column name. Defaults to overwriting the input geometry column in place.",
    )
    tolerance: float = Field(
        default=0.0001,
        description="Douglas-Peucker tolerance for `method=simplify`.",
    )
    buffer_distance: float = Field(
        default=0.0,
        description="Buffer distance for `method=buffer` (CRS units).",
    )
    precision_decimals: int = Field(
        default=6,
        description="Decimal places to keep for `method=set_precision`.",
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
        if self.method not in _VALID_METHODS:
            raise ValueError(
                f"spatial_process: method must be one of {sorted(_VALID_METHODS)}; "
                f"got {self.method!r}."
            )

        tags = dict(self.asset_tags or {})
        for k in (self.kinds or ["python", "spatial"]):
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            key=AssetKey.from_user_string(asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"Spatial Process: {self.method} on column {self.geometry_column!r}.",
            tags=tags,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: Any) -> pd.DataFrame:
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            try:
                from shapely.geometry.base import BaseGeometry
                from shapely.geometry import shape
                from shapely import wkt as _wkt
                from shapely.ops import transform as _transform
            except ImportError as e:
                raise ImportError("shapely is required for spatial_process: pip install shapely") from e

            import json

            df = upstream.copy()
            geom_col = _self.geometry_column
            out_col = _self.output_column or geom_col
            if geom_col not in df.columns:
                context.log.warning(
                    f"spatial_process: geometry_column {geom_col!r} not in upstream "
                    f"DataFrame. Available: {list(df.columns)[:10]}. Returning upstream unchanged."
                )
                return df

            def _to_geom(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                if isinstance(v, BaseGeometry):
                    return v
                if isinstance(v, dict):
                    return shape(v)
                s = str(v).strip()
                if not s or s.lower() == "nan":
                    return None
                try:
                    if s.startswith("{"):
                        return shape(json.loads(s))
                    return _wkt.loads(s)
                except Exception:
                    return None

            method = _self.method

            def _apply(g):
                g = _to_geom(g)
                if g is None or g.is_empty:
                    return None
                if method == "centroid":
                    return g.centroid
                if method == "boundary":
                    return g.boundary
                if method == "polygon_to_lines":
                    return g.boundary
                if method == "polygon_to_points":
                    from shapely.geometry import MultiPoint
                    if hasattr(g, "exterior") and g.exterior is not None:
                        return MultiPoint(list(g.exterior.coords))
                    if hasattr(g, "coords"):
                        return MultiPoint(list(g.coords))
                    return MultiPoint([g.centroid])
                if method == "line_to_polygon":
                    from shapely.geometry import Polygon as _Poly
                    if hasattr(g, "coords"):
                        coords = list(g.coords)
                        if len(coords) >= 3:
                            if coords[0] != coords[-1]:
                                coords = coords + [coords[0]]
                            return _Poly(coords)
                    return g
                if method == "convex_hull":
                    return g.convex_hull
                if method == "envelope":
                    return g.envelope
                if method == "simplify":
                    return g.simplify(_self.tolerance, preserve_topology=True)
                if method == "buffer":
                    return g.buffer(_self.buffer_distance)
                if method == "set_precision":
                    n = _self.precision_decimals
                    def _round(x, y, z=None):
                        return (round(x, n), round(y, n)) if z is None else (round(x, n), round(y, n), round(z, n))
                    return _transform(_round, g)
                return g

            df[out_col] = df[geom_col].apply(_apply)
            context.log.info(
                f"spatial_process: applied {method!r} on column {geom_col!r}; "
                f"{df[out_col].notna().sum()} non-null outputs out of {len(df)} rows."
            )
            context.add_output_metadata({
                "dagster/row_count": MetadataValue.int(len(df)),
                "method": MetadataValue.text(method),
                "geometry_column": MetadataValue.text(geom_col),
                "output_column": MetadataValue.text(out_col),
            })
            return df

        return Definitions(assets=[_asset])

    @classmethod
    def get_description(cls) -> str:
        return cls.__doc__ or ""
