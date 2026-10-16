"""GeoSimplify.

Reduce the vertex count of each geometry using Shapely's
Douglas-Peucker simplification.

Use case: lighten complex polygons for plotting, shrink tile sizes,
or speed up downstream spatial joins. Higher `tolerance` removes more
vertices (smoother shape, less fidelity to the original).
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



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization


class GeoSimplifyComponent(Component, Model, Resolvable):
    """Apply Douglas-Peucker simplification to a geometry column."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream asset key providing a (Geo)DataFrame with a geometry column")
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
    geometry_column: Union[str, int] = Field(default="geometry")

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

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
        def _asset(context: AssetExecutionContext, upstream: Any) -> pd.DataFrame:
            # Defensive Output/MaterializeResult unwrap — see summarize for the rationale.
            # Tolerates upstream authors who annotate `-> Output` or
            # return `Output(value=df, ...)` / `MaterializeResult(value=df)`.
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
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
