"""GeoBuffer.

Expand each geometry by a distance, producing a polygon "buffer." Useful
for trade-area / radius-coverage analysis.

Important: meaningful buffers in real-world units (meters / miles) require
projecting from a geographic CRS (EPSG:4326, lat/lon in degrees) to a
projected CRS (a UTM zone or Web Mercator). By default this component
projects to EPSG:3857 (Web Mercator) before buffering and projects back —
accurate enough for most analytics use; for survey-grade accuracy specify
the right UTM zone via `projected_crs`.
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


_METER_FACTORS = {
    "meters": 1.0, "m": 1.0,
    "kilometers": 1000.0, "km": 1000.0,
    "miles": 1609.344, "mi": 1609.344,
    "feet": 0.3048, "ft": 0.3048,
    "yards": 0.9144, "yd": 0.9144,
}



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


class GeoBufferComponent(Component, Model, Resolvable):
    """Expand each geometry by `distance` units, producing a polygon buffer."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream asset key providing a (Geo)DataFrame with a geometry column")
    distance: float = Field(description="Buffer distance, in `units`.")
    units: str = Field(
        default="meters",
        description="Unit for `distance`: meters / km / miles / feet / yards.",
    )
    geometry_column: Union[str, int] = Field(default="geometry", description="Input + output geometry column name.")
    projected_crs: str = Field(
        default="EPSG:3857",
        description=(
            "Projected CRS used for the buffer math. Default is Web Mercator "
            "(EPSG:3857) — accurate enough for analytics use. For survey-grade "
            "results, pick the appropriate UTM zone (e.g. EPSG:32610 for "
            "California / Oregon)."
        ),
    )
    resolution: int = Field(
        default=16,
        description="Number of segments per quarter-circle in the resulting buffer polygon.",
    )

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
        distance = self.distance
        units = self.units.lower()
        if units not in _METER_FACTORS:
            raise ValueError(
                f"Unknown unit {units!r}. Valid: {sorted(_METER_FACTORS)}."
            )
        distance_m = distance * _METER_FACTORS[units]
        geometry_column = self.geometry_column
        projected_crs = self.projected_crs
        resolution = self.resolution

        tags = dict(self.asset_tags or {})
        for k in (self.kinds or ["python", "spatial"]):
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            key=AssetKey.from_user_string(asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"Buffer each geometry by {distance} {units}.",
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
                    raise ValueError(
                        f"Input has no {geometry_column!r} column. Run "
                        "`points_from_latlon` (or equivalent) first."
                    )
                gdf = gpd.GeoDataFrame(
                    upstream, geometry=geometry_column, crs="EPSG:4326"
                )
            else:
                gdf = upstream.copy()
                if gdf.crs is None:
                    gdf = gdf.set_crs("EPSG:4326")

            original_crs = gdf.crs
            # Project → buffer → project back. Buffer in degrees is meaningless
            # for real-world distance units.
            projected = gdf.to_crs(projected_crs)
            projected["geometry"] = projected.geometry.buffer(
                distance_m, resolution=resolution
            )
            out = projected.to_crs(original_crs)
            context.add_output_metadata({
                "dagster/row_count": MetadataValue.int(len(out)),
                "buffer_distance_meters": MetadataValue.float(float(distance_m)),
                "projected_crs": MetadataValue.text(projected_crs),
            })
            return out

        return Definitions(assets=[_asset])
