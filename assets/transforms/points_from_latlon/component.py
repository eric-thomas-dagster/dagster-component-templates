"""PointsFromLatLon.

Build Shapely Point geometries from a DataFrame's latitude / longitude
columns and add them as a `geometry` column. Output is a
GeoDataFrame-compatible pandas DataFrame (downstream geo components accept either).
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


class PointsFromLatLonComponent(Component, Model, Resolvable):
    """Build Shapely Point geometries from lat/lon columns."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream asset key providing a DataFrame")
    longitude_column: Union[str, int] = Field(description="Column with longitude values (X / east-west).")
    latitude_column: Union[str, int] = Field(description="Column with latitude values (Y / north-south).")
    geometry_column: Union[str, int] = Field(
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

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

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
            key=AssetKey.from_user_string(asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or "Build Shapely Point geometries from lat/lon columns.",
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
                raise ImportError(
                    "geopandas is required for points_from_latlon: pip install geopandas"
                ) from e

            df = upstream.copy()
            if longitude_column not in df.columns or latitude_column not in df.columns:
                context.log.warning(
                    f"points_from_latlon: lat/lng columns ({latitude_column!r}/{longitude_column!r}) "
                    f"not in upstream (have {list(df.columns)[:10]}). Returning upstream unchanged."
                )
                return df
            if drop_invalid:
                df = df.dropna(subset=[longitude_column, latitude_column])
                # also drop rows where coords can't coerce to float
                df = df[
                    pd.to_numeric(df[longitude_column], errors="coerce").notna()
                    & pd.to_numeric(df[latitude_column], errors="coerce").notna()
                ]
            # Drop pre-existing geometry column to avoid "already exists" crash.
            if geometry_column in df.columns:
                df = df.drop(columns=[geometry_column])
            if "geometry" in df.columns and geometry_column != "geometry":
                df = df.drop(columns=["geometry"])
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
