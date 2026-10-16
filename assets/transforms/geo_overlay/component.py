"""GeoOverlay.

Combine two geometry DataFrames using a set-theoretic spatial operation
(intersection / union / difference / symmetric_difference / identity).

Both inputs must have a geometry column. The output is one row per
overlay-result feature, carrying attributes from both inputs (column names
suffixed `_1` / `_2` on collisions, like GeoPandas does by default).
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


_VALID_OPS = {"intersection", "union", "difference", "symmetric_difference", "identity"}



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class GeoOverlayComponent(Component, Model, Resolvable):
    """Set-theoretic overlay of two (Geo)DataFrames."""

    asset_name: str = Field(description="Output Dagster asset name")
    left_asset_key: str = Field(description="Upstream asset key — first geometry source.")
    right_asset_key: str = Field(description="Upstream asset key — second geometry source.")
    how: str = Field(
        default="intersection",
        description=(
            "Set operation: 'intersection' (only where both overlap), 'union' "
            "(everywhere either has coverage), 'difference' (left minus right), "
            "'symmetric_difference' (xor), 'identity' (left intersect right + "
            "remainder of left). Defaults to 'intersection'."
        ),
    )
    keep_geom_type: bool = Field(
        default=True,
        description=(
            "Only keep result features whose geometry type matches the left "
            "input. Avoids the GeoDataFrame mixed-type quirk where overlay can "
            "leave behind stray points / lines from polygon ops."
        ),
    )
    geometry_column: Union[str, int] = Field(default="geometry")

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('asset_tags',))

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        left_asset_key = self.left_asset_key
        right_asset_key = self.right_asset_key
        how = self.how.lower()
        if how not in _VALID_OPS:
            raise ValueError(f"Unknown overlay op {how!r}. Valid: {sorted(_VALID_OPS)}.")
        keep_geom_type = self.keep_geom_type
        geometry_column = self.geometry_column

        tags = dict(self.asset_tags or {})
        for k in (self.kinds or ["python", "spatial"]):
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            key=AssetKey.from_user_string(asset_name),
            ins={
                "left": AssetIn(key=AssetKey.from_user_string(left_asset_key)),
                "right": AssetIn(key=AssetKey.from_user_string(right_asset_key)),
            },
            group_name=self.group_name,
            description=self.description or f"Spatial overlay ({how}) of {left_asset_key} and {right_asset_key}.",
            tags=tags,
            owners=self.owners or [],
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, left: Any, right: pd.DataFrame) -> pd.DataFrame:
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(left, dict):
                _frames = [v for v in left.values() if isinstance(v, pd.DataFrame)]
                left = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            try:
                import geopandas as gpd
            except ImportError as e:
                raise ImportError("geopandas required: pip install geopandas") from e

            def _to_gdf(df):
                if isinstance(df, gpd.GeoDataFrame):
                    return df.copy()
                if geometry_column not in df.columns:
                    raise ValueError(f"Input has no {geometry_column!r} column.")
                return gpd.GeoDataFrame(df, geometry=geometry_column)

            left_gdf = _to_gdf(left)
            right_gdf = _to_gdf(right)
            # Match CRS — overlay raises when CRSs differ.
            if left_gdf.crs is None and right_gdf.crs is not None:
                left_gdf = left_gdf.set_crs(right_gdf.crs)
            elif right_gdf.crs is None and left_gdf.crs is not None:
                right_gdf = right_gdf.set_crs(left_gdf.crs)
            elif left_gdf.crs is not None and right_gdf.crs != left_gdf.crs:
                right_gdf = right_gdf.to_crs(left_gdf.crs)

            result = gpd.overlay(left_gdf, right_gdf, how=how, keep_geom_type=keep_geom_type)
            context.add_output_metadata({
                "dagster/row_count": MetadataValue.int(len(result)),
                "overlay_op": MetadataValue.text(how),
            })
            return result

        return Definitions(assets=[_asset])
