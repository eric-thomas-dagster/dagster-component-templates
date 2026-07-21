"""PolyBuild.

Construct polygon (or polyline) geometries from a points DataFrame by
grouping the rows into rings and ordering them by a sequence column.spatial tool.

The standard use is: an input table with one row per vertex, columns
identifying which polygon each vertex belongs to (`group_column`),
ordering within the ring (`sequence_column`), and the lat/lng of each
vertex. Output is one row per polygon with a Shapely `Polygon` (or
`LineString` when `output_type="line"`) in `geometry_column`.

Open polygons (start vertex != end vertex) auto-close. Polygons with
fewer than 3 distinct vertices fall back to `LineString` regardless of
the configured output_type.
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


class PolyBuildComponent(Component, Model, Resolvable):
    """Build polygon / polyline geometries from per-vertex rows."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key — DataFrame with one row per vertex")
    group_column: Union[str, int] = Field(description="Column identifying which polygon each vertex belongs to")
    sequence_column: Optional[Union[str, int]] = Field(
        default=None,
        description=(
            "Column ordering vertices within each polygon. If None, the "
            "upstream's natural row order within each group is used (matches "
            "Poly-build's behavior when SequenceField is left blank)."
        ),
    )
    # Two input modes — exactly ONE must be provided:
    #   1. latitude_column + longitude_column → build Points from coords
    #   2. geometry_column                    → use existing Shapely Points
    #      (matches Poly-build's <SpatialObj field=X/>)
    latitude_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column with vertex latitude values (lat/lng mode)",
    )
    longitude_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column with vertex longitude values (lat/lng mode)",
    )
    input_geometry_column: Optional[Union[str, int]] = Field(
        default=None,
        description=(
            "Column with existing Shapely Point geometries (geometry mode). "
            "Use this instead of latitude_column + longitude_column when your "
            "upstream already has point geometries (e.g. from a CreatePoints "
            "or PointsFromLatLon component upstream)."
        ),
    )
    output_type: str = Field(
        default="polygon",
        description="'polygon' (closed ring; auto-closes) or 'line' (open polyline)",
    )
    geometry_column: Union[str, int] = Field(
        default="geometry",
        description="Output column name for the built Shapely geometry",
    )
    crs: str = Field(
        default="EPSG:4326",
        description="Coordinate reference system the lat/lng values are in (default WGS84)",
    )
    keep_first_attributes: bool = Field(
        default=True,
        description="Carry forward the first vertex's row attributes onto the output (False = drop all non-key columns)",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    include_preview_metadata: bool = Field(default=False)
    preview_rows: int = Field(default=20)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _self = self
        asset_name = self.asset_name

        tags = dict(self.asset_tags or {})
        for k in (self.kinds or ["python", "spatial"]):
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            key=AssetKey.from_user_string(asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"Build {self.output_type}s from per-vertex rows grouped by {self.group_column}.",
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
                from shapely.geometry import Polygon, LineString, Point  # noqa: F401
            except ImportError as exc:
                raise ImportError(
                    "shapely is required for poly_build: pip install shapely"
                ) from exc

            df = upstream.copy()

            # Mode selection: geometry-column OR lat/lng coords.
            use_geom_col = bool(_self.input_geometry_column)
            required_cols = [_self.group_column]
            if _self.sequence_column:
                required_cols.append(_self.sequence_column)
            if use_geom_col:
                required_cols.append(_self.input_geometry_column)
            else:
                if not (_self.latitude_column and _self.longitude_column):
                    raise ValueError(
                        "poly_build: must set either `input_geometry_column` OR "
                        "both `latitude_column` + `longitude_column`."
                    )
                required_cols += [_self.latitude_column, _self.longitude_column]
            _missing_required = [c for c in required_cols if c and c not in df.columns]
            if _missing_required:
                context.log.warning(
                    f"poly_build: required columns {_missing_required} not in upstream "
                    f"DataFrame. Available: {list(df.columns)[:10]}. Returning upstream unchanged."
                )
                return df

            # Sort by group then sequence so each group's vertices come in ring
            # order. If no sequence_column, preserve upstream's natural row order
            # within each group (matches the no-sequence-field default).
            sort_cols = [_self.group_column] + ([_self.sequence_column] if _self.sequence_column else [])
            df = df.sort_values(sort_cols, kind="stable")

            # Parse cell value (Shapely / WKT-string / GeoJSON-string / dict)
            # into a Shapely geometry so we can read .x/.y or .coords uniformly.
            from shapely.geometry.base import BaseGeometry
            from shapely.geometry import shape as _shape
            from shapely import wkt as _wkt
            import json as _json
            def _to_geom(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                if isinstance(v, BaseGeometry):
                    return v
                if isinstance(v, dict):
                    try: return _shape(v)
                    except Exception: return None
                s = str(v).strip()
                if not s or s.upper() in ("NONE", "NAN", "NULL"):
                    return None
                try:
                    if s.startswith("{"):
                        return _shape(_json.loads(s))
                    return _wkt.loads(s)
                except Exception:
                    return None

            def _coords_of(group_df):
                """Yield (x, y) tuples per vertex, regardless of input mode."""
                if use_geom_col:
                    for raw in group_df[_self.input_geometry_column]:
                        geom = _to_geom(raw)
                        if geom is None:
                            continue
                        if hasattr(geom, "x") and hasattr(geom, "y"):
                            yield (geom.x, geom.y)
                        elif hasattr(geom, "coords"):
                            yield from list(geom.coords)
                else:
                    for x, y in zip(group_df[_self.longitude_column], group_df[_self.latitude_column]):
                        if pd.notna(x) and pd.notna(y):
                            yield (x, y)

            output_rows: List[Dict] = []
            skip_cols = {_self.group_column, _self.geometry_column,
                         _self.sequence_column, _self.latitude_column,
                         _self.longitude_column, _self.input_geometry_column}
            skip_cols.discard(None)
            for group_key, group_df in df.groupby(_self.group_column, sort=False):
                coords = list(_coords_of(group_df))
                if len(coords) < 2:
                    continue   # not enough vertices for even a LineString
                geom = None
                if _self.output_type == "polygon" and len(coords) >= 3:
                    if coords[0] != coords[-1]:
                        coords = coords + [coords[0]]   # auto-close
                    geom = Polygon(coords)
                else:
                    geom = LineString(coords)
                row: Dict = {_self.group_column: group_key, _self.geometry_column: geom}
                if _self.keep_first_attributes:
                    first = group_df.iloc[0].to_dict()
                    for k, v in first.items():
                        if k in skip_cols:
                            continue
                        row[k] = v
                output_rows.append(row)

            result = pd.DataFrame(output_rows)
            context.log.info(
                f"poly_build: built {len(result)} {_self.output_type}s from "
                f"{len(df)} input vertices."
            )
            metadata = {
                "dagster/row_count": MetadataValue.int(len(result)),
                "polygons_built": MetadataValue.int(len(result)),
                "vertices_consumed": MetadataValue.int(len(df)),
                "output_type": MetadataValue.text(_self.output_type),
                "crs": MetadataValue.text(_self.crs),
            }
            if _self.include_preview_metadata and len(result) > 0:
                metadata["preview"] = MetadataValue.md(
                    result.head(_self.preview_rows).astype(str).to_markdown(index=False)
                )
            context.add_output_metadata(metadata)
            return result

        return Definitions(assets=[_asset])

    @classmethod
    def get_description(cls) -> str:
        return cls.__doc__ or ""
