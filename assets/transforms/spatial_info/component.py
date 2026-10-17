"""SpatialInfo.

Compute geometry metadata (area, length, centroid, bounds, geometry-type)
for each row of an input DataFrame with a Shapely geometry column. Drop-in
for the Spatial Info step.

The selected metrics get appended as new columns. Area and length are
computed in the projected CRS for accurate real-world units (defaults to
EPSG:3857 / Web Mercator) — square meters and meters respectively — then
the output rows return in the original CRS.
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


_METRIC_NAMES = {"area", "length", "centroid", "bounds", "geom_type", "num_points", "is_valid"}



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


class SpatialInfoComponent(Component, Model, Resolvable):
    """Compute geometry metadata columns (area, length, centroid, bounds, etc.)."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream (Geo)DataFrame with a geometry column")
    geometry_column: Union[str, int] = Field(default="geometry", description="Geometry column to inspect")
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

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned. A partitioned run sends only the rows in its partition.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides the flat partition fields.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the rows upstream rows to the current date partition (time partitions).",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream rows to the current static/dynamic partition key.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Name of the static axis in a multi-partition, e.g. 'customer' or 'region'.",
    )
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=(), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions
        )
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        _self = self
        asset_name = self.asset_name

        tags = dict(self.asset_tags or {})
        for k in (self.kinds or ["python", "spatial"]):
            tags[f"dagster/kind/{k}"] = ""

        @asset(partitions_def=partitions_def, 
            key=AssetKey.from_user_string(asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"Append geometry metadata columns ({', '.join(self.metrics)}).",
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
            # Filter to current partition if partitioned
            if context.has_partition_key or context.has_partition_key_range:
                # One filter pass per partition key: a normal tick gives exactly one,
                # a single_run backfill gives the whole range. Rows are selected by
                # position so keys that match overlapping rows never duplicate them.
                _pk_base = upstream
                _pk_pos = _pk_base.reset_index(drop=True) if hasattr(_pk_base, 'reset_index') else _pk_base
                _pk_keep = set()
                _pk_filtered = False
                for _pk_iter in context.partition_keys:
                    upstream = _pk_pos
                    _pk = _pk_iter
                    _is_multi = hasattr(_pk, "keys_by_dimension")
                    _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                    _static_key = next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                    if partition_date_column and partition_date_column in upstream.columns and _date_key:
                        _col_dates = pd.to_datetime(upstream[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                        upstream = upstream[_col_dates == _date_key]
                    if partition_static_column and partition_static_column in upstream.columns and _static_key:
                        upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                    elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                        upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
                    if upstream is not _pk_pos:
                        _pk_filtered = True
                        _pk_keep.update(upstream.index)
                upstream = _pk_base.iloc[sorted(_pk_keep)] if _pk_filtered else _pk_base
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


# BEGIN scaffold partitions-def builder (inlined per component; keep copies identical, see tests/test_partition_port_scaffold.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from dagster import (  # noqa: E402,F401
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    HourlyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    WeeklyPartitionsDefinition,
)


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")
# END scaffold partitions-def builder
