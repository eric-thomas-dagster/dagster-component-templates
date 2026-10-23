"""DriveTime.

Compute drive-time isochrones (polygons reachable within N minutes by car)
for each row in an input DataFrame.and **Trade Area** (when configured for drive-time vs straight-line buffer).

Provider-agnostic at the field level — pick one of:
  - `openrouteservice` (default; free tier with an API key; ~40 req/min)
  - `google`   (Google Maps Directions API — paid, requires billing)
  - `mapbox`   (Mapbox Isochrone API — paid, requires billing)
  - `osrm`     (self-hosted OSRM, fully free; requires `osrm_base_url`)

v1 ships full openrouteservice support. The other providers raise
NotImplementedError with a pointer to add the implementation — same
component shape, drop-in once the relevant Python client + key are added.

Input: a (Geo)DataFrame with either (a) `geometry` Point column, or
(b) explicit `latitude_column` / `longitude_column` fields.
Output: same DataFrame + a `geometry` column whose value for each row is
the drive-time isochrone polygon (a Shapely Polygon / MultiPolygon).
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


_VALID_PROVIDERS = {"openrouteservice", "google", "mapbox", "osrm"}
_VALID_PROFILES = {
    "driving", "driving-car",     # car
    "walking", "foot-walking",     # walking
    "cycling", "cycling-regular",  # cycling
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


class DriveTimeComponent(Component, Model, Resolvable):
    """Compute drive-time isochrones for each input point via a routing API."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream asset key providing a (Geo)DataFrame")
    drive_minutes: float = Field(
        description="Isochrone size in minutes (e.g. 15 = polygon reachable within 15 min drive)."
    )
    provider: str = Field(
        default="openrouteservice",
        description=(
            "Routing provider. v1 ships openrouteservice; google / mapbox / "
            "osrm raise NotImplementedError until wired (same component shape)."
        ),
    )
    api_key_env_var: Optional[str] = Field(
        default="OPENROUTESERVICE_API_KEY",
        description=(
            "Env var holding the routing provider's API key. Defaults to "
            "OPENROUTESERVICE_API_KEY. Not needed for `provider=osrm` "
            "(self-hosted, no auth)."
        ),
    )
    osrm_base_url: Optional[str] = Field(
        default=None,
        description="Required when `provider=osrm`. e.g. `http://localhost:5000`.",
    )
    profile: str = Field(
        default="driving-car",
        description=(
            "Travel mode. openrouteservice profiles: 'driving-car' (default), "
            "'foot-walking', 'cycling-regular'. Other names get normalized "
            "to ORS conventions."
        ),
    )

    geometry_column: Union[str, int] = Field(
        default="geometry",
        description="Input + output geometry column name. Output isochrone replaces input point.",
    )
    latitude_column: Optional[Union[str, int]] = Field(
        default=None,
        description=(
            "If the input has no geometry column yet, name the lat column here "
            "+ longitude_column. We'll build the Point ourselves."
        ),
    )
    longitude_column: Optional[Union[str, int]] = Field(default=None)

    group_name: Optional[str] = Field(default=None)

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned. A partitioned run sends only the rows in its partition.",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
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
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        drive_minutes = self.drive_minutes
        provider = self.provider.lower()
        if provider not in _VALID_PROVIDERS:
            raise ValueError(
                f"Unknown provider {provider!r}. Valid: {sorted(_VALID_PROVIDERS)}."
            )
        api_key_env_var = self.api_key_env_var
        osrm_base_url = self.osrm_base_url
        profile = self.profile
        geometry_column = self.geometry_column
        latitude_column = self.latitude_column
        longitude_column = self.longitude_column

        tags = dict(self.asset_tags or {})
        for k in (self.kinds or ["python", "spatial", "routing"]):
            tags[f"dagster/kind/{k}"] = ""

        @asset(partitions_def=partitions_def, 
            key=AssetKey.from_user_string(asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"Drive-time isochrones ({drive_minutes} min, via {provider}).",
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
                from shapely.geometry import shape
            except ImportError as e:
                raise ImportError(
                    "geopandas + shapely required: pip install geopandas shapely"
                ) from e

            df = upstream.copy()
            # Materialize (lon, lat) pairs from either the geometry column
            # or the lat/lon column fields. Geometry values may arrive as
            # Shapely objects (native) OR WKT / GeoJSON strings (CSV-read
            # upstreams) OR Python dicts (JSON column); parse all three.
            from shapely.geometry.base import BaseGeometry
            from shapely import wkt as _wkt
            import json as _json
            def _to_geom(g):
                if g is None or (isinstance(g, float) and pd.isna(g)):
                    return None
                if isinstance(g, BaseGeometry):
                    return g
                if isinstance(g, dict):
                    try:
                        return shape(g)
                    except Exception:
                        return None
                s = str(g).strip()
                if not s or s.upper() in ("NONE", "NAN", "NULL"):
                    return None
                try:
                    if s.startswith("{"):
                        return shape(_json.loads(s))
                    return _wkt.loads(s)
                except Exception:
                    return None
            if geometry_column in df.columns and df[geometry_column].notna().any():
                _parsed = df[geometry_column].apply(_to_geom)
                _mask = _parsed.notna()
                coords = [(g.x, g.y) for g in _parsed[_mask].tolist()]
                rows_with_geom = df[_mask].reset_index(drop=True)
            elif latitude_column and longitude_column:
                df = df.dropna(subset=[latitude_column, longitude_column])
                coords = list(zip(
                    pd.to_numeric(df[longitude_column], errors="coerce"),
                    pd.to_numeric(df[latitude_column], errors="coerce"),
                ))
                rows_with_geom = df.reset_index(drop=True)
            else:
                context.log.warning(
                    "drive_time: input has no geometry column, and latitude_column / "
                    "longitude_column weren't both set. Returning upstream unchanged."
                )
                return df.copy()

            if provider == "openrouteservice":
                import os
                try:
                    import openrouteservice as ors
                except ImportError as e:
                    raise ImportError(
                        "openrouteservice client required: pip install openrouteservice"
                    ) from e
                api_key = os.environ.get(api_key_env_var or "OPENROUTESERVICE_API_KEY")
                if not api_key:
                    context.log.warning(
                        f"drive_time: env var {api_key_env_var!r} not set. "
                        "Returning upstream unchanged; provide an OpenRouteService key "
                        "at https://openrouteservice.org/dev/#/signup to enable isochrones."
                    )
                    _passthru = df.copy()
                    # Mirror the geometry to `<geom>_TradeArea` + the canonical
                    # `SpatialObj_TradeArea` / `SpatialObject_TradeArea` aliases
                    # so downstream summarize / spatial_join steps that reference
                    # the trade-area column don't blow up with KeyError just
                    # because the API key wasn't set.
                    if geometry_column in _passthru.columns:
                        for _alias in (
                            f"{geometry_column}_TradeArea",
                            "SpatialObj_TradeArea",
                            "SpatialObject_TradeArea",
                        ):
                            if _alias != geometry_column:
                                _passthru[_alias] = _passthru[geometry_column]
                    return _passthru
                ors_profile = {
                    "driving": "driving-car",
                    "walking": "foot-walking",
                    "cycling": "cycling-regular",
                }.get(profile, profile)
                client = ors.Client(key=api_key)
                # ORS isochrones endpoint accepts batched coordinates.
                # 5 locations per call cap on free tier; chunk defensively.
                isochrones: List = []
                chunk = 5
                seconds = int(drive_minutes * 60)
                for i in range(0, len(coords), chunk):
                    batch = coords[i:i + chunk]
                    resp = client.isochrones(
                        locations=batch,
                        profile=ors_profile,
                        range=[seconds],
                        range_type="time",
                    )
                    for feat in resp.get("features", []):
                        isochrones.append(shape(feat["geometry"]))
                if len(isochrones) != len(rows_with_geom):
                    context.log.warning(
                        f"ORS returned {len(isochrones)} isochrones for "
                        f"{len(rows_with_geom)} inputs — padding with None."
                    )
                    isochrones += [None] * (len(rows_with_geom) - len(isochrones))
                rows_with_geom[geometry_column] = isochrones
                # Also write `<geom>_TradeArea` + the canonical Alteryx-ish
                # `SpatialObj_TradeArea` / `SpatialObject_TradeArea` aliases.
                # Source workflows reference these names regardless of what
                # the input geometry column was called upstream.
                for _alias in (
                    f"{geometry_column}_TradeArea",
                    "SpatialObj_TradeArea",
                    "SpatialObject_TradeArea",
                ):
                    if _alias != geometry_column:
                        rows_with_geom[_alias] = isochrones
                out = gpd.GeoDataFrame(rows_with_geom, geometry=geometry_column, crs="EPSG:4326")

            elif provider == "google":
                # Google Maps Directions API doesn't return isochrones natively
                # — we use the Distance Matrix API to find every nearby point
                # within `drive_minutes`. For true isochrones, use mapbox / ORS.
                # This impl trades a circular-buffer approximation: take the
                # max drive distance @ avg speed and buffer the origin.
                import os
                try:
                    import googlemaps
                except ImportError as e:
                    raise ImportError("googlemaps required: pip install googlemaps") from e
                api_key = os.environ.get(api_key_env_var or "GOOGLE_MAPS_API_KEY")
                if not api_key:
                    raise EnvironmentError(
                        f"Env var {api_key_env_var!r} not set. Enable Distance "
                        "Matrix API at https://console.cloud.google.com/apis/library/distancematrix-backend.googleapis.com"
                    )
                gmaps = googlemaps.Client(key=api_key)
                # For each origin, generate compass-rose probe points + ask
                # the Distance Matrix for actual drive times. Then buffer the
                # origin by the max reachable distance.
                from shapely.geometry import Polygon
                import math
                isochrones = []
                for (lon, lat) in coords:
                    # 8 probe directions at the maximum possible distance
                    # (assuming 100 km/h max — adjust if you have local speeds).
                    max_km = drive_minutes / 60 * 100
                    probes = []
                    for bearing in range(0, 360, 45):
                        rad = math.radians(bearing)
                        dlat = (max_km / 111.0) * math.cos(rad)
                        dlon = (max_km / (111.0 * math.cos(math.radians(lat)))) * math.sin(rad)
                        probes.append((lat + dlat, lon + dlon))
                    # One Distance Matrix call per origin (probes are dests)
                    resp = gmaps.distance_matrix(
                        origins=[(lat, lon)],
                        destinations=probes,
                        mode="driving",
                    )
                    reachable_points = [(lon, lat)]
                    for probe_idx, elem in enumerate(resp["rows"][0]["elements"]):
                        if elem.get("status") == "OK" and elem["duration"]["value"] <= drive_minutes * 60:
                            reachable_points.append((probes[probe_idx][1], probes[probe_idx][0]))
                    if len(reachable_points) >= 3:
                        isochrones.append(Polygon(reachable_points).convex_hull)
                    else:
                        isochrones.append(None)
                rows_with_geom[geometry_column] = isochrones
                # Also write `<geom>_TradeArea` + the canonical Alteryx-ish
                # `SpatialObj_TradeArea` / `SpatialObject_TradeArea` aliases.
                # Source workflows reference these names regardless of what
                # the input geometry column was called upstream.
                for _alias in (
                    f"{geometry_column}_TradeArea",
                    "SpatialObj_TradeArea",
                    "SpatialObject_TradeArea",
                ):
                    if _alias != geometry_column:
                        rows_with_geom[_alias] = isochrones
                out = gpd.GeoDataFrame(rows_with_geom, geometry=geometry_column, crs="EPSG:4326")

            elif provider == "mapbox":
                # Mapbox Isochrone API: GET /isochrone/v1/{profile}/{lon,lat}
                import os
                import requests
                api_key = os.environ.get(api_key_env_var or "MAPBOX_ACCESS_TOKEN")
                if not api_key:
                    raise EnvironmentError(
                        f"Env var {api_key_env_var!r} not set. Get a token "
                        "at https://account.mapbox.com/access-tokens/"
                    )
                mb_profile = {
                    "driving": "driving",
                    "driving-car": "driving",
                    "walking": "walking",
                    "foot-walking": "walking",
                    "cycling": "cycling",
                    "cycling-regular": "cycling",
                }.get(profile, "driving")
                isochrones = []
                for (lon, lat) in coords:
                    url = f"https://api.mapbox.com/isochrone/v1/mapbox/{mb_profile}/{lon},{lat}"
                    resp = requests.get(
                        url,
                        params={
                            "contours_minutes": int(drive_minutes),
                            "polygons": "true",
                            "access_token": api_key,
                        },
                        timeout=30,
                    )
                    if resp.status_code != 200:
                        context.log.warning(
                            f"Mapbox returned {resp.status_code} for ({lon},{lat}): {resp.text[:200]}"
                        )
                        isochrones.append(None)
                        continue
                    feats = resp.json().get("features", [])
                    isochrones.append(shape(feats[0]["geometry"]) if feats else None)
                rows_with_geom[geometry_column] = isochrones
                # Also write `<geom>_TradeArea` + the canonical Alteryx-ish
                # `SpatialObj_TradeArea` / `SpatialObject_TradeArea` aliases.
                # Source workflows reference these names regardless of what
                # the input geometry column was called upstream.
                for _alias in (
                    f"{geometry_column}_TradeArea",
                    "SpatialObj_TradeArea",
                    "SpatialObject_TradeArea",
                ):
                    if _alias != geometry_column:
                        rows_with_geom[_alias] = isochrones
                out = gpd.GeoDataFrame(rows_with_geom, geometry=geometry_column, crs="EPSG:4326")

            elif provider == "osrm":
                # Self-hosted OSRM doesn't ship an isochrone endpoint out of
                # the box — but if you've added the `osrm-isochrone` project
                # alongside, it exposes the same shape. We hit the /isochrone
                # endpoint and parse the GeoJSON response.
                import requests
                if not osrm_base_url:
                    raise EnvironmentError(
                        "provider='osrm' requires `osrm_base_url` "
                        "(e.g. 'http://localhost:5000'). Run an OSRM "
                        "container with the osrm-isochrone add-on."
                    )
                base = osrm_base_url.rstrip("/")
                osrm_profile = {
                    "driving": "driving",
                    "driving-car": "driving",
                    "walking": "foot",
                    "foot-walking": "foot",
                    "cycling": "bike",
                    "cycling-regular": "bike",
                }.get(profile, "driving")
                isochrones = []
                for (lon, lat) in coords:
                    url = f"{base}/isochrone/v1/{osrm_profile}/{lon},{lat}"
                    resp = requests.get(
                        url,
                        params={
                            "contours_minutes": int(drive_minutes),
                            "polygons": "true",
                        },
                        timeout=30,
                    )
                    if resp.status_code != 200:
                        context.log.warning(
                            f"OSRM returned {resp.status_code} for ({lon},{lat}): {resp.text[:200]}"
                        )
                        isochrones.append(None)
                        continue
                    feats = resp.json().get("features", [])
                    isochrones.append(shape(feats[0]["geometry"]) if feats else None)
                rows_with_geom[geometry_column] = isochrones
                # Also write `<geom>_TradeArea` + the canonical Alteryx-ish
                # `SpatialObj_TradeArea` / `SpatialObject_TradeArea` aliases.
                # Source workflows reference these names regardless of what
                # the input geometry column was called upstream.
                for _alias in (
                    f"{geometry_column}_TradeArea",
                    "SpatialObj_TradeArea",
                    "SpatialObject_TradeArea",
                ):
                    if _alias != geometry_column:
                        rows_with_geom[_alias] = isochrones
                out = gpd.GeoDataFrame(rows_with_geom, geometry=geometry_column, crs="EPSG:4326")

            else:
                raise ValueError(f"Unhandled provider {provider!r}")

            context.add_output_metadata({
                "dagster/row_count": MetadataValue.int(len(out)),
                "drive_minutes": MetadataValue.float(float(drive_minutes)),
                "provider": MetadataValue.text(provider),
                "profile": MetadataValue.text(profile),
            })
            return out

        return Definitions(assets=[_asset])


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
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
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
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(
                start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"],
            )
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
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
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
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule)
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
