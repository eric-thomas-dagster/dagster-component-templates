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


class DriveTimeComponent(Component, Model, Resolvable):
    """Compute drive-time isochrones for each input point via a routing API."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a (Geo)DataFrame")
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
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
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

        @asset(
            key=AssetKey.from_user_string(asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            group_name=self.group_name,
            description=self.description or f"Drive-time isochrones ({drive_minutes} min, via {provider}).",
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
