# spatial_join

Spatially join a points DataFrame (with lat/lng columns) against a regions DataFrame (with a GeoJSON geometry column) using GeoPandas. Each point is enriched with the attributes of the region polygon it falls within. Supports left join (keep all points) or inner join (keep only matched points).

This component takes two upstream assets: one for points and one for regions.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Points asset key providing a DataFrame with lat/lng columns |
| `regions_asset_key` | `str` | required | Regions asset key providing a DataFrame with a geometry column |
| `lat_column` | `str` | `"latitude"` | Column name in the points DataFrame for latitude |
| `lng_column` | `str` | `"longitude"` | Column name in the points DataFrame for longitude |
| `geometry_column` | `str` | `"geometry"` | Column in the regions DataFrame with GeoJSON geometry dicts or strings |
| `how` | `str` | `"left"` | Join type: `left` or `inner` |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Example YAML

```yaml
component_type: spatial_join
description: Join user events to administrative regions.

asset_name: events_joined_regions
upstream_asset_key: events
regions_asset_key: admin_regions
lat_column: latitude
lng_column: longitude
geometry_column: geometry
how: left
group_name: geospatial
```

## Metadata Logged

- `input_points` — Number of rows in the points DataFrame
- `input_regions` — Number of rows in the regions DataFrame
- `output_rows` — Number of rows in the joined result
- `join_type` — Join type used (`left` or `inner`)

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Requirements

- `dagster`
- `pandas`
- `geopandas`
- `shapely`
