# point_in_polygon

Test whether geographic points in a DataFrame fall inside GeoJSON polygon features. Annotates each row with the matching polygon's name (or null if no polygon contains the point). GeoJSON can be loaded from a local file path or a remote URL. Optionally adds a boolean column indicating whether each point is inside any polygon.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame with lat/lng columns |
| `lat_column` | `str` | `"latitude"` | Column name containing latitude values |
| `lng_column` | `str` | `"longitude"` | Column name containing longitude values |
| `geojson_path` | `Optional[str]` | `null` | Path to a local GeoJSON file with polygon features |
| `geojson_url` | `Optional[str]` | `null` | URL to a GeoJSON file (alternative to `geojson_path`) |
| `polygon_name_field` | `Optional[str]` | `null` | GeoJSON feature property key to use as polygon name |
| `output_column` | `str` | `"region"` | Column name for the matched polygon name |
| `output_inside_column` | `Optional[str]` | `null` | Optional boolean column: True if point is inside any polygon |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Example YAML

```yaml
component_type: point_in_polygon
description: Tag each event with the US state it occurred in.

asset_name: events_with_state
upstream_asset_key: events
lat_column: latitude
lng_column: longitude
geojson_path: /data/regions/us_states.geojson
polygon_name_field: NAME
output_column: state
output_inside_column: in_us
group_name: geospatial
```

## Metadata Logged

- `rows` — Total input rows
- `matched_count` — Rows matched to a polygon
- `polygon_count` — Number of polygons loaded from GeoJSON
- `match_rate` — Percentage of rows that matched a polygon

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Requirements

- `dagster`
- `pandas`
- `shapely`
