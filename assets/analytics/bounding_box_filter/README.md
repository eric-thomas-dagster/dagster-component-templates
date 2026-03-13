# bounding_box_filter

Filter a DataFrame of geographic points to keep only rows whose lat/lng coordinates fall inside a rectangular bounding box. Set `keep_outside: true` to invert the filter (keep points outside the box). Row counts before and after filtering are logged to asset metadata.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame with lat/lng columns |
| `lat_column` | `str` | `"latitude"` | Column name containing latitude values |
| `lng_column` | `str` | `"longitude"` | Column name containing longitude values |
| `min_lat` | `float` | required | Southern boundary (minimum latitude) |
| `max_lat` | `float` | required | Northern boundary (maximum latitude) |
| `min_lng` | `float` | required | Western boundary (minimum longitude) |
| `max_lng` | `float` | required | Eastern boundary (maximum longitude) |
| `keep_outside` | `bool` | `false` | If `true`, keep points OUTSIDE the bounding box |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Example YAML

```yaml
component_type: bounding_box_filter
description: Filter GPS events to the contiguous United States.

asset_name: us_gps_events
upstream_asset_key: all_gps_events
lat_column: latitude
lng_column: longitude
min_lat: 24.396308
max_lat: 49.384358
min_lng: -125.0
max_lng: -66.93457
keep_outside: false
group_name: geospatial
```

## Metadata Logged

- `input_rows` — Total rows in the upstream DataFrame
- `output_rows` — Rows remaining after filtering
- `filtered_rows` — Rows removed by the filter
- `keep_outside` — Whether the filter was inverted
- `bounding_box` — Human-readable bounding box coordinates

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Requirements

- `dagster`
- `pandas`
