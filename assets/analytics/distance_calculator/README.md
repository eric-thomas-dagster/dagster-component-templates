# distance_calculator

Calculate the geographic distance between two lat/lng coordinate pairs (origin and destination) per row in a DataFrame. Supports the Haversine formula (fast, suitable for most use cases) or the geodesic/Vincenty formula (precise, requires `geopy`). Output unit can be km, miles, or meters.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `lat1_column` | `str` | required | Column name for origin latitude |
| `lng1_column` | `str` | required | Column name for origin longitude |
| `lat2_column` | `str` | required | Column name for destination latitude |
| `lng2_column` | `str` | required | Column name for destination longitude |
| `output_column` | `str` | `"distance_km"` | Column name for the computed distance |
| `unit` | `str` | `"km"` | Distance unit: `km`, `miles`, or `meters` |
| `formula` | `str` | `"haversine"` | Formula: `haversine` (fast) or `vincenty` (precise, requires geopy) |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Example YAML

```yaml
component_type: distance_calculator
description: Calculate haversine distance between pickup and dropoff coordinates.

asset_name: deliveries_with_distance
upstream_asset_key: deliveries
lat1_column: pickup_lat
lng1_column: pickup_lng
lat2_column: dropoff_lat
lng2_column: dropoff_lng
output_column: distance_km
unit: km
formula: haversine
group_name: geospatial
```

## Metadata Logged

- `rows` — Total input rows
- `formula` — Distance formula used
- `unit` — Output distance unit
- `mean_distance` — Mean distance across all rows
- `max_distance` — Maximum distance in the dataset

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Requirements

- `dagster`
- `pandas`
- `geopy` (only required when `formula: vincenty`)
