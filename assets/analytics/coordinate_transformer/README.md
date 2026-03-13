# coordinate_transformer

Reproject coordinate columns in a DataFrame from one coordinate reference system (CRS) to another using pyproj. Supports any EPSG code or PROJ string — common use cases include converting WGS84 (EPSG:4326) to Web Mercator (EPSG:3857), UTM zones, or national grid systems. Transformed values are written to configurable output columns.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame with coordinate columns |
| `x_column` | `str` | required | Input column for X values (longitude in geographic CRS) |
| `y_column` | `str` | required | Input column for Y values (latitude in geographic CRS) |
| `source_crs` | `str` | `"EPSG:4326"` | Source CRS as EPSG code or PROJ string |
| `target_crs` | `str` | `"EPSG:3857"` | Target CRS as EPSG code or PROJ string |
| `output_x_column` | `str` | `"x_transformed"` | Output column name for transformed X values |
| `output_y_column` | `str` | `"y_transformed"` | Output column name for transformed Y values |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Example YAML

```yaml
component_type: coordinate_transformer
description: Reproject event coordinates from WGS84 to Web Mercator.

asset_name: events_web_mercator
upstream_asset_key: events
x_column: longitude
y_column: latitude
source_crs: EPSG:4326
target_crs: EPSG:3857
output_x_column: x_mercator
output_y_column: y_mercator
group_name: geospatial
```

## Metadata Logged

- `rows` — Total rows transformed
- `source_crs` — Source coordinate reference system
- `target_crs` — Target coordinate reference system
- `output_x_column` — Name of the transformed X column
- `output_y_column` — Name of the transformed Y column

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Requirements

- `dagster`
- `pandas`
- `pyproj`
