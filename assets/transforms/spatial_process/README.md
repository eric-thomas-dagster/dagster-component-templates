# SpatialProcessComponent

Apply a single geometry operation to a column of Shapely geometries.
Drop-in for Alteryx's **Spatial Process** tool.

## Supported Methods

| `method` | What it does | Example |
|---|---|---|
| `centroid` | Replaces each polygon with its centroid point | Store-area polygons → store-center points |
| `boundary` | Returns the outer ring (a LineString) | Polygon → outline |
| `polygon_to_lines` | Same as `boundary` — explicit name match for Alteryx | |
| `polygon_to_points` | Explodes the exterior ring vertices to a MultiPoint | Building corners |
| `line_to_polygon` | Closes a polyline into a polygon (auto-closes the ring) | |
| `convex_hull` | Smallest convex polygon containing the geometry | Service-territory shape |
| `envelope` | Axis-aligned bounding box | Quick spatial index region |
| `simplify` | Douglas-Peucker simplification (configurable `tolerance`) | Reduce vertex count for rendering |
| `buffer` | Buffer the geometry by `buffer_distance` (CRS units) | Service area / proximity zone |
| `set_precision` | Round coordinates to `precision_decimals` places | Compress / dedupe near-identical geoms |

## Use Cases

- Generate centroid points from polygon assets for nearest-neighbor / KNN
- Build buffer zones around customer locations for trade-area analysis
- Simplify boundary polygons for faster web rendering / smaller geojson
- Convert polyline tracks back to closed polygons (e.g., GPS perimeter walks)

## Example

```yaml
type: dagster_community_components.SpatialProcessComponent
attributes:
  asset_name: store_centroids
  upstream_asset_key: store_polygons
  method: centroid
  geometry_column: geometry
  group_name: geospatial
```

## Inputs and Outputs

| | Type | Description |
|-|------|-------------|
| Input | `pd.DataFrame` | One row per geometry. `geometry_column` holds Shapely objects OR WKT/GeoJSON strings — both are accepted. |
| Output | `pd.DataFrame` | Same shape; `output_column` (defaults to `geometry_column`) carries the result of `method`. |

## Notes

- WKT / GeoJSON strings are auto-parsed via `shapely.wkt.loads` / `shapely.geometry.shape`.
- For real-world units in `buffer`, project to a metric CRS first (e.g., EPSG:3857 for Web Mercator meters or a UTM zone for survey accuracy).
- `set_precision` rounds coordinates in place — useful before writing GeoJSON.
- All ops short-circuit on `None` / empty geometries to keep the row count stable.

## Fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `asset_name` | yes | — | Output Dagster asset name |
| `upstream_asset_key` | yes | — | Upstream DataFrame asset key |
| `method` | yes | `centroid` | Operation (see table above) |
| `geometry_column` | no | `geometry` | Input geometry column |
| `output_column` | no | (in place) | Where to write the result; defaults to overwriting `geometry_column` |
| `tolerance` | no | `0.0001` | Simplify tolerance (Douglas-Peucker) |
| `buffer_distance` | no | `0.0` | Buffer distance for `buffer` (CRS units) |
| `precision_decimals` | no | `6` | Coordinate precision for `set_precision` |
| `group_name` | no | `None` | Dagster asset group name |
