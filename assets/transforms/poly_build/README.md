# PolyBuildComponent

Construct polygon (or polyline) geometries from a points DataFrame by
grouping rows into rings and ordering them by a sequence column. Drop-in
for Alteryx's **Poly-Build** spatial tool.

## Use Cases

- Build trade-area polygons from store boundary vertex tables
- Reconstruct administrative boundaries from a flat per-vertex export
- Turn GPS track points into a polyline per trip
- Build building footprints from corner-coordinate tables

## Example

```yaml
type: dagster_community_components.PolyBuildComponent
attributes:
  asset_name: store_trade_polygons
  upstream_asset_key: store_vertex_rows
  group_column: store_id
  sequence_column: vertex_order
  latitude_column: lat
  longitude_column: lng
  output_type: polygon
  geometry_column: geometry
  crs: EPSG:4326
  group_name: geospatial
```

## Inputs and Outputs

| | Type | Description |
|-|------|-------------|
| Input | `pd.DataFrame` | One row per vertex; must have `group_column`, `sequence_column`, `latitude_column`, `longitude_column` |
| Output | `pd.DataFrame` | One row per built geometry; `geometry_column` holds a Shapely `Polygon` or `LineString` |

## Notes

- `output_type: polygon` (default) closes the ring automatically if the
  start and end vertices differ. Falls back to `LineString` when fewer than
  3 distinct vertices are available.
- `output_type: line` produces an open polyline (no auto-close).
- `keep_first_attributes: true` (default) carries the first vertex's
  non-key columns onto the output row. Set `false` to drop them.
- Rows with `NaN` lat/lng are dropped per-group; groups with <2 vertices
  are skipped entirely (logged at info level).
- `crs` defaults to `EPSG:4326`. Downstream `geo_buffer`, `geo_overlay`,
  etc. expect the same CRS unless explicitly reprojected.
