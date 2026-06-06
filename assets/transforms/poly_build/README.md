# PolyBuildComponent

Construct polygon (or polyline) geometries from a points DataFrame by
grouping rows into rings and ordering them by a sequence column. Drop-in
for Alteryx's **Poly-Build** spatial tool.

## Use Cases

- Build trade-area polygons from store boundary vertex tables
- Reconstruct administrative boundaries from a flat per-vertex export
- Turn GPS track points into a polyline per trip
- Build building footprints from corner-coordinate tables

## Input Modes

Pick **one** input mode based on what the upstream DataFrame already has:

| Mode | Required fields | Use when |
|------|-----------------|----------|
| **lat/lng** | `latitude_column` + `longitude_column` | Upstream has raw numeric lat/lng columns |
| **geometry** | `input_geometry_column` | Upstream already has Shapely `Point` geometries (e.g. from `CreatePointsComponent` or `geocoder`) — matches Alteryx PolyBuild's `<SpatialObj field="X"/>` |

## Example — lat/lng mode

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

## Example — geometry-column mode (Alteryx convention)

```yaml
type: dagster_community_components.PolyBuildComponent
attributes:
  asset_name: store_trade_polygons
  upstream_asset_key: store_vertex_points
  group_column: store_id
  sequence_column: vertex_order
  input_geometry_column: Centroid    # Point geoms from CreatePoints (Alteryx default name)
  output_type: polygon
  group_name: geospatial
```

## Inputs and Outputs

| | Type | Description |
|-|------|-------------|
| Input | `pd.DataFrame` | One row per vertex; must have `group_column`, optionally `sequence_column`, plus EITHER `latitude_column`+`longitude_column` OR `input_geometry_column` |
| Output | `pd.DataFrame` | One row per built geometry; `geometry_column` holds a Shapely `Polygon` or `LineString` |

## Notes

- `sequence_column` is optional. When omitted, vertices come out in the
  upstream's natural row order within each group (matches Alteryx
  PolyBuild's behavior when `SequenceField` is left blank).
- `output_type: polygon` (default) closes the ring automatically if the
  start and end vertices differ. Falls back to `LineString` when fewer
  than 3 distinct vertices are available.
- `output_type: line` produces an open polyline (no auto-close).
- `keep_first_attributes: true` (default) carries the first vertex's
  non-key columns onto the output row. Set `false` to drop them.
- Rows with `NaN` lat/lng (or null geoms) are dropped per-group;
  groups with <2 vertices are skipped entirely (logged at info level).
- `crs` defaults to `EPSG:4326`. Downstream `geo_buffer`, `geo_overlay`,
  etc. expect the same CRS unless explicitly reprojected.

## Fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `asset_name` | yes | — | Output Dagster asset name |
| `upstream_asset_key` | yes | — | Upstream asset key — DataFrame with one row per vertex |
| `group_column` | yes | — | Column identifying which polygon each vertex belongs to |
| `sequence_column` | no | `None` | Column ordering vertices within each group; `None` = upstream row order |
| `latitude_column` | one-of | `None` | Vertex latitude (lat/lng mode) |
| `longitude_column` | one-of | `None` | Vertex longitude (lat/lng mode) |
| `input_geometry_column` | one-of | `None` | Existing Shapely Points (geometry mode) |
| `output_type` | no | `polygon` | `polygon` (auto-closes) or `line` |
| `geometry_column` | no | `geometry` | Output column name for the built Shapely geometry |
| `crs` | no | `EPSG:4326` | Coordinate reference system of input coords |
| `keep_first_attributes` | no | `true` | Carry first vertex's non-key columns onto output |
| `group_name` | no | `None` | Dagster asset group name |
