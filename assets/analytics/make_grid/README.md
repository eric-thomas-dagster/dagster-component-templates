# MakeGrid

Tile the bounding box of the input geometries (or an explicit bbox) into a regular grid of squares with a configurable cell size. Outputs one row per cell with cell_id, row, col, and a polygon geometry — useful for heatmap-style aggregation by spatial bucket.

## Example

```yaml
type: dagster_component_templates.MakeGridComponent
attributes:
  asset_name: city_grid_cells
  upstream_asset_key: cities_with_geometry
  cell_size_meters: 100000
  src_crs: "EPSG:4326"
  metric_crs: "EPSG:3857"
  group_name: spatial
```


## Requirements

```
pandas
geopandas
shapely
numpy
```
