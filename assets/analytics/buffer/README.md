# Buffer

Reads a WKT `geometry` column and replaces each geometry with its buffer at the given radius — e.g. a 5km circle around each store. Useful for service-area / catchment analyses paired with `spatial_join`.

## Example

```yaml
type: dagster_component_templates.BufferComponent
attributes:
  asset_name: store_service_areas
  upstream_asset_key: stores_with_geometry
  geometry_column: geometry
  radius_meters: 5000
  metric_crs: "EPSG:3857"
  src_crs: "EPSG:4326"
  group_name: spatial
```


## Requirements

```
pandas
geopandas
shapely
```
