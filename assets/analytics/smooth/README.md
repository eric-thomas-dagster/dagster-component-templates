# Smooth

Apply shapely's `simplify` (Douglas-Peucker) to each geometry, dropping points that fall within `tolerance_meters` of the simplified path. Useful before rendering large polygons in the UI or shipping geo data over the wire.

## Example

```yaml
type: dagster_component_templates.SmoothComponent
attributes:
  asset_name: simplified_zones
  upstream_asset_key: store_service_areas
  geometry_column: geometry
  tolerance_meters: 500
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
