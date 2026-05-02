# CreatePoints

Takes a DataFrame with lat/lng columns and returns a GeoDataFrame with a `geometry` column of shapely Points (in EPSG:4326 by default). The starting block of any spatial pipeline — once you have geometry, buffer / spatial_join / make_grid all work.

## Example

```yaml
type: dagster_component_templates.CreatePointsComponent
attributes:
  asset_name: stores_with_geometry
  upstream_asset_key: stores_raw
  lat_column: lat
  lng_column: lng
  crs: "EPSG:4326"
  group_name: spatial
```


## Requirements

```
pandas
geopandas
shapely
```
