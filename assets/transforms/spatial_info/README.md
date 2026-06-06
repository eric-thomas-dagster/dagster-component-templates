# SpatialInfoComponent

Append geometry metadata columns (area, length, centroid, bounds, geom
type, vertex count, validity) to a DataFrame with a Shapely geometry
column. Drop-in for Alteryx's **Spatial Info** tool.

## Use Cases

- Add `area_sq_miles` to a trade-area polygon DataFrame for size reports
- Add `centroid_x` / `centroid_y` to building footprints for plotting
- Add `length_meters` to road segments for cost calculations
- Add `bounds` (minx/miny/maxx/maxy) for spatial indexing / filtering
- Validate geometry integrity (`is_valid`) before downstream spatial joins

## Example

```yaml
type: dagster_community_components.SpatialInfoComponent
attributes:
  asset_name: store_trade_area_metrics
  upstream_asset_key: store_trade_areas
  geometry_column: geometry
  metrics: [area, length, centroid, bounds, geom_type]
  projected_crs: EPSG:3857
  area_unit: sq_miles
  length_unit: miles
  group_name: geospatial
```

## Inputs and Outputs

| | Type | Description |
|-|------|-------------|
| Input | `pd.DataFrame` | Must have a Shapely geometry column |
| Output | `pd.DataFrame` | Original columns plus selected metric columns |

## Notes

- `metrics` accepts a subset of: `area`, `length`, `centroid`, `bounds`,
  `geom_type`, `num_points`, `is_valid`.
- Area and length are computed in `projected_crs` (default Web Mercator,
  EPSG:3857) for real-world units. For survey-grade results, set
  `projected_crs` to the appropriate UTM zone.
- `area_unit` and `length_unit` control the output column suffix and
  scaling: e.g. `area_sq_miles`, `length_meters`.
- Centroid coordinates are returned in the input CRS (default EPSG:4326).
