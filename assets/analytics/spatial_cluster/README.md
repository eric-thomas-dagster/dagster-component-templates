# spatial_cluster

Cluster geographic points in a DataFrame using DBSCAN (density-based, handles arbitrary shapes and noise) or K-means (fixed number of spherical clusters). DBSCAN uses the haversine metric with a configurable distance threshold in km. Cluster labels are written to a configurable output column; noise points receive -1 in DBSCAN mode. Optionally adds cluster centroid lat/lng columns.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame with lat/lng columns |
| `lat_column` | `str` | `"latitude"` | Column name containing latitude values |
| `lng_column` | `str` | `"longitude"` | Column name containing longitude values |
| `algorithm` | `str` | `"dbscan"` | Clustering algorithm: `dbscan` or `kmeans` |
| `eps_km` | `float` | `1.0` | DBSCAN: max distance in km between neighborhood points |
| `min_samples` | `int` | `3` | DBSCAN: minimum points to form a dense region |
| `n_clusters` | `int` | `5` | K-means: number of clusters |
| `output_column` | `str` | `"spatial_cluster"` | Column name for cluster labels |
| `include_cluster_center` | `bool` | `false` | Add `cluster_center_lat` and `cluster_center_lng` columns |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Example YAML

```yaml
component_type: spatial_cluster
description: Cluster store visit events by geographic proximity using DBSCAN.

asset_name: store_visits_clustered
upstream_asset_key: store_visits
lat_column: latitude
lng_column: longitude
algorithm: dbscan
eps_km: 0.5
min_samples: 5
output_column: spatial_cluster
include_cluster_center: true
group_name: geospatial
```

## Metadata Logged

- `rows` — Total input rows
- `algorithm` — Clustering algorithm used
- `unique_clusters` — Number of distinct clusters found (excluding noise)
- `cluster_sizes` — Row count per cluster label

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Requirements

- `dagster`
- `pandas`
- `scikit-learn`
- `numpy`
