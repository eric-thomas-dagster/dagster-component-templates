# external_cube_metric

Declare a Cube semantic-layer metric (measure OR dimension) as an external Dagster asset. Cube owns the definition; Dagster puts it in the catalog with lineage.

## Example

```yaml
type: dagster_community_components.ExternalCubeMetricAsset
attributes:
  asset_key: cube/orders/count
  cube_name: Orders
  measure_name: Orders.count
  metric_type: count
  cube_playground_url: http://localhost:4000/#/build
```

For dimensions:

```yaml
attributes:
  asset_key: cube/orders/status
  cube_name: Orders
  dimension_name: Orders.status
  metric_type: string
```

## Fields

| Field | Type | Description |
|---|---|---|
| `asset_key` | string | Dagster asset key, `/`-separated. |
| `cube_name` | string | Cube name (e.g. `Orders`, `Customers`). |
| `measure_name` | string | Cube measure like `Orders.count`. Set this OR `dimension_name`. |
| `dimension_name` | string | Cube dimension like `Orders.status`. Set this OR `measure_name`. |
| `metric_type` | string | `count` / `sum` / `avg` / `string` / `time` / `number` / `boolean`. |
| `cube_playground_url` | string | Clickable link to the metric in Cube Playground. |
| `cube_docs_url` | string | Link to the schema file / docs. |
| `group_name` / `description` / `owners` / `tags` / `kinds` / `deps` | — | Standard catalog fields. |

## Related

- [`cube_query_asset`](../../assets/analytics/cube_query_asset) — execute Cube queries and materialize DataFrames.
