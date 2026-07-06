# cube_query_asset

Run a query against a **Cube** semantic-layer server ([cube.dev](https://cube.dev/)) and materialize the result as a Dagster asset (pandas DataFrame). Puts governed metrics into your Dagster catalog with lineage, freshness, and downstream deps.

## Why this exists

Cube is a semantic layer — you define metrics ONCE in Cube's schema (a "cube" model like `Orders.count`, `Orders.totalAmount`), and every consumer (BI, LLM, ML pipeline, dashboard) gets the same numbers. Dagster's role is orchestration: fetch the metric on a schedule, land it in the warehouse, chain it to downstream data assets, or hand it to a reverse-ETL sink.

## Example

Local Cube dev-server (no auth):

```yaml
type: dagster_community_components.CubeQueryAssetComponent
attributes:
  asset_name: cube_orders_by_status
  api_url_env_var: CUBE_URL   # http://localhost:4000
  query:
    measures:
      - Orders.count
      - Orders.totalAmount
    dimensions:
      - Orders.status
    timeDimensions:
      - dimension: Orders.createdAt
        granularity: day
        dateRange: ["2024-01-01", "2024-12-31"]
    order:
      Orders.createdAt: asc
```

Cube Cloud (JWT auth):

```yaml
attributes:
  # ...
  api_url_env_var: CUBE_API_URL       # https://<tenant>.cubecloud.dev/cubejs-api
  api_token_env_var: CUBE_API_TOKEN   # JWT — the component prepends "Bearer " automatically
```

## Fields

| Field | Type | Description |
|---|---|---|
| `asset_name` | string | Output asset name. |
| `query` | dict | Cube JSON query. Full shape at https://cube.dev/docs/product/apis-integrations/rest-api/reference. |
| `api_url_env_var` | string | Env var with Cube base URL. Default `CUBE_URL`. |
| `api_token_env_var` | string | Optional. JWT env var for Cube Cloud / production. Omit for dev. |
| `request_timeout_seconds` | int | Default 60. |
| `group_name` / `description` / `owners` / `tags` / `kinds` / `deps` | — | Standard catalog fields. |

## Query shape

Cube's REST v1 query is a JSON dict:

```yaml
query:
  measures: ["Orders.count", "Orders.totalAmount"]
  dimensions: ["Orders.status", "Customers.plan"]
  timeDimensions:
    - dimension: Orders.createdAt
      granularity: day     # or week / month / quarter / year
      dateRange: ["2024-01-01", "2024-12-31"]
  filters:
    - member: Orders.status
      operator: equals
      values: ["completed"]
  order:
    Orders.createdAt: asc
  limit: 10000
  offset: 0
```

Standard SQL-ish semantics: measures aggregate; dimensions group; timeDimensions add a time-bucketed grouping.

## Materialization metadata

- `cube_url` (clickable)
- `cube_measures`, `cube_dimensions` (JSON)
- `cube_row_count`
- `preview` — first 10 rows rendered as a Markdown table
- `columns` — list of column names in the returned DataFrame
- `cube_annotation` — Cube's per-column metadata (title, shortTitle, type, format) for downstream tools

## Requirements

```
requests>=2.28
pandas>=1.5.0
tabulate>=0.9.0
```

## Related

- [`external_cube_metric`](../../../external_assets/external_cube_metric) — declare-only external asset for surfacing Cube metrics in the Dagster catalog without executing.
- Pair with `langchain_chain_asset` for "natural-language question → Cube JSON → LLM summary" flows.
- Pair with `dataframe_to_snowflake` / `dataframe_to_bigquery` etc. to land Cube metrics as warehouse tables.
