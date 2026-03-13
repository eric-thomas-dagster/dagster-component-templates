# GraphQL Asset Component

Executes one or more GraphQL queries against an API, flattens the JSON responses with `pandas.json_normalize`, and writes results to a destination database via SQLAlchemy — with optional cursor-based or page-based pagination.

---

## What it does

1. For each entry in `queries`, POSTs `{"query": ..., "variables": ...}` to the GraphQL endpoint.
2. Navigates the `data_path` (e.g. `data.repository.issues.nodes`) to extract the result array.
3. Flattens nested JSON with `pandas.json_normalize`.
4. Writes to a table named `{asset_name}_{query_name}` via SQLAlchemy.
5. If `paginate: true`, loops using `pageInfo.endCursor` / `pageInfo.hasNextPage` for cursor pagination, or an integer `page` variable as fallback.

---

## Required packages

| Package | Minimum version | Purpose |
|---------|----------------|---------|
| `dagster` | 1.8.0 | Orchestration framework |
| `requests` | 2.31.0 | HTTP requests to GraphQL endpoint |
| `pandas` | 1.3.0 | Flatten nested JSON responses |
| `sqlalchemy` | 2.0.0 | Database connectivity |

Install with:

```bash
pip install requests>=2.31.0 pandas>=1.3.0 sqlalchemy>=2.0.0
```

You will also need the appropriate SQLAlchemy dialect driver (e.g. `psycopg2` for PostgreSQL, `pymysql` for MySQL).

---

## Configuration fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | `str` | — | Dagster asset key name. Also used as the table name prefix. |
| `endpoint_env_var` | `str` | — | Env var holding the GraphQL endpoint URL. |
| `api_key_env_var` | `str \| null` | `null` | Env var holding the API key. Omit to disable auth. |
| `auth_header` | `str` | `Authorization` | HTTP header name for the API key. |
| `auth_prefix` | `str` | `Bearer` | Prefix before the key (e.g. `"Bearer"`). Set to `""` for raw key. |
| `queries` | `list` | — | List of query configs (see below). |
| `database_url_env_var` | `str` | — | Env var holding the SQLAlchemy database URL. |
| `target_schema` | `str \| null` | `null` | Database schema for output tables. |
| `if_exists` | `str` | `replace` | `"replace"`, `"append"`, or `"fail"`. |
| `paginate` | `bool` | `false` | Enable automatic pagination. |
| `page_size` | `int` | `100` | Records per page when paginating. |
| `max_pages` | `int` | `100` | Maximum pages per query as a safety cap. |
| `group_name` | `str \| null` | `graphql` | Dagster asset group name shown in the UI. |
| `deps` | `list \| null` | `null` | Upstream asset keys for lineage. |

### Query config fields

Each item in `queries` supports:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | `str` | yes | Suffix for the output table name: `{asset_name}_{name}`. |
| `query` | `str` | yes | The GraphQL query string. |
| `variables` | `dict \| null` | no | Static variables passed with the query. |
| `data_path` | `str \| null` | no | Dot-separated path to the result array, e.g. `data.users`. |

---

## Pagination

When `paginate: true`:

1. **Cursor-based** (preferred): the component looks for `pageInfo.hasNextPage` and `pageInfo.endCursor` anywhere in the response. If found, it passes `cursor` as a variable on the next request and stops when `hasNextPage` is false.
2. **Integer page** (fallback): if no `pageInfo` is found, the component increments a `page` variable starting at 1 and stops when a page returns fewer records than `page_size`.

Both strategies respect `max_pages` as a hard cap.

---

## Example YAML

### GitHub issues with cursor pagination

```yaml
type: dagster_component_templates.GraphQLAssetComponent
attributes:
  asset_name: github_data
  endpoint_env_var: GITHUB_GRAPHQL_URL
  api_key_env_var: GITHUB_TOKEN
  database_url_env_var: DATABASE_URL
  queries:
    - name: issues
      query: |
        query($owner: String!, $repo: String!, $cursor: String) {
          repository(owner: $owner, name: $repo) {
            issues(first: 100, after: $cursor) {
              nodes { number title state createdAt }
              pageInfo { endCursor hasNextPage }
            }
          }
        }
      variables:
        owner: my-org
        repo: my-repo
      data_path: data.repository.issues.nodes
  paginate: true
  page_size: 100
  group_name: github
  deps:
    - raw/repositories
```

### Multiple queries from the same API

```yaml
type: dagster_component_templates.GraphQLAssetComponent
attributes:
  asset_name: shopify_data
  endpoint_env_var: SHOPIFY_GRAPHQL_URL
  api_key_env_var: SHOPIFY_TOKEN
  database_url_env_var: DATABASE_URL
  target_schema: raw_shopify
  queries:
    - name: orders
      query: |
        { orders(first: 250) { edges { node { id name totalPrice createdAt } } } }
      data_path: data.orders.edges
    - name: products
      query: |
        { products(first: 250) { edges { node { id title vendor status } } } }
      data_path: data.products.edges
  if_exists: replace
  group_name: shopify
```

---

## Required environment variables

| Variable | Description |
|----------|-------------|
| `GITHUB_GRAPHQL_URL` (or custom) | Full GraphQL endpoint URL, e.g. `https://api.github.com/graphql` |
| `GITHUB_TOKEN` (or custom) | API key / personal access token |
| `DATABASE_URL` (or custom) | SQLAlchemy connection URL for the destination database |

---

## Materialization metadata

| Key | Description |
|-----|-------------|
| `total_rows` | Total rows written across all queries |
| `queries_executed` | Number of queries that ran |
| `per_query_rows` | JSON object mapping each query name to its row count |
