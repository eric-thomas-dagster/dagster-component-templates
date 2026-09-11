# DataFrame to Braze

Reverse-ETL sink — batch-POST rows to Braze's REST API for customer
activation flows. Each row becomes a user attribute update or a custom
catalog item.

## Two source shapes (pick one)

- **`upstream_asset_key`** — read a DataFrame from another Dagster asset
- **`source:`** — inline config, one of:
  - `{kind: sql, resource_key | database_url_env_var, query}` — SQL query
  - `{kind: csv, path, read_csv_kwargs}` — CSV file
  - `{kind: inline, rows: [...]}` — literal rows

## Two Braze endpoints (pick via `endpoint`)

- **`users_track`** (default) — POSTs to `/users/track`. Each row is one
  user; columns become `attributes` and/or `custom_attributes` on their
  Braze profile. Batches up to **75** users per call.
- **`catalogs`** — POSTs to `/catalogs/{name}/items`. Each row becomes
  a catalog item (id + fields). Batches up to **50** items per call.

## Auth (recommended: reference a `BrazeResource`)

Prefer configuring auth on a shared `BrazeResourceComponent` and
referencing it via `resource_key`. Multiple Braze sinks in the same
project share one auth block:

```yaml
type: dagster_community_components.BrazeResourceComponent
attributes:
  resource_key: braze
  api_key_env_var: BRAZE_API_KEY
  rest_endpoint: https://rest.iad-01.braze.com
```

Then reference it on every sink:

```yaml
type: dagster_community_components.DataframeToBrazeComponent
attributes:
  resource_key: braze
  # ... rest of config
```

Inline fallback (for one-off use — sets auth on the sink directly):
`api_key_env_var: BRAZE_API_KEY` + `rest_endpoint: https://rest.iad-01.braze.com`.

## Configuration

| Field | Required | Default | What |
|---|---|---|---|
| `asset_name` | yes | — | Dagster asset name |
| `upstream_asset_key` | one-of | — | Upstream Dagster asset providing the DataFrame |
| `source` | one-of | — | Inline `{kind: sql/csv/inline, ...}` source config |
| `resource_key` | one-of | — | `BrazeResourceComponent` resource key |
| `api_key_env_var` | one-of | — | Inline auth: env var holding the REST API key |
| `rest_endpoint` | one-of | — | Inline auth: region-specific Braze REST URL |
| `endpoint` | | `users_track` | `users_track` or `catalogs` |
| `catalog_name` | if `catalogs` | | Braze catalog name |
| `fields_map` | | | Explicit `source_col: braze_field` mapping (rename columns inline) |
| `user_id_column` | | `external_id` | For `users_track`: column with user's external_id (falls back to `braze_id`) |
| `attribute_columns` | | (all others) | For `users_track` when `fields_map` isn't set: which columns become top-level attributes |
| `custom_attribute_columns` | | | Columns nested under `custom_attributes` on the user profile |
| `item_id_column` | | `id` | For `catalogs`: column with the item's id |
| `batch_size` | | 75 or 50 | Rows per HTTP request |
| `request_timeout_seconds` | | `30` | Per-request timeout (only for inline auth path) |
| `dry_run` | | `false` | Build payloads but skip the POST |
| `group_name`, `description`, `owners`, `asset_tags`, `kinds`, `freshness_*`, `retry_policy_*`, `partition_*` | | | Standard Dagster asset config |

## Column mapping — `fields_map` vs `attribute_columns`

Two ways to control what data lands on the Braze user profile:

**`fields_map` (preferred)** — explicit `source_col: braze_field` map.
Renames on the way to Braze:

```yaml
fields_map:
  email_addr: email               # db column "email_addr" -> Braze "email"
  first: first_name
  quote_count_30d: quote_count_30d
```

Only columns in the map are sent. Any column also in `custom_attribute_columns`
lands nested under `custom_attributes`.

**`attribute_columns` (legacy pass-through)** — pass columns through
unchanged (no renaming):

```yaml
attribute_columns: [email, first_name, last_name]
```

## Examples

**Warehouse → Braze user-attribute sync (upstream DataFrame + resource):**

```yaml
type: dagster_community_components.DataframeToBrazeComponent
attributes:
  asset_name: braze_customer_segment_export
  upstream_asset_key: fct_quotes_daily
  resource_key: braze
  endpoint: users_track
  fields_map:
    external_id: external_id
    email_addr: email
    first: first_name
    quote_count_30d: quote_count_30d
    segment: segment
  custom_attribute_columns: [quote_count_30d, segment]
  freshness_cron: "0 8 * * *"
  partition_type: daily
  partition_start: "2026-01-01"
```

**Direct SQL → Braze (no upstream Dagster asset):**

```yaml
type: dagster_community_components.DataframeToBrazeComponent
attributes:
  asset_name: braze_daily_reengagement_pool
  source:
    kind: sql
    resource_key: warehouse
    query: |
      SELECT customer_id AS external_id, email, first_name, segment
      FROM analytics.reengagement_pool
      WHERE cohort_date = CURRENT_DATE
  resource_key: braze
  endpoint: users_track
  custom_attribute_columns: [segment]
```

**Direct CSV → Braze (one-off / demo):**

```yaml
type: dagster_community_components.DataframeToBrazeComponent
attributes:
  asset_name: braze_backfill_from_csv
  source:
    kind: csv
    path: /data/exports/braze_backfill.csv
  resource_key: braze
  endpoint: users_track
```

**Warehouse → Braze catalog upsert:**

```yaml
type: dagster_community_components.DataframeToBrazeComponent
attributes:
  asset_name: braze_product_catalog_sync
  upstream_asset_key: dim_products_current
  resource_key: braze
  endpoint: catalogs
  catalog_name: products
  item_id_column: sku
  fields_map:
    sku: sku
    product_name: name
    price_usd: price_usd
```

## Payload shapes

**`users_track`** — Braze's [`/users/track`](https://www.braze.com/docs/api/endpoints/user_data/post_user_track):

```json
{
  "attributes": [
    {
      "external_id": "cust_123",
      "email": "alice@example.com",
      "first_name": "Alice",
      "custom_attributes": { "quote_count_30d": 4, "segment": "high_intent" }
    }
  ]
}
```

**`catalogs`** — Braze's [`/catalogs/{name}/items`](https://www.braze.com/docs/api/endpoints/catalogs):

```json
{ "items": [{ "id": "sku-001", "name": "T-Shirt Red", "price_usd": 24.99 }] }
```

## Materialization metadata

Each run attaches: `rows_total`, `rows_sent`, `rows_failed`, `batches`,
`endpoint`, `batch_size`, `dry_run`. Watch `rows_failed` for rate-limit
or auth issues.

## Related

- **`BrazeResourceComponent`** — shared auth + endpoint config (recommended)
- **`SalesforceRecordUpsertComponent`** — sibling reverse-ETL sink with a very similar shape (source/fields_map/resource pattern)
