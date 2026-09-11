# DataFrame to Braze

Reverse-ETL sink — batch-POST DataFrame rows to Braze's REST API for
customer activation flows. Each row becomes a user attribute update or a
custom catalog item.

Two endpoints supported (pick via `endpoint`):

- **`users_track`** (default) — POSTs to `/users/track`. Each row is one
  user; columns become `attributes` and/or `custom_attributes` on that
  user's Braze profile. Batches up to **75** users per call.
- **`catalogs`** — POSTs to `/catalogs/{name}/items`. Each row becomes
  a catalog item (id + fields). Batches up to **50** items per call.

## Configuration

| Field | Required | Default | What |
|---|---|---|---|
| `asset_name` | yes | — | Dagster asset name |
| `upstream_asset_key` | yes | — | Asset key of the upstream DataFrame |
| `api_key_env_var` | | `BRAZE_API_KEY` | Env var holding the REST API key |
| `rest_endpoint` | yes | — | Braze REST endpoint URL (region-specific — `https://rest.iad-01.braze.com`, `https://rest.iad-02.braze.com`, `https://rest.fra-01.braze.com`, etc.) |
| `endpoint` | | `users_track` | `users_track` or `catalogs` |
| `catalog_name` | if `catalogs` | | Braze catalog name |
| `user_id_column` | | `external_id` | For `users_track`: column with user's external_id (falls back to `braze_id`) |
| `attribute_columns` | | (all others) | Top-level user attributes |
| `custom_attribute_columns` | | | Nested under `custom_attributes` on the user profile |
| `item_id_column` | | `id` | For `catalogs`: column with the item's id |
| `batch_size` | | 75 or 50 | Rows per HTTP request |
| `request_timeout_seconds` | | `30` | Per-request timeout |
| `dry_run` | | `false` | Build payloads but skip the HTTP POST |
| `group_name`, `description`, `owners`, `asset_tags`, `kinds`, `freshness_*`, `retry_policy_*`, `partition_*` | | | Standard Dagster asset config |

## Example: warehouse → Braze user-attribute sync

```yaml
type: dagster_community_components.DataframeToBrazeComponent
attributes:
  asset_name: braze_customer_segment_export
  upstream_asset_key: fct_quotes_daily        # dbt fact table upstream

  api_key_env_var: BRAZE_API_KEY
  rest_endpoint: https://rest.iad-01.braze.com

  endpoint: users_track
  user_id_column: external_id
  attribute_columns:
    - email
    - first_name
    - last_name
    - last_quote_at
  custom_attribute_columns:
    - quote_count_30d
    - segment
    - churn_risk_score

  batch_size: 75
  freshness_cron: "0 8 * * *"
  partition_type: daily
  partition_start: "2026-01-01"
```

## Example: warehouse → Braze catalog upsert

```yaml
type: dagster_community_components.DataframeToBrazeComponent
attributes:
  asset_name: braze_product_catalog_sync
  upstream_asset_key: dim_products_current

  api_key_env_var: BRAZE_API_KEY
  rest_endpoint: https://rest.fra-01.braze.com

  endpoint: catalogs
  catalog_name: products
  item_id_column: sku
  batch_size: 50

  freshness_cron: "0 */4 * * *"
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
      "custom_attributes": {
        "quote_count_30d": 4,
        "segment": "high_intent"
      }
    }
  ]
}
```

**`catalogs`** — Braze's [`/catalogs/{name}/items`](https://www.braze.com/docs/api/endpoints/catalogs):

```json
{
  "items": [
    { "id": "sku-001", "name": "T-Shirt Red", "price_usd": 24.99 }
  ]
}
```

## Materialization metadata

Each run attaches:

- `rows_total`, `rows_sent`, `rows_failed`
- `batches` — number of HTTP requests
- `endpoint`, `rest_endpoint`, `batch_size`
- `dry_run` — bool

Watch `rows_failed` for rate-limit or auth issues. Set
`retry_policy_max_retries` on the asset so transient errors don't fail
the whole materialization.

## Auth notes

Braze REST API keys are scoped per capability:

- `users.track` — needed for `endpoint: users_track`
- `catalogs.<name>.update_items` — needed for `endpoint: catalogs`

Create keys in the Braze dashboard under Settings → REST API Keys with
only the scopes you need.
