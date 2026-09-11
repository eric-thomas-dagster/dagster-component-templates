# DataFrame to Customer.io

Reverse-ETL sink — batch-POST DataFrame rows to Customer.io via the
v2 batch endpoint (`POST /api/v2/batch`, up to 100 operations per call).
Delegates wire concerns to `CustomerIoResource`.

Pairs with **`CustomerIoResourceComponent`** for shared auth.

## Two modes (via `mode`)

- **`identify`** (default) — upsert customer profile attributes. Each
  row becomes one `identify` op with `identifiers` + `attributes`.
- **`event`** — bulk-track events. Each row becomes one `event` op with
  `identifiers` + `name` + optional `data` + optional `timestamp`.

## Two source shapes

- `upstream_asset_key` — read from another Dagster asset
- `source: {kind: sql | csv | inline, ...}` — inline source config

## Identifier semantics

Each op must carry `id`, `email`, or `cio_id`. Configure via
`id_column` (preferred), `email_column`, `cio_id_column`. Rows without
any resolvable identifier are silently dropped.

## Examples

**Profile sync (identify):**

```yaml
type: dagster_community_components.DataframeToCustomerIoComponent
attributes:
  asset_name: customer_io_daily_profile_sync
  upstream_asset_key: dim_customers_current
  resource_key: customer_io
  mode: identify
  id_column: customer_id
  email_column: email
  fields_map:
    first: first_name        # → attributes.first_name
    last: last_name
    ltv: lifetime_value_usd
    segment: segment
  freshness_cron: "0 8 * * *"
```

**Event tracking:**

```yaml
type: dagster_community_components.DataframeToCustomerIoComponent
attributes:
  asset_name: customer_io_purchase_events
  upstream_asset_key: fct_purchases_daily
  resource_key: customer_io
  mode: event
  id_column: customer_id
  event_name_column: event_type
  event_timestamp_column: event_ts  # coerced to unix seconds
  fields_map:
    revenue: revenue
    product_id: product_id
  partition_type: daily
  partition_start: "2026-01-01"
```

## Materialization metadata

- `rows_total`, `ops_sent`, `ops_failed`, `soft_errors` (per-op errors
  reported in Customer.io's response body)
- `batches`, `mode`, `dry_run`
