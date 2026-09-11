# DataFrame to Iterable

Reverse-ETL sink — batch-POST DataFrame rows to Iterable via the bulk
endpoints. Delegates all wire concerns to `IterableResource`.

Pairs with **`IterableResourceComponent`** for shared auth.

## Two modes (via `mode`)

- **`users`** (default) — `/api/users/bulkUpdate` (up to 1,000 users
  per call). Each row is one user. Uses `userId` + `email` for
  identification; extra fields land under `dataFields`.
- **`events`** — `/api/events/trackBulk` (up to 1,000 events per call).
  Each row is one event with an `eventName`. Extra fields land under
  `dataFields`.

## Two source shapes

- `upstream_asset_key` — read from another Dagster asset
- `source: {kind: sql | csv | inline, ...}` — inline source config

## Examples

**Users bulk-upsert:**

```yaml
type: dagster_community_components.DataframeToIterableComponent
attributes:
  asset_name: iterable_daily_user_sync
  upstream_asset_key: dim_customers_current
  resource_key: iterable
  mode: users
  email_column: email
  user_id_column: customer_id
  prefer_user_id: true
  fields_map:
    first: firstName          # → dataFields.firstName
    last: lastName
    ltv: lifetime_value_usd
    segment: segment
  freshness_cron: "0 8 * * *"
```

**Events bulk-track:**

```yaml
type: dagster_community_components.DataframeToIterableComponent
attributes:
  asset_name: iterable_order_events
  upstream_asset_key: fct_orders_daily
  resource_key: iterable
  mode: events
  event_name_column: event_type
  email_column: customer_email
  campaign_id_column: campaign_id     # optional
  fields_map:
    revenue: revenue
    product_id: productId
    quantity: quantity
  partition_type: daily
  partition_start: "2026-01-01"
```

## Identifier semantics

Each row must have `userId` OR `email` (or both). With
`prefer_user_id: true`, Iterable disambiguates via `userId` when both
are present. Rows without any identifier are silently dropped.

## Materialization metadata

- `rows_total`, `rows_sent`, `rows_failed`, `soft_errors` (per-row
  failures reported in Iterable's response body — `invalidEmails` for
  users, `disallowedEventNames` for events)
- `batches`, `mode`, `dry_run`
