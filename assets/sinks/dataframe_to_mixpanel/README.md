# DataFrame to Mixpanel

Reverse-ETL sink — batch-POST DataFrame rows to Mixpanel via the bulk
endpoints. Delegates all wire concerns to `MixpanelResource`.

Pairs with **`MixpanelResourceComponent`** for shared auth.

## Two modes (via `mode`)

- **`events`** (default) — `/import` (up to 2,000 events per call,
  requires service-account HTTP-Basic auth). Each row becomes one
  Mixpanel event with `distinct_id`, optional `time` (unix seconds),
  optional `$insert_id` (dedup key).
- **`profiles`** — `/engage#profile-set` (up to 50 profile ops per call,
  uses project-token auth). Each row becomes one `$distinct_id` + `$set`
  op.

## Two source shapes

- `upstream_asset_key` — read from another Dagster asset
- `source: {kind: sql | csv | inline, ...}` — inline source config

## Examples

**Events bulk-import:**

```yaml
type: dagster_community_components.DataframeToMixpanelComponent
attributes:
  asset_name: mixpanel_daily_event_import
  upstream_asset_key: fct_activity_daily
  resource_key: mixpanel
  mode: events
  event_column: event_name
  distinct_id_column: user_id
  time_column: event_ts                  # → unix seconds
  insert_id_column: event_id             # dedup ($insert_id)
  properties_map:
    plan: plan
    revenue: revenue_usd
    referrer: referrer
  partition_type: daily
  partition_start: "2026-01-01"
```

**Profiles bulk-set:**

```yaml
type: dagster_community_components.DataframeToMixpanelComponent
attributes:
  asset_name: mixpanel_daily_profile_sync
  upstream_asset_key: dim_customers_current
  resource_key: mixpanel
  mode: profiles
  distinct_id_column: user_id
  properties_map:
    first: $first_name       # Mixpanel reserved profile prop
    last: $last_name
    email: $email
    ltv: lifetime_value_usd  # custom prop
    segment: segment
  freshness_cron: "0 8 * * *"
```

## Identifier semantics

`distinct_id_column` is required for both modes. Rows without a
`distinct_id` are silently dropped. For events mode, rows without a
resolvable event name (from `event_column` or `default_event`) are also
dropped.

## Materialization metadata

- `rows_total`, `rows_sent`, `rows_failed`
- `batches`, `mode`, `dry_run`
- `num_records_imported` (events mode only — Mixpanel's response count)
