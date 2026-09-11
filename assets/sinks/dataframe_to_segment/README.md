# DataFrame to Segment

Reverse-ETL sink — batch-POST DataFrame rows to Segment via
`POST /v1/batch`. The resource auto-packs ops under Segment's 500KB
per-batch payload limit.

Pairs with **`SegmentResourceComponent`** for shared auth.

## Two modes (via `mode`)

- **`identify`** (default) — set/update user traits (profile sync)
- **`track`** — record events (event stream)

## Two source shapes

- `upstream_asset_key` — read from another Dagster asset
- `source: {kind: sql | csv | inline, ...}` — inline source config

## Identifier semantics

Each op must carry `userId` OR `anonymousId`. Configure via
`user_id_column` (preferred) and/or `anonymous_id_column`. Rows without
any identifier are silently dropped.

## Examples

**Identify (traits sync):**

```yaml
type: dagster_community_components.DataframeToSegmentComponent
attributes:
  asset_name: segment_daily_identify_sync
  upstream_asset_key: dim_customers_current
  resource_key: segment
  mode: identify
  user_id_column: customer_id
  traits_map:
    email: email
    first: firstName
    last: lastName
    ltv: lifetimeValueUsd
    plan: plan
  freshness_cron: "0 8 * * *"
```

**Track (event stream):**

```yaml
type: dagster_community_components.DataframeToSegmentComponent
attributes:
  asset_name: segment_daily_track
  upstream_asset_key: fct_activity_daily
  resource_key: segment
  mode: track
  user_id_column: user_id
  event_column: event_name
  timestamp_column: event_ts       # coerced to ISO 8601
  properties_map:
    plan: plan
    revenue: revenue
    referrer: referrer
  partition_type: daily
  partition_start: "2026-01-01"
```

## Materialization metadata

- `rows_total`, `ops_sent`, `ops_failed`
- `batches`, `mode`, `dry_run`
