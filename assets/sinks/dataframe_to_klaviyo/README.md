# DataFrame to Klaviyo

Reverse-ETL sink — batch-POST DataFrame rows to Klaviyo as profile
updates via the async bulk-import job endpoint
(`/api/profile-bulk-import-jobs`, up to 10,000 profiles per job).

Pairs with **`KlaviyoResourceComponent`** for shared auth.

## Two source shapes (pick one)

- `upstream_asset_key` — read from another Dagster asset
- `source: {kind: sql | csv | inline, ...}` — inline source config

## Field layout

Klaviyo profiles have a fixed set of top-level "reserved" attributes
(`email`, `phone_number`, `external_id`, `first_name`, `last_name`,
`organization`, `title`, `image`, `location`). Everything else must be
sent under `properties` (Klaviyo's custom-attribute bag).

The sink handles this automatically:

- Values from `email_column` / `phone_column` / `external_id_column`
  land on the reserved keys.
- Values from `fields_map` land at top-level if the target is a reserved
  key, else under `properties`.
- Legacy pass-through (`property_columns`) works the same way.

Every profile must carry at least one of `email` / `phone_number` /
`external_id` — rows without any identifier are silently dropped.

## Example

```yaml
type: dagster_community_components.DataframeToKlaviyoComponent
attributes:
  asset_name: klaviyo_daily_profile_sync
  upstream_asset_key: dim_customers_current
  resource_key: klaviyo
  email_column: email
  external_id_column: customer_id
  fields_map:
    first: first_name         # reserved → top level
    last: last_name
    ltv: lifetime_value_usd   # custom → nested under `properties`
    segment: segment
  list_id: "abc123"           # optional Klaviyo list to add profiles to
  freshness_cron: "0 8 * * *"
  partition_type: daily
  partition_start: "2026-01-01"
```

## Materialization metadata

- `rows_total`, `profiles_sent`, `profiles_failed`
- `batches` — number of bulk-import jobs POSTed
- `job_ids` — list of Klaviyo job ids; poll via `GET /api/profile-bulk-import-jobs/{id}` if you need to confirm completion
- `list_id`, `dry_run`
