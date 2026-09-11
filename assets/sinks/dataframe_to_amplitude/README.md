# DataFrame to Amplitude

Reverse-ETL sink — batch-POST DataFrame rows as Amplitude events via
the HTTP V2 API (`POST /2/httpapi`, up to 1,000 events per call).
Delegates wire concerns to `AmplitudeResource`.

Pairs with **`AmplitudeResourceComponent`** for shared auth.

## Two source shapes (pick one)

- `upstream_asset_key` — read from another Dagster asset
- `source: {kind: sql | csv | inline, ...}` — inline source config

## Identifier semantics

Each event MUST carry `user_id` OR `device_id` (or both). Rows lacking
both are silently dropped. Configure via `user_id_column` and/or
`device_id_column`.

## Event type

Set `event_type_column` (per-row) OR `default_event_type` (static
fallback for rows where the column is null). One must be present.

## Property layout

- `event_properties_map: {src_col: prop_key}` — lands under `event_properties`
- `user_properties_map: {src_col: prop_key}` — lands under `user_properties`
  (Amplitude merges these into the user's profile on ingest)
- Legacy pass-through: `event_property_columns` — a list of source columns
  that all become event_properties (used only when neither map is set)

## Example

```yaml
type: dagster_community_components.DataframeToAmplitudeComponent
attributes:
  asset_name: amplitude_daily_event_export
  upstream_asset_key: fct_activity_daily
  resource_key: amplitude

  event_type_column: event_name
  user_id_column: user_id
  device_id_column: device_id           # fallback when user_id is null
  time_column: event_ts                  # coerced to unix ms
  insert_id_column: event_id             # dedup key (Amplitude drops repeats for 7d)

  event_properties_map:
    plan: plan
    revenue: revenue_usd
    referrer: referrer
  user_properties_map:
    email: email
    segment: segment

  partition_type: daily
  partition_start: "2026-01-01"
  freshness_cron: "0 * * * *"
```

## Materialization metadata

- `rows_total`, `events_sent`, `events_failed`, `events_ingested`
  (Amplitude's `events_ingested` count from the response body)
- `batches`, `dry_run`
