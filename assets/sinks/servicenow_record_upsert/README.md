# `ServiceNowRecordUpsertComponent`

Reverse-ETL sink: mirror an upstream DataFrame into a **ServiceNow table** via the Table API. Search-then-write per row — ServiceNow has no native upsert endpoint.

For each upstream row:
1. `GET /api/now/table/{table}?sysparm_query={key_field}={value}` — look up existing record.
2. If found → `PATCH /api/now/table/{table}/{sys_id}` (partial update, unchanged fields preserved).
3. If not found → `POST /api/now/table/{table}` (new record).

## When to use

- Sync operational data from a warehouse / dbt marts back INTO ServiceNow — CMDB items, incidents, change requests, catalog items.
- Mirror alert-monitoring output into ServiceNow incidents so on-call sees them via the standard queue.
- Push a computed field (SLA at-risk flag, business-critical tag) into ServiceNow so it's visible to the ITIL workflow.

## Pairs with

- **`servicenow_resource`** — connection + auth (required). Provides the workhorse HTTP client with retry.
- **`servicenow_ingestion`** — the READ-side counterpart (Table API → warehouse via dlt).

Together they cover **both directions** of ServiceNow data movement.

## Example

```yaml
type: dagster_component_templates.ServiceNowRecordUpsertComponent
attributes:
  asset_name: servicenow_incidents_from_alerts
  upstream_asset_key: alerts_to_incidents
  resource_key: servicenow_resource

  table: incident
  key_field: correlation_id           # must be in fields_map values
  fields_map:
    alert_id: correlation_id          # upstream col → ServiceNow field
    title: short_description
    details: description
    severity: impact
  batch_size: 500
```

## Behavior + gotchas

- **Uniqueness** — `key_field` should be indexed unique on the target ServiceNow table. If two Dagster runs race on the same key with no unique index, both can create separate records. Add a Unique index via *System Definition → Dictionary → {table}.{field}*.
- **Blank vs missing** — literal `""` string values are sent verbatim (ServiceNow stores as blank). `None` / `NaN` values are omitted from the request body so the existing ServiceNow value is preserved.
- **Rate limits** — ServiceNow's default rate limit on developer instances is 60 req/min for basic auth. Each row triggers 1-2 requests (find + optional PATCH/POST). The resource retries on 429/5xx with exponential backoff up to `max_retries` (default 3).
- **In-run cache** — records created earlier in the same run are cached by key so duplicate upstream rows (or eventual-consistency lag on ServiceNow's search index) don't double-post.
- **key_field must be in fields_map** — the sink validates at build time that `key_field` appears in `fields_map` values. Otherwise the upsert can't propagate the match key to the ServiceNow row.

## Related

- `servicenow_resource` — connection + workhorse (read + write methods).
- `servicenow_ingestion` — read side (dlt-backed).
- `servicenow_sensor` — event-driven trigger for downstream Dagster work on ServiceNow state changes.
