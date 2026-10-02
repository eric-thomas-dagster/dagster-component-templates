# `ZocdocAvailabilityUpsertComponent`

Reverse-ETL: push warehouse-computed provider availability/timeslots into Zocdoc's **provider-scheduling surface** via:

```
PUT /v1/providers/{provider_id}/calendar/timeslots?date=YYYY-MM-DD
```

Follows `salesforce_record_upsert`'s dual-source pattern (`upstream_asset_key` OR inline `source:`).

## PHI Safety

**Read this before pointing this component at a real, patient-connected Zocdoc practice.**

This component writes *availability* (open timeslots) — it never reads or writes patient data. But the write payload still carries operational schedule detail (visit-reason restrictions, patient-type eligibility), and this repo treats everything touching a PHI-adjacent system conservatively:

1. **No data preview, anywhere, under any configuration.** There is no `include_preview_metadata`/preview field in this component at all — the capability does not exist in the code.
2. **Row/group counts only.** Materialization metadata exposes exactly: `rows_processed`, `groups_processed`, `rows_upserted`, `rows_skipped_no_key`, `rows_errored`, `groups_oversized` (all integers), plus — only on failure — a `first_errors` list containing bare `provider_id`/`date` identifiers (operational/provider-schedule identifiers, never patient data) and an HTTP status code. No timeslot field values (start times, visit-reason ids, patient-type) ever appear in metadata.
3. **Sanitized errors.** The one external HTTP call this component makes is isolated in the module-level `_zocdoc_put_timeslots` function. On a non-2xx response it raises `ZocdocAPIError`, carrying **only the HTTP status code and a fixed, generic description** — never the request body (the actual timeslot list) or response body.

This design is verified by `tests/test_phi_safety.py`.

## Critical semantics: full replace, not additive — read before configuring

Verified from api-docs.zocdoc.com's "Create Timeslots" guide:

- **"Subsequent requests for a single date will override previous data"** and **"All open timeslots must be included in each request per date."** This is NOT an incremental/additive upsert — every `PUT` call for a given `(provider_id, date)` must include *every* open slot you want to exist for that provider on that date, or the omitted ones disappear.
- **"Submitting an empty request body will remove all slots for the given date."**
- Zocdoc accepts **0–1500 timeslot items per request**. Because the call is a full replace, a `(provider_id, date)` group that exceeds 1500 rows **cannot be safely split across multiple PUT calls** — the second call would wipe the first's slots. This component detects that case and **refuses to call the API for that group**, reporting it under `groups_oversized` instead of silently corrupting that day's calendar.

This component groups upstream rows by their mapped `(provider_id, date)` pair and issues exactly **one `PUT` per group** — never one per row.

## `fields_map`

Upstream column → Zocdoc timeslot field. Values must include all of the required targets, plus any of the optional ones you need:

| Target (required) | Meaning |
|---|---|
| `provider_id` | Zocdoc provider id — also the grouping key (one PUT call per distinct value). |
| `date` | `YYYY-MM-DD` — also the grouping key. |
| `location_id` | Zocdoc location id for the slot. |
| `start_time` | ISO-8601 datetime. |
| `time_zone` | IANA timezone string (e.g. `America/New_York`). |

| Target (optional) | Meaning |
|---|---|
| `allowed_visit_reason_ids` | Comma-separated string or list, per row. |
| `excluded_visit_reason_ids` | Comma-separated string or list, per row. |
| `patient_type` | `new` \| `established` \| `all`. |

Validated at `build_defs` time — missing a required target, or mapping to an unrecognized target, raises `ValueError` immediately.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | — (required) | Output Dagster asset name. |
| `upstream_asset_key` | `Optional[str]` | `None` | Upstream asset providing the DataFrame. Mutually exclusive with `source:`. |
| `source` | `Optional[Dict]` | `None` | Inline source config (`sql` / `csv` / `inline`). Mutually exclusive with `upstream_asset_key`. |
| `resource_key` | `str` | `"zocdoc_resource"` | Resource key registered by `ZocdocResourceComponent`. |
| `fields_map` | `Dict[str, str]` | — (required) | See above. |
| `batch_size` | `int` | `5000` | Max upstream rows per run (safety cap). |
| `group_name` / `description` / `owners` / `tags` / `kinds` | — | — | Standard catalog fields. |

## Example

```yaml
type: dagster_component_templates.ZocdocAvailabilityUpsertComponent
attributes:
  asset_name: zocdoc_availability_sync
  upstream_asset_key: dbt_marts_provider_open_slots
  resource_key: zocdoc_resource
  fields_map:
    zocdoc_provider_id: provider_id
    slot_date: date
    zocdoc_location_id: location_id
    slot_start_time: start_time
    slot_time_zone: time_zone
    visit_reason_ids: allowed_visit_reason_ids
    patient_type: patient_type
  batch_size: 5000
```

## Behavior + gotchas

- **Group = `(provider_id, date)`, not row.** Every row sharing the same mapped `provider_id` and `date` is sent in a single `PUT`. If your upstream only contains a subset of a provider's open slots for a date that already has other slots live in Zocdoc, this call **will delete the ones you didn't include**.
- **Rows missing any required field are skipped** (`rows_skipped_no_key`), never sent with a null/placeholder value.
- **Oversized groups (>1500 slots) are never partially written.** They're skipped wholesale and counted under `groups_oversized`, with the `provider_id`/`date` (not slot content) reported in `first_errors`.
- **No delete operation exposed directly** — the only way to remove a day's slots through this component is to upsert an empty group for it, which isn't currently reachable since groups are derived from non-empty upstream rows. To explicitly clear a day, call Zocdoc directly or extend this component deliberately — don't infer deletion from absence.

## Pairs with

- `zocdoc_resource` — OAuth2 client_credentials connection (required).
- `zocdoc_appointments_ingestion` — the read-side counterpart (booked appointments → DataFrame).
