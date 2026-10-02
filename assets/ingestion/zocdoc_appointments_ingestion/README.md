# `ZocdocAppointmentsIngestionComponent`

Fetches booked appointments for a provider/practice via Zocdoc's `GET /v1/appointments` endpoint and materializes the result as a pandas DataFrame, for operational/analytics reporting. Uses the `zocdoc_resource` component for authentication.

## PHI Safety

**Read this before pointing this component at a production, patient-connected Zocdoc practice.**

Zocdoc's appointment records are Protected Health Information: patient names, contact details, date of birth, visit reasons, and provider notes can all appear in the raw API response. Dagster's event log (materialization metadata, run logs, error messages) is a **less-trusted surface** than Zocdoc itself — broader internal audience, longer retention, and not covered by the same access controls as the source system. This component is built so none of that PHI ever lands there:

1. **No data preview, anywhere, under any configuration.** Unlike most DataFrame-producing components in this repo — several ingestion components (e.g. `gong_calls_ingestion`) optionally render a markdown `head()` of the data into materialization metadata — this component has **no `include_preview_metadata` field at all**. The capability does not exist in the code, so it cannot be misconfigured on. There is no feature flag to disable; there is nothing to disable.
2. **Row counts only.** Materialization metadata exposes exactly: `rows_fetched`, `pages_fetched`, `total_count_reported` (all integers), plus an echo of the filter *values the caller already configured* (`from_date_time`, `to_date_time`, and any of `practice_ids`/`provider_ids`/`location_ids`/`statuses` that were set). None of this is read back from the API response body — it's the request parameters the caller already had.
3. **Sanitized errors.** The one external HTTP call this component makes is isolated in the module-level `_zocdoc_list_appointments` function. On a non-2xx response it raises `ZocdocAPIError`, which carries **only the HTTP status code and a fixed, generic description string** — never `response.text`, `response.json()`, or the request body. A network-level exception (connection error, timeout) is also caught and re-raised as a generic `ZocdocAPIError` rather than surfacing the underlying exception's `str()`, since some HTTP client exceptions embed the request (including query params) in their message.
4. **The DataFrame itself is the asset's materialized *value*, not its metadata.** This is unavoidable and by design — the component's entire purpose is to deliver appointment data to a warehouse for reporting. The safety guarantee here is specifically about Dagster's event log/UI, which is a different, less access-controlled surface than wherever the DataFrame is persisted downstream (typically a warehouse table with its own access controls).

This design is verified by `tests/test_phi_safety.py`, which asserts that patient-identifying field values from a realistic fake Zocdoc response (`first_name`, `last_name`, `email_address`, `phone_number`, `date_of_birth`, free-text `notes`) never appear as substrings anywhere in the metadata dict a materialization emits, nor in any error message raised on an API failure.

## Verified Zocdoc API facts (api-docs.zocdoc.com, 2026)

- **Endpoint**: `GET /v1/appointments`.
- **Query parameters**: `page` (0-indexed), `page_size` (1-100), `statuses` (comma-delimited), `practice_ids` / `provider_ids` / `location_ids` (comma-delimited), `start_time_utc_min`/`start_time_utc_max`, `created_time_utc_min`/`created_time_utc_max`, `last_modified_time_utc_min`/`last_modified_time_utc_max`, `sort_by` (`start_time_utc` | `created_time_utc` | `last_modified_time_utc`), `sort_direction` (`ascending` | `descending`).
- **Response shape**: `{request_id, page, page_size, total_count, next_url, data: [...]}`. This component paginates by following `next_url`'s presence/absence and incrementing `page`, stopping when either exhausted or `limit` is reached.
- **Auth**: Bearer token from `zocdoc_resource` (OAuth2 client_credentials).

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | — (required) | Name of the asset to create. |
| `resource_key` | `str` | `"zocdoc_resource"` | Key of the `ZocdocResource` this asset depends on. |
| `from_date_time` | `Optional[str]` | `None` | Start of the appointment start-time window (ISO-8601) → `start_time_utc_min`. Required unless a time-based `partition_type` is set. |
| `to_date_time` | `Optional[str]` | `None` | End of the window → `start_time_utc_max`. |
| `practice_ids` / `provider_ids` / `location_ids` | `Optional[str]` | `None` | Comma-separated id filters. |
| `statuses` | `Optional[str]` | `None` | Comma-separated appointment statuses. |
| `sort_by` / `sort_direction` | `Optional[str]` | `None` | Sort controls (see API facts above). |
| `page_size` | `int` | `100` | Results per page (Zocdoc max 100). |
| `limit` | `int` | `1000` | Max appointments fetched across all pages (safety cap). |
| `partition_type` / `partition_start` / `partition_values` / `dynamic_partition_name` / `partition_dimensions` | — | `None` | Standard partitioning fields (see `FIELD_CONVENTIONS.md`). |
| `description` / `group_name` / `owners` / `asset_tags` / `kinds` / `deps` | — | — | Standard catalog fields. |

## Example

```yaml
type: dagster_component_templates.ZocdocAppointmentsIngestionComponent
attributes:
  asset_name: zocdoc_appointments
  resource_key: zocdoc_resource
  practice_ids: "prac_123"
  from_date_time: "2026-06-01T00:00:00Z"
  to_date_time: "2026-07-01T00:00:00Z"
  statuses: "booked,confirmed"
  limit: 1000
```

## Pairs with

- `zocdoc_resource` — OAuth2 client_credentials connection (required).
- `zocdoc_availability_upsert` — the write-side counterpart (DataFrame → provider timeslots).
