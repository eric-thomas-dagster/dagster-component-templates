# Google Calendar Ingestion

List events from a Google Calendar via a service account. Returns a pandas DataFrame, one row per event in the configured time window.

Pairs naturally with `gemini_llm` / `openai_llm` for meeting summaries, with `transformations/*` for event analytics, or with `sinks/*` to land events in a warehouse.

## Required packages

```
dagster>=1.8.0
pandas>=1.5.0
google-auth>=2.0.0
google-api-python-client>=2.0.0
```

## Setup

1. **Enable the Calendar API** on the SA's GCP project. The component surfaces a `403 SERVICE_DISABLED` with the activation URL on first call.
2. **Share the target calendar with the SA email** (the `client_email` in your JSON):
   - Open Google Calendar.
   - Find the calendar in the left sidebar → three-dot menu → **Settings and sharing**.
   - Scroll to **Share with specific people or groups** → **+ Add people and groups**.
   - Paste the SA email → Permission: **See all event details** → Send.
3. Set `calendar_id` in YAML to the OWNER's email (or a named calendar's ID). **Don't pass `'primary'`** — that's the SA's own (empty) calendar.

## Output schema

| Column | Type | Notes |
|---|---|---|
| `id` | str | Event ID |
| `summary` | str | Event title |
| `description` | str | Body (may be HTML) |
| `status` | str | `confirmed` / `tentative` / `cancelled` |
| `start` / `end` | str | ISO8601 dateTime, or YYYY-MM-DD if all-day |
| `all_day` | bool | True if start has no time component |
| `location` | str | Free-text location |
| `organizer` | str | Email |
| `attendees` | list[str] | Attendee emails |
| `attendee_count` | int | |
| `html_link` | str | Link to the event in Google Calendar |
| `hangout_link` | str | Google Meet URL if attached |
| `created` / `updated` | str | ISO8601 |

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `asset_name` | yes | — | Output asset name |
| `credentials` / `credentials_path` | no | — | SA auth — falls back to `GOOGLE_APPLICATION_CREDENTIALS` |
| `calendar_id` | no | `primary` | Calendar ID — set to the owner's email or a named calendar |
| `time_min` | no | — | RFC3339 lower bound; defaults to now |
| `time_max` | no | — | RFC3339 upper bound; defaults to `now + days_ahead` |
| `days_ahead` | no | `30` | Used when `time_max` is unset |
| `max_results` | no | `250` | Cap on returned events |
| `single_events` | no | `true` | Expand recurring events into instances |
| `show_deleted` | no | `false` | Include deleted events |
| Standard Dagster attrs (description / group_name / deps / tags / owners / partition_*) | | | |

## Example

```yaml
type: dagster_component_templates.GoogleCalendarIngestionComponent
attributes:
  asset_name: upcoming_events
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
  calendar_id: ethomasii@gmail.com
  days_ahead: 30
  max_results: 250
  group_name: calendar
```
