# `GreenhouseCandidateUpdateComponent`

Reverse-ETL sink: update existing **Greenhouse candidates'** tags and/or custom fields via `PATCH /v1/candidates/{id}`.

> **Update-only, not an upsert.** Candidates must already exist in Greenhouse (created via the ATS UI or the application-submission API, not this sink) — `candidate_id_column` must hold a real Greenhouse candidate ID.

## When to use

- Push a computed resume-screening score, sourcing tag, or diversity-pipeline flag from a warehouse back onto existing Greenhouse candidate records so recruiters see it in context.

## Prerequisites

1. **Custom fields already created** in Greenhouse for anything in `custom_fields_map` — use the field's internal name, not its display label (check your account's field configuration if updates silently no-op).
2. **A valid `on_behalf_of_user_id`** configured on the `greenhouse` resource — Greenhouse's Harvest API requires this on every write and checks that user's permissions server-side.

## Pairs with

- **`greenhouse`** resource — Harvest API key auth + `update_candidate` convenience method (required).
- **`greenhouse_harvest_ingestion`** — the READ-side counterpart (dlt-based bulk pull; does not use this resource).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `resource_key` | optional (default `greenhouse`) | Resource key registered by GreenhouseResourceComponent. |
| `candidate_id_column` | required | Upstream column holding the Greenhouse candidate ID. |
| `tags_column` | optional | Upstream column holding tags to set (replaces, does not merge). |
| `custom_fields_map` | optional | Upstream column -> Greenhouse custom field internal name. |
| `max_rows` | optional (default `10000`) | Safety cap on total rows per run. |

## Example
```yaml
type: dagster_component_templates.GreenhouseCandidateUpdateComponent
attributes:
  asset_name: greenhouse_candidate_scores
  upstream_asset_key: candidate_screening_scores
  resource_key: greenhouse
  candidate_id_column: greenhouse_candidate_id
  tags_column: computed_tags
  custom_fields_map:
    screening_score: screening_score
  group_name: reverse_etl
```
