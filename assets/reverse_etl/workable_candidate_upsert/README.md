# `WorkableCandidateUpsertComponent`

Reverse-ETL sink: mirror an upstream DataFrame into **Workable candidates** via **search-then-write**. Workable supports an exact-match candidate filter (`GET /candidates?email=`), so this is a true upsert: find the candidate by email, then update the match (and optionally move its pipeline stage), or create a new candidate.

## When to use

- Sync computed applicant/candidate data (enrichment, scoring, sourced leads) from a warehouse INTO Workable so recruiters see it in-context.
- Advance candidates through pipeline stages programmatically based on upstream signals (e.g. an assessment score crossing a threshold).

## Create behavior: job pipeline vs talent pool

New candidates (no existing match by email) are created one of two ways, controlled by `job_shortcode`:

- **`job_shortcode` set** -> `POST /jobs/{shortcode}/candidates`. The candidate is attached to that job's pipeline immediately.
- **`job_shortcode` unset** -> `POST /talent_pool/candidates`. The candidate lands in the account-wide talent pool, not attached to any job — useful for general sourcing before a job assignment is known.

## Stage moves

`stage_column` only applies to **existing** candidates found by email. If set, and the column has a non-null value for a row, this sink calls `move_candidate` after updating fields. Workable's `/candidates/{id}/move` endpoint **requires** `member_id` (the acting account member) — this component validates that `member_id` is set whenever `stage_column` is configured, raising at component-build time otherwise.

## Prerequisites

1. A `email_column` in your upstream data — Workable's documented exact-match filter (`GET /candidates?email=`) is how this sink recognizes the same candidate across runs.
2. If using `stage_column`, a `member_id` (the acting Workable account member's id).

## Pairs with

- **`workable_resource`** — Bearer token auth (required).
- **`workable_ingestion`** — the READ-side counterpart (dlt-based bulk pull of jobs/candidates/stages/members).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `resource_key` | optional (default `workable_resource`) | Resource key registered by WorkableResourceComponent. |
| `email_column` | required | Upstream column holding the candidate's email (match key). |
| `name_column` | required | Upstream column holding the candidate's full name (used on create, mapped to Workable's `name` field). |
| `job_shortcode` | optional | If set, new candidates are created via the job pipeline endpoint; else via the talent pool. |
| `fields_map` | optional (default `{}`) | Upstream column -> Workable candidate field (phone, summary, address, cover_letter, etc). Applied on both create and update. |
| `stage_column` | optional | Upstream column holding a target pipeline stage slug; triggers a move on existing matches. Requires `member_id`. |
| `member_id` | optional | Acting Workable account member id. Required only if `stage_column` is used. |
| `max_rows` | optional (default `10000`) | Safety cap on total rows per run. |

## Example

```yaml
type: dagster_component_templates.WorkableCandidateUpsertComponent
attributes:
  asset_name: workable_candidates_mirror
  upstream_asset_key: dbt_marts_applicants
  resource_key: workable_resource
  email_column: email
  name_column: full_name
  job_shortcode: ABCD1234
  fields_map:
    phone: phone
    summary: summary
  group_name: reverse_etl
```

## Example — talent pool + stage moves

```yaml
type: dagster_component_templates.WorkableCandidateUpsertComponent
attributes:
  asset_name: workable_candidates_mirror
  source:
    kind: inline
    rows:
      - email: jane@example.com
        full_name: Jane Doe
        stage: sourced
  resource_key: workable_resource
  email_column: email
  name_column: full_name
  stage_column: stage
  member_id: "123456"
```
