# `LeverCandidateUpsertComponent`

Reverse-ETL sink: mirror an upstream DataFrame into **Lever opportunities (candidates)** via **search-then-write**. Unlike Greenhouse's update-only candidate PATCH elsewhere in this repo, Lever's Opportunities API supports a documented exact-match `email` filter, so this is a true **upsert**.

For each upstream row:
1. `GET /opportunities?email={email}` -- look up an existing opportunity by email.
2. If found -- add tags / update stage / archive as configured (none, some, or all of these per row).
3. If not found -- `POST /opportunities?perform_as=...` to create a new opportunity, seeding name/headline/stage/tags from the row.

## When to use

- Sync computed recruiting-pipeline signals (lead score, outreach stage, sourcing tags) from a warehouse INTO Lever so recruiters see them in-context.
- Bulk-import sourced candidates from an external sourcing tool or spreadsheet into Lever as new opportunities.
- Advance or archive existing opportunities in bulk based on a downstream decision (e.g. an automated screening step).

## Prerequisites

1. **A `lever_resource`** registered with a valid API key and a `perform_as` Lever user ID (see that resource's README -- `perform_as` is required by Lever on every mutating call).
2. **An email column** in your upstream data -- this is how the sink recognizes the same candidate across runs.

## Pairs with

- **`lever_resource`** -- Basic auth (blank password) + `perform_as` (required).
- **`lever_ingestion`** -- the READ-side counterpart (dlt-based bulk pull).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `resource_key` | optional (default `lever_resource`) | Resource key registered by LeverResourceComponent. |
| `email_column` | required | Upstream column holding the candidate's email (match key). |
| `name_column` | optional | Candidate full name. Used on CREATE only. |
| `headline_column` | optional | Candidate headline (e.g. current title/company). Used on CREATE only. |
| `tags_column` | optional | Comma-separated string or list of tags. Added via `addTags` -- ADDITIVE ONLY. |
| `stage_id_column` | optional | Lever stage ID. `update_stage` on an existing match; `"stage"` on create. |
| `archive_reason_column` | optional | Archive reason. Only applies to an EXISTING match, never on create. |
| `posting_id` | optional | Static Lever posting ID, passed to `create_opportunity` only. |
| `max_rows` | optional (default `10000`) | Safety cap on total rows per run. |

## Example
```yaml
type: dagster_component_templates.LeverCandidateUpsertComponent
attributes:
  asset_name: lever_candidates_mirror
  upstream_asset_key: dbt_marts_applicants
  resource_key: lever_resource
  email_column: email
  name_column: full_name
  headline_column: current_title
  tags_column: tags
  stage_id_column: stage_id
  group_name: reverse_etl
```

## Behavior + gotchas

- **Tags are additive only.** Lever's API has no "replace all tags" call -- `add_tags` merges onto whatever tags already exist on the opportunity. There is no way for this sink (or Lever's API itself) to remove a tag.
- **Archive only applies to existing matches.** A brand-new opportunity is never created pre-archived by this sink, even if `archive_reason_column` has a value for that row.
- **A matched row with no configured action still counts as `rows_updated`** (logged at debug level) -- this reflects that the row was successfully matched in Lever, even if none of `tags_column`/`stage_id_column`/`archive_reason_column` applied to it.
- **`perform_as` is mandatory.** Every write call requires it; the `lever_resource` resource enforces this via a required config field, not an env var (it's a user ID, not a secret).
- **Blank vs missing.** Empty-string and null/NaN values are both treated as "not supplied" -- rows with no email are skipped and counted in `rows_skipped_no_key`; optional columns with blank values simply don't trigger their corresponding action.

## Related

- `lever_resource` -- connection + workhorse (search/create/tag/stage/archive methods).
- `lever_ingestion` -- read side (dlt-backed REST API source).
