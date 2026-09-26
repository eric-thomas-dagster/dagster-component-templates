# `LaunchDarklySegmentUpdateComponent`

Reverse-ETL sink: sync a computed cohort into a **LaunchDarkly segment's target list** via a semantic patch, adding or removing context keys per row.

Every row is treated as an **add** unless `in_segment_column` evaluates falsy, in which case that row's key is **removed** instead — so a single sync can both add newly-qualifying contexts and evict ones that no longer qualify.

## When to use

- Push a computed cohort (beta testers, at-risk accounts, internal staff, feature-adoption segments) from a warehouse INTO a LaunchDarkly segment so feature-flag targeting rules can reference it directly, instead of duplicating the targeting logic inside LaunchDarkly itself.

## Prerequisites

1. **The target segment must already exist** in LaunchDarkly (Project → Environment → Segments) — this sink only updates membership, it doesn't create segments.

## Pairs with

- **`launchdarkly`** resource — API token auth + semantic-patch segment update (required).
- **`launchdarkly_ingestion`** — the READ-side counterpart (dlt-based bulk pull; does not use this resource).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `resource_key` | optional (default `launchdarkly`) | Resource key registered by LaunchDarklyResourceComponent. |
| `project_key` | required | LaunchDarkly project key. |
| `env_key` | required | LaunchDarkly environment key. |
| `segment_key` | required | LaunchDarkly segment key to update. |
| `context_key_column` | required | Upstream column holding the context key. |
| `in_segment_column` | optional | If false, removes instead of adds. If unset, every row is added. |
| `context_kind` | optional (default `user`) | LaunchDarkly context kind. |
| `max_rows` | optional (default `10000`) | Safety cap on total rows per run. |

## Example
```yaml
type: dagster_component_templates.LaunchDarklySegmentUpdateComponent
attributes:
  asset_name: launchdarkly_beta_testers_sync
  upstream_asset_key: computed_beta_cohort
  resource_key: launchdarkly
  project_key: my-project
  env_key: production
  segment_key: beta-testers
  context_key_column: user_key
  in_segment_column: is_beta_tester
  group_name: reverse_etl
```
