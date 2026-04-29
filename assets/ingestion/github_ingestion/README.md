# GitHub Ingestion Component

Ingest GitHub repository activity (issues, pull requests, comments, reactions) using [dlt](https://dlthub.com)'s verified `github_reactions` source.

## Overview

dlt handles API authentication, pagination, rate limiting, and incremental loading. This component wraps it as a Dagster asset that returns a pandas DataFrame by default, or persists directly to a destination if you set one.

## Source-specific fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_name` | `str` | yes | Name of the output asset |
| `access_token` | `str` | yes | GitHub Personal Access Token (use env var interpolation: `"{{ env('GITHUB_ACCESS_TOKEN') }}"`) |
| `owner` | `str` | yes | Repository owner (username or organization) |
| `repositories` | `List[str]` | yes | List of repository names to extract from |
| `resources` | `List[str]` | no | Subset of resources to extract — defaults to `["issues", "pull_requests"]`. Available: `issues`, `pull_requests`, `comments`, `reactions` |

## Standard fields

`description`, `group_name`, `owners`, `asset_tags`, `kinds`, `freshness_max_lag_minutes`, `freshness_cron`, `include_sample_metadata`, `deps` — same convention as every other component in this library.

## Destination

By default this asset runs an in-memory DuckDB pipeline and returns a pandas DataFrame for downstream Dagster transformations. To persist directly to a warehouse or object store, set `destination`:

```yaml
type: dagster_component_templates.GitHubIngestionComponent
attributes:
  asset_name: github_repos
  access_token: "{{ env('GITHUB_ACCESS_TOKEN') }}"
  owner: my-org
  repositories: [repo-1, repo-2]
  destination: snowflake          # or bigquery, postgres, filesystem, ...
  dataset_name: github_raw
  persist_only: true
```

Credentials come from env vars (`DESTINATION__SNOWFLAKE__CREDENTIALS__*`) or, for projects with multiple destination accounts, from inline `destination_credentials_url` / `destination_credentials_env_var`.

**For full destination configuration — env-var conventions, multi-account setups, every supported dlt destination — see [`../DESTINATIONS.md`](../DESTINATIONS.md).**

## Example — DataFrame mode (default)

```yaml
type: dagster_component_templates.GitHubIngestionComponent
attributes:
  asset_name: github_activity
  access_token: "{{ env('GITHUB_ACCESS_TOKEN') }}"
  owner: my-org
  repositories: [my-app, my-lib]
  resources: [issues, pull_requests, comments]
  group_name: github
```

The asset emits a pandas DataFrame combining all selected resources, with a `_resource_type` column tagging each row.

## Example — persist to Snowflake

```yaml
type: dagster_component_templates.GitHubIngestionComponent
attributes:
  asset_name: github_repos
  access_token: "{{ env('GITHUB_ACCESS_TOKEN') }}"
  owner: my-org
  repositories: [my-app]
  destination: snowflake
  dataset_name: github_raw
  persist_only: true
```

Set `DESTINATION__SNOWFLAKE__CREDENTIALS__*` env vars before running. The asset emits a `MaterializeResult` with destination metadata.

## Notes

- **Incremental loading**: dlt tracks state automatically across runs.
- **Schema evolution**: dlt accommodates new fields without manual migrations.
- **Rate limiting**: dlt respects GitHub's rate limits; configure a higher-tier token for large pulls.
- **Non-SQL destinations**: setting `destination=filesystem` (or any vector store / lake format) requires `persist_only=true`. The component logs a warning and returns a `MaterializeResult` if you forget.

## Asset dependencies

```yaml
deps:
  - some_upstream_asset
  - schema/scoped_asset
```

Dependencies declared here draw lineage edges in the Dagster graph without loading data at runtime.

## Learn more

- [dlt GitHub source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/github)
- [dlt destinations overview](https://dlthub.com/docs/dlt-ecosystem/destinations)
- [`../DESTINATIONS.md`](../DESTINATIONS.md) — configuration reference for this library
