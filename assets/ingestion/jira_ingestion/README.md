# Jira Ingestion Component


Ingest [Jira](https://www.atlassian.com/software/jira) project tracking data using [dlt](https://dlthub.com)'s verified `jira` source.


## Overview

dlt handles API authentication, pagination, rate limiting, and incremental loading. This component wraps it as a Dagster asset that returns a pandas DataFrame by default, or persists directly to a destination if you set one.


## Source-specific fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_name` | `str` | yes | Name of the output asset |
| `domain` | `str` | yes | Jira Cloud domain (e.g. `your-company.atlassian.net`) |
| `email` | `str` | yes | Atlassian account email |
| `api_token` | `str` | yes | Atlassian API token |
| `resources` | `List[str]` | no | Defaults to `["issues", "projects"]`. Available: `issues`, `users`, `projects`, `boards` |

## Standard fields

`description`, `group_name`, `owners`, `asset_tags`, `kinds`, `freshness_max_lag_minutes`, `freshness_cron`, `include_sample_metadata`, `deps` — same convention as every other component in this library.


## Destination

By default this asset runs an in-memory DuckDB pipeline and returns a pandas DataFrame for downstream Dagster transformations. To persist directly to a warehouse or object store, set `destination`:


```yaml
type: dagster_component_templates.JiraIngestionComponent
attributes:
  asset_name: jira_data
  # ... source config ...
  destination: postgres          # or bigquery, postgres, filesystem, ...
  dataset_name: jira_raw
  persist_only: true
```

Credentials come from env vars (`DESTINATION__<NAME>__CREDENTIALS__*`) or, for projects with multiple destination accounts, from inline `destination_credentials_url` / `destination_credentials_env_var`.


**For full destination configuration — env-var conventions, multi-account setups, every supported dlt destination — see [`../DESTINATIONS.md`](../DESTINATIONS.md).**


## Example — DataFrame mode (default)

```yaml
type: dagster_component_templates.JiraIngestionComponent
attributes:
  asset_name: jira_data
  domain: my-company.atlassian.net
  email: "{{ env('JIRA_EMAIL') }}"
  api_token: "{{ env('JIRA_API_TOKEN') }}"
  resources: [issues, projects]
  group_name: jira
```

The asset emits a pandas DataFrame combining all selected resources, with a `_resource_type` column tagging each row.


## Example — persist to Postgres

```yaml
type: dagster_component_templates.JiraIngestionComponent
attributes:
  asset_name: jira_data
  domain: my-company.atlassian.net
  email: "{{ env('JIRA_EMAIL') }}"
  api_token: "{{ env('JIRA_API_TOKEN') }}"
  resources: [issues]
  destination: postgres
  dataset_name: jira_raw
  persist_only: true
```

Set `DESTINATION__POSTGRES__CREDENTIALS__*` env vars before running. The asset emits a `MaterializeResult` with destination metadata.


## Notes

- **Tokens, not passwords**: use an [Atlassian API token](https://id.atlassian.com/manage-profile/security/api-tokens), not your account password.
- **Custom fields**: Jira custom fields appear as `customfield_NNNN` columns.
- **Non-SQL destinations**: setting `destination=filesystem` (or any vector store / lake format) requires `persist_only=true`. The component logs a warning and returns a `MaterializeResult` if you forget.

## Asset dependencies

```yaml
deps:
  - some_upstream_asset
  - schema/scoped_asset
```

Dependencies declared here draw lineage edges in the Dagster graph without loading data at runtime.


## Learn more

- [dlt Jira source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/jira)
- [dlt destinations overview](https://dlthub.com/docs/dlt-ecosystem/destinations)
- [`../DESTINATIONS.md`](../DESTINATIONS.md) — configuration reference for this library
