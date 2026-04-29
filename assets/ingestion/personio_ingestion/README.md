# Personio Ingestion Component


Ingest [Personio](https://www.personio.com) HR data using [dlt](https://dlthub.com)'s verified `personio` source.


## Overview

dlt handles API authentication, pagination, rate limiting, and incremental loading. This component wraps it as a Dagster asset that returns a pandas DataFrame by default, or persists directly to a destination if you set one.


## Source-specific fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_name` | `str` | yes | Name of the output asset |
| `client_id` | `str` | yes | Personio API client ID |
| `client_secret` | `str` | yes | Personio API client secret |
| `resources` | `List[str]` | no | Defaults to `["employees", "absences"]`. Available: `employees`, `absences`, `attendances`, `recruiting` |

## Standard fields

`description`, `group_name`, `owners`, `asset_tags`, `kinds`, `freshness_max_lag_minutes`, `freshness_cron`, `include_sample_metadata`, `deps` — same convention as every other component in this library.


## Destination

By default this asset runs an in-memory DuckDB pipeline and returns a pandas DataFrame for downstream Dagster transformations. To persist directly to a warehouse or object store, set `destination`:


```yaml
type: dagster_component_templates.PersonioIngestionComponent
attributes:
  asset_name: personio_data
  # ... source config ...
  destination: postgres          # or bigquery, postgres, filesystem, ...
  dataset_name: personio_raw
  persist_only: true
```

Credentials come from env vars (`DESTINATION__<NAME>__CREDENTIALS__*`) or, for projects with multiple destination accounts, from inline `destination_credentials_url` / `destination_credentials_env_var`.


**For full destination configuration — env-var conventions, multi-account setups, every supported dlt destination — see [`../DESTINATIONS.md`](../DESTINATIONS.md).**


## Example — DataFrame mode (default)

```yaml
type: dagster_component_templates.PersonioIngestionComponent
attributes:
  asset_name: personio_data
  client_id: "{{ env('PERSONIO_CLIENT_ID') }}"
  client_secret: "{{ env('PERSONIO_CLIENT_SECRET') }}"
  resources: [employees, absences]
  group_name: personio
```

The asset emits a pandas DataFrame combining all selected resources, with a `_resource_type` column tagging each row.


## Example — persist to Postgres

```yaml
type: dagster_component_templates.PersonioIngestionComponent
attributes:
  asset_name: personio_data
  client_id: "{{ env('PERSONIO_CLIENT_ID') }}"
  client_secret: "{{ env('PERSONIO_CLIENT_SECRET') }}"
  resources: [employees, absences, attendances]
  destination: postgres
  dataset_name: personio_raw
  persist_only: true
```

Set `DESTINATION__POSTGRES__CREDENTIALS__*` env vars before running. The asset emits a `MaterializeResult` with destination metadata.


## Notes

- **Credentials**: Personio rotates client_id/client_secret per integration — store both in env vars.
- **Recruiting data**: requires the recruiting module to be enabled in Personio.
- **Non-SQL destinations**: setting `destination=filesystem` (or any vector store / lake format) requires `persist_only=true`. The component logs a warning and returns a `MaterializeResult` if you forget.

## Asset dependencies

```yaml
deps:
  - some_upstream_asset
  - schema/scoped_asset
```

Dependencies declared here draw lineage edges in the Dagster graph without loading data at runtime.


## Learn more

- [dlt Personio source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/personio)
- [dlt destinations overview](https://dlthub.com/docs/dlt-ecosystem/destinations)
- [`../DESTINATIONS.md`](../DESTINATIONS.md) — configuration reference for this library
