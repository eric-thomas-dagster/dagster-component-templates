# Workable Ingestion Component


Ingest [Workable](https://www.workable.com) recruiting data using [dlt](https://dlthub.com)'s verified `workable` source.


## Overview

dlt handles API authentication, pagination, rate limiting, and incremental loading. This component wraps it as a Dagster asset that returns a pandas DataFrame by default, or persists directly to a destination if you set one.


## Source-specific fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_name` | `str` | yes | Name of the output asset |
| `api_token` | `str` | yes | Workable API token |
| `subdomain` | `str` | yes | Workable subdomain (e.g. `your-company` from `your-company.workable.com`) |
| `resources` | `List[str]` | no | Defaults to `["jobs", "candidates"]`. Available: `jobs`, `candidates`, `stages`, `members` |

## Standard fields

`description`, `group_name`, `owners`, `asset_tags`, `kinds`, `freshness_max_lag_minutes`, `freshness_cron`, `include_preview_metadata`, `deps` — same convention as every other component in this library.


## Destination

By default this asset runs an in-memory DuckDB pipeline and returns a pandas DataFrame for downstream Dagster transformations. To persist directly to a warehouse or object store, set `destination`:


```yaml
type: dagster_component_templates.WorkableIngestionComponent
attributes:
  asset_name: workable_data
  # ... source config ...
  destination: postgres          # or bigquery, postgres, filesystem, ...
  dataset_name: workable_raw
  persist_only: true
```

Credentials come from env vars (`DESTINATION__<NAME>__CREDENTIALS__*`) or, for projects with multiple destination accounts, from inline `destination_credentials_url` / `destination_credentials_env_var`.


**For full destination configuration — env-var conventions, multi-account setups, every supported dlt destination — see [`../DESTINATIONS.md`](../DESTINATIONS.md).**


## Example — DataFrame mode (default)

```yaml
type: dagster_component_templates.WorkableIngestionComponent
attributes:
  asset_name: workable_data
  api_token: "{{ env('WORKABLE_API_TOKEN') }}"
  subdomain: my-company
  resources: [jobs, candidates]
  group_name: workable
```

The asset emits a pandas DataFrame combining all selected resources, with a `_resource_type` column tagging each row.


## Example — persist to Postgres

```yaml
type: dagster_component_templates.WorkableIngestionComponent
attributes:
  asset_name: workable_data
  api_token: "{{ env('WORKABLE_API_TOKEN') }}"
  subdomain: my-company
  resources: [jobs, candidates, stages]
  destination: postgres
  dataset_name: workable_raw
  persist_only: true
```

Set `DESTINATION__POSTGRES__CREDENTIALS__*` env vars before running. The asset emits a `MaterializeResult` with destination metadata.


## Notes

- **Token scope**: the API token must have read access to the chosen resources.
- **Incremental loading**: dlt tracks state automatically across runs.
- **Non-SQL destinations**: setting `destination=filesystem` (or any vector store / lake format) requires `persist_only=true`. The component logs a warning and returns a `MaterializeResult` if you forget.


## Multiple accounts (multi-tenant)

To pull from multiple accounts / customers, create one `defs/<name>/defs.yaml` per instance — each referencing its own env var for credentials:

```yaml
# defs/customer_a/defs.yaml
type: ...
attributes:
  asset_name: customer_a_data
  # Replace `<credential_field>` with this component's auth field name (see Configuration above):
  <credential_field>: "{{ env('<VENDOR>_<CREDENTIAL>_CUSTOMER_A') }}"
  # ... other source config (account-specific IDs, etc.) ...
```

```yaml
# defs/customer_b/defs.yaml
type: ...
attributes:
  asset_name: customer_b_data
  <credential_field>: "{{ env('<VENDOR>_<CREDENTIAL>_CUSTOMER_B') }}"
  # ...
```

Both assets show up independently in the Dagster catalog with their own credentials, schedules, freshness policies, etc. See [`../../MULTI_INSTANCE.md`](../../MULTI_INSTANCE.md) for the full pattern (works for any component with credentials), a Python scaffold script for many tenants, multi-destination combinations, and when partitioning a single asset by customer is the better choice.

## Asset dependencies

```yaml
deps:
  - some_upstream_asset
  - schema/scoped_asset
```

Dependencies declared here draw lineage edges in the Dagster graph without loading data at runtime.


## Learn more

- [dlt Workable source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/workable)
- [dlt destinations overview](https://dlthub.com/docs/dlt-ecosystem/destinations)
- [`../DESTINATIONS.md`](../DESTINATIONS.md) — configuration reference for this library
