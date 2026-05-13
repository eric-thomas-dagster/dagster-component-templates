# Salesforce Ingestion Component


Ingest [Salesforce](https://www.salesforce.com) CRM objects using [dlt](https://dlthub.com)'s verified `salesforce` source.


## Overview

dlt handles API authentication, pagination, rate limiting, and incremental loading. This component wraps it as a Dagster asset that returns a pandas DataFrame by default, or persists directly to a destination if you set one.


## Source-specific fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_name` | `str` | yes | Name of the output asset |
| `username` | `str` | yes | Salesforce username |
| `password` | `str` | yes | Salesforce password |
| `security_token` | `str` | yes | Salesforce security token |
| `sf_objects` | `List[str]` | no | Objects to extract — defaults to `["Account", "Opportunity", "Contact", "Lead"]`. Any standard or custom (`__c`) object is supported. |

## Standard fields

`description`, `group_name`, `owners`, `asset_tags`, `kinds`, `freshness_max_lag_minutes`, `freshness_cron`, `include_preview_metadata`, `deps` — same convention as every other component in this library.


## Destination

By default this asset runs an in-memory DuckDB pipeline and returns a pandas DataFrame for downstream Dagster transformations. To persist directly to a warehouse or object store, set `destination`:


```yaml
type: dagster_component_templates.SalesforceIngestionComponent
attributes:
  asset_name: salesforce_data
  # ... source config ...
  destination: snowflake          # or bigquery, postgres, filesystem, ...
  dataset_name: salesforce_raw
  persist_only: true
```

Credentials come from env vars (`DESTINATION__<NAME>__CREDENTIALS__*`) or, for projects with multiple destination accounts, from inline `destination_credentials_url` / `destination_credentials_env_var`.


**For full destination configuration — env-var conventions, multi-account setups, every supported dlt destination — see [`../DESTINATIONS.md`](../DESTINATIONS.md).**


## Example — DataFrame mode (default)

```yaml
type: dagster_component_templates.SalesforceIngestionComponent
attributes:
  asset_name: salesforce_data
  username: "{{ env('SALESFORCE_USERNAME') }}"
  password: "{{ env('SALESFORCE_PASSWORD') }}"
  security_token: "{{ env('SALESFORCE_TOKEN') }}"
  sf_objects: [Account, Opportunity, Contact]
  group_name: salesforce
```

The asset emits a pandas DataFrame combining all selected resources, with a `_resource_type` column tagging each row.


## Example — persist to Snowflake

```yaml
type: dagster_component_templates.SalesforceIngestionComponent
attributes:
  asset_name: salesforce_data
  username: "{{ env('SALESFORCE_USERNAME') }}"
  password: "{{ env('SALESFORCE_PASSWORD') }}"
  security_token: "{{ env('SALESFORCE_TOKEN') }}"
  sf_objects: [Account, Opportunity, Contact, Lead]
  destination: snowflake
  dataset_name: salesforce_raw
  persist_only: true
```

Set `DESTINATION__SNOWFLAKE__CREDENTIALS__*` env vars before running. The asset emits a `MaterializeResult` with destination metadata.


## Notes

- **Security token**: regenerate via Settings > Personal Setup > Reset My Security Token.
- **Custom objects**: append `__c` (e.g. `MyObject__c`).
- **Bulk API**: large objects use Salesforce Bulk API — dlt handles polling.
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

- [dlt Salesforce source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/salesforce)
- [dlt destinations overview](https://dlthub.com/docs/dlt-ecosystem/destinations)
- [`../DESTINATIONS.md`](../DESTINATIONS.md) — configuration reference for this library
