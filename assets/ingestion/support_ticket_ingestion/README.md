# Support Ticket Ingestion Component

Ingest support ticket data from help desk platforms using [dlt](https://dlthub.com). Currently uses dlt's verified `zendesk_support` source for Zendesk; other platforms emit sample data for prototyping.

## Overview

dlt handles authentication, pagination, rate limiting, and incremental loading. This component wraps it as a Dagster asset that returns a pandas DataFrame by default, or persists directly to a destination if you set one.

## Source-specific fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_name` | `str` | yes | Name of the output asset |
| `platform` | `str` | yes | Support platform — `zendesk` is fully supported; other values emit sample data |

Zendesk credentials are read from environment variables at run-time:

| Env var | Description |
|---|---|
| `ZENDESK_SUBDOMAIN` | Your Zendesk subdomain (e.g. `acme` for `acme.zendesk.com`) |
| `ZENDESK_EMAIL` | Email address used for the API |
| `ZENDESK_API_TOKEN` | Zendesk API token |

## Standard fields

`description`, `group_name`, `owners`, `asset_tags`, `kinds`, `freshness_max_lag_minutes`, `freshness_cron`, `include_preview_metadata`, `deps` — same convention as every other component in this library.

## Destination

By default this asset runs an in-memory DuckDB pipeline and returns a pandas DataFrame for downstream Dagster transformations. To persist directly to a warehouse or object store, set `destination`:

```yaml
type: dagster_component_templates.SupportTicketIngestionComponent
attributes:
  asset_name: support_tickets
  platform: zendesk
  destination: snowflake          # or bigquery, postgres, filesystem, ...
  dataset_name: support_raw
  persist_only: true
```

Credentials come from env vars (`DESTINATION__SNOWFLAKE__CREDENTIALS__*`) or, for projects with multiple destination accounts, from inline `destination_credentials_url` / `destination_credentials_env_var`.

**For full destination configuration — env-var conventions, multi-account setups, every supported dlt destination — see [`../DESTINATIONS.md`](../DESTINATIONS.md).**

## Example — DataFrame mode (default)

```yaml
type: dagster_component_templates.SupportTicketIngestionComponent
attributes:
  asset_name: support_tickets
  platform: zendesk
  group_name: support
```

The asset emits a pandas DataFrame of tickets. If Zendesk credentials are missing in default mode, the component logs a warning and returns sample data so prototypes can run end-to-end without secrets.

## Example — persist to Snowflake

```yaml
type: dagster_component_templates.SupportTicketIngestionComponent
attributes:
  asset_name: support_tickets
  platform: zendesk
  destination: snowflake
  dataset_name: support_raw
  persist_only: true
```

Set `DESTINATION__SNOWFLAKE__CREDENTIALS__*` plus `ZENDESK_SUBDOMAIN`, `ZENDESK_EMAIL`, `ZENDESK_API_TOKEN` env vars before running. The asset emits a `MaterializeResult` with destination metadata.

## Notes

- **Incremental loading**: dlt tracks state across runs.
- **Schema evolution**: dlt accommodates new fields without manual migrations.
- **Sample-data fallback**: in DataFrame mode, missing Zendesk credentials produce a small synthetic DataFrame so component graphs can be validated without secrets. In destination mode, missing credentials cause the run to fail rather than persist synthesized data.
- **Other platforms**: setting `platform` to anything other than `zendesk` returns sample data only — and is blocked entirely when a destination is set.
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

- [dlt Zendesk source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/zendesk)
- [dlt destinations overview](https://dlthub.com/docs/dlt-ecosystem/destinations)
- [`../DESTINATIONS.md`](../DESTINATIONS.md) — configuration reference for this library
