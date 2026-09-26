# `FreshdeskContactUpsertComponent`

Reverse-ETL sink: mirror an upstream DataFrame into **Freshdesk contacts** via **search-then-write** — Freshdesk has no single-call native upsert. This sink lists `/contacts` filtered by `unique_external_id` (a real, documented filter param), then updates the match or creates a new contact.

## When to use

- Sync computed customer data (plan tier, health score) from a warehouse INTO Freshdesk contacts so support agents see it in-context.

## Prerequisites

1. **A stable external ID column** in your upstream data — `unique_external_id` is how this sink recognizes the same contact across runs.

## Pairs with

- **`freshdesk_resource`** — API key auth (required).
- **`freshdesk_ingestion`** — the READ-side counterpart (dlt-based bulk pull).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `resource_key` | optional (default `freshdesk_resource`) | Resource key registered by FreshdeskResourceComponent. |
| `external_id_column` | required | Upstream column holding a stable external ID (Freshdesk match key). |
| `fields_map` | required | Upstream column -> Freshdesk contact field (e.g. name, email, phone). |
| `max_rows` | optional (default `10000`) | Safety cap on total rows per run. |

## Example
```yaml
type: dagster_component_templates.FreshdeskContactUpsertComponent
attributes:
  asset_name: freshdesk_contacts_mirror
  upstream_asset_key: dbt_marts_customers
  resource_key: freshdesk_resource
  external_id_column: customer_id
  fields_map:
    name: full_name
    email: email
  group_name: reverse_etl
```
