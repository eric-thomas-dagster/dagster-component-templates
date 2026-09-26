# `PipedrivePersonUpsertComponent`

Reverse-ETL sink: mirror an upstream DataFrame into **Pipedrive persons** via **search-then-write** — Pipedrive has no native upsert endpoint. This sink searches `/persons/search` by `key_field`, then updates the match or creates a new person.

## When to use

- Sync computed customer data (health scores, lead score, plan tier) from a warehouse INTO Pipedrive persons so sales sees it in-context on a deal.

## Prerequisites

1. **`key_field` must uniquely identify a person** — typically `email`, or a custom field's hash key (found in Pipedrive Settings → Data fields → Person fields).

## Pairs with

- **`pipedrive_resource`** — API token auth (required).
- **`pipedrive_ingestion`** — the READ-side counterpart (dlt-based bulk pull).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `resource_key` | optional (default `pipedrive_resource`) | Resource key registered by PipedriveResourceComponent. |
| `key_field` | required | Pipedrive field key to search on. Must be present in fields_map values. |
| `key_column` | required | Upstream column holding the value to match key_field on. |
| `fields_map` | required | Upstream column -> Pipedrive person field key. |
| `max_rows` | optional (default `10000`) | Safety cap on total rows per run. |

## Example
```yaml
type: dagster_component_templates.PipedrivePersonUpsertComponent
attributes:
  asset_name: pipedrive_persons_mirror
  upstream_asset_key: dbt_marts_customers
  resource_key: pipedrive_resource
  key_field: email
  key_column: email
  fields_map:
    name: full_name
    email: email
  group_name: reverse_etl
```
