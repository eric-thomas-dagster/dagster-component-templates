# `HeapUserPropertySyncComponent`

Reverse-ETL sink: push computed user properties from an upstream DataFrame into **Heap** via `add_user_properties`.

> **Why this exists instead of `heap_ingestion`:** Heap's public API is write-only (`track`/`identify`/`add_user_properties`/`delete_user`) — there is no bulk read/list endpoint, so a pull-based ingestion connector genuinely isn't possible against this API. This sink uses that same write API for what it's actually good for: pushing warehouse-computed properties (plan tier, health score, lifecycle stage) onto Heap users for product-analytics segmentation.

Heap's API has no read-back or dedup concept — this calls the endpoint once per row, every run; Heap itself handles the property overwrite idempotently server-side.

## Pairs with

- **`heap`** resource — write-only Data API wrapper (required).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `resource_key` | optional (default `heap`) | Resource key registered by HeapResourceComponent. |
| `identity_column` | required | Upstream column holding the Heap identity. |
| `properties_map` | required | Upstream column -> Heap user property name. |
| `max_rows` | optional (default `10000`) | Safety cap on total rows per run. |

## Example
```yaml
type: dagster_component_templates.HeapUserPropertySyncComponent
attributes:
  asset_name: heap_user_properties_sync
  upstream_asset_key: dbt_marts_customers
  resource_key: heap
  identity_column: email
  properties_map:
    plan_tier: plan_tier
    health_score: health_score
  group_name: reverse_etl
```
