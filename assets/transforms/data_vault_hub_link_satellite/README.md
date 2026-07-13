# DataVaultHubLinkSatelliteComponent

Take a raw source DataFrame and generate **Data Vault 2.0** Hub / Link / Satellite tables with all the standard system columns. Emits 2–3 assets per config:

- `<entity>_hub` — unique business keys + `hash_key` + `load_date` + `record_source`
- `<entity>_sat` — descriptive attributes + `hash_key` + `hash_diff` + `load_date` + `record_source`
- `<entity>_link` (optional) — combines hash keys of two-or-more hubs to represent a relationship

Each asset is **independently materializable** via SDA — you see full lineage back to the raw source, and you can partition / schedule / test each layer separately. This is the SDA-vs-task-scheduling story for DV2.0 modeling: instead of one big DAG task that populates raw → hub → sat → link, you get four Dagster assets with proper dependency + observability.

## Example — Customer entity

```yaml
type: dagster_community_components.DataVaultHubLinkSatelliteComponent
attributes:
  entity: customer
  upstream_asset_key: raw_customers
  business_keys: [customer_id]
  satellite_columns: [name, email, phone, address, updated_at]
  record_source: "erp_customers"
  group_name: data_vault_raw
```

Emits `customer_hub` + `customer_sat`.

## Example — Customer↔Order link

```yaml
attributes:
  entity: customer_order
  upstream_asset_key: raw_customer_orders
  business_keys: [customer_id, order_id]
  link_business_keys: [customer_id, order_id]
  satellite_columns: [order_status, order_amount, updated_at]
  record_source: "erp_orders"
```

Emits `customer_order_hub` + `customer_order_link` + `customer_order_sat`.

## System columns

| Column | Type | Notes |
|---|---|---|
| `hash_key` | string | SHA-256 (default) or MD5 of the business-key columns |
| `hash_diff` | string | Only on satellites — hash of descriptive columns, for change detection |
| `load_date` | timestamp | UTC materialization timestamp |
| `record_source` | string | Configurable source tag |

## Chaining

Common downstream shape:
- Raw source ingestion (e.g. `mssql_workspace` or `oracle_ingestion`) → `data_vault_hub_link_satellite` → downstream marts via `dataframe_to_bigquery` / `warehouse_pipeline` / dbt.
