# BigQuery CTAS

Materialize a SELECT as a real BigQuery table / view / materialized view. The transform layer of any BQ-native ELT.

```yaml
type: dagster_component_templates.BigQueryCreateTableFromQueryAssetComponent
attributes:
  asset_name: orders_clean
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
  destination_table_id: my-project.analytics.orders_clean
  materialization: table          # table | view | materialized_view
  query: |
    SELECT order_id, customer_id, amount, DATE(created_at) AS order_date
    FROM `my-project.raw.orders`
    WHERE status = 'completed'
  partition_field: order_date
  cluster_fields: [customer_id]
  table_options:
    description: "Cleaned orders"
    labels: {tier: gold}
```

The asset's stored value is a one-row DataFrame with destination metadata (row_count, table_size_bytes, bytes_billed, columns). Downstream components reference the BQ table by id, not the DataFrame contents.

Required SA roles: `roles/bigquery.dataEditor` on the destination dataset + `roles/bigquery.jobUser` on the project (or `roles/bigquery.admin` / `roles/owner`).
