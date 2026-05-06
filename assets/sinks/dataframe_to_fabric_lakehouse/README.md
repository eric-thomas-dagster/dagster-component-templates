# DataFrame → Microsoft Fabric Lakehouse

Write a DataFrame to a Microsoft Fabric Lakehouse table as Delta on
OneLake. Tables auto-register in the Fabric SQL endpoint, so once written
they're queryable from Fabric Data Warehouse + Power BI + any T-SQL
client without additional setup.

## OneLake URL

The component constructs the abfss URL automatically:

```
abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/<table>
```

You provide `workspace_id`, `lakehouse_name`, `table_name` — the rest is
derived.

## Companion components

- `fabric_workspace` — discover lakehouses + warehouses + pipelines as
  Dagster external assets
- `dataframe_to_table` (existing) — for Fabric Data Warehouse via SQL
  endpoint (mssql+pyodbc URL pattern)

## Validation status

Code-validated against the OneLake URL spec + delta-rs azure storage
options. End-to-end validation requires a Fabric capacity + lakehouse
in your tenant.
