# Warehouse Multi-Field Formula (Alteryx equivalent)

Apply ONE formula template to N columns via CTAS. Saves writing the same expression N times when you want to e.g. uppercase every string column, round every numeric column, or trim whitespace across a set.

The expression template uses `{col}` as the placeholder for each column name.

## YAML

```yaml
type: dagster_component_templates.WarehouseMultiFieldFormulaComponent
attributes:
  asset_name: customers_normalized
  database_url: snowflake://...
  dialect: snowflake
  upstream_table: raw.customers
  output_table: analytics.customers_normalized
  expression: "UPPER(TRIM({col}))"
  columns: [name, email, address]
  output_mode: replace          # replace | add_suffix | add_prefix
  mode: replace
  deps: [raw_customers_in_warehouse]
```

## Output modes

| Mode | Behavior | Generated SQL shape |
|---|---|---|
| `replace` (default-ish) | New columns overwrite originals (same names). Requires `SELECT * EXCEPT ()` — works on DuckDB / BigQuery / Snowflake / Databricks. | `SELECT * EXCEPT (name, email), UPPER(TRIM(name)) AS name, UPPER(TRIM(email)) AS email FROM ...` |
| `add_suffix` | Original passes through; new columns named `<col><suffix>` (suffix defaults to `_calc`). Works on all dialects. | `SELECT *, UPPER(TRIM(name)) AS name_calc, UPPER(TRIM(email)) AS email_calc FROM ...` |
| `add_prefix` | Same as add_suffix but prefix instead. | `SELECT *, UPPER(TRIM(name)) AS calc_name, ... FROM ...` |

## Sibling components

- `warehouse_formula` — one inline expression per output column (each can be different)
- `warehouse_multi_row_formula` — row-relative formulas (LAG/LEAD/window functions) as a declarative DSL
