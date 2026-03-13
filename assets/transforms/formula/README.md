# Formula Component

Create or update columns using pandas eval expressions. Enables calculated fields — totals, margins, ratios, and derived metrics — without writing custom Python transformation code.

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `expressions` | `dict` | required | Mapping of `{output_column: expression_string}` using pandas eval syntax |
| `drop_source_columns` | `Optional[List[str]]` | `None` | Columns to drop after applying formulas |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.FormulaComponent
attributes:
  asset_name: orders_with_metrics
  upstream_asset_key: raw_orders
  expressions:
    total_revenue: "price * quantity"
    margin_pct: "(revenue - cost) / revenue * 100"
    discount_amount: "list_price - price"
  drop_source_columns:
    - list_price
  group_name: calculations
```

## Expression Syntax

Expressions use pandas eval syntax. Column names are referenced directly by name. Standard arithmetic operators (`+`, `-`, `*`, `/`, `**`, `%`) and comparison operators are supported.

Examples:

| Output Column | Expression |
|---|---|
| `total` | `"price * quantity"` |
| `margin_pct` | `"(revenue - cost) / revenue * 100"` |
| `is_large` | `"amount > 1000"` |
| `full_name` | `"first_name + ' ' + last_name"` |

If `pandas.eval()` raises an exception (e.g. for string concatenation), the component falls back to Python's built-in `eval()` with DataFrame columns injected as locals.

## Ordering

Expressions are evaluated in dictionary insertion order. Later expressions can reference columns created by earlier expressions in the same component.

## Notes

- Setting an expression to an existing column name will overwrite that column.
- Use `drop_source_columns` to clean up intermediate columns after formula application.
- Errors in individual expressions raise immediately and halt execution.
