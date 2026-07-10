# TM1CubeDataIngestionComponent

Materialize a slice of a **TM1 cube** via MDX as a Dagster DataFrame asset. Turn multidimensional planning cube data into flat rows for downstream Dagster transforms, dbt models, or warehouse loads.

Pairs with [`tm1_resource`](../../../resources/tm1_resource/) for auth.

## Query modes

**MDX mode** — pass a full MDX statement:

```yaml
mdx: |
  SELECT
    NON EMPTY [Period].Members ON ROWS,
    NON EMPTY [Account].Members ON COLUMNS
  FROM [Finance]
  WHERE ([Version].[Actual], [Year].[2026])
```

**Simplified mode** — cube + row/column dims + filters:

```yaml
cube: Sales
row_dimensions: [Period]
column_dimensions: [Product]
filters: {Version: Actual, Year: "2026"}
```

The component generates the equivalent MDX for you. Good for "give me every combination of these dims" — the common warehouse-load pattern.

## Emitted schema

Each cell in the cellset becomes one row. Columns include:
- **One column per row/column-axis dimension** — populated with the member name for that cell
- `value` — the numeric measure
- `ordinal` — cell position (useful when reconstructing sparse cubes)

## Config reference

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | str | — | Asset key for the emitted DataFrame. |
| `cube` | str | — | TM1 cube name. |
| `mdx` | str | — | Explicit MDX. When set, ignores simplified fields. |
| `row_dimensions` | list[str] | — | Simplified: dims on rows. |
| `column_dimensions` | list[str] | — | Simplified: dims on columns. |
| `filters` | dict | — | Simplified: `{dimension: member}` WHERE clause. |
| `group_name` | str | — | Dagster asset group. |
| `resource_key` | str | `tm1_resource` | Resource key. |

## Chaining

Common downstream patterns:

- `dataframe_to_snowflake` — land the slice in a warehouse table for BI
- `pct_change` → `top_n_per_group` — planning cube analytics
- `dataframe_to_csv` — export for spreadsheet users

## Related

- [`tm1_resource`](../../../resources/tm1_resource/)
- [`tm1_process_trigger_job`](../../../jobs/tm1_process_trigger_job/) — run a TI process before ingesting
- [`tm1_workspace`](../../../integrations/tm1_workspace/) — auto-emit assets per cube (workspace shape)
