# Polars Pipeline

Run a multi-step polars LazyFrame chain inside a **single** Dagster asset. Polars's query planner fuses filters, eliminates intermediate columns, and parallelizes the whole sequence — but only **within** one lazy chain.

## Why this exists

`summarize(backend=polars)` / `filter(backend=polars)` / etc. each run one operation per Dagster asset. Spread across separate assets, the asset boundary forces materialization at every step — the lazy chain is broken, and there's no fusion or pushdown across steps.

This component takes a **sequence of polars operations** and runs them as one chain. You get polars's full query optimization, at the cost of coarser lineage (one asset for the whole sequence).

| | Per-asset transforms (`backend: polars`) | This component |
|---|---|---|
| Lineage granularity | Per step | Per pipeline |
| Polars query-plan fusion | No (asset boundary breaks chain) | Yes |
| Predicate pushdown across steps | No | Yes (within chain) |
| Streaming engine | No | Optional (`streaming: true`) |
| Cross-asset reuse of intermediate | Yes (any asset can consume) | No |

Rule of thumb: if each step deserves its own catalog entry + lineage, use per-asset. If the steps are tightly coupled and you want throughput, use this.

## YAML

```yaml
type: dagster_component_templates.PolarsPipelineComponent
attributes:
  asset_name: top_customers_by_region
  upstream_asset_key: raw_orders
  operations:
    - op: filter
      predicate: "status = 'paid'"
    - op: with_columns
      expressions:
        net_amount: "amount - tax"
    - op: group_by
      group_by: [region, customer_id]
      aggregations:
        total:       {col: net_amount, agg: sum}
        order_count: {col: order_id,   agg: count}
    - op: sort
      by: [region, total]
      descending: [false, true]
    - op: head_per_group
      group_by: [region]
      n: 5
  output_type: polars
  group_name: analytics
```

## Supported operations

Each entry in `operations:` is `{op: <kind>, ...op-specific params}`:

| op | Params | Notes |
|---|---|---|
| `filter` | `predicate: "<sql>"` | SQL predicate via polars's SQLContext |
| `with_columns` | `expressions: {name: <sql_expr>}` | Add/replace columns from SQL expressions |
| `select` | `columns: [a, b]` | Keep only these columns |
| `drop` | `columns: [a, b]` | Drop these columns |
| `rename` | `mapping: {old: new}` | Rename columns |
| `group_by` | `group_by: [cols]`, `aggregations: {out: {col, agg}}` | Same agg shape as `summarize` |
| `sort` | `by: [cols]`, `descending: bool/list` | Sort |
| `head` / `tail` | `n: int` | Take first/last N rows |
| `head_per_group` | `group_by: [cols]`, `n: int` | Top-N per group; sort first if order matters |
| `unique` | `subset: [cols]`, `keep: first/last/none` | Dedup |
| `drop_nulls` | `subset: [cols]` (optional) | Drop rows with null in subset |
| `fill_null` | `value: <any>` | Fill nulls |
| `cast` | `mapping: {col: 'Int64'}` | Type cast |

Supported `agg` values in `group_by`: `sum / mean / avg / min / max / count / median / std / var / first / last / nunique`.

## Streaming

Set `streaming: true` to use polars's streaming engine on the final `.collect()`. Works for out-of-core frames (larger than memory). No syntactic change to operations.

## Output

`output_type: polars` (default) returns a polars DataFrame so downstream polars-aware components can chain. `output_type: pandas` converts at the boundary for pandas-only downstream consumers.
