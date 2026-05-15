# PctChangeComponent

Period-over-period diff and percent change on a value column. Right for week-over-week / month-over-month growth metrics — what you'd write as `df.groupby(...)[col].pct_change()` in pandas, but exposed as a declarative YAML asset.

## Output

Adds two columns to the input DataFrame (names configurable):

- `<value>_diff` — absolute change vs the row N periods back (NaN for the first row in each group)
- `<value>_pct` — percent change vs N periods back (ratio form `0.05` by default, or `5.0` with `as_percent: true`)

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | `str` | Yes | — | Output asset name |
| `upstream_asset_key` | `str` | Yes | — | Upstream DataFrame asset |
| `value_column` | `str` | Yes | — | Column to compute change on |
| `order_by` | `str` | No | — | Sort column (typically a date) before computing diff |
| `group_by` | `List[str]` | No | — | Compute change *within* each group (e.g. per region) |
| `periods` | `int` | No | `1` | Lag periods. `1` = vs previous row; `7` = vs 7 rows back |
| `diff_column` | `str` | No | `<value>_diff` | Override the diff output column name |
| `pct_column` | `str` | No | `<value>_pct` | Override the pct output column name |
| `as_percent` | `bool` | No | `false` | `false` = ratio (0.05), `true` = scaled (5.0) |
| `fill_first` | `bool` | No | `false` | If `true`, fill leading NaNs with 0 |

## Examples

Week-over-week revenue growth per region:

```yaml
type: dagster_component_templates.PctChangeComponent
attributes:
  asset_name: revenue_wow
  upstream_asset_key: daily_revenue
  value_column: revenue
  order_by: date
  group_by: [region]
  periods: 7
  as_percent: true
```

Single-series MoM:

```yaml
type: dagster_component_templates.PctChangeComponent
attributes:
  asset_name: total_revenue_mom
  upstream_asset_key: monthly_totals
  value_column: revenue
  order_by: month
  as_percent: true
```

## When to use this vs alternatives

- `window_calculation` — full window-function toolkit (lag/lead/row_number/rank). Use when you need more than just diff+pct, or when you want both the lagged value AND the pct change.
- `running_total` — cumulative running sum/avg/max. Different operation (running aggregate, not period change).
- `formula` — for the truly custom case. `pct_change` is a wrapper around the two-line common case so you don't write it by hand.

## See also

- `window_calculation`, `running_total`, `formula`, `summarize`
