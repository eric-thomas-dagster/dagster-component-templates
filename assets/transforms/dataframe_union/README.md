# DataframeUnion

Stack two or more upstream DataFrame assets vertically, equivalent to SQL UNION ALL. Supports any number of inputs via a list of asset keys, with configurable index behavior and column alignment strategy.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_keys` | `List[str]` | required | List of asset keys to union (two or more) |
| `ignore_index` | `bool` | `true` | Reset row index after concatenation |
| `join` | `str` | `"outer"` | `outer` keeps all columns (fills missing with NaN); `inner` keeps only columns common to all inputs |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Behavior

- Each asset key in `upstream_asset_keys` becomes an upstream dependency. Dagster resolves them as `input_0`, `input_1`, etc.
- DataFrames are concatenated in the order they appear in `upstream_asset_keys`.
- With `join: outer`, columns missing from some DataFrames are filled with `NaN`.
- With `join: inner`, only columns present in all DataFrames are retained.
- With `ignore_index: true`, the output row index is reset to a clean integer sequence.

## YAML Example

```yaml
type: dagster_component_templates.DataframeUnion
attributes:
  asset_name: all_transactions
  upstream_asset_keys:
    - transactions_q1
    - transactions_q2
    - transactions_q3
    - transactions_q4
  ignore_index: true
  join: outer
  group_name: transforms
```

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.
