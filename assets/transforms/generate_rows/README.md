# GenerateRowsComponent

Expand a DataFrame by repeating existing rows, appending fixed new rows, or
performing a cross join (cartesian product) with a set of new rows.

## Use Cases

- Repeat each row N times for stress-testing downstream pipelines
- Append reference rows or default records to a DataFrame
- Cross-join products with regions to generate a full product-region matrix
- Generate synthetic data by combining an existing dataset with parameter sets

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `asset_name` | `str` | Yes | — | Output Dagster asset name |
| `upstream_asset_key` | `str` | Yes | — | Upstream asset key providing a DataFrame |
| `mode` | `str` | No | `"repeat"` | Expansion mode: `"repeat"`, `"append"`, or `"cross_join"` |
| `n` | `int` | No | `1` | For `mode="repeat"`: number of times to repeat each row |
| `new_rows` | `List[dict]` | No | `None` | For `mode="append"` or `"cross_join"`: rows to use |
| `reset_index` | `bool` | No | `True` | Reset the index after expansion |
| `group_name` | `str` | No | `None` | Dagster asset group name |

## Modes

| Mode | Description |
|------|-------------|
| `repeat` | Concatenate the DataFrame `n` times (each row repeated) |
| `append` | Concatenate the DataFrame with `new_rows` appended at the end |
| `cross_join` | Cartesian product: every upstream row paired with every row in `new_rows` |

## Example YAML

```yaml
type: dagster_component_templates.GenerateRowsComponent
attributes:
  asset_name: products_by_region
  upstream_asset_key: products
  mode: cross_join
  new_rows:
    - region: North
    - region: South
    - region: East
    - region: West
  reset_index: true
  group_name: product_catalog
```

### Repeat Example

```yaml
type: dagster_component_templates.GenerateRowsComponent
attributes:
  asset_name: events_x5
  upstream_asset_key: raw_events
  mode: repeat
  n: 5
  group_name: load_testing
```

## Inputs and Outputs

| | Type | Description |
|-|------|-------------|
| Input | `pd.DataFrame` | The upstream DataFrame to expand |
| Output | `pd.DataFrame` | Expanded DataFrame |

## IO Manager Note

This component returns a plain `pd.DataFrame`. Ensure your Dagster environment
has an IO manager configured that can handle DataFrames.

## Notes

- For `cross_join`, a temporary `_key` column is added and removed automatically.
- If `mode` is `"append"` or `"cross_join"` but `new_rows` is not set, the DataFrame is returned unchanged with a warning.
- Output size for `cross_join` is `len(upstream) * len(new_rows)`.
