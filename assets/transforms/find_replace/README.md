# FindReplace

Map values in a target column of the main DataFrame against a reference lookup DataFrame, replacing matched values with their corresponding replacements. Useful for code-to-label translations, category remapping, and value standardization.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Main DataFrame asset key |
| `lookup_asset_key` | `str` | required | Reference/lookup DataFrame asset key |
| `lookup_key_column` | `str` | required | Column in the lookup table to match against |
| `lookup_value_column` | `str` | required | Column in the lookup table containing replacement values |
| `target_column` | `str` | required | Column in the main DataFrame to look up |
| `output_column` | `Optional[str]` | `null` | Output column name (defaults to overwriting `target_column`) |
| `default_value` | `Optional[str]` | `null` | Value for unmatched rows (defaults to the original value) |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Behavior

- The lookup table is converted into a dictionary keyed by `lookup_key_column` with values from `lookup_value_column`.
- Values in `target_column` are mapped through this dictionary.
- If a value has no match: `default_value` is used when set; otherwise the original value is kept.
- If `output_column` is null, the mapped values overwrite `target_column` in place.

## YAML Example

```yaml
type: dagster_component_templates.FindReplace
attributes:
  asset_name: orders_with_region_names
  upstream_asset_key: orders
  lookup_asset_key: region_codes
  lookup_key_column: code
  lookup_value_column: region_name
  target_column: region_code
  output_column: region_name
  default_value: Unknown
  group_name: transforms
```

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.
