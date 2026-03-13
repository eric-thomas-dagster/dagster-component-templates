# RegexParser

Apply regular expressions to a DataFrame column to extract capture groups into new columns, produce boolean match indicators, substitute text, or split values into multiple rows. Supports all four common regex operation modes and optional regex flags.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `column` | `str` | required | Column to apply regex to |
| `pattern` | `str` | required | Regex pattern |
| `mode` | `str` | `"extract"` | Operation mode: `extract`, `match`, `replace`, or `split` |
| `replacement` | `Optional[str]` | `null` | Replacement string for mode `replace` |
| `output_columns` | `Optional[List[str]]` | `null` | Names for extracted capture groups (mode `extract`) |
| `output_column` | `Optional[str]` | `null` | Name for result column in `match` or `replace` mode |
| `flags` | `int` | `0` | Regex flags (0=none, 2=IGNORECASE) |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Modes

- **extract**: Uses `str.extract` to pull capture groups into new columns. Column names come from `output_columns` or are auto-generated as `{column}_extracted_0`, `{column}_extracted_1`, etc.
- **match**: Produces a boolean column indicating whether the pattern matches. Result goes into `output_column` (defaults to overwriting `column`).
- **replace**: Substitutes matches with `replacement`. Result goes into `output_column` (defaults to overwriting `column`).
- **split**: Splits each value by the pattern and explodes into separate rows.

## YAML Example

```yaml
type: dagster_component_templates.RegexParser
attributes:
  asset_name: extracted_phone_parts
  upstream_asset_key: raw_contacts
  column: phone_number
  pattern: "^(\\d{3})-(\\d{3})-(\\d{4})$"
  mode: extract
  replacement: null
  output_columns:
    - area_code
    - exchange
    - subscriber
  output_column: null
  flags: 0
  group_name: transforms
```

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.
