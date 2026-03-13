# MarkdownStripperComponent

Strip markdown formatting from text columns, converting to plain text or HTML. Uses the `markdown` library with a regex fallback for `strip` mode.

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `upstream_asset_key` | string | yes | — | Upstream asset key providing a DataFrame |
| `group_name` | string | no | null | Dagster asset group name |
| `columns` | array of strings | yes | — | Columns to process |
| `mode` | string (enum) | no | `"strip"` | `strip`, `to_html`, `to_plaintext` |
| `new_column_suffix` | string | no | null | Write to new columns with this suffix instead of overwriting |

## Example

```yaml
component_type: dagster_component_templates.MarkdownStripperComponent
asset_name: stripped_markdown_docs
upstream_asset_key: raw_markdown_docs
columns: [content, summary]
mode: strip
new_column_suffix: _plain
```
