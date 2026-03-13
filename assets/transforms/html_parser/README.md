# HtmlParserComponent

Strip HTML tags from text columns or extract specific elements. Uses `beautifulsoup4`.

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `upstream_asset_key` | string | yes | — | Upstream asset key providing a DataFrame |
| `group_name` | string | no | null | Dagster asset group name |
| `columns` | array of strings | yes | — | Columns to process |
| `mode` | string (enum) | no | `"strip_tags"` | `strip_tags`, `extract_text`, `extract_links`, `extract_tables` |
| `parser` | string (enum) | no | `"html.parser"` | `html.parser`, `lxml`, `html5lib` |
| `new_column_suffix` | string | no | null | Write to new columns with this suffix instead of overwriting |

## Example

```yaml
component_type: dagster_component_templates.HtmlParserComponent
asset_name: parsed_html_content
upstream_asset_key: raw_web_pages
columns: [body, description]
mode: strip_tags
parser: html.parser
new_column_suffix: _text
```
