# EmailParserComponent

Parse raw email content (RFC 2822 format) from a column into structured fields. Uses Python's built-in `email` library — no extra dependencies required.

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `upstream_asset_key` | string | yes | — | Upstream asset key providing a DataFrame |
| `group_name` | string | no | null | Dagster asset group name |
| `column` | string | yes | — | Column containing raw email strings |
| `extract_fields` | array of strings | no | `["from","to","subject","date","body"]` | Fields to extract |
| `output_prefix` | string | no | `""` | Prefix for output column names |
| `drop_source` | boolean | no | false | Drop the source column after parsing |

## Example

```yaml
component_type: dagster_component_templates.EmailParserComponent
asset_name: parsed_emails
upstream_asset_key: raw_email_messages
column: raw_email
extract_fields: [from, to, subject, date, body]
output_prefix: "email_"
drop_source: false
```
