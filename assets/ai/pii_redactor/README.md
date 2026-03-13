# PiiRedactorComponent

Detect and redact PII from text columns using Microsoft Presidio, replacing entities with type placeholders, masks, hashes, or synthetic values.

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `upstream_asset_key` | string | yes | — | Upstream asset key providing a DataFrame |
| `group_name` | string | no | null | Dagster asset group name |
| `text_column` | string | yes | — | Column containing text to redact |
| `output_column` | string | no | null | Output column (defaults to overwriting text_column) |
| `entity_types` | array of strings | no | null (all) | Filter to specific entity types |
| `replacement_style` | enum | no | `"placeholder"` | `placeholder`, `mask`, `hash`, or `synthetic` |
| `language` | string | no | `"en"` | Language of the text |
| `score_threshold` | number | no | `0.5` | Minimum confidence score |

## Replacement styles

- `placeholder`: Replaces with `<ENTITY_TYPE>` e.g. `<PERSON>`
- `mask`: Replaces with `****`
- `hash`: Replaces with SHA256 hash prefix
- `synthetic`: Falls back to `<ENTITY_TYPE>` placeholder

## Example

```yaml
component_type: dagster_component_templates.PiiRedactorComponent
asset_name: redacted_messages
upstream_asset_key: raw_user_messages
text_column: message_body
output_column: redacted_body
replacement_style: placeholder
score_threshold: 0.5
```
