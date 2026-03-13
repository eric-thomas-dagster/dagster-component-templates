# PiiDetectorComponent

Detect personally identifiable information (PII) in text columns using Microsoft Presidio. Each row gets a list of detected entity dicts with type, text span, position, and confidence score.

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `upstream_asset_key` | string | yes | — | Upstream asset key providing a DataFrame |
| `group_name` | string | no | null | Dagster asset group name |
| `text_column` | string | yes | — | Column containing text to analyze |
| `output_column` | string | no | `"pii_entities"` | Column to write detected entity dicts |
| `entity_types` | array of strings | no | null (all) | Filter to specific entity types |
| `language` | string | no | `"en"` | Language of the text |
| `score_threshold` | number | no | `0.5` | Minimum confidence score |
| `add_count_column` | boolean | no | `true` | Add `{output_column}_count` column |

## Output format

Each row in `output_column` contains a list of dicts:
```json
[{"type": "PERSON", "text": "John", "start": 0, "end": 4, "score": 0.85}]
```

## Example

```yaml
component_type: dagster_component_templates.PiiDetectorComponent
asset_name: pii_detected_text
upstream_asset_key: raw_user_messages
text_column: message_body
entity_types: [PERSON, EMAIL_ADDRESS, PHONE_NUMBER]
score_threshold: 0.5
add_count_column: true
```
