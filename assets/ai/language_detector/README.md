# LanguageDetectorComponent

Detect the language of text in a column and output ISO 639-1 language codes (e.g. "en", "es", "fr"). Supports multiple backends.

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `upstream_asset_key` | string | yes | — | Upstream asset key providing a DataFrame |
| `group_name` | string | no | null | Dagster asset group name |
| `text_column` | string | yes | — | Column containing text to analyze |
| `output_column` | string | no | `"language"` | Column to write language code |
| `confidence_column` | string | no | null | Column to write confidence score (optional) |
| `backend` | enum | no | `"langdetect"` | `langdetect`, `fasttext`, or `lingua` |
| `fallback_language` | string | no | `"unknown"` | Value when detection fails |

## Example

```yaml
component_type: dagster_component_templates.LanguageDetectorComponent
asset_name: language_detected_text
upstream_asset_key: raw_text_data
text_column: content
output_column: language
confidence_column: language_confidence
backend: langdetect
```
