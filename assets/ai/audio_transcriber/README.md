# AudioTranscriberComponent

Transcribe audio files from a file path column using OpenAI Whisper (local model). Supports multiple model sizes from tiny to large and optional language hinting.

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `upstream_asset_key` | string | yes | — | Upstream asset key providing a DataFrame |
| `group_name` | string | no | null | Dagster asset group name |
| `audio_path_column` | string | yes | — | Column containing local audio file paths |
| `output_column` | string | no | `"transcription"` | Column to write transcription text |
| `language_column` | string | no | null | Column to write detected language |
| `model_size` | enum | no | `"base"` | `tiny`, `base`, `small`, `medium`, or `large` |
| `language` | string | no | null | Language hint to skip auto-detection |

## Example

```yaml
component_type: dagster_component_templates.AudioTranscriberComponent
asset_name: transcribed_audio
upstream_asset_key: audio_file_records
audio_path_column: file_path
output_column: transcription
model_size: base
```
