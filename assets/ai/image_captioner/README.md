# ImageCaptionerComponent

Generate captions for images from a file path or URL column using a vision-capable LLM via LiteLLM. Supports any model with vision capabilities including GPT-4o, Claude 3, and Gemini.

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `upstream_asset_key` | string | yes | — | Upstream asset key providing a DataFrame |
| `group_name` | string | no | null | Dagster asset group name |
| `image_path_column` | string | yes | — | Column with image file paths or URLs |
| `output_column` | string | no | `"caption"` | Column to write caption |
| `model` | string | no | `"gpt-4o-mini"` | Vision-capable LLM model |
| `prompt` | string | no | `"Describe this image concisely."` | Prompt sent with each image |
| `max_tokens` | integer | no | `200` | Max response tokens |
| `api_key_env_var` | string | no | null | Env var name for API key |

## Example

```yaml
component_type: dagster_component_templates.ImageCaptionerComponent
asset_name: captioned_images
upstream_asset_key: image_file_records
image_path_column: image_path
model: gpt-4o-mini
prompt: "Describe this image concisely."
api_key_env_var: OPENAI_API_KEY
```
