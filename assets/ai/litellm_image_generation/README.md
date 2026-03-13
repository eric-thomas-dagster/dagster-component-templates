# LiteLLM Image Generation

Generate images from text prompts in a DataFrame column using LiteLLM. Supports DALL-E, Stable Diffusion, and other image generation models.

## Overview

`LitellmImageGenerationComponent` reads an upstream Dagster asset as a DataFrame, generates images for each row's prompt using `litellm.image_generation()`, and writes image URLs or base64-encoded data to a new column.

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `asset_name` | string | required | Output Dagster asset name |
| `upstream_asset_key` | string | required | Upstream asset key providing a DataFrame |
| `prompt_column` | string | required | Column containing image prompts |
| `output_column` | string | `"image_url"` | Column to write image URLs or base64 data |
| `model` | string | `"dall-e-3"` | Image generation model |
| `size` | enum | `"1024x1024"` | Image dimensions |
| `quality` | enum | `"standard"` | Image quality (standard or hd) |
| `response_format` | enum | `"url"` | Return format: url or b64_json |
| `api_key_env_var` | string | null | Env var name for API key |
| `group_name` | string | null | Dagster asset group name |

## Example

```yaml
type: dagster_component_templates.LitellmImageGenerationComponent
attributes:
  asset_name: product_hero_images
  upstream_asset_key: product_descriptions
  prompt_column: description
  model: dall-e-3
  size: 1024x1024
  quality: hd
  api_key_env_var: OPENAI_API_KEY
```

## Requirements

```
litellm>=1.0.0
pandas>=1.5.0
```
