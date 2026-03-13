# Image LLM Extractor Component

Send images to a vision LLM (GPT-4o, Claude, Gemini) and extract structured fields as new DataFrame columns. Like an invoice extractor but for any image type — define the fields you want and the LLM fills them in.

## Overview

The Image LLM Extractor Component sends each image to a vision-capable large language model via litellm and asks it to extract configurable structured fields. Results are parsed from JSON and added as new columns to the DataFrame. Supports file path inputs (base64-encoded) or direct URLs.

## Features

- **Flexible field extraction**: Define any fields with descriptions
- **Vision LLM support**: Works with GPT-4o, Claude 3, Gemini via litellm
- **File or URL input**: Base64-encode local files or pass URLs directly
- **Optional prompt prefix**: Add domain-specific instructions
- **Null-safe**: Missing fields become `None`

## Use Cases

1. **Product image analysis**: Extract name, price, brand, color from product photos
2. **Receipt parsing**: Extract merchant, total, date from receipt images
3. **Real estate images**: Extract room type, features, condition
4. **Medical imaging metadata**: Extract anatomical region, modality description

## Prerequisites

- `litellm>=1.0.0`, `Pillow>=9.0.0`
- API key for your chosen vision model provider

## Configuration

### Product Image Extraction

```yaml
type: dagster_component_templates.ImageLlmExtractorComponent
attributes:
  asset_name: product_data
  upstream_asset_key: product_images
  image_column: image_path
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  extraction_fields:
    product_name: name or title of the product
    price: visible price including currency
    brand: brand name or logo
    color: primary color of the product
```

### Using Claude with URL Images

```yaml
type: dagster_component_templates.ImageLlmExtractorComponent
attributes:
  asset_name: image_analysis
  upstream_asset_key: image_urls
  image_column: url
  input_type: url
  model: claude-3-5-sonnet-20241022
  api_key_env_var: ANTHROPIC_API_KEY
  extraction_fields:
    scene_type: type of scene depicted
    dominant_color: main color in the image
    has_text: whether the image contains text
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `image_column` | string | required | Column with image paths or URLs |
| `extraction_fields` | object | required | `{column_name: description}` mapping |
| `prompt_prefix` | string | `None` | Extra instruction for the LLM |
| `model` | string | `gpt-4o-mini` | Vision LLM model name |
| `max_tokens` | integer | `500` | Max response tokens |
| `api_key_env_var` | string | `OPENAI_API_KEY` | Env var for API key |
| `input_type` | string | `file` | `file` or `url` |
| `group_name` | string | `None` | Asset group |

## Output

Original DataFrame plus one column per extraction field.
