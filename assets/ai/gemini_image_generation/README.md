# Gemini Image Generation (a.k.a. Nano Banana)

Native, single-vendor image generation / editing component for Google's Gemini 2.5 Flash Image — the model the community calls **"Nano Banana"**. Goes directly through the `google-genai` SDK; no LiteLLM dependency.

For multi-vendor / model-switching workflows, see [`litellm_image_generation`](../litellm_image_generation/README.md). Both components have the same field shape so swapping is straightforward.

## When to use this vs. `litellm_image_generation`

| Use this when... | Use `litellm_image_generation` when... |
|---|---|
| Your stack is Google-only and you don't want a LiteLLM dep | You multiplex across DALL-E, Stable Diffusion, Replicate, etc. |
| You want to surface Gemini-specific features (response_modalities, edit-with-context) | You want a single config to switch vendors via `model:` |
| You need image-to-image editing where a source image is part of the request | You only need text-to-image |

## Required packages

```
dagster>=1.8.0
pandas>=1.5.0
google-genai>=0.3.0
```

## Required env var

```bash
GEMINI_API_KEY=...      # or GOOGLE_API_KEY (component falls back to this)
```

Get a key at https://aistudio.google.com/app/apikey.

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `asset_name` | yes | — | Output asset name |
| `upstream_asset_key` | yes | — | Upstream DataFrame asset key — one row = one image |
| `api_key_env_var` | no | `GEMINI_API_KEY` | Env var holding the API key |
| `image_model` | no | `gemini-2.5-flash-image-preview` | Gemini image model id |
| `prompt_column` | one of these required | — | Column with per-row prompt text |
| `prompt_template` | one of these required | — | Static template with `{column}` placeholders |
| `input_image_column` | no | — | Optional column with source-image file paths (image-to-image edit mode) |
| `output_dir` | no | `/tmp/gemini_image_generation` | Where to save generated PNGs |
| `output_path_column` | no | `generated_image_path` | Column added with the saved file path |
| `output_filename_template` | no | `row_{idx}.png` | Per-row filename pattern |
| `temperature` | no | `1.0` | Generation temperature (0.0–2.0) |
| `rate_limit_delay` | no | `0.5` | Seconds between API calls |
| `max_retries` | no | `3` | Retries on transient errors |
| `description` / `group_name` / `deps` / `tags` / `owners` | no | — | Standard Dagster asset attributes |
| `partition_*` | no | — | Canonical registry partition shape |

## Modes

### Text-to-image (default)

```yaml
prompt_column: description
```

### Image-to-image edit

```yaml
prompt_column: edit_instruction
input_image_column: source_image_path     # column of file paths
```

The component sends `[prompt, source_image_bytes]` as the request contents — Gemini interprets the source image as the canvas and the prompt as edit instructions.

## Failed rows

If a row fails (rate limit exhausted, no inline image returned, transient error after retries), the component:

- Writes `None` into `output_path_column` for that row.
- Adds an `<output_path_column>_error` column with the error message.
- Logs the failure but **continues processing** — one bad row doesn't fail the whole asset.

## Example

```yaml
type: dagster_component_templates.GeminiImageGenerationComponent
attributes:
  asset_name: product_hero_images
  upstream_asset_key: product_descriptions
  api_key_env_var: GEMINI_API_KEY
  prompt_column: description
  output_dir: /tmp/product_hero_images
  group_name: ai_media
```

## Why "Nano Banana"?

Google launched `gemini-2.5-flash-image` (preview) under the codename *Nano Banana*. The nickname stuck because of how strong it was at fine-grained image edits at launch. The model id is configurable, so when Google ships GA / next-gen models you just change `image_model:`.
