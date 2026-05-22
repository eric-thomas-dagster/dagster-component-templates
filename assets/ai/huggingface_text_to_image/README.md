# HuggingFace Text-to-Image

Generate images from text prompts via [`huggingface_hub.InferenceClient.text_to_image()`](https://huggingface.co/docs/huggingface_hub/package_reference/inference_client#huggingface_hub.InferenceClient.text_to_image). Supports multi-provider routing through HuggingFace (`wavespeed`, `falai`, `replicate`, `together`, etc.) so you can land on the cheapest or fastest backend without rewriting Dagster code.

Each prompt becomes one PNG saved to `output_dir`. The asset returns a `MaterializeResult` with the list of saved file paths in metadata.

## Examples

### FLUX.1-dev via wavespeed (fast generator)

```yaml
type: dagster_community_components.HuggingfaceTextToImageComponent
attributes:
  asset_key: hf/images/airship
  model: black-forest-labs/FLUX.1-dev
  provider: wavespeed
  prompts:
    - "A steampunk airship in the clouds"
    - "A cyberpunk city at night, neon reflections in rain puddles"
  output_dir: ./generated_images
  hf_token_env_var: HF_TOKEN
```

### Stable Diffusion XL, default provider

```yaml
type: dagster_community_components.HuggingfaceTextToImageComponent
attributes:
  asset_key: hf/images/landscape
  model: stabilityai/stable-diffusion-xl-base-1.0
  prompts:
    - "A serene mountain landscape at sunset, photorealistic"
  output_dir: ./generated_images
  hf_token_env_var: HF_TOKEN
```

### With explicit generation parameters

```yaml
type: dagster_community_components.HuggingfaceTextToImageComponent
attributes:
  asset_key: hf/images/marketing_hero
  model: black-forest-labs/FLUX.1-dev
  provider: wavespeed
  prompts:
    - "A modern co-working space with diverse professionals"
  output_dir: ./generated_images
  generation_kwargs:
    guidance_scale: 7.5
    num_inference_steps: 30
    width: 1024
    height: 1024
  hf_token_env_var: HF_TOKEN
```

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_key` | `str` | ✓ | — | Dagster asset key |
| `model` | `str` | ✓ | — | Text-to-image HF Hub model id |
| `prompts` | `List[str]` | ✓ | — | One PNG generated per prompt |
| `provider` | `str` | — | — | Inference provider (wavespeed / falai / replicate / together / …) |
| `output_dir` | `str` | — | `./generated_images` | Where to write PNGs |
| `filename_template` | `str` | — | `{asset}_{index}_{slug}.png` | Filename template |
| `hf_token_env_var` | `str` | — | `HF_TOKEN` | Env var with HF token |
| `generation_kwargs` | `Dict` | — | — | Extra kwargs for `text_to_image()` |
| `group_name`, `description`, `owners`, `asset_tags`, `kinds`, `deps` | — | — | — | Standard catalog metadata |

## Materialization metadata

| Key | Description |
|---|---|
| `model` / `provider` | Configured model + routing provider |
| `output_dir` | Absolute path of the output directory |
| `prompt_count` / `saved_count` / `failed_count` | Per-batch stats |
| `saved_paths` | Markdown list of every saved image file |
| `errors` | Markdown list of `(prompt, error)` for any failures |

## Requirements

```
huggingface-hub>=0.20.0
Pillow>=10.0.0
```

## See also

- [HuggingFace Inference Providers documentation](https://huggingface.co/docs/inference-providers/index)
- [`huggingface_pipeline`](https://dagster-component-ui.vercel.app/c/huggingface_pipeline) — `transformers.pipeline()` tasks
- [`huggingface_chat_completion`](https://dagster-component-ui.vercel.app/c/huggingface_chat_completion) — chat models
- [`litellm_image_generation`](https://dagster-component-ui.vercel.app/c/litellm_image_generation) — multi-provider image gen via LiteLLM
