# HuggingFace Pipeline

Run any [`transformers.pipeline()`](https://huggingface.co/docs/transformers/en/main_classes/pipelines) task on a list of inputs and surface the per-input results as Dagster catalog metadata. **No DataFrame, no upstream asset required** — just a list of strings, URLs, or file paths.

## Two execution modes

| Mode | Engine | When to use |
|---|---|---|
| `local` | `transformers.pipeline()` in-process | Free; downloads the model on first run. Best for small models, prototyping, or when you have GPU available |
| `inference_api` | `huggingface_hub.InferenceClient` (HF-hosted) | Zero local compute; works with any deployed Hub model. Some models are pay-as-you-go |

## When to use this vs. the task-specific components

`huggingface_pipeline` is the **fastest path** for "demo HuggingFace in 5 minutes" — no upstream DataFrame asset, no pandas in your project.

For **batch inference over a column of inputs**, use the task-specific components instead — they integrate with the rest of the DataFrame-centric registry:

| HF task | Task-specific component (DataFrame batch) |
|---|---|
| object-detection | [`image_object_detector`](https://dagster-component-ui.vercel.app/c/image_object_detector) |
| image-classification | [`image_classifier`](https://dagster-component-ui.vercel.app/c/image_classifier) |
| text-classification | [`text_classifier`](https://dagster-component-ui.vercel.app/c/text_classifier) / [`sentiment_analyzer`](https://dagster-component-ui.vercel.app/c/sentiment_analyzer) |
| zero-shot-classification | [`zero_shot_classifier`](https://dagster-component-ui.vercel.app/c/zero_shot_classifier) |
| automatic-speech-recognition | [`audio_transcriber`](https://dagster-component-ui.vercel.app/c/audio_transcriber) |

## Examples

### Sentiment classification (local mode)

```yaml
type: dagster_community_components.HuggingfacePipelineComponent
attributes:
  asset_key: hf/sentiment
  task: text-classification
  model: cardiffnlp/twitter-roberta-base-sentiment-latest
  mode: local
  inputs:
    - "I love this product!"
    - "Terrible experience, would not recommend."
    - "It's fine. Nothing special."
```

### Object detection on URLs (Inference API mode)

```yaml
type: dagster_community_components.HuggingfacePipelineComponent
attributes:
  asset_key: hf/detect_objects
  task: object-detection
  model: facebook/detr-resnet-50
  mode: inference_api
  hf_token_env_var: HF_TOKEN
  inputs:
    - https://huggingface.co/datasets/mishig/sample_images/resolve/main/cats.jpg
    - https://huggingface.co/datasets/mishig/sample_images/resolve/main/dogs.jpg
```

### Zero-shot classification with explicit labels

```yaml
type: dagster_community_components.HuggingfacePipelineComponent
attributes:
  asset_key: hf/topic_classification
  task: zero-shot-classification
  model: facebook/bart-large-mnli
  mode: local
  inputs:
    - "The new iPhone has incredible battery life."
    - "Inflation is the highest it's been in 40 years."
    - "The team came back from 3 goals down to win."
  call_kwargs:
    candidate_labels: ["technology", "finance", "sports", "politics"]
```

### Audio transcription (Whisper, local with GPU)

```yaml
type: dagster_community_components.HuggingfacePipelineComponent
attributes:
  asset_key: hf/transcribe
  task: automatic-speech-recognition
  model: openai/whisper-small
  mode: local
  inputs:
    - /local/path/to/audio.mp3
  pipeline_kwargs:
    device: 0  # GPU 0
```

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_key` | `str` | Dagster asset key |
| `task` | `str` | HuggingFace pipeline task name |
| `model` | `str` | HF Hub model id |
| `inputs` | `List[str]` | List of inputs to run inference on |

### Optional

| Field | Type | Default | Description |
|---|---|---|---|
| `mode` | `"local"` \| `"inference_api"` | `"local"` | Execution backend |
| `hf_token_env_var` | `str` | — | Env var with HF token (required for inference_api + gated models) |
| `pipeline_kwargs` | `Dict[str, Any]` | — | Extra kwargs for `transformers.pipeline()` |
| `call_kwargs` | `Dict[str, Any]` | — | Extra kwargs for each pipeline call |
| `group_name` | `str` | `"huggingface"` | Dagster asset group name |
| `owners`, `asset_tags`, `kinds`, `deps`, `description` | — | — | Standard catalog metadata |

## Materialization metadata

Each run records:

| Key | Description |
|---|---|
| `task` / `model` / `mode` | The configured task, model id, and execution mode |
| `input_count` | Number of inputs processed |
| `success_count` | Inputs that returned a result |
| `failure_count` | Inputs that raised an exception (the asset still materializes; failures land as `{"error": "..."}` in the preview) |
| `preview` | JSON preview of up to the first 5 results |

## Requirements

```
huggingface-hub>=0.20.0
# For mode=local:
# transformers>=4.30.0
# torch>=2.0.0
# Pillow>=10.0.0  # for image tasks
```

`transformers` + `torch` are NOT required by default — only when `mode: local` is used. The component raises a clear `ImportError` with the install command if you select a mode whose dependencies aren't installed.

## See also

- [HuggingFace pipelines documentation](https://huggingface.co/docs/transformers/en/main_classes/pipelines)
- [`huggingface_dataset_asset`](https://dagster-component-ui.vercel.app/c/huggingface_dataset_asset) — observe a HF Hub dataset's metadata
- [`huggingface_model_asset`](https://dagster-component-ui.vercel.app/c/huggingface_model_asset) — observe a HF Hub model's metadata
- [`huggingface_inference_endpoint`](https://dagster-component-ui.vercel.app/c/huggingface_inference_endpoint) — call your own deployed HF Inference Endpoint
