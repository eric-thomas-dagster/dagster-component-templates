# HuggingFace Model (observed)

Surfaces a [HuggingFace Hub model](https://huggingface.co/models) as a Dagster `observable_source_asset`. Each observation polls the Hub and emits an `ObserveResult` with downloads, likes, pipeline tag, last-modified, license, and file count.

Use it to track the version of a model that downstream inference assets depend on — e.g. re-run evals when `last_modified` advances.

## Example

```yaml
type: dagster_community_components.HuggingfaceModelAssetComponent
attributes:
  asset_key: hf/models/sentiment_model
  model_id: cardiffnlp/twitter-roberta-base-sentiment-latest
```

## Observation metadata

| Key | Description |
|---|---|
| `model_id` | Hub model id |
| `downloads` | All-time downloads |
| `likes` | Hub likes |
| `pipeline_tag` | Canonical pipeline task (`text-classification`, `object-detection`, …) |
| `library_name` | `transformers`, `diffusers`, `sentence-transformers`, etc. |
| `last_modified` | ISO timestamp of last commit |
| `private` / `gated` | Visibility flags |
| `file_count` | Number of files in the repo |
| `tags` | First 20 tags from the model card |
| `license` | License from the model card |
| `base_model` | Base model if this is a fine-tune |
| `hub_url` | Clickable URL to the model page |

If the Hub call fails, the observation still emits with `error` set.

## Wiring downstream

```yaml
# Downstream inference asset triggers re-eval when the model updates:
type: dagster_community_components.HuggingfacePipelineComponent
attributes:
  asset_key: hf/eval/sentiment_eval
  task: text-classification
  model: cardiffnlp/twitter-roberta-base-sentiment-latest
  inputs: ["..."]
  deps:
    - hf/models/sentiment_model
```

## Requirements

```
huggingface-hub>=0.20.0
```

## See also

- [`huggingface_dataset_asset`](https://dagster-component-ui.vercel.app/c/huggingface_dataset_asset) — same shape for Hub datasets
- [`huggingface_pipeline`](https://dagster-component-ui.vercel.app/c/huggingface_pipeline) — run any pipeline task on a list of inputs
