# HuggingFace Dataset (observed)

Surfaces a [HuggingFace Hub dataset](https://huggingface.co/datasets) as a Dagster `observable_source_asset`. On each observation, polls the Hub API and emits an `ObserveResult` with the dataset's current downloads, likes, last-modified timestamp, configs, license, and file count.

The dataset itself lives on the HF Hub — Dagster surfaces its **metadata** in the catalog, not its raw contents. Use it to track the version of a dataset that downstream assets depend on (e.g., "embeddings asset needs to re-materialize if the source HF dataset's `last_modified` advanced").

## Example

```yaml
type: dagster_community_components.HuggingfaceDatasetAssetComponent
attributes:
  asset_key: hf/datasets/imdb
  dataset_id: imdb
  group_name: huggingface
```

## Wiring downstream

```yaml
# Downstream asset that depends on the observation:
type: dagster_community_components.SqlTransformComponent  # or any other
attributes:
  asset_name: imdb_embeddings
  deps:
    - hf/datasets/imdb        # re-materialize whenever the dataset observation updates
  ...
```

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | — (required) | Dagster asset key |
| `dataset_id` | `str` | — (required) | HF Hub dataset id (e.g. `imdb`, `mozilla-foundation/common_voice_16_0`) |
| `revision` | `str` | — | Optional revision (branch / tag / commit) |
| `hf_token_env_var` | `str` | — | Env var with HF token (required for gated / private datasets) |
| `group_name` | `str` | `"huggingface"` | Dagster asset group name |
| `owners`, `asset_tags`, `kinds`, `deps`, `description` | — | — | Standard catalog metadata |

## Observation metadata

| Key | Description |
|---|---|
| `dataset_id` | The Hub dataset id |
| `downloads` | All-time download count from the Hub |
| `likes` | Hub likes count |
| `last_modified` | ISO timestamp of the last Hub commit |
| `private` / `gated` | Visibility flags |
| `file_count` | Number of files in the dataset repo (`siblings`) |
| `configs` | Available configurations (comma-separated) |
| `task_categories` | Task tags from the dataset card |
| `license` | License declared in the dataset card |
| `hub_url` | Clickable URL to the dataset page |

If the Hub call fails (e.g. dataset deleted / network), the observation still emits — with the `error` key set and the `hub_url` for manual debugging.

## Requirements

```
huggingface-hub>=0.20.0
```

## See also

- [`huggingface_model_asset`](https://dagster-component-ui.vercel.app/c/huggingface_model_asset) — the same shape for HF Hub models
- [`huggingface_pipeline`](https://dagster-component-ui.vercel.app/c/huggingface_pipeline) — run any pipeline task on a list of inputs
