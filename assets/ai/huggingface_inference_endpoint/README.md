# HuggingFace Inference Endpoint

Calls a [dedicated HuggingFace Inference Endpoint](https://huggingface.co/inference-endpoints) — a paid, scaled, dedicated deployment of a model from your HF account. Different from the shared public Inference API (`huggingface_pipeline` `mode: inference_api`).

## When to use this

| Use case | Component |
|---|---|
| Try out a model with no compute setup | `huggingface_pipeline` `mode: local` (free, downloads model) |
| Cheap inference on a popular Hub model | `huggingface_pipeline` `mode: inference_api` (shared, pay-as-you-go) |
| **Dedicated endpoint with SLA / VPC / autoscale** | **`huggingface_inference_endpoint`** (this component) |

## Two ways to reference the endpoint

1. **By name** — the endpoint name in your HF account. The component looks up the URL via `HfApi.get_inference_endpoint()`. Best for production (URL can change; name is stable).
2. **By URL** — the direct `https://xxxx.aws.endpoints.huggingface.cloud` URL. Fastest, but URL changes if you delete + recreate the endpoint.

## Examples

### Named endpoint

```yaml
type: dagster_community_components.HuggingfaceInferenceEndpointComponent
attributes:
  asset_key: hf/endpoint/sentiment
  endpoint_name: my-sentiment-endpoint
  task: text-classification
  inputs:
    - "I love this product!"
    - "Terrible experience."
  hf_token_env_var: HF_TOKEN
```

### Endpoint URL

```yaml
type: dagster_community_components.HuggingfaceInferenceEndpointComponent
attributes:
  asset_key: hf/endpoint/sentiment
  endpoint_url: https://abc123.us-east-1.aws.endpoints.huggingface.cloud
  task: text-classification
  inputs:
    - "I love this product!"
  hf_token_env_var: HF_TOKEN
```

### Cross-account endpoint

```yaml
type: dagster_community_components.HuggingfaceInferenceEndpointComponent
attributes:
  asset_key: hf/endpoint/team_summarizer
  endpoint_name: summarizer-v2
  namespace: my-org
  task: summarization
  inputs:
    - "Long text body to summarize here..."
  hf_token_env_var: HF_TOKEN
```

## Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_key` | `str` | ✓ | Dagster asset key |
| `task` | `str` | ✓ | Inference task |
| `inputs` | `List[str]` | ✓ | List of inputs |
| `endpoint_name` | `str` | one of | Name of dedicated endpoint |
| `endpoint_url` | `str` | one of | Direct endpoint URL |
| `namespace` | `str` | — | HF namespace if endpoint in another account |
| `hf_token_env_var` | `str` | ✓ | Env var with HF token |
| `call_kwargs` | `Dict` | — | Extra kwargs per call |
| `group_name`, `description`, `owners`, `asset_tags`, `kinds`, `deps` | — | — | Standard catalog metadata |

## Materialization metadata

| Key | Description |
|---|---|
| `endpoint_name` / `endpoint_url` | The resolved endpoint reference + URL |
| `endpoint_status` | Endpoint status from the HF API (running / scaledToZero / failed / …) |
| `task` | Configured inference task |
| `input_count` / `success_count` / `failure_count` | Per-batch stats |
| `preview` | JSON preview of up to the first 5 results |

## Requirements

```
huggingface-hub>=0.20.0
```

## See also

- [HuggingFace Inference Endpoints documentation](https://huggingface.co/docs/inference-endpoints/index)
- [`huggingface_pipeline`](https://dagster-component-ui.vercel.app/c/huggingface_pipeline) — local or shared-API alternative
