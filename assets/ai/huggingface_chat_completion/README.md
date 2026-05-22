# HuggingFace Chat Completion

Call a chat-completion model via the [HuggingFace router](https://huggingface.co/docs/inference-providers/index) (`https://router.huggingface.co/v1`) using the standard OpenAI SDK. The router exposes hundreds of hosted chat models — Kimi-K2, Llama-3.3, Mistral, Qwen, DeepSeek, etc. — through one OpenAI-compatible endpoint.

## When to use this vs. other LLM components

| Goal | Component |
|---|---|
| Chat with any HF-routed model, OpenAI-compatible API | **`huggingface_chat_completion`** (this one) |
| Chat with the OpenAI API directly | [`openai_llm`](https://dagster-component-ui.vercel.app/c/openai_llm) |
| Chat with Anthropic Claude directly | [`anthropic_llm`](https://dagster-component-ui.vercel.app/c/anthropic_llm) |
| Multi-provider routing via LiteLLM | [`litellm_inference_asset`](https://dagster-component-ui.vercel.app/c/litellm_inference_asset) |
| Run a `transformers.pipeline()` task | [`huggingface_pipeline`](https://dagster-component-ui.vercel.app/c/huggingface_pipeline) |

`huggingface_chat_completion` is specifically for **hosted chat completions via HF**. One SDK, any model the router supports — useful when you want to swap models without swapping SDKs.

## Examples

### Simple prompt (Kimi-K2)

```yaml
type: dagster_community_components.HuggingfaceChatCompletionComponent
attributes:
  asset_key: hf/chat/photosynthesis
  model: moonshotai/Kimi-K2-Instruct-0905
  prompt: "Describe the process of photosynthesis."
  hf_token_env_var: HF_TOKEN
```

### Full messages array (system + user)

```yaml
type: dagster_community_components.HuggingfaceChatCompletionComponent
attributes:
  asset_key: hf/chat/scientist
  model: meta-llama/Llama-3.3-70B-Instruct
  messages:
    - role: system
      content: "You are an expert physicist. Be precise."
    - role: user
      content: "Explain Hawking radiation in two sentences."
  temperature: 0.3
  max_tokens: 500
  hf_token_env_var: HF_TOKEN
```

### Mistral with low temperature for deterministic output

```yaml
type: dagster_community_components.HuggingfaceChatCompletionComponent
attributes:
  asset_key: hf/chat/sql_classifier
  model: mistralai/Mistral-7B-Instruct-v0.3
  prompt: "Is the following SQL safe to run? SELECT * FROM users; Answer YES or NO only."
  temperature: 0.0
  max_tokens: 5
  hf_token_env_var: HF_TOKEN
```

## Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `asset_key` | `str` | ✓ | Dagster asset key |
| `model` | `str` | ✓ | HF Hub model id available on the router |
| `prompt` | `str` | one of | Simple user-message prompt |
| `messages` | `List[Dict]` | one of | Full OpenAI-shaped chat history |
| `hf_token_env_var` | `str` | — (default `HF_TOKEN`) | Env var with HF token |
| `temperature` | `float` | — | Sampling temperature |
| `max_tokens` | `int` | — | Output token cap |
| `top_p` | `float` | — | Nucleus sampling |
| `base_url` | `str` | — (default HF router) | OpenAI-compat base URL — change to point at a different OpenAI-compat endpoint |
| `extra_kwargs` | `Dict` | — | Forward to `chat.completions.create()` |
| `group_name`, `description`, `owners`, `asset_tags`, `kinds`, `deps` | — | — | Standard catalog metadata |

## Materialization metadata

| Key | Description |
|---|---|
| `model` | The configured model id |
| `base_url` | The endpoint used |
| `response` | The assistant message (rendered as markdown in the UI) |
| `response_length_chars` | Length of the response in characters |
| `finish_reason` | OpenAI finish reason (stop / length / content_filter / tool_calls) |
| `prompt_tokens` / `completion_tokens` / `total_tokens` | Token usage from the response |
| `message_count` | Number of input messages |

## Requirements

```
openai>=1.0.0
```

## See also

- [HuggingFace Inference Providers documentation](https://huggingface.co/docs/inference-providers/index)
- [`huggingface_pipeline`](https://dagster-component-ui.vercel.app/c/huggingface_pipeline) — `transformers.pipeline()` tasks
- [`huggingface_text_to_image`](https://dagster-component-ui.vercel.app/c/huggingface_text_to_image) — image generation
