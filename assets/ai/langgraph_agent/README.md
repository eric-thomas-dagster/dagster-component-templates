# langgraph_agent

Run a multi-step **LangGraph `StateGraph`** as a single Dagster asset. Each step is an LLM call that reads the shared state (initial input + all prior step outputs) and appends its own output back to state. Steps chain linearly by default; a step can conditionally route to another step or to `END` based on a regex check against its output.

## Why LangGraph vs a plain chain

- **Explicit stateful graph.** Every node reads/writes a typed state dict — inspectable, replayable, and richer than a linear chain's implicit variables.
- **Conditional routing.** Early-exit, retry-on-parse-fail, or branch-and-merge patterns are one-liners (`condition_regex`).
- **First-class streaming.** The compiled graph exposes intermediate node outputs for observability.
- **Composable.** Multiple `langgraph_agent` assets can compose into larger DAGs via Dagster `deps`.

If your pipeline is a single prompt over rows of a DataFrame, use [`langchain_chain_asset`](../langchain_chain_asset). If you need multi-step reasoning where each step's output feeds the next, this is the right component.

## Example

```yaml
type: dagster_component_templates.LangGraphAgentComponent
attributes:
  asset_name: research_report
  input_prompt: "How do vector databases handle high-cardinality metadata filters?"
  llm_provider: openai
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  system_message: "You are a rigorous technical researcher."

  steps:
    - name: plan
      prompt: |
        Break the question below into 3 sub-questions. Output ONLY a numbered list.
        Question: {input}
      next: research

    - name: research
      prompt: |
        Answer each sub-question in 2-3 sentences.
        Sub-questions:
        {outputs.plan}
      next: synthesize

    - name: synthesize
      prompt: |
        Combine the findings into one paragraph.
        Original question: {input}
        Findings: {outputs.research}
```

## Conditional routing

`condition_regex` lets a step branch based on its own output. Combine with `condition_else` to specify the false branch (defaults to `END`):

```yaml
steps:
  - name: classify
    prompt: "Is the message below spam? Answer 'SPAM' or 'HAM'.\n\n{input}"
    condition_regex: "SPAM"
    next: quarantine
    condition_else: allow
  - name: quarantine
    prompt: "Explain in one sentence why this message is spam:\n{input}"
  - name: allow
    prompt: "Summarize this legitimate message in one sentence:\n{input}"
```

## Fields

| Field | Type | Description |
|---|---|---|
| `asset_name` | string | Output asset name. |
| `input_prompt` | string | Seed prompt; templated as `{input}` in each step. Supports `{run_id}`, `{partition_key}`, `{partition_keys.<dim>}`. |
| `steps` | list | Ordered LLM steps. Each has `name`, `prompt`, optional `next`/`condition_regex`/`condition_else`, and optional per-step model/temp/max_tokens/system_message overrides. |
| `llm_provider` | string | `openai` (default), `anthropic`, `azure_openai`, `google`, or `ollama`. |
| `model` | string | Default model (per-step override supported). |
| `api_key_env_var` | string | Env var holding the provider key. |
| `api_base_env_var` | string | Env var holding a custom base URL (Azure / Ollama / self-hosted). |
| `system_message` | string | Default system message; per-step override supported. |
| `temperature` | number | Default `0.0`. |
| `max_tokens` | integer | Default `1024`. |
| `group_name` / `description` / `owners` / `asset_tags` / `kinds` / `deps` | — | Standard Dagster catalog fields. |
| `retry_policy_max_retries` / `retry_policy_delay_seconds` / `retry_policy_backoff` | — | Opt-in `RetryPolicy` on the asset. |

## Output

Materialized value is a dict:

```python
{
  "input": "<initial prompt>",
  "outputs": {"plan": "...", "research": "...", "synthesize": "..."},
  "final": "<last step's text>",
  "steps_run": ["plan", "research", "synthesize"],
  "stopped_by": "end_of_pipeline" | "conditional_end" | "step_error",
  "model": "gpt-4o-mini",
  "provider": "openai",
}
```

Asset metadata surfaces: `final_answer` (markdown), `steps_run`, `steps_run_count`, `model`, `provider`, `stopped_by`, and a collapsible `step_outputs` JSON blob.

## Requirements

```
langgraph>=0.2.0
langchain-core>=0.3.0
langchain-openai>=0.2.0   # or langchain-anthropic / -google-genai / -ollama
```

## Related

- `langchain_chain_asset` — single-prompt row-wise chain over a DataFrame.
- `anthropic_agent` / `openai_agent` / `gemini_agent` — single-shot ReAct-style tool-calling agents.
- `litellm_agent` — multi-provider tool-calling agent via LiteLLM.
