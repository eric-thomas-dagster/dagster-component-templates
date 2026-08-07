# AgenticPipelineComponent

**One YAML file. Whole agentic pipeline.** Sibling of `polars_pipeline`, `warehouse_pipeline`, `pyspark_pipeline`, `snowpark_pipeline`, `ml_pipeline` — the "pipeline component" family, for LLM / agent workflows.

Standardize what an agentic pipeline looks like across your org. `source` + `steps` + `outputs`. Reviewers scan a fixed schema, CI validates against one shape, new hires learn one file and can build any agentic workflow.

## Quick example

```yaml
type: dagster_community_components.AgenticPipelineComponent
attributes:
  asset_name_prefix: research_bot
  source:
    kind: literal
    text: "Explain how transformer attention works."
  steps:
    - id: routed
      op: route
      router:
        model: gpt-4o-mini
        api_key_env_var: OPENAI_API_KEY
      specialists:
        - {name: technical, description: "CS / ML questions.",
           model: gpt-4o, api_key_env_var: OPENAI_API_KEY,
           system_prompt: "You are a senior ML engineer."}
        - {name: general,   description: "General topics.",
           model: gpt-4o-mini, api_key_env_var: OPENAI_API_KEY,
           system_prompt: "You are a helpful assistant."}
      fallback: general

    - id: critiqued
      op: critique_loop
      source: routed
      drafter: {model: gpt-4o, api_key_env_var: OPENAI_API_KEY}
      critic:  {model: gpt-4o, api_key_env_var: OPENAI_API_KEY}
      iterations: 2

    - id: debated
      op: debate
      source: critiqued
      proposers:
        - {model: gpt-4o, api_key_env_var: OPENAI_API_KEY,
           system_prompt: "Argue for accessibility."}
        - {model: gpt-4o, api_key_env_var: OPENAI_API_KEY,
           system_prompt: "Argue for precision."}
      arbitrator:
        model: gpt-4o
        api_key_env_var: OPENAI_API_KEY

    - id: final
      op: synthesize
      sources: [routed, critiqued, debated]
      model: gpt-4o
      api_key_env_var: OPENAI_API_KEY

  outputs:
    assets: [routed, critiqued, debated, final]
    text_sinks:
      - {from: final, path: /tmp/final.txt}
```

## Op menu (v1 = 5)

| op | Shape | What it does |
|---|---|---|
| `llm_call` | 1 LLM call | Single completion over source text. Simplest possible op. |
| `route` | 1 router → 1 specialist | Router LLM picks the best specialist agent; specialist answers. Multi-agent selection. |
| `debate` | N proposers → 1 arbitrator | Each proposer writes a proposal; arbitrator picks the winner. Multi-agent consensus. |
| `critique_loop` | drafter → critic → drafter, N times | Drafter writes; critic reviews; drafter revises. Refinement loop. |
| `synthesize` | 1 LLM merges N upstream step texts | Combine multiple prior step outputs into one coherent response. Fan-in. |

## State model

Every step's output is a dict `{text: str, ...op-specific fields}`. Steps read text from a prior step by `source:` id; omit `source:` and it defaults to the most recent step. This is the same "chain by id" pattern as `ml_pipeline`.

The `assets:` list picks which step outputs become first-class Dagster assets — each emitted as the full step dict (with router reasoning, all proposals, critique history, usage counts, etc.).

## Model support

Any provider LiteLLM supports (100+ providers via one API):

```yaml
model: gpt-4o                                      # OpenAI
model: claude-haiku-4-5-20251001                   # Anthropic
model: gemini/gemini-2.5-flash                     # Google
model: groq/llama-3.3-70b-versatile                # Groq
model: bedrock/anthropic.claude-3-5-sonnet-...     # AWS Bedrock
model: ollama/llama3.2                             # local Ollama
```

Each step (router, specialist, drafter, critic, proposer, arbitrator) picks its own model — mix cheap-fast routers with premium-slow drafters.

## Sources — where the input comes from

| kind | Config | Use case |
|---|---|---|
| `literal` | `{kind: literal, text: "..."}` | Static prompt, quick demos |
| `file` | `{kind: file, path: /path/to.txt}` | Prompt / doc from disk |
| `url` | `{kind: url, url: https://...}` | Fetch a public document / API response |
| `upstream_asset` | `{kind: upstream_asset, upstream_asset_key: raw/text}` | Chain the pipeline downstream of any other Dagster asset |

## Sinks — where the outputs go

| kind | Config | Notes |
|---|---|---|
| `assets` (required) | `[step_id, step_id, ...]` | Step outputs become first-class Dagster assets |
| `text_sinks` | `[{from: step_id, path: /tmp/out.txt}]` | Writes step's text field. Path is `{partition_key}`-aware |
| `json_sinks` | `[{from: step_id, path: /tmp/out.json}]` | Writes full step dict (metadata, usage, history). Path is `{partition_key}`-aware |

## Partitioning

`AgenticPipelineComponent` is fully partition-aware. `{partition_key}` templates in source text / URL / path get substituted at compute time; sinks get the same treatment.

Declare partitions via `post_processing:`:

```yaml
type: dagster_community_components.AgenticPipelineComponent
attributes: {...}

post_processing:
  assets:
    - target: "*"
      attributes:
        partitions_def: {type: daily, start_date: "2026-01-01"}
        automation_condition: "{{ dg.AutomationCondition.eager() }}"
        tags: {tier: gold, team: ai-platform}
        owners: ["ai-team@company.com"]
```

## When to reach for this vs. the narrow AI components

| You want... | Reach for |
|---|---|
| One LLM call per row of a DataFrame | `litellm_inference_asset` / `openai_llm` / `openrouter_llm` |
| An agent with MCP tools (single-shot ReAct loop) | `litellm_agent` / `openai_agent` |
| Planner picks N tools in parallel → synthesize | `supervisor_agent` |
| ReAct chain with per-step Dagster assets | `iterative_supervisor_agent` |
| Multi-output branching per case | `llm_multi_path_router` |
| **Compose several of the above shapes into one pipeline** | **`agentic_pipeline` (this)** |

This is the "compose it all yourself in one YAML" alternative — the AI-side match to `ml_pipeline` for the ML domain.
