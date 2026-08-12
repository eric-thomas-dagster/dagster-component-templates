# AgenticPipelineComponent

**One YAML file. Whole agentic pipeline.** Sibling of `polars_pipeline`, `warehouse_pipeline`, `pyspark_pipeline`, `snowpark_pipeline`, `ml_pipeline` — the "pipeline component" family, for LLM / agent workflows.

Standardize what an agentic pipeline looks like across your org. `source` + `steps` + `outputs`. Reviewers scan a fixed schema, CI validates against one shape, new hires learn one file and can build any agentic workflow.

## Why Dagster (not just a job runner)

Any workflow tool can *run* a chain of LLM calls. What Dagster does that a job-based orchestrator or a plain script doesn't:

**Every step is a versioned asset with typed metadata.** Every step's output is a first-class asset in the catalog. Click it, see every past materialization: what the router picked, arbitrator reasoning, cost, latency, model, timestamp — **no log-grepping**. Job-based tools give you a run log; Dagster gives you a browsable, filterable decision history per asset per partition.

**Rich, typed metadata on every materialization** (all present by default — no configuration):

| Field | Type | What Dagster does with it |
|---|---|---|
| `<step>__text` | `MarkdownMetadataValue` | Renders the agent's output inline in the asset UI. |
| `<step>__cost_usd` | `FloatMetadataValue` | Promote to a **Dagster+ Insights** custom metric via the UI; once promoted, dashboards + per-metric alerts follow (`alert if cost > $10 in 1h`). |
| `<step>__latency_ms` | `IntMetadataValue` | Same — promote via UI, then plot latency over time / alert on regressions. |
| `<step>__tokens_total` | `IntMetadataValue` | Same — promote via UI, then set token budget alerts. |
| `<step>__n_llm_calls` | `IntMetadataValue` | Same — promote via UI, then track fan-out drift. |
| `<step>__model_fingerprint` | `TextMetadataValue` | e.g. `gpt-4o-mini→gpt-4o` — spot when a partition was rerun with a different model. |
| `<step>__materialized_at` | `TimestampMetadataValue` | When the LLM call fired. Rerun-from-cache vs. fresh call is visible. |
| `<step>__op` | `TextMetadataValue` | Which pipeline op (route / debate / critique_loop / ...). |
| `<step>__partition_key` | `TextMetadataValue` | Echoes the partition for easy filtering. |
| `<step>__proposals` | `JsonMetadataValue` | (debate op) Every proposal, with its model + text. |
| `<step>__history` | `JsonMetadataValue` | (critique_loop) Full drafter/critic transcript across iterations. |
| `<step>__router_reasoning` | `TextMetadataValue` | (route op) Why the router picked this specialist. |

**Per-step kinds — filter the catalog by pipeline op.** Every asset gets its op name as a `kind` tag (`route`, `debate`, `critique_loop`, `synthesize`, `llm_call`). Filter the catalog to "show me every debate step across every pipeline" — impossible with job-based tools.

**Partitions — time-travel to any decision.** `{partition_key}` in your source text / URL / file path templates at compute time. Every partition's materialization is independently browsable. "What did the pipeline decide for `2026-03-05`?" is one click, not a log-search.

**Dagster+ Insights** (Dagster+ only). Because `cost_usd`, `latency_ms`, `tokens_total`, `n_llm_calls` ship as typed numeric metadata, you can **promote any of them into a custom Insights metric from the Dagster+ UI** — a few clicks, no code, no separate observability pipeline. Once promoted, each metric gets a dashboardable time-series and configurable alerts (`alert if any pipeline's median cost per partition exceeds $0.50`). With a job-based orchestrator you'd first instrument the numeric values yourself, then build the whole export → Grafana → alertmanager pipeline before you could even define the alert.

**Lineage — the pipeline connects to your data graph.** Use `kind: upstream_asset` on the source, and the pipeline's inputs show as parents in the asset graph. Job-based flows have no such graph.

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

### Common fields (every step)

| Field | Required | Default | Description |
|---|---|---|---|
| `id` | ✅ | — | Step id — referenced by `source:` / `sources:` on downstream steps and by `outputs.assets:`. |
| `op` | ✅ | — | One of `llm_call` / `route` / `debate` / `critique_loop` / `synthesize`. |
| `source` |  | Most recent step | Prior step id whose text feeds this step. `synthesize` uses `sources:` (plural, list) instead. |

### LLM sub-config (used inside every op)

The router / specialist / drafter / critic / proposer / arbitrator / synthesizer sub-blocks all take the same shape:

| Field | Required | Default | Description |
|---|---|---|---|
| `model` | ✅ | — | LiteLLM model string (`gpt-4o`, `claude-haiku-4-5-20251001`, `gemini/gemini-2.5-flash`, `ollama/llama3.2`, `bedrock/…`, `groq/…`, …). |
| `api_key_env_var` |  | Provider default (e.g. `OPENAI_API_KEY`) | Env var holding the provider's API key. |
| `api_base_env_var` |  | — | Env var holding a custom base URL (self-hosted, proxies, Azure OpenAI). |
| `system_prompt` |  | — | System prompt for this LLM's persona. |
| `temperature` |  | `0.0` | Sampling temperature. |
| `max_tokens` |  | `2048` | Max completion tokens. |

### `op: llm_call`

Single completion. LLM sub-config lives inline on the step (no `model` nesting).

| Field | Required | Default | Description |
|---|---|---|---|
| `model` | ✅ | — | LiteLLM model string. |
| `api_key_env_var` |  | Provider default | Env var holding API key. |
| `system_prompt` |  | — | System prompt. |
| `prompt_template` |  | `"{text}"` | Template around the source's text; `{text}` is the source text, standard `{partition_key}` etc. tokens also substituted. |
| `temperature` |  | `0.0` | Sampling temperature. |
| `max_tokens` |  | `2048` | Max completion tokens. |
| `api_base_env_var` |  | — | Custom base URL env var. |

### `op: route`

Router LLM picks a specialist by name via tool-call; specialist LLM answers.

| Field | Required | Default | Description |
|---|---|---|---|
| `router` | ✅ | — | LLM sub-config for the router — see above. |
| `specialists` | ✅ | — | List of specialist LLMs (2+). Each: `{name, description, model, ...LLM sub-config}`. `name` must be a valid identifier (used as a tool-call name); `description` seeds the router's routing decision. |
| `fallback` |  | — | Specialist name to fall back to if the router can't pick. If unset and the router fails, an exception is raised. |
| `include_reasoning` |  | `true` | If true, the router asks for a free-text `reasoning` field alongside the tool call and surfaces it as the `router_reasoning` metadata field. |

### `op: debate`

Proposers each write a proposal; arbitrator judges + picks a winner.

| Field | Required | Default | Description |
|---|---|---|---|
| `proposers` | ✅ | — | List of LLM sub-configs (2+). Each proposer sees the source text + any `system_prompt` on it (that's where you seed each proposer's persona / argument). |
| `arbitrator` | ✅ | — | LLM sub-config for the judge. Sees the source + all proposals and picks `winner_index`. Surfaces `arbitrator_reasoning`. |

### `op: critique_loop`

Drafter writes; critic reviews; drafter revises; repeat.

| Field | Required | Default | Description |
|---|---|---|---|
| `drafter` | ✅ | — | LLM sub-config for the drafter. |
| `critic` | ✅ | — | LLM sub-config for the critic. |
| `iterations` | ✅ | — | Number of drafter → critic → drafter cycles. Total LLM calls = `2 * iterations + 1`. |

### `op: synthesize`

One LLM merges the outputs of N prior steps into a single response. This is the fan-in shape.

| Field | Required | Default | Description |
|---|---|---|---|
| `sources` | ✅ | — | List of prior step ids to merge (replaces the singular `source:`). |
| `model` | ✅ | — | LiteLLM model string. |
| `api_key_env_var` |  | Provider default | Env var holding API key. |
| `system_prompt` |  | — | System prompt. |
| `prompt_template` |  | Auto-composes source texts | Template for the merge. Each source appears as `{<step_id>_text}` in the template. |
| `temperature` |  | `0.0` | Sampling temperature. |
| `max_tokens` |  | `2048` | Max completion tokens. |
| `api_base_env_var` |  | — | Custom base URL env var. |

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

| kind | When to use |
|---|---|
| `literal` | Static prompt, quick demos |
| `file` | Prompt / doc from disk |
| `url` | Fetch a public document / API response |
| `upstream_asset` | Chain the pipeline downstream of any other Dagster asset |

All string fields on sources support `{partition_key}` substitution — a partitioned pipeline reads a different file / URL / literal per partition.

### `kind: literal`

| Field | Required | Description |
|---|---|---|
| `kind` | ✅ | `literal` |
| `text` | ✅ | Static text used as the pipeline's initial input. `{partition_key}` templated. |

### `kind: file`

| Field | Required | Description |
|---|---|---|
| `kind` | ✅ | `file` |
| `path` | ✅ | Filesystem path. `{partition_key}` templated (`questions/{partition_key}.txt`). Relative paths resolve against the code-location's working directory. |

### `kind: url`

| Field | Required | Description |
|---|---|---|
| `kind` | ✅ | `url` |
| `url` | ✅ | URL to fetch. `{partition_key}` templated. Response body becomes the source text. |
| `headers` |  | Optional `{header: value}` map. Values `{partition_key}`-templated. |

### `kind: upstream_asset`

| Field | Required | Description |
|---|---|---|
| `kind` | ✅ | `upstream_asset` |
| `upstream_asset_key` | ✅ | Asset key of the Dagster asset to load. Its output becomes the pipeline's initial source text — the pipeline's inputs then show as parents in the asset graph. |

## Sinks — where the outputs go

The `outputs:` block has three keys — `assets:` (required), and optional `text_sinks:` / `json_sinks:` for side-effect writes to disk.

| Sink | Required | Description |
|---|---|---|
| `assets` | ✅ | List of step ids whose outputs become first-class Dagster assets. Each emits the full step dict as materialization metadata (text, cost, latency, tokens, model, reasoning, proposals, history, etc.). |
| `text_sinks` |  | Writes each listed step's `text` field to disk. Path is `{partition_key}`-aware. Parent dir auto-created. |
| `json_sinks` |  | Writes each listed step's full dict (all metadata + usage + history) to disk as JSON. Path is `{partition_key}`-aware. Parent dir auto-created. |

### `text_sinks[]` / `json_sinks[]` entry

| Field | Required | Description |
|---|---|---|
| `from` | ✅ | Step id whose output to write. |
| `path` | ✅ | Filesystem path. `{partition_key}` templated. Relative paths resolve against the code-location's working directory. |

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

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name_prefix` | `str` | Prefix for emitted asset names. Each step in outputs.assets becomes '{prefix}_{step_id}'. |
| `source` | `Dict[str, Any]` | Data source. Shapes: {kind: literal, text: '...'} \| {kind: file, path: '...'} \| {kind: url, url: '...'} \| {kind: upstream_asset, upstream_asset_key: '...'}. All string fields are {partition_key}-templated. |
| `steps` | `List[Dict[str, Any]]` | Ordered pipeline steps. Each step: {id, op, ...op-specific args}. Steps chain by id — a step with `source: <id>` reads that step's text; omit `source:` and it defaults to the most recent step's text. 5 ops: llm_call \| route \| debate \| critique_loop \| synthesize. |
| `outputs` | `Dict[str, Any]` | Output declaration. Shape: {assets: [<step_ids>], text_sinks: [{from, path}], json_sinks: [{from, path}]}. `assets:` step outputs become first-class Dagster assets; `text_sinks:` writes step text to disk; `json_sinks:` writes full step dict. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"agents"` | Group name for emitted assets. |
| `kinds` | `List[str]` | — | Asset kinds. Default: ['llm', 'agent', 'pipeline']. |
| `tags` | `Dict[str, str]` | — | Additional tags on emitted assets. |
| `owners` | `List[str]` | — | Asset owners. |
| `description` | `str` | — | Description on emitted assets. |

[//]: # (FIELDS:END)
