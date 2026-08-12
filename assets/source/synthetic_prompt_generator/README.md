# SyntheticPromptGeneratorComponent

Emit deterministic prompt text strings for LLM / agent demos. Completes the "synthetic <modality>" family alongside `synthetic_image_generator`, `synthetic_audio_generator`, `synthetic_pdf_generator`, and `synthetic_video_generator`.

**Primary use:** wire straight into `AgenticPipelineComponent`'s `source: {kind: upstream_asset, ...}` contract. The pipeline consumes plain `str`, this component emits plain `str` — no glue asset required, no DataFrame extraction.

## v1 modes (systematic — pick one)

| Mode | Config | Emits |
|---|---|---|
| **Literal per-key** | `prompts: {key: text}` | Each key = a static partition; the mapped text = that partition's output. |
| **Templated** | `topics: [...]` + `template: "{topic} …"` | Each topic = a static partition; template rendered per topic. `{topic}` and `{partition_key}` both substitute the topic value. |
| **Fixed single** | `prompt: "..."` | Unpartitioned; the single string is the output. |

The three modes are mutually exclusive — set exactly one. Validation runs at `build_defs` time; misconfiguration surfaces as a clear error before the asset materializes.

Output type per materialization: `str`. Metadata surfaced on every materialization: `mode`, `char_count`, `preview` (markdown), and `partition_key` when partitioned.

**v2 (planned, not yet built):** LLM- or local-ML-driven prompt synthesis — take a topic pool + a persona description and have a small LLM generate paraphrased variants at build_defs time or per materialization. v1 covers the systematic case cleanly; v2 will layer on top without changing the emit contract, so downstream YAML doesn't need to change.

## Chained-into-AgenticPipeline example

Standalone project layout:

```
defs/
├── prompts/defs.yaml            # SyntheticPromptGeneratorComponent
└── pipeline/defs.yaml           # AgenticPipelineComponent — source: upstream_asset
```

`defs/prompts/defs.yaml`:

```yaml
type: dagster_community_components.SyntheticPromptGeneratorComponent
attributes:
  asset_name: research_topics
  prompts:
    attention: "Explain how transformer attention works in ~200 words."
    rag:       "How does retrieval-augmented generation improve LLM factuality?"
    rnn:       "Compare RNN vs Transformer architectures for sequence modeling."
```

`defs/pipeline/defs.yaml`:

```yaml
type: dagster_community_components.AgenticPipelineComponent
attributes:
  asset_name_prefix: analysis
  source:
    kind: upstream_asset
    upstream_asset_key: research_topics       # <- wires the pipeline to the prompt asset
  steps:
    - id: routed
      op: route
      router: {model: gpt-4o-mini, api_key_env_var: OPENAI_API_KEY}
      specialists:
        - {name: technical, description: "CS / ML questions.", model: gpt-4o, api_key_env_var: OPENAI_API_KEY,
           system_prompt: "You are a senior ML engineer."}
        - {name: general, description: "General topics.", model: gpt-4o-mini, api_key_env_var: OPENAI_API_KEY,
           system_prompt: "You are a helpful assistant."}
      fallback: general
    - id: critiqued
      op: critique_loop
      source: routed
      drafter: {model: gpt-4o, api_key_env_var: OPENAI_API_KEY}
      critic:  {model: gpt-4o, api_key_env_var: OPENAI_API_KEY}
      iterations: 2
  outputs:
    assets: [routed, critiqued]
    text_sinks:
      - {from: critiqued, path: "out/{partition_key}.md"}
```

Asset graph:

```
   research_topics                          (SyntheticPromptGeneratorComponent)
        │  static partitions: [attention, rag, rnn]
        │  each partition → the mapped prompt string
        ▼
   analysis_routed                          (AgenticPipelineComponent, op: route)
        │
        ▼
   analysis_critiqued                       (op: critique_loop)
        │
        ▼
   out/{partition_key}.md                   (text_sink, one file per partition)
```

Materialize with:

```bash
uv run dg launch --assets '*' --partition attention
uv run dg launch --assets '*' --partition rag
uv run dg launch --assets '*' --partition rnn
```

Each partition runs the whole chain against its own prompt — three prompts in, three markdown briefs out, one per partition in the sink directory.

## Templated mode example

Same shape, but generate the prompts by rendering one template per topic:

```yaml
type: dagster_community_components.SyntheticPromptGeneratorComponent
attributes:
  asset_name: benchmark_prompts
  topics:
    - "sorting algorithms"
    - "graph traversal"
    - "dynamic programming"
  template: |
    Give a clear introductory explanation of {topic}, then a small worked example. ~300 words.
```

## Fields

### One of these three modes (required — pick exactly one)

| Field | Type | Description |
|---|---|---|
| `prompts` | `Dict[str, str]` | Mode A. Keys become static partitions; mapped strings are their outputs. |
| `topics` + `template` | `List[str]` + `str` | Mode B. Topics become partitions; template rendered per topic with `{topic}` / `{partition_key}` substituted. Both required together. |
| `prompt` | `str` | Mode C. Unpartitioned single-string output. |

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Dagster asset name. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"prompts"` | Group name for the emitted asset. |
| `kinds` | `List[str]` | `["prompt", "synthetic"]` | Asset kinds. |
| `tags` | `Dict[str, str]` | — | Additional tags. |
| `owners` | `List[str]` | — | Asset owners. |
| `description` | `str` | Auto-generated | Description. |
| `deps` | `List[str]` | — | Additional dependencies (upstream asset keys). |

## Emitted metadata

On every materialization the asset surfaces:

| Metadata key | Type | Description |
|---|---|---|
| `mode` | Text | `"prompts"` / `"template"` / `"prompt"`. |
| `char_count` | Int | Length of the emitted prompt string. |
| `preview` | Markdown | First 400 characters, rendered inline in the Dagster UI. |
| `partition_key` | Text | The partition that produced this materialization (when partitioned). |

## When to reach for this vs. the alternatives

- **This component** — you want a *single string* per materialization, straight into `AgenticPipelineComponent`'s `upstream_asset` source, with static partitions from your prompt catalog.
- **`synthetic_data_generator` with `schema_type: support_tickets` / `product_reviews` / `moderation_content`** — you need a *DataFrame* of realistic-shaped text (with columns), not one prompt per partition. Use when the downstream is a DataFrame transform, not an LLM string consumer.
- **`file` source on `AgenticPipelineComponent`** — you already have prompts on disk. Wire `source: {kind: file, path: "prompts/{partition_key}.txt"}` and skip this component entirely.
- **`literal` source on `AgenticPipelineComponent`** — one hardcoded prompt for a quickstart / smoke test.
