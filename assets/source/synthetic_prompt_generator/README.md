# SyntheticPromptGeneratorComponent

Emit `str`-valued prompt assets for LLM / agent demos. Completes the "synthetic <modality>" family alongside `synthetic_image_generator`, `synthetic_audio_generator`, `synthetic_pdf_generator`, and `synthetic_video_generator`.

**Primary use:** wire straight into `AgenticPipelineComponent`'s `source: {kind: upstream_asset, ...}` contract. Pipeline consumes `str`, this component emits `str` — no glue asset required.

## Five modes (pick one)

| # | Mode | Triggered by | What it does |
|---|---|---|---|
| A | LITERAL | `prompts: {key: text}` | Each key = a static partition; mapped text = its output. |
| B | TEMPLATE | `topics:` + `template: "{topic} …"` | Each topic = a static partition; template rendered. |
| C | FIXED | `prompt: "..."` | Unpartitioned single string. |
| D | COMPOSED (v1.5) | `topics:` + any of the 6 levers | Deterministic template composition from `persona` / `style` / `length` / `task_type` / `format_hint` / `depth` — no LLM, no network. |
| E | LLM (v2) | `topics:` + `paraphrase_model:` | LiteLLM-elevated paraphraser. Same 6 levers, but the LLM interprets them as natural-language hints instead of enum lookups. Optional `include_wrong_variants:` for adversarial variants. |

Modes are mutually exclusive — validation surfaces misconfiguration at `build_defs` time.

All modes emit `str` per materialization. Materialization metadata: `mode`, `char_count`, `preview` (markdown), and `partition_key` when partitioned.

## The 6 shared levers (modes D + E)

| Lever | Values | What it does |
|---|---|---|
| `persona` | `student` / `engineer` / `executive` / `novice` / `expert` | Rewrites the opening — "As a student trying to learn…" vs. "For an expert audience, cover in depth…". |
| `style` | `formal` / `casual` / `technical` / `journalistic` | Sets word choice + sentence rhythm. |
| `length` | `short` / `medium` / `long` | Adds an explicit target-word-count hint. |
| `task_type` | `question` / `instruction` / `comparison` / `analysis` / `explanation` / `debate` | Reshapes verb + framing (`How does X work?` vs `Compare X and Y` vs `Debate X`). |
| `format_hint` | `bullets` / `paragraphs` / `table` / `code` | Adds "in bullet points" / "as a table" / etc. |
| `depth` | `intro` / `intermediate` / `advanced` | Sets assumed background. |

Set as few or as many as you want. All levers are optional in D; only `topics:` is required.

## Mode D (v1.5) — systematic composition, no LLM

Deterministic template composition — persona-based opener × style hint × length hint × format hint × depth hint × task-type verb — seeded per `(topic, variant_idx)` so re-materializing produces the same prompt bit-for-bit. Small template pool (2-3 phrasings per lever), so `count_per_topic > ~10` starts to repeat. Great for CI, cost-free demos, or reproducible eval sets.

```yaml
type: dagster_community_components.SyntheticPromptGeneratorComponent
attributes:
  asset_name: composed_prompts
  topics: [attention, rag, rnn]
  persona: engineer
  style: technical
  length: medium
  format_hint: bullets
  depth: intermediate
  count_per_topic: 3         # 3 topics × 3 variants = 9 partitions
  seed: 42
```

Sample outputs (deterministic per seed):

```
attention__v0: "What are the load-bearing details of attention in practice? Include the actual technical detail — variable names, complexity, edge cases. Aim for ~250 words. Use bullet points. Assume the reader has basic familiarity with the field."

rag__v1:       "Cover rag for someone who ships production systems. Include the actual technical detail — variable names, complexity, edge cases. Aim for ~250 words. Present the answer as a bulleted list. Assume the reader has basic familiarity with the field."
```

## Mode E (v2) — LLM-elevated paraphraser

Same lever surface as Mode D, but the LLM interprets each lever as a natural-language hint and freely paraphrases. Requires `litellm` and an API key. Produces much more varied / natural phrasings than Mode D, at the cost of per-materialization LLM calls.

```yaml
type: dagster_community_components.SyntheticPromptGeneratorComponent
attributes:
  asset_name: llm_prompts
  topics: ["transformer attention", "retrieval-augmented generation"]
  persona: engineer
  style: technical
  length: medium
  format_hint: bullets
  depth: intermediate
  count_per_topic: 3
  paraphrase_model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  temperature: 0.7
  # Optional: adversarial-slot every 3rd variant for robustness eval
  # include_wrong_variants: true
```

Sample outputs (real gpt-4o-mini, verified live):

```
transformer attention__v0: "Explain the concept of transformer attention with a focus on technical
details for an intermediate audience. Please format your response in bullet points and aim for a medium
length. Include the following aspects: - The role of query, key, and value vectors  - The calculation
process of attention scores  - The significance of scaled dot-product attention  - Multi-head attention…"

transformer attention__v1: "Provide a technical overview of transformer attention aimed at an intermediate
audience. Please structure your response in bullet points and keep it to a medium length. Cover the
following aspects: - Definition and purpose of attention in transformers  - Key components…"
```

**When to use `include_wrong_variants: true`:** downstream is a robustness eval that wants to see how the agentic pipeline handles ambiguous / jargon-heavy / under-specified prompts. Every 3rd variant gets an explicit "make it deliberately bad" instruction.

## Chained-into-AgenticPipeline example

Standalone project layout:

```
defs/
├── prompts/defs.yaml            # SyntheticPromptGeneratorComponent
└── pipeline/defs.yaml           # AgenticPipelineComponent — source: upstream_asset
```

`defs/prompts/defs.yaml` (Mode A, simplest to grok):

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

Materialize:

```bash
uv run dg launch --assets '*' --partition attention
uv run dg launch --assets '*' --partition rag
uv run dg launch --assets '*' --partition rnn
```

Now iterating on prompts is a one-file YAML edit (add/rename keys under `prompts:`, or swap to Mode D/E for lever-driven variants) — every prompt / decision / draft lives as a browsable Dagster asset with typed metadata.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Dagster asset name. |

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `api_key_env_var` | `str` | — | Mode E: env var holding the LLM provider's API key. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `description` | `str` | — | — |
| `group_name` | `str` | `"prompts"` | — |
| `kinds` | `List[str]` | — | — |
| `tags` | `Dict[str, str]` | — | — |
| `owners` | `List[str]` | — | — |
| `deps` | `List[str]` | — | — |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `topics` | `List[str]` | — | List of topics. In mode B, template is rendered per topic. In mode D (composed) and mode E (LLM), each topic is expanded into `count_per_topic` variants. Partition keys are `{topic}` when count=1, `{topic}__v{n}` when >1. |
| `format_hint` | `str` | — | bullets \| paragraphs \| table \| code |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `prompts` | `Dict[str, str]` | — | Mode A: literal per-key mapping. Keys become static partitions; the mapped string is the partition's output. |
| `template` | `str` | — | Mode B: template rendered per topic. Substitutes `{topic}` and `{partition_key}` with the topic value. Requires `topics`. |
| `prompt` | `str` | — | Mode C: unpartitioned single-string prompt. |
| `persona` | `str` | — | student \| engineer \| executive \| novice \| expert |
| `style` | `str` | — | formal \| casual \| technical \| journalistic |
| `length` | `str` | — | short \| medium \| long |
| `task_type` | `str` | — | question \| instruction \| comparison \| analysis \| explanation \| debate |
| `depth` | `str` | — | intro \| intermediate \| advanced |
| `count_per_topic` | `int` | `1` | Modes D + E: number of prompt variants per topic. Partition keys are `{topic}` when 1, `{topic}__v{n}` (0-indexed) when >1. |
| `seed` | `int` | `42` | Reproducibility seed for mode D (composed picking) and mode E (LLM seed). |
| `paraphrase_model` | `str` | — | Mode E: LiteLLM model string (e.g. 'gpt-4o-mini', 'claude-haiku-4-5-20251001'). Presence triggers LLM mode — the same v1.5 levers become natural-language hints instead of enum lookups. |
| `api_base_env_var` | `str` | — | Mode E: env var holding a custom API base URL (self-hosted / proxies). |
| `system_prompt` | `str` | — | Mode E: override the default paraphraser system prompt. |
| `temperature` | `float` | `0.7` | — |
| `max_tokens` | `int` | `300` | — |
| `include_wrong_variants` | `bool` | `false` | Mode E: pepper adversarial / under-specified variants (every 3rd) into the output — useful for downstream robustness / eval flows. |

[//]: # (FIELDS:END)

## When to reach for this vs. the alternatives

- **This component (Mode A/B/C)** — you want a `str`-valued asset with static prompts and no variation. Cheapest, most deterministic.
- **This component (Mode D)** — you want variety across a topic set but no LLM cost / dependency. CI-safe, reproducible, ~150 possible phrasings per topic.
- **This component (Mode E)** — you want the diverse, natural-sounding prompts an LLM can produce. Small cost per materialization (~$0.0001 with gpt-4o-mini). Add `include_wrong_variants: true` for robustness-eval workflows.
- **`synthetic_data_generator` with `schema_type: support_tickets` / `product_reviews`** — you need a *DataFrame* of realistic-shaped text (with columns), not one prompt per partition.
- **`file` source directly on `AgenticPipelineComponent`** — prompts already exist on disk. Wire `source: {kind: file, path: "prompts/{partition_key}.txt"}` and skip this component.
- **`literal` source directly on `AgenticPipelineComponent`** — one hardcoded prompt for a quickstart / smoke test.
