# AgenticPipelineComponent

**One YAML file. Whole agentic pipeline.** Sibling of `polars_pipeline`, `warehouse_pipeline`, `pyspark_pipeline`, `snowpark_pipeline`, `ml_pipeline` — the "pipeline component" family, for LLM / agent workflows.

Standardize what an agentic pipeline looks like across your org. `source` + `steps` + `outputs`. Reviewers scan a fixed schema, CI validates against one shape, new hires learn one file and can build any agentic workflow.

**Full worked examples** (every one run for real, output genuine, not illustrative):

- [`examples/support_ticket_triage/`](examples/support_ticket_triage/README.md) — agent categorizes tickets by severity, a real existing `FilterComponent` deterministically keeps only the severe ones (via `invoke_component`, no reimplementation, no new lineage node), then `delegate` dynamically routes to whichever registered agent is actually tagged `capabilities: [triage]` — with a second, deliberately-wrong-capability agent registered alongside it to prove the filtering really works.
- [`examples/document_summarization/`](examples/document_summarization/README.md) — one `delegate` step, structured output (`summary` + `action_items`), extracted verbatim from a real sample document.
- [`examples/route_to_specialist/`](examples/route_to_specialist/README.md) — three registered specialist agents, **no** `required_capabilities` set — proves the picker LLM does genuine semantic matching (billing/technical/general question → correct specialist, verified with three different real questions).
- [`examples/debate_best_answer/`](examples/debate_best_answer/README.md) — two debater agents (shared capability tag, distinguished by semantics) + one arbitrator (distinct tag), joined with typed `inputs:` ports into a final verdict that genuinely weighs both arguments.
- [`examples/governed_invoice_matching/`](examples/governed_invoice_matching/README.md) — cascade → decide → verify → approve (exceptions only) → record, with a pipeline policy and a per-step policy; run end to end in tests with the model call scripted (not yet against a live model).
- [`examples/qwen_local_agent/`](examples/qwen_local_agent/README.md) — **new, not yet run against a live Ollama**: a local-Qwen3 `tool_use_loop` confined by a `policy:` (read-only folder, no writes, env/host allowlists, budget). Enforcement is covered by the component's tests; see the README for exactly what is and isn't verified.
- [`examples/pipeline_incident_triage/`](examples/pipeline_incident_triage/README.md) — the first `tool_use_loop` showcase: one agent, three REAL tools (a live vendor status-page check, a real `git log`, a real runbook search), iterating freely until it has enough evidence to decide "wait, don't debug" vs. actually troubleshoot. Caught Snowflake's status page actually degraded, live, mid-build.

## Reasoning models, local models (Qwen, DeepSeek-R1) and agent policy

**Reasoning output.** Models that think out loud (Qwen3, DeepSeek-R1, ...) often inline `<think>…</think>` in the reply.
Every model call now splits it off: the step's `text` is the clean answer (so JSON parsing, routing and `classify`/`extract`
keep working), and the thinking is attached as `<step>__reasoning` metadata (markdown, per model). Provider-supplied
`reasoning_content` is kept the same way. A truncated, never-closed `<think>` yields reasoning and no answer rather than a
mangled answer.

**Ollama / local models.** `model: ollama/<name>` works with any LiteLLM-reachable local server (`api_base_env_var` for a
remote one, or vLLM/LM Studio via their OpenAI-compatible URL). When tools are involved the model is routed to
`ollama_chat/` (the route with tool support) and `tool_choice: required` is downgraded to `auto`; a `<tool_call>{…}</tool_call>`
the server did not parse is recovered. See [`examples/qwen_local_agent/`](examples/qwen_local_agent/README.md).

**`policy:` -- guardrails on what the agent can do.** Omit it and nothing changes. Set it and the pipeline enforces, at its
own boundary, which env vars it may read (and hand to MCP subprocesses), which paths it may read/write, which hosts it may
call, which tools the model may see and with what arguments, which components `invoke_component` may run, and a budget
(cost / tokens / model calls / seconds). Typos in the block fail at load time. Denied tool calls are returned to the model
as an error so it can adapt; other violations raise `PolicyViolation` (or just log with `on_violation: warn`). Usage and
violations are recorded on each step as `<step>__policy_usage` / `<step>__policy_violation_detail`.

```yaml
policy:
  env:        {allow: [OPENAI_API_KEY, "MCP_*"]}
  filesystem: {read: [./data], write: [./out]}
  network:    {allow_hosts: [api.openai.com, "*.corp.example.com"]}
  tools:
    deny: ["*delete*", "*write*"]
    max_calls_per_run: 20
    arg_rules: [{tool: "fs__*", arg: path, under: [./data]}]     # also: hosts, allow_regex, deny_regex
  components: {allow: [FilterComponent]}
  budget: {max_cost_usd: 0.50, max_llm_calls: 40, max_seconds: 300}
```

### Cross-run budgets, circuit breaker, spend metadata

`budget.max_cost_usd` etc. cap ONE run. To cap spend across runs, give the budget a durable `ledger` and add per-day / month /
partition caps:

```yaml
policy:
  budget:
    max_cost_usd: 0.50                      # per run (unchanged)
    max_cost_usd_per_day: 25                # UTC day, summed over every run sharing the ledger
    max_tokens_per_day: 5000000
    max_cost_usd_per_month: 400
    max_cost_usd_per_partition: 2.00        # all runs of one partition key
    ledger: {dir: s3://my-bucket/agentic-ledger, compact_after_days: 7}   # local path or any fsspec URL
    breaker: {trip_after_violations: 3, cool_down_minutes: 60}            # optional, window_minutes defaults to the cool-down
    on_unknown_cost: warn                   # warn (default) | fail
```

- Before every LLM call the ledger total (which already includes this run's earlier calls) is checked against each cap: spent >= cap
  refuses the call; after a call, spent > cap reports "exceeded". Both go through the normal `on_violation` (`fail` raises `PolicyViolation`
  naming the cap, the spent amount and the scope key such as `day '2026-10-23'`; `warn` logs and continues). There is no `skip` mode.
- **Ledger layout / concurrency.** One small JSON file per (run, step) under `day/<date>/`, `month/<yyyy-mm>/`, `partition/<key>/`, written with
  atomic replace by that run only, so concurrent runs never lose each other's entries; readers sum the directory. Re-writing the same
  (run_id, step) is idempotent (per-field max), so a retried or re-delivered step is not double counted; the flip side is that a retry
  inside the same run that spends more than the failed attempt only adds the difference. **Read cost:** every cap check lists and reads the
  entries of up to three directories, per LLM call. `compact_after_days` folds older entries into one `_compacted-*.json` (idempotent even if two
  runs compact at once). Caps are enforced against what is in the ledger: a ledger that is unreachable fails the run (fail closed).
- **Circuit breaker.** Every budget violation (per-run or cross-run) is recorded in the ledger. After `trip_after_violations` of them within the
  window no LLM call starts until `cool_down_minutes` after the tripping violation; the error says when it re-opens. The breaker refuses even
  under `on_violation: warn`.
- **Unknown cost.** `litellm.completion_cost` has no price for some models (local, new, custom). Token caps still work. If a cost cap is set,
  such a call logs ONE warning per run (`warn`) or raises (`fail`); it is counted as `agentic_step_unknown_cost_calls` and contributes $0 to
  every total, so cost caps under-count for those models.
- Step policies may use the per-day/month/partition caps (checked against the pipeline's ledger) but not `ledger`/`breaker` themselves.

**Spend metadata (always emitted, no policy needed).** Every asset gets `agentic_step_cost_usd`, `agentic_step_tokens`,
`agentic_step_llm_calls`, `agentic_step_tool_calls`, `agentic_step_unknown_cost_calls`. The first emitted asset also carries the run totals as
numeric metadata: `agentic_cost_usd`, `agentic_tokens`, `agentic_llm_calls`, `agentic_tool_calls`, `agentic_latency_ms`,
`agentic_budget_violations`, and with a ledger `agentic_spent_today_usd` (+ `agentic_remaining_today_usd` when a daily cap is set, plus month /
partition equivalents). Those keys are the module-level `INSIGHTS_METRICS` list in `component.py` (key, unit_type, display name): register
them as Dagster+ Insights custom metrics (by metadata key) and Insights aggregates them across assets. Totals ride one asset on purpose, so summing
across assets does not multiply them. That Insights actually ingests them needs a live Dagster+ deployment and is not verified here.

### Prompt versions

Each LLM step carries `agentic_prompt_version` and `agentic_prompt_hash` (and every asset `agentic_prompt_versions`, a `{step: version}` map).
The hash (`p-` + 8 hex) covers the declared prompt template(s) and system prompt (placeholders unsubstituted, so no run-time data), model,
temperature and other LLM params, merged persona fields and nested sub-configs; wiring (`id`, `source`, `inputs`, `when`, `policy`) is
excluded. Name a version yourself with `version: v3` on the step (or on its `persona`); the hash is still emitted so an un-bumped edit is
detectable. `prompt_versions({"steps": [...], "personas": {...}})` is a pure module-level helper returning `{step_id: {version, hash}}`.
Each step's Dagster `code_version` hashes that step with *its own* persona resolved plus the non-budget policy, so editing one prompt (or
one persona) changes only the steps that use it, and Dagster then marks that asset stale and, once it is re-materialized, its downstream;
tuning a budget cap changes no version. The A/B experiments below key their outcomes by these versions.

This is enforcement at the component boundary, **not an OS sandbox**: an MCP stdio server still runs with the worker's OS
permissions (the policy controls its environment and which of its tools the model may call). In `per_step_ops` mode the
usage counters travel with the pipeline state, so budgets are still run-wide.

### A/B a prompt or model, judged by real outcomes

```yaml
type: ...AgenticPipelineComponent
attributes:
  asset_name_prefix: support
  partition_type: daily
  partition_start: "2026-01-01"
  source: {kind: file, path: "tickets/{partition_key}.txt"}
  steps:
    - {id: draft, op: llm_call, model: gpt-4o-mini, prompt_template: "Reply to: {text}"}
  outputs:
    assets: [draft]
    json_sinks: [{from: draft, path: "replies/{partition_key}-{receipt_id}.json"}]   # the id feedback must carry back
  receipts:                         # ONE generation receipt per run, in your ContextReceiptStoreComponent
    resource_key: context_receipts
    outcome_signals: {approve: true}          # optional: approve decisions become outcomes automatically
  experiments:
    - name: bigger-model
      steps: [draft]                          # candidate overrides may touch ONLY these steps
      variants: {gpt4o: {draft: {model: gpt-4o, temperature: 0.2}}}   # control = the config above
      fraction: 0.3                           # share of units that get the candidate
      seed: 7                                 # same (seed, name, partition) -> same variant, forever
      min_outcomes: 30
      promote_when: {confidence: 0.9, min_outcomes: 60}
```

Feed real feedback (thumbs, edits, escalations) back with `receipt_outcome_ingestion` (`mapping.receipt_id` = the `receipt_id` the sink wrote).
The WARN check `experiment_verdict` then reads `outcome_samples_by_code_version` for the control and candidate prompt versions and reports
`verdict` (`candidate_better` / `live_better` meaning control better / `inconclusive` / `insufficient_evidence`), `diff`, `ci_low` / `ci_high`,
`p_candidate_better`, `n_control` / `n_candidate`, `ready_to_promote` (needs `promote_when`) and mean cost per variant (from the
`agentic_cost_usd` materialization metadata). It never promotes anything: you edit the base config yourself. The statistics are the same bootstrap
as `rag_pipeline`'s `shadow_vs_live` (an inlined, drift-tested copy).

Things to know:

- **Assignment** is `sha256(seed|name|unit key) < fraction`; `unit: partition` (default for partitioned pipelines) keeps a partition on one variant across
  re-runs, `unit: run` re-rolls on every run (a re-run can flip sides). Not a stratified split: small fractions of few partitions can be lopsided.
- **What is recorded.** The receipt item's `source_code_version` is the combined prompt version of the experimented steps *under the assigned variant*
  (`id=version+id=version` for several), purpose `experiment:<name>:<variant>`; without an experiment it is a hash of every LLM step's version, purpose
  `generation`. Metadata on every emitted asset: `receipt_id`, `agentic_experiment`, `agentic_experiment_variant`, `agentic_receipt_code_version`, and the
  effective `agentic_prompt_version(s)`. The assets' Dagster `code_version` is the BASE config's, so running an experiment never marks anything stale.
  Editing a step you did NOT list while the experiment runs pools both recipes under the same versions; stop and restart the experiment instead.
- **Overrides**: `model`, `prompt_template`, `system_prompt` (`system`), `temperature`, `persona`, `version`, `max_tokens`, `api_key_env_var`, `api_base_env_var`.
  A variant must change the prompt version (identical overrides are rejected at load). Steps that set the model in a nested config (`route`, `debate`, ...)
  cannot be experimented on. A `persona` override only fills fields the step does not set inline (explicit step fields always win over a persona).
  `policy` and budgets apply to both variants unchanged.
- **Outcome signals** (`receipts.outcome_signals`, opt-in): `approve` decided approved -> `accepted` (0.5) / rejected -> `escalated` (-0.7) (timeouts and
  `pending` record nothing unless you map `timeout: <value>`); `verify` pass +0.5 / fail -0.7; `eval` overall scaled from `min..max` (default `1..scale`)
  to [-1, 1]. Each carries `outcome_ref: auto:<step>:<kind>` so an op retry supersedes itself. A receipt's effective outcome is the MEAN of its outcomes, so
  auto signals average with real downstream feedback on the same receipt. `verify` / `eval` are model-judged proxies, not real outcomes; the pipeline refuses
  to experiment on the judge step whose result is the signal. `approve` human decisions are the closest thing to a real outcome the pipeline sees itself.
- **Failure**: a store error is logged and the run carries on (`receipt_id` shows `unrecorded`) unless `receipts.fail_on_error: true`.
- Not verified here: a real warehouse-backed (BigQuery/SQL) store and a real Dagster+ instance for the cost lookup (tested on SQLite and an ephemeral instance).

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

### Asset graph this YAML produces

Four Dagster assets (one per step in `outputs.assets:`), chained by the `source:` field on each step. The final step is a fan-in `synthesize` op that pulls text from all three prior steps:

```
   source (literal)
   "Explain how transformer attention works."
        │
        ▼
   ┌───────────────────────────┐
   │  research_bot_routed      │  op: route          (router → specialist)
   └──────────┬────────────────┘
              │
              ▼
   ┌───────────────────────────┐
   │  research_bot_critiqued   │  op: critique_loop  (drafter ↔ critic × N)
   └──────────┬────────────────┘
              │
              ▼
   ┌───────────────────────────┐
   │  research_bot_debated     │  op: debate         (proposers → arbitrator)
   └──────────┬────────────────┘
              │
              │  ┌── source: routed ──────────┐
              │  ├── source: critiqued ───────┤
              │  └── source: debated ─────────┤
              ▼                               ▼
   ┌───────────────────────────────────────────┐
   │  research_bot_final                       │  op: synthesize  (fan-in)
   └──────────┬────────────────────────────────┘
              │
              ▼
        /tmp/final.txt   (text_sink)
```

Solid arrows = `source:` field on the next step's YAML. The three source-lines into `final` = its `sources: [routed, critiqued, debated]` fan-in — synthesize merges N upstream step texts.

Each asset name is `{asset_name_prefix}_{step_id}` (`research_bot_routed`, `research_bot_critiqued`, …). All four appear in the Dagster catalog and get typed metadata on every materialization.

### Scaffold this exact demo end-to-end

The walkthrough at [`examples/agentic_pipeline.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/agentic_pipeline.md) ships a one-liner that scaffolds a working `create-dagster` project with this 5-step pipeline preloaded, partitioned across 3 static partitions:

```bash
curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-community-components-cli/main/examples/setup_agentic_pipeline_demo.sh | bash
cd agentic-pipeline-demo
uv run dg dev
```

Costs ~$0.001 per full run (10 LLM calls × `gpt-4o-mini`). See the walkthrough for the partition demo, sink layout, and the Dagster+ Insights promotion recipe.

## Second example — chain downstream of another Dagster asset

The `upstream_asset` source is the most valuable pattern because it wires the pipeline into your existing data graph. The pipeline shows up as a downstream node of whatever asset produced the text — full lineage from raw source through the whole agentic reasoning chain to the final sink.

The cleanest way to try this is with the sibling `SyntheticPromptGeneratorComponent` — a partition-aware `str`-emitting prompt asset built specifically to snap into `source: {kind: upstream_asset, ...}`.

**Two-file project layout:**

`defs/prompts/defs.yaml` — emits a `str`-valued asset, one prompt per partition:

```yaml
type: dagster_community_components.SyntheticPromptGeneratorComponent
attributes:
  asset_name: research_topics
  prompts:
    attention: "Explain how transformer attention works in ~200 words."
    rag:       "How does retrieval-augmented generation improve LLM factuality?"
    rnn:       "Compare RNN vs Transformer architectures for sequence modeling."
```

`defs/pipeline/defs.yaml` — the agentic pipeline consumes it:

```yaml
type: dagster_community_components.AgenticPipelineComponent
attributes:
  asset_name_prefix: analysis
  source:
    kind: upstream_asset
    upstream_asset_key: research_topics       # <-- wires to the prompt asset
  steps:
    - id: routed
      op: route
      router: {model: gpt-4o-mini, api_key_env_var: OPENAI_API_KEY}
      specialists:
        - {name: technical, description: "CS / ML questions.",
           model: gpt-4o, api_key_env_var: OPENAI_API_KEY,
           system_prompt: "You are a senior ML engineer."}
        - {name: general, description: "General topics.",
           model: gpt-4o-mini, api_key_env_var: OPENAI_API_KEY,
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

Asset graph across the two defs files (both files share the same partition set — Dagster stitches the partitioned dependency automatically):

```
   research_topics                 (SyntheticPromptGeneratorComponent)
        │  static partitions: [attention, rag, rnn]
        │  each partition → the mapped prompt string
        ▼
   analysis_routed                 (AgenticPipelineComponent, op: route)
        │
        ▼
   analysis_critiqued              (op: critique_loop)
        │
        ▼
   out/{partition_key}.md          (text_sink, one file per partition)
```

Materialize one partition, then backfill all three:

```bash
uv run dg launch --assets '*' --partition attention
uv run dg launch --assets '*' --partition rag
uv run dg launch --assets '*' --partition rnn
```

Now iterating on prompts is a one-file YAML edit (add/remove/rename keys under `prompts:`) instead of a code change — and every prompt / decision / draft lives as a browsable Dagster asset with typed metadata.

**Alternative source shapes:**

- **Wikipedia / any HTTP source** — replace `SyntheticPromptGeneratorComponent` with a custom asset that returns a `str` (Wikipedia REST summary, an internal doc, a slack message body, etc.). The `upstream_asset` source accepts a `str` or a dict with a `text` key; anything else gets `str()`'d.
- **`file` source** — if the prompts already exist on disk, skip both components and use `source: {kind: file, path: "prompts/{partition_key}.txt"}` directly on the pipeline.
- **`literal` source** — one hardcoded prompt for a quickstart / smoke test (the Quick Example above).

See [`SyntheticPromptGeneratorComponent`](../../source/synthetic_prompt_generator/) for the three v1 modes (literal per-key mapping, template + topics, fixed single prompt) and the v2 roadmap (LLM- / local-ML-driven prompt synthesis).

## Op menu

| op | Shape | What it does |
|---|---|---|
| `llm_call` | 1 LLM call | Single completion over source text. Simplest possible op. |
| `route` | 1 router LLM → 1 specialist | Router LLM picks the best specialist agent from a set; specialist answers. Multi-agent selection based on soft signal ("does this read as a bug or a question?"). |
| `conditional_route` | deterministic picker → 1 specialist | Same shape as `route` but the picker is a code path (regex / contains / equals / jsonpath), not an LLM. Cheap, testable, reviewable. Use when the branch signal is structured (a label field, a subject-line pattern). |
| `debate` | N proposers → 1 arbitrator | Each proposer writes a proposal; arbitrator picks the winner. Multi-agent consensus. |
| `critique_loop` | drafter → critic → drafter, N times | Drafter writes; critic reviews; drafter revises. Optional `until_score_gte: N` — critic scores each draft `SCORE: X/100` and the loop stops early when X ≥ N (skipping the revise step). Cost-controlled refinement. |
| `synthesize` | 1 LLM merges N upstream step texts | Combine multiple prior step outputs into one coherent response. Fan-in. |
| `mcp_call` | 1 MCP tool invocation, no LLM | Direct call to an MCP server tool over stdio / http / sse / fastmcp. `tool_args` strings support `{text}` (source) + `{port_name}` (typed inputs) + `{partition_key}` + `{partition.<name>}` substitution. Turns "grounding-data fetch" into a first-class asset with lineage + metadata. |
| `tool_use_loop` | LLM has MCP tools; iterates picks until `finalize` | Open-ended agent tool-use loop. LLM picks a tool, sees the result, picks the next tool, ... bounded by `max_iterations`. One Dagster asset materializes with the full tool-call trajectory in metadata. The shape people reach for LangGraph for, done as one first-class asset. |
| `handoff` | Invoke user-provided callable (LangGraph / AutoGen / CrewAI / DSPy) | Bring existing framework code as ONE step of the Dagster pipeline. Framework's internal per-node lineage lives in asset metadata; adjacent Dagster steps stay first-class. Composition over wrapping. |
| `map` | Apply an LLM call to each item in a list source | Fan-out one LLM call per item in a JSON list. Aggregate per-item outputs into one asset (metadata carries the per-item results). `max_concurrent` opt-in threading. |
| `extract` | Text → structured JSON via schema | Reliable structured extraction using tool_choice-forced JSON. Supply `output_schema` (JSON Schema object); the returned dict lands in metadata (`extracted:`) and its JSON serialization becomes `text` for downstream steps. |
| `classify` | Text → label from a fixed set | Cheapest common op. Supply `labels: [...]`; LLM picks one via tool_choice-forced enum. Optional `include_rationale: true` for one-sentence explanations. |
| `reduce` | LLM-fold over a list, chunk by chunk | Solves the "list too big for one context window" problem. `chunk_size` items per fold call; prior summary + new chunk → updated summary. |
| `self_reflect` | ONE LLM call producing draft + self-critique + revised | Cost-sensitive alternative to `critique_loop` (which is 2N+1 calls). Structured `DRAFT / CRITIQUE / REVISED` sections parsed automatically. |
| `sub_pipeline` | Invoke an inline sub-pipeline as one step | Compose / reuse common step blocks without duplicating YAML. `steps:` is a full inline sub-pipeline; `output_step_id` picks which sub-step's text flows back to this asset. Sub-state isolated from outer state. |
| `agent_call` | Invoke a pre-declared agent by name | Dispatch to an agent declared in the top-level `agents:` block. Kinds: `openai_assistant` (thread + run against OpenAI Assistants API), `remote_agent` (authenticated HTTP call to your own deployed agent — sync or async polling), `handoff` (Python callable for LangGraph/AutoGen/CrewAI). One asset materializes with the agent's reply + latency + cost. |
| `delegate` | 1 picker LLM → 1 DISCOVERED agent (MCP or HTTP) | Dynamic sibling of `route`/`agent_call`: instead of a fixed inline list, the candidate pool is discovered at build time from `AgentCardComponent` instances in the same defs folder and/or an external agent-card manifest. `required_tags`/`required_capabilities`/`input_mode` cheaply pre-filter the pool (no LLM involved) before a picker LLM matches your `task:` against each surviving candidate's declared `skills` (same forced-function-call mechanism as `route`); the pick is then invoked — over MCP (reusing this file's own `_call_mcp_tool_async`, which validates the rendered `tool_args` against the picked agent's real, server-declared `inputSchema` before calling, by default) or plain HTTP (reusing `_call_remote_agent`). Use this when the set of agents isn't known at pipeline-write time, scales to hundreds of registered agents, or changes without a YAML edit. |
| `invoke_component` | Call an EXISTING component's real asset directly, in-process | Reuse a deterministic transform already in this repo's catalog (`FilterComponent`, `SortComponent`, `UniqueDedupComponent`, ...) mid-pipeline, between two agent steps — without reimplementing its logic and without it showing up as a new node in your asset lineage. Resolves `component_type`, instantiates it with `attributes` (same shape as its own YAML config), calls its real `build_defs()`, then calls the resulting `AssetsDefinition` directly as a function with `dagster.build_asset_context()` (a real Dagster testing utility for invoking an asset's body outside of a run) and this step's data as a DataFrame. No materialization event, no graph node — pure in-process reuse of real, already-tested logic. |
| `plan` | planner → executor (± tools) → verifier → **replan** → synthesis | Plan-and-execute. The planner breaks the goal into ordered tasks; the executor does each (a plain call, or a tool loop when `executor.mcp_servers` is set) with the results so far; an optional `verifier` checks each task and a failure **replans the remaining work** (`max_replans`); a final call writes the answer. |
| `best_of_n` | N candidates → verifier/vote → 1 winner | Sample N answers (optionally across `models:`), keep the best by a `verifier` (`llm` / `regex` / `json` / `contains`) or by majority vote (self-consistency). Candidates, scores and agreement land in metadata. |
| `cascade` | models cheapest→strongest, escalate on failure | Try the cheap/local model first; escalate only when its answer fails `accept` (a verifier) or errors. `escalations` records which model answered and why earlier ones didn't. |
| `verify` | answer + evidence → verdict | Grounding check: is the answer supported by the evidence? Returns `verdict`, `score`, `passed`, `unsupported_claims`; `on_fail: fail` makes it a hard gate. The answer passes through unchanged. |
| `eval` | output (+ reference) → rubric scores | LLM-as-judge on a weighted `criteria:` rubric; `scores`, `rationales`, `overall`; `fail_below` is a hard gate. |
| `decide` | input + evidence → ONE named outcome | A decision record: pick from a fixed `outcomes:` menu with reason, confidence and quoted evidence. Off-menu answers use `default_outcome` or fail — never silently pick. Feed `when:` to branch on `outcome`. |
| `approve` | human gate with a review queue | Token `<approval_dir>/<key>.json`, **bound to what was approved** (see [Approvals](#approvals-what-a-token-is-bound-to)). Writes a pending record to `<approval_dir>/pending/` (the review queue), can wait `wait_seconds`, and applies a safe default on timeout (`on_timeout: reject`). Options `reuse` (`until_changed` default / `once` / `always`), `max_age_hours`, `bind_to`. Pair with `when:` so only exceptions reach a person. `mode: park` ends the run at the gate and resumes it from a sensor (no worker waits; see [Park an approval](#park-an-approval-mode-park-no-worker-waits)). |

## Typed named inputs (v2 — join any op from any prior op by port name)

Any op can declare `inputs: {port_name: {from: step_id} | {literal: value}}` to read from arbitrary prior steps by name. Each port name becomes a `{port_name}` placeholder in `prompt_template` and `system_prompt` (and, for `mcp_call`, in string `tool_args`).

This is the "execution-plan graph" shape — every node has typed named I/O, edges wire specific outputs to specific inputs, joins are first-class instead of a synthesize-only special case. Direct visual match to Prefect-style execution plans or LangGraph joins.

```yaml
- id: preliminary
  op: synthesize
  inputs:
    issue_facts:      {from: intake}
    reproduction:     {from: reproduction}
    docs_evidence:    {from: docs_evidence}
    repo_evidence:    {from: repo_evidence}
    history_evidence: {from: history_evidence}
    triage_policy:    {literal: "defect | expected | needs-info"}
  model: gpt-4o
  api_key_env_var: OPENAI_API_KEY
  system_prompt: |
    You are the triage lead. Merge the evidence + policy into a decision.
  prompt_template: |
    ISSUE_FACTS
    ==========
    {issue_facts}

    REPRODUCTION
    ============
    {reproduction}

    ... (one section per named input)
```

**Backward compat:** `source: <id>` and `sources: [<ids>]` still work. If both are present, `inputs:` takes precedence for named substitution; `source:` continues to feed `{text}` for legacy templates.

**Supported ops:** `llm_call`, `synthesize`, `mcp_call`. The multi-agent ops (`route` / `debate` / `critique_loop`) have their own structured sub-configs — use them as-is; use `inputs:` on the join steps that consume their outputs.

## Named personas — declare LLM sub-configs once, reference by name

Instead of duplicating `system_prompt` / `model` / `temperature` across steps that share a persona, declare them once at the top of the component:

```yaml
type: dagster_community_components.AgenticPipelineComponent
attributes:
  personas:
    security_skeptic:
      model: gpt-4o
      api_key_env_var: OPENAI_API_KEY
      system_prompt: "You are a security-focused reviewer. Challenge risks explicitly."
      temperature: 0.3
    perf_skeptic:
      model: gpt-4o
      api_key_env_var: OPENAI_API_KEY
      system_prompt: "You are performance-minded. Challenge scale assumptions."
    lead_engineer:
      model: gpt-4o
      api_key_env_var: OPENAI_API_KEY
      system_prompt: "You are a senior engineer picking the strongest critique."

  steps:
    - id: skeptic_debate
      op: debate
      proposers:
        - persona: security_skeptic
        - persona: perf_skeptic
      arbitrator:
        persona: lead_engineer
```

**Persona fields** (all optional; merged into the referencing sub-config):

    model, api_key_env_var, api_base_env_var, system_prompt, temperature, max_tokens,
    reasoning_effort, thinking_budget

**Reference sites** — a `persona: <name>` field is recognized at:

- **Step level** — `llm_call`, `classify`, `extract`, `reduce`, `self_reflect`, `map`, `tool_use_loop`
- **Sub-config level** — `route.router`, `route.specialists[*]`, `conditional_route.specialists[*]`, `debate.proposers[*]`, `debate.arbitrator`, `critique_loop.drafter`, `critique_loop.critic`

**Merge rules**: inline fields on the step ALWAYS win over persona-provided fields — the persona is a defaults-provider, not an override. Undeclared fields on the persona bundle (e.g. accidentally-declared `tools`) are silently dropped so a persona can't leak arbitrary config into unrelated sub-configs.

**Nested pipelines** — personas declared at the top level are inherited into `sub_pipeline` steps automatically.

## Named agents — call pre-built agents by name (`agent_call`)

Not every "agent" is an LLM completion. Some agents live elsewhere: an OpenAI Assistant with pre-configured tools, an HTTP endpoint you deployed to Vercel / Cloud Run / Modal, or a Python callable wired to LangGraph / AutoGen / CrewAI. Declare each one once in a top-level `agents:` block and dispatch to it with the `agent_call` op.

```yaml
type: dagster_community_components.AgenticPipelineComponent
attributes:
  agents:
    triager_openai:
      kind: openai_assistant
      assistant_id_env_var: OPENAI_ASSISTANT_ID
      api_key_env_var: OPENAI_API_KEY
      poll_interval_seconds: 1
      poll_timeout_seconds: 60

    ops_bot:
      kind: remote_agent
      url: "https://ops-bot.internal.example.com/chat"
      method: POST
      auth_bearer_env_var: OPS_BOT_TOKEN   # → Authorization: Bearer <token>
      payload_template:
        prompt: "{prompt}"
        priority: "{extra.priority}"
      response_text_path: "$.output.text"
      # Async / long-poll (optional — only if the endpoint returns a status_url)
      poll_url_path: "$.status_url"
      poll_terminal_status_path: "$.status"
      poll_terminal_success: [completed]
      poll_interval_seconds: 2
      poll_timeout_seconds: 120

    plan_and_act:
      kind: handoff
      entry_module: mypkg.agents
      entry_callable: run_langgraph_agent
      framework: langgraph

  source: {kind: literal, text: "Look up ticket #4131 and summarize."}

  steps:
    - id: replied
      op: agent_call
      agent: ops_bot
      prompt_template: "{text}"
      extra_context:
        priority: "high"
```

**Agent kinds**:

| kind | What it is | Required fields |
|---|---|---|
| `openai_assistant` | Thread + run against OpenAI's Assistants API. The assistant's tools / files / system prompt are configured on OpenAI's side; you pass the user prompt and poll the run to completion. | `assistant_id_env_var`, `api_key_env_var` |
| `remote_agent` | Authenticated HTTP call to your own deployed agent. Supports sync (immediate JSON reply) and async (returns a `status_url`; polled until terminal status). | `url`, `method` (default `POST`), `response_text_path` (JSON path into the reply) |
| `handoff` | Python callable — for wrapping LangGraph / AutoGen / CrewAI / DSPy agents. The callable receives `(prompt, **kwargs)` and returns `{"final_answer": str, "n_llm_calls": int, "cost_usd": float}`. | `entry_module`, `entry_callable` |

**`op: agent_call` fields**:

| Field | Required | Default | Description |
|---|---|---|---|
| `agent` | ✅ | — | Name of an agent declared in the top-level `agents:` block. |
| `prompt_template` |  | `"{text}"` | Prompt sent to the agent. Supports `{text}` (source), `{port_name}` (typed inputs), and `{extra.<key>}` for values from `extra_context`. |
| `system_prompt` |  | — | For `remote_agent` / `handoff` agents that accept a system prompt (forwarded in `payload_template` / callable kwargs). |
| `extra_context` |  | — | Free-form `{key: value}` map passed into the prompt (`{extra.key}`) and into the outbound payload / callable kwargs. |
| `source` / `inputs` |  | — | Standard step-input plumbing (same shape as every other op). |

**Nested pipelines** — agents declared at the top level are inherited into `sub_pipeline` steps automatically (same as personas).

### Reasoning & governance ops, `when:` and per-step `policy:`

The ops above (`plan`, `best_of_n`, `cascade`, `verify`, `eval`, `decide`, `approve`) are about *whether to trust* a model's output and
*who is accountable* for acting on it. Two step-level keys work on **any** op:

- **`when:`** run the step only if a condition on an earlier step holds; otherwise it is recorded as `skipped` (text passed through, no model
  call). `{source: <step id>, field: <result key — default: its text>, equals | in | contains | regex | gt | gte | lt | lte | truthy | falsy, not: true}`.
  This is how you "review only the exceptions": `approve` with `when: {source: decision, field: outcome, equals: needs_review}`.
- **`policy:`** a step-level policy that **narrows** the pipeline `policy:` — it can only be *more* restrictive, never less (every check must
  pass the pipeline policy first). Its `budget:` counts only what that step spends. Use it to give `triage` read-only tools and `publish` none.

Every emitted asset also carries a per-step Dagster `code_version` (a hash of the step, personas and policy) and a `<step>__pipeline_version`
metadata value, so a changed prompt marks exactly that step and what depends on it stale. See
[`examples/governed_invoice_matching/`](examples/governed_invoice_matching/README.md) for all of it in one pipeline.

### Common fields (every step)

| Field | Required | Default | Description |
|---|---|---|---|
| `id` | ✅ | — | Step id — referenced by `source:` / `sources:` on downstream steps and by `outputs.assets:`. |
| `op` | ✅ | — | Any op in the **Op menu** above. |
| `when` |  | — | Run only if a condition on an earlier step holds (see *Reasoning & governance ops*). |
| `policy` |  | — | Step-level policy that narrows the pipeline `policy:`. |
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
| `reasoning_effort` |  | — | Reasoning-model effort level (`low` / `medium` / `high`). Forwarded to OpenAI `o1` / `o3` models via LiteLLM `reasoning_effort`. Silently dropped by models that don't accept it (LiteLLM `drop_params=true`). |
| `thinking_budget` |  | — | Max tokens the model may spend on reasoning traces before the visible answer. Forwarded to Gemini 2.5+ (`thinking_budget`) and Anthropic thinking models. Set `0` on Gemini for short structured outputs to avoid silent truncation (thinking tokens are drawn from `max_tokens`). Silently dropped by models that don't accept it. |

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

Two input modes — pick one:
- **`inputs: {port: {from: id}, ...}`** — typed named join. Each port becomes a `{port_name}` placeholder in `prompt_template` + `system_prompt`. Preferred for new pipelines.
- **`sources: [<step_ids>]`** — positional legacy shape. Each source's text is labeled and concatenated; template uses `{combined}` + `{n_sources}`.

| Field | Required | Default | Description |
|---|---|---|---|
| `inputs` | one of | — | `{port_name: {from: step_id} | {literal: value}}`. Typed named multi-input join. |
| `sources` | one of | — | `[<step_ids>]`. Positional list — legacy shape. |
| `model` | ✅ | — | LiteLLM model string. |
| `api_key_env_var` |  | Provider default | Env var holding API key. |
| `system_prompt` |  | — | System prompt. `{port_name}` placeholders substitute when `inputs:` is used. |
| `prompt_template` |  | `inputs`: labeled per-port sections; `sources`: `{combined}` fan-in | Prompt template. |
| `temperature` |  | `0.0` | Sampling temperature. |
| `max_tokens` |  | `4096` | Max completion tokens. |
| `api_base_env_var` |  | — | Custom base URL env var. |

### `op: mcp_call`

Direct MCP tool call — no LLM, deterministic step. Use for the "fetch grounding data as a first-class asset" pattern (swap a `url:` source for a real MCP step with lineage + metadata).

| Field | Required | Default | Description |
|---|---|---|---|
| `server` | ✅ | — | MCP server spec: `{name, type: stdio|http|sse, command|url, env|headers|headers_env}`. `env` is a literal dict (no `${VAR}` interpolation — for secrets, leave unset and let the stdio subprocess inherit parent env). `headers_env: {header: env_var_name}` for deferred http/sse secrets. |
| `mcp_tool_name` | ✅ | — | Tool name as the MCP server exposes it (e.g. `get_issue`). |
| `tool_args` |  | `{}` | `{arg_name: value}`. String values support `{text}` (source) + `{port_name}` (typed inputs) substitution. |
| `parse_as` |  | `auto` | `auto` (try JSON, fall back to text) / `json` / `text`. |
| `source` |  | Most recent step | Legacy — feeds `{text}` in string `tool_args`. |
| `inputs` |  | — | Typed named inputs — feeds `{port_name}` in string `tool_args`. |

### `op: tool_use_loop`

Open-ended agent tool-use loop. LLM picks a tool → tool runs → LLM sees result → picks next tool → ... bounded by `max_iterations` OR the LLM calling the synthetic `finalize` tool. One Dagster asset materializes with the full tool-call trajectory in metadata. Cost / latency / tokens rolled up across every LLM + MCP call in the loop.

The shape LangGraph is known for, done as ONE first-class Dagster asset. Reach for this when the pipeline needs exploratory reasoning ("figure out what data to fetch, then answer") rather than a fixed step DAG.

| Field | Required | Default | Description |
|---|---|---|---|
| `model` | ✅ | — | LLM the agent runs on. |
| `api_key_env_var` |  | — | Env var holding the model provider's key. |
| `mcp_servers` | ✅ | — | List of MCP server configs (same shape as `mcp_call.server`). Tools from every server are auto-discovered. |
| `max_iterations` |  | `10` | Hard cap. Loop stops here regardless of finalize state. |
| `system_prompt` |  | Sensible default | Agent instructions. Should include "call `finalize` when done." |
| `prompt_template` |  | `{text}` | Initial user turn. Supports `{text}` + `{port_name}` + `{partition.<name>}` substitution. |
| `allowed_tools` |  | All discovered | Optional allowlist (tool names). |
| `finalize_tool_name` |  | `finalize` | Name of the synthetic finalize tool the LLM calls to end the loop. |
| `temperature` |  | `0.0` | |
| `max_tokens` |  | `2048` | Per-call max completion tokens. |

Emitted metadata includes `tool_call_trace`, `n_llm_calls`, `n_tool_calls`, `stop_reason` (`finalize_called` | `final_answer_no_tool` | `max_iterations`), plus per-turn latency + cost.

### `op: handoff`

Framework-composition op. Invoke a user-provided callable — bring your own LangGraph / AutoGen / CrewAI / DSPy code as ONE step of the Dagster pipeline. Adjacent Dagster steps stay first-class; the framework's per-node lineage lives inside this asset's metadata blob.

| Field | Required | Default | Description |
|---|---|---|---|
| `entry_module` | ✅ | — | Python module path (e.g. `my_project.reasoners`). |
| `entry_callable` | ✅ | — | Function name in that module. Signature: `def fn(**initial_state) -> dict`. |
| `initial_state` |  | `{}` | Dict of kwargs passed to the callable. String values get `{text}` + `{port_name}` + `{partition.<name>}` substitution. |
| `output_text_key` |  | `final_answer` | Key in the returned dict whose value is the downstream text. |
| `framework` |  | — | Metadata-only label (`langgraph` / `autogen` / `crewai` / `dspy` / etc.) surfaced in metadata + logs. |

Framework deps live in the customer's project (not `dagster-community-components`) — the component imports by string at runtime. Best-effort roll-up: if the callable's return dict has `cost_usd` / `n_nodes_executed` / `n_llm_calls` / `n_tool_calls` / `tokens_total`, those surface at the top level so they land in Insights alongside native Dagster + LiteLLM cost tracking.

### CrewAI end-to-end example

Compose a two-agent CrewAI research/writer crew as ONE step of a Dagster pipeline. Adjacent steps stay native `llm_call` / `route` / `synthesize` — you get CrewAI's multi-agent orchestration on the inside of one asset, and Dagster's asset graph, cost/latency metadata, backfills, and Insights outside.

**1. Install CrewAI in your project (not in `dagster-community-components`):**

```bash
uv add crewai crewai-tools langchain-openai
```

**2. Write the crew as a plain Python callable — `my_project/crews/research.py`:**

```python
from crewai import Agent, Crew, Process, Task
from langchain_openai import ChatOpenAI


def run_research_crew(topic: str, **_) -> dict:
    """Two-agent crew: researcher → writer. Returns the shape the
    `handoff` op expects (final_answer + best-effort cost roll-up)."""
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.2)

    researcher = Agent(
        role="Senior Research Analyst",
        goal=f"Uncover the most important recent developments in {topic}",
        backstory="You are a rigorous analyst who prefers primary sources.",
        llm=llm,
        verbose=False,
    )
    writer = Agent(
        role="Technical Writer",
        goal="Turn research bullets into a concise executive brief",
        backstory="You write clean, quotable prose. No fluff.",
        llm=llm,
        verbose=False,
    )

    research_task = Task(
        description=f"Research the latest on: {topic}",
        agent=researcher,
        expected_output="A bulleted list of the 5 most important findings, each with a citable source.",
    )
    write_task = Task(
        description="Turn the research bullets into a 200-word executive brief.",
        agent=writer,
        expected_output="A polished 200-word brief.",
        context=[research_task],
    )

    crew = Crew(
        agents=[researcher, writer],
        tasks=[research_task, write_task],
        process=Process.sequential,
        verbose=False,
    )
    result = crew.kickoff()

    # CrewAI 0.60+ tracks usage; roll it up into the shape `op: handoff`
    # picks up automatically (cost_usd / n_llm_calls / tokens_total /
    # n_nodes_executed become asset metadata + feed Dagster+ Insights).
    usage = getattr(crew, "usage_metrics", None)
    tokens = int(getattr(usage, "total_tokens", 0) or 0) if usage else 0
    n_calls = int(getattr(usage, "successful_requests", 0) or 0) if usage else 0

    return {
        "final_answer": str(result),
        "n_llm_calls": n_calls,
        "tokens_total": tokens,
        "n_nodes_executed": len(crew.tasks),
        # cost_usd optional — supply if you compute it yourself; LiteLLM's
        # native cost tracking on adjacent steps still works regardless.
    }
```

**3. Point the pipeline at it — `defs/research.yaml`:**

```yaml
type: dagster_community_components.AgenticPipelineComponent
attributes:
  asset_name_prefix: research_brief

  source:
    kind: literal
    text: "quantum computing chip breakthroughs 2026"

  steps:
    # The CrewAI multi-agent handoff. This entire two-agent crew is ONE
    # Dagster asset — its internal graph (researcher → writer) lives in
    # metadata, not in Dagster's asset graph.
    - id: crew_research
      op: handoff
      framework: crewai
      entry_module: my_project.crews.research
      entry_callable: run_research_crew
      initial_state:
        topic: "{text}"       # {text} = the upstream literal source

    # Adjacent native Dagster step — critique the crew's brief for
    # accuracy + hedge language. Runs on gpt-4o-mini via LiteLLM.
    - id: critique
      op: critique_loop
      source: crew_research
      drafter: {model: gpt-4o-mini, api_key_env_var: OPENAI_API_KEY}
      critic:  {model: gpt-4o-mini, api_key_env_var: OPENAI_API_KEY}
      iterations: 1

  outputs:
    assets: [crew_research, critique]
    text_sinks:
      - {from: critique, path: "output/research_brief_{partition_key}.md"}
```

**4. What lands in Dagster:**

Two assets — `crew_research` and `critique` — each with the standard cost/latency/tokens metadata. `crew_research`'s metadata carries the CrewAI-specific roll-up you returned from `run_research_crew`:

```
crew_research
  polars/... (n/a for handoff)
  framework:             crewai
  n_llm_calls:           4        ← from crew.usage_metrics.successful_requests
  tokens_total:          2847     ← total across both agents
  n_nodes_executed:      2        ← len(crew.tasks)
  handoff_wall_seconds:  6.31
  handoff_entry:         my_project.crews.research.run_research_crew
```

**Why compose instead of wrap**

- **Backfills / retries / partitions** — `dg launch --assets research_brief --partition 2026-08-24` re-runs one day of the CrewAI crew as a first-class Dagster run. CrewAI itself has no concept of partitions.
- **Cost visibility** — CrewAI's usage bubbles up alongside adjacent LiteLLM cost from the `critique` step. One asset, one row in Insights.
- **Substitution without rewrite** — swap `handoff` for a native `debate` op later without touching the surrounding pipeline. Or add a `route` step upstream to send only certain topics to the CrewAI crew and route the rest to a cheaper single-model path.
- **Framework deps stay in YOUR project** — `dagster-community-components` doesn't import `crewai`. Users who don't use CrewAI don't pay for its install footprint or version pins.

The same shape works for LangGraph, AutoGen, LlamaIndex Agents, DSPy — swap the `framework:` label and the callable. See `op: agent_call` (below the op menu) for the pre-declared-agent pattern when you want to reuse the same crew across multiple pipelines.

### `op: conditional_route`

Deterministic picker — regex / contains / equals / JSON-path against upstream text — routes to the matching specialist. Sibling of `route`, but the pick is a code path, not an LLM call.

| Field | Required | Default | Description |
|---|---|---|---|
| `conditions` | ✅ | — | List of `{when: {regex\|contains\|equals\|jsonpath: ...}, then: <specialist_name>}`. First match wins. |
| `default` | ✅ | — | Specialist that runs when no condition matches. |
| `specialists` | ✅ | — | List of `{name, description, model, api_key_env_var, system_prompt, temperature, max_tokens}` — same shape as `route`'s specialists. |
| `source` |  | Most recent step | Upstream to inspect. |

`conditions[N].when` supports one of:
- `regex: "p[01]"` — Python `re.search` (case-insensitive) against upstream text
- `contains: "urgent"` — substring test
- `equals: "p0"` — exact match after strip
- `jsonpath: "$.labels[0]"` — parse upstream as JSON, walk path, truthy check on resolved node. Combine with `value_equals` / `value_contains` for exact/substring comparisons on the resolved node.

Half the cost of `route` (skips the router LLM). Rule of thumb: if you can write the picker as a unit test, use `conditional_route`.

### `op: map`

Apply an LLM call to each item in a list source. Aggregates per-item results into ONE Dagster asset — per-item outputs live in metadata (`items:`), joined text lives in `text`. Fan-out inside a single step.

| Field | Required | Default | Description |
|---|---|---|---|
| `model` | ✅ | — | LLM per item. |
| `api_key_env_var` |  | — | Env var for the model provider. |
| `prompt_template` |  | `{item}` | Per-item prompt. Placeholders: `{item}` (current), `{index}`, `{n}` (total), `{text}` (raw source), plus typed inputs `{port_name}`. |
| `system_prompt` |  | — | Applied to every per-item call. |
| `max_concurrent` |  | `1` | Thread-pool workers. `1` = sequential; higher for I/O-bound throughput. |
| `output_join` |  | `newlines` | How to join per-item texts into the top-level `text`. `newlines` (double-newline separator) / `jsonl` / `none` (downstream reads `items[]`). |
| `temperature` |  | `0.0` | |
| `max_tokens` |  | `1024` | Per-call max completion tokens. |

Source parsing: JSON array → items. Falls back to `dict → data|items|results|values` list, then to non-empty newlines.

### `op: extract`

Structured JSON extraction — text → dict matching an `output_schema`. Uses `tool_choice="required"` with a function whose parameters are the schema, so the LLM is forced to emit valid JSON via a tool call. More reliable than prompt-engineering "return JSON".

| Field | Required | Default | Description |
|---|---|---|---|
| `model` | ✅ | — | |
| `api_key_env_var` |  | — | |
| `output_schema` | ✅ | — | JSON Schema object (`{type: object, properties: {...}, required: [...]}`). |
| `strict` |  | `true` | Missing required fields raise. `false` → return the extracted dict as-is. |
| `system_prompt` |  | Sensible default | Override for domain-specific extraction instructions. |
| `prompt_template` |  | `{text}` | User prompt template. |
| `temperature` |  | `0.0` | |
| `max_tokens` |  | `1024` | |

Extracted dict → `metadata.extracted` + JSON-serialized → `text` (so downstream steps can `source: <this step>` normally).

### `op: classify`

Text → label from a fixed set. Simplest, most common enterprise op. Uses `tool_choice="required"` with a single-field enum parameter.

| Field | Required | Default | Description |
|---|---|---|---|
| `model` | ✅ | — | Small model is fine (`gpt-4o-mini` etc.). |
| `api_key_env_var` |  | — | |
| `labels` | ✅ | — | Non-empty list of label strings — the enum. |
| `include_rationale` |  | `true` | Include a one-sentence explanation. `false` → cheaper. |
| `system_prompt` |  | Sensible default | |
| `prompt_template` |  | `{text}` | |
| `temperature` |  | `0.0` | |
| `max_tokens` |  | `256` | |

`text` = the picked label. `metadata.label` + `metadata.rationale` + `metadata.labels` (the full set). Raises if the LLM returns a label outside `labels`.

### `op: reduce`

Fold an LLM over chunks of a list. Solves "list too big for one context window" — chunk by chunk, prior summary + next chunk → updated summary. Sequential (later chunks depend on earlier ones).

| Field | Required | Default | Description |
|---|---|---|---|
| `model` | ✅ | — | |
| `api_key_env_var` |  | — | |
| `chunk_size` |  | `10` | Items per fold call. Bigger chunk → fewer calls + wider context requirement. |
| `initial_prompt_template` |  | Sensible default | First-chunk prompt. Placeholders: `{items}` (rendered), `{n}`. |
| `fold_prompt_template` |  | Sensible default | Subsequent-chunk prompt. Placeholders: `{prior}` (running summary), `{items}`, `{n}`, `{chunk_index}`, `{n_chunks}`. |
| `system_prompt` |  | — | Applied to every fold call. |
| `temperature` |  | `0.0` | |
| `max_tokens` |  | `2048` | Per-call max completion tokens (for the growing summary). |

Metadata: `n_items`, `n_chunks`, `chunk_size`, model fingerprint like `gpt-4o-mini@reduce×5`.

### `op: self_reflect`

ONE LLM call that produces draft + self-critique + revised. Cost-sensitive alternative to `critique_loop` (which is `2N+1` calls). Uses a structured prompt requiring `DRAFT / CRITIQUE / REVISED` sections.

| Field | Required | Default | Description |
|---|---|---|---|
| `model` | ✅ | — | |
| `api_key_env_var` |  | — | |
| `system_prompt` |  | Sensible default | Override to force domain-specific critique dimensions. |
| `prompt_template` |  | `{text}` | |
| `temperature` |  | `0.0` | |
| `max_tokens` |  | `4096` | Needs room for all three sections. |

Metadata: `draft`, `critique`, `revised`, `parsed` (bool — whether the structured parse succeeded; `false` → whole response returned as `text`). Cheaper than a 2-iteration critique_loop (~40% of the calls) at some quality cost.

### `op: sub_pipeline`

Invoke an inline sub-pipeline (a `steps:` list) as ONE step of the outer pipeline. Enables composition + reuse of common patterns without duplicating YAML.

| Field | Required | Default | Description |
|---|---|---|---|
| `steps` | ✅ | — | Inline sub-pipeline step list (same schema as top-level `steps:`). |
| `output_step_id` |  | Last sub-step | Which sub-step's `text` flows back to this asset's `text`. |
| `source` |  | Most recent outer step | Feeds the sub-pipeline's initial `source.text`. |
| `sub_source` |  | — | Override the sub-source completely (`{kind: literal|file|url, ...}`) — use when the sub-pipeline should start from something other than the upstream text. |
| `inputs` |  | — | Typed inputs — each `port_name` becomes a readable step id inside the sub-pipeline (sub-steps can `source: <port_name>`). |

Sub-pipeline state is completely isolated from outer state — sub-steps can't see outer step ids (except via `inputs:` port bridging). Cost / latency / tokens roll up from the sub-pipeline into this step's metadata.

### `op: agent_call`

Dispatch to a pre-built agent declared in the top-level `agents:` block. See the [Named agents](#named-agents--call-pre-built-agents-by-name-agent_call) section above for the agent-declaration shape + supported kinds (`openai_assistant`, `remote_agent`, `handoff`).

| Field | Required | Default | Description |
|---|---|---|---|
| `agent` | ✅ | — | Name of an agent from the top-level `agents:` block. |
| `prompt_template` |  | `"{text}"` | Prompt string. Supports `{text}`, `{port_name}` (from `inputs:`), and `{extra.<key>}` (from `extra_context:`). |
| `system_prompt` |  | — | System prompt; forwarded to `remote_agent` payloads / `handoff` callable kwargs where supported. |
| `extra_context` |  | — | `{key: value}` map — passed into the prompt (`{extra.key}`) and into the outbound payload / callable kwargs. |
| `source` / `inputs` |  | — | Standard input plumbing. |

Metadata: `agent`, `agent_kind`, `n_llm_calls`, `cost_usd` (when the agent reports it), `latency_ms`. The `remote_agent` kind additionally surfaces `url`, `method`, `status_code`, and `polled` (bool — whether the async-poll path was taken).

### `op: delegate`

Dynamic sibling of `route`/`agent_call`: instead of a fixed, inline-YAML agent list, the candidate pool is *discovered* — from `AgentCardComponent` instances declared in the same defs folder, and/or an external agent-card manifest — and a picker LLM matches your `task:` against each candidate's declared `skills`/`tags` before invoking the pick over MCP or plain HTTP. See the [`agent_card` component](../agent_card/README.md) for how to declare an agent.

```yaml
steps:
  - id: handled
    op: delegate
    source: customer_message          # optional -- extra context for the picker
    task: "Look up the refund status for this customer's order."
    required_tags: [refunds]          # optional -- pre-filter candidates by skill tag before the LLM sees them
    required_capabilities: [lookup]   # optional -- pre-filter by a verb taxonomy specific to this project (see agent_card/README.md)
    input_mode: text/plain            # default -- drops candidates whose declared default_input_modes excludes this
    registry:
      discover_siblings: true         # default -- scan AgentCardComponent instances in this defs folder
      manifest_path: /path/to/agents.json   # optional -- also merge in an external agent-card manifest
      max_candidates: 20
    picker:
      model: gpt-4o-mini
      api_key_env_var: OPENAI_API_KEY
    fallback: general_support_agent   # optional -- agent_id to use if the picker fails to pick validly
    validate_mcp_schema: true         # default -- validate tool_args against the picked MCP agent's real inputSchema
```

| Field | Required | Default | Description |
|---|---|---|---|
| `task` | ✅ | — | Free-text description of what this step needs done; also passed to the invoked agent as its instruction. |
| `picker.model` | ✅ | — | Model making the pick. Same sub-config shape as every other op's LLM config. |
| `required_tags` |  | — | Pre-filter candidates by skill tag (topic/domain) before the picker LLM ever sees them. |
| `required_capabilities` |  | — | Pre-filter candidates by `capabilities` (a verb taxonomy specific to this project — critique, supervise, triage, translate, ... — see [`agent_card` README](../agent_card/README.md)) before the picker LLM ever sees them. The lever that keeps `delegate` usable with hundreds of registered agents. |
| `input_mode` |  | `text/plain` | MIME type of what this step is sending. Candidates that DO declare `default_input_modes` (a real A2A AgentCard field) but don't list this mode are dropped; cards that don't declare modes at all are never excluded on this basis. |
| `registry.discover_siblings` |  | `true` | Scan sibling `AgentCardComponent` instances in the same defs folder. |
| `registry.manifest_url` / `manifest_path` |  | — | Also merge in an external flat JSON array of agent cards (same dual-source loader `catalog_agent` uses for its component manifest). `manifest_path` takes precedence when both are set. |
| `registry.max_candidates` |  | `20` | Cap on candidates sent to the picker LLM. |
| `fallback` |  | — | `agent_id` to use if the picker fails to pick validly; without it, an invalid pick fails the step (same as `route`). |
| `validate_mcp_schema` |  | `true` | For an MCP-backed pick: lists the server's tools and validates the rendered `tool_args` against that tool's real, server-declared `inputSchema` before calling — catches a `tool_args_template` that doesn't actually match the real tool, with a clear error, instead of a confusing live MCP failure. Set `false` only if a server's schema introspection is unreliable. |
| `source` |  | — | Optional upstream ref. Its text becomes `{extra.src_text}` for a card's `tool_args_template`/HTTP payload — NOT automatically part of `task`; see the gotcha note below. `task:` stands alone without it. |
| `inputs` |  | — | Typed named ports (`{port: {from: step_id}}`, same primitive every other op uses) — substituted as `{port_name}` directly into `task` BEFORE anything else happens, so a step like an arbitrator can reference two prior steps by name in one `task:` string. |

Metadata: `picked_agent_id`, `picker_reasoning`, `picking_source` (`picker_tool_call` or `fallback`), `invocation_mode` (`mcp` or `http`), `invocation_detail`, `candidates_considered`, `latency_ms`.

Invocation reuses this component's own `_call_mcp_tool_async` (the same helper `mcp_call`/`tool_use_loop` use) and `_call_remote_agent` (the same helper `agent_call`'s `remote_agent` kind uses) verbatim — no new network-call code. The `http` path is a pragmatic bridge, not full A2A-protocol compliance (no task polling/streaming, no OAuth negotiation) — just enough synchronous request/response to dynamically reach a hosted agent.

**A real trap, worth stating plainly:** an MCP agent card's `tool_args_template` substitutes `{prompt}` with this step's `task:` text — an *instruction*, not your upstream data. If this `delegate` step also has data flowing in via `source:`/a prior step, that data is a SEPARATE placeholder, `{extra.src_text}` — template something like `{ticket_context: "{extra.src_text}"}`, or combine both: `"Task: {prompt}\n\nData:\n{extra.src_text}"`. Forgetting `{extra.src_text}` doesn't fail loudly — the agent just never receives the real data, and an LLM-backed agent will still confidently answer, fabricated from nothing. See [`examples/support_ticket_triage/README.md`](examples/support_ticket_triage/README.md) for exactly this mistake caught via real end-to-end verification.

**Ceiling, stated plainly:** `required_tags`/`required_capabilities`/`input_mode` and `validate_mcp_schema` are the real enforcement surfaces available — A2A's MIME-type modes, MCP's `inputSchema`, and this project's own capability-tag convention. There's no standard for deeper semantic matching than "does the declared MIME type match" / "does the JSON Schema validate" — an agent can still be the wrong *semantic* fit for a task even after passing every check here. That's still the picker LLM's job.

**A real layout trap, confirmed via an actual `dg launch` run (not just inspection):** Dagster's component scanner only loads a component from a file literally named `defs.yaml`, one per directory. If you declare an `AgentCardComponent` as, say, `triage_agent.yaml` sitting next to your pipeline's own `defs.yaml` in the same folder, it is **never loaded at all** — `delegate`'s sibling-discovery finds zero candidates, silently (the bare `try/except: pass` in `_discover_sibling_agent_cards` swallows the mismatch rather than erroring). Each `AgentCardComponent` needs its own sibling directory, e.g.:

```
defs/
  my_pipeline/
    defs.yaml        # the AgenticPipelineComponent
  my_agent/
    defs.yaml         # one AgentCardComponent
```

`delegate`'s sibling scan reads the shared *parent* directory (`defs/` above) — so siblings need to be, structurally, siblings of the pipeline, not files alongside it in the same directory.

### `op: invoke_component`

Reuse a deterministic transform already in this repo's component catalog — `FilterComponent`, `SortComponent`, `UniqueDedupComponent`, `RankComponent`, or any of the ~112 `assets/transforms/*` components — mid-pipeline, between two agent steps, without reimplementing its logic and without it appearing as a new node in your asset lineage.

**How it works:** resolves `component_type`, instantiates it with `attributes` (the exact same shape you'd write under that component's own YAML `attributes:` block), calls its *real* `build_defs()`, then calls the resulting `AssetsDefinition` **directly as a function** — Dagster assets are directly callable, the same mechanism Dagster's own testing docs use — passing `dagster.build_asset_context()` (a real, public Dagster testing utility built for invoking an asset's body outside of a real run) and this step's current data as a DataFrame. No materialization event, no run, no new asset/lineage node — the target component's real, already-tested logic runs in-process and hands back a result.

```yaml
steps:
  - id: filter_severe
    op: invoke_component
    source: categorized_tickets                     # a prior step whose text is a JSON list
    component_type: dagster_community_components.FilterComponent
    attributes:
      asset_name: placeholder                       # required by FilterComponent's own schema; unused here
      upstream_asset_key: placeholder/upstream       # ditto -- no real asset graph gets built
      condition: 'category == "severe"'
  - id: triaged
    op: delegate
    source: filter_severe
    task: "Triage this batch of severe support tickets."
    required_capabilities: [triage]
    picker: {model: gpt-4o-mini, api_key_env_var: OPENAI_API_KEY}
```

| Field | Required | Default | Description |
|---|---|---|---|
| `component_type` | ✅ | — | Dotted path, e.g. `dagster_community_components.FilterComponent`. |
| `attributes` | ✅ | — | Dict passed to the target component's constructor — same shape as its own YAML `attributes:` block. **Important:** most components' own Pydantic model requires asset-graph-specific fields (an `asset_name`, an `upstream_asset_key` or `source`) even though this op bypasses the graph entirely. Their *values* don't matter here (no real asset gets registered) — only that they satisfy that component's own validation. Pass placeholders for those. |
| `input_kwarg` |  | auto-detected | Which of the target asset's input names receives this step's data. Auto-detected when the target declares exactly one input (the common case for a single-upstream transform); required if it declares more than one. |
| `asset_name` |  | — | If the target's `build_defs()` returns more than one asset, which one (by Dagster asset name) to invoke. |
| `resources` |  | — | Forwarded to `build_asset_context(resources=...)`, for a target that needs a real Dagster resource (e.g. a warehouse client) to run. |
| `partition_key` |  | — | Forwarded to `build_asset_context(partition_key=...)`. |
| `source` / `inputs` |  | — | Its text is parsed as a JSON list (same parser `map`/`reduce` use) and converted to a DataFrame for the target's input. |

Metadata: `component_type`, `asset_name`, `input_kwarg`, `n_input_rows`, `latency_ms`. Output `items`/`text` are in the same shape `map`/`classify` produce, so `invoke_component` composes freely with any other op before or after it — including `delegate`, for exactly the "deterministic filter/rank/dedupe mid-stream between two agent steps" pattern.

## State model

Every step's output is a dict `{text: str, ...op-specific fields}`. Steps read text from a prior step by `source:` id; omit `source:` and it defaults to the pipeline's original input — **not** "whatever step ran last." Dependency structure is always declared, never implied by position in the `steps:` list: two steps that don't reference each other are independent branches off the same input, regardless of the order they're written in or executed in. Use typed `inputs: {name: {from: <id>}}` ("ports") for multi-parent fan-in — see "Typed named inputs" below. The asset-graph lineage shown for `outputs.assets` is derived from these same declared refs, so it always reflects the real logical dependency structure, not execution order.

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
| `database` | Run a SQL query and use the result (rendered as a markdown table) as context |
| `upstream_asset` | Chain the pipeline downstream of any other Dagster asset |

All string fields on sources support `{partition_key}` substitution — a partitioned pipeline reads a different file / URL / literal per partition.

### `kind: literal`

| Field | Required | Description |
|---|---|---|
| `kind` | ✅ | `literal` |
| `text` | ✅ | Static text used as the pipeline's initial input. `{partition_key}` / `{partition.<name>}` / `{partition_start}` / `{partition_end}` templated. **Partitioned with no placeholder here and none in any step: rejected at load** (every partition would process identical input). |

### `kind: file`

| Field | Required | Description |
|---|---|---|
| `kind` | ✅ | `file` |
| `full_read_per_partition` |  | `true` = every partition deliberately reads the same file; silences the load warning. |
| `path` | ✅ | Filesystem path. `{partition_key}` / `{partition_start}` / `{partition_end}` templated (`questions/{partition_key}.txt`). Relative paths resolve against the code-location's working directory. Partitioned with no placeholder: **warning at load**. |

### `kind: url`

| Field | Required | Description |
|---|---|---|
| `kind` | ✅ | `url` |
| `url` | ✅ | URL to fetch. `{partition_key}` / `{partition_start}` / `{partition_end}` templated. Response body becomes the source text. Partitioned with no placeholder: **warning at load**. |
| `headers` |  | Optional `{header: value}` map sent with the request. Values are templated with `{partition_key}` / `{partition.<name>}` / `{partition_start}` / `{partition_end}`. Not for secrets. |
| `headers_env` |  | Optional `{Header-Name: ENV_VAR}` map, resolved at run time with `dagster.EnvVar` so tokens never sit in YAML (`headers_env: {Authorization: SRC_AUTH}`; the env var holds the full header value, e.g. `Bearer ...`). An unset variable fails the run naming the variable and header; values are never logged. |
| `full_read_per_partition` |  | `true` silences the load-time "no partition placeholder" warning for a source that deliberately re-reads the same content in every partition (a "latest snapshot"). |

### `kind: database`

| Field | Required | Description |
|---|---|---|
| `kind` | ✅ | `database` |
| `query` | ✅ | SQL query to run. Placeholders `{partition_key}`, `{partition.<name>}`, `{partition_start}`, `{partition_end}` (ISO timestamps of a time-window partition) are bound as **complete quoted SQL string literals** with embedded quotes doubled: write `WHERE day >= {partition_start}`, not `'{partition_start}'` (a placeholder already inside quotes is rejected). `{{` / `}}` are literal braces. Same semantics as `context_engineering_pipeline`'s warehouse query. Partitioned with no placeholder: **warning at load** (every partition reads the whole result). Placeholders without partitioning, or `{partition_start}` without a daily/weekly/monthly/hourly partition: rejected at load. Result is rendered as a markdown table and used as the pipeline's initial input. |
| `database_url` |  | SQLAlchemy connection URL. Set this OR `database_url_env_var`. |
| `database_url_env_var` |  | Env var holding the connection URL. |
| `full_read_per_partition` |  | `true` = every partition deliberately reads the whole result; silences the load warning. |
| `row_limit` |  | Max rows to render into the table (default 500) — keeps large result sets from blowing out the LLM context window. |

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
attributes:
  asset_name_prefix: daily_brief
  source: {kind: literal, text: "Summarise {partition_key}"}
  steps:
    - id: brief
      op: llm_call
      prompt_template: "{text}"
      model: gpt-4o-mini
      api_key_env_var: OPENAI_API_KEY
  outputs:
    assets: [brief]

post_processing:
  assets:
    - target: "*"
      attributes:
        partitions_def: {type: daily, start_date: "2026-01-01"}
        automation_condition: "{{ dg.AutomationCondition.eager() }}"
        tags: {tier: gold, team: ai-platform}
        owners: ["ai-team@company.com"]
```


### Approvals: what a token is bound to

An approval is bound to **what was approved**, not just to a key. When `approve` requests a decision it writes `pending/<key>.json` with a `fingerprint` (sha256 of the step id plus what the reviewer is shown: the full input text and the summary; `bind_to: [text, summary]` is the default, `[summary]` or `[text]` narrows it) and an `approve_by_writing.json` to copy. Approving **the pending record as written** is the natural flow: the token copies that fingerprint (`jq '.approve_by_writing.json + {approver: "me", approved_at: (now|todate)}' pending/K.json > K.json`).

| Option | Default | Meaning |
|---|---|---|
| `reuse` | `until_changed` | `until_changed`: the token is honoured while its fingerprint matches the current request. `once`: honoured, then **consumed** (`consumed_at` written to the token, kept as the audit record) when a run passes the gate, so every run needs a fresh approval. `always`: any token approves every later run of the key, fingerprint ignored (explicit opt-in). |
| `max_age_hours` | none | A token older than this (its `approved_at`, else the file's modification time) is expired. Applies under every `reuse` mode. |
| `bind_to` | `[text, summary]` | What is fingerprinted. Use `[summary]` when the source text is regenerated by a model each run, or every re-run would differ and need a new approval. |

A token that exists but cannot be honoured is never a mysterious failure: the run log warns with the exact reason, the pending record gets `state` + `reason`, and approval is requested again. Reasons: `input changed since approval`, `approval expired: ...`, `consumed: ...`, or `token carries no fingerprint` (a hand-written `{"approved": true}` is not bound to anything, so `until_changed` / `once` do not honour it; use `reuse: always` if that is what you want). A rejection token is bound the same way. Consumption and expiry touch only that key's own files, so concurrent partitions never share state; it works the same on a local directory and on any fsspec URI. A request that nobody answered with `on_timeout: reject` stays in the queue marked `outcome: timeout_default_reject`, so it can still be approved as written. `once` consumes when the run passes the gate; if a later step then fails, the approval is already spent.

### Park an approval (`mode: park`): no worker waits

`mode: wait` (the default) polls the approval store for `wait_seconds`, so a worker or pod sits pinned for as long as the reviewer takes. `mode: park` ends the run at the gate instead:

```yaml
- {id: gate, op: approve, source: draft, approval_dir: s3://acme/approvals, mode: park,
   park_expires_hours: 48, on_timeout: reject, max_resumes: 3}      # checkpoint_dir defaults to <approval_dir>/checkpoints
```

1. The run writes the pending record (`parked: true`, shown by `agent_review_queue`) and a **JSON checkpoint** of the state so far (`<checkpoint_dir>/<prefix>.<step>.<key>.<fingerprint>.json`, written atomically; strict JSON, never pickle), then **succeeds and exits**.
2. The component generates `<prefix>_approval_resume_job` and `<prefix>_approval_resume_sensor` (every `park_sensor_interval_seconds`, default 30; started by default). When the approval store holds a decision the *same rules as `wait`* honour (fingerprint, `reuse`, `max_age_hours`), the sensor requests a **new run** (`run_key` = checkpoint + fingerprint + attempt number; tags `agentic/resume_checkpoint`, `agentic/resume_of_run`; the checkpoint's partition). Dagster has no pause/resume of a run, so a resume is always a second run.
3. The resumed run claims the checkpoint (exactly one run can), loads it, **skips every step already done** (no second model call, no second generation receipt, same experiment variant), applies the decision (`on_reject: stop` fails the run like `wait`), runs the rest and archives the checkpoint to `checkpoints/archive/`. Without a decision once `park_expires_hours` has passed, the sensor resumes it to apply `on_timeout` (`approve` / `reject` / `fail`); `pending` or no `park_expires_hours` means it waits forever.

What the Dagster UI shows after the parked run: the steps before the gate **are materialized** (their values are final); the gate's asset and everything after it are **not materialized** by that run (no event, so a never-run asset stays "never materialized" and a previously materialized one keeps its old value and shows as stale), and the gate's asset carries an `AssetObservation` with `agentic_parked`. The first materialized asset has `agentic_parked: true`; the run is tagged `agentic/parked`. With `per_step_ops: true` the steps after the gate still appear as instant no-op steps in the parked run. Steps are parked by list position: independent branches listed after the gate also wait.

Rules worth knowing: a failed resume leaves the checkpoint resumable (new run per attempt) up to `max_resumes`, then it needs a person; per-run caps in `policy.budget` (calls/tokens/cost) are cumulative across a parked pipeline's runs while the wall-clock cap restarts, and the budget ledger records the resumed run's spend under its own run id; if the YAML changed while parked, the remaining steps use the new configuration (logged); a new input for the same key supersedes the old checkpoint. Load-time errors: `park` with `wait_seconds`, a `park` gate inside a `sub_pipeline`/`map`, or an `invoke_component` step before the gate (its raw records need not be JSON; a non-JSON value elsewhere fails the run naming the step, use `mode: wait` there). `wait` and `park` gates can be mixed. Not exercised: a live Dagster daemon and a real object store (tests drive the sensor with `build_sensor_context` and run what it requests); exclusive-create is atomic on a local filesystem and on fsspec backends that honour it.

### One partition per run, approvals per partition, `backfill_policy`

With the first-class `partition_type` / `partition_values` / `partition_dimensions` fields, each run resolves `{partition_key}`, sink paths and `approve` keys from **one** partition, and its output is one value per partition. So:

- `backfill_policy: multi_run` (one run per partition) is the supported shape; `backfill_policy_max_partitions_per_run` must be unset or `1`. `single_run` (or a cap above 1) is **rejected at load time**: a run covering several partitions has no single `partition_key`, so templates and approval keys could not be resolved. If such a run is forced anyway (for example by launching a range from the UI against a policy set elsewhere) it fails with a clear error instead of silently running as unpartitioned.
- **A source that never mentions the partition** reads identical input in every partition, so it is checked at load, per kind: `literal` is rejected (constant text can never differ) unless some step uses a partition placeholder (then a fixed source is a deliberate choice); `file` / `url` / `database` log a WARNING naming the field (the content behind a path/URL can legitimately change between runs, and a database query *can* be bound, but a deliberate full re-read is possible); `upstream_asset` is exempt. Set `source.full_read_per_partition: true` on a `file` / `url` / `database` source to say the re-read is deliberate and silence the warning (rejected on `literal` / `upstream_asset`, where it has no meaning).
- `approve` tokens are `<approval_dir>/<key>.json`, with the partition key as the default key, so partitions never share an approval. A key containing characters outside `A-Za-z0-9._-` gets a short hash suffix (`x/y` and `x_y` are different partitions and must not share a token); keys that are already safe are unchanged. Under partitioning, two `approve` steps without their own `key`, or a fixed `key` with no `{partition_key}` / `{partition.<name>}` in it, are rejected at load time (they would share one human decision across steps or partitions).
- The load-time guards (source warning/rejection, approve-key checks, `backfill_policy` validation) only see partitions declared with the component's own fields; partitions attached later through `post_processing` are not visible then. The **run-time** check does cover them: a run that targets a partition range (or an asset whose `partitions_def` was attached elsewhere) without a single partition key fails with the "covers several partitions" error, and `{partition_start}` / `{partition_end}` in a source resolve from the running asset's `partitions_def`. Not covered for `post_processing` partitions: the load-time warnings and the `backfill_policy` default (set `multi_run` on the asset yourself, or declare partitions with the component fields).

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
| `source` | `Dict[str, Any]` | Data source. Shapes: {kind: literal, text: '...'} \| {kind: file, path: '...'} \| {kind: url, url: '...'} \| {kind: database, query: 'SELECT ...', database_url: '...' or database_url_env_var: '...', row_limit: 500} (quer… _(full docs in schema.json + component README)_ |
| `steps` | `List[Dict[str, Any]]` | Ordered pipeline steps. Each step: {id, op, ...op-specific args}. Two wiring modes (choose per step, they compose): 1. **Legacy single-source**: `source: <step_id>` reads that step's text into `{text}` in the prompt (def… _(full docs in schema.json + component README)_ |
| `outputs` | `Dict[str, Any]` | Output declaration. Shape: {assets: [<step_ids>], text_sinks: [{from, path}], json_sinks: [{from, path}]}. `assets:` step outputs become first-class Dagster assets; `text_sinks:` writes step text to disk; `json_sinks:` w… _(full docs in schema.json + component README)_ |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"agents"` | Group name for emitted assets. |
| `kinds` | `List[str]` | — | Asset kinds. Default: ['llm', 'agent', 'pipeline']. |
| `tags` | `Union[Dict[str, Any], List[str]]` | — | Additional tags on emitted assets. |
| `owners` | `List[str]` | — | Asset owners. |
| `description` | `str` | — | Description on emitted assets. |
| `backfill_policy` | `Literal['single_run', 'multi_run']` | — | Backfill execution strategy for a partitioned pipeline. 'multi_run' launches one run per partition (the only shape this pipeline supports: each run resolves {partition_key}, sink paths and approval keys for exactly one p… _(full docs in schema.json + component README)_ |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily' \| 'weekly' \| 'monthly' \| 'hourly' \| 'static' \| 'dynamic' \| 'multi' \| None (unpartitioned). |
| `partition_cron_schedule` | `str` | — | Required when partition_type='cron'. A cron expression restricting the date axis to a custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on Dagster's TimeWindowPartitionsDefinition(cron_schedule=...). |
| `partition_cron_fmt` | `str` | `"%Y-%m-%d"` | strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule. |
| `partition_start` | `str` | — | ISO date for time-based partition types (daily/weekly/monthly/hourly/multi). |
| `partition_values` | `Union[str, List[str]]` | — | Comma-separated string OR list — the fixed partition keys for static/multi partitioning. e.g. ['NVDA', 'TSLA', 'META'] or 'NVDA,TSLA,META'. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name}. Set instead of partition_type for multi-dimensional partitioning. |
| `partition_key_parser` | `str` | — | Format template for parsing composite partition keys into named fields — e.g. '{owner}/{repo}#{issue_number}' means partition key 'dagster-io/dagster#30000' → {owner: 'dagster-io', repo: 'dagster', issue_number: '30000'}… _(full docs in schema.json + component README)_ |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `policy` | `Dict[str, Any]` | — | Guardrails on what this pipeline will do on the agent's behalf; omitted = no restrictions. Sections (all optional): `on_violation: fail\|warn` (default fail); `env.allow: [NAME, 'PREFIX_*']` (env vars the pipeline may re… _(full docs in schema.json + component README)_ |
| `receipts` | `Dict[str, Any]` | — | One generation receipt per run in a ContextReceiptStore resource (`resource_key`, `caller_id`, `policy_version`, `fail_on_error`, `outcome_signals`); see "A/B a prompt or model". |
| `experiments` | `List[Dict[str, Any]]` | — | One A/B experiment: `{name, steps, variants, fraction, seed, unit, min_outcomes, margin, window_days, promote_when, ...}`; adds the WARN check `experiment_verdict` with `receipts`. |
| `personas` | `Dict[str, Dict[str, Any]]` | — | Named reusable LLM sub-configs. Each persona bundles `{model, api_key_env_var, api_base_env_var, system_prompt, temperature, max_tokens, reasoning_effort, thinking_budget}`. Reference from any step / sub-config via `pers… _(full docs in schema.json + component README)_ |
| `agents` | `Dict[str, Dict[str, Any]]` | — | Named pre-built agents. Each entry declares `kind:` plus kind-specific connection config. Reference from a step via `op: agent_call, agent: <name>`. Supported kinds: `openai_assistant` (assistant_id + api_key_env_var — c… _(full docs in schema.json + component README)_ |
| `per_step_ops` | `bool` | `false` | When True, emit as a `@dg.graph_multi_asset` with one @op per step (visible in the Runs page + finer-grained retry). State dict flows through the IO manager between ops. Default False keeps the single-op @multi_asset sha… _(full docs in schema.json + component README)_ |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition when partition_type='dynamic'. |
| `backfill_policy_max_partitions_per_run` | `int` | — | For backfill_policy='multi_run' only. Must be unset or 1: a run with several partitions has no single partition_key. |

[//]: # (FIELDS:END)
