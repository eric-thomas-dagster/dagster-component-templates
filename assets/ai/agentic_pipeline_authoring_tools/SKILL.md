---
name: agentic-pipeline-authoring
description: Author, check, and preview an `AgenticPipelineComponent` YAML (dagster-community-components). Use when drafting, editing, or debugging an agentic_pipeline component.yaml -- picking an op, wiring step dependencies, setting partitioning, or diagnosing a load-time error.
---

# Authoring an agentic_pipeline YAML

`AgenticPipelineComponent` is "one YAML, one agentic pipeline": a `steps:` list where each step is a typed op (25
available), each step becomes a real Dagster asset, and dependencies are declared explicitly -- never implied by
list position. This skill pairs with the `agentic_pipeline_authoring_tools` MCP server (tools: `list_ops`,
`describe_op`, `list_examples`, `get_example`, `validate_pipeline`, `preview_pipeline`); if that server isn't
attached in this session, add it with:

```bash
claude mcp add agentic-pipeline-authoring -- python -m dagster_community_components.assets.ai.agentic_pipeline_authoring_tools.component
```

## The rules

1. **One partition per run.** A partitioned pipeline materializes exactly one partition key per run. Dagster's own
   `materialize`/backfills already split any selected range into one run per partition for you. Never try to author
   "one run that covers several partitions" -- that's rejected at load time (and the error message now explains
   why, as of the fix shipped alongside this server).

2. **The source/step graph is explicit, not positional.** Every step reads from:
   - `source: <step_id>` (legacy, single upstream) -- omitted means the reserved id `source` (the pipeline's
     original input), **not** "whatever step ran last in the list."
   - `inputs: {port_name: {from: step_id} | {literal: value}}` (typed, named, any number of upstreams) --
     recommended for any join/fan-in.
   A step with neither is an independent branch off the root input, not an implicit continuation of the step
   before it. Call `preview_pipeline` on a draft to see the real edges it produces before assuming the graph is
   right.

3. **`outputs:` picks which steps become assets.** `outputs.assets: [<step_ids>]` -- a step NOT listed here still
   runs and can be read by a later step via `source`/`inputs`, it just doesn't materialize as its own Dagster asset.
   `outputs.text_sinks`/`outputs.json_sinks` are side-effect writes to disk, independent of that.

4. **Picking an op: start from `list_ops`, then `describe_op`.** Rough map of what's available:
   - Single LLM call: `llm_call`. Structured output: `extract` (JSON Schema) / `classify` (fixed labels).
   - Branching: `conditional_route` (deterministic: regex/contains/equals/jsonpath) vs. `route` (an LLM picks) vs.
     `delegate` (an LLM picks from a DISCOVERED pool of registered agents, not a fixed inline list).
   - Multiple candidates / iteration: `debate` (proposers + arbitrator), `critique_loop` (draft/critique/revise N
     times), `best_of_n` (N candidates, pick the best), `cascade` (cheapest-first model escalation),
     `self_reflect` (one-shot draft+critique+revised, cheaper than critique_loop).
   - Fan-out / fan-in: `map` (LLM per item in a list, one asset), `reduce` (fold over chunks), `synthesize` (merge
     N prior steps into one).
   - Tools / agents: `mcp_call` (one deterministic tool call, no LLM), `tool_use_loop` (open-ended agent+tools
     loop), `agent_call` (a pre-built agent declared in `agents:`), `handoff` (bring-your-own LangGraph/AutoGen/
     CrewAI/DSPy callable as one step).
   - Governance / control flow: `verify` (grounding/hallucination check against evidence), `eval` (LLM-as-judge
     rubric scoring), `plan` (plan -> execute -> verify -> replan -> synthesize), `decide` (named-outcome decision
     record), `approve` (human-in-the-loop gate -- see below), `invoke_component` (reuse an EXISTING deterministic
     component's real compute function mid-pipeline, e.g. a `filter`/`sort`/`rank`, without a new asset-graph node),
     `sub_pipeline` (nest another steps: list as one step).

5. **`approve` steps: `mode: wait` holds a worker; `mode: park` ends the run at the gate and a generated
   `<asset>_approval_resume_sensor` resumes it once a correctly-fingerprinted decision token exists.** A
   hand-written decision file with no `fingerprint` is refused by default (`reuse: until_changed`) -- reviewer
   tooling must copy the pending record's own `fingerprint` into the token (the `slack_approval_gate` component's
   `target: agentic_pipeline` mode does this correctly; so does agentic_pipeline's own `approve_pending()` helper).
   Under partitioning, give every `approve` step a `key` containing `{partition_key}` (or omit `key` entirely) --
   a fixed key is rejected at load time, since one human decision would otherwise clear every partition's gate.

6. **A tool-using step per item (map + tool_use_loop composition).** There is no single op that runs a full
   tool-use loop once per item in a list. Compose it: an `extract`/`classify` step (or your own upstream logic)
   produces the item list, then either (a) a `map` step for cheap per-item LLM calls with no tool access, or (b)
   when each item genuinely needs tools, loop at the orchestration layer -- a `handoff` step whose callable iterates
   the items and invokes a `tool_use_loop`-equivalent per item in Python (bring-your-own-loop), since `handoff`'s
   contract (`def fn(**initial_state) -> dict`) is exactly the escape hatch for "a shape map/tool_use_loop don't
   cover individually." Ask `describe_op` on `map` and `tool_use_loop` before assuming neither fits.

7. **Common mistakes `validate_pipeline`/`preview_pipeline` will catch:**
   - A literal source with no `{partition_key}`/`{partition.<name>}`/`{partition_start}`/`{partition_end}`
     placeholder under partitioning, and no step using one either -- every partition would process identical input.
   - `backfill_policy: single_run` on a partitioned pipeline -- rejected (no single partition_key to resolve).
   - `partition_type: hourly` with a bare `partition_start` -- accepted as midnight now, but confirm with
     `describe_op`/the component's own field docs if you need a specific hour.
   - A step referencing an `id` that doesn't exist, or a `server_resource_key`/`skill_keys` entry not satisfied
     anywhere in the project (`preview_pipeline`'s `needs` section lists exactly what must exist).

## Workflow for a new pipeline

1. `list_examples` -> `get_example` on the closest match; start from it, don't write from a blank page.
2. For each op you're unsure about, `describe_op` before guessing its fields.
3. `validate_pipeline` on your draft; fix every reported `step_id`/`field`/`message`.
4. `preview_pipeline` -- confirm the asset list and edges match your mental model of the graph, and that `needs`
   names only env vars/servers you actually intend to provide.
5. Only then hand the YAML back as a finished answer.
