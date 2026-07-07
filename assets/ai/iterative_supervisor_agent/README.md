# iterative_supervisor_agent

Iterative / chained supervisor. Planner runs **once per step**, sees all prior tool outputs, and picks the NEXT tool call — or declares `done`. Each step is its own Dagster asset. This is the "ReAct loop with full lineage" primitive.

Static DAG shape (`max_iterations` step assets pre-declared at YAML load), dynamic termination (whichever step says `done` short-circuits later steps into no-ops).

## Why this vs. SupervisorAgentComponent

| Feature | SupervisorAgentComponent | IterativeSupervisorAgentComponent |
|---|---|---|
| Planning | Single-shot — all picks upfront | Per-step — planner runs every iteration |
| Tool chaining | No (each tool independent) | **Yes** — step N sees step N-1's output |
| Cost | Cheaper (1 planner call) | More expensive (`max_iterations` planner calls) |
| DAG shape | Fan-out (N tools in parallel) | Chain (N steps sequential) |
| Best for | Tools that don't depend on each other | Tasks where each step needs prior context |

## Assets emitted (`max_iterations + 1` per YAML block)

| Asset | Purpose |
|---|---|
| `<step_asset_prefix>_1` … `<step_asset_prefix>_<N>` | Per-iteration planner + tool call. Emits `{iteration, done, tool, args, reasoning, tool_output}`. |
| `<synthesis_asset_name>` | Reads all step outputs, writes the final answer. |

Steps that fire *after* a `done` step short-circuit to no-op rows.

## Fields

| Field | Type | Description |
|---|---|---|
| `step_asset_prefix` | string (default `"agent_step"`) | Assets are named `<prefix>_1`, `<prefix>_2`, ... |
| `synthesis_asset_name` | string | Final synthesis asset. |
| `task` | string | The task the agent works on. |
| `tools` | list | Bounded tool set (same shape as `SupervisorAgentComponent`). |
| `max_iterations` | int (default `5`, range 1–15) | Number of pre-declared step assets. |
| `model` / `api_key_env_var` | | OpenAI-compatible model. |

## Example

```yaml
type: dagster_community_components.IterativeSupervisorAgentComponent
attributes:
  step_asset_prefix: agent_step
  synthesis_asset_name: agent_final_answer
  task: |
    Compute 149 euros × 12 months to find the annual cost. THEN translate
    the answer to French. Return the French sentence.
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  max_iterations: 5
  tools:
    - name: math_expert
      description: "Do arithmetic. Args: math expression string."
      system_message: "You are a calculator. Return the number + one-line explanation."
    - name: translator
      description: "Translate text. Args: {text, target_language}."
      system_message: "You are a translator. Return ONLY the translated text."
```

## Related

- `supervisor_agent` — single-shot planner, no chaining. Cheaper. Use when tools are independent.
- `mcp_tool_picker` — MCP-backed supervisor (single-shot). Iterative-MCP would follow the same pattern as this component.
- `langgraph_agent` / `openai_agent` / `anthropic_agent` — full-blown ReAct loops inside a single asset (no per-step Dagster visibility).
