# supervisor_agent

Planner LLM reads a task and picks which specialist tools to invoke, from a **bounded YAML-declared set**. Each pick fans out to its own downstream Dagster asset (per-tool LLM persona). A synthesizer LLM reads all tool outputs and writes the final grounded answer.

The "agent of agents" pattern with full Dagster lineage. Every step is a visible asset in `dg dev`.

## Why this shape

- **Bounded, safe.** The tool set is declared in YAML. The planner picks BY NAME. It cannot invent tools, cannot write code, cannot escape the sandbox.
- **Auditable.** The planner's picks + reasons are stored as an asset (`plan_asset_name`). Every tool's output is an asset. Full lineage.
- **Composable.** Gate the plan on an `asset_check`. Publish it to Slack for approval before tools run. Replay with the same task to reproduce.
- **Static DAG shape.** The N assets one-per-tool are declared at YAML load — makes the fan-out visible in `dg dev`. Tools the planner didn't pick still materialize (as empty DataFrames), so the DAG looks the same across runs.

For truly unbounded fan-out (planner may pick 3 tools OR 300), pair this with dynamic partitions on the tool executor. This component is the demoable, visually-consistent shape.

## Assets emitted

One YAML block → `2 + N` assets:

| Asset | Purpose |
|---|---|
| `<plan_asset_name>` | Planner's picks: DataFrame with columns `tool` / `tool_input` / `reason`. |
| `<tool.name>_result` (×N) | Per-tool asset — runs its LLM persona if the planner picked it; empty otherwise. |
| `<synthesis_asset_name>` | Final synthesized answer, grounded in the invoked tools' outputs. |

## Fields

| Field | Type | Description |
|---|---|---|
| `plan_asset_name` | string | Planner asset name. |
| `synthesis_asset_name` | string | Synthesizer asset name. |
| `task` | string | The task / question the supervisor plans against. |
| `tools` | list | Bounded list of `{name, description, system_message}`. |
| `model` | string (default `gpt-4o-mini`) | Model for planner + tools + synthesizer. |
| `api_key_env_var` | string (default `OPENAI_API_KEY`) | OpenAI-compatible key env var. |
| `api_base_env_var` | string, optional | Custom base URL env var (Vercel AI Gateway, etc.). |
| `temperature` | float (default `0.2`) | LLM temperature. |
| `planner_max_tokens` / `tool_max_tokens` / `synthesis_max_tokens` | int | Token caps for each stage. |
| `synthesis_system_message` | string, optional | Override synthesizer prompt. |
| `group_name` / `kinds` | string / list | Standard asset metadata. |

## Example

```yaml
type: dagster_community_components.SupervisorAgentComponent
attributes:
  plan_asset_name: supervisor_plan
  synthesis_asset_name: final_answer
  task: "How does Dagster compare to Airflow for ML pipelines?"
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  tools:
    - name: web_search
      description: "Search recent web for information + citations."
      system_message: |
        You are a web search agent. Return 2-3 relevant factual
        snippets with source names.
    - name: kb_expert
      description: "Answer from internal docs / KB with citations."
      system_message: |
        You are a docs-QA agent. Cite doc sections. Say so if unsure.
    - name: math_expert
      description: "Do arithmetic on numbers cited in the task."
      system_message: |
        You are a calculation agent. Return the number + one-line
        explanation.
```

## Related

- `data_remediation_asset` — companion primitive for the "agent-picks-actions" side of the pattern (bounded action space, executed declaratively).
- `openai_agent` / `anthropic_agent` / `langgraph_agent` — full-blown agent components (multi-step reasoning inside a single asset). Supervisor is the *between-assets* orchestration primitive.
- `mcp_tool_call` — for MCP-flavored tools (see the MCP Tool Picker walkthrough).
