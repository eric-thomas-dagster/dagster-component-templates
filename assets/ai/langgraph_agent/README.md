# langgraph_agent

Run a **LangGraph `StateGraph`** as a single Dagster asset, in one of two mutually exclusive modes:

- **`steps`** — a declarative prompt-chain DSL. Each step is an LLM call that reads the shared state (initial input + all prior step outputs) and appends its own output back to state. A step continues to another step only if it explicitly sets `next`; it can also route conditionally based on a regex check against its own output. Every node is hardcoded to "template a prompt, call one LLM" — there's no way to give a node custom logic, tool calls, or retrieval.
- **`graph_fn`** — bring your own graph. Point at a dotted path to a callable in your own project that returns an already-built LangGraph graph (a compiled `StateGraph`, or anything with `.invoke(state)`). The graph's real logic — tool calls, custom control flow, retrieval, subgraphs, checkpointers, anything LangGraph can do — lives in your own committed Python; this component just wires it up as a Dagster asset and invokes it.

**Which mode should I use?** If your pipeline is genuinely "templated prompt → LLM → templated prompt → LLM", `steps` is less code. The moment a node needs to do anything else — call a tool, hit a vector store, run arbitrary Python — `steps` can't express it and you want `graph_fn` instead, with the graph itself defined and versioned as real Python (locally, or registered with LangGraph Platform/Server if you deploy graphs separately).

If your pipeline is a single prompt over rows of a DataFrame, use [`langchain_chain_asset`](../langchain_chain_asset) instead of either mode here.

## Why LangGraph vs a plain chain (steps mode)

- **Explicit stateful graph.** Every node reads/writes a typed state dict — inspectable, replayable, and richer than a linear chain's implicit variables.
- **Conditional routing.** Early-exit, retry-on-parse-fail, or branch-and-merge patterns are one-liners (`condition_regex`).
- **Composable.** Multiple `langgraph_agent` assets can compose into larger DAGs via Dagster `deps`.

Note: streaming intermediate node outputs and checkpointed execution are things LangGraph itself supports, but this component does not currently expose either — they're only available if you drop to `graph_fn` and build that into your own graph.

## Example: `steps` mode

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

`condition_regex` lets a step branch based on its own output. Combine with `condition_else` to specify the false branch (defaults to `END`). Neither `quarantine` nor `allow` below sets `next`, so each one terminates at `END` right after running — they're alternate terminal outcomes of `classify`, not a continuation into each other:

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

## Example: `graph_fn` mode (bring your own graph)

```yaml
type: dagster_component_templates.LangGraphAgentComponent
attributes:
  asset_name: support_triage
  input_prompt: "{run_id}: escalation queue sweep"
  graph_fn: "myproject.graphs.support:build_graph"
```

```python
# myproject/graphs/support.py
from langgraph.graph import StateGraph, END

def build_graph(context):
    """Called as build_graph(context) -> graph. Build whatever graph you
    want using LangGraph's full API -- tool-calling nodes, retrieval,
    subgraphs, checkpointers -- none of that is expressible via `steps`."""
    graph = StateGraph(dict)
    graph.add_node("triage", my_triage_node)     # real Python, real tool calls
    graph.add_node("escalate", my_escalate_node)
    graph.set_entry_point("triage")
    graph.add_conditional_edges("triage", my_router, {"escalate": "escalate", END: END})
    graph.add_edge("escalate", END)
    return graph.compile()
```

The asset resolves `graph_fn` at *materialize* time (not component-load time), calls it once to get the graph, builds an initial state dict (`input_prompt`'s resolved value under `input`, plus any `initial_state` keys — `initial_state` wins on conflict), and calls `graph.invoke(state)`. Whatever your graph returns is the asset's materialized value; if it's a dict with a `final` key, that also becomes the `final_answer` metadata.

## Component-level fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | — | Output Dagster asset name. |
| `input_prompt` | `str` | `None` | Required in `steps` mode ({input} in every step). Optional in `graph_fn` mode (merged into initial state under `input`). |
| `steps` | `List[LangGraphStep]` | `None` | Declarative prompt-chain mode. Mutually exclusive with `graph_fn` — set exactly one. See the per-step fields below. |
| `graph_fn` | `str` | `None` | `'module.path:function_name'` dotted path to a callable returning an already-built graph. Mutually exclusive with `steps` — set exactly one. |
| `initial_state` | `Dict[str, Any]` | `None` | `graph_fn` mode only. Extra keys merged into the initial state, winning over `input_prompt`'s `input` key on conflict. |
| `llm_provider` / `model` / `api_key_env_var` / `api_base_env_var` / `system_message` / `temperature` / `max_tokens` | — | — | `steps` mode only — LLM call defaults, each overridable per-step. Unused in `graph_fn` mode. |

The table below (auto-generated) documents the per-step fields — each entry in `steps: [...]`:

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Unique node name. Referenced by `next` and templated as {outputs.<name>}. |
| `prompt` | `str` | Prompt template. Use {input} for the initial user prompt and {outputs.<step_name>} to reference a prior step's output. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `model` | `str` | — | Optional per-step model override (defaults to component-level model). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `system_message` | `str` | — | Optional system message for this step (overrides component-level system_message). |
| `temperature` | `float` | — | Optional per-step temperature override. |
| `max_tokens` | `int` | — | Optional per-step max_tokens override. |
| `next` | `str` | — | Next step name. Omitting this (or setting 'END') terminates this branch at END — there is no implicit fall-through to the next declared step, so every step that should continue must set this explicitly. |
| `condition_regex` | `str` | — | Optional regex. If set, the step's output is tested against it: on match, routes to `next`; on no-match, routes to `condition_else` (or END if unset). Great for early-exit or self-review loops. |
| `condition_else` | `str` | — | Where to route when condition_regex does NOT match. Defaults to END. |

[//]: # (FIELDS:END)

## Output

`steps` mode — materialized value is a dict:

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

Asset metadata surfaces: `mode` ("steps"), `final_answer` (markdown), `steps_run`, `steps_run_count`, `model`, `provider`, `stopped_by`, and a collapsible `step_outputs` JSON blob.

`graph_fn` mode — materialized value is whatever your own graph's final state dict is. Asset metadata surfaces: `mode` ("graph_fn"), `graph_fn` (the dotted path), a collapsible `final_state` JSON blob (non-JSON-serializable values, like LangChain message objects, are coerced to strings), and `final_answer` (markdown) if the final state has a `final` key.

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
