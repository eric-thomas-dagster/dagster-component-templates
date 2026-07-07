# adaptive_research_brief

Planner LLM decides **N sub-topics at runtime**. A row-wise researcher writes one note per subtopic. A synthesizer combines the notes into a markdown brief. Where dynamic-N agentic patterns fit.

## Why this shape

- **Truly runtime N.** Planner decides 3 subtopics or 12 based on topic scope. The `subtopic_notes` asset iterates over however many the planner emitted.
- **Auditable.** Every subtopic + focus is a row on `research_plan`. Every research note is a row on `subtopic_notes`. Full lineage.
- **Single-asset iteration (v1).** All N notes are computed inside `subtopic_notes` in one materialization. For **per-subtopic UI visibility** and independent retry, pair the notes asset with dynamic partitions — see the walkthrough.

## Assets emitted (3 per YAML block)

| Asset | Purpose |
|---|---|
| `<plan_asset_name>` | Planner's list of sub-topics (variable N ≤ `max_subtopics`). |
| `<notes_asset_name>` | Row-wise researcher LLM — one note per subtopic. |
| `<brief_asset_name>` | Final markdown brief with headings + executive summary. |

## Fields

| Field | Type | Description |
|---|---|---|
| `plan_asset_name` / `notes_asset_name` / `brief_asset_name` | string | Asset names. |
| `topic` | string | The topic / question. |
| `max_subtopics` | int (default 8, range 1-30) | Upper bound the planner is told about. |
| `model` / `api_key_env_var` | | OpenAI-compatible model config. |
| `researcher_system_message` / `brief_system_message` | string, optional | Persona overrides. |

## Example

```yaml
type: dagster_community_components.AdaptiveResearchBriefComponent
attributes:
  plan_asset_name: research_plan
  notes_asset_name: subtopic_notes
  brief_asset_name: research_brief
  topic: |
    Prepare a competitive brief on Anthropic. Cover product, pricing,
    safety approach, and recent research directions.
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  max_subtopics: 8
```

## Related

- `supervisor_agent` — planner picks WHICH tools (of DIFFERENT kinds) to call. This component picks HOW MANY of the SAME work item (research a subtopic).
- `mcp_tool_picker` — MCP-tool version of the supervisor pattern.
- `langchain_chain_asset` — the row-wise LLM primitive this component wraps for the researcher step.
