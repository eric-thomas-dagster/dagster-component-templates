# agentic_pipeline_authoring_tools

Read-only authoring/introspection tools for [`agentic_pipeline`](../agentic_pipeline) -- draft, check and preview a pipeline YAML from Claude Code or from an agent running inside a code location, against agentic_pipeline's own real validation, not a second hand-maintained copy of its schema.

| Tool | Answers |
|---|---|
| `list_ops()` | every op name + one-line summary |
| `describe_op(op)` | that op's real JSON Schema (fields/types/defaults/descriptions) -- generated live from the real Pydantic model, identical to the sibling [`ops_schema.json`](../agentic_pipeline/ops_schema.json) file |
| `list_examples()` | every shipped example's name + title + one-paragraph summary (from its README.md) |
| `get_example(name)` | that example's `pipeline.yaml` + `README.md`, verbatim |
| `validate_pipeline(yaml)` | agentic_pipeline's own load-time validation (real `AgenticPipelineComponent(**attrs)` construction + `build_defs()`) -- errors report `step_index`/`step_id`/`op`/`field` where resolvable |
| `preview_pipeline(yaml)` | the assets it would create (`<prefix>_<step>`), the edges between steps, whether (and how) it's partitioned, and the env vars / MCP servers / skill resource keys it needs -- **without running a single step** (`build_defs()` only constructs definitions; it never executes the pipeline body) |

Every result is a compact dict; failures come back as `{"error": "..."}` (unknown op/example, bad YAML), never an exception. `yaml` accepts either a bare attributes mapping or a full `type: .../attributes: {...}` component.yaml.

## Using from Claude Code (`claude mcp add`)

```bash
claude mcp add agentic-pipeline-authoring -- python -m dagster_community_components.assets.ai.agentic_pipeline_authoring_tools.component
```

No environment variables are required for `list_ops`/`describe_op`/`list_examples`/`get_example`/`validate_pipeline`/`preview_pipeline` -- all of them are pure introspection over files already on disk (agentic_pipeline's own `component.py` + `examples/`), so this works unmodified right after `pip install dagster-community-components`.

Pair it with [`SKILL.md`](./SKILL.md) (copy or symlink to `.claude/skills/agentic-pipeline-authoring/SKILL.md`) so Claude Code knows the pipeline's rules (one partition per run, the source/step graph, `outputs:`, which op to reach for, what each example shows, common mistakes) without re-deriving them from scratch each session.

## Wiring it into agentic_pipeline

Same dual-mode shape as `dagster_instance_tools`/`dagster_workflow_tools`/`knowledge_search_server`: `component.py` is itself a stdio MCP server (`pip install mcp`; verified with mcp 1.30.0 and 2.3.0).

```yaml
- id: draft_pipeline
  op: tool_use_loop
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  prompt_template: "Draft an agentic_pipeline YAML that {text}. Use list_ops/describe_op to pick the right op(s), list_examples/get_example for a close starting point, and validate_pipeline/preview_pipeline to check your draft before answering."
  mcp_servers:
    - name: authoring
      type: stdio
      command: [python, -m, dagster_community_components.assets.ai.agentic_pipeline_authoring_tools.component]
```

Tools are namespaced by the loop as `authoring__list_ops`, etc.

## As a Dagster resource

```yaml
type: dagster_community_components.AgenticPipelineAuthoringToolsComponent
attributes: {allow: [list_ops, describe_op, validate_pipeline, preview_pipeline]}
```
`context.resources.agentic_pipeline_authoring_tools.call("validate_pipeline", {"yaml": "..."})`.

## Referencing it by key (`mcp_server_resource_key`)

Agents pick an MCP server by `server_resource_key` only -- the inline `command: [python, -m, ...]` shape above works, but every `mcp_servers:` entry that wants this toolset has to repeat it by hand. Set `mcp_server_resource_key` on the component instead, and it registers a second, [`MCPServerResource`](../../../resources/mcp_server_resource)-shaped resource under that key -- built from this component's own `allow`/`max_result_chars` settings.

```yaml
type: dagster_community_components.AgenticPipelineAuthoringToolsComponent
attributes:
  resource_key: agentic_pipeline_authoring_tools
  mcp_server_resource_key: agentic_pipeline_authoring_tools_mcp
```

Then, in any MCP-aware component instead of an inline `server:`:
```yaml
server_resource_key: agentic_pipeline_authoring_tools_mcp
```

No env vars are strictly required by this server, but `mcp_extra_env_vars` is still available (same convention as the other three components) for anything a deployment-specific wrapper might need.

## How agentic_pipeline/component.py is loaded

By path (`Path(__file__).parent.parent / "agentic_pipeline" / "component.py"`), not via `dagster_community_components.assets.ai.agentic_pipeline.component` -- that dotted import only resolves in a built/installed wheel (hatchling's `force-include` nests `assets/` under the package at **build** time only; it isn't a real package path against this repo's own dev source checkout). A sibling-relative load works identically in both layouts, since `agentic_pipeline/` and this component's own directory are always siblings under `assets/ai/`. The module is loaded once and cached for the life of the process (it's ~9000 lines with its own heavy imports -- litellm/sqlalchemy/jsonschema).

## `preview_pipeline`'s "needs" section

- `env_vars`: a best-effort static scan of every string value under a key ending in `_env_var`, plus `bearer_env`, plus the values of any `headers_env` dict, anywhere in the parsed attributes. Does **not** account for `{{ env.X }}`-style Jinja templating a surrounding Dagster component.yaml might apply before this server ever sees the dict -- that's resolved at the YAML-loading layer.
- `mcp_servers`: every server entry referenced by a step (`mcp_call.server`, `tool_use_loop.mcp_servers`, `plan.executor.mcp_servers`), inline or via `server_resource_key`, including inside a `sub_pipeline`'s nested steps.
- `skill_resource_keys`: every `tool_use_loop.skill_keys` entry.
- A pipeline that references a `server_resource_key`/skill key not satisfied by anything in the snippet itself still previews correctly -- `preview_pipeline` stubs in a placeholder resource for whatever's missing so a standalone YAML snippet (not a fully-wired project) can still be checked.

## Safety

Read-only: every tool is introspection over files already on disk (`agentic_pipeline/component.py`, `examples/`) plus in-memory Pydantic construction. `validate_pipeline`/`preview_pipeline` never execute a pipeline step -- `build_defs()` only constructs Dagster definitions objects. `allow` is a hard allow-list; `max_result_chars` bounds every result (an example's README/pipeline.yaml, or a large preview, can otherwise be sizable).

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | `"agentic_pipeline_authoring_tools"` | Resource key the toolset is registered under |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `allow` | `List[str]` | — | `f"Hard allow-list of tools (default: all). Valid: {', '.join(ALL_TOOLS)}."` |
| `max_result_chars` | `int` | `20000` | Upper bound on the serialized size of any tool result (an example's README/pipeline.yaml or a preview can be large). |
| `mcp_server_resource_key` | `str` | — | If set, ALSO registers a stdio MCP-server-shaped resource (same name/type/command/env shape as MCPServerResource) under this key, built from this component's own settings -- python -m this module, with AGENTIC_PIPELINE_A… _(full docs in schema.json + component README)_ |
| `mcp_extra_env_vars` | `List[str]` | — | mcp_server_resource_key only. Extra env var NAMES to copy (current value, read fresh every time a consumer resolves the resource) from this process into the spawned MCP server's env. MCP's stdio client does not inherit t… _(full docs in schema.json + component README)_ |

[//]: # (FIELDS:END)
