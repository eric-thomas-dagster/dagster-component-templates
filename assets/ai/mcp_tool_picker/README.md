# mcp_tool_picker

Planner LLM picks **real MCP tool calls** to invoke from a bounded YAML-declared set. Each pick becomes a Dagster asset that actually calls the MCP server (stdio / http / sse). A synthesizer LLM reads all tool outputs and writes the grounded final answer.

The "agent picks MCPs" pattern with full Dagster lineage.

## Why this shape

Same safety story as `supervisor_agent`, but the tools are real MCP server tools instead of LLM personas:
- **Bounded**: the tool list is declared in YAML. Planner picks BY NAME.
- **Auditable**: every pick + args + reason lands as an asset. Every MCP call's result is an asset.
- **Real MCP**: each tool invocation is a real `mcp.ClientSession.call_tool(...)` — stdio subprocess, HTTP endpoint, or SSE.

## Assets emitted

Emits `2 + N` assets per YAML block:

| Asset | Purpose |
|---|---|
| `<plan_asset_name>` | Planner's picks — DataFrame `[tool, args, reason]`. |
| `<tool.name>_result` (×N) | Per-tool asset; invokes MCP iff planner picked it. |
| `<synthesis_asset_name>` | Grounded final answer citing each MCP tool by name. |

## Fields

| Field | Type | Description |
|---|---|---|
| `plan_asset_name` | string | Planner asset name. |
| `synthesis_asset_name` | string | Synthesizer asset name. |
| `task` | string | Task the planner plans against. |
| `tools` | list of `MCPToolPickerToolSpec` | Bounded list. Each: `name`, `description`, `server` (MCPServerSpec), `mcp_tool_name`, optional `args_schema_hint`, `parse_as`. |
| `model` / `api_key_env_var` / `temperature` / `planner_max_tokens` / `synthesis_max_tokens` | | OpenAI-compatible model config. |

`server` supports stdio / http / sse transports (same shape as `MCPToolCallComponent`).

## Example

```yaml
type: dagster_community_components.MCPToolPickerComponent
attributes:
  plan_asset_name: mcp_plan
  synthesis_asset_name: mcp_final_answer
  task: "Find the largest .py file under /tmp/demo and show its first 20 lines."
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  tools:
    - name: list_dir
      description: "List files with sizes in a directory."
      args_schema_hint: '{path: string}'
      server:
        name: fs
        type: stdio
        command: ["npx", "-y", "@modelcontextprotocol/server-filesystem", "/tmp/demo"]
      mcp_tool_name: list_directory_with_sizes
    - name: read_head
      description: "Read a text file's contents."
      args_schema_hint: '{path: string}'
      server:
        name: fs
        type: stdio
        command: ["npx", "-y", "@modelcontextprotocol/server-filesystem", "/tmp/demo"]
      mcp_tool_name: read_text_file
```

## Related

- `supervisor_agent` — same pattern, tools are LLM personas instead of MCP calls.
- `mcp_tool_call` — one-shot MCP tool call, no LLM planning. Use when you know exactly which tool + args to invoke.
- `openai_agent` / `anthropic_agent` — full-blown agents (multi-step ReAct inside one asset). Use for tight tool-use loops; `mcp_tool_picker` is the *between-assets* orchestration primitive.
