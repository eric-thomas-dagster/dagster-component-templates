# dagster_workflow_tools

"Workflow as tool": **WRITE**-capable tools over the Dagster instance so an agent can trigger a
real materialize/run-job -- not just inspect, the way [`dagster_instance_tools`](../dagster_instance_tools)
(read-only) does. Deliberately a separate component: `dagster_instance_tools`' whole safety story
is "tools only call read APIs on the instance" -- bolting a write capability onto it would
silently change that guarantee for every existing user.

| Tool | Does |
|---|---|
| `materialize_assets(asset_keys, run_config?, tags?, partition_key?)` | Triggers a real, synchronous materialization of the given asset keys. Blocks until the run finishes. Returns `{success, run_id, asset_keys}`. |
| `run_job(job_name, run_config?, tags?, partition_key?)` | Triggers a real, synchronous run of an existing named job from `definitions_module`. Blocks until the run finishes. Returns `{success, run_id, job_name}`. |

Every result is a compact dict; failures come back as `{"error": "..."}` (bad job name, disallowed
asset/job, run failure), never as an exception. Every triggered run is tagged
`dagster-community/workflow-tool=true` plus `default_tags` plus any caller-supplied `tags` -- a
stable, auditable marker for "an agent triggered this run," independent of which tool/agent
component actually called it.

## Wiring it into agentic_pipeline

Same shape as `dagster_instance_tools`: `tool_use_loop` / `mcp_call` / `agent_call` get tools
**only from MCP servers**, so `component.py` is itself a stdio MCP server. **Must be invoked with
`python -m`, not a bare `python component.py`** -- `materialize_assets`/`run_job` need the real
importable module path (`__spec__.name`) to reconstruct the job in a step's own subprocess, which
a bare script invocation never has (same limitation `dagster.reconstructable()` itself documents
for code defined in `__main__`).

```yaml
- id: remediate
  op: tool_use_loop
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  prompt_template: "The `orders` asset is stale. Re-materialize it and report whether the run succeeded."
  mcp_servers:
    - name: dagster_workflow
      type: stdio
      command: [python, -m, dagster_community_components.assets.ai.dagster_workflow_tools.component]
      env:
        DAGSTER_HOME: /abs/path/to/dagster_home
        DAGSTER_WORKFLOW_TOOLS_DEFINITIONS_MODULE: my_project.definitions
        DAGSTER_WORKFLOW_TOOLS_ALLOW_ASSETS: "orders,orders_summary"
```

Tools are namespaced by the loop as `dagster_workflow__materialize_assets`, etc. Server env vars:
`DAGSTER_HOME` (required), `DAGSTER_WORKFLOW_TOOLS_DEFINITIONS_MODULE` (required -- unlike
`dagster_instance_tools`, a write tool must know its own scope, there is no degrade-gracefully-
without-it mode), `..._ALLOW` (csv, default both tools), `..._ALLOW_ASSETS` / `..._ALLOW_JOBS`
(csv hard allow-lists -- strongly recommended in production: an agent should materialize specific,
intended targets, not anything in the repo), `..._DEFAULT_TAGS` (JSON object, set automatically by
`mcp_server_resource_key` below -- see "Referencing it by key").

## Referencing it by key (`mcp_server_resource_key`)

The YAML above works, but it repeats `DAGSTER_HOME`/`DAGSTER_WORKFLOW_TOOLS_*` by hand in every
`mcp_servers:` entry that wants this toolset. Set `mcp_server_resource_key` on the component
instead, and it registers a second, [`MCPServerResource`](../../../resources/mcp_server_resource)-
shaped resource under that key -- built from this component's own `definitions_module`/`allow`/
`allow_assets`/`allow_jobs`/`default_tags` settings. `DAGSTER_HOME` is read from the environment
at **run time** (every time a consumer resolves this resource and calls its `.model_dump()`, not
once at component load) -- so it reflects whatever `DAGSTER_HOME` actually is when a run executes,
not a value frozen at code-location-load time. MCP's stdio client does **not** inherit the parent's
full environment (only a small Dagster-unaware safe-list), so `DAGSTER_HOME` still has to be
forwarded explicitly; if it's unset when the resource is actually resolved, the spawned server
process's own `main()` raises a clear `SystemExit` the moment it starts (the same way it always
has for a bare `python -m` invocation) rather than the component failing to build.

```yaml
type: dagster_community_components.DagsterWorkflowToolsComponent
attributes:
  definitions_module: my_project.definitions
  allow_assets: [orders, orders_summary]
  mcp_server_resource_key: dagster_workflow_mcp
```

Then, in any `mcp_servers`-aware component (`mcp_tool_call`, `mcp_tool_picker`, the agent
components, `agentic_pipeline`'s `tool_use_loop`/`mcp_call`):

```yaml
mcp_servers:
  - server_resource_key: dagster_workflow_mcp
```

No inline `command`/`env` to hand-write or keep in sync. Leave `mcp_server_resource_key` unset to
skip this entirely -- the existing `resource_key`-only, in-process `context.resources.
dagster_workflow_tools.call(...)` path is unchanged either way, and the two can coexist (the
component can register both resources at once).

## As a Dagster resource

```yaml
type: dagster_community_components.DagsterWorkflowToolsComponent
attributes:
  definitions_module: my_project.definitions
  allow_assets: [orders, orders_summary]
  default_tags: {team: support}
```
`context.resources.dagster_workflow_tools.call("materialize_assets", {"asset_keys": ["orders"]})`.

## How it actually triggers a run (verified, not guessed)

`dagster.execute_job` -- dagster's own documented out-of-process execution entrypoint -- not
`instance.submit_run`, which needs a live `BaseWorkspaceRequestContext` this script does not have
(confirmed via that method's own signature). `execute_job` needs a `ReconstructableJob`, built via
`dagster._core.definitions.reconstruct.build_reconstructable_job` pointing back at this file's own
`_reconstruct_job` function (the string-based reconstruction strategy, since the job to run is only
known at call time). For `materialize_assets`, the ad-hoc job is built with
`dagster.define_asset_job(name=..., selection=...).resolve(asset_graph=...)` -- **not**
`Definitions.resolve_implicit_job_def_def_for_assets`, which returns a job literally named
`__ASSET_JOB` (dagster's reserved name for the implicit per-repo asset job) and raises
`DagsterInvalidDefinitionError` when wrapped standalone like this (confirmed with a real run).
Confirmed working end-to-end against a real two-asset `Definitions` and a real on-disk
`DagsterInstance`: real `ASSET_MATERIALIZATION` events, real `RUN_SUCCESS`.

## Known limitation

`execute_job` blocks synchronously for the whole run's duration -- there is no built-in timeout, so
a long-running job holds the tool call open for just as long. An async "submit and poll" mode would
need `instance.submit_run`, which needs a live workspace connection this script does not have; that
is a real architectural gap, not something this component papers over. If your workflow needs
fire-and-forget triggering with separate status polling, you'll need a different mechanism (e.g. a
real Dagster+ GraphQL `launchRun` call against a running deployment) -- not implemented here.

## Safety

- `allow` (tool names), `allow_assets`, `allow_jobs` are hard allow-lists -- anything outside them
  is refused with a clear error, never silently run.
- Every triggered run is tagged `dagster-community/workflow-tool=true` plus `default_tags` plus
  caller-supplied `tags`, for audit/attribution.
- Every call writes one audit log line (`dagster_workflow_tools call tool=... args=...`, on logger
  `dagster_workflow_tools.audit`) -- never results. Secrets in error text are redacted the same way
  `dagster_instance_tools` redacts log lines (heuristic -- treat it as a safety net, not a
  guarantee).
- This component can trigger real runs against a real instance. Review `allow_assets`/`allow_jobs`
  carefully before wiring it into an agent loop you don't fully trust.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `definitions_module` | `str` | Required 'pkg.module[:attr]' that yields the Definitions this tool is allowed to materialize/run (attr defaults to `defs`). Unlike dagster_instance_tools (read-only), a write tool must know its own scope -- there is no '… _(full docs in schema.json + component README)_ |

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | `"dagster_workflow_tools"` | Resource key the toolset is registered under |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `allow` | `List[str]` | — | `f"Hard allow-list of tools (default: all). Valid: {', '.join(ALL_TOOLS)}."` |
| `allow_assets` | `List[str]` | — | Hard allow-list of asset keys materialize_assets may target (default: any asset in definitions_module). Recommended in production -- an agent should materialize specific, intended assets, not anything in the repo. |
| `allow_jobs` | `List[str]` | — | Hard allow-list of job names run_job may target (default: any job in definitions_module). |
| `default_tags` | `Dict[str, str]` | — | Tags merged onto every triggered run, alongside dagster-community/workflow-tool=true (e.g. {'triggered_by': 'support-agent'}) -- for run-tag attribution/audit. |
| `mcp_server_resource_key` | `str` | — | If set, ALSO registers a stdio MCP-server-shaped resource (same name/type/command/env shape as MCPServerResource) under this key, built from this component's own settings -- python -m this module, with the DAGSTER_WORKFL… _(full docs in schema.json + component README)_ |

[//]: # (FIELDS:END)
