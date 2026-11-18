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
| `launch_job(job_name, run_config?, tags?)` | Launches a named job via the GraphQL `launchRun` mutation and returns `{success, run_id, job_name, url}` **immediately** -- does not wait for the run to finish. Requires the requester (never a tool argument -- see below) to be a listed owner of the job. See "launch_job" below. |

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

MCP's stdio client doesn't inherit anything *else* from the current environment either, though --
if your Dagster instance's own config needs more than `DAGSTER_HOME` to resolve (e.g. a **Dagster+
agent**, which commonly also needs `DAGSTER_CLOUD_AGENT_LABEL` and other `DAGSTER_CLOUD_*` vars),
list those names in `mcp_extra_env_vars` too -- otherwise the spawned server crashes at startup
trying to resolve its instance. Their *current values* are copied in fresh at the same run time as
`DAGSTER_HOME`, same mechanism.

```yaml
type: dagster_community_components.DagsterWorkflowToolsComponent
attributes:
  definitions_module: my_project.definitions
  allow_assets: [orders, orders_summary]
  mcp_server_resource_key: dagster_workflow_mcp
  mcp_extra_env_vars: [DAGSTER_CLOUD_AGENT_LABEL]
```

**Worth deciding up front for a Dagster+ code location:** which instance this server should
actually launch runs through. A Dagster+ agent's own `DAGSTER_HOME` is its own agent process's
home directory -- not necessarily a sensible target for runs an agent tool triggers. (Confirmed
working end-to-end against a plain, non-Dagster+ `DAGSTER_HOME`: the server started and correctly
listed `materialize_assets`/`run_job`.)

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

## Known limitation (`materialize_assets` / `run_job`)

`execute_job` blocks synchronously for the whole run's duration -- there is no built-in timeout, so
a long-running job holds the tool call open for just as long. This isn't "fixed" by making these
two tools non-blocking too: `instance.submit_run` (the obvious async alternative) needs a live
`BaseWorkspaceRequestContext` this bare script does not have, confirmed via that method's own
signature -- a real architectural gap, not something this component papers over. If your workflow
needs fire-and-forget triggering, use `launch_job` instead (below) -- a separate tool with its own
config and its own, stricter authorization, not a mode switch on these two.

## `launch_job` (non-blocking, for chat agents)

Unlike `materialize_assets`/`run_job`, `launch_job` returns `{success, run_id, job_name, url}`
**immediately** via the real GraphQL `launchRun` mutation (`dagster-graphql`'s
`GrapheneLaunchRunMutation` -- confirmed directly against the installed package's own schema, the
same mutation in OSS and Dagster+) instead of `execute_job`. This is the tool a chat agent
(`slack_agent_request_sensor`, `anthropic_agent`/`openai_agent`/`litellm_agent`) should use to
launch something without holding the conversation open for the run's whole duration.

**Setup** -- three fields, all required to enable it (leave any unset and `launch_job` refuses
clearly; `materialize_assets`/`run_job` are unaffected either way):

```yaml
type: dagster_community_components.DagsterWorkflowToolsComponent
attributes:
  definitions_module: my_project.definitions
  graphql_url: http://localhost:3000/graphql        # or your Dagster+ deployment's GraphQL URL
  repository_location_name: my_project               # the code location's own name
  # repository_name: "__repository__"                # default -- only override for a named @repository
  graphql_api_token_env_var: DAGSTER_CLOUD_API_TOKEN  # Dagster+ only; omit for a local OSS webserver
```

**Authorization happens in code, never in the prompt.** `launch_job` needs a `requester` --
the identity of whoever asked -- and refuses outright if none is set (it will NOT silently allow
an anonymous launch). The requester must be a listed owner of the job:

- **Asset job**: owners are read **live** from the union of its own assets' `owners` catalog
  metadata (the same field every asset in this repo already declares) -- nothing to configure here.
- **Non-asset job** (or one you want to authorize separately from its assets): add it to
  `job_owners: {job_name: [owners]}`. Same email / `team:<name>` convention as every `owners:`
  field in this repo -- `team:` entries never authorize (no group-membership check; a real
  requester identity is an email or platform user id, which can never equal that literal string).
- A job with **neither** source fails closed -- `launch_job` refuses it outright, there is no
  implicit "anyone can launch this" default.

**Getting the real requester in**: `requester` is set once per spawned MCP server process via the
`DAGSTER_WORKFLOW_TOOLS_REQUESTER` env var -- never a tool argument the model could set itself.
The calling agent component's own `{tag.<key>}` substitution (see `anthropic_agent`/`openai_agent`/
`litellm_agent`'s README) resolves it from a real Dagster run tag right before spawning the
subprocess, so the identity always traces back to whatever set that tag (e.g.
`slack_agent_request_sensor` setting `slack/user` to the message sender's resolved email).

- **Inline `mcp_servers` entry** (not sharing this component's `mcp_server_resource_key`): set the
  placeholder directly in the agent's own YAML:
  ```yaml
  mcp_servers:
    - name: workflow
      type: stdio
      command: [python, -m, dagster_community_components.assets.ai.dagster_workflow_tools.component]
      env:
        DAGSTER_WORKFLOW_TOOLS_REQUESTER: "{tag.slack/user}"
  ```
- **Shared `mcp_server_resource_key`**: set `requester_template` on `DagsterWorkflowToolsComponent`
  itself to the same `"{tag.<key>}"` string -- this component passes it through verbatim (it never
  resolves `{tag.*}` itself); the calling agent's substitution still does the real work.

## What `launch_job` does NOT do

- Accept a `partition_key` the way `materialize_assets`/`run_job` do -- out of scope for v1, noted
  rather than silently dropped.
- Resolve a requester's *group* membership for a `team:`-prefixed owner -- see above.
- Fall back to any default identity when `requester` is unset -- it refuses instead.

## Listing its tools (`inspection_asset_name`)

Set `inspection_asset_name` to ALSO emit a declare-only asset (nothing to materialize -- the tool set is fixed
Python code, not a configurable server) reporting `tool_names` + `tool_descriptions` as metadata, so a UI can show
them without spawning the MCP server to ask. `allowed_tools` matches against `<mcp_servers: entry name>__<tool>`.

## Safety

- `allow` (tool names), `allow_assets`, `allow_jobs` are hard allow-lists -- anything outside them
  is refused with a clear error, never silently run.
- `launch_job` additionally requires a per-requester owners check on top of `allow_jobs` -- see
  "launch_job" above. This is the one tool here whose authorization depends on WHO is calling, not
  just WHAT is being called.
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
| `mcp_extra_env_vars` | `List[str]` | — | mcp_server_resource_key only. Extra env var NAMES to copy (their current value, read fresh every time a consumer resolves the resource -- same run-time timing as DAGSTER_HOME) from this process into the spawned MCP serve… _(full docs in schema.json + component README)_ |
| `inspection_asset_name` | `str` | — | If set, ALSO emits a declare-only asset by this name with the allowed tool names + descriptions as metadata (tool_names, tool_descriptions, tool_count) -- the toolset is fixed Python code, not a configurable server, so t… _(full docs in schema.json + component README)_ |
| `graphql_url` | `str` | — | GraphQL endpoint for launch_job (e.g. 'http://localhost:3000/graphql' for a local OSS webserver, or your Dagster+ deployment's GraphQL URL). Required to enable launch_job -- there is no implicit default, since a Dagster+… _(full docs in schema.json + component README)_ |
| `graphql_api_token_env_var` | `str` | — | Env var holding a Dagster+ user/API token, sent as the 'Dagster-Cloud-Api-Token' header on the launch_job GraphQL request. Leave unset for a local OSS webserver with no auth configured. |
| `repository_location_name` | `str` | — | Code location name launch_job's GraphQL selector targets -- required to enable launch_job (the name this code location is loaded under: dagster_cloud.yaml's 'location_name', or the module/package name for a plain OSS workspace.yaml entry). |
| `repository_name` | `str` | `"__repository__"` | Repository name within that code location, for launch_job's GraphQL selector. '__repository__' (the default) is Dagster's own name for the implicit single repository every plain `Definitions` object becomes (verified dir… _(full docs in schema.json + component README)_ |
| `job_owners` | `Dict[str, List[str]]` | — | {job_name: [owners]} -- launch_job's authorization source for a job with no asset metadata to check (a non-asset job, or one you want to authorize separately from its assets' own owners). An asset job's owners are read L… _(full docs in schema.json + component README)_ |
| `requester_template` | `str` | — | mcp_server_resource_key only. A template string -- typically '{tag.<key>}', e.g. '{tag.slack/user}' -- passed through VERBATIM as this spawned server's DAGSTER_WORKFLOW_TOOLS_REQUESTER env value. This component never res… _(full docs in schema.json + component README)_ |

[//]: # (FIELDS:END)
