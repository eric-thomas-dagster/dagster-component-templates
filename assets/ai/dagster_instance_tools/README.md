# dagster_instance_tools

"Ask the pipeline": read-only tools over the Dagster instance so an agent can answer *why is this asset stale / failing?* with real data.

| Tool | Answers |
|---|---|
| `list_recent_failures(hours, limit)` | failed runs: failed step keys, innermost error class + first message line, asset keys |
| `get_run_summary(run_id)` | status, timings, steps, failed-step error excerpt (redacted, truncated), tags |
| `get_run_logs(run_id, level, max_lines)` | tail of event-log messages, secrets redacted |
| `asset_status(asset_key)` | last materialization (time/run/data+code version), later failed run, partition counts, latest check results |
| `asset_lineage(asset_key, depth, direction)` | upstream/downstream assets |
| `stale_assets(limit, max_age_hours)` | stale assets (data-version staleness, or age/failed-run based) |
| `receipt_stats(asset_key, window_hours)` | optional: how answers built from the asset turned out (ContextReceiptStore) |

Every result is a compact dict; failures come back as `{"error": "..."}` (bad run id, unknown tool, disabled tool), never as an exception. `asset_key` is `a/b/c` (path joined by `/`).

## Wiring it into agentic_pipeline

`tool_use_loop`, `mcp_call` and `agent_call` get tools **only from MCP servers** (stdio / http / sse / fastmcp); there is no plain-callable tool mechanism. So `component.py` is itself a stdio MCP server (`pip install mcp`). It finds the instance through `DAGSTER_HOME`.

```yaml
- id: diagnose
  op: tool_use_loop
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  prompt_template: "Why is the asset `orders` stale or failing? Check status, the failing run's logs, and upstream lineage. Answer in 3 sentences."
  mcp_servers:
    - name: dagster
      type: stdio
      command: [python, /abs/path/to/assets/ai/dagster_instance_tools/component.py]
      env:
        DAGSTER_HOME: /abs/path/to/dagster_home
        DAGSTER_INSTANCE_TOOLS_ALLOW: "list_recent_failures,get_run_summary,get_run_logs,asset_status,asset_lineage"
        DAGSTER_INSTANCE_TOOLS_DEFINITIONS_MODULE: my_project.definitions   # optional, see below
```

Tools are namespaced by the loop as `dagster__asset_status`, etc. Server env vars: `DAGSTER_HOME` (required), `DAGSTER_INSTANCE_TOOLS_ALLOW` (csv), `..._MAX_CHARS` (default 8000), `..._DEFINITIONS_MODULE` (`pkg.mod[:attr]`), `..._RECEIPT_STORE` (`pkg.mod:factory`, a zero-arg callable returning a ContextReceiptStore; enables `receipt_stats`). The server only exposes the allowed tools. The process must use the same Python env as your project if you set a definitions module.

## As a Dagster resource

```yaml
type: dagster_community_components.DagsterInstanceToolsComponent
attributes: {allow: [asset_status, get_run_summary], definitions_module: my_project.definitions}
```
`context.resources.dagster_instance_tools.call("asset_status", {"asset_key": "orders"})`. With `store_resource_key` set, the named resource (a ContextReceiptStore) powers `receipt_stats`.

## What needs the code location (`definitions_module`)

The instance (event log) alone cannot know the asset graph or partition set. Without `definitions_module`:
- `asset_lineage`: **upstream only**, from materialization data-provenance (assets materialized with tracked inputs); `downstream` returns a clear error.
- `asset_status.partitions`: materialized count + sample only; total / missing / failed are `null`.
- `stale_assets`: age-based (older than `max_age_hours`, or a failed run after the last materialization) instead of data-version staleness.
- check results: discovered from the asset's last materialization run and recent check evaluations.

With it: full graph lineage, partition total/failed/missing, and Dagster's own data-version staleness (uses a private `dagster._core` resolver; falls back to age-based if unavailable).

## Safety

- Read-only: only instance read APIs are called; nothing launches, terminates, wipes or reports.
- `redact_text`: masks `Authorization` headers / `Bearer` tokens, `key=value` pairs with secret-looking keys (token, secret, password, api_key, ...), URL passwords, common credential shapes (sk-, ghp_, xox*, AKIA, JWT, private keys) and long base64/hex blobs. Applied to log lines and error text, and again to every result before it is returned. It is heuristic: treat it as a safety net, not a guarantee.
- `allow` is a hard list; `max_result_chars` bounds every result; each call logs `dagster_instance_tools call tool=... args=...` on logger `dagster_instance_tools.audit` (never results).

## Limits
- Logs are event-log messages only; compute stdout/stderr captures are not included.
- Built and tested against dagster 1.13 on an OSS `DagsterInstance` (local sqlite). Not verified against Dagster+ (it is the same instance API, but unconfirmed) or a remote Postgres instance.
- The new-style `FreshnessPolicy` state is surfaced in `asset_status.freshness` best-effort (untested; the freshness daemon populates it).
