# report_tools_server

Deterministic chart / PDF / Word / Excel file builders for a chat agent. Someone asks the Slack
(or Teams) bot to "chart failures over the last week" or "put this in a PDF", and gets the file
in the thread -- the model never writes plotting or document code, it fills in a schema and this
server renders it.

| Tool | Use it for |
|---|---|
| `make_chart(kind, title, series \| labels+values \| timestamps+bucket, ...)` | a PNG chart (line/bar/barh/area/scatter/pie) |
| `make_pdf(title, sections, ...)` | a PDF report |
| `make_docx(title, sections, ...)` | a Word document (same `sections` shape as make_pdf) |
| `make_xlsx(sheets)` | an Excel workbook |

No network, no credentials: every tool's output is a pure function of its own arguments.
Generated files are written under `files_dir/<dagster_run_id>/`, with a `manifest.json`
recording every file's id/name/media_type/path/size. `slack_reply` / `teams_reply` read that
manifest after the run and post every file into the thread (see those components' READMEs,
"Delivering generated files") -- the same deterministic pattern this repo already uses so a
model can never forget to post an answer.

## `make_chart`: three ways to give it data

Pass **exactly one** of:

- `series`: `[{name, x: [...], y: [...]}, ...]` -- general-purpose, any number of series.
- `labels` + `values`: one ranked/categorical series (also the shape for a pie).
- `timestamps` (+ `bucket`: `"hour"`, `"day"`, or `"week"`, optional `group_by`): hand it raw
  event times (e.g. every failed run's `created_at`) and it buckets and **zero-fills** the gaps
  itself, so a chart never has a hole where nothing happened. `group_by` (a parallel list, same
  length as `timestamps`) splits into one series per group -- e.g. one failure count line per
  `job_name`.

`kind: "auto"` (default) picks a line for a time series, a sorted-descending horizontal bar for
ranked categories, and a stacked bar for more than one series/group. `kind: "pie"` with more
than 5 slices falls back to a sorted bar and says so in `warnings` -- a pie with that many
slices is unreadable, not a judgment call worth asking the model to make. Bars get direct value
labels when there are 12 or fewer of them; axes are always labelled (`unit` is appended to
`y_label` in parentheses); colours come from the Okabe-Ito colour-blind-safe palette; no 3D, no
legend for a single series.

Bad input comes back as `{"error": "..."}` with a reason an agent can act on (e.g. `"series[1]
('failures') y has 7 values but x has 6"`), never an exception.

## `make_pdf` / `make_docx`: shared `sections` shape

```yaml
sections:
  - heading: "Last 7 days"
    text: "3 jobs failed more than once."
    bullets: ["orders_pipeline: 4 failures", "billing_sync: 2 failures"]
    table: {columns: ["job", "failures"], rows: [["orders_pipeline", 4], ["billing_sync", 2]]}
    chart_file_id: "a1b2c3d4e5f6"   # from an earlier make_chart call IN THE SAME RUN
```

`chart_file_id` embeds a chart made earlier in the same run -- this component looks it up in
that run's own manifest, so the id only ever has to travel through the model's own context
between tool calls, never a file path.

## Setup

```yaml
type: dagster_community_components.ReportToolsServerComponent
attributes:
  mcp_server_resource_key: report_tools_mcp
  files_dir: /var/dagster/report_tools_files
```

Then, in the agent's own `mcp_servers`:

```yaml
mcp_servers:
  - server_resource_key: report_tools_mcp
```

For an inline (non-shared) `mcp_servers` entry instead of `mcp_server_resource_key`, run this
file directly as a stdio server:

```yaml
mcp_servers:
  - name: report_tools
    type: stdio
    command: [python, -m, dagster_community_components.assets.ai.report_tools_server.component]
    env:
      REPORT_TOOLS_FILES_DIR: /var/dagster/report_tools_files
```

`run_id_template` (default `"{run_id}"`) is passed through **verbatim** as the spawned server's
`REPORT_TOOLS_RUN_ID` env value -- this component never resolves the placeholder itself, the
calling agent's own `{run_id}`/`{tag.*}` substitution does, right before it launches the
subprocess. This is what scopes a run's files (and its manifest) to that run alone.

## Caps and retention

`max_file_bytes` (default 5 MB) and `max_files_per_run` (default 20) bound a single run; both
come back as a clear tool error rather than a huge or runaway file. `file_retention_hours`
(default 24) deletes a run's files (and its manifest) that many hours after they were written --
swept opportunistically on each tool call, not a scheduled job, since this MCP server has no way
to be told "the run finished" on its own (it is typically a fresh subprocess per run already;
retention exists for the shared `files_dir` persisting across runs, not for cleanup within one).

## What this does NOT do

- Decide *what* to chart or write -- that's the agent's job; this only renders what it's told.
- Post anything anywhere -- see `slack_reply` / `teams_reply` for delivery into a chat thread.
- Create a Slack canvas, Adaptive Card, or any rich chat-native format -- PNG/PDF/DOCX/XLSX only.
- Fetch data itself -- pass it arguments from whatever tool already returned the numbers (e.g.
  the standard Dagster+ MCP server's `list_runs`/`get_*_metrics`).

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `files_dir` | `str` | Local directory root for generated files, one subfolder per dagster/run_id. Create it with write access for whichever process runs the chat agent. |

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | `"report_tools"` | Resource key the toolset is registered under |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `allow` | `List[str]` | — | `f"Hard allow-list of tools (default: all). Valid: {', '.join(ALL_TOOLS)}."` |
| `max_file_bytes` | `int` | `5000000` | Per-file size cap in bytes. A file over this is rejected, not truncated. |
| `max_files_per_run` | `int` | `20` | Max files a single run (dagster/run_id) may generate across all tools. |
| `file_retention_hours` | `float` | `24.0` | A run's generated files (and its manifest) are deleted this many hours after they were written -- swept opportunistically on each tool call, not a scheduled job. |
| `run_id_template` | `str` | `"{run_id}"` | Template for the REPORT_TOOLS_RUN_ID env var set on the stdio MCP server (mcp_server_resource_key only) -- left as the literal string '{run_id}' here; the CALLING agent's own generic {run_id}/{tag.*} substitution resolve… _(full docs in schema.json + component README)_ |
| `mcp_server_resource_key` | `str` | — | If set, ALSO registers a stdio MCP-server-shaped resource (same name/type/command/env shape as MCPServerResource) under this key, built from this component's own settings -- python -m this module, with REPORT_TOOLS_* env… _(full docs in schema.json + component README)_ |
| `mcp_extra_env_vars` | `List[str]` | — | mcp_server_resource_key only. Extra env var NAMES to copy (read fresh every time a consumer resolves the resource) from this process into the spawned MCP server's env. |

[//]: # (FIELDS:END)

## Requirements

```
dagster
pydantic
matplotlib
reportlab
python-docx
openpyxl
mcp>=1.30,<3
```
