# slack_chat_tools

A chat-triggered agent run (see [`slack_agent_request_sensor`](../../../sensors/slack_agent_request_sensor))
knows almost nothing on its own -- its prompt only ever carries `{tag.<key>}` identifiers
(channel, thread, requester), never the actual message text. `get_thread()` is how the agent
actually reads the conversation: it fetches the live Slack thread, fresh, every time it's called.
No database, no caching -- there's nothing here to go stale or leak across runs.

## Type

```
dagster_community_components.SlackChatToolsComponent
```

| Tool | Does |
|---|---|
| `get_thread()` | The last `max_messages` messages in the thread this run is answering (or the single message, if it isn't in a thread), oldest first, as `{user, text, ts}` rows. **No arguments** -- it always reads THIS run's own channel/thread; there is no channel/thread_ts tool argument the model could use to read a different conversation. |

## Why this always reads the right conversation without the model telling it to

`get_thread` takes no parameters on purpose. The channel and thread identifiers come from this
server process's own environment -- `SLACK_CHAT_TOOLS_CHANNEL` / `SLACK_CHAT_TOOLS_THREAD_TS` --
set via a `{tag.<key>}` placeholder (default: `{tag.slack/channel}` / `{tag.slack/thread_ts}`)
resolved by the **calling agent's own** generic tag substitution (see
`anthropic_agent`/`openai_agent`/`litellm_agent`'s README) right before it spawns this subprocess.
Same trusted, server-side mechanism `dagster_workflow_tools`' `launch_job` uses for its requester
identity -- the model never supplies the channel/thread itself, so it can't be tricked (by a
cleverly-worded message, or prompt injection in the thread history) into reading a different
conversation.

## Setup

```yaml
type: dagster_community_components.SlackChatToolsComponent
attributes:
  mcp_server_resource_key: slack_chat_mcp
  slack_bot_token_env_var: SLACK_BOT_TOKEN   # needs channels:history (or groups:history)
  max_messages: 20
```

Then, in the agent's own `mcp_servers`:

```yaml
mcp_servers:
  - server_resource_key: slack_chat_mcp
```

`channel_template`/`thread_ts_template` default to `"{tag.slack/channel}"` /
`"{tag.slack/thread_ts}"` and are passed through **verbatim** as this spawned server's env --
this component never resolves the placeholder itself, the calling agent's substitution does. Only
override them if your sensor names the identifier tags something else.

For an inline (non-shared) `mcp_servers` entry instead of `mcp_server_resource_key`, just set the
env values directly in the agent's own YAML:

```yaml
mcp_servers:
  - name: chat
    type: stdio
    command: [python, assets/ai/slack_chat_tools/component.py]
    env:
      SLACK_CHAT_TOOLS_CHANNEL: "{tag.slack/channel}"
      SLACK_CHAT_TOOLS_THREAD_TS: "{tag.slack/thread_ts}"
```

## `conversations.replies` works for a solo message too

`slack_agent_request_sensor` always sets the `slack/thread_ts` tag to the real thread root if
there is one, else the triggering message's own `ts` -- and Slack's own `conversations.replies`
API returns just that one message when called on a `ts` that isn't actually part of a thread
(confirmed against Slack's own API reference). So this component never needs separate logic for
"is this a thread or a solo message" -- one call handles both.

## Seeing which receipts a past answer came from (`receipts_db_path`)

Set `receipts_db_path` to the SAME SQLite file a [`slack_reply`](../../sinks/slack_reply)
instance (and the agent's `knowledge_search_server`/`hybrid_knowledge_search_server`) are
configured with, and `get_thread()` attaches a `receipt_ids` list to any message in the thread
that was posted by the bot itself (`bot_id` present) -- looked up from the small
`context_receipt_message_links` table `slack_reply` writes after posting, never from parsing
the message text. A user message never gets this field. This is what lets the model (or a
human reading the raw tool output) connect a past reply back to the exact searches it was built
from -- see `slack_reply`'s own README ("Closing the loop") for the full design.

## Posting a file mid-conversation (`report_files_dir`, `upload_files`)

`get_thread()` never posts anything -- but when `report_files_dir` is set (to the SAME
`files_dir` a [`report_tools_server`](../report_tools_server) the agent calls is configured
with), this component ALSO registers an `upload_files(file_ids)` tool: the agent passes one or
more `file_id`s a `make_chart`/`make_pdf`/`make_docx`/`make_xlsx` call already returned, and
this posts each one into the SAME thread `get_thread()` reads -- never a channel/thread_ts
argument the model could redirect. This is for a file the person wants to see mid-conversation;
[`slack_reply`](../../sinks/slack_reply) already posts every file a run generated at the end of
the run regardless, so `upload_files` is optional, not the only way files get delivered. Needs
the `files:write` bot scope and `run_id_template` (left at its default) so this run's own files
can be found.

```yaml
  report_files_dir: /var/dagster/report_tools_files
```

## A canvas instead of a PDF (`enable_canvas_tool`, `create_canvas`)

Set `enable_canvas_tool` to also register `create_canvas(title, markdown)` -- for a long
document the person will keep and edit, a Slack canvas is often a better fit than a PDF.
Creates the canvas from markdown, tabs it into THIS channel (`canvases.create` with
`channel_id` set), and returns `{canvas_id, link}` -- the model includes the link in its own
answer text, which `slack_reply` posts normally; there's no separate upload step the way a
`report_tools_server` file needs. The link format
(`https://<workspace>.slack.com/docs/<team_id>/<canvas_id>`) comes straight from Slack's own
canvas documentation, not a guess. Needs `canvases:write` -- a NEW scope beyond everything else
this component uses.

```yaml
  enable_canvas_tool: true
```

## What this does NOT do

- Accept a channel/thread override from the model for `get_thread()` -- see above; this is
  deliberate, not a gap. `upload_files` has the same constraint.
- Cache or write anything across calls -- every `get_thread()` call hits the real Slack API
  fresh; `receipts_db_path`/`report_files_dir` (when set) are read-only lookups against tables
  `slack_reply`/`report_tools_server` write, never written here.
- Decide *what* chart/document to make -- see [`report_tools_server`](../report_tools_server).
- Rate ANSWERS -- that's `slack_reply`'s `watch_reactions`, not this component.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | `"slack_chat_tools"` | Resource key the toolset is registered under. |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `max_messages` | `int` | `20` | Most recent messages get_thread returns. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `slack_bot_token_env_var` | `str` | `"SLACK_BOT_TOKEN"` | Env var holding the bot's 'xoxb-...' token. Needs channels:history (or groups:history). |
| `channel_template` | `str` | `"{tag.slack/channel}"` | Template string for this spawned server's SLACK_CHAT_TOOLS_CHANNEL env value -- the calling agent's own {tag.<key>} substitution resolves it at run time (see README). Set to a literal channel id instead if every run shou… _(full docs in schema.json + component README)_ |
| `thread_ts_template` | `str` | `"{tag.slack/thread_ts}"` | Template string for this spawned server's SLACK_CHAT_TOOLS_THREAD_TS env value, same mechanism as channel_template. |
| `max_result_chars` | `int` | `8000` | Upper bound on the serialized size of any tool result. |
| `receipts_db_path` | `str` | — | SQLite file path -- the SAME receipts_db_path a slack_reply instance (and the knowledge_search_server/hybrid_knowledge_search_server the agent calls) are configured with. When set, get_thread() attaches receipt_ids to an… _(full docs in schema.json + component README)_ |
| `report_files_dir` | `str` | — | Local directory root -- the SAME files_dir a report_tools_server this agent calls is configured with. When set, ALSO registers an upload_files tool so the agent can post a generated chart/PDF/doc/spreadsheet (by file_id)… _(full docs in schema.json + component README)_ |
| `run_id_template` | `str` | `"{run_id}"` | Template for the spawned server's SLACK_CHAT_TOOLS_RUN_ID env var -- left as the literal string '{run_id}' here; the CALLING agent's own generic {run_id}/{tag.*} substitution resolves it to the real run id before the sub… _(full docs in schema.json + component README)_ |
| `enable_canvas_tool` | `bool` | `false` | Also registers create_canvas(title, markdown): creates a Slack canvas (better than a PDF for a long document people will keep and edit), tabs it into this channel, and returns its link for the agent to include in its own… _(full docs in schema.json + component README)_ |
| `mcp_server_resource_key` | `str` | — | If set, ALSO registers a stdio MCP-server-shaped resource (same name/type/command/env shape as MCPServerResource) under this key, built from this component's own settings. channel_template/thread_ts_template are passed t… _(full docs in schema.json + component README)_ |

[//]: # (FIELDS:END)

## Requirements

```
dagster
slack_sdk>=3.0
```
