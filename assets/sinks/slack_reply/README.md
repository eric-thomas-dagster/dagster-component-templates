# slack_reply

Posts a chat agent's answer back into the Slack thread it's answering -- deterministically. A
model is never trusted to remember to call a "post" tool: if it forgets, or the run fails
outright, the human would otherwise see nothing. This component emits **two** things to prevent
that: a plain downstream asset for the success path, and a run-status sensor for the failure
path, so a crashed run still gets a reply instead of silence.

## Type

```
dagster_community_components.SlackReplyComponent
```

## The two halves

1. **`{asset_name}`** -- a plain asset, downstream of `upstream_asset_key` (the agent asset).
   Reads the answer from the upstream output (`final_answer`, the agent components' own
   documented shape, by default -- override with `answer_field`), posts it to the thread named
   by the run's own `slack/channel` + `slack/thread_ts` tags (set by
   [`slack_agent_request_sensor`](../../../sensors/slack_agent_request_sensor)), splits or
   truncates a long answer, and raises `dg.Failure` loudly if those tags are missing -- this
   asset should only ever run as part of that chain.
2. **`{asset_name}_failure_watcher`** -- a `run_status_sensor` (not gated to this asset
   specifically) that fires for **any** failed run in the deployment, checks whether it carries
   the same `slack/channel`/`slack/thread_ts` tags, and if so posts a short error reply instead.
   This is why it isn't just a plain downstream asset with error handling: if the **agent asset
   itself** fails, Dagster skips everything downstream of it in that run -- a plain dependency
   would never even execute to report the failure. A run-status sensor fires independently of
   the run's own step graph.

## Closing the loop: receipts and ratings (`receipts_db_path`, `watch_reactions`)

The agent's answer may have been built from one or more `search_knowledge`/`search` calls
against a `knowledge_search_server`/`hybrid_knowledge_search_server`, each of which (when
*that* server's own `receipts_db_path` is set) returns a `receipt_id`. Today, closing the loop
("the reader thought this answer was good/bad") required the agent to end its reply with
`ref: <receipt_id>` and have that parsed back out of the thread -- fragile, and visible to
users. This component gives that a real mechanism instead:

- **`receipts_db_path`**: point it at the SAME SQLite file the agent's search server(s) use.
  After posting, `{asset_name}` looks up every receipt `context.run.run_id` produced (via that
  table's `dagster_run_id` column -- make sure the search server's own `run_id_template` is set,
  the default) and links them to the posted message(s), in a small table
  (`context_receipt_message_links`) this component and `slack_chat_tools` both read. This is a
  **small table in the receipts store**, not Slack message metadata -- no new Slack API
  plumbing, and `slack_chat_tools.get_thread()` can return the linked receipt ids for any bot
  message without parsing text (see that component's own README).
- **`watch_reactions`** (needs `receipts_db_path` set): also emits a third thing,
  **`{asset_name}_rating_watcher`** -- a sensor that polls reactions on every message this
  asset has linked to a receipt, and records a `thumbs_up`/`thumbs_down` outcome (via the same
  `record_feedback`-shaped outcome event `knowledge_search_server` itself writes) against each
  linked `receipt_id`. The reactor's resolved email becomes the outcome's `outcome_ref` --
  reacting again with the OPPOSITE emoji supersedes the earlier rating when scoring (the same
  "changing your mind isn't double-counted" behavior `record_feedback`'s own `user_id` already
  has), and the same reaction is never re-recorded tick after tick. Needs the `reactions:read`
  bot scope in addition to `chat:write`.
- This reuses the `reactions.get` + poll shape `slack_approval_gate` already established for
  watching reactions on a specific message -- but keyed by a bounded cursor
  (`rating_lookback_messages`) rather than a per-partition sidecar file, since `{asset_name}`
  isn't partitioned per message the way an approval gate is.

```yaml
  receipts_db_path: /tmp/receipts.sqlite3
  watch_reactions: true
```

## Delivering generated files (`report_files_dir`)

A model can forget to post a file it made, the same way it can forget to post an answer at all
-- so this is handled the same deterministic way. Point `report_files_dir` at the SAME
`files_dir` a [`report_tools_server`](../../ai/report_tools_server) the agent calls is
configured with, and after posting the answer, `{asset_name}` reads that run's manifest (every
`make_chart`/`make_pdf`/`make_docx`/`make_xlsx` file the run produced) and uploads each one into
the same thread with `files_upload_v2` -- needs the `files:write` bot scope. A file may contain
data the agent read while answering, so it's visible to whoever can already see the channel, same
as the answer text itself. Posted filenames land in this asset's own materialization metadata
(`files_posted`); an upload failure here is logged and skipped, never a reason to fail an
already-posted answer.

```yaml
  report_files_dir: /var/dagster/report_tools_files
```

## Showing progress (`update_reaction_on_completion`, `placeholder_ts`)

Without this, `slack_agent_request_sensor` adds its acknowledgement reaction (👀 by default)
when it picks a message up and nothing visibly changes after that -- so a normal 20-30s answer
looks stuck. Two independent fixes, on by default:

- **`update_reaction_on_completion`** (default true): once `{asset_name}` posts the answer, it
  removes `ack_reaction` (must match the sensor's own `acknowledgement_reaction`) and adds
  `done_reaction` to the ORIGINAL triggering message (the `slack/message_ts` tag the sensor
  sets) -- `{asset_name}_failure_watcher` adds `error_reaction` instead on a crash. Needs
  `reactions:write`.
- **`placeholder_ts`**: if `slack_agent_request_sensor`'s own `placeholder_text` is set, the
  run carries a `slack/placeholder_ts` tag -- `{asset_name}` then edits that message in place
  (`chat.update`) with the final answer instead of posting a new one (falling back to a new
  post if the edit fails), and `{asset_name}_failure_watcher` edits it with the error. Nothing
  to set here; this is driven entirely by that tag's presence.

```yaml
  ack_reaction: eyes
  done_reaction: white_check_mark
  error_reaction: warning
```

## Minimal example

```yaml
type: dagster_community_components.SlackReplyComponent
attributes:
  asset_name: chat_reply
  upstream_asset_key: chat_agent
  webserver_base_url: http://localhost:3000   # powers the "View run" link; omit to skip it
```

## What this does NOT do

- Decide *what* to say -- that's the agent's job; this only posts what it already decided.
- Format Slack mrkdwn beyond what the agent's own text already contains -- no markdown
  conversion, tables, or block-kit layout in v1.
- Attribute citations for knowledge-base answers specially -- out of scope for this pass; the
  agent's own answer text carries whatever citations it chose to include.
- Rate-limit or dedupe repeated failure notifications for the same thread -- every failed run
  carrying the tags gets its own reply.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Output asset name. |
| `upstream_asset_key` | `Union[str, List[str]]` | The chat agent asset to depend on and read the answer from (slash-delimited). Exactly one -- a list of more than one is a validation error. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `deps` | `List[str]` | — | Lineage-only upstream asset keys, beyond upstream_asset_key -- no value loaded. |
| `group_name` | `str` | — | Dagster asset group name. |
| `description` | `str` | — | Asset description. |
| `owners` | `List[str]` | — | Asset owners -- team names ('team:analytics') or email addresses. |
| `asset_tags` | `Union[Dict[str, Any], List[str]]` | — | Additional key-value tags applied to the asset in the Dagster catalog. |
| `kinds` | `List[str]` | — | Asset kinds for the catalog. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `slack_bot_token_env_var` | `str` | `"SLACK_BOT_TOKEN"` | Env var holding the bot's 'xoxb-...' token. Needs chat:write. |
| `answer_field` | `str` | — | Which key of the upstream asset's output dict holds the answer text. Defaults to 'final_answer' (the agent components' own documented output shape) when the upstream value is a dict; used verbatim (str()'d) otherwise. |
| `convert_markdown` | `bool` | `true` | Convert common markdown a model writes into Slack's mrkdwn dialect before posting: **x**/__x__ -> *x*, [text](url) -> <url\|text>, '# Heading' -> *Heading*, '* item'/'- item' -> bullet list items, and a markdown table ->… _(full docs in schema.json + component README)_ |
| `max_chars_per_message` | `int` | `3000` | Split or truncate (see on_long_answer) an answer longer than this many characters. |
| `on_long_answer` | `Literal['split', 'truncate']` | `"split"` | 'split' (default): post as multiple sequential thread replies, each <= max_chars_per_message. 'truncate': post one reply, cut short with an ellipsis. |
| `include_run_link` | `bool` | `true` | Append a link to the Dagster run (requires webserver_base_url) to the last reply posted. |
| `run_link_label` | `str` | `"View run"` | Link text for include_run_link. |
| `webserver_base_url` | `str` | — | Base URL of the Dagster webserver (e.g. 'http://localhost:3000' or your Dagster+ deployment URL) -- used to build the run link. Leave unset to omit the link entirely. |
| `monitor_failures` | `bool` | `true` | Also emit {asset_name}_failure_watcher, a run-status sensor that posts a short error reply for ANY failed run carrying slack/channel + slack/thread_ts tags -- so a crashed run is never silent in the channel. Set false to… _(full docs in schema.json + component README)_ |
| `error_message_template` | `str` | `"⚠️ Something went wrong answering this: {error}"` | Template for the failure-path reply. Placeholder: {error}. |
| `update_reaction_on_completion` | `bool` | `true` | On success, remove ack_reaction and add done_reaction to the ORIGINAL triggering message (the slack/message_ts tag slack_agent_request_sensor sets) -- so a 20-30s answer doesn't just look stuck at its acknowledgement rea… _(full docs in schema.json + component README)_ |
| `ack_reaction` | `str` | `"eyes"` | The reaction slack_agent_request_sensor posted on pickup (must match its own acknowledgement_reaction) -- removed on completion. Only used when update_reaction_on_completion is true. |
| `done_reaction` | `str` | `"white_check_mark"` | Reaction added to the original message once the reply posts successfully. Only used when update_reaction_on_completion is true. |
| `error_reaction` | `str` | `"warning"` | Reaction added to the original message when the run fails (added by {asset_name}_failure_watcher). Only used when update_reaction_on_completion is true. |
| `receipts_db_path` | `str` | — | SQLite file path -- the SAME receipts_db_path a knowledge_search_server/hybrid_knowledge_search_server the agent calls is configured with (and which must also have run_id_template set, the default, so its receipts carry… _(full docs in schema.json + component README)_ |
| `report_files_dir` | `str` | — | Local directory root -- the SAME files_dir a report_tools_server this agent calls is configured with. When set, after posting the answer, this asset reads this run's manifest (written by report_tools_server's make_chart/… _(full docs in schema.json + component README)_ |
| `watch_reactions` | `bool` | `false` | Also emit {asset_name}_rating_watcher, a sensor that polls reactions on every Slack message this asset has linked to a receipt (via receipts_db_path, which must also be set) and records a thumbs_up/thumbs_down outcome ag… _(full docs in schema.json + component README)_ |
| `positive_reaction` | `str` | `"+1"` | Slack reaction name (as reactions.get returns it, e.g. '+1' for 👍) that records a thumbs_up outcome. Only used when watch_reactions is true. |
| `negative_reaction` | `str` | `"-1"` | Slack reaction name (e.g. '-1' for 👎) that records a thumbs_down outcome. Only used when watch_reactions is true. |
| `rating_poll_interval_seconds` | `int` | `30` | Minimum seconds between rating_watcher ticks. Only used when watch_reactions is true. |
| `rating_lookback_messages` | `int` | `200` | Most-recently-linked (channel, message_ts) pairs to keep polling for reactions, and the cursor's bound. Only used when watch_reactions is true. |
| `resolve_rater_email` | `bool` | `true` | Resolve the reacting user's Slack user id to an email address (needs users:read.email) before using it as the outcome's outcome_ref. When false, the raw Slack user id is used instead. Only used when watch_reactions is true. |

[//]: # (FIELDS:END)

## Requirements

```
dagster
slack_sdk>=3.0
```
