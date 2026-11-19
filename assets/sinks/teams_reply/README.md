# teams_reply

Posts a chat agent's answer back into the Teams thread it's answering -- deterministically.
Teams analog of [`slack_reply`](../slack_reply) -- no shared code, but the same design: a
model is never trusted to remember to call a "post" tool: if it forgets, or the run fails
outright, the human would otherwise see nothing. This component emits **two** things to
prevent that: a plain downstream asset for the success path, and a run-status sensor for the
failure path, so a crashed run still gets a reply instead of silence.

## Type

```
dagster_community_components.TeamsReplyComponent
```

## The two halves

1. **`{asset_name}`** -- a plain asset, downstream of `upstream_asset_key` (the agent asset).
   Reads the answer from the upstream output (`final_answer`, the agent components' own
   documented shape, by default -- override with `answer_field`), posts it (as HTML --
   Teams message bodies aren't plain text) to the thread named by the run's own
   `teams/team_id` + `teams/channel_id` + `teams/thread_id` tags (set by
   [`teams_agent_request_sensor`](../../../sensors/teams_agent_request_sensor)), splits or
   truncates a long answer, and raises `dg.Failure` loudly if those tags are missing -- this
   asset should only ever run as part of that chain.
2. **`{asset_name}_failure_watcher`** -- a `run_status_sensor` (not gated to this asset
   specifically) that fires for **any** failed run in the deployment, checks whether it carries
   the same three tags, and if so posts a short error reply instead. This is why it isn't just
   a plain downstream asset with error handling: if the **agent asset itself** fails, Dagster
   skips everything downstream of it in that run -- a plain dependency would never even execute
   to report the failure. A run-status sensor fires independently of the run's own step graph.

## Closing the loop: receipts and ratings (`receipts_db_path`, `watch_reactions`)

Same design and the same answer to the "closing the loop" problem as `slack_reply`'s own
README -- read that for the full rationale. The Teams-specific wrinkle:

- **`receipts_db_path`**: point it at the SAME SQLite file the agent's search server(s) use.
  After posting, `{asset_name}` looks up every receipt `context.run.run_id` produced and links
  them to the posted reply/replies, in the same `context_receipt_message_links` table
  `slack_reply`/`slack_chat_tools` use (`platform="teams"`).
  **Addressing difference, not a copy-paste**: Slack's `reactions.get(channel, timestamp)`
  addresses any message with just a channel id + its own ts; Microsoft Graph has no such flat
  addressing -- reading or reacting to a specific reply needs its PARENT thread id too, and
  every Graph call needs a `team_id` the channel id alone doesn't carry. So this component (and
  `teams_chat_tools`) store `channel` as `"{team_id}|{channel_id}"` and `message_ts` as
  `"{thread_id}|{reply_id}"` -- composites, not literal ids -- in that same generic table.
- **`watch_reactions`** (needs `receipts_db_path` set): also emits a third thing,
  **`{asset_name}_rating_watcher`** -- a sensor that polls reactions on every reply this asset
  has linked to a receipt, and records a `thumbs_up`/`thumbs_down` outcome against each linked
  `receipt_id`. Unlike Slack (a separate `reactions.get` call per message), Microsoft Graph
  returns a message's `reactions[]` inline on the SAME `GET .../replies` call already used to
  read a thread -- confirmed against Microsoft's own Graph reference -- so this groups linked
  replies by thread and makes one replies-list call per thread, not one per reply. The
  reactor's resolved email becomes the outcome's `outcome_ref` -- reacting again with the
  OPPOSITE emoji supersedes the earlier rating when scoring, and the same reaction is never
  re-recorded tick after tick. Needs delegated `ChannelMessage.Send` (the same scope already
  provisioned for posting) -- reading reactions needs no separate scope at all.

```yaml
  receipts_db_path: /tmp/receipts.sqlite3
  watch_reactions: true
```

## Auth -- Microsoft Graph, delegated only

Same real constraint, and the same one-time setup, as `teams_run_request_sensor` -- see its
own README ("Auth"). `oauth_state_dir` can be the SAME directory `teams_agent_request_sensor`
uses; they share nothing in code, but refresh-token rotation state for the same Entra
app/account is safe to share.

## Delivering generated files (`report_files_dir`)

Same problem, same deterministic fix as `slack_reply`'s own "Delivering generated files" --
read that for the rationale. The Teams-specific wrinkle: unlike Slack, Microsoft Graph has no
inline file attachment on a channel message, so this asset instead uploads each file to the
channel's own **Files folder** (the SharePoint document library backing it) and posts a reply
linking to it.

Point `report_files_dir` at the SAME `files_dir` a
[`report_tools_server`](../../ai/report_tools_server) the agent calls is configured with. After
posting the answer, `{asset_name}` reads that run's manifest, resolves the channel's Files
folder once (`GET .../channels/{id}/filesFolder`, confirmed against Microsoft's own Graph
reference), and uploads each file with `PUT .../content` (Graph's "upload small file" call --
up to 250 MB, well above `report_tools_server`'s own 5 MB default cap), posting one reply per
file with a link to it (`webUrl`).

**New permission, not a reuse of ChannelMessage.Send**: this needs delegated
`Files.ReadWrite` (or `Files.ReadWrite.All`) -- a DIFFERENT scope than the `ChannelMessage.Send`
already provisioned for posting replies and reactions. Add it to the same Entra app
registration and re-consent before setting `report_files_dir`. Files may contain data the agent
read while answering, so the same people who can already see the channel's Files tab can see
them.

```yaml
  report_files_dir: /var/dagster/report_tools_files
```

## Showing progress (`update_reaction_on_completion`, `placeholder_id`)

Same two independent fixes as `slack_reply`'s own README section, both on by default -- read
that for the full rationale:

- **`update_reaction_on_completion`**: once `{asset_name}` posts the answer, it removes
  `ack_reaction` (must match `teams_agent_request_sensor`'s own `acknowledgement_reaction`) and
  adds `done_reaction` to the ORIGINAL triggering message (the `teams/message_id` tag the
  sensor sets) -- `{asset_name}_failure_watcher` adds `error_reaction` instead on a crash.
  Same delegated `ChannelMessage.Send` scope already needed for posting/reacting -- confirmed
  against Microsoft's own `chatmessage-unsetreaction` reference (v1.0, not beta-only): no
  separate permission for removing a reaction either.
- **`placeholder_id`**: if `teams_agent_request_sensor`'s own `placeholder_text` is set, the
  run carries a `teams/placeholder_id` tag -- `{asset_name}` then edits that reply in place
  (`PATCH .../replies/{id}`, needs delegated `ChannelMessage.ReadWrite` -- a DIFFERENT, broader
  scope than `ChannelMessage.Send`) with the final answer instead of posting a new one (falling
  back to a new reply if the edit fails), and `{asset_name}_failure_watcher` edits it with the
  error. Nothing to set here; this is driven entirely by that tag's presence.

```yaml
  ack_reaction: "👀"
  done_reaction: "✅"
  error_reaction: "⚠️"
```

## Minimal example

```yaml
type: dagster_community_components.TeamsReplyComponent
attributes:
  asset_name: chat_reply
  upstream_asset_key: chat_agent
  oauth_state_dir: /var/dagster/teams_agent_oauth_state
  webserver_base_url: http://localhost:3000   # powers the "View run" link; omit to skip it
```

## What this does NOT do

- Decide *what* to say -- that's the agent's job; this only posts what it already decided.
- Render anything beyond the minimal HTML escaping (`&`/`<`/`>`, newlines to `<br>`) needed to
  post plain text as a valid Teams message body -- no Adaptive Cards, tables, or rich layout
  in v1.
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
| `oauth_state_dir` | `str` | Local path or fsspec URI dedicated to this asset's '.teams_oauth_state.json' (a rotated refresh token, if Microsoft ever issues one). Can be the SAME directory teams_agent_request_sensor uses. |

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
| `teams_tenant_id_env_var` | `str` | `"TEAMS_TENANT_ID"` | Env var holding the Azure AD tenant id. |
| `teams_client_id_env_var` | `str` | `"TEAMS_CLIENT_ID"` | Env var holding the Azure AD app registration client id. |
| `teams_client_secret_env_var` | `str` | `"TEAMS_CLIENT_SECRET"` | Env var holding the Azure AD app registration client secret. |
| `teams_refresh_token_env_var` | `str` | `"TEAMS_REFRESH_TOKEN"` | Env var holding the INITIAL OAuth refresh token (see teams_run_request_sensor's README, 'Auth'). |
| `answer_field` | `str` | — | Which key of the upstream asset's output dict holds the answer text. Defaults to 'final_answer' (the agent components' own documented output shape) when the upstream value is a dict; used verbatim (str()'d) otherwise. |
| `max_chars_per_message` | `int` | `3000` | Split or truncate (see on_long_answer) an answer longer than this many characters. |
| `on_long_answer` | `Literal['split', 'truncate']` | `"split"` | 'split' (default): post as multiple sequential thread replies, each <= max_chars_per_message. 'truncate': post one reply, cut short with an ellipsis. |
| `include_run_link` | `bool` | `true` | Append a link to the Dagster run (requires webserver_base_url) to the last reply posted. |
| `run_link_label` | `str` | `"View run"` | Link text for include_run_link. |
| `webserver_base_url` | `str` | — | Base URL of the Dagster webserver (e.g. 'http://localhost:3000' or your Dagster+ deployment URL) -- used to build the run link. Leave unset to omit the link entirely. |
| `monitor_failures` | `bool` | `true` | Also emit {asset_name}_failure_watcher, a run-status sensor that posts a short error reply for ANY failed run carrying teams/team_id + teams/channel_id + teams/thread_id tags -- so a crashed run is never silent in the ch… _(full docs in schema.json + component README)_ |
| `error_message_template` | `str` | `"⚠️ Something went wrong answering this: {error}"` | Template for the failure-path reply. Placeholder: {error}. |
| `update_reaction_on_completion` | `bool` | `true` | On success, remove ack_reaction and add done_reaction to the ORIGINAL triggering message (the teams/message_id tag teams_agent_request_sensor sets) -- so a 20-30s answer doesn't just look stuck at its acknowledgement rea… _(full docs in schema.json + component README)_ |
| `ack_reaction` | `str` | `"👀"` | The reaction teams_agent_request_sensor posted on pickup (must match its own acknowledgement_reaction) -- removed on completion. Only used when update_reaction_on_completion is true. |
| `done_reaction` | `str` | `"✅"` | Reaction added to the original message once the reply posts successfully. Only used when update_reaction_on_completion is true. |
| `error_reaction` | `str` | `"⚠️"` | Reaction added to the original message when the run fails (added by {asset_name}_failure_watcher). Only used when update_reaction_on_completion is true. |
| `receipts_db_path` | `str` | — | SQLite file path -- the SAME receipts_db_path a knowledge_search_server/hybrid_knowledge_search_server the agent calls is configured with (and which must also have run_id_template set, the default, so its receipts carry… _(full docs in schema.json + component README)_ |
| `report_files_dir` | `str` | — | Local directory root -- the SAME files_dir a report_tools_server this agent calls is configured with. When set, after posting the answer, this asset reads this run's manifest (written by report_tools_server's make_chart/… _(full docs in schema.json + component README)_ |
| `watch_reactions` | `bool` | `false` | Also emit {asset_name}_rating_watcher, a sensor that polls reactions on every Teams reply this asset has linked to a receipt (via receipts_db_path, which must also be set) and records a thumbs_up/thumbs_down outcome agai… _(full docs in schema.json + component README)_ |
| `positive_reaction` | `str` | `"👍"` | Reaction (any Unicode emoji character, or a legacy name like 'like') that records a thumbs_up outcome. Only used when watch_reactions is true. |
| `negative_reaction` | `str` | `"👎"` | Reaction that records a thumbs_down outcome. Only used when watch_reactions is true. |
| `rating_poll_interval_seconds` | `int` | `30` | Minimum seconds between rating_watcher ticks. Only used when watch_reactions is true. |
| `rating_lookback_messages` | `int` | `200` | Most-recently-linked replies to keep polling for reactions, and the cursor's bound. Only used when watch_reactions is true. |
| `resolve_rater_email` | `bool` | `true` | Resolve the reacting user's Azure AD object id to an email address (needs delegated User.Read.All) before using it as the outcome's outcome_ref. When false, the raw Azure AD object id is used instead. Only used when watch_reactions is true. |

[//]: # (FIELDS:END)

## Requirements

```
dagster
requests
```
