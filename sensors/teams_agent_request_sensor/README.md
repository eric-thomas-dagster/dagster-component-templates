# teams_agent_request_sensor

A free-form chat agent for Microsoft Teams, in the style of
[`teams_run_request_sensor`](../teams_run_request_sensor) but for open-ended questions instead
of fixed phrases -- Teams analog of [`slack_agent_request_sensor`](../slack_agent_request_sensor):
this sensor polls a channel and launches a chat-agent asset (an `anthropic_agent` /
`openai_agent` / `litellm_agent` asset, plus a downstream
[`teams_reply`](../../assets/sinks/teams_reply) asset) for a message that @-mentions the bot,
or is a reply in a thread the bot already answered. The agent decides what to do with it --
answer a question, call a read tool, call `dagster_workflow_tools`' `launch_job` -- this
sensor's only job is noticing the message and launching the right run.

## Type

```
dagster_community_components.TeamsAgentRequestSensorComponent
```

## Why there's no owners check here, unlike teams_run_request_sensor

`teams_run_request_sensor` launches a specific, pre-declared job/asset directly, so it has to
gate who can trigger it. This sensor only launches the **agent** -- a read-only conversation by
default. The actual safety-critical decision ("can this person launch THIS job") belongs to
whatever write tool the agent calls, not to "can this person talk to the bot at all" (that's
Teams' own channel-membership model already handling access). `dagster_workflow_tools`'
`launch_job` is where the real owners check happens, using the same `teams/user` tag this
sensor sets -- see its README.

## Identifiers only in tags -- never the message text

Every `RunRequest` carries `teams/team_id`, `teams/channel_id`, `teams/thread_id`,
`teams/message_id`, `teams/user` (plus `dagster/isolation: disabled` by default -- see
"Isolation" in `slack_agent_request_sensor`'s own README, which applies identically here). The
message text itself is never tagged -- the agent reads it live via
[`teams_chat_tools`](../../assets/ai/teams_chat_tools)'s `get_thread()`, using those same tag
values, right when it actually needs it.

## `trigger_mode`

- **`mention`** (default): a brand-new conversation only starts when a message @-mentions the
  bot. Unlike Slack's `<@BOT_ID>` text-substring check, this reads the structured
  `mentions[]` field Graph already returns on every message (`mentioned.application.id`) --
  confirmed against Microsoft's own `chatMessageMentionedIdentitySet` schema. `bot_application_id`
  defaults to `teams_client_id_env_var`'s own value (the Azure AD app registration's client
  id) since there's no Teams equivalent of Slack's `auth.test` self-lookup under a delegated
  token (it represents the consenting human, not the bot).
- **`all`**: matches every new message unconditionally. **Never use this on a busy shared
  channel.** Unlike `slack_agent_request_sensor`, there is no separate `dm` mode -- this
  sensor only polls team channels, never 1:1 personal chats, the same scope
  `teams_run_request_sensor` already has.

A reply in an already-tracked thread (see `follow_up_threads`) matches regardless of
`trigger_mode`.

## `follow_up_threads` -- more expensive here than on Slack

Unlike Slack's `conversations.replies(oldest=...)`, Microsoft Graph's
`.../messages/{id}/replies` endpoint has **no delta query and no `$filter`** on
`createdDateTime`/`lastModifiedDateTime` (confirmed against Microsoft's own
`chatmessage-list-replies` reference) -- only `$top` (hard-capped at 50) and `@odata.nextLink`
pagination. So this sensor re-fetches **every current reply** in each tracked thread on every
tick and diffs client-side against the last-seen `createdDateTime`, rather than getting new
replies for free the way the Slack version does. This is real, not free: it's
**N full replies-list calls per tick, one per tracked thread** (bounded by
`known_threads_limit`). Set `follow_up_threads: false` for a channel with many concurrent
long-lived threads, or lower `known_threads_limit` / `messages_per_poll`.

## Isolation and in-process execution

Same mechanics and the same two separate knobs as `slack_agent_request_sensor`:
`set_isolation_disabled_tag` (tags `dagster/isolation: disabled` to skip the container
startup cost) and `use_in_process_executor` (sets `run_config` to skip the
multi-process-executor's per-step subprocess spawn) -- see that component's own README
("Isolation") for the full Dagster+ non-isolated-run mechanics, which apply identically
regardless of chat platform.

## Auth -- Microsoft Graph, delegated only

Same real constraint as `teams_run_request_sensor` -- see its own README ("Auth") for the
full one-time setup (Entra ID app registration, delegated `ChannelMessage.Read.All` +
`offline_access` + `ChannelMessage.Send` + `User.Read.All`, a one-time interactive consent for
a dedicated bot account, `oauth_state_dir` for a rotated refresh token). The one addition this
sensor needs beyond that setup: **reacting to a message** (`post_acknowledgement`) also uses
delegated `ChannelMessage.Send` -- confirmed against Microsoft's own `chatmessage-setreaction`
reference, no additional scope or consent needed.

## What this does NOT do

- Classify intent or decide *what* to do with a message -- that's the agent's job, downstream.
- Authorize who can trigger the agent beyond `trigger_mode` -- see "Why there's no owners
  check" above; write-authorization lives in `dagster_workflow_tools`' `launch_job`.
- Listen via a push-based mechanism (Graph change notifications / webhooks) -- polling only,
  v1, same as `teams_run_request_sensor`.
- Rate-limit by user -- nothing here stops one person from asking many questions in a row
  (each still costs a real run).

## Minimal example

```yaml
type: dagster_community_components.TeamsAgentRequestSensorComponent
attributes:
  sensor_name: ask_dagster_teams
  teams_team_id: "19:xxxxxxxx@thread.tacv2"
  teams_channel_id: "19:yyyyyyyy@thread.tacv2"
  asset_selection: "group:chat_agent"   # the agent asset + its downstream teams_reply asset
  oauth_state_dir: /var/dagster/teams_agent_oauth_state

  trigger_mode: mention
  follow_up_threads: true
  default_status: STOPPED
```

## Full example

See `example.yaml`.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `sensor_name` | `str` | Unique sensor name. |
| `teams_team_id` | `str` | Teams team id (Teams client -> team -> ... -> Get link). |
| `teams_channel_id` | `str` | Teams channel id inside the team (e.g. '19:xxx@thread.tacv2'). |
| `asset_selection` | `Union[str, List[str]]` | The agent asset (and anything downstream, e.g. a teams_reply asset) to materialize when a message matches: explicit key list, the Dagster selection DSL (tag:/group:/kind:/boolean composition), '*', or a bare fnmatch glob… _(full docs in schema.json + component README)_ |
| `oauth_state_dir` | `str` | Local path or fsspec URI (s3://, gs://, abfs://) dedicated to this sensor's '.teams_oauth_state.json' -- holds a rotated OAuth refresh token, if Microsoft ever issues one. Without a durable place to persist it, the next… _(full docs in schema.json + component README)_ |

### Sensor configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `minimum_interval_seconds` | `int` | `10` | Minimum seconds between sensor evaluations. Chat latency is roughly this plus the run-queue dequeue time plus the LLM call -- keep this low. |
| `default_status` | `str` | `"STOPPED"` | 'RUNNING' or 'STOPPED' -- initial status of the sensor. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `teams_tenant_id_env_var` | `str` | `"TEAMS_TENANT_ID"` | Env var holding the Azure AD tenant id. |
| `teams_client_id_env_var` | `str` | `"TEAMS_CLIENT_ID"` | Env var holding the Azure AD app registration client id. Also the default source for bot_application_id when that field is left unset. |
| `teams_client_secret_env_var` | `str` | `"TEAMS_CLIENT_SECRET"` | Env var holding the Azure AD app registration client secret. |
| `teams_refresh_token_env_var` | `str` | `"TEAMS_REFRESH_TOKEN"` | Env var holding the INITIAL OAuth refresh token (one-time interactive consent for the dedicated bot account -- see README, 'Auth'). Only the seed; a later rotation is persisted to oauth_state_dir instead. |
| `bot_application_id` | `str` | — | This bot's own Azure AD application (client) id, for mention-detection -- a message's mentions[] carries mentioned.application.id when a bot/app is @-mentioned (confirmed against Graph's own chatMessageMentionedIdentityS… _(full docs in schema.json + component README)_ |
| `trigger_mode` | `Literal['mention', 'all']` | `"mention"` | 'mention' (default): only a message that @-mentions the bot starts a NEW conversation (a reply in an already-tracked thread still matches regardless of mode -- see follow_up_threads). 'all' matches every new root message… _(full docs in schema.json + component README)_ |
| `follow_up_threads` | `bool` | `true` | Also launch a run for a NEW REPLY in a thread this sensor has already launched a run for -- not gated by trigger_mode. Unlike Slack, Graph's replies endpoint has no delta/incremental query (confirmed against Microsoft's… _(full docs in schema.json + component README)_ |
| `known_threads_limit` | `int` | `200` | Most-recently-active threads to keep polling for follow-ups. Bounds both cursor size and the extra replies-list calls follow_up_threads costs per tick. |
| `resolve_requester_email` | `bool` | `true` | Resolve the message sender's Azure AD object id to an email address (needs delegated User.Read.All) before setting the teams/user tag -- most owners: checks downstream (e.g. dagster_workflow_tools' launch_job) use emails… _(full docs in schema.json + component README)_ |
| `messages_per_poll` | `int` | `50` | Messages/replies to request per Graph page (Graph's own hard cap is 50). |
| `max_pages_per_poll` | `int` | `3` | Follow at most this many '@odata.nextLink' pages per poll, for both new-root-message and (per-thread) replies polling. |
| `process_backlog_on_first_run` | `bool` | `false` | If false (default), the first-ever tick seeds the cursor to 'now' and processes nothing -- activating this sensor never answers a channel's past history. |
| `post_acknowledgement` | `bool` | `true` | React to a matched message immediately (acknowledgement_reaction) so the sender sees it was received before the agent run even starts. Needs delegated ChannelMessage.Send (the same scope already provisioned for replies). |
| `acknowledgement_reaction` | `str` | `"👀"` | Reaction for post_acknowledgement -- any Unicode emoji character, or a legacy name ('like', 'heart', 'laugh', 'surprised', 'sad', 'angry'). |
| `set_isolation_disabled_tag` | `bool` | `true` | Tag every RunRequest dagster/isolation: disabled, so the run starts in-process on the code location server instead of spinning up an isolated container (which can take up to 3 minutes on Dagster+ -- far too slow for chat… _(full docs in schema.json + component README)_ |
| `use_in_process_executor` | `bool` | `true` | Tag every RunRequest with run_config={'execution': {'config': {'in_process': {}}}}, so the agent asset and its downstream teams_reply asset run as plain function calls in one step-process instead of each spawning its own… _(full docs in schema.json + component README)_ |

[//]: # (FIELDS:END)

## Requirements

```
dagster
requests
```
