# slack_agent_request_sensor

A free-form chat agent for Slack, in the style of
[`slack_run_request_sensor`](../slack_run_request_sensor) but for open-ended questions instead
of fixed phrases: this sensor polls a channel and launches a chat-agent asset (an
`anthropic_agent` / `openai_agent` / `litellm_agent` asset, plus a downstream
[`slack_reply`](../../assets/sinks/slack_reply) asset) for a message that @-mentions the bot, is
in a DM, or is a reply in a thread the bot already answered. The agent decides what to do with
it -- answer a question, call a read tool, call `dagster_workflow_tools`' `launch_job` -- this
sensor's only job is noticing the message and launching the right run.

## Type

```
dagster_community_components.SlackAgentRequestSensorComponent
```

## Why there's no owners check here, unlike slack_run_request_sensor

`slack_run_request_sensor` launches a specific, pre-declared job/asset directly, so it has to
gate who can trigger it. This sensor only launches the **agent** -- a read-only conversation by
default. The actual safety-critical decision ("can this person launch THIS job") belongs to
whatever write tool the agent calls, not to "can this person talk to the bot at all" (that's
Slack's own channel-membership model already handling access). `dagster_workflow_tools`'
`launch_job` is where the real owners check happens, using the same `slack/user` tag this sensor
sets -- see its README. `trigger_mode` here is about chattiness, not security.

## Identifiers only in tags -- never the message text

Every `RunRequest` carries exactly four Slack tags: `slack/channel`, `slack/thread_ts`,
`slack/message_ts`, `slack/user` (plus `dagster/isolation: disabled` by default -- see
"Isolation" below). The message text itself is never tagged -- the agent reads it live via
[`slack_chat_tools`](../../assets/ai/slack_chat_tools)'s `get_thread()`, using those same tag
values, right when it actually needs it.

## `trigger_mode`

- **`mention`** (default): a brand-new conversation only starts when a message @-mentions the
  bot (`<@BOT_USER_ID>` in its text). `bot_user_id` auto-discovers via `auth.test` if unset.
- **`dm`** / **`all`**: match every new message unconditionally -- functionally identical to
  each other; `dm` exists only so a DM channel's config reads as a DM, not as "answer literally
  everything" by accident. **Never use these on a busy shared channel.**

A reply in an already-tracked thread (see `follow_up_threads`) matches regardless of
`trigger_mode` -- continuing a conversation doesn't need a fresh mention.

## `follow_up_threads` -- and why it needs a second kind of poll

`conversations.history` (used to find new root messages) does **not** return thread replies --
confirmed against Slack's own API reference, not assumed; replies live only behind
`conversations.replies`. So this sensor tracks, per thread it has already launched a run for, the
last reply timestamp seen in it, and polls `conversations.replies(channel, ts=thread_ts,
oldest=last_seen)` for each tracked thread every tick -- that endpoint's own `oldest` has the
same exclusive-by-default semantics as `conversations.history`'s, so it naturally returns only
genuine new replies, parent never included again.

This is **N extra Slack API calls per tick**, one per tracked thread (bounded by
`known_threads_limit`, which keeps only the most-recently-active threads). Set
`follow_up_threads: false` for a channel with many concurrent long-lived threads, or lower
`known_threads_limit`.

## Isolation

Every `RunRequest` is tagged `dagster/isolation: disabled` by default (`set_isolation_disabled_tag`)
so the run starts in-process on the code location server instead of spinning up an isolated
container -- Dagster+'s own docs state an isolated run "may take up to 3 minutes to start," far
too slow for chat.

- **Resources**: a non-isolated run gets 0.25 vCPU / 1GB RAM on Serverless; on Hybrid it shares
  whatever the code location server itself has.
- **Concurrency**: `max_concurrent_non_isolated_runs` defaults to **1** on Dagster+. A run
  launched beyond that limit **falls back to fully isolated** -- not queued -- so a second
  person asking while the first answer is still running would silently get a slow isolated run.
  Raise this setting if more than one concurrent chat question is expected.
- **Default-enabled or not**: Dagster's own two docs pages disagree here -- the deployment
  settings reference states `non_isolated_runs.enabled` defaults to `true`, while the Serverless
  run-isolation guide says it "must first be enabled" in Full deployment settings. Check your own
  deployment's Settings page rather than assume either.
- **Shared resources**: a non-isolated run shares the code location server's resources with
  every sensor and schedule for that location -- including this one. A heavy agent run can
  starve or crash the server. **A dedicated code location for the chat agent is recommended**,
  the same way Dagster's own docs advise for Hybrid.
- End-to-end latency is roughly `minimum_interval_seconds` + the run-queue dequeue time + the
  LLM call -- expect something in the 10-40s range, dominated by the first two; this estimate is
  **not** independently confirmed down to the second against Dagster's own docs.
- **`use_in_process_executor`** (default `true`): also sets
  `run_config={"execution": {"config": {"in_process": {}}}}` on every `RunRequest`. This is a
  second, separate knob from `set_isolation_disabled_tag` -- that tag skips container
  *startup*; this setting skips `multi_or_in_process_executor`'s default subprocess *per step*,
  so the agent step and the downstream `slack_reply` step run as plain function calls in one
  process instead of two. Requires the code location that owns these assets to use the default
  executor (or no explicit `executor=` at all) -- if that `Definitions` sets something else
  (`k8s_job_executor`, `celery_executor`, `docker_executor`, ...), that executor's own config
  schema rejects an `in_process` key it doesn't recognize, and the run fails with a
  `RunConfigValidationInvalid` error. Set `use_in_process_executor: false` if that's the case.

## Auth -- Slack app setup

Same bot-token model as `slack_run_request_sensor` -- no shared code, but the same one-time
setup, plus the extra `reactions:write` scope for `post_acknowledgement`.

**One-time setup:**

1. Create a Slack app at https://api.slack.com/apps (or reuse the one `slack_run_request_sensor`
   / `slack_approval_gate` / `slack_message_send` already use in this workspace -- they share
   nothing in code, but can share the same installed app and token).
2. In the app's settings, under **"What it can do in Slack?"** (OAuth & Permissions → Bot
   Token Scopes), add:
   - `channels:history` (public channel) or `groups:history` (private channel).
   - `chat:write` -- unless `post_acknowledgement: false`.
   - `reactions:write` -- unless `post_acknowledgement: false`. Powers the immediate
     `acknowledgement_reaction` (default: 👀 `eyes`) on a matched message.
   - `users:read.email` -- unless `resolve_requester_email: false`. Turns the message
     sender's Slack user id into the email address your `owners:` lists (and
     `dagster_workflow_tools`' `launch_job` authorization) actually use.
3. Under **"What it can respond to in Slack?"** (Event Subscriptions): leave this **off**.
   This sensor polls on a timer -- it never subscribes to live Slack events, so there's
   nothing to turn on here (and nothing to point at a public HTTPS endpoint, which this
   design doesn't have or need).
4. **Install the app to the workspace** (reinstall any time you add a scope), then **invite
   the bot to the target channel** -- a bot token can only read conversations it's a member
   of, even a public one.
5. Export:

```bash
export SLACK_BOT_TOKEN=xoxb-...
```

## Showing progress (`placeholder_text`) and the completion reaction

Without this, the sensor adds its acknowledgement reaction (👀 by default) when it picks a
message up and nothing visibly changes after that -- so a normal 20-30s answer looks stuck.
Two independent, composable fixes:

- **The reaction**: once `slack_reply` posts the answer (or its `failure_watcher` catches a
  crash), it removes the acknowledgement reaction and adds a done/error one instead -- see
  `slack_reply`'s own README ("update_reaction_on_completion"). Nothing to set here; it reads
  the `slack/message_ts` tag this sensor already puts on every run.
- **`placeholder_text`**: set this to post a real message (e.g. `"Looking into it…"`) right
  after acknowledging, tagged `slack/placeholder_ts` so `slack_reply` can find and *edit* it
  (`chat.update`) with the final answer instead of posting a new one. We checked Slack's own
  docs rather than assuming: the newer `agents.sessions.setStatus` DOES work in a regular
  channel thread (not just the AI-assistant pane the older `assistant.threads.setStatus` is
  scoped to), but it has no free-text field -- just an enum (`active`/`processing`/`suspended`/
  `closed`) -- and needs the Agents feature enabled on the workspace. A placeholder message is
  what actually shows free text like "Looking into it…", everywhere, with no feature flag.

```yaml
  placeholder_text: "Looking into it…"
```

## What this does NOT do

- Classify intent or decide *what* to do with a message -- that's the agent's job, downstream.
- Authorize who can trigger the agent beyond `trigger_mode` -- see "Why there's no owners check"
  above; write-authorization lives in `dagster_workflow_tools`' `launch_job`.
- Listen via a push-based mechanism (Slack Socket Mode / Events API) -- polling only, v1.
- Rate-limit by user -- a follow-up: nothing here stops one person from asking many questions in
  a row (each still costs a real run).

## Minimal example

```yaml
type: dagster_community_components.SlackAgentRequestSensorComponent
attributes:
  sensor_name: ask_dagster
  slack_channel_id: "C0123ABCDEF"
  asset_selection: "group:chat_agent"   # the agent asset + its downstream slack_reply asset

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
| `slack_channel_id` | `str` | Channel ID to poll (e.g. 'C0123ABCDEF' or a DM's 'D0123ABCDEF') -- NOT a '#channel-name'; conversations.history requires the real id. |
| `asset_selection` | `Union[str, List[str]]` | The agent asset (and anything downstream, e.g. a slack_reply asset) to materialize when a message matches: explicit key list, the Dagster selection DSL (tag:/group:/kind:/boolean composition), '*', or a bare fnmatch glob… _(full docs in schema.json + component README)_ |

### Sensor configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `minimum_interval_seconds` | `int` | `10` | Minimum seconds between sensor evaluations. Chat latency is roughly this plus the run-queue dequeue time plus the LLM call -- keep this low. |
| `default_status` | `str` | `"STOPPED"` | 'RUNNING' or 'STOPPED' -- initial status of the sensor. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `slack_bot_token_env_var` | `str` | `"SLACK_BOT_TOKEN"` | Env var holding the bot's 'xoxb-...' token. Needs channels:history (or groups:history) + chat:write + reactions:write + users:read.email scopes. |
| `bot_user_id` | `str` | — | This bot's own Slack user id, for mention-detection ('<@BOT_ID>' in a message) and self-exclusion. Auto-discovered via auth.test if unset -- set explicitly only to skip that one extra API call per tick. |
| `trigger_mode` | `Literal['mention', 'dm', 'all']` | `"mention"` | 'mention' (default): only a message that @-mentions the bot starts a NEW conversation (a reply in an already-tracked thread still matches regardless of mode -- see follow_up_threads). 'dm' and 'all' both match every new… _(full docs in schema.json + component README)_ |
| `follow_up_threads` | `bool` | `true` | Also launch a run for a NEW REPLY in a thread this sensor has already launched a run for -- not gated by trigger_mode, so once a thread is tracked, every new reply in it triggers another run, not just ones that re-mentio… _(full docs in schema.json + component README)_ |
| `known_threads_limit` | `int` | `200` | Most-recently-active threads to keep polling for follow-ups. Bounds both cursor size and the extra conversations.replies calls follow_up_threads costs per tick. |
| `resolve_requester_email` | `bool` | `true` | Resolve the message sender's Slack user id to an email address (needs users:read.email) before setting the slack/user tag -- most owners: checks downstream (e.g. dagster_workflow_tools' launch_job) use emails, not Slack… _(full docs in schema.json + component README)_ |
| `messages_per_poll` | `int` | `200` | Root messages to request per Slack page (Slack's own recommended max is 200). |
| `max_pages_per_poll` | `int` | `3` | Follow at most this many pagination cursors per poll, for both new-root-message and (per-thread) new-reply polling. |
| `process_backlog_on_first_run` | `bool` | `false` | If false (default), the first-ever tick seeds the cursor to 'now' and processes nothing -- activating this sensor never answers a channel's past history. |
| `post_acknowledgement` | `bool` | `true` | React to a matched message immediately (acknowledgement_reaction) so the sender sees it was received before the agent run even starts. |
| `acknowledgement_reaction` | `str` | `"eyes"` | Reaction name for post_acknowledgement (one of Slack's built-in emoji names, e.g. 'eyes', 'thinking_face'). |
| `placeholder_text` | `str` | — | If set, post this text as a new thread reply right after acknowledging a matched message, and tag the run slack/placeholder_ts with that reply's own ts. slack_reply then updates THIS message with the final answer (chat.u… _(full docs in schema.json + component README)_ |
| `set_isolation_disabled_tag` | `bool` | `true` | Tag every RunRequest dagster/isolation: disabled, so the run starts in-process on the code location server instead of spinning up an isolated container (which can take up to 3 minutes on Dagster+ -- far too slow for chat… _(full docs in schema.json + component README)_ |
| `use_in_process_executor` | `bool` | `true` | Tag every RunRequest with run_config={'execution': {'config': {'in_process': {}}}}, so the agent asset and its downstream slack_reply asset run as plain function calls in one step-process instead of each spawning its own… _(full docs in schema.json + component README)_ |

[//]: # (FIELDS:END)

## Requirements

```
dagster
slack_sdk>=3.0
```
