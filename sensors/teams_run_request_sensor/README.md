# teams_run_request_sensor

Replaces the person who reads a Teams channel for "can you run X" messages and manually
launches the job. This sensor polls the channel itself, matches the message text against
configured trigger phrases, and launches a `RunRequest` for the matching target -- but
**only** when the requester is already an authorized owner of that target.

## Type

```
dagster_community_components.TeamsRunRequestSensorComponent
```

## Why authorization is load-bearing, not optional

Anyone can post to a Teams channel. Without a real check, this component would let any
channel member launch production jobs by typing a sentence. So every match is checked
against the target's own catalog metadata first:

- **`asset_selection` triggers** (the common case): owners are read **live** from the
  matched asset(s)' own `owners:` field -- the same catalog metadata every asset in this
  repo already declares (team names or email addresses). Nothing is duplicated into this
  sensor's own config.
- **`job_name` triggers**: a bare Dagster job has no asset metadata to check, so the
  trigger must list `owners:` explicitly. This is required, not optional -- a `job_name`
  trigger with no `owners:` fails validation at config time rather than silently
  authorizing everyone.

`require_owner_authorization: false` turns this off entirely. Read its field description
before setting it -- it means any channel member can launch any configured trigger's
target.

## Command matching is deterministic, not an LLM

Each trigger lists plain-text `phrases`. A message matches when any of its trigger's
phrases appears (case-insensitively, by default) anywhere in the message text -- first
matching trigger (in config order) wins. There is no intent-classification model here on
purpose: a sentence that can silently launch a job either matches a phrase you wrote down
or it doesn't. An LLM "close enough" match would make the set of things a channel member
can trigger impossible to audit from the YAML alone.

## What a message looks like

Given the minimal example's trigger (`phrases: ["run the nightly etl", "kick off nightly"]`,
`asset_selection: "group:nightly_etl"`), any of these channel messages fire it -- as long
as the sender is a listed owner of `nightly_etl`:

```
Hey, can someone run the nightly etl job? it didn't kick off on its own
```
```
@databot kick off nightly please
```

**Pasting a link instead of typing a sentence also works**, with no phrase needed at all.
If a teammate uses Dagster's own "Copy link" on the asset or job and pastes it:

```
can someone run this: https://dagster.example.com/assets/nightly/etl
```
```
need this rerun -> https://dagster.example.com/locations/my_project/jobs/nightly_etl_job
```

This is `match_dagster_links` (on by default) -- it recognizes the real Dagster UI URL
shapes (`/assets/<key>`, `/locations/<repo>/jobs/<name>`) and matches the extracted
asset key or job name directly against each trigger's own resolved target, no `phrases:`
match required. It only checks the URL's **path shape and target name** -- not which
Dagster instance the link actually points at -- so a same-named target on a different
deployment would also match; the owners-authorization gate still applies regardless, so
this never loosens who can trigger what. Set `match_dagster_links: false` if you'd rather
require a typed phrase every time.

A message that matches NO trigger (`good morning team`) or comes from someone who isn't
a listed owner is simply skipped -- a threaded reply explains which, by default
(`post_acknowledgement` / `on_unauthorized`).

## Auth -- a DELEGATED token, not app-only

Confirmed against Microsoft Graph's own docs: posting a channel message
(`POST /teams/{id}/channels/{id}/messages`) has **no working application (app-only)
permission** at all -- only `Teamwork.Migrate.All`, scoped to migration tooling.
`ChannelMessage.Send` and the application form of `ChannelMessage.Read.All` also hit a
separate Microsoft "Protected API" approval process beyond ordinary tenant-admin consent.
The **delegated** versions of both need only ordinary admin consent, so -- matching
`teams_approval_gate`'s own prior verification of this exact constraint -- this component
authenticates as a dedicated "requests bot" Teams account via a one-time interactive
consent, then refreshes that token on every poll. It is a separate implementation from
`teams_approval_gate` (no shared code, per this repo's convention), but makes the same
auth decision for the same reason.

**One-time setup:**

1. Register an app in Entra ID (Azure AD) → App registrations → New, with **delegated**
   permissions:
   - `ChannelMessage.Read.All` (or the resource-specific `ChannelMessage.Read.Group`, lower
     friction -- scoped to one team via the Teams app manifest instead of tenant-wide).
   - `ChannelMessage.Send` -- unless `post_acknowledgement: false`.
   - `User.Read.All` -- unless `resolve_requester_email: false`. This is what turns a
     message sender's Azure AD object id into the email address your `owners:` lists
     actually use.
   - `offline_access`.
2. Create a client secret under Certificates & secrets.
3. Run an interactive OAuth consent **once**, signed in as the dedicated bot account (a
   normal Teams user added to the target team/channel) -- e.g. via MSAL's device-code flow
   -- to capture the first refresh token.
4. Export as:

```bash
export TEAMS_TENANT_ID=<tenant-guid>
export TEAMS_CLIENT_ID=<client-guid>
export TEAMS_CLIENT_SECRET=<secret-value>
export TEAMS_REFRESH_TOKEN=<refresh-token-from-step-3>
```

Azure AD can rotate the refresh token on any refresh call. `TEAMS_REFRESH_TOKEN` (or
whatever `teams_refresh_token_env_var` names) is only the **initial seed** -- once
Microsoft returns a different refresh token, it's persisted to
`<oauth_state_dir>/.teams_oauth_state.json` and preferred over the env var from then on.
`oauth_state_dir` must be a durable, dedicated location -- without it, the next rotation
breaks this sensor's auth entirely.

## What this does NOT do

- **Verify `team:`-prefixed owners.** An asset's (or trigger's) `owners:` may list
  `"team:analytics"`. There is no Azure AD group-membership lookup here, so a `team:`
  entry never matches a Teams message sender -- it fails closed. Only an exact email or
  literal Azure AD object id can authorize a chat-triggered run.
- **Replay channel history.** `process_backlog_on_first_run: false` (the default) seeds
  the cursor to "now" the first time this sensor ever ticks, so turning it on doesn't
  suddenly launch every job anyone has ever asked for in that channel's history.
- **Guarantee zero missed messages under a backlog.** Microsoft Graph's channel-messages
  endpoint has no delta/incremental query and no `$filter` on `createdDateTime` (confirmed
  against Microsoft's own API reference). Each poll re-lists the `messages_per_poll` most
  recent messages (max 50, Graph's own hard cap) across up to `max_pages_per_poll` pages
  and filters client-side by `createdDateTime > cursor`. Keep `minimum_interval_seconds`
  short enough that a channel never accumulates more new messages than that between polls.
- **Classify intent beyond a plain substring match.** See "Command matching" above --
  deliberate, not a missing feature.

## Minimal example

```yaml
type: dagster_community_components.TeamsRunRequestSensorComponent
attributes:
  sensor_name: teams_run_requests
  teams_team_id: "19:abc123@thread.tacv2"
  teams_channel_id: "19:xyz789@thread.tacv2"
  oauth_state_dir: /shared/teams_run_request_sensor

  triggers:
    - phrases: ["run the nightly etl", "kick off nightly"]
      asset_selection: "group:nightly_etl"   # owners come from nightly_etl's own metadata

  default_status: STOPPED
```

`ack_message_template` and `unauthorized_message_template` also accept a **list of
strings** instead of one -- a random entry is picked each time, so replies don't read as
copy-pasted:

```yaml
  ack_message_template:
    - "✅ Got it — launching <b>{target}</b> for you."
    - "👍 On it — <b>{target}</b> is launching now."
    - "🚀 Launching <b>{target}</b>..."
```

## Full example

See `example.yaml` (also shows a `job_name` trigger with an explicit `owners:` list).

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `sensor_name` | `str` | Unique sensor name. |
| `teams_team_id` | `str` | Teams team id (Teams client -> team -> ... -> Get link). |
| `teams_channel_id` | `str` | Teams channel id inside the team (e.g. '19:xxx@thread.tacv2'). |
| `triggers` | `List[TriggerSpec]` | Phrase-to-target mappings this sensor recognizes. At least one required. |
| `oauth_state_dir` | `str` | Local path or fsspec URI (s3://, gs://, abfs://) dedicated to this sensor's '.teams_oauth_state.json' -- holds a rotated OAuth refresh token, if Microsoft ever issues one. Without a durable place to persist it, the next… _(full docs in schema.json + component README)_ |

### Sensor configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `minimum_interval_seconds` | `int` | `30` | Minimum seconds between sensor evaluations. |
| `default_status` | `str` | `"STOPPED"` | 'RUNNING' or 'STOPPED' -- initial status of the sensor. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `teams_tenant_id_env_var` | `str` | `"TEAMS_TENANT_ID"` | Env var holding the Azure AD tenant id. |
| `teams_client_id_env_var` | `str` | `"TEAMS_CLIENT_ID"` | Env var holding the Azure AD app registration client id. |
| `teams_client_secret_env_var` | `str` | `"TEAMS_CLIENT_SECRET"` | Env var holding the Azure AD app registration client secret. |
| `teams_refresh_token_env_var` | `str` | `"TEAMS_REFRESH_TOKEN"` | Env var holding the INITIAL OAuth refresh token (one-time interactive consent for the dedicated bot account -- see this file's module docstring, 'Auth'). Only the seed; a later rotation is persisted to oauth_state_dir instead. |
| `require_owner_authorization` | `bool` | `true` | Require the message sender to be a listed owner of the matched target before launching it. THIS IS THE SAFETY CHECK THIS COMPONENT EXISTS TO ENFORCE -- setting it to false means ANY channel member can launch ANY configur… _(full docs in schema.json + component README)_ |
| `resolve_requester_email` | `bool` | `true` | Resolve the message sender's Azure AD object id to an email address via 'GET /users/{id}' (needs delegated User.Read.All) before checking owners -- most owners: lists use emails, not Azure AD object ids. When false, only… _(full docs in schema.json + component README)_ |
| `require_all_owners` | `bool` | `false` | For an asset_selection trigger matching MULTIPLE assets with different owners: false (default) authorizes the requester if they own ANY matched asset; true requires them to own EVERY matched asset. Ignored for job_name t… _(full docs in schema.json + component README)_ |
| `case_sensitive_phrases` | `bool` | `false` | Match trigger phrases case-sensitively. Default: case-insensitive. |
| `match_dagster_links` | `bool` | `true` | Also match a message that contains a link straight to the target asset or job in the Dagster UI (e.g. pasted via 'Copy link'), not just a typed phrase -- same owners-authorization gate either way. Recognizes the real dag… _(full docs in schema.json + component README)_ |
| `messages_per_poll` | `int` | `50` | Messages to request per Graph page (Graph's own hard cap is 50). |
| `max_pages_per_poll` | `int` | `3` | Follow at most this many '@odata.nextLink' pages per poll -- bounds how far back a single tick can catch up after an outage. |
| `process_backlog_on_first_run` | `bool` | `false` | If false (default), the first-ever tick seeds the cursor to 'now' and processes nothing -- activating this sensor never replays a channel's past run requests. Set true only if that replay is actually wanted. |
| `post_acknowledgement` | `bool` | `true` | Reply in-thread when a match is authorized and launched, and (per on_unauthorized) when it's rejected. Needs delegated ChannelMessage.Send. |
| `on_unauthorized` | `Literal['skip', 'reply']` | `"reply"` | 'reply' (default) posts unauthorized_message_template as a thread reply; 'skip' just logs it. Ignored when post_acknowledgement is false. |
| `ack_message_template` | `Union[str, List[str]]` | `"✅ Got it — launching <b>{target}</b> for you."` | HTML reply posted on an authorized match. Placeholders: {target}, {requester}, {phrase}. Give a list instead of a single string to have one chosen at random each time, so replies don't feel copy-pasted. |
| `unauthorized_message_template` | `Union[str, List[str]]` | `"⚠️ Sorry, you're not listed as an owner of <b>{target}</b>, so I can't launch it from chat. Ask an owner to run it, or ask to be added as one."` | HTML reply posted on an unauthorized match (on_unauthorized: reply). Placeholders: {target}, {requester}, {phrase}. Also accepts a list for a randomly-chosen reply, same as ack_message_template. |
| `skip_in_branch_deployments` | `bool` | `true` | Skip every tick while running inside a Dagster+ branch deployment (DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT=1). A branch deployment runs its own copy of every sensor alongside the full deployment's -- without this, a branch de… _(full docs in schema.json + component README)_ |

[//]: # (FIELDS:END)

## Requirements

```
dagster
requests
```
