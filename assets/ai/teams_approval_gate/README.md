# teams_approval_gate

Microsoft Teams HITL approval gate. Slack analog for `SlackApprovalGate`
— same asset + sensor shape, same JSON token format. Three modes, picked with `target:`:

- **`target: human_approval_gate`** *(default)* — composes with `HumanApprovalGateComponent` via the shared
  `approval_dir` (drop-in swap of Slack for Teams).
- **`target: agentic_pipeline`** — watches `agentic_pipeline`'s own `approve` op (`mode: park`) review queue
  directly: no separate `HumanApprovalGateComponent` needed. See [Using with agentic_pipeline](#using-with-agentic_pipeline) below.
- **`target: run_request`** — no token file, no separate gate component at all: on quorum, the watcher fires a
  `RunRequest` directly. See [target: run_request](#target-run_request) below.

## `correlation_mode`: no sidecar file by default

The config of a pending approval (required approvers, allowlist, timeout, ...) used to live in a
sidecar file (`<approval_dir>/<safe_key>.teams_state.json`) written at post time and globbed on
every sensor tick. **As of `correlation_mode='message_metadata'` (the new default), it doesn't** —
a plain Teams chatMessage has no hidden-metadata field the way Slack does, so that config instead
rides inside an Adaptive Card attachment's `content`: a custom `_dagster_metadata` key alongside
the card's own fields. This is real, confirmed against Graph's own docs, not guessed — attachment
`content` is stored and echoed back as an opaque string (a Loop-component attachment uses the exact
same field for a completely different schema in one of Graph's own examples), so nothing is
rendered to a human reading the channel, and the LIST channel-messages endpoint returns
`attachments` inline by default (confirmed against Graph's own reference examples) — no
per-message fetch, no extra Graph scope beyond what posting already needs. The watcher discovers
pending approvals by scanning the channel (bounded by `history_lookback_hours`) instead of
globbing a directory, and keeps escalation-already-sent / already-resolved bookkeeping in
Dagster's own native sensor cursor — not a file. `target: human_approval_gate` still writes the
one decision-token file that component reads; `target: run_request` writes no file at all (aside
from the OAuth refresh-token rotation file, if `approval_dir` is set).

Set `correlation_mode: sidecar_file` to restore the original file-based behavior (not available
for `target: run_request`).

## Type

```
dagster_community_components.TeamsApprovalGateComponent
```

## What it does

1. **Posts** an approval message to a Teams channel via MS Graph
2. **Polls** the message for votes (reactions OR thread replies) every `poll_interval_seconds`
3. When quorum is reached, **writes the standard JSON approval token** to `{approval_dir}/{partition_key}.json`
4. Downstream `HumanApprovalGateComponent` + `FilesystemMonitorSensorComponent` consume the token unchanged

## Approval modes

Teams reactions are limited to 6 built-in emoji (like / heart / laugh /
surprised / sad / angry) — no custom emoji, unlike Slack. Two modes:

- **`mode: reactions`** *(default)* — approvers react with the configured
  emoji. Fast, no typing. Default: `like` = approve, `angry` = reject.
- **`mode: reply`** — approvers post a thread reply containing the
  configured keyword. Default: `APPROVE` / `REJECT`. More flexible; the
  approver can include context in the same message.

Both modes read votes via MS Graph and write the same token format.

## Auth (Azure AD app registration) — a DELEGATED token, not app-only

**Corrected from an earlier version of this README**, which claimed application (app-only) permissions worked for
sending. Confirmed directly against Microsoft Graph's own docs: sending a channel message
(`POST /teams/{id}/channels/{id}/messages`) has **no working application permission** — only `Teamwork.Migrate.All`
is listed for app-only, and that is explicitly scoped to migration/import tooling (Microsoft's own Q&A confirms
app-only send attempts fail with "not supported in application-only context"). `ChannelMessage.Send` is a
**delegated** permission only — so, unlike an app-only client-credentials flow, posting needs a real user (the
"approval bot") to consent once, interactively, and capture an OAuth refresh token. After that one-time step,
everything is pure outbound HTTPS — no public inbound webhook/bot endpoint, same Dagster+ Serverless constraint
this component's reactions/replies-polling design already respects elsewhere.

**One-time setup:**

1. Register an app at Azure Portal → Azure Active Directory → App registrations → New.
2. Grant **delegated** permissions `ChannelMessage.Send` + `ChannelMessage.Read.All` (or the resource-specific
   `ChannelMessage.Read.Group`) + `offline_access`.
3. Create a client secret under Certificates & secrets.
4. Run an interactive OAuth consent ONCE, signed in as a dedicated "approval bot" Teams account (added to the
   target team/channel) — e.g. via MSAL's device-code flow — to capture the first refresh token.

Export as:

```bash
export TEAMS_TENANT_ID=<tenant-guid>
export TEAMS_CLIENT_ID=<client-guid>
export TEAMS_CLIENT_SECRET=<secret-value>
export TEAMS_REFRESH_TOKEN=<refresh-token-from-step-4>
```

**Azure AD can rotate the refresh token on any refresh call.** `TEAMS_REFRESH_TOKEN` (or whatever env var
`teams_refresh_token_env_var` names) is only the INITIAL seed — once Microsoft returns a different refresh token,
it's persisted to `<approval_dir>/.teams_oauth_state.json` and preferred over the env var from then on.

## Using with agentic_pipeline

`agentic_pipeline`'s own `approve` op (`mode: park`) writes a pending-review record at `<approval_dir>/pending/<key>.json` (fields include `summary`, `preview`, `fingerprint`, and `approve_by_writing`) and ends the run; a separately-generated `<asset>_approval_resume_sensor` later resumes it once a valid, correctly-fingerprinted decision token exists at `<approval_dir>/<key>.json`. A hand-written decision file with no `fingerprint` is **not** honoured — by default (`reuse: until_changed`) the pipeline refuses it.

`target: agentic_pipeline` closes this loop without a separate `HumanApprovalGateComponent`:

```yaml
type: dagster_community_components.TeamsApprovalGateComponent
attributes:
  asset_name: triage_gate
  target: agentic_pipeline
  approval_dir: /shared/approvals/triage    # the SAME approval_dir your approve step uses
  teams_team_id: "19:abc123@thread.tacv2"
  teams_channel_id: "19:xyz789@thread.tacv2"
  teams_tenant_id_env_var: TEAMS_TENANT_ID
  teams_client_id_env_var: TEAMS_CLIENT_ID
  teams_client_secret_env_var: TEAMS_CLIENT_SECRET
  teams_refresh_token_env_var: TEAMS_REFRESH_TOKEN
  mode: reply
  approver_allowlist: [aad-user-id-1]
  required_approvers: 1
```

This emits only `{asset_name}_pending_watcher` (no asset — there's no single upstream asset to post; the pipeline's own `approve` step is the source). Each tick:

1. Scans `<approval_dir>/pending/*.json` for records with no decision yet.
2. For a newly-seen record, posts the pipeline's own `summary` to the channel (same `message_template`/`{upstream_text}` placeholder as the other mode — `{upstream_text}` is the pending record's `summary`, falling back to its `preview`).
3. Once quorum is reached (or on timeout, per `on_timeout`), writes the decision by copying the pending record's `fingerprint` into the token — the same contract `agentic_pipeline`'s own `approve_pending()` reviewer-tooling helper uses — so the pipeline's resume sensor honours it.

`upstream_asset_key` is ignored in this mode. Partitioning / `kinds` / `group_name` / `tags` / `owners` / `backfill_policy` are also ignored — there's no asset for them to apply to.

## `target: run_request`

For when there's no separate gate component to hand a decision token to — just "post to Teams,
wait for a vote, launch something":

```yaml
type: dagster_community_components.TeamsApprovalGateComponent
attributes:
  asset_name: deploy_gate
  target: run_request
  upstream_asset_key: release_notes
  teams_team_id: "19:abc123@thread.tacv2"
  teams_channel_id: "19:xyz789@thread.tacv2"
  approver_allowlist: ["aad-user-id-1"]
  required_approvers: 1
  run_request_mode: asset_selection
  run_request_asset_selection: [deploy_to_prod]
```

`approval_dir` isn't needed (and can be left unset, unless your Azure AD app is expected to rotate
refresh tokens — see [Auth](#auth-azure-ad-app-registration--a-delegated-token-not-app-only)
above) — `target: run_request` requires `correlation_mode: message_metadata` (the default) and
writes no decision file at all. On quorum, the watcher fires a `dg.RunRequest` with:

- `run_key`: `{sensor_name}:{team_id}:{channel_id}:{message_id}` — Dagster's own run-key dedup
  (not a local file) is what stops a duplicate launch if the sensor processes the same resolved
  message twice.
- `run_request_mode: job` + `run_request_job_name`, or `run_request_mode: asset_selection`
  (default) + `run_request_asset_selection`.
- Tags (namespaced by `run_request_tag_prefix`, default `approval`): `approval/approved`
  (`"true"`/`"false"`), `approval/approver`, `approval/reason`, `approval/partition_key`.

## Minimal example

```yaml
type: dagster_community_components.TeamsApprovalGateComponent
attributes:
  asset_name: report_approval_teams
  upstream_asset_key: mir_report
  approval_dir: /tmp/mir_approvals

  teams_team_id:    "19:abc123@thread.tacv2"
  teams_channel_id: "19:xyz789@thread.tacv2"
  # TEAMS_TENANT_ID / TEAMS_CLIENT_ID / TEAMS_CLIENT_SECRET / TEAMS_REFRESH_TOKEN env vars must be set --
  # see "Auth" above. These field names (teams_*_env_var) all have defaults matching those var names.

  approver_allowlist:
    - "aad-user-id-1"
    - "aad-user-id-2"
```

## Full example

See `example.yaml`.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Base name. Emits `{asset_name}_posted` asset + `{asset_name}_watcher` sensor (target: human_approval_gate / run_request), or `{asset_name}_pending_watcher` sensor only (target: agentic_pipeline). |
| `teams_team_id` | `str` | Teams team id (from Teams client → team → three-dot menu → Get link). |
| `teams_channel_id` | `str` | Teams channel id inside the team (e.g. `19:xxx@thread.tacv2`). |
| `approver_allowlist` | `List[str]` | Azure AD object IDs (GUIDs) of users allowed to vote. Reactions/replies from anyone else are ignored. Find via Azure Portal → Azure AD → Users → {user} → Object ID. |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `poll_interval_seconds` | `int` | `60` | Sensor cadence — how often to poll Teams for new votes. Default 60s (Teams API is rate-limited; 30s is fine for a handful of partitions but pump higher at scale). |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | — |
| `kinds` | `List[str]` | — | Asset kinds. Default: ['human', 'approval', 'teams']. |
| `owners` | `List[str]` | — | — |
| `tags` | `Union[Dict[str, Any], List[str]]` | — | — |
| `description` | `str` | — | — |
| `backfill_policy` | `Literal['single_run', 'multi_run']` | — | Backfill execution strategy for a partitioned asset. 'single_run' sends ONE approval request covering the whole backfill range (the range's partition keys are joined into a single request/sidecar-state key, so one approv… _(full docs in schema.json + component README)_ |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | — |
| `partition_cron_schedule` | `str` | — | Required when partition_type='cron'. A cron expression restricting the date axis to a custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on Dagster's TimeWindowPartitionsDefinition(cron_schedule=...). |
| `partition_cron_fmt` | `str` | `"%Y-%m-%d"` | strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule. |
| `partition_end_offset` | `int` | `0` | Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition. 0 (the Dagster default) means a time-based partition set only ever holds COMPLETED windows -- e.g. a daily set never includes today… _(full docs in schema.json + component README)_ |
| `partition_timezone` | `str` | — | IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default). |
| `partition_start` | `str` | — | — |
| `partition_values` | `Any` | — | — |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | — |

### Sensor configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `target` | `Literal['human_approval_gate', 'agentic_pipeline', 'run_req…` | `"human_approval_gate"` | 'human_approval_gate' (default): post `upstream_asset_key`'s text, write a bare decision token for a separately-configured `HumanApprovalGateComponent` to consume -- unchanged from this component's original behavior. 'ag… _(full docs in schema.json + component README)_ |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `mode` | `str` | `"reactions"` | 'reactions' (default; approvers react with the configured emoji — Teams limits to 6 built-ins: like, heart, laugh, surprised, sad, angry) OR 'reply' (approvers post a thread reply containing the configured keyword — more… _(full docs in schema.json + component README)_ |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `Union[str, List[str]]` | — | target: human_approval_gate / run_request only (required there, ignored for target: agentic_pipeline). Upstream asset whose text is posted to Teams for approval. |
| `approval_dir` | `str` | — | Required for target: human_approval_gate / agentic_pipeline; unused (and may be left unset) for target: run_request, which writes no file at all. Matches HumanApprovalGateComponent's `approval_dir` so the existing downst… _(full docs in schema.json + component README)_ |
| `teams_tenant_id_env_var` | `str` | `"TEAMS_TENANT_ID"` | Env var holding the Azure AD tenant id. |
| `teams_client_id_env_var` | `str` | `"TEAMS_CLIENT_ID"` | Env var holding the Azure AD app registration client id. |
| `teams_client_secret_env_var` | `str` | `"TEAMS_CLIENT_SECRET"` | Env var holding the Azure AD app registration client secret. |
| `teams_refresh_token_env_var` | `str` | `"TEAMS_REFRESH_TOKEN"` | Env var holding the INITIAL OAuth refresh token (from the one-time interactive consent for the dedicated approval-bot account -- see this file's module docstring, 'Auth'). Only the seed -- if Microsoft ever rotates it, t… _(full docs in schema.json + component README)_ |
| `approve_reaction` | `str` | `"like"` | Reaction type for approval. One of: like, heart, laugh, surprised, sad, angry. |
| `reject_reaction` | `str` | `"angry"` | Reaction type for rejection. |
| `approve_reply_keyword` | `str` | `"APPROVE"` | Keyword (case-insensitive) that marks a reply as an approval vote (mode='reply'). |
| `reject_reply_keyword` | `str` | `"REJECT"` | Keyword (case-insensitive) that marks a reply as a rejection vote. |
| `message_template` | `str` | — | Optional custom message template (HTML). Placeholders: `{upstream_text}`, `{approve}` (reaction name or keyword), `{reject}`, `{required_approvers}`, `{n_allowlisted}`, `{partition_key}`, `{mode}`. |
| `required_approvers` | `int` | `1` | How many allowlisted approvers must vote-approve before the token is written. |
| `timeout_hours` | `float` | — | Optional. If quorum isn't reached within this many hours, applies `on_timeout` policy. |
| `on_timeout` | `str` | `"escalate"` | `escalate` (default) \| `reject` \| `approve` |
| `escalate_teams_user` | `str` | — | Azure AD object ID pinged when timeout escalates. Only used when `on_timeout: escalate`. |
| `correlation_mode` | `Literal['message_metadata', 'sidecar_file']` | `"message_metadata"` | 'message_metadata' (default): no per-pending-approval file at all. A plain Teams chatMessage has no hidden-metadata field the way Slack does, so the approval's own config (required_approvers, approver_allowlist, timeout_… _(full docs in schema.json + component README)_ |
| `history_lookback_hours` | `float` | `168.0` | correlation_mode='message_metadata' only: how far back (hours) the watcher scans channel messages for still-open approval cards, bounding how many pages of the list-channel-messages call it walks each tick (there's no co… _(full docs in schema.json + component README)_ |
| `run_request_mode` | `Literal['job', 'asset_selection']` | `"asset_selection"` | target: run_request only. 'job': launch run_request_job_name. 'asset_selection': materialize run_request_asset_selection. |
| `run_request_job_name` | `str` | — | target: run_request + run_request_mode='job' (required then). Job name to launch on quorum. |
| `run_request_asset_selection` | `List[str]` | — | target: run_request + run_request_mode='asset_selection' (required then). Asset keys to materialize on quorum. |
| `run_request_run_config` | `Dict[str, Any]` | — | target: run_request only. Optional run config passed through to the launched RunRequest. |
| `run_request_tag_prefix` | `str` | `"approval"` | target: run_request only. Tag namespace on the launched run: '{prefix}/approved' ('true'/'false'), '{prefix}/approver', '{prefix}/reason', '{prefix}/partition_key'. |
| `dynamic_partition_name` | `str` | — | — |
| `backfill_policy_max_partitions_per_run` | `int` | — | For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run). |

[//]: # (FIELDS:END)

## Getting the Team / Channel IDs

Teams client → open the team → three-dot menu → **Get link to team** or
**Get link to channel** → parse the URL:

```
https://teams.microsoft.com/l/team/19%3aabc...@thread.tacv2/...
                                    ^^^^^^^^^^^^^^^^^^^^^^^^ ← team id (URL-decoded)
```

The channel ID appears in the channel-link URL the same way.

## Getting Azure AD user object IDs

Portal → Azure Active Directory → Users → click a user → copy the
**Object ID** GUID at the top. That's what goes in `approver_allowlist`.

## Why not Adaptive Cards with interactive buttons?

Teams Adaptive Cards support buttons that POST back to a webhook you
host — great UX, but requires a public webhook endpoint. That doesn't
work in Dagster+ Serverless (no ingress). Reactions + replies polling
works everywhere Dagster runs.

Roadmap: Adaptive Cards path when a customer wants it + has a webhook
they can expose (Hybrid deploys, on-prem).

## Composition with HumanApprovalGate

Same pattern as SlackApprovalGate — drop the Teams gate alongside the
Human gate, both write into the same `approval_dir`. The Human gate +
downstream sensor consume the token regardless of who wrote it (Human /
Slack / Teams / raw file drop / webhook).

```yaml
# defs/report_teams/defs.yaml
type: dagster_community_components.TeamsApprovalGateComponent
attributes:
  approval_dir: /tmp/mir_approvals
  # ...

# defs/report_approval/defs.yaml
type: dagster_community_components.HumanApprovalGateComponent
attributes:
  approval_dir: /tmp/mir_approvals
  upstream_asset_key: mir_report
  # ...
```

## Cloud storage — `approval_dir` accepts URIs, not just local paths

Only relevant for `target: human_approval_gate` / `agentic_pipeline` (the decision-token file;
`target: run_request` writes no decision file) and the OAuth refresh-token rotation file (written
regardless of target, if `approval_dir` is set) and `correlation_mode: sidecar_file` (the
pending-state file). Dagster+ Serverless containers don't share a local filesystem across runs or
code locations, so a production deploy of any of these needs cloud object storage. `approval_dir`
accepts:

| Shape | Example | Requires |
|---|---|---|
| Local path | `/tmp/approvals` | nothing (pathlib fast-path) |
| S3 | `s3://my-bucket/approvals` | `pip install fsspec s3fs` |
| GCS | `gs://my-bucket/approvals` | `pip install fsspec gcsfs` |
| Azure ADLS | `abfs://container@account.dfs.core.windows.net/approvals` | `pip install fsspec adlfs` |

Auth follows the driver's default credential chain (env vars,
instance profile, service account key, `AZURE_CLIENT_ID` +
`AZURE_CLIENT_SECRET` + `AZURE_TENANT_ID`, etc.). No YAML changes —
just swap the `approval_dir` value.

For MS-shop customers running on Dagster+ Azure hybrid, `abfs://` +
`adlfs` is the natural pairing.
