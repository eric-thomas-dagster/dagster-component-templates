# Slack Approval Gate

Human-in-the-loop approval via Slack reactions. Composes with the existing `HumanApprovalGateComponent` — Slack side handles posting + reaction polling + quorum accounting; when quorum is reached, this component writes the same JSON approval token file the existing gate already reads. Everything downstream of the gate is unchanged.

## Why reactions polling (not interactive buttons)

Interactive buttons + slash commands need a public webhook Slack can POST to. That doesn't work in Dagster+ Serverless — containers are short-lived, no fixed URL. Reactions polling is pure outbound HTTPS to Slack's API and works everywhere Dagster runs. Trade-off: 30-60s approval-detection latency instead of instant. Fine for human-in-the-loop.

## What ships in one YAML

Two Dagster definitions from a single `SlackApprovalGateComponent` YAML block:

1. **`{asset_name}_posted` asset** — materialization posts the upstream asset's text to Slack, seeds the message with approve + reject emoji reactions (so voters see the buttons), and stores `message_ts + channel_id + approver_allowlist + timeout + ...` in a sidecar file. Partitioned to match upstream.
2. **`{asset_name}_watcher` sensor** (default_status: RUNNING) — polls Slack every `poll_interval_seconds` for each posted-but-not-tokened partition. Counts allowlisted reactions; on quorum, writes the standard JSON approval token file the existing `HumanApprovalGateComponent` reads.

## Slack bot setup

1. Create an app at <https://api.slack.com/apps> (from scratch)
2. Add scopes under **OAuth & Permissions**:
   - `chat:write` — post the approval message
   - `reactions:read` — poll for votes
   - `reactions:write` — seed the initial approve/reject emoji reactions
3. Install to your workspace, copy the `xoxb-...` bot token
4. Invite the bot to the approval channel: `/invite @your-bot` in Slack
5. Export the token: `export SLACK_BOT_TOKEN=xoxb-...`

## Getting Slack user IDs

Right-click on a user in Slack → **View profile** → **⋮ More** → **Copy member ID**. Or via API: `slack_sdk.WebClient(token).users_lookupByEmail(email=...)`.

## Fields

- **`asset_name`** *(required)* — Base name for the emitted asset + sensor.
- **`upstream_asset_key`** *(required)* — Asset whose text is posted for approval.
- **`approval_dir`** *(required)* — Shared token dir with `HumanApprovalGateComponent`. Sensor writes `<safe_partition_key>.json` here on quorum.
- **`slack_channel`** *(required)* — Channel ID or `#channel-name`.
- **`slack_bot_token_env_var`** *(default `SLACK_BOT_TOKEN`)*.
- **`approve_emoji`** / **`reject_emoji`** *(defaults `white_check_mark` / `x`)*.
- **`ping_users_on_post`** *(optional)* — Slack user IDs to @-mention.
- **`message_template`** *(optional)* — Placeholders: `{upstream_text}`, `{approve_emoji}`, `{reject_emoji}`, `{required_approvers}`, `{n_allowlisted}`, `{partition_key}`.
- **`required_approvers`** *(default 1)* — Quorum size.
- **`approver_allowlist`** *(required)* — Slack user IDs allowed to vote.
- **`timeout_hours`** *(optional)* — Timeout policy trigger.
- **`on_timeout`** *(default `escalate`)* — `escalate` (ping user, keep waiting) | `reject` (auto-fail) | `approve` (auto-pass — use carefully).
- **`escalate_slack_user`** — User ID pinged on escalation.
- **`poll_interval_seconds`** *(default 30)* — Sensor cadence.
- Full partitioning surface (`partition_type` / `partition_start` / `partition_values` / `dynamic_partition_name` / `partition_dimensions`).

## Token shape (written on quorum)

Compatible with `HumanApprovalGateComponent`'s reader:

```json
{
  "approved": true,
  "approver": "U1234ALICE,U5678BOB",
  "reason": "Slack quorum reached: 2/2 approved",
  "timestamp": "2026-08-19T14:32:16Z",
  "source": "slack_approval_gate",
  "slack_message_ts": "1734567890.123456"
}
```

## Composed with HumanApprovalGateComponent

The full HITL pattern is two YAMLs:

```yaml
# 1. Slack-driven approval — posts + polls + writes token
type: dagster_community_components.SlackApprovalGateComponent
attributes:
  asset_name: report_approval_slack
  upstream_asset_key: mir_report
  approval_dir: /tmp/approvals
  slack_channel: "#dagster-approvals"
  required_approvers: 2
  approver_allowlist: [U1234ALICE, U5678BOB, U9012CAROL]
  partition_type: dynamic
  dynamic_partition_name: mir_investigations

# 2. Existing gate — consumes the token, blocks downstream via asset check
type: dagster_community_components.HumanApprovalGateComponent
attributes:
  asset_name: report_approval
  upstream_asset_key: mir_report
  approval_dir: /tmp/approvals             # SAME DIR
  partition_type: dynamic                  # SAME PARTITIONS
  dynamic_partition_name: mir_investigations
```

## Rate-limit considerations

Slack's `reactions.get` is Tier 3 (~50 req/min). At 30s cadence per partition:
- ≤50 pending partitions → fine (100 polls/min = 2 polls/partition/min)
- Approaching 100 → bump `poll_interval_seconds` to 60-120
- Sensor is per-instance (one bot). If you have many gates in one deployment sharing a bot, raise the interval.

## What this does NOT do

- **Instant approval UX.** Reactions polling is 30-60s. If you need instant, use Socket Mode (requires Hybrid deploy with a long-running agent) — future component.
- **Threaded discussion feedback.** Approvers can discuss in the same thread but the discussion doesn't flow back to Dagster. The `reason` field just gets `<Slack-quorum>`.
- **Revise-and-retry on rejection.** Rejection is terminal — the gate's asset check fails ERROR and downstream stays blocked. See `rejection_feedback_loop` for the revise pattern.

## Cloud storage — `approval_dir` accepts URIs, not just local paths

Dagster+ Serverless containers don't share a local filesystem across
runs or code locations, so a production Slack-approval deploy needs
cloud object storage for the sidecar + token files. `approval_dir` accepts:

| Shape | Example | Requires |
|---|---|---|
| Local path | `/tmp/approvals` | nothing (pathlib fast-path) |
| S3 | `s3://my-bucket/approvals` | `pip install fsspec s3fs` |
| GCS | `gs://my-bucket/approvals` | `pip install fsspec gcsfs` |
| Azure ADLS | `abfs://container@account.dfs.core.windows.net/approvals` | `pip install fsspec adlfs` |

Auth follows the driver's default credential chain (AWS env vars /
instance profile, GCP service account key, Azure SP creds). No YAML
changes — just swap the `approval_dir` value. The sidecar
`.slack_state.json` and the eventual approval token `.json` both live
under whatever URI `approval_dir` points at.
