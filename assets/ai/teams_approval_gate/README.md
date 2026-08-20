# teams_approval_gate

Microsoft Teams HITL approval gate. Slack analog for `SlackApprovalGate`
— same asset + sensor shape, same JSON token format, composes with
`HumanApprovalGateComponent` via the shared `approval_dir` (drop-in swap
of Slack for Teams).

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

## Auth (Azure AD app registration)

You need an Azure AD app registration with **application permissions**
(client-credentials flow, tenant admin consent required):

- `ChannelMessage.Read.All` — poll reactions / replies
- `ChannelMessage.Send` — post approval + escalation messages

Register at:

> Azure Portal → Azure Active Directory → App registrations → New

Grant application permissions + click **"Grant admin consent"**. Copy:
- **Tenant ID** (from Overview page)
- **Client ID** (from Overview page)
- **Client Secret** (create under Certificates & secrets)

Export as:

```bash
export TEAMS_TENANT_ID=<tenant-guid>
export TEAMS_CLIENT_ID=<client-guid>
export TEAMS_CLIENT_SECRET=<secret-value>
```

## Minimal example

```yaml
type: dagster_community_components.TeamsApprovalGateComponent
attributes:
  asset_name: report_approval_teams
  upstream_asset_key: mir_report
  approval_dir: /tmp/mir_approvals

  teams_team_id:    "19:abc123@thread.tacv2"
  teams_channel_id: "19:xyz789@thread.tacv2"

  approver_allowlist:
    - "aad-user-id-1"
    - "aad-user-id-2"
```

## Full example

See `example.yaml`.

## Fields

| Name | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | | |
| `upstream_asset_key` | string | yes | | |
| `approval_dir` | string | yes | | Shared with HumanApprovalGateComponent. |
| `teams_tenant_id_env_var` | string | | `TEAMS_TENANT_ID` | |
| `teams_client_id_env_var` | string | | `TEAMS_CLIENT_ID` | |
| `teams_client_secret_env_var` | string | | `TEAMS_CLIENT_SECRET` | |
| `teams_team_id` | string | yes | | |
| `teams_channel_id` | string | yes | | |
| `mode` | enum | | `reactions` | `reactions` or `reply` |
| `approve_reaction` | enum | | `like` | Teams built-in emoji. |
| `reject_reaction` | enum | | `angry` | |
| `approve_reply_keyword` | string | | `APPROVE` | |
| `reject_reply_keyword` | string | | `REJECT` | |
| `message_template` | string | | | Optional custom HTML template. |
| `required_approvers` | integer | | `1` | Quorum size. |
| `approver_allowlist` | array | yes | | Azure AD object IDs (GUIDs). |
| `timeout_hours` | number | | | Optional; applies `on_timeout` policy. |
| `on_timeout` | enum | | `escalate` | `escalate` / `reject` / `approve` |
| `escalate_teams_user` | string | | | Azure AD object ID pinged on escalation. |
| `poll_interval_seconds` | integer | | `60` | Sensor cadence. |

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

Dagster+ Serverless containers don't share a local filesystem across
runs or code locations, so a production Teams-approval deploy needs
cloud object storage for the token directory. `approval_dir` accepts:

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
