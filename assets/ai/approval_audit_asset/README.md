# approval_audit_asset

Materialize the full who/when/why history of an approval workflow as a
Dagster DataFrame asset. Governance surface for the approval-gate
components (`HumanApprovalGate`, `SlackApprovalGate`, `TeamsApprovalGate`).

## Type

```
dagster_community_components.ApprovalAuditAssetComponent
```

## Category

`ai` (approval workflows)

## What it does

Reads every JSON approval token in the shared `approval_dir` and emits
one row per token:

| Column | Type | Notes |
|---|---|---|
| `partition_key` | str | Filename stem (e.g. `dagster-io_dagster#30000`) |
| `approved` | bool | |
| `approver` | str | Email / Slack user id / Teams user id |
| `reason` | str | Approver-supplied rationale |
| `feedback` | str | Optional — rejection feedback for re-run loops |
| `decided_at` | str | ISO timestamp — file mtime if not in token |
| `token_file` | str | Absolute path |
| `upstream_asset` | str | Optional — from `upstream_asset` field if present |
| `source` | str | Inferred: `slack` \| `teams` \| `filesystem` \| `external` |

Rows sorted by `decided_at` DESC.

## Minimal example

```yaml
type: dagster_community_components.ApprovalAuditAssetComponent
attributes:
  asset_name: approval_audit
  approval_dir: /tmp/mir_approvals
```

That's it — reads `*.json` in the dir, emits the audit DataFrame on
each materialization.

## Full example

See `example.yaml`.

## Fields

| Name | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | | Dagster asset name for the DataFrame. |
| `approval_dir` | string | yes | | Directory of JSON approval tokens. |
| `recursive` | boolean | | `false` | Walk subdirectories. |
| `token_glob` | string | | `*.json` | Glob pattern. |
| `group_name` | string | | `governance` | |
| `kinds` | array | | `['audit', 'approval', 'governance']` | |
| `tags` | object | | | |
| `owners` | array | | | |
| `description` | string | | | |
| `freshness_max_lag_minutes` | integer | | | Freshness SLA. |
| `freshness_cron` | string | | | Paired with `max_lag_minutes`. |
| `schedule_cron` | string | | | If set, emit a schedule that auto-materializes on this cron. |

## Composition

Point at the same `approval_dir` your approval-gate component uses:

```yaml
# defs/human_approval/defs.yaml
type: dagster_community_components.HumanApprovalGateComponent
attributes:
  asset_name: report_approved
  upstream_asset_key: mir_report
  approval_dir: /tmp/mir_approvals    # ← writes tokens here

# defs/approval_audit/defs.yaml
type: dagster_community_components.ApprovalAuditAssetComponent
attributes:
  asset_name: approval_audit
  approval_dir: /tmp/mir_approvals    # ← reads tokens from here
  schedule_cron: "15 0 * * *"          # daily 00:15 UTC
```

Now every approval decision — whether from Slack, Teams, direct file
drop, or an external webhook — shows up in one DataFrame you can chart
in Insights or export to your GRC system.

## What it doesn't do

- **Doesn't delete or archive tokens.** Read-only. Rotate the dir yourself.
- **Doesn't decode run lineage.** Rows carry a `token_file` path — join to your run history externally if you need the "which run consumed this approval" answer.
- **Doesn't enforce policy.** Audit only. Wire an asset check downstream if you need "fail if any approval > N days old."
