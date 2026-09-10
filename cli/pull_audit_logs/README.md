# `pull_audit_logs.py`

Pull Dagster+ audit log entries by date range + optional filters, and
dump to CSV / JSON.

The Dagster+ web UI shows audit logs under Cloud Settings but doesn't
expose a bulk download or SIEM-friendly export. This CLI hits the same
GraphQL endpoint the UI does (`auditLog.auditLogEntries`) and
paginates through the full result set.

- **Script:** [`./pull_audit_logs.py`](./pull_audit_logs.py)
- **Requires:** Python 3.8+ (stdlib only — no external deps) + a Dagster+ user API token with **org-admin scope**
- **Plan:** Dagster+ **Pro** (audit logs are a Pro feature)

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/cli/pull_audit_logs/pull_audit_logs.py \
    -o pull_audit_logs.py
chmod +x pull_audit_logs.py

export DAGSTER_CLOUD_API_TOKEN=user:xxxxxxxx
```

## Two subcommands

### `deployments` — list deployments in the org

Token sanity check.

```bash
./pull_audit_logs.py --org acme deployments
```

### `pull` — the audit log fetch

```bash
# Everything in a date range, all deployments, CSV to stdout:
./pull_audit_logs.py --org acme \
    pull --start 2026-09-01 --end 2026-09-10

# Scoped to prod, one event type, JSON out:
./pull_audit_logs.py --org acme \
    pull --start 2026-09-01 --end 2026-09-10 \
    --deployments prod \
    --event-types UPDATE_CODE_LOCATION \
    --output-json audit.json

# All events for a specific user across 9 months:
./pull_audit_logs.py --org acme \
    pull --start 2026-01-01 --end 2026-09-30 \
    --user-emails alice@acme.com \
    --output-csv alice_9mo.csv
```

## Options

### Top-level (all subcommands)

| Flag | Required | Default | Description |
|---|---|---|---|
| `--org` | yes | — | Dagster+ org name (e.g. `acme` for `acme.dagster.cloud`) |
| `--token-env` | | `DAGSTER_CLOUD_API_TOKEN` | Env var name holding the user API token (org-admin scope) |
| `--include-branch-deployments` | | off | Include branch deployments in `deployments` output |

### `pull` subcommand

| Flag | Required | Default | Description |
|---|---|---|---|
| `--start` | yes | — | Start date (`YYYY-MM-DD`, inclusive) |
| `--end` | yes | — | End date (`YYYY-MM-DD`, inclusive) |
| `--deployments` | | (all) | Comma-separated deployment names to filter on |
| `--user-emails` | | (all) | Comma-separated actor emails to filter on |
| `--event-types` | | (all) | Comma-separated `AuditLogEventType` enum values (e.g. `USER_LOGIN,CREATE_CODE_LOCATION`). Unknown values are rejected server-side. |
| `--output-csv <path>` | | (stdout) | Write CSV to this path instead of stdout |
| `--output-json <path>` | | | Write JSON to this path (JSON array of row objects) |
| `--dry-run` | | off | Print the assembled filter object without executing |
| `--page-size` | | `500` | Entries per API call. CLI walks the cursor until fewer than this come back. |

## Output columns

CSV / stdout:

| Column | Description |
|---|---|
| `id` | ULID event id — also serves as pagination cursor |
| `timestamp` | Epoch seconds (float, sub-second precision) |
| `timestamp_iso` | Same timestamp as UTC ISO 8601 |
| `event_type` | `AuditLogEventType` enum value |
| `deployment` | Deployment the event was scoped to |
| `actor` | Convenience column: `authorUserEmail` if present, else `token:<id>`, else `system` |
| `author_user_email` | Raw actor email (blank for machine actors) |
| `author_agent_token_id` | Raw agent-token id (blank for user actors) |
| `event_metadata` | JSON payload — encoded as single-line JSON string in CSV, nested object in JSON output |

`event_metadata` shape varies by `event_type` (e.g. `CREATE_CODE_LOCATION` events carry `code_location_name`, `image`, `git.commit_hash`, etc.). Handle it as free-form JSON downstream.

## Output destinations

Three, pick any:

- **stdout** (default) — CSV with headers.
- **`--output-csv <path>`** — CSV file with headers.
- **`--output-json <path>`** — JSON array of row objects (event_metadata kept as nested objects).

## Common failure modes

| Symptom | Cause | Fix |
|---|---|---|
| `HTTP 401` (HTML error page) | Missing `operationName` in request body, or token lacks org-admin scope, or org isn't on Pro | The CLI always sends `operationName`, so this points at auth. Verify the token is org-admin and the org is on Pro. |
| `HTTP 500` with `PythonError: Internal Server Error` | Server bug on the unfiltered-with-limit path | The CLI always sends at least the date-range filter, so you shouldn't hit this. If you do, narrow `--start`/`--end`. |
| `Value '<name>' does not exist in 'AuditLogEventType' enum` | Typo in `--event-types` | Cross-check the value against the Dagster+ UI (Cloud Settings → Audit Log filters) — the enum is authoritative on the server. |
| `Float cannot represent non numeric value: '<iso-string>'` | Wouldn't happen through this CLI (dates are pre-converted); shows up if you edit the query manually | `afterDatetime`/`beforeDatetime` are Float epochs, not ISO strings |

## Sharing with customers

Self-contained, stdlib only — safe to copy directly to a customer environment. Two pre-flight requirements to flag when handing this over:

1. **Token must be org-admin.** User API tokens without org-admin scope get an HTML 401 from the edge (not a GraphQL error) — the error looks like a generic auth failure.
2. **Org must be on Dagster+ Pro.** The audit log resolver returns HTTP 500 on Standard/Free tenants.
