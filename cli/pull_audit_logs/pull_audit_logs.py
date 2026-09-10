#!/usr/bin/env python3
"""pull_audit_logs.py — pull Dagster+ audit log entries by date range +
optional filters (deployment, user, event type), and dump to CSV / JSON.

The Dagster+ web UI shows audit logs under Cloud Settings but doesn't
expose a bulk download or SIEM-friendly export. This CLI hits the
`auditLog.auditLogEntries` GraphQL endpoint the UI does and paginates
through the full result set.

Verified against the real Dagster+ schema (2026-09):

    fullDeployments                    — enumerate org's deployments
    auditLog.auditLogEntries(          — the paginated log entries
        limit, cursor, filters)

Filter shape (`AuditLogFilters`):

    eventTypes:      [AuditLogEventType!]   — enum values (see below)
    userEmails:      [String!]              — actor emails
    deploymentNames: [String!]              — deployment names
    afterDatetime:   Float                  — epoch seconds (inclusive)
    beforeDatetime:  Float                  — epoch seconds (inclusive)

Pagination is cursor-based: pass the last entry's `id` as `cursor` to
get the next page. This CLI walks the cursor until fewer than
`--page-size` entries come back.

Usage:
    export DAGSTER_CLOUD_API_TOKEN=user:xxxxxx

    # List deployments (token sanity check):
    ./pull_audit_logs.py --org acme deployments

    # Everything in a date range, all deployments, CSV to stdout:
    ./pull_audit_logs.py --org acme \\
        pull --start 2026-09-01 --end 2026-09-10

    # Scoped to prod, one event type, JSON out:
    ./pull_audit_logs.py --org acme \\
        pull --start 2026-09-01 --end 2026-09-10 \\
        --deployments prod \\
        --event-types UPDATE_CODE_LOCATION \\
        --output-json audit.json

    # Everything since 2026-01-01 for a specific user:
    ./pull_audit_logs.py --org acme \\
        pull --start 2026-01-01 --end 2026-09-30 \\
        --user-emails alice@acme.com \\
        --output-csv alice_9mo.csv

Output columns (CSV):
    id, timestamp, timestamp_iso, event_type, deployment, actor,
    author_user_email, author_agent_token_id, event_metadata

`actor` is a convenience column: user email if present, else agent
token id, else "system". `event_metadata` is the raw JSON payload
encoded as a single-line JSON string in CSV (kept as a nested object
in JSON output).

Requires: Python 3.8+ / stdlib only. No external deps + a Dagster+
user API token with org-admin scope. Audit logs are a Dagster+ Pro
feature.

Notes (verified 2026-09-10):

  - Requests must include `operationName` in the POST body — the edge
    rejects bare queries against the audit log field with an HTML 401.
    The CLI sets `operationName: "AuditLog"` on every call.
  - `auditLogEntries(limit: N)` **without** any filter returns HTTP 500
    (server bug: the unfiltered-with-limit path). This CLI always
    passes at least the date-range filter (`afterDatetime` /
    `beforeDatetime`), so you don't hit the bug in practice.
  - `afterDatetime` / `beforeDatetime` are Float epoch seconds, NOT
    ISO strings — the schema types them as Float. The CLI converts
    `--start` / `--end` YYYY-MM-DD dates for you.
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional


# ── HTTP / GraphQL ──────────────────────────────────────────────────────
def _post_graphql(endpoint: str, token: str, query: str,
                  operation_name: str,
                  variables: Optional[Dict[str, Any]] = None,
                  max_retries: int = 3) -> Dict[str, Any]:
    """POST with automatic retry on 5xx. `operation_name` is REQUIRED on
    audit log requests — the edge rejects unnamed queries against the
    auditLog field with an HTML 401."""
    import time as _time
    body = json.dumps({
        "operationName": operation_name,
        "query": query,
        "variables": variables or {},
    }).encode("utf-8")
    req = urllib.request.Request(
        endpoint, data=body, method="POST",
        headers={"Content-Type": "application/json", "Dagster-Cloud-Api-Token": token},
    )
    last_err: Optional[str] = None
    for attempt in range(max_retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body_text = e.read().decode("utf-8", errors="replace")[:800]
            last_err = f"HTTP {e.code} @ {endpoint}: {body_text}"
            if e.code >= 500 and attempt < max_retries:
                _time.sleep(1 * (3 ** attempt))
                continue
            raise RuntimeError(last_err) from e
        errs = payload.get("errors")
        if errs and attempt < max_retries:
            last_err = json.dumps(errs)[:400]
            _time.sleep(1 * (3 ** attempt))
            continue
        if errs and not payload.get("data"):
            raise RuntimeError(f"GraphQL error @ {endpoint}: {json.dumps(errs)[:1500]}")
        if errs:
            print(f"WARN: partial GraphQL error @ {endpoint}: {json.dumps(errs)[:1500]}", file=sys.stderr)
        _time.sleep(0.05)   # gentle rate limit
        return payload.get("data") or {}
    raise RuntimeError(f"exhausted retries @ {endpoint}: {last_err}")


def _org_endpoint(org: str) -> str:
    """The 'org endpoint' — actually redirects to /prod/graphql. Audit
    logs are org-wide and any deployment's /graphql serves them."""
    return f"https://{org}.dagster.cloud/prod/graphql"


# ── Queries (verified against Dagster+ 2026-09) ────────────────────────
Q_LIST_DEPLOYMENTS = """
  query Deployments {
    fullDeployments {
      deploymentName
      deploymentId
      deploymentType
      deploymentStatus
      isBranchDeployment
    }
  }
"""

Q_AUDIT = """
  query AuditLog($limit: Int, $cursor: String, $filters: AuditLogFilters) {
    auditLog {
      auditLogEntries(limit: $limit, cursor: $cursor, filters: $filters) {
        id
        eventType
        authorUserEmail
        authorAgentTokenId
        timestamp
        deploymentName
        eventMetadata
      }
    }
  }
"""


# ── Deployment enumeration ──────────────────────────────────────────────
def _list_deployments(org: str, token: str,
                      include_branches: bool = False) -> List[Dict[str, Any]]:
    data = _post_graphql(_org_endpoint(org), token, Q_LIST_DEPLOYMENTS, "Deployments")
    deps = data.get("fullDeployments") or []
    return [d for d in deps if include_branches or not d.get("isBranchDeployment")]


# ── Audit log fetch (paginated) ────────────────────────────────────────
def _fetch_audit_entries(
    org: str, token: str, filters: Dict[str, Any], page_size: int,
) -> List[Dict[str, Any]]:
    """Walk the cursor until fewer than `page_size` entries come back
    (or an empty page). Returns all matching entries."""
    endpoint = _org_endpoint(org)
    all_entries: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    page = 0
    while True:
        page += 1
        vars_: Dict[str, Any] = {"limit": page_size, "filters": filters}
        if cursor is not None:
            vars_["cursor"] = cursor
        data = _post_graphql(endpoint, token, Q_AUDIT, "AuditLog", vars_)
        entries = ((data or {}).get("auditLog") or {}).get("auditLogEntries") or []
        print(f"  page {page}: fetched {len(entries)} entries (cursor={cursor[:16] + '…' if cursor else 'None'})", file=sys.stderr)
        all_entries.extend(entries)
        if len(entries) < page_size:
            break
        cursor = entries[-1]["id"]
    return all_entries


# ── Filter assembly ────────────────────────────────────────────────────
def _build_filters(args: argparse.Namespace) -> Dict[str, Any]:
    filters: Dict[str, Any] = {}
    # Always include the date range — the server-side unfiltered-with-limit
    # path returns HTTP 500, so we need at least one filter.
    filters["afterDatetime"] = _to_epoch(args.start, end_of_day=False)
    filters["beforeDatetime"] = _to_epoch(args.end, end_of_day=True)
    if args.deployments:
        filters["deploymentNames"] = [d.strip() for d in args.deployments.split(",") if d.strip()]
    if args.user_emails:
        filters["userEmails"] = [e.strip() for e in args.user_emails.split(",") if e.strip()]
    if args.event_types:
        filters["eventTypes"] = [t.strip() for t in args.event_types.split(",") if t.strip()]
    return filters


# ── Row shaping ────────────────────────────────────────────────────────
def _row_from_entry(e: Dict[str, Any]) -> Dict[str, Any]:
    ts = float(e.get("timestamp") or 0.0)
    iso = _dt.datetime.fromtimestamp(ts, tz=_dt.timezone.utc).isoformat() if ts else ""
    author_email = e.get("authorUserEmail")
    author_token = e.get("authorAgentTokenId")
    actor = author_email or (f"token:{author_token}" if author_token else "system")
    return {
        "id": e.get("id"),
        "timestamp": ts,
        "timestamp_iso": iso,
        "event_type": e.get("eventType"),
        "deployment": e.get("deploymentName"),
        "actor": actor,
        "author_user_email": author_email or "",
        "author_agent_token_id": author_token or "",
        "event_metadata": e.get("eventMetadata") or {},
    }


# ── Commands ────────────────────────────────────────────────────────────
def cmd_pull(args: argparse.Namespace) -> int:
    token = os.environ.get(args.token_env)
    if not token:
        sys.exit(f"ERROR: env var {args.token_env!r} is empty or unset")

    filters = _build_filters(args)
    print(
        f"Pulling audit log: {args.start}..{args.end}  "
        f"deployments={filters.get('deploymentNames') or '(all)'}  "
        f"users={filters.get('userEmails') or '(all)'}  "
        f"event_types={filters.get('eventTypes') or '(all)'}",
        file=sys.stderr,
    )
    if args.dry_run:
        print(f"[DRY RUN] filters={json.dumps(filters, default=str)}", file=sys.stderr)
        return 0

    entries = _fetch_audit_entries(args.org, token, filters, args.page_size)
    rows = [_row_from_entry(e) for e in entries]
    print(f"→ {len(rows)} total entries", file=sys.stderr)

    columns = [
        "id", "timestamp", "timestamp_iso", "event_type", "deployment",
        "actor", "author_user_email", "author_agent_token_id", "event_metadata",
    ]

    if args.output_json:
        with open(args.output_json, "w") as f:
            json.dump(rows, f, indent=2, default=str)
        print(f"→ wrote {args.output_json} ({len(rows)} rows)", file=sys.stderr)
    if args.output_csv:
        with open(args.output_csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=columns)
            w.writeheader()
            for r in rows:
                r = dict(r)
                r["event_metadata"] = json.dumps(r["event_metadata"], separators=(",", ":"), default=str)
                w.writerow(r)
        print(f"→ wrote {args.output_csv} ({len(rows)} rows)", file=sys.stderr)
    if not (args.output_json or args.output_csv):
        w = csv.DictWriter(sys.stdout, fieldnames=columns)
        w.writeheader()
        for r in rows:
            r = dict(r)
            r["event_metadata"] = json.dumps(r["event_metadata"], separators=(",", ":"), default=str)
            w.writerow(r)

    if not rows:
        print(
            "\nNo entries. Common causes:\n"
            "  - No audit events in the window matching your filters.\n"
            "  - Token lacks org-admin scope (audit logs are admin-only).\n"
            "  - Org isn't on Dagster+ Pro (audit logs are a Pro feature).",
            file=sys.stderr,
        )
    return 0


def cmd_deployments(args: argparse.Namespace) -> int:
    token = os.environ.get(args.token_env)
    if not token:
        sys.exit(f"ERROR: env var {args.token_env!r} is empty or unset")
    for d in _list_deployments(args.org, token, include_branches=args.include_branch_deployments):
        tag = "  (BRANCH)" if d.get("isBranchDeployment") else ""
        print(f"  {d['deploymentName']:20s}  id={d['deploymentId']}  {d.get('deploymentType')}  status={d.get('deploymentStatus')}{tag}")
    return 0


def _to_epoch(s: str, end_of_day: bool = False) -> float:
    dt = _dt.datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    if end_of_day and dt.hour == 0 and dt.minute == 0 and dt.second == 0:
        dt = dt + _dt.timedelta(days=1) - _dt.timedelta(seconds=1)
    return dt.timestamp()


def main() -> int:
    p = argparse.ArgumentParser(
        prog="pull_audit_logs",
        description="Pull Dagster+ audit log entries by date range + optional filters.",
    )
    p.add_argument("--org", required=True, help="Dagster+ org name (e.g. ericthomas-dagster).")
    p.add_argument("--token-env", default="DAGSTER_CLOUD_API_TOKEN",
                   help="Env var name holding the Dagster+ user API token (org-admin scope).")
    p.add_argument("--include-branch-deployments", action="store_true",
                   help="Include branch deployments in `deployments` output (default: only full deployments).")

    sub = p.add_subparsers(dest="cmd", required=True)

    pu = sub.add_parser("pull", help="Pull audit log entries into CSV / JSON.")
    pu.add_argument("--start", required=True, help="Start date (YYYY-MM-DD, inclusive).")
    pu.add_argument("--end",   required=True, help="End date   (YYYY-MM-DD, inclusive).")
    pu.add_argument("--deployments", default=None,
                    help="Comma-separated deployment names to filter on. Default: all.")
    pu.add_argument("--user-emails", default=None,
                    help="Comma-separated actor emails to filter on. Default: all.")
    pu.add_argument("--event-types", default=None,
                    help=("Comma-separated event type names to filter on (e.g. "
                          "USER_LOGIN,CREATE_CODE_LOCATION,UPDATE_CODE_LOCATION). "
                          "Values are Dagster+'s AuditLogEventType enum; unknown "
                          "values are rejected server-side. Default: all."))
    pu.add_argument("--output-csv", default=None)
    pu.add_argument("--output-json", default=None)
    pu.add_argument("--dry-run", action="store_true",
                    help="Print the assembled filter object without executing.")
    pu.add_argument("--page-size", type=int, default=500,
                    help="Entries per API call. CLI walks the cursor until "
                         "fewer than this come back. Default: 500.")
    pu.set_defaults(func=cmd_pull)

    d = sub.add_parser("deployments", help="List deployments in the org (token sanity check).")
    d.set_defaults(func=cmd_deployments)

    args = p.parse_args()
    return args.func(args) or 0


if __name__ == "__main__":
    sys.exit(main())
