#!/usr/bin/env python3
"""pull_credit_usage.py — pull Dagster+ credit usage sliced across
deployment × code location × asset × day, and dump to CSV / JSON.

The Dagster+ web UI shows credit usage under Insights, but doesn't
expose a cross-deployment / per-code-location / per-asset download as
one report. This script hits the same GraphQL endpoints the UI does
and merges the results into one table.

Queries verified against real Dagster+ schema (2026-09):

    fullDeployments                    — enumerate org's deployments
    reportingMetricsByDeployment       — org-level: one row per deployment
                                          (uses org endpoint /prod/graphql or
                                          any deployment endpoint)
    reportingMetricsByAsset            — deployment-level: one row per asset
                                          (must be per-deployment endpoint)
    metricTypesForDeployment           — list the metric names available
                                          (`__dagster_dagster_credits`,
                                          `__dagster_execution_time_ms`, …)

The `granularity: DAILY` selector returns:
    timestamps: [epoch, epoch, …]   ← day boundaries
    metrics:
      - entity: <DagsterCloudDeployment | ReportingAsset>
        aggregateValue: <total-over-window>
        values: [<day1>, <day2>, …]  ← one number per timestamp

So daily bucketing comes free — no manual window-splitting needed.

Usage:
    export DAGSTER_CLOUD_API_TOKEN=user:xxxxxx

    # Rollup per deployment (last 30 days, all deployments):
    ./pull_credit_usage.py --org ericthomas-dagster \\
        credits --start 2026-08-10 --end 2026-09-10 --group-by deployment

    # Daily breakdown per deployment × day (14-day trend chart):
    ./pull_credit_usage.py --org ericthomas-dagster \\
        credits --start 2026-08-27 --end 2026-09-10 \\
        --group-by deployment,day --output-csv daily.csv

    # Per-asset with code-location dimension (per-deployment fan-out):
    ./pull_credit_usage.py --org ericthomas-dagster \\
        --deployments prod,staging \\
        credits --start 2026-08-10 --end 2026-09-10 \\
        --group-by deployment,code_location,asset --output-csv assets.csv

    # Everything at once — deployment × code_location × asset × day:
    ./pull_credit_usage.py --org ericthomas-dagster \\
        credits --start 2026-08-27 --end 2026-09-10 \\
        --group-by deployment,code_location,asset,day \\
        --output-csv all.csv

    # See what metric types your Dagster+ has (`__dagster_dagster_credits`,
    # `__dagster_execution_time_ms`, plus custom-Insights metrics):
    ./pull_credit_usage.py --org ericthomas-dagster metric-types

Group-by axes (composable, comma-separated):
    deployment       — one row per deployment
    code_location    — one row per (deployment, code_location)
    asset            — one row per (deployment, code_location, asset_key)
    day              — appended to any of the above (uses granularity: DAILY)

Output columns:
    <axes...>, credits, compute_seconds

Requires: Python 3.8+ / stdlib only. No external deps.
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
from typing import Any, Dict, List, Optional, Tuple


# ── HTTP / GraphQL ──────────────────────────────────────────────────────
def _post_graphql(endpoint: str, token: str, query: str,
                  variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    body = json.dumps({"query": query, "variables": variables or {}}).encode("utf-8")
    req = urllib.request.Request(
        endpoint, data=body, method="POST",
        headers={"Content-Type": "application/json", "Dagster-Cloud-Api-Token": token},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")[:800]
        raise RuntimeError(f"HTTP {e.code} @ {endpoint}: {body_text}") from e
    if payload.get("errors"):
        errs = json.dumps(payload["errors"])[:1500]
        if not payload.get("data"):
            raise RuntimeError(f"GraphQL error @ {endpoint}: {errs}")
        print(f"WARN: partial GraphQL error @ {endpoint}: {errs}", file=sys.stderr)
    return payload.get("data") or {}


def _org_endpoint(org: str) -> str:
    """The 'org endpoint' — actually redirects to /prod/graphql. Use it
    for queries that are org-scoped (deployment enumeration + cross-
    deployment metrics)."""
    return f"https://{org}.dagster.cloud/prod/graphql"


def _deployment_endpoint(org: str, deployment: str) -> str:
    return f"https://{org}.dagster.cloud/{deployment}/graphql"


# ── Queries (verified against Dagster+ 2026-09) ────────────────────────
Q_LIST_DEPLOYMENTS = """
  query ListDeployments {
    fullDeployments {
      deploymentName
      deploymentId
      deploymentType
      deploymentStatus
      isBranchDeployment
    }
  }
"""

Q_METRIC_TYPES = """
  query MetricTypes {
    metricTypesForDeployment {
      ... on MetricTypeList {
        metricTypes {
          metricName displayName unitType category priority visible
        }
      }
      ... on PythonError { message }
      ... on UnauthorizedError { message }
    }
  }
"""

# Per-deployment rollup — org-scoped. Runs at /prod/graphql (or any
# deployment endpoint; the query itself is org-scoped internally).
Q_BY_DEPLOYMENT = """
  query ByDeployment($after: Float!, $before: Float!,
                     $ids: [Int!]!, $metric: String!,
                     $granularity: ReportingMetricsGranularity!) {
    reportingMetricsByDeployment(
      metricsFilter: { deploymentIds: $ids }
      metricsSelector: {
        metricName: $metric
        granularity: $granularity
        aggregationFunction: SUM
        sortTarget: AGGREGATION_VALUE
        sortDirection: DESCENDING
        after: $after
        before: $before
      }
    ) {
      __typename
      ... on ReportingMetrics {
        timestamps
        metrics {
          entity {
            ... on DagsterCloudDeployment { deploymentName deploymentId }
          }
          aggregateValue
          values
        }
      }
      ... on ReportingInputError { message }
      ... on PythonError { message }
      ... on UnauthorizedError { message }
    }
  }
"""

# Per-asset (also carries code_location + repository_name). Must be
# run against each deployment's endpoint (assets are deployment-scoped).
Q_BY_ASSET = """
  query ByAsset($after: Float!, $before: Float!,
                $metric: String!,
                $granularity: ReportingMetricsGranularity!) {
    reportingMetricsByAsset(
      metricsFilter: {}
      metricsSelector: {
        metricName: $metric
        granularity: $granularity
        aggregationFunction: SUM
        sortTarget: AGGREGATION_VALUE
        sortDirection: DESCENDING
        after: $after
        before: $before
      }
    ) {
      __typename
      ... on ReportingMetrics {
        timestamps
        metrics {
          entity {
            ... on ReportingAsset {
              assetKey { path }
              codeLocationName
              repositoryName
              assetGroup
            }
          }
          aggregateValue
          values
        }
      }
      ... on ReportingInputError { message }
      ... on PythonError { message }
      ... on UnauthorizedError { message }
    }
  }
"""


# The two metric names we surface. Verified via metricTypesForDeployment
# on ericthomas-dagster/prod (2026-09-10) — these are stable Dagster+
# built-ins, not per-org custom metrics.
CREDIT_METRIC = "__dagster_dagster_credits"
COMPUTE_METRIC = "__dagster_execution_time_ms"


# ── Deployment enumeration ──────────────────────────────────────────────
def _list_deployments(org: str, token: str,
                      include_branches: bool = False) -> List[Dict[str, Any]]:
    data = _post_graphql(_org_endpoint(org), token, Q_LIST_DEPLOYMENTS)
    deps = data.get("fullDeployments") or []
    return [d for d in deps if include_branches or not d.get("isBranchDeployment")]


# ── Per-deployment metric fetch ─────────────────────────────────────────
def _fetch_by_deployment(
    org: str, token: str, deployment_ids: List[int], metric: str,
    after: float, before: float, granularity: str,
) -> Dict[str, Any]:
    """Returns {timestamps: [...], metrics: [{deployment_name, aggregate,
    values_by_ts: {ts: val}}]}."""
    data = _post_graphql(
        _org_endpoint(org), token, Q_BY_DEPLOYMENT,
        {"after": after, "before": before, "ids": deployment_ids,
         "metric": metric, "granularity": granularity},
    )
    node = data.get("reportingMetricsByDeployment") or {}
    if node.get("__typename") != "ReportingMetrics":
        msg = node.get("message") or json.dumps(node)[:300]
        raise RuntimeError(f"reportingMetricsByDeployment: {msg}")
    ts = node.get("timestamps") or []
    out: List[Dict[str, Any]] = []
    for e in node.get("metrics") or []:
        ent = e.get("entity") or {}
        dname = ent.get("deploymentName")
        if not dname: continue
        vals = e.get("values") or []
        out.append({
            "deployment": dname,
            "aggregate": float(e.get("aggregateValue") or 0.0),
            "values_by_ts": dict(zip((float(t) for t in ts), (float(v or 0.0) for v in vals))),
        })
    return {"timestamps": [float(t) for t in ts], "metrics": out}


# ── Per-asset fetch (per deployment endpoint) ───────────────────────────
def _fetch_by_asset(
    org: str, deployment: str, token: str, metric: str,
    after: float, before: float, granularity: str,
) -> Dict[str, Any]:
    """Returns {timestamps: [...], assets: [{asset_key, code_location,
    aggregate, values_by_ts}]}."""
    data = _post_graphql(
        _deployment_endpoint(org, deployment), token, Q_BY_ASSET,
        {"after": after, "before": before, "metric": metric, "granularity": granularity},
    )
    node = data.get("reportingMetricsByAsset") or {}
    if node.get("__typename") != "ReportingMetrics":
        msg = node.get("message") or json.dumps(node)[:300]
        raise RuntimeError(f"reportingMetricsByAsset [{deployment}]: {msg}")
    ts = node.get("timestamps") or []
    out: List[Dict[str, Any]] = []
    for e in node.get("metrics") or []:
        ent = e.get("entity") or {}
        ak = "/".join((ent.get("assetKey") or {}).get("path") or [])
        cl = ent.get("codeLocationName") or ""
        vals = e.get("values") or []
        out.append({
            "asset_key": ak,
            "code_location": cl,
            "repository": ent.get("repositoryName") or "",
            "aggregate": float(e.get("aggregateValue") or 0.0),
            "values_by_ts": dict(zip((float(t) for t in ts), (float(v or 0.0) for v in vals))),
        })
    return {"timestamps": [float(t) for t in ts], "assets": out}


# ── Aggregation ─────────────────────────────────────────────────────────
def _rows_from_deployment_result(
    metric_result: Dict[str, Any],
    axes: List[str],
) -> List[Dict[str, Any]]:
    """Flatten reportingMetricsByDeployment into row dicts. Only useful
    when neither `code_location` nor `asset` is in the axes."""
    rows: List[Dict[str, Any]] = []
    daily = "day" in axes
    for m in metric_result["metrics"]:
        base = {"deployment": m["deployment"]}
        if not daily:
            rows.append({**base, "value": m["aggregate"]})
        else:
            for ts, val in m["values_by_ts"].items():
                d = _dt.datetime.fromtimestamp(ts, tz=_dt.timezone.utc).strftime("%Y-%m-%d")
                rows.append({**base, "day": d, "value": val})
    return rows


def _rows_from_asset_result(
    deployment: str, asset_result: Dict[str, Any], axes: List[str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    daily = "day" in axes
    for a in asset_result["assets"]:
        base = {
            "deployment": deployment,
            "code_location": a["code_location"],
            "asset_key": a["asset_key"],
        }
        if not daily:
            rows.append({**base, "value": a["aggregate"]})
        else:
            for ts, val in a["values_by_ts"].items():
                d = _dt.datetime.fromtimestamp(ts, tz=_dt.timezone.utc).strftime("%Y-%m-%d")
                rows.append({**base, "day": d, "value": val})
    return rows


def _rollup(rows: List[Dict[str, Any]], axes: List[str]) -> Dict[tuple, float]:
    buckets: Dict[tuple, float] = {}
    for r in rows:
        key = tuple(r.get(a, "") for a in axes)
        buckets[key] = buckets.get(key, 0.0) + float(r.get("value") or 0.0)
    return buckets


# ── Commands ────────────────────────────────────────────────────────────
def cmd_credits(args: argparse.Namespace) -> int:
    token = os.environ.get(args.token_env)
    if not token:
        sys.exit(f"ERROR: env var {args.token_env!r} is empty or unset")

    start_epoch, end_epoch = _parse_date_range(args.start, args.end)
    axes = [a.strip() if a.strip() != "asset" else "asset_key"
            for a in args.group_by.split(",") if a.strip()]
    valid = {"deployment", "code_location", "asset_key", "day"}
    for a in axes:
        if a not in valid:
            sys.exit(f"ERROR: unknown --group-by axis {a!r}. Valid: {sorted(valid | {'asset'})}")

    # Enumerate deployments (unless caller pinned a list)
    if args.deployments:
        wanted = set(args.deployments.split(","))
        all_deps = _list_deployments(args.org, token, include_branches=args.include_branch_deployments)
        deployments = [d for d in all_deps if d["deploymentName"] in wanted]
        missing = wanted - {d["deploymentName"] for d in deployments}
        if missing:
            print(f"WARN: --deployments {sorted(missing)} not found in org", file=sys.stderr)
    else:
        deployments = _list_deployments(args.org, token, include_branches=args.include_branch_deployments)
    print(f"Deployments: {', '.join(d['deploymentName'] for d in deployments)}", file=sys.stderr)

    granularity = "DAILY" if "day" in axes else "MONTHLY"
    if args.dry_run:
        print(f"[DRY RUN] window={args.start}..{args.end}   granularity={granularity}   axes={axes}", file=sys.stderr)
        return 0

    # Two data paths:
    #   1. axes ⊆ {deployment, day}  → single reportingMetricsByDeployment call (all deployments).
    #   2. axes touches {code_location, asset_key} → per-deployment reportingMetricsByAsset fan-out.
    needs_per_asset = ("code_location" in axes) or ("asset_key" in axes)

    credit_rows: List[Dict[str, Any]] = []
    compute_rows: List[Dict[str, Any]] = []

    if not needs_per_asset:
        ids = [d["deploymentId"] for d in deployments]
        credits = _fetch_by_deployment(args.org, token, ids, CREDIT_METRIC, start_epoch, end_epoch, granularity)
        compute = _fetch_by_deployment(args.org, token, ids, COMPUTE_METRIC, start_epoch, end_epoch, granularity)
        credit_rows  = _rows_from_deployment_result(credits, axes)
        compute_rows = _rows_from_deployment_result(compute, axes)
    else:
        for d in deployments:
            dname = d["deploymentName"]
            print(f"  [{dname}] per-asset fetch ...", file=sys.stderr)
            credits = _fetch_by_asset(args.org, dname, token, CREDIT_METRIC, start_epoch, end_epoch, granularity)
            compute = _fetch_by_asset(args.org, dname, token, COMPUTE_METRIC, start_epoch, end_epoch, granularity)
            credit_rows  += _rows_from_asset_result(dname, credits, axes)
            compute_rows += _rows_from_asset_result(dname, compute, axes)

    credit_buckets  = _rollup(credit_rows,  axes)
    compute_buckets = _rollup(compute_rows, axes)
    all_keys = sorted(set(credit_buckets) | set(compute_buckets),
                      key=lambda k: (-credit_buckets.get(k, 0.0), *k))

    columns = axes + ["credits", "compute_seconds"]
    out_rows: List[Dict[str, Any]] = []
    for key in all_keys:
        row: Dict[str, Any] = dict(zip(axes, key))
        row["credits"] = round(credit_buckets.get(key, 0.0), 3)
        row["compute_seconds"] = round(compute_buckets.get(key, 0.0) / 1000.0, 3)  # ms → s
        out_rows.append(row)

    if args.output_json:
        with open(args.output_json, "w") as f:
            json.dump(out_rows, f, indent=2, default=str)
        print(f"→ wrote {args.output_json} ({len(out_rows)} rows)", file=sys.stderr)
    if args.output_csv:
        with open(args.output_csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=columns)
            w.writeheader()
            w.writerows(out_rows)
        print(f"→ wrote {args.output_csv} ({len(out_rows)} rows)", file=sys.stderr)
    if not (args.output_json or args.output_csv):
        w = csv.DictWriter(sys.stdout, fieldnames=columns)
        w.writeheader()
        w.writerows(out_rows)

    if not out_rows:
        print(
            f"\nNo data rows. Common causes:\n"
            f"  - Deployments have no asset runs in [{args.start} .. {args.end}].\n"
            f"  - Custom metrics on rare deployments can lag ~15 min behind ingestion.\n"
            f"  - Try `metric-types` to confirm __dagster_dagster_credits is visible on your org.",
            file=sys.stderr,
        )
    return 0


def cmd_metric_types(args: argparse.Namespace) -> int:
    """List every metric name the current deployment exposes — helpful
    for custom Insights metrics, and to verify `__dagster_dagster_credits`
    exists on your version."""
    token = os.environ.get(args.token_env)
    if not token:
        sys.exit(f"ERROR: env var {args.token_env!r} is empty or unset")
    data = _post_graphql(_org_endpoint(args.org), token, Q_METRIC_TYPES)
    node = (data or {}).get("metricTypesForDeployment") or {}
    if "metricTypes" not in node:
        sys.exit(f"ERROR: {json.dumps(node)[:400]}")
    for m in node["metricTypes"]:
        vis = "" if m.get("visible") else "  (hidden)"
        print(f"  {m['metricName']:50s}  {m.get('displayName',''):40s}  unit={m.get('unitType') or '?':8s}{vis}")
    return 0


def cmd_deployments(args: argparse.Namespace) -> int:
    token = os.environ.get(args.token_env)
    if not token:
        sys.exit(f"ERROR: env var {args.token_env!r} is empty or unset")
    for d in _list_deployments(args.org, token, include_branches=args.include_branch_deployments):
        tag = "  (BRANCH)" if d.get("isBranchDeployment") else ""
        print(f"  {d['deploymentName']:20s}  id={d['deploymentId']}  {d.get('deploymentType')}  status={d.get('deploymentStatus')}{tag}")
    return 0


def _parse_date_range(start: str, end: str) -> Tuple[float, float]:
    def _to_epoch(s: str, end_of_day: bool = False) -> float:
        dt = _dt.datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_dt.timezone.utc)
        if end_of_day and dt.hour == 0 and dt.minute == 0 and dt.second == 0:
            dt = dt + _dt.timedelta(days=1) - _dt.timedelta(seconds=1)
        return dt.timestamp()
    return _to_epoch(start), _to_epoch(end, end_of_day=True)


def main() -> int:
    p = argparse.ArgumentParser(
        prog="pull_credit_usage",
        description="Pull Dagster+ credit usage across deployment × code location × asset × day.",
    )
    p.add_argument("--org", required=True, help="Dagster+ org name (e.g. ericthomas-dagster).")
    p.add_argument("--token-env", default="DAGSTER_CLOUD_API_TOKEN",
                   help="Env var name holding the Dagster+ user API token.")
    p.add_argument("--deployments", default=None,
                   help="Comma-separated deployment names. Default: all in the org.")
    p.add_argument("--include-branch-deployments", action="store_true",
                   help="Include branch deployments (default: only full deployments).")

    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("credits", help="Pull credit usage into CSV / JSON.")
    c.add_argument("--start", required=True, help="Start date (YYYY-MM-DD, inclusive).")
    c.add_argument("--end",   required=True, help="End date   (YYYY-MM-DD, inclusive).")
    c.add_argument("--group-by", default="deployment,code_location,asset",
                   help=("Aggregation axes, comma-separated. Any of: "
                         "deployment, code_location, asset, day. Default: "
                         "deployment,code_location,asset."))
    c.add_argument("--output-csv", default=None)
    c.add_argument("--output-json", default=None)
    c.add_argument("--dry-run", action="store_true")
    c.set_defaults(func=cmd_credits)

    mt = sub.add_parser("metric-types", help="List metric types available on your Dagster+.")
    mt.set_defaults(func=cmd_metric_types)

    d = sub.add_parser("deployments", help="List deployments in the org.")
    d.set_defaults(func=cmd_deployments)

    args = p.parse_args()
    return args.func(args) or 0


if __name__ == "__main__":
    sys.exit(main())
