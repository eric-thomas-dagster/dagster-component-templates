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

Notes on Dagster+ Insights internals (verified 2026-09-10):

  - The Insights API caps a single query window at **120 days**.
    Longer ranges are auto-split into 120-day chunks.
  - There are TWO metric stores: **VICTORIA_METRICS** (recent data
    with per-asset granularity, retention ≈ 6 months) and
    **POSTGRES** (long-tail history, back to org origination).
    Default `--store BOTH` queries both and unions.
  - `reportingMetricsByAsset.metricsFilter.codeLocations` is
    supported by POSTGRES but returns HTTP 500 on VM — the script
    never uses that filter. Code-location is joined client-side
    via `assetNodes`.
  - `reportingMetricsByDeployment` returns
    `ReportingInputError: Branch deployment metrics are not yet
    supported in VictoriaMetrics` on VM regardless of whether the
    IDs are branches. Script uses per-asset queries and rolls up
    deployment totals client-side instead.
  - Default `metricsFilter.limit` is 10 — always pass an explicit
    high value.
  - VM 500s with `PythonError: Internal Server Error (Trace ID:
    …)` are the "no data in this window" signal; the script skips
    them without retrying so POSTGRES can pick up the slack.
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
                  variables: Optional[Dict[str, Any]] = None,
                  max_retries: int = 3) -> Dict[str, Any]:
    """POST with automatic retry on 5xx / transient GraphQL PythonError.
    Backs off exponentially: 1s, 3s, 9s. Rate-limits by sleeping 100ms
    between successful calls to avoid overwhelming the Insights backend
    on large fan-outs."""
    import time as _time
    body = json.dumps({"query": query, "variables": variables or {}}).encode("utf-8")
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
            # Dagster+ Insights returns HTTP 500 with `PythonError:
            # Internal Server Error` when a query window has no data
            # in VictoriaMetrics (common for older date ranges past
            # VM's ~6-month retention). That's NOT a transient error —
            # retrying won't help. Distinguish from real 500s (gateway
            # timeouts, etc.) which usually don't have the trace-id
            # marker.
            if e.code == 500 and "Internal Server Error" in body_text and "Trace ID" in body_text:
                raise RuntimeError(last_err) from e   # skip retries
            if e.code >= 500 and attempt < max_retries:
                _time.sleep(1 * (3 ** attempt))
                continue
            raise RuntimeError(last_err) from e
        # PythonError with 200 status — retry once for actual transient
        # errors; the VM-empty-window case comes back as HTTP 500 (handled
        # above), so anything in this branch is worth retrying.
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

Q_ASSET_LOCATIONS = """
  query AssetLocations {
    assetNodes {
      assetKey { path }
      repository {
        name
        location { name }
      }
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

# Per-asset — the single query we run for every axis combination.
# `limit` defaults to 10 server-side, which silently truncates any org
# with more than 10 credits-consuming assets in the window — always
# pass an explicit high limit.
# We fan out one call per deployment (assets are deployment-scoped),
# then aggregate client-side.
#
# `metricsStoreType` MATTERS: Dagster+ backends the reporting metrics
# by either POSTGRES or VICTORIA_METRICS. As of 2026-09, real Dagster+
# tenants (including SaaS) route credits + compute to VICTORIA_METRICS
# — omitting the store type OR passing POSTGRES returns empty results.
# Overridable via --store.
#
# NOTE: `reportingMetricsByDeployment` also exists but returns
# `ReportingInputError: Branch deployment metrics are not yet supported
# in VictoriaMetrics` on live VM tenants, so we don't use it — the
# per-deployment rollup is computed by summing per-asset rows client-
# side. Both `codeLocationName` and `repositoryName` on the
# reportingMetricsByAsset response are ALSO empty on VM (VM isn't
# indexed by code_location), so when the user asks for the
# `code_location` axis we hit `assetNodes` separately for the
# {asset_key: code_location} mapping and join.
#
# Per-asset (also carries code_location + repository_name). Must be
# run against each deployment's endpoint (assets are deployment-scoped).
# Same metricsStoreType story — VICTORIA_METRICS is the live data path.
Q_BY_ASSET = """
  query ByAsset($after: Float!, $before: Float!,
                $metric: String!,
                $granularity: ReportingMetricsGranularity!,
                $store: MetricsStoreType!,
                $limit: Int!) {
    reportingMetricsByAsset(
      metricsFilter: { limit: $limit }
      metricsStoreType: $store
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


# ── Asset → code_location mapping (per deployment endpoint) ────────────
#
# VictoriaMetrics-backed reportingMetricsByAsset returns EMPTY strings
# for codeLocationName / repositoryName / assetGroup — the store isn't
# indexed by those dimensions. To get the code_location for each asset
# we hit `assetNodes` on the same endpoint and build a client-side map.
def _fetch_asset_to_location(org: str, deployment: str, token: str) -> Dict[str, str]:
    """Returns {asset_key_slash_joined: code_location_name}."""
    data = _post_graphql(_deployment_endpoint(org, deployment), token, Q_ASSET_LOCATIONS)
    out: Dict[str, str] = {}
    for n in (data.get("assetNodes") or []):
        ak = "/".join((n.get("assetKey") or {}).get("path") or [])
        loc = ((n.get("repository") or {}).get("location") or {}).get("name") or ""
        if ak:
            out[ak] = loc
    return out


# ── Per-asset fetch (per deployment endpoint) ───────────────────────────
def _fetch_by_asset(
    org: str, deployment: str, token: str, metric: str,
    after: float, before: float, granularity: str, store: str,
    limit: int = 5000,
) -> Dict[str, Any]:
    """Returns {timestamps: [...], assets: [{asset_key, code_location,
    aggregate, values_by_ts}]}."""
    data = _post_graphql(
        _deployment_endpoint(org, deployment), token, Q_BY_ASSET,
        {"after": after, "before": before, "metric": metric,
         "granularity": granularity, "store": store, "limit": limit},
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

    # Route: everything goes through per-asset queries, then aggregates
    # client-side. Rationale:
    #   - VictoriaMetrics-backed `reportingMetricsByDeployment` returns
    #     `ReportingInputError: Branch deployment metrics are not yet
    #     supported in VictoriaMetrics` — the query is effectively
    #     broken on live SaaS tenants as of 2026-09.
    #   - `reportingMetricsByAsset` returns EMPTY strings for
    #     codeLocationName / repositoryName / assetGroup on VM. We fetch
    #     the {asset_key: code_location} mapping separately via
    #     `assetNodes` on the same deployment endpoint and join.
    credit_rows: List[Dict[str, Any]] = []
    compute_rows: List[Dict[str, Any]] = []
    need_locations = "code_location" in axes

    # Which underlying stores to query. VM caps at ~6 months retention;
    # POSTGRES holds long-tail history. When the user picks `BOTH`
    # (default), we hit both and union the rows — the two stores hold
    # DIFFERENT data (POSTGRES is often historical / decommissioned
    # code locations, VM is current), so summing across is correct
    # rather than double-counting.
    stores = ("VICTORIA_METRICS", "POSTGRES") if args.store == "BOTH" else (args.store,)

    # Dagster+ caps a single reportingMetrics query at 120 days.
    # Auto-chunk anything longer.
    MAX_WINDOW_SEC = 120 * 86400
    time_chunks: List[Tuple[float, float]] = []
    _t = start_epoch
    while _t < end_epoch:
        chunk_end = min(_t + MAX_WINDOW_SEC, end_epoch)
        time_chunks.append((_t, chunk_end))
        _t = chunk_end
    if len(time_chunks) > 1:
        print(
            f"Window > 120d; splitting into {len(time_chunks)} chunk(s) "
            f"(Dagster+ reporting queries cap at 120 days each).",
            file=sys.stderr,
        )

    def _run_fetches(dep_name: str, store_name: str, loc_map: Dict[str, str]) -> None:
        """Fans out one metric fetch per time chunk. Appends to the
        enclosing credit_rows / compute_rows.

        NOTE: no per-code-location pagination — the VictoriaMetrics
        backend returns HTTP 500 whenever the `codeLocations` filter
        is set. Only POSTGRES supports it, and POSTGRES's aggregate
        already fits comfortably under `--limit` at typical org
        sizes. To handle >5000 assets in a single (deployment × chunk),
        split the date range with `--start/--end` and re-run.
        """
        for (t_start, t_end) in time_chunks:
            chunk_desc = f"{_dt.datetime.fromtimestamp(t_start, tz=_dt.timezone.utc).date()}..{_dt.datetime.fromtimestamp(t_end, tz=_dt.timezone.utc).date()}"
            try:
                credits = _fetch_by_asset(args.org, dep_name, token, CREDIT_METRIC, t_start, t_end, granularity, store_name, args.limit)
                compute = _fetch_by_asset(args.org, dep_name, token, COMPUTE_METRIC, t_start, t_end, granularity, store_name, args.limit)
            except RuntimeError as e:
                # Common: VM 500s when window has no data past retention (~6mo).
                # Skip quietly and let POSTGRES pick up the slack.
                print(f"    [{dep_name}] store={store_name} {chunk_desc} skipped ({e[:120] if isinstance(e, str) else str(e)[:120]})", file=sys.stderr)
                continue
            n_credit = len(credits["assets"])
            n_compute = len(compute["assets"])
            if n_credit >= args.limit or n_compute >= args.limit:
                print(
                    f"    [{dep_name}] WARN: store={store_name} {chunk_desc} "
                    f"hit --limit={args.limit} (credits={n_credit}, compute={n_compute}). "
                    f"Results may be truncated. Raise --limit or split the date range.",
                    file=sys.stderr,
                )
            if need_locations:
                for a in credits["assets"] + compute["assets"]:
                    if not a.get("code_location"):
                        a["code_location"] = loc_map.get(a["asset_key"], "")
            credit_rows.extend(_rows_from_asset_result(dep_name, credits, axes))
            compute_rows.extend(_rows_from_asset_result(dep_name, compute, axes))

    for d in deployments:
        dname = d["deploymentName"]
        # Fetch assetNodes once per deployment — reused for both the
        # code_location join AND the pagination batch list.
        loc_map: Dict[str, str] = {}
        try:
            loc_map = _fetch_asset_to_location(args.org, dname, token)
        except RuntimeError as e:
            print(f"    [{dname}] WARN: code_location fetch failed: {e}", file=sys.stderr)

        distinct_locs = sorted({v for v in loc_map.values() if v})
        print(f"  [{dname}] {len(distinct_locs)} code location(s): {distinct_locs}", file=sys.stderr)

        for st in stores:
            print(f"  [{dname}] fetch store={st} ...", file=sys.stderr)
            _run_fetches(dname, st, loc_map)

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
    c.add_argument("--store", default="BOTH",
                   choices=("VICTORIA_METRICS", "POSTGRES", "BOTH"),
                   help=("Dagster+ metrics store to query. VICTORIA_METRICS "
                         "holds ~6 months of recent data with per-asset "
                         "granularity but empty codeLocationName; POSTGRES "
                         "holds long-tail history (back to org origination) "
                         "with populated codeLocationName but a smaller "
                         "distinct-asset set. BOTH (default) queries both "
                         "and unions — required for date ranges spanning "
                         "the ~6-month VM boundary."))
    c.add_argument("--limit", type=int, default=5000,
                   help=("Max assets returned per (deployment × store × "
                         "time chunk). Server default is 10 — always pass "
                         "an explicit high value. For orgs with > 5000 "
                         "credits-consuming assets in a single window, "
                         "either raise --limit further or split the "
                         "date range with narrower --start/--end and "
                         "re-run."))
    c.set_defaults(func=cmd_credits)

    mt = sub.add_parser("metric-types", help="List metric types available on your Dagster+.")
    mt.set_defaults(func=cmd_metric_types)

    d = sub.add_parser("deployments", help="List deployments in the org.")
    d.set_defaults(func=cmd_deployments)

    args = p.parse_args()
    return args.func(args) or 0


if __name__ == "__main__":
    sys.exit(main())
