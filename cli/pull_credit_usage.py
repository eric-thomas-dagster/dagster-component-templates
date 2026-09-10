#!/usr/bin/env python3
"""pull_credit_usage.py — pull Dagster+ credit usage sliced across
deployment × code location × asset × day, and dump to CSV / JSON.

The Dagster+ web UI shows credit usage under Insights, but it doesn't
expose a cross-deployment / per-code-location / per-asset breakdown as
a single downloadable report. This script hits the same GraphQL
endpoints the UI does and merges the results into one table.

Two levels of endpoint:
    Org-scoped:         https://<org>.dagster.cloud/graphql
    Deployment-scoped:  https://<org>.dagster.cloud/<deployment>/graphql

The org endpoint enumerates deployments; the per-deployment endpoint
fetches insights metrics for that deployment's assets. The script
walks both and joins.

Usage:
    # ── One-liner: last 30 days, rolled up per deployment × code location × asset,
    #    written to CSV (with headers).
    ./pull_credit_usage.py \\
        --org ericthomas-dagster \\
        --token-env DAGSTER_CLOUD_API_TOKEN \\
        --deployments prod,staging \\
        --start 2026-08-10 --end 2026-09-10 \\
        --group-by asset \\
        --output-csv credits.csv

    # ── Daily breakdown per deployment (rollup — one row per deployment × day):
    ./pull_credit_usage.py --org ericthomas-dagster \\
        --token-env DAGSTER_CLOUD_API_TOKEN \\
        --start 2026-08-10 --end 2026-09-10 \\
        --group-by deployment,day \\
        --output-csv credits_daily.csv

    # ── Introspect the Insights schema on YOUR org (schema evolves —
    #    run this once if the queries below need tweaking against a
    #    newer or older Dagster+ version).
    ./pull_credit_usage.py --org ericthomas-dagster \\
        --token-env DAGSTER_CLOUD_API_TOKEN \\
        --deployments prod introspect

    # ── Dry-run: show the queries + endpoints without executing.
    ./pull_credit_usage.py --org ericthomas-dagster \\
        --deployments prod --token-env DAGSTER_CLOUD_API_TOKEN \\
        --start 2026-08-10 --end 2026-09-10 \\
        --group-by asset --dry-run

Group-by axes (composable, comma-separated):
    deployment       — one row per deployment
    code_location    — one row per (deployment, code_location)
    asset            — one row per (deployment, code_location, asset_key)
    day              — one row per (…, day) — appended to any of the above

Output columns (present when the axis is in --group-by):
    deployment, code_location, asset_key, day, dagster_credits, compute_seconds

Requires: Python 3.8+. No external deps.

────────────────────────────────────────────────────────────────────────
NOTE ON SCHEMA VERSIONING

Dagster+ Insights' GraphQL surface is an internal / semi-public API
that evolves across releases. The queries below reflect the shape as
of 2026-09. Run `<script> ... introspect` (or
`--introspect` on the top-level) if any query returns "field ... does
not exist on type ..." — the field probably got renamed / moved, and
the introspect output will show you the new shape.

Known query candidates (with expected field paths):

    # Enumerate deployments (org endpoint)
    query { fullDeployments { deploymentName deploymentType } }

    # Per-deployment: aggregate credits over a time window, bucketed
    # by asset key. This is the "assetsMetrics" family under Insights.
    query {
      assetsMetrics(
        startEpochSeconds: 1725000000, endEpochSeconds: 1727500000,
      ) {
        assetKey { path }
        codeLocationName
        metrics { metricName metricValue }   # metricName includes __dagster_dagster_credits__
      }
    }

    # Alt: some deployments expose usage under `insightsMetrics` or
    # `dagsterCloudUsage` — introspect if the above returns empty.

────────────────────────────────────────────────────────────────────────
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
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


# ── HTTP / GraphQL helpers ──────────────────────────────────────────────
def _post_graphql(endpoint: str, token: str, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    body = json.dumps({"query": query, "variables": variables or {}}).encode("utf-8")
    req = urllib.request.Request(
        endpoint,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Dagster-Cloud-Api-Token": token,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")[:800]
        raise RuntimeError(f"HTTP {e.code} @ {endpoint}: {body_text}") from e
    if payload.get("errors"):
        # Some Dagster+ queries return errors alongside partial data — surface both.
        errs_str = json.dumps(payload["errors"])[:1500]
        # Empty `data` = hard error; non-empty = warn but continue.
        if not payload.get("data"):
            raise RuntimeError(f"GraphQL error @ {endpoint}: {errs_str}")
        print(f"WARNING: GraphQL partial-error @ {endpoint}: {errs_str}", file=sys.stderr)
    return payload.get("data") or {}


def _org_endpoint(org: str) -> str:
    return f"https://{org}.dagster.cloud/graphql"


def _deployment_endpoint(org: str, deployment: str) -> str:
    return f"https://{org}.dagster.cloud/{deployment}/graphql"


# ── Queries ─────────────────────────────────────────────────────────────
#
# These are the shape as of 2026-09. If a field breaks, introspect the
# schema on YOUR org via `--introspect`.

Q_LIST_DEPLOYMENTS = """
  query ListDeployments {
    fullDeployments {
      deploymentName
      deploymentType
      deploymentId
    }
  }
"""

# Primary asset-level credits query. Times are epoch seconds.
Q_ASSET_CREDITS = """
  query AssetCredits($start: Float!, $end: Float!) {
    assetsMetrics(startEpochSeconds: $start, endEpochSeconds: $end) {
      assetKey { path }
      codeLocationName
      metrics {
        metricName
        metricValue
      }
    }
  }
"""

# Fallback if the primary shape returns nothing — some Dagster+ versions
# expose credits under a different top-level query. Introspect + edit
# as needed for your version.
Q_INSIGHTS_METRICS_FALLBACK = """
  query InsightsMetrics($start: Float!, $end: Float!) {
    insightsMetrics(startEpochSeconds: $start, endEpochSeconds: $end) {
      key
      values { asset { key { path } } codeLocationName numericValue }
    }
  }
"""

# Introspection — expose the types we care about.
Q_INTROSPECT_TYPES = """
  query IntrospectInsightsTypes {
    __schema {
      queryType {
        fields {
          name
          description
          args { name type { name kind ofType { name kind } } }
          type { name kind ofType { name kind } }
        }
      }
    }
  }
"""


# ── Credit-usage extraction ─────────────────────────────────────────────
#
# The metrics list on each asset row typically includes multiple
# named metrics (dagster credits, compute seconds, per-metadata-key
# custom Insights metrics …). We pick out the credit + compute
# metrics; everything else is ignored.
#
# The exact metric name for credits varies by Dagster+ version:
#    __dagster_dagster_credits__
#    dagster_credits
#    dagster/credits
# Same for compute seconds. Match by substring to be resilient.

CREDIT_METRIC_HINTS = ("credit", "credits")
COMPUTE_METRIC_HINTS = ("compute_second", "compute-second", "compute seconds")


def _pick_metric(metrics: Sequence[Dict[str, Any]], hints: Sequence[str]) -> Optional[float]:
    for m in metrics or []:
        name = (m.get("metricName") or "").lower()
        if any(h in name for h in hints):
            v = m.get("metricValue")
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None
    return None


def _fetch_asset_metrics(
    org: str, deployment: str, token: str,
    start_epoch: float, end_epoch: float,
) -> List[Dict[str, Any]]:
    """Return list of {asset_key, code_location, credits, compute_seconds} for
    the deployment over the window."""
    endpoint = _deployment_endpoint(org, deployment)
    try:
        data = _post_graphql(endpoint, token, Q_ASSET_CREDITS,
                             {"start": start_epoch, "end": end_epoch})
        rows = data.get("assetsMetrics") or []
    except RuntimeError as e:
        # Primary query failed. Try fallback shape.
        print(f"    [{deployment}] primary assetsMetrics query failed ({e}); trying fallback ...", file=sys.stderr)
        try:
            data = _post_graphql(endpoint, token, Q_INSIGHTS_METRICS_FALLBACK,
                                 {"start": start_epoch, "end": end_epoch})
            # Fallback shape flattening — attempt best-effort. If your
            # deployment uses yet another shape, run --introspect to see it.
            fallback: List[Dict[str, Any]] = []
            for m in data.get("insightsMetrics") or []:
                key = (m.get("key") or "").lower()
                if not any(h in key for h in CREDIT_METRIC_HINTS + COMPUTE_METRIC_HINTS):
                    continue
                for v in m.get("values") or []:
                    ak = v.get("asset", {}).get("key", {}).get("path") or []
                    fallback.append({
                        "asset_key": "/".join(ak),
                        "code_location": v.get("codeLocationName") or "",
                        "_metric_name": key,
                        "_metric_value": v.get("numericValue"),
                    })
            return fallback
        except RuntimeError as e2:
            print(f"    [{deployment}] fallback also failed: {e2}", file=sys.stderr)
            return []

    out: List[Dict[str, Any]] = []
    for row in rows:
        ak = "/".join((row.get("assetKey") or {}).get("path") or [])
        cl = row.get("codeLocationName") or ""
        credits = _pick_metric(row.get("metrics") or [], CREDIT_METRIC_HINTS)
        cpu     = _pick_metric(row.get("metrics") or [], COMPUTE_METRIC_HINTS)
        out.append({
            "asset_key":       ak,
            "code_location":   cl,
            "credits":         credits or 0.0,
            "compute_seconds": cpu or 0.0,
        })
    return out


def _list_deployments(org: str, token: str) -> List[str]:
    data = _post_graphql(_org_endpoint(org), token, Q_LIST_DEPLOYMENTS)
    deps = data.get("fullDeployments") or []
    return [d["deploymentName"] for d in deps if d.get("deploymentName")]


# ── Aggregation ─────────────────────────────────────────────────────────
def _aggregate(rows: List[Dict[str, Any]], group_by: Sequence[str]) -> List[Dict[str, Any]]:
    """Roll rows up by the requested axes. Every row must carry the axis
    fields the caller asked to group by."""
    valid_axes = {"deployment", "code_location", "asset_key", "day"}
    axes = [a if a != "asset" else "asset_key" for a in group_by]
    for a in axes:
        if a not in valid_axes:
            raise ValueError(f"unknown group-by axis {a!r}. Valid: {sorted(valid_axes | {'asset'})}")
    buckets: Dict[tuple, Dict[str, float]] = {}
    for r in rows:
        key = tuple(r.get(a, "") for a in axes)
        agg = buckets.setdefault(key, {"credits": 0.0, "compute_seconds": 0.0})
        agg["credits"]         += float(r.get("credits") or 0.0)
        agg["compute_seconds"] += float(r.get("compute_seconds") or 0.0)
    out: List[Dict[str, Any]] = []
    for key, agg in buckets.items():
        row: Dict[str, Any] = dict(zip(axes, key))
        row.update(agg)
        out.append(row)
    # Deterministic ordering — descending credits then ascending key.
    out.sort(key=lambda r: (-r.get("credits", 0.0), *(str(r.get(a, "")) for a in axes)))
    return out


def _day_bucket_epoch_range(start_epoch: float, end_epoch: float) -> Iterable[Tuple[str, float, float]]:
    """Yield (YYYY-MM-DD, window_start_epoch, window_end_epoch) day buckets
    fully covering [start_epoch, end_epoch]. Used for --group-by day."""
    start_dt = _dt.datetime.fromtimestamp(start_epoch, tz=_dt.timezone.utc)
    end_dt   = _dt.datetime.fromtimestamp(end_epoch,   tz=_dt.timezone.utc)
    cur = _dt.datetime(start_dt.year, start_dt.month, start_dt.day, tzinfo=_dt.timezone.utc)
    while cur.timestamp() < end_dt.timestamp():
        nxt = cur + _dt.timedelta(days=1)
        yield (cur.strftime("%Y-%m-%d"), cur.timestamp(), min(nxt.timestamp(), end_dt.timestamp()))
        cur = nxt


# ── Subcommands ─────────────────────────────────────────────────────────
def cmd_credits(args: argparse.Namespace) -> int:
    token = os.environ.get(args.token_env)
    if not token:
        sys.exit(f"ERROR: env var {args.token_env!r} is empty or unset")

    start_epoch, end_epoch = _parse_date_range(args.start, args.end)
    group_by = [a.strip() for a in args.group_by.split(",") if a.strip()]

    deployments = args.deployments.split(",") if args.deployments else _list_deployments(args.org, token)
    print(f"Deployments: {', '.join(deployments)}", file=sys.stderr)

    if args.dry_run:
        print(f"[DRY RUN] Would fetch credits from:", file=sys.stderr)
        for d in deployments:
            print(f"  {_deployment_endpoint(args.org, d)}", file=sys.stderr)
        print(f"  window: {args.start} → {args.end} ({start_epoch:.0f} → {end_epoch:.0f})",
              file=sys.stderr)
        print(f"  group_by: {group_by}", file=sys.stderr)
        print(f"[DRY RUN] Query shape:\n{Q_ASSET_CREDITS}", file=sys.stderr)
        return 0

    # Fetch. If day is in group_by we split the window into daily
    # sub-queries so each row carries a day. Otherwise one query per
    # deployment covering the whole window.
    all_rows: List[Dict[str, Any]] = []
    daily = "day" in group_by
    windows: List[Tuple[Optional[str], float, float]]
    if daily:
        windows = [(d, s, e) for d, s, e in _day_bucket_epoch_range(start_epoch, end_epoch)]
    else:
        windows = [(None, start_epoch, end_epoch)]

    for d in deployments:
        for (day_str, w_start, w_end) in windows:
            print(f"  [{d}] window {day_str or f'{args.start}..{args.end}'} ...", file=sys.stderr)
            rows = _fetch_asset_metrics(args.org, d, token, w_start, w_end)
            for r in rows:
                r["deployment"] = d
                if day_str:
                    r["day"] = day_str
                all_rows.append(r)

    rolled = _aggregate(all_rows, group_by)
    print(f"Wrote {len(rolled)} row(s).", file=sys.stderr)

    # Emit
    axes_present = [a if a != "asset" else "asset_key" for a in group_by]
    columns = axes_present + ["credits", "compute_seconds"]

    if args.output_json:
        with open(args.output_json, "w") as f:
            json.dump(rolled, f, indent=2, default=str)
        print(f"→ wrote {args.output_json}", file=sys.stderr)
    if args.output_csv:
        with open(args.output_csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=columns)
            w.writeheader()
            for r in rolled:
                w.writerow({c: r.get(c, "") for c in columns})
        print(f"→ wrote {args.output_csv}", file=sys.stderr)
    if not (args.output_json or args.output_csv):
        # Stdout table for interactive use
        w = csv.DictWriter(sys.stdout, fieldnames=columns)
        w.writeheader()
        for r in rolled:
            w.writerow({c: r.get(c, "") for c in columns})
    return 0


def cmd_introspect(args: argparse.Namespace) -> int:
    """Print the top-level query field names + their args for the
    deployment endpoint — helps identify the actual insights fields
    available on YOUR org's Dagster+ version."""
    token = os.environ.get(args.token_env)
    if not token:
        sys.exit(f"ERROR: env var {args.token_env!r} is empty or unset")

    for deployment in (args.deployments.split(",") if args.deployments else _list_deployments(args.org, token)):
        endpoint = _deployment_endpoint(args.org, deployment)
        print(f"\n=== {deployment} @ {endpoint} ===", file=sys.stderr)
        try:
            data = _post_graphql(endpoint, token, Q_INTROSPECT_TYPES)
        except RuntimeError as e:
            print(f"  introspect failed: {e}", file=sys.stderr)
            continue
        fields = ((data.get("__schema") or {}).get("queryType") or {}).get("fields") or []
        # Filter to insights-related fields.
        hints = ("insight", "credit", "usage", "metric", "asset")
        interesting = [f for f in fields
                       if any(h in (f.get("name") or "").lower() for h in hints)]
        for f in sorted(interesting, key=lambda f: f["name"]):
            args_str = ", ".join(
                f"{a['name']}: {(a.get('type') or {}).get('name') or (a.get('type') or {}).get('ofType', {}).get('name') or '?'}"
                for a in (f.get("args") or [])
            )
            ret = (f.get("type") or {}).get("name") or (f.get("type") or {}).get("ofType", {}).get("name") or "?"
            print(f"  {f['name']}({args_str}) -> {ret}")
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


# ── CLI entry ───────────────────────────────────────────────────────────
def main() -> int:
    p = argparse.ArgumentParser(
        prog="pull_credit_usage",
        description="Pull Dagster+ credit usage across deployment × code location × asset × day.",
    )
    p.add_argument("--org", required=True, help="Dagster+ org name (e.g. ericthomas-dagster).")
    p.add_argument("--token-env", default="DAGSTER_CLOUD_API_TOKEN",
                   help="Env var name holding the Dagster+ user API token.")
    p.add_argument("--deployments", default=None,
                   help="Comma-separated deployment names. Default: all deployments in the org.")

    sub = p.add_subparsers(dest="cmd", required=True)

    # `credits` subcommand
    c = sub.add_parser("credits", help="Pull credit usage into CSV / JSON.")
    c.add_argument("--start", required=True, help="Start date (YYYY-MM-DD, inclusive).")
    c.add_argument("--end",   required=True, help="End date   (YYYY-MM-DD, inclusive).")
    c.add_argument("--group-by", default="deployment,code_location,asset",
                   help=("Aggregation axes, comma-separated. Any of: "
                         "deployment, code_location, asset, day. Default: "
                         "deployment,code_location,asset."))
    c.add_argument("--output-csv",  default=None, help="Write CSV to this path.")
    c.add_argument("--output-json", default=None, help="Write JSON to this path.")
    c.add_argument("--dry-run", action="store_true", help="Print the query + endpoints, don't execute.")
    c.set_defaults(func=cmd_credits)

    # `introspect` subcommand
    i = sub.add_parser("introspect",
                       help="Print the deployment-endpoint query fields (insights-related) so you can verify the query shape against your org's Dagster+ version.")
    i.set_defaults(func=cmd_introspect)

    args = p.parse_args()
    return args.func(args) or 0


if __name__ == "__main__":
    sys.exit(main())
