#!/usr/bin/env python3
"""sync_custom_metrics.py — sync custom Insights metrics to a Dagster+ deployment.

Mirrors the shape of `dagster-cloud deployment alert-policies sync`:
takes a YAML manifest, upserts each metric by name via the Dagster+
GraphQL API. Idempotent — re-running with the same manifest is a no-op.

Usage:
    ./sync_custom_metrics.py sync metrics.yaml \\
        --deployment-url https://acme.dagster.cloud/prod \\
        --token-env DAGSTER_CLOUD_API_TOKEN

    ./sync_custom_metrics.py sync metrics.yaml \\
        --deployment-url https://acme.dagster.cloud/prod \\
        --token-env DAGSTER_CLOUD_API_TOKEN \\
        --dry-run

Manifest shape (YAML):
    metrics:
      - name: rows_ingested
        description: sum of rows_ingested metadata across all ingestion assets
        metadata_key: rows_ingested
        aggregation: SUM         # SUM | AVG | MIN | MAX
        unit: rows
        asset_selection: "group:ingestion"
      - name: pipeline_cost_usd
        description: total pipeline compute cost
        metadata_key: cost_usd
        aggregation: SUM
        unit: usd
        asset_selection: "*"

Requires: Python 3.8+, PyYAML.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any, Dict, List


# --------------------------------------------------------------------------
# GraphQL client
# --------------------------------------------------------------------------
def graphql(deployment_url: str, token: str, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    endpoint = deployment_url.rstrip("/") + "/graphql"
    body = json.dumps({"query": query, "variables": variables}).encode("utf-8")
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
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP {e.code}: {e.read().decode('utf-8', errors='replace')[:500]}") from e

    if payload.get("errors"):
        raise RuntimeError(f"GraphQL errors: {json.dumps(payload['errors'])[:1000]}")
    return payload.get("data") or {}


# --------------------------------------------------------------------------
# GraphQL queries — schema names may vary. Adjust to match your Dagster+
# deployment's actual schema; introspect via `__schema` if unsure.
# --------------------------------------------------------------------------

Q_LIST_METRICS = """
  query ListInsightsCustomMetrics {
    insightsCustomMetrics {
      name
      description
      metadataKey
      aggregation
      unit
      assetSelection
    }
  }
"""

Q_UPSERT_METRIC = """
  mutation UpsertInsightsCustomMetric(
    $name: String!,
    $description: String,
    $metadataKey: String!,
    $aggregation: InsightsMetricAggregation!,
    $unit: String,
    $assetSelection: String
  ) {
    saveInsightsCustomMetric(
      name: $name,
      description: $description,
      metadataKey: $metadataKey,
      aggregation: $aggregation,
      unit: $unit,
      assetSelection: $assetSelection
    ) {
      name
    }
  }
"""

Q_DELETE_METRIC = """
  mutation DeleteInsightsCustomMetric($name: String!) {
    deleteInsightsCustomMetric(name: $name) {
      success
    }
  }
"""


VALID_AGGREGATIONS = {"SUM", "AVG", "MIN", "MAX"}


# --------------------------------------------------------------------------
# Manifest loading + validation
# --------------------------------------------------------------------------
def load_manifest(path: str) -> List[Dict[str, Any]]:
    try:
        import yaml
    except ImportError:
        sys.exit("ERROR: PyYAML not installed. Install with: pip install pyyaml")
    with open(path, "r") as f:
        doc = yaml.safe_load(f)
    if not isinstance(doc, dict) or "metrics" not in doc:
        sys.exit(f"ERROR: manifest {path!r} must be a mapping with a top-level `metrics:` list")
    metrics = doc["metrics"]
    if not isinstance(metrics, list):
        sys.exit("ERROR: `metrics:` must be a list")

    validated: List[Dict[str, Any]] = []
    seen: set = set()
    for i, m in enumerate(metrics):
        if not isinstance(m, dict):
            sys.exit(f"ERROR: metrics[{i}] must be a mapping")
        name = m.get("name")
        metadata_key = m.get("metadata_key")
        aggregation = (m.get("aggregation") or "SUM").upper()

        if not name or not isinstance(name, str):
            sys.exit(f"ERROR: metrics[{i}] missing required `name` (string)")
        if not metadata_key or not isinstance(metadata_key, str):
            sys.exit(f"ERROR: metrics[{i}].name={name!r} missing required `metadata_key` (string)")
        if aggregation not in VALID_AGGREGATIONS:
            sys.exit(
                f"ERROR: metrics[{i}].name={name!r} has invalid aggregation={aggregation!r}. "
                f"Must be one of: {sorted(VALID_AGGREGATIONS)}"
            )
        if name in seen:
            sys.exit(f"ERROR: duplicate metric name {name!r} in manifest")
        seen.add(name)

        validated.append({
            "name": name,
            "description": m.get("description") or "",
            "metadataKey": metadata_key,
            "aggregation": aggregation,
            "unit": m.get("unit") or "",
            "assetSelection": m.get("asset_selection") or "*",
        })
    return validated


# --------------------------------------------------------------------------
# Sync operations
# --------------------------------------------------------------------------
def cmd_sync(args: argparse.Namespace) -> int:
    token = os.environ.get(args.token_env)
    if not token:
        sys.exit(f"ERROR: env var {args.token_env!r} is empty or unset")

    desired = load_manifest(args.manifest)
    print(f"Loaded {len(desired)} metric(s) from {args.manifest}", file=sys.stderr)

    if args.dry_run:
        print("[DRY RUN] Would upsert:", file=sys.stderr)
        for m in desired:
            print(f"  - {m['name']}: {m['aggregation']}({m['metadataKey']}) on {m['assetSelection']!r}",
                  file=sys.stderr)
        return 0

    try:
        current = graphql(args.deployment_url, token, Q_LIST_METRICS, {})
        existing_names = {m["name"] for m in (current.get("insightsCustomMetrics") or [])}
    except Exception as e:  # noqa: BLE001
        print(f"WARNING: could not list existing metrics ({e}); continuing with blind upserts", file=sys.stderr)
        existing_names = set()

    upserted = 0
    for m in desired:
        verb = "update" if m["name"] in existing_names else "create"
        graphql(args.deployment_url, token, Q_UPSERT_METRIC, m)
        print(f"  {verb} {m['name']}", file=sys.stderr)
        upserted += 1

    print(f"Synced {upserted} metric(s) to {args.deployment_url}", file=sys.stderr)

    if args.prune:
        desired_names = {m["name"] for m in desired}
        to_delete = existing_names - desired_names
        if to_delete:
            print(f"Pruning {len(to_delete)} metric(s) not in manifest:", file=sys.stderr)
            for name in sorted(to_delete):
                graphql(args.deployment_url, token, Q_DELETE_METRIC, {"name": name})
                print(f"  delete {name}", file=sys.stderr)
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    token = os.environ.get(args.token_env)
    if not token:
        sys.exit(f"ERROR: env var {args.token_env!r} is empty or unset")
    data = graphql(args.deployment_url, token, Q_LIST_METRICS, {})
    metrics = data.get("insightsCustomMetrics") or []
    if not metrics:
        print("(no custom Insights metrics in deployment)", file=sys.stderr)
        return 0
    for m in metrics:
        print(f"{m['name']}\t{m.get('aggregation', 'SUM')}({m.get('metadataKey', '')})\t{m.get('assetSelection', '*')}")
    return 0


# --------------------------------------------------------------------------
# CLI entry point
# --------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        prog="sync_custom_metrics",
        description="Sync custom Insights metrics to a Dagster+ deployment (idempotent upsert-by-name).",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_sync = sub.add_parser("sync", help="Upsert metrics from a YAML manifest.")
    p_sync.add_argument("manifest", help="Path to the YAML manifest.")
    p_sync.add_argument("--deployment-url", required=True)
    p_sync.add_argument("--token-env", default="DAGSTER_CLOUD_API_TOKEN")
    p_sync.add_argument("--dry-run", action="store_true")
    p_sync.add_argument("--prune", action="store_true", help="Delete metrics in deployment that aren't in the manifest.")
    p_sync.set_defaults(func=cmd_sync)

    p_list = sub.add_parser("list", help="List current custom Insights metrics in the deployment.")
    p_list.add_argument("--deployment-url", required=True)
    p_list.add_argument("--token-env", default="DAGSTER_CLOUD_API_TOKEN")
    p_list.set_defaults(func=cmd_list)

    args = parser.parse_args()
    return args.func(args) or 0


if __name__ == "__main__":
    sys.exit(main())
