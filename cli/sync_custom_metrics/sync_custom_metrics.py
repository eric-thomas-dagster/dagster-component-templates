#!/usr/bin/env python3
"""sync_custom_metrics.py — sync custom Insights metrics to a Dagster+ deployment.

Mirrors the shape of `dagster-cloud deployment alert-policies sync`:
takes a YAML manifest, upserts each metric via the Dagster+ GraphQL
API. Idempotent — matches existing metrics by `metadata_key` (the
natural key) and updates them; creates new ones when unmatched.

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
      - metadata_key: rows_ingested       # the asset metadata key to promote
        display_name: Rows Ingested       # optional; shown in Insights UI
        description: Rows ingested per materialization
        unit_type: INTEGER                # INTEGER | TIME_MS | TIME_SECONDS | FLOAT | BYTES
      - metadata_key: cost_usd
        display_name: Compute Cost (USD)
        unit_type: FLOAT

Dagster+ Insights aggregates automatically across the assets/runs that
emit the metadata key; you don't specify an aggregation or selection
per metric — that's a UI-side choice.

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
# Real Dagster+ mutations (as of 2026-09)
# --------------------------------------------------------------------------

Q_LIST_METRICS = """
  query ListCustomMetrics {
    customMetrics {
      id
      metadataKey
      displayName
      description
      unitType
    }
  }
"""

Q_CREATE_METRIC = """
  mutation CreateCustomMetric($customMetricInput: CustomMetricInput!) {
    createCustomMetric(customMetricInput: $customMetricInput) {
      __typename
      ... on CreateCustomMetricSuccess { customMetric { id metadataKey } }
      ... on CustomMetricError { message }
      ... on PythonError { message stack }
      ... on UnauthorizedError { message }
    }
  }
"""

Q_UPDATE_METRIC = """
  mutation UpdateCustomMetric($customMetricId: String!, $customMetricUpdateInput: CustomMetricUpdateInput!) {
    updateCustomMetric(customMetricId: $customMetricId, customMetricUpdateInput: $customMetricUpdateInput) {
      __typename
      ... on UpdateCustomMetricSuccess { customMetric { id metadataKey } }
      ... on CustomMetricError { message }
      ... on PythonError { message stack }
      ... on UnauthorizedError { message }
    }
  }
"""

Q_DELETE_METRIC = """
  mutation DeleteCustomMetric($customMetricId: String!) {
    deleteCustomMetric(customMetricId: $customMetricId) {
      __typename
      ... on DeleteCustomMetricSuccess { customMetricId }
      ... on CustomMetricError { message }
      ... on PythonError { message }
      ... on UnauthorizedError { message }
    }
  }
"""


VALID_UNIT_TYPES = {"INTEGER", "TIME_MS", "TIME_SECONDS", "FLOAT", "BYTES"}


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
        metadata_key = m.get("metadata_key")
        if not metadata_key or not isinstance(metadata_key, str):
            sys.exit(f"ERROR: metrics[{i}] missing required `metadata_key` (string)")
        if metadata_key in seen:
            sys.exit(f"ERROR: duplicate metadata_key {metadata_key!r} in manifest")
        seen.add(metadata_key)

        unit_type = m.get("unit_type")
        if unit_type and unit_type not in VALID_UNIT_TYPES:
            sys.exit(
                f"ERROR: metrics[{i}].metadata_key={metadata_key!r} has invalid unit_type={unit_type!r}. "
                f"Must be one of: {sorted(VALID_UNIT_TYPES)}"
            )

        validated.append({
            "metadataKey": metadata_key,
            "displayName": m.get("display_name") or None,
            "description": m.get("description") or None,
            "unitType":    unit_type,   # None → server default
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
            print(f"  - {m['metadataKey']}: display={m['displayName']!r} unit={m['unitType']}",
                  file=sys.stderr)
        return 0

    try:
        current = graphql(args.deployment_url, token, Q_LIST_METRICS, {})
        existing_by_key = {m["metadataKey"]: m for m in (current.get("customMetrics") or [])}
    except Exception as e:  # noqa: BLE001
        print(f"WARNING: could not list existing metrics ({e}); continuing with blind creates", file=sys.stderr)
        existing_by_key = {}

    created = updated = 0
    for m in desired:
        existing = existing_by_key.get(m["metadataKey"])
        if existing:
            update_input = {
                "displayName": m["displayName"],
                "description": m["description"],
                "unitType":    m["unitType"],
            }
            result = graphql(
                args.deployment_url, token, Q_UPDATE_METRIC,
                {"customMetricId": existing["id"], "customMetricUpdateInput": update_input},
            )
            top = (result.get("updateCustomMetric") or {})
            if top.get("__typename") not in ("UpdateCustomMetricSuccess",):
                raise RuntimeError(f"{m['metadataKey']}: {top}")
            print(f"  update {m['metadataKey']}", file=sys.stderr)
            updated += 1
        else:
            create_input = {k: v for k, v in m.items() if v is not None}
            result = graphql(args.deployment_url, token, Q_CREATE_METRIC,
                             {"customMetricInput": create_input})
            top = (result.get("createCustomMetric") or {})
            if top.get("__typename") not in ("CreateCustomMetricSuccess",):
                raise RuntimeError(f"{m['metadataKey']}: {top}")
            print(f"  create {m['metadataKey']}", file=sys.stderr)
            created += 1

    print(f"Synced {created + updated} metric(s) to {args.deployment_url} "
          f"({created} created, {updated} updated)", file=sys.stderr)

    if args.prune:
        desired_keys = {m["metadataKey"] for m in desired}
        to_delete = [ex for k, ex in existing_by_key.items() if k not in desired_keys]
        if to_delete:
            print(f"Pruning {len(to_delete)} metric(s) not in manifest:", file=sys.stderr)
            for ex in to_delete:
                graphql(args.deployment_url, token, Q_DELETE_METRIC, {"customMetricId": ex["id"]})
                print(f"  delete {ex['metadataKey']}", file=sys.stderr)
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    token = os.environ.get(args.token_env)
    if not token:
        sys.exit(f"ERROR: env var {args.token_env!r} is empty or unset")
    data = graphql(args.deployment_url, token, Q_LIST_METRICS, {})
    metrics = data.get("customMetrics") or []
    if not metrics:
        print("(no custom Insights metrics in deployment)", file=sys.stderr)
        return 0
    for m in metrics:
        print(f"{m['metadataKey']}\t{m['id']}\t{m.get('unitType') or '(default)'}\t{m.get('displayName') or ''}")
    return 0


# --------------------------------------------------------------------------
# CLI entry point
# --------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        prog="sync_custom_metrics",
        description="Sync custom Insights metrics to a Dagster+ deployment (idempotent upsert-by-metadata-key).",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_sync = sub.add_parser("sync", help="Upsert metrics from a YAML manifest.")
    p_sync.add_argument("manifest", help="Path to the YAML manifest.")
    p_sync.add_argument("--deployment-url", required=True)
    p_sync.add_argument("--token-env", default="DAGSTER_CLOUD_API_TOKEN")
    p_sync.add_argument("--dry-run", action="store_true")
    p_sync.add_argument("--prune", action="store_true", help="Delete metrics not in manifest.")
    p_sync.set_defaults(func=cmd_sync)

    p_list = sub.add_parser("list", help="List current custom Insights metrics.")
    p_list.add_argument("--deployment-url", required=True)
    p_list.add_argument("--token-env", default="DAGSTER_CLOUD_API_TOKEN")
    p_list.set_defaults(func=cmd_list)

    args = parser.parse_args()
    return args.func(args) or 0


if __name__ == "__main__":
    sys.exit(main())
