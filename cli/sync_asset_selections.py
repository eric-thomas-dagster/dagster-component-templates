#!/usr/bin/env python3
"""sync_asset_selections.py — sync named asset selections to a Dagster+ deployment.

Mirrors the shape of `dagster-cloud deployment alert-policies sync`:
takes a YAML manifest, upserts each selection by name via the Dagster+
GraphQL API. Idempotent — re-running with the same manifest is a no-op.

Usage:
    ./sync_asset_selections.py sync selections.yaml \\
        --deployment-url https://acme.dagster.cloud/prod \\
        --token-env DAGSTER_CLOUD_API_TOKEN

    ./sync_asset_selections.py sync selections.yaml \\
        --deployment-url https://acme.dagster.cloud/prod \\
        --token-env DAGSTER_CLOUD_API_TOKEN \\
        --dry-run

Manifest shape (YAML):
    selections:
      - name: high_priority_assets
        description: assets tagged priority=high
        selection: "tag:priority=high"
      - name: analytics_downstream
        selection: "+group:analytics"

Requires: Python 3.8+, PyYAML.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional


# --------------------------------------------------------------------------
# GraphQL client
# --------------------------------------------------------------------------
def graphql(deployment_url: str, token: str, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    """POST a GraphQL query. Returns the `data` dict; raises on errors."""
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
# GraphQL queries — schema names may vary across Dagster+ versions.
# Adjust if your deployment's schema differs from the defaults below;
# introspect via `{ __schema { mutationType { fields { name } } } }`.
# --------------------------------------------------------------------------

Q_LIST_SELECTIONS = """
  query ListAssetSelections {
    assetSelections {
      name
      description
      selection
    }
  }
"""

Q_UPSERT_SELECTION = """
  mutation UpsertAssetSelection($name: String!, $description: String, $selection: String!) {
    saveAssetSelection(name: $name, description: $description, selection: $selection) {
      name
    }
  }
"""

Q_DELETE_SELECTION = """
  mutation DeleteAssetSelection($name: String!) {
    deleteAssetSelection(name: $name) {
      success
    }
  }
"""


# --------------------------------------------------------------------------
# Manifest loading + validation
# --------------------------------------------------------------------------
def load_manifest(path: str) -> List[Dict[str, Any]]:
    try:
        import yaml
    except ImportError:
        sys.exit(
            "ERROR: PyYAML not installed. Install with: pip install pyyaml"
        )
    with open(path, "r") as f:
        doc = yaml.safe_load(f)
    if not isinstance(doc, dict) or "selections" not in doc:
        sys.exit(f"ERROR: manifest {path!r} must be a mapping with a top-level `selections:` list")
    sels = doc["selections"]
    if not isinstance(sels, list):
        sys.exit("ERROR: `selections:` must be a list")

    validated: List[Dict[str, Any]] = []
    seen: set = set()
    for i, s in enumerate(sels):
        if not isinstance(s, dict):
            sys.exit(f"ERROR: selections[{i}] must be a mapping")
        name = s.get("name")
        selection = s.get("selection")
        if not name or not isinstance(name, str):
            sys.exit(f"ERROR: selections[{i}] missing required `name` (string)")
        if not selection or not isinstance(selection, str):
            sys.exit(f"ERROR: selections[{i}].name={name!r} missing required `selection` (string)")
        if name in seen:
            sys.exit(f"ERROR: duplicate selection name {name!r} in manifest")
        seen.add(name)
        validated.append({
            "name": name,
            "description": s.get("description") or "",
            "selection": selection,
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
    print(f"Loaded {len(desired)} selection(s) from {args.manifest}", file=sys.stderr)

    if args.dry_run:
        print("[DRY RUN] Would upsert:", file=sys.stderr)
        for s in desired:
            print(f"  - {s['name']}: {s['selection']}", file=sys.stderr)
        return 0

    # Fetch current state to detect diffs (log-only; upsert is idempotent).
    try:
        current = graphql(args.deployment_url, token, Q_LIST_SELECTIONS, {})
        existing_names = {s["name"] for s in (current.get("assetSelections") or [])}
    except Exception as e:  # noqa: BLE001
        print(f"WARNING: could not list existing selections ({e}); continuing with blind upserts", file=sys.stderr)
        existing_names = set()

    upserted = 0
    for s in desired:
        verb = "update" if s["name"] in existing_names else "create"
        graphql(args.deployment_url, token, Q_UPSERT_SELECTION, s)
        print(f"  {verb} {s['name']}", file=sys.stderr)
        upserted += 1

    print(f"Synced {upserted} selection(s) to {args.deployment_url}", file=sys.stderr)

    if args.prune:
        desired_names = {s["name"] for s in desired}
        to_delete = existing_names - desired_names
        if to_delete:
            print(f"Pruning {len(to_delete)} selection(s) not in manifest:", file=sys.stderr)
            for name in sorted(to_delete):
                graphql(args.deployment_url, token, Q_DELETE_SELECTION, {"name": name})
                print(f"  delete {name}", file=sys.stderr)
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    token = os.environ.get(args.token_env)
    if not token:
        sys.exit(f"ERROR: env var {args.token_env!r} is empty or unset")
    data = graphql(args.deployment_url, token, Q_LIST_SELECTIONS, {})
    sels = data.get("assetSelections") or []
    if not sels:
        print("(no asset selections in deployment)", file=sys.stderr)
        return 0
    for s in sels:
        print(f"{s['name']}\t{s.get('selection', '')}")
    return 0


# --------------------------------------------------------------------------
# CLI entry point
# --------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        prog="sync_asset_selections",
        description="Sync named asset selections to a Dagster+ deployment (idempotent upsert-by-name).",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # sync
    p_sync = sub.add_parser("sync", help="Upsert selections from a YAML manifest.")
    p_sync.add_argument("manifest", help="Path to the YAML manifest.")
    p_sync.add_argument("--deployment-url", required=True, help="Dagster+ deployment URL (e.g. https://acme.dagster.cloud/prod).")
    p_sync.add_argument("--token-env", default="DAGSTER_CLOUD_API_TOKEN", help="Env var holding the Dagster+ user API token.")
    p_sync.add_argument("--dry-run", action="store_true", help="List what would be upserted without calling the API.")
    p_sync.add_argument("--prune", action="store_true", help="After upsert, delete selections that exist in the deployment but not in the manifest.")
    p_sync.set_defaults(func=cmd_sync)

    # list
    p_list = sub.add_parser("list", help="List current asset selections in the deployment.")
    p_list.add_argument("--deployment-url", required=True)
    p_list.add_argument("--token-env", default="DAGSTER_CLOUD_API_TOKEN")
    p_list.set_defaults(func=cmd_list)

    args = parser.parse_args()
    return args.func(args) or 0


if __name__ == "__main__":
    sys.exit(main())
