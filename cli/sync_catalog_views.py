#!/usr/bin/env python3
"""sync_catalog_views.py — sync Catalog Views (a.k.a. named asset selections)
to a Dagster+ deployment via the GraphQL API.

Mirrors the shape of `dagster-cloud deployment alert-policies sync`:
takes a YAML manifest, upserts each view by name. Idempotent —
re-running with the same manifest is a no-op. Uses the real Dagster+
mutation `createOrUpdateCatalogView`.

Usage:
    ./sync_catalog_views.py sync catalog_views.yaml \\
        --deployment-url https://acme.dagster.cloud/prod \\
        --token-env DAGSTER_CLOUD_API_TOKEN

    ./sync_catalog_views.py sync catalog_views.yaml \\
        --deployment-url https://acme.dagster.cloud/prod \\
        --token-env DAGSTER_CLOUD_API_TOKEN \\
        --dry-run

Manifest shape (YAML):
    catalog_views:
      - name: high_priority_assets
        description: Assets tagged priority=high
        icon: star                       # any icon name Dagster+ accepts
        is_private: false
        # Provide EITHER a raw asset-selection query (recommended) …
        query_selection: "tag:priority=high"
        # … OR structured filters (empty lists ok):
        groups: []
        kinds: []
        tags: []
        owners: []
        code_locations: []
        columns: []
        column_tags: []
        table_names: []

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

Q_LIST_CATALOG_VIEWS = """
  query ListCatalogViews {
    catalogViews {
      id
      name
      description
      icon
      isPrivate
      selection {
        querySelection
        kinds
        columns
        tableNames
        tags { key value }
        columnTags { key value }
        groups { groupName repositoryName repositoryLocationName }
        codeLocations { repositoryName repositoryLocationName }
        owners {
          __typename
          ... on UserAssetOwner { email }
          ... on TeamAssetOwner { team }
        }
      }
    }
  }
"""

Q_UPSERT_CATALOG_VIEW = """
  mutation UpsertCatalogView(
    $id: String,
    $name: String!,
    $description: String!,
    $icon: String!,
    $isPrivate: Boolean!,
    $selection: CatalogViewSelectionInput!
  ) {
    createOrUpdateCatalogView(
      id: $id,
      name: $name,
      description: $description,
      icon: $icon,
      isPrivate: $isPrivate,
      selection: $selection
    ) {
      __typename
      ... on CatalogView { id name }
      ... on PythonError { message stack }
      ... on UnauthorizedError { message }
    }
  }
"""

Q_DELETE_CATALOG_VIEW = """
  mutation DeleteCatalogView($id: String!) {
    deleteCatalogView(id: $id) {
      __typename
      ... on PythonError { message }
      ... on UnauthorizedError { message }
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
        sys.exit("ERROR: PyYAML not installed. Install with: pip install pyyaml")
    with open(path, "r") as f:
        doc = yaml.safe_load(f)
    if not isinstance(doc, dict) or "catalog_views" not in doc:
        sys.exit(f"ERROR: manifest {path!r} must be a mapping with a top-level `catalog_views:` list")
    views = doc["catalog_views"]
    if not isinstance(views, list):
        sys.exit("ERROR: `catalog_views:` must be a list")

    validated: List[Dict[str, Any]] = []
    seen: set = set()
    for i, v in enumerate(views):
        if not isinstance(v, dict):
            sys.exit(f"ERROR: catalog_views[{i}] must be a mapping")
        name = v.get("name")
        if not name or not isinstance(name, str):
            sys.exit(f"ERROR: catalog_views[{i}] missing required `name` (string)")
        if name in seen:
            sys.exit(f"ERROR: duplicate catalog_view name {name!r} in manifest")
        seen.add(name)

        selection = {
            "querySelection": v.get("query_selection") or None,
            "groups":         v.get("groups") or [],
            "kinds":          v.get("kinds") or [],
            "owners":         v.get("owners") or [],
            "codeLocations":  v.get("code_locations") or [],
            "columns":        v.get("columns") or [],
            "tableNames":     v.get("table_names") or [],   # must be [] not None
            "tags":           v.get("tags") or [],           # each: {key: str, value: str}
            "columnTags":     v.get("column_tags") or [],
        }
        validated.append({
            "name":        name,
            "description": v.get("description") or "",
            "icon":        v.get("icon") or "globe",   # default matches Dagster+ UI defaults
            "isPrivate":   bool(v.get("is_private", False)),
            "selection":   selection,
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
    print(f"Loaded {len(desired)} catalog view(s) from {args.manifest}", file=sys.stderr)

    if args.dry_run:
        print("[DRY RUN] Would upsert:", file=sys.stderr)
        for v in desired:
            print(f"  - {v['name']}: {v['selection'].get('querySelection') or '(structured filters)'}",
                  file=sys.stderr)
        return 0

    # Fetch current state so we can pass `id` for update path + prune.
    try:
        current = graphql(args.deployment_url, token, Q_LIST_CATALOG_VIEWS, {})
        existing_by_name = {v["name"]: v for v in (current.get("catalogViews") or [])}
    except Exception as e:  # noqa: BLE001
        print(f"WARNING: could not list existing views ({e}); continuing with blind creates", file=sys.stderr)
        existing_by_name = {}

    upserted = 0
    for v in desired:
        variables = dict(v)
        existing = existing_by_name.get(v["name"])
        if existing:
            variables["id"] = existing["id"]
            verb = "update"
        else:
            variables["id"] = None
            verb = "create"
        result = graphql(args.deployment_url, token, Q_UPSERT_CATALOG_VIEW, variables)
        typename = (result.get("createOrUpdateCatalogView") or {}).get("__typename")
        if typename and typename not in ("CatalogView",):
            raise RuntimeError(f"{v['name']}: {typename} — {result['createOrUpdateCatalogView']}")
        print(f"  {verb} {v['name']}", file=sys.stderr)
        upserted += 1

    print(f"Synced {upserted} catalog view(s) to {args.deployment_url}", file=sys.stderr)

    if args.prune:
        desired_names = {v["name"] for v in desired}
        to_delete = [ex for name, ex in existing_by_name.items() if name not in desired_names]
        if to_delete:
            print(f"Pruning {len(to_delete)} view(s) not in manifest:", file=sys.stderr)
            for ex in to_delete:
                res = graphql(args.deployment_url, token, Q_DELETE_CATALOG_VIEW, {"id": ex["id"]})
                print(f"  delete {ex['name']}  ({res})", file=sys.stderr)
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    token = os.environ.get(args.token_env)
    if not token:
        sys.exit(f"ERROR: env var {args.token_env!r} is empty or unset")
    data = graphql(args.deployment_url, token, Q_LIST_CATALOG_VIEWS, {})
    views = data.get("catalogViews") or []
    if not views:
        print("(no catalog views in deployment)", file=sys.stderr)
        return 0
    for v in views:
        sel = v.get("selection") or {}
        expr = sel.get("querySelection") or json.dumps({k: sel[k] for k in sel if sel[k] and k != "querySelection"})
        print(f"{v['name']}\t{v['id']}\t{expr}")
    return 0


# --------------------------------------------------------------------------
# CLI entry point
# --------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        prog="sync_catalog_views",
        description="Sync Catalog Views (named asset selections) to a Dagster+ deployment.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_sync = sub.add_parser("sync", help="Upsert catalog views from a YAML manifest.")
    p_sync.add_argument("manifest", help="Path to the YAML manifest.")
    p_sync.add_argument("--deployment-url", required=True, help="e.g. https://acme.dagster.cloud/prod")
    p_sync.add_argument("--token-env", default="DAGSTER_CLOUD_API_TOKEN")
    p_sync.add_argument("--dry-run", action="store_true")
    p_sync.add_argument("--prune", action="store_true", help="Delete views not in manifest.")
    p_sync.set_defaults(func=cmd_sync)

    p_list = sub.add_parser("list", help="List current catalog views in the deployment.")
    p_list.add_argument("--deployment-url", required=True)
    p_list.add_argument("--token-env", default="DAGSTER_CLOUD_API_TOKEN")
    p_list.set_defaults(func=cmd_list)

    args = parser.parse_args()
    return args.func(args) or 0


if __name__ == "__main__":
    sys.exit(main())
