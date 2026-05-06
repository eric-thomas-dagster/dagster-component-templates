"""Shared lineage-graph building logic.

Imported by each lineage_to_<catalog> component. Same file is copied into
each component dir (registry doesn't have first-class cross-component
shared modules).
"""

import hashlib
import json
import os
import time
from typing import Optional


def build_lineage_payload(repo_def) -> dict:
    """Walk the live asset graph and build a catalog-ready payload."""
    asset_graph = repo_def.asset_graph
    all_keys = list(asset_graph.toposorted_asset_keys)

    nodes = []
    edges = []
    group_summary: dict = {}

    for key in all_keys:
        node = asset_graph.get(key)
        key_str = key.to_user_string()

        raw_metadata = node.metadata or {}
        safe_metadata = {}
        for k, v in raw_metadata.items():
            if k.startswith(("dagster_dbt/", "dagster/")):
                continue
            try:
                json.dumps(v)
                safe_metadata[k] = v
            except Exception:
                safe_metadata[k] = str(v)

        fp = node.freshness_policy_or_from_metadata
        nodes.append({
            "asset_key": key.path,
            "asset_key_string": key_str,
            "group": node.group_name,
            "kinds": sorted(node.kinds) if node.kinds else [],
            "description": (node.description or "")[:500],
            "metadata": safe_metadata,
            "freshness_policy": str(fp) if fp else None,
            "parent_count": len(node.parent_keys),
            "child_count": len(node.child_keys),
        })

        grp = node.group_name or "ungrouped"
        group_summary.setdefault(grp, []).append(key_str)

        for parent_key in node.parent_keys:
            edges.append({
                "upstream": parent_key.to_user_string(),
                "downstream": key_str,
            })

    return {
        "sync_metadata": {
            "synced_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": "dagster_asset_graph",
            "total_nodes": len(nodes),
            "total_edges": len(edges),
            "total_groups": len(group_summary),
            "assets_with_freshness_policy": sum(1 for n in nodes if n["freshness_policy"]),
        },
        "nodes": nodes,
        "edges": edges,
        "group_summary": {
            grp: {"count": len(assets), "assets": assets}
            for grp, assets in sorted(group_summary.items())
        },
    }


def build_lineage_from_dagster_plus_graphql(log, dagster_plus_token_env: str) -> Optional[dict]:
    """Query Dagster+ GraphQL for the full deployment-wide asset graph.

    Returns None if the API is unavailable (OSS, missing token, etc.).
    """
    cloud_url = os.environ.get("DAGSTER_CLOUD_URL")
    deployment = os.environ.get("DAGSTER_CLOUD_DEPLOYMENT_NAME", "prod")
    token = os.environ.get(dagster_plus_token_env)

    if not (cloud_url and token):
        return None

    graphql_url = f"{cloud_url}/{deployment}/graphql"

    try:
        import requests

        query = """
        query AssetLineageQuery {
            assetNodes {
                assetKey { path }
                groupName
                computeKind
                description
                dependencyKeys { path }
                dependedByKeys { path }
                opNames
                repository { name location { name } }
                freshnessPolicy { cronSchedule maximumLagMinutes }
            }
        }
        """

        resp = requests.post(
            graphql_url,
            json={"query": query},
            headers={"Dagster-Cloud-Api-Token": token, "Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        asset_nodes = resp.json().get("data", {}).get("assetNodes", [])
        if not asset_nodes:
            return None

        nodes = []
        edges = []
        group_summary: dict = {}

        for an in asset_nodes:
            key_str = "/".join(an["assetKey"]["path"])
            code_location = an.get("repository", {}).get("location", {}).get("name", "unknown")
            grp = an.get("groupName") or "ungrouped"

            nodes.append({
                "asset_key": an["assetKey"]["path"],
                "asset_key_string": key_str,
                "group": grp,
                "kinds": [an["computeKind"]] if an.get("computeKind") else [],
                "description": (an.get("description") or "")[:500],
                "metadata": {"code_location": code_location},
                "freshness_policy": str(an["freshnessPolicy"]) if an.get("freshnessPolicy") else None,
                "code_location": code_location,
                "parent_count": len(an.get("dependencyKeys", [])),
                "child_count": len(an.get("dependedByKeys", [])),
            })

            group_summary.setdefault(grp, []).append(key_str)

            for dep_key in an.get("dependencyKeys", []):
                edges.append({
                    "upstream": "/".join(dep_key["path"]),
                    "downstream": key_str,
                })

        log.info(f"GraphQL: fetched {len(nodes)} assets across deployment")
        return {
            "sync_metadata": {
                "synced_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source": "dagster_plus_graphql",
                "total_nodes": len(nodes),
                "total_edges": len(edges),
                "total_groups": len(group_summary),
                "assets_with_freshness_policy": sum(1 for n in nodes if n["freshness_policy"]),
                "code_locations": list({n["code_location"] for n in nodes}),
            },
            "nodes": nodes,
            "edges": edges,
            "group_summary": {
                grp: {"count": len(assets), "assets": assets}
                for grp, assets in sorted(group_summary.items())
            },
        }

    except Exception as e:
        log.warning(f"GraphQL query failed, falling back to local graph: {e}")
        return None


def hash_payload(payload: dict) -> str:
    """Hash the structural content (nodes + edges) for change detection."""
    structural = json.dumps(
        {"nodes": payload["nodes"], "edges": payload["edges"]},
        sort_keys=True,
    )
    return hashlib.sha256(structural.encode()).hexdigest()[:16]


def get_token(token_env: str) -> str:
    """Get API token from environment, raising if missing."""
    token = os.environ.get(token_env)
    if not token:
        raise RuntimeError(f"Missing {token_env} environment variable")
    return token


def build_source_system(component) -> dict:
    """Build source-system identity from component config + env."""
    return {
        "platform": getattr(component, "platform_name", None) or "dagster",
        "platform_display_name": getattr(component, "platform_display_name", None) or "Dagster",
        "deployment": getattr(component, "deployment_name", "") or os.environ.get("DAGSTER_CLOUD_DEPLOYMENT_NAME", "local"),
        "dagster_ui_url": getattr(component, "dagster_ui_url", "") or os.environ.get("DAGSTER_CLOUD_URL", ""),
        "organization": getattr(component, "organization", ""),
        "code_location": getattr(component, "code_location_name", ""),
    }
