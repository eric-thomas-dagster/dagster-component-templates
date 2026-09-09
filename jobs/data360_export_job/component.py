"""Data360ExportJobComponent.

Op-shaped job that walks the live asset graph and pushes lineage to
Precisely Data360 Govern via the Catalog REST endpoint (OAuth2
client-credentials auth).

Complements the asset-shaped `lineage_to_data360` sink. Prefer this
job shape for scheduled "sync my Dagster asset graph to Data360"
workloads; use the asset chain when you want automation-condition-
driven pushes.
"""

import hashlib
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import dagster as dg
from pydantic import Field


DATA360_DEFAULT_TOKEN_URL = "https://api.data.precisely.com/oauth/token"


def _build_payload_from_repo(repo_def) -> Dict[str, Any]:
    asset_graph = repo_def.asset_graph
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, str]] = []
    for key in list(asset_graph.toposorted_asset_keys):
        node = asset_graph.get(key)
        key_str = key.to_user_string()
        raw_metadata = node.metadata or {}
        safe_metadata: Dict[str, Any] = {}
        for k, v in raw_metadata.items():
            if k.startswith(("dagster_dbt/", "dagster/")):
                continue
            try:
                json.dumps(v); safe_metadata[k] = v
            except Exception:
                safe_metadata[k] = str(v)
        nodes.append({
            "asset_key": key.path,
            "asset_key_string": key_str,
            "group": node.group_name,
            "kinds": sorted(node.kinds) if node.kinds else [],
            "description": (node.description or "")[:500],
            "metadata": safe_metadata,
        })
        for parent_key in node.parent_keys:
            edges.append({"upstream": parent_key.to_user_string(), "downstream": key_str})
    return {
        "source_system": {
            "deployment": os.environ.get("DAGSTER_DEPLOYMENT", ""),
            "dagster_ui_url": os.environ.get("DAGSTER_UI_URL", ""),
            "data360_asset_type": "DagsterAsset",
        },
        "sync_metadata": {
            "synced_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": "dagster_asset_graph",
            "total_nodes": len(nodes),
            "total_edges": len(edges),
        },
        "nodes": nodes,
        "edges": edges,
    }


def _hash_structural(payload: Dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps({"nodes": payload["nodes"], "edges": payload["edges"]}, sort_keys=True).encode()
    ).hexdigest()[:16]


def _transform(payload) -> Tuple[List[dict], List[dict]]:
    ss = payload.get("source_system", {})
    deployment = ss.get("deployment", "")
    ui_url = ss.get("dagster_ui_url", "")
    asset_type = ss.get("data360_asset_type", "DagsterAsset")

    parent_map = {}
    for edge in payload["edges"]:
        parent_map.setdefault(edge["downstream"], []).append(edge["upstream"])

    objects, edges = [], []
    for node in payload["nodes"]:
        key_str = node["asset_key_string"]
        custom_props = {
            "dagster_group": node["group"] or "",
            "dagster_kinds": ",".join(node["kinds"]),
            "dagster_deployment": deployment,
        }
        for k, v in node.get("metadata", {}).items():
            custom_props[k] = str(v)
        objects.append({
            "objectIdentifier": key_str,
            "assetType": asset_type,
            "name": key_str,
            "description": node.get("description", ""),
            "externalUrl": f"{ui_url}/assets/{'/'.join(node['asset_key'])}" if ui_url else "",
            "customProperties": custom_props,
        })
        for upstream in parent_map.get(key_str, []):
            edges.append({
                "sourceIdentifier": upstream,
                "sourceAssetType": asset_type,
                "targetIdentifier": key_str,
                "targetAssetType": asset_type,
                "lineageType": "DATA_FLOW",
            })
    return objects, edges


def _push(log, objects, edges, catalog_base, token_url, client_id_env, client_secret_env, timeout_seconds):
    import requests
    client_id = os.environ.get(client_id_env, "")
    client_secret = os.environ.get(client_secret_env, "")
    if not client_id or not client_secret:
        raise RuntimeError(f"Data360 creds missing: set {client_id_env} and {client_secret_env}.")

    token_resp = requests.post(
        token_url,
        data={"grant_type": "client_credentials"},
        auth=(client_id, client_secret),
        timeout=timeout_seconds,
    )
    token_resp.raise_for_status()
    token = token_resp.json()["access_token"]
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    base = catalog_base.rstrip("/")
    pushed_objects = pushed_edges = 0
    for obj in objects:
        resp = requests.post(f"{base}/objects", json=obj, headers=headers, timeout=timeout_seconds)
        resp.raise_for_status()
        pushed_objects += 1
    for edge in edges:
        resp = requests.post(f"{base}/lineage", json=edge, headers=headers, timeout=timeout_seconds)
        resp.raise_for_status()
        pushed_edges += 1
    log.info(f"Data360: upserted {pushed_objects} objects + {pushed_edges} lineage edges")
    return pushed_objects, pushed_edges


class Data360ExportJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-shaped job that walks the asset graph and pushes lineage to Precisely Data360."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = manual / sensor-triggered).")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Dagster job tags")

    catalog_url: str = Field(
        default="https://api.data.precisely.com/data360/catalog",
        description="Data360 Govern Catalog REST base URL.",
    )
    token_url: str = Field(
        default=DATA360_DEFAULT_TOKEN_URL,
        description="OAuth2 token endpoint (override for non-global Data360 tenants).",
    )
    client_id_env: str = Field(
        default="PRECISELY_DIS_CLIENT_ID",
        description="Env var with the Data360 OAuth2 client_id.",
    )
    client_secret_env: str = Field(
        default="PRECISELY_DIS_CLIENT_SECRET",
        description="Env var with the Data360 OAuth2 client_secret.",
    )
    request_timeout_seconds: int = Field(default=30, ge=1, description="Per-request HTTP timeout.")

    only_export_on_change: bool = Field(default=True)
    fail_on_catalog_error: bool = Field(default=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _walk_and_push(context: dg.OpExecutionContext):
            repo = getattr(context, "repository_def", None)
            if repo is None:
                raise RuntimeError("context.repository_def unavailable — must run inside a code location.")
            payload = _build_payload_from_repo(repo)
            payload_hash = _hash_structural(payload)

            if _self.only_export_on_change:
                last_hash = None
                try:
                    runs = context.instance.get_runs(
                        filters=dg.RunsFilter(job_name=_self.job_name, statuses=[dg.DagsterRunStatus.SUCCESS]),
                        limit=2,
                    )
                    for r in runs:
                        if r.run_id == context.run_id:
                            continue
                        last_hash = (r.tags or {}).get("data360_export/payload_hash")
                        if last_hash:
                            break
                except Exception:
                    pass
                if last_hash == payload_hash:
                    context.log.info(f"Asset graph unchanged (hash={payload_hash}); skipping push.")
                    context.instance.add_run_tags(context.run_id, {"data360_export/payload_hash": payload_hash})
                    return {"pushed": False, "skipped_unchanged": True, "payload_hash": payload_hash}

            objects, edges = _transform(payload)
            try:
                pushed_objs, pushed_edges = _push(
                    context.log, objects, edges,
                    _self.catalog_url, _self.token_url,
                    _self.client_id_env, _self.client_secret_env,
                    _self.request_timeout_seconds,
                )
            except Exception as exc:
                if _self.fail_on_catalog_error:
                    raise
                context.log.warning(f"Data360 push failed ({type(exc).__name__}: {exc}) — continuing")
                return {"pushed": False, "error": str(exc), "payload_hash": payload_hash}

            try:
                context.instance.add_run_tags(context.run_id, {"data360_export/payload_hash": payload_hash})
            except Exception:
                pass

            return {
                "pushed": True,
                "objects_pushed": pushed_objs,
                "edges_pushed": pushed_edges,
                "payload_hash": payload_hash,
                "total_nodes": payload["sync_metadata"]["total_nodes"],
                "total_edges": payload["sync_metadata"]["total_edges"],
            }

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _walk_and_push()

        defs_kwargs: Dict[str, Any] = {"jobs": [_the_job]}
        if self.schedule:
            defs_kwargs["schedules"] = [dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule,
                job=_the_job,
                default_status=(
                    dg.DefaultScheduleStatus.STOPPED
                    if self.default_status.upper() == "STOPPED"
                    else dg.DefaultScheduleStatus.RUNNING
                ),
            )]
        return dg.Definitions(**defs_kwargs)
