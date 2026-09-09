"""PurviewExportJobComponent.

Op-shaped job that walks the live asset graph and pushes lineage to
Microsoft Purview Data Map (Apache Atlas v2 entity bulk API).

Complements the asset-shaped `lineage_to_purview` sink. Prefer this
job shape for scheduled "sync my Dagster asset graph to Purview"
workloads; use the asset chain when you want automation-condition-
driven pushes.
"""

import hashlib
import json
import os
import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


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
            "platform": "dagster",
            "deployment": os.environ.get("DAGSTER_DEPLOYMENT", "local"),
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


def _transform(payload):
    ss = payload.get("source_system", {})
    platform = ss.get("platform", "dagster")
    deployment = ss.get("deployment", "local")
    qn_prefix = f"{platform}://{deployment}"

    entities = []
    for node in payload["nodes"]:
        key_str = node["asset_key_string"]
        entities.append({
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": f"{qn_prefix}/{key_str}",
                "name": key_str,
                "description": (node.get("description") or "")[:500],
                "userDescription": (node.get("description") or "")[:500],
            },
            "guid": f"-{abs(hash(key_str)) % 10**12}",
        })

    for i, edge in enumerate(payload["edges"]):
        up, dn = edge["upstream"], edge["downstream"]
        edge_id = f"{up}->{dn}"
        entities.append({
            "typeName": "Process",
            "attributes": {
                "qualifiedName": f"{qn_prefix}/process/{up}__to__{dn}",
                "name": f"dagster_transform_{i}",
                "inputs": [{"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": f"{qn_prefix}/{up}"}}],
                "outputs": [{"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": f"{qn_prefix}/{dn}"}}],
            },
            "guid": f"-{abs(hash(edge_id)) % 10**12}",
        })
    return {"entities": entities}


def _push(log, transformed, base_url, token_env):
    import requests
    token = os.environ.get(token_env)
    if not token:
        raise RuntimeError(f"Missing {token_env} environment variable")
    resp = requests.post(
        f"{base_url}/datamap/api/atlas/v2/entity/bulk",
        json=transformed,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    n = len(transformed.get("entities", []))
    log.info(f"Purview: ingested {n} Atlas entities (DataSets + Process lineage)")


class PurviewExportJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-shaped job that walks the asset graph and pushes lineage to Microsoft Purview."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = manual / sensor-triggered).")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Dagster job tags")

    catalog_url: str = Field(
        default="https://my-account.purview.azure.com",
        description="Purview account base URL. /datamap/api/atlas/v2/entity/bulk is appended.",
    )
    api_token_env: str = Field(
        default="PURVIEW_ACCESS_TOKEN",
        description="Env var with the Purview access token (Azure AD bearer).",
    )

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
                        last_hash = (r.tags or {}).get("purview_export/payload_hash")
                        if last_hash:
                            break
                except Exception:
                    pass
                if last_hash == payload_hash:
                    context.log.info(f"Asset graph unchanged (hash={payload_hash}); skipping push.")
                    context.instance.add_run_tags(context.run_id, {"purview_export/payload_hash": payload_hash})
                    return {"pushed": False, "skipped_unchanged": True, "payload_hash": payload_hash}

            transformed = _transform(payload)
            try:
                _push(context.log, transformed, _self.catalog_url, _self.api_token_env)
            except Exception as exc:
                if _self.fail_on_catalog_error:
                    raise
                context.log.warning(f"Purview push failed ({type(exc).__name__}: {exc}) — continuing")
                return {"pushed": False, "error": str(exc), "payload_hash": payload_hash}

            try:
                context.instance.add_run_tags(context.run_id, {"purview_export/payload_hash": payload_hash})
            except Exception:
                pass

            return {
                "pushed": True,
                "entities_pushed": len(transformed["entities"]),
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
