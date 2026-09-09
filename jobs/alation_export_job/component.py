"""AlationExportJobComponent.

Op-shaped job that walks the live asset graph and pushes lineage to
Alation Data Catalog via the Integration v2 lineage endpoint.

Complements the asset-shaped `lineage_to_alation` sink. Prefer this job
shape for scheduled "sync my Dagster asset graph to Alation" workloads
(no lineage assets added to your graph); use the asset chain when you
want automation-condition-driven pushes.
"""

import hashlib
import json
import os
import time
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


# ── asset-graph walk (self-contained; mirrors lineage_graph_extractor) ────
def _build_payload_from_repo(repo_def) -> Dict[str, Any]:
    asset_graph = repo_def.asset_graph
    all_keys = list(asset_graph.toposorted_asset_keys)
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, str]] = []
    for key in all_keys:
        node = asset_graph.get(key)
        key_str = key.to_user_string()
        raw_metadata = node.metadata or {}
        safe_metadata: Dict[str, Any] = {}
        for k, v in raw_metadata.items():
            if k.startswith(("dagster_dbt/", "dagster/")):
                continue
            try:
                json.dumps(v)
                safe_metadata[k] = v
            except Exception:
                safe_metadata[k] = str(v)
        fp = getattr(node, "freshness_policy_or_from_metadata", None)
        nodes.append({
            "asset_key": key.path,
            "asset_key_string": key_str,
            "group": node.group_name,
            "kinds": sorted(node.kinds) if node.kinds else [],
            "description": (node.description or "")[:500],
            "metadata": safe_metadata,
            "freshness_policy": str(fp) if fp else None,
        })
        for parent_key in node.parent_keys:
            edges.append({"upstream": parent_key.to_user_string(), "downstream": key_str})
    return {
        "source_system": {
            "platform": "dagster",
            "deployment": os.environ.get("DAGSTER_DEPLOYMENT", ""),
            "organization": os.environ.get("DAGSTER_ORGANIZATION", ""),
            "dagster_ui_url": os.environ.get("DAGSTER_UI_URL", ""),
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


# ── Alation transform + push (mirrors lineage_to_alation sink) ────────────
def _transform(payload):
    ss = payload.get("source_system", {})
    platform = ss.get("platform", "dagster")
    deployment = ss.get("deployment", "")
    ui_url = ss.get("dagster_ui_url", "")
    prefix = f"api/{platform}/{deployment}" if deployment else f"api/{platform}"

    dataflow_objects = []
    for node in payload["nodes"]:
        asset_path = "/".join(node["asset_key"])
        dataflow_objects.append({
            "external_id": f"{prefix}/{asset_path}",
            "title": node["asset_key_string"],
            "description": node.get("description", ""),
            "url": f"{ui_url}/assets/{asset_path}" if ui_url else "",
            "content": json.dumps({
                "source_platform": platform,
                "deployment": deployment,
                "organization": ss.get("organization", ""),
                "group": node["group"],
                "kinds": node["kinds"],
                "metadata": node["metadata"],
                "freshness_policy": node["freshness_policy"],
            }),
        })

    paths = []
    for edge in payload["edges"]:
        up, dn = edge["upstream"], edge["downstream"]
        paths.append([
            [{"otype": "external", "key": f"{prefix}/{up}"}],
            [{"otype": "dataflow", "key": f"{prefix}/{dn}"}],
            [{"otype": "external", "key": f"{prefix}/{dn}"}],
        ])

    return {"dataflow_objects": dataflow_objects, "paths": paths}


def _push(log, transformed, base_url, token_env):
    import requests
    token = os.environ.get(token_env)
    if not token:
        raise RuntimeError(f"Missing {token_env} environment variable")
    resp = requests.post(
        f"{base_url}/integration/v2/lineage/",
        json=transformed,
        headers={"TOKEN": token, "Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    log.info(f"Alation lineage job submitted: {resp.json()}")


# ── component ────────────────────────────────────────────────────────────
class AlationExportJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-shaped job that walks the asset graph and pushes lineage to Alation."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = manual / sensor-triggered).")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Dagster job tags")

    catalog_url: str = Field(
        default="https://alation.example.com",
        description="Alation base URL. /integration/v2/lineage/ is appended.",
    )
    api_token_env: str = Field(
        default="ALATION_API_TOKEN",
        description="Env var holding the Alation API token (sent as `TOKEN` header).",
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
                        last_hash = (r.tags or {}).get("alation_export/payload_hash")
                        if last_hash:
                            break
                except Exception:
                    pass
                if last_hash == payload_hash:
                    context.log.info(f"Asset graph unchanged (hash={payload_hash}); skipping push.")
                    context.instance.add_run_tags(context.run_id, {"alation_export/payload_hash": payload_hash})
                    return {"pushed": False, "skipped_unchanged": True, "payload_hash": payload_hash}

            transformed = _transform(payload)
            try:
                _push(context.log, transformed, _self.catalog_url, _self.api_token_env)
            except Exception as exc:
                if _self.fail_on_catalog_error:
                    raise
                context.log.warning(f"Alation push failed ({type(exc).__name__}: {exc}) — continuing")
                return {"pushed": False, "error": str(exc), "payload_hash": payload_hash}

            try:
                context.instance.add_run_tags(context.run_id, {"alation_export/payload_hash": payload_hash})
            except Exception:
                pass

            return {
                "pushed": True,
                "dataflow_objects": len(transformed["dataflow_objects"]),
                "paths": len(transformed["paths"]),
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
