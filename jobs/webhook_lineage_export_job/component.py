"""WebhookLineageExportJobComponent.

Op-shaped job that walks the live asset graph and POSTs the raw payload
to any HTTP webhook (Slack, internal endpoint, n8n, Zapier, whatever).
Optional Bearer auth.

Complements the asset-shaped `lineage_to_webhook` sink. Prefer this
job shape for scheduled "push my Dagster lineage to X endpoint"
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
            "deployment": os.environ.get("DAGSTER_DEPLOYMENT", ""),
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


def _push(log, transformed, base_url, token_env):
    import requests
    headers = {"Content-Type": "application/json"}
    token = os.environ.get(token_env) if token_env else None
    if token:
        headers["Authorization"] = f"Bearer {token}"
    resp = requests.post(base_url, json=transformed, headers=headers, timeout=30)
    resp.raise_for_status()
    log.info(f"Webhook: {resp.status_code}")


class WebhookLineageExportJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-shaped job that walks the asset graph and POSTs the raw
    lineage payload to any HTTP webhook.

    Distinct from the more general `http_webhook_job` (which posts a
    static / configurable body). This one specifically composes the
    Dagster asset-graph lineage payload and pushes THAT — matching the
    asset-shaped `lineage_to_webhook` sink.
    """

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = manual / sensor-triggered).")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Dagster job tags")

    webhook_url: str = Field(
        default="https://example.com/lineage-webhook",
        description="Full webhook URL to POST the lineage payload to.",
    )
    api_token_env: Optional[str] = Field(
        default=None,
        description="Optional env var with a Bearer token. Omit for unauthenticated webhooks.",
    )

    only_export_on_change: bool = Field(default=True)
    fail_on_webhook_error: bool = Field(default=True)

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
                        last_hash = (r.tags or {}).get("webhook_lineage_export/payload_hash")
                        if last_hash:
                            break
                except Exception:
                    pass
                if last_hash == payload_hash:
                    context.log.info(f"Asset graph unchanged (hash={payload_hash}); skipping push.")
                    context.instance.add_run_tags(context.run_id, {"webhook_lineage_export/payload_hash": payload_hash})
                    return {"pushed": False, "skipped_unchanged": True, "payload_hash": payload_hash}

            try:
                _push(context.log, payload, _self.webhook_url, _self.api_token_env)
            except Exception as exc:
                if _self.fail_on_webhook_error:
                    raise
                context.log.warning(f"Webhook push failed ({type(exc).__name__}: {exc}) — continuing")
                return {"pushed": False, "error": str(exc), "payload_hash": payload_hash}

            try:
                context.instance.add_run_tags(context.run_id, {"webhook_lineage_export/payload_hash": payload_hash})
            except Exception:
                pass

            return {
                "pushed": True,
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
