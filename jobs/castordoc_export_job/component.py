"""CastordocExportJobComponent.

Op-shaped job that walks the live asset graph and enriches matching Castor
(CastorDoc / Coalesce Catalog) tables via its public GraphQL API.

Complements the asset-shaped `lineage_to_castordoc` sink. Prefer this job
shape for scheduled "sync my whole Dagster asset graph to Castor" workloads
(no lineage assets added to your graph); use the asset chain when you want
automation-condition-driven pushes.

Architecture note (mirrors lineage_to_castordoc/component.py -- read that
file's module docstring for the full writeup): Castor's public API has no
concept of registering a brand-new external/custom lineage node the way
Alation's dataflow objects do. Both `upsertLineages` and the UI-based
"Manual Lineage Importer" only connect TABLE/DASHBOARD records Castor's own
connectors already discovered. So this job resolves each Dagster asset to
an existing Castor table by exact, case-insensitive name match (on the
asset key's last path segment) and enriches that record -- it does not
and cannot make Dagster itself a lineage-graph node in Castor. Unmatched
assets, and edges with an unmatched endpoint, are skipped and counted,
never forced or silently dropped.
"""

import hashlib
import json
import os
import time
from typing import Any, Dict, List, Optional

import dagster as dg
import requests
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


# ── Castor transform + push (mirrors lineage_to_castordoc sink) ───────────
def _transform(payload):
    nodes_out = []
    for node in payload["nodes"]:
        asset_key = node.get("asset_key") or []
        match_name = asset_key[-1] if asset_key else node["asset_key_string"]
        nodes_out.append({
            "asset_key_string": node["asset_key_string"],
            "match_name": match_name,
            "description": node.get("description") or "",
            "group": node.get("group") or "",
            "kinds": node.get("kinds") or [],
        })
    edges_out = [
        {"upstream": e["upstream"], "downstream": e["downstream"]}
        for e in payload.get("edges", [])
    ]
    return {"nodes": nodes_out, "edges": edges_out}


def _castor_call(base_url, headers, op_name, query, variables):
    resp = requests.post(
        f"{base_url}?op={op_name}",
        headers=headers,
        json={"query": query, "variables": variables},
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("errors"):
        raise RuntimeError(f"Castor {op_name!r} returned errors: {body['errors']}")
    return (body.get("data") or {}).get(op_name)


def _resolve_table_id(base_url, headers, name):
    query = """
    query GetTables($scope: GetTablesScope) {
      getTables(scope: $scope) { data { id name } }
    }
    """
    result = _castor_call(base_url, headers, "getTables", query, {"scope": {"nameContains": name}})
    for row in (result or {}).get("data") or []:
        if (row.get("name") or "").lower() == name.lower():
            return row.get("id")
    return None


def _push(log, transformed, base_url, token_env, push_tags=True, push_data_quality=False, push_lineage_edges=True):
    token = os.environ.get(token_env)
    if not token:
        raise RuntimeError(f"Missing {token_env} environment variable")
    headers = {"Authorization": f"Token {token}", "Content-Type": "application/json"}

    match_name_to_id: Dict[str, Optional[str]] = {}
    for n in transformed["nodes"]:
        if n["match_name"] not in match_name_to_id:
            match_name_to_id[n["match_name"]] = _resolve_table_id(base_url, headers, n["match_name"])

    matched: Dict[str, Optional[str]] = {
        n["asset_key_string"]: match_name_to_id[n["match_name"]] for n in transformed["nodes"]
    }
    matched_count = sum(1 for v in matched.values() if v)
    unmatched_count = sum(1 for v in matched.values() if not v)

    description_inputs = []
    tag_inputs = []
    for n in transformed["nodes"]:
        table_id = matched.get(n["asset_key_string"])
        if not table_id:
            continue
        if n["description"]:
            description_inputs.append({"id": table_id, "externalDescription": n["description"]})
        if push_tags:
            tag_inputs.append({"entityType": "TABLE", "entityId": table_id, "label": "dagster"})
            if n["group"]:
                tag_inputs.append({"entityType": "TABLE", "entityId": table_id, "label": f"dagster:group:{n['group']}"})
            for kind in n["kinds"]:
                tag_inputs.append({"entityType": "TABLE", "entityId": table_id, "label": f"dagster:kind:{kind}"})

    if description_inputs:
        _castor_call(
            base_url, headers, "updateTableDescriptions",
            "mutation UpdateTableDescriptions($data: [UpdateTableDescriptionInput!]!) "
            "{ updateTableDescriptions(data: $data) { id } }",
            {"data": description_inputs},
        )

    if push_tags and tag_inputs:
        _castor_call(
            base_url, headers, "attachTags",
            "mutation AttachTags($data: [BaseTagEntityInput!]!) { attachTags(data: $data) }",
            {"data": tag_inputs},
        )

    quality_pushed = 0
    if push_data_quality:
        run_at = time.strftime("%Y-%m-%dT%H:%M:%SZ")
        for n in transformed["nodes"]:
            table_id = matched.get(n["asset_key_string"])
            if not table_id:
                continue
            _castor_call(
                base_url, headers, "upsertDataQualities",
                "mutation UpsertDataQualities($data: UpsertQualityChecksInput!) "
                "{ upsertDataQualities(data: $data) { id } }",
                {"data": {
                    "tableId": table_id,
                    "qualityChecks": [{
                        "externalId": f"dagster:{n['asset_key_string']}:tracked",
                        "name": "Tracked by Dagster",
                        "status": "SUCCESS",
                        "runAt": run_at,
                        "description": (
                            "This table is orchestrated/tracked by a Dagster asset. "
                            "Not a real test outcome -- see README Architecture notes."
                        ),
                    }],
                }},
            )
            quality_pushed += 1

    lineage_pushed = 0
    lineage_skipped = 0
    if push_lineage_edges:
        lineage_inputs = []
        for e in transformed["edges"]:
            parent_id = matched.get(e["upstream"])
            child_id = matched.get(e["downstream"])
            if parent_id and child_id:
                lineage_inputs.append({"parentTableId": parent_id, "childTableId": child_id})
            else:
                lineage_skipped += 1
        if lineage_inputs:
            _castor_call(
                base_url, headers, "upsertLineages",
                "mutation UpsertLineages($data: [UpsertLineageInput!]!) "
                "{ upsertLineages(data: $data) { id } }",
                {"data": lineage_inputs},
            )
        lineage_pushed = len(lineage_inputs)

    log.info(
        f"Castor sync: {matched_count} assets matched to existing tables "
        f"({unmatched_count} unmatched, skipped); {len(description_inputs)} descriptions, "
        f"{len(tag_inputs)} tags, {quality_pushed} quality records, "
        f"{lineage_pushed} lineage edges pushed ({lineage_skipped} edges skipped, endpoint(s) unmatched)."
    )
    return {
        "matched": matched_count,
        "unmatched": unmatched_count,
        "descriptions_pushed": len(description_inputs),
        "tags_attached": len(tag_inputs),
        "quality_records_pushed": quality_pushed,
        "lineage_edges_pushed": lineage_pushed,
        "lineage_edges_skipped": lineage_skipped,
    }


# ── component ────────────────────────────────────────────────────────────
class CastordocExportJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-shaped job that walks the asset graph and enriches matching Castor tables."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = manual / sensor-triggered).")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Dagster job tags")

    catalog_url: str = Field(
        default="https://api.castordoc.com/public/graphql",
        description=(
            "Castor/Coalesce Catalog GraphQL endpoint. EU tenants use api.castordoc.com "
            "(default); US tenants use https://api.us.castordoc.com/public/graphql."
        ),
    )
    api_token_env: str = Field(
        default="CASTORDOC_API_TOKEN",
        description="Env var holding the Castor API token (sent as `Token` header, not Bearer).",
    )

    push_tags: bool = Field(
        default=True,
        description="Attach `dagster` / `dagster:group:<group>` / `dagster:kind:<kind>` tags onto matched Castor tables via attachTags.",
    )
    push_data_quality: bool = Field(
        default=False,
        description=(
            "Push a best-effort 'tracked by Dagster' presence record via upsertDataQualities on each "
            "matched table. NOT a real test result -- the asset-graph walk carries no pass/fail data."
        ),
    )
    push_lineage_edges: bool = Field(
        default=True,
        description=(
            "Push lineage edges via upsertLineages for asset-graph edges where BOTH endpoints already "
            "resolve to an existing Castor table. Edges with an unmatched endpoint are skipped (Castor "
            "has no concept of an external/custom node)."
        ),
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
                        last_hash = (r.tags or {}).get("castordoc_export/payload_hash")
                        if last_hash:
                            break
                except Exception:
                    pass
                if last_hash == payload_hash:
                    context.log.info(f"Asset graph unchanged (hash={payload_hash}); skipping push.")
                    context.instance.add_run_tags(context.run_id, {"castordoc_export/payload_hash": payload_hash})
                    return {"pushed": False, "skipped_unchanged": True, "payload_hash": payload_hash}

            transformed = _transform(payload)
            try:
                result = _push(
                    context.log, transformed, _self.catalog_url, _self.api_token_env,
                    push_tags=_self.push_tags, push_data_quality=_self.push_data_quality,
                    push_lineage_edges=_self.push_lineage_edges,
                )
            except Exception as exc:
                if _self.fail_on_catalog_error:
                    raise
                context.log.warning(f"Castor push failed ({type(exc).__name__}: {exc}) — continuing")
                return {"pushed": False, "error": str(exc), "payload_hash": payload_hash}

            try:
                context.instance.add_run_tags(context.run_id, {"castordoc_export/payload_hash": payload_hash})
            except Exception:
                pass

            return {
                "pushed": True,
                "payload_hash": payload_hash,
                "total_nodes": payload["sync_metadata"]["total_nodes"],
                "total_edges": payload["sync_metadata"]["total_edges"],
                **result,
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
