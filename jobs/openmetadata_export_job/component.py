"""OpenMetadataExportJobComponent.

Op-shaped job that walks the live asset graph and pushes lineage +
metadata to OpenMetadata's REST API. Self-contained — no shared
helpers; duplicates the asset-graph walk + OpenMetadata push logic
inline so the file works dropped into any Dagster project as-is.

Complements (does not replace) the asset-shaped `lineage_to_openmetadata`
sink. The asset version requires a `lineage_graph_extractor` +
`lineage_graph` + `lineage_to_openmetadata` 3-asset chain, materialized
on its own schedule. This job is a single op that does the whole thing
in one tick — useful for teams that want a job-shaped, schedulable
"export Dagster lineage to my OpenMetadata catalog" without adding
three new assets to their graph.
"""

import hashlib
import json
import os
import time
from typing import Any, Dict, List, Optional, Set

import dagster as dg
from pydantic import Field


# ── asset-graph walk ─────────────────────────────────────────────────────
def _build_payload_from_repo(repo_def) -> Dict[str, Any]:
    """Walk the live asset graph and produce a canonical lineage payload.

    Mirrors lineage_graph_extractor's _build_payload_from_repo so this
    component stays self-contained.
    """
    asset_graph = repo_def.asset_graph
    all_keys = list(asset_graph.toposorted_asset_keys)

    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, str]] = []

    for key in all_keys:
        node = asset_graph.get(key)
        key_str = key.to_user_string()

        # JSON-safe metadata (drop dagster-internal namespaces, stringify the rest)
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
            edges.append({
                "upstream": parent_key.to_user_string(),
                "downstream": key_str,
            })

    return {
        "source_system": {
            "dagster_ui_url": os.environ.get("DAGSTER_UI_URL", ""),
            "deployment": os.environ.get("DAGSTER_DEPLOYMENT", ""),
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
    structural = json.dumps(
        {"nodes": payload["nodes"], "edges": payload["edges"]},
        sort_keys=True,
    )
    return hashlib.sha256(structural.encode()).hexdigest()[:16]


# ── OpenMetadata transform + push (mirrors lineage_to_openmetadata sink) ──
def _safe_part(name: str) -> str:
    """OpenMetadata FQN parts must be ASCII-safe."""
    if not name:
        return "default"
    out = []
    for ch in name:
        if ch.isalnum() or ch in "_-":
            out.append(ch)
        else:
            out.append("_")
    return "".join(out) or "default"


def _build_plan(payload: dict, service_name: str, database_name: str) -> dict:
    """Walk the payload and emit a plan of OpenMetadata entities +
    lineage edges to push."""
    ss = payload.get("source_system", {})
    ui_url = ss.get("dagster_ui_url", "")
    deployment = ss.get("deployment", "")

    svc_part = _safe_part(service_name)
    db_part = _safe_part(database_name)
    service_fqn = svc_part
    database_fqn = f"{svc_part}.{db_part}"

    schemas_seen: Set[str] = set()
    schemas: List[dict] = []
    tables: List[dict] = []
    table_fqn_by_key: Dict[str, str] = {}

    for node in payload["nodes"]:
        key_str = node["asset_key_string"]
        group = node.get("group") or "default"
        schema_part = _safe_part(group)
        table_part = _safe_part(key_str)

        schema_fqn = f"{svc_part}.{db_part}.{schema_part}"
        table_fqn = f"{schema_fqn}.{table_part}"
        table_fqn_by_key[key_str] = table_fqn

        if schema_part not in schemas_seen:
            schemas_seen.add(schema_part)
            schemas.append({"name": schema_part, "database_fqn": database_fqn})

        external_url = ""
        if ui_url and node.get("asset_key"):
            external_url = f"{ui_url.rstrip('/')}/assets/{'/'.join(node['asset_key'])}"

        # OpenMetadata Table.columns requires at least one column; emit
        # one synthetic "_value" column when the asset has no schema.
        # Real schema-bearing assets get one column per metadata key.
        raw_metadata = node.get("metadata") or {}
        columns: List[dict] = []
        for k, v in raw_metadata.items():
            columns.append({
                "name": _safe_part(k),
                "dataType": "STRING",
                "description": f"From Dagster metadata: {k}",
            })
        if not columns:
            columns = [{"name": "_value", "dataType": "STRING"}]

        tables.append({
            "name": table_part,
            "schema_fqn": schema_fqn,
            "description": node.get("description") or "",
            "external_url": external_url,
            "columns": columns,
            "tags_props": {
                "dagster_group": group,
                "dagster_kinds": ",".join(node.get("kinds") or []),
                "dagster_deployment": deployment,
                "dagster_asset_key": key_str,
            },
        })

    edges: List[dict] = []
    for edge in payload["edges"]:
        u_fqn = table_fqn_by_key.get(edge["upstream"])
        d_fqn = table_fqn_by_key.get(edge["downstream"])
        if not (u_fqn and d_fqn):
            continue
        edges.append({"from_fqn": u_fqn, "to_fqn": d_fqn})

    return {
        "service": {"name": svc_part, "fqn": service_fqn},
        "database": {"name": db_part, "service_fqn": service_fqn, "fqn": database_fqn},
        "schemas": schemas,
        "tables": tables,
        "edges": edges,
    }


def _push(log, plan: dict, base_url: str, token_env: str, verify_ssl: bool):
    """Push the plan to OpenMetadata. Each PUT is idempotent (upsert).

    Order matters: service → database → schemas → tables → lineage edges.
    """
    import requests

    token = os.environ.get(token_env)
    if not token:
        raise RuntimeError(f"Missing {token_env} environment variable")
    base = base_url.rstrip("/")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    def _put(path: str, body: dict, label: str):
        url = f"{base}/api/v1/{path}"
        resp = requests.put(url, json=body, headers=headers, timeout=30, verify=verify_ssl)
        if not resp.ok:
            raise RuntimeError(
                f"OpenMetadata PUT {path} failed ({resp.status_code}) for {label}: {resp.text[:300]}"
            )

    # 1. Database service. OpenMetadata requires a concrete serviceType;
    # MySQL is a stable choice that renders cleanly in the UI. The
    # displayName makes the Dagster origin obvious.
    _put(
        "services/databaseServices",
        {
            "name": plan["service"]["name"],
            "displayName": "Dagster",
            "serviceType": "Mysql",
            "connection": {"config": {"type": "Mysql"}},
        },
        f"service {plan['service']['name']}",
    )

    # 2. Database.
    _put(
        "databases",
        {
            "name": plan["database"]["name"],
            "service": plan["database"]["service_fqn"],
        },
        f"database {plan['database']['name']}",
    )

    # 3. Schemas.
    for s in plan["schemas"]:
        _put(
            "databaseSchemas",
            {"name": s["name"], "database": s["database_fqn"]},
            f"schema {s['name']}",
        )

    # 4. Tables.
    for t in plan["tables"]:
        body = {
            "name": t["name"],
            "databaseSchema": t["schema_fqn"],
            "description": t["description"],
            "columns": t["columns"],
        }
        if t.get("external_url"):
            body["sourceUrl"] = t["external_url"]
        if t.get("tags_props"):
            body["extension"] = t["tags_props"]
        _put("tables", body, f"table {t['name']}")

    # 5. Lineage edges.
    for e in plan["edges"]:
        _put(
            "lineage",
            {
                "edge": {
                    "fromEntity": {"id": e["from_fqn"], "type": "table"},
                    "toEntity": {"id": e["to_fqn"], "type": "table"},
                }
            },
            f"lineage {e['from_fqn']} -> {e['to_fqn']}",
        )

    log.info(
        f"OpenMetadata: ensured 1 service, 1 db, {len(plan['schemas'])} schemas, "
        f"{len(plan['tables'])} tables, {len(plan['edges'])} lineage edges"
    )


# ── component ────────────────────────────────────────────────────────────
class OpenMetadataExportJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-shaped job that walks the asset graph and pushes lineage +
    metadata to OpenMetadata.

    On each run, the op:
      1. Walks the live asset graph (current code location).
      2. Builds an OpenMetadata plan (service → database → schemas →
         tables → lineage edges).
      3. PUTs each entity idempotently to `{catalog_url}/api/v1/...`.

    Pair with `schedule:` for a recurring catalog sync, or trigger
    manually when the asset graph changes meaningfully.
    """

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = manual / sensor-triggered).")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Dagster job tags")

    # OpenMetadata destination
    catalog_url: str = Field(
        default="https://openmetadata.example.com",
        description="OpenMetadata host (e.g. https://openmetadata.acme.com). /api/v1/... is appended.",
    )
    api_token_env: str = Field(
        default="OPENMETADATA_API_TOKEN",
        description="Env var holding the JWT bearer token. Generate one in OpenMetadata under Settings → Bots.",
    )
    service_name: str = Field(
        default="dagster",
        description="OpenMetadata DatabaseService name to ensure-exist + parent the asset graph under.",
    )
    database_name: str = Field(
        default="default",
        description="OpenMetadata Database name under the service.",
    )
    verify_ssl: bool = Field(
        default=True,
        description="Verify TLS cert. Set False for self-signed dev OpenMetadata.",
    )

    # Behavior
    only_export_on_change: bool = Field(
        default=True,
        description=(
            "If True, hash the structural payload and skip the push when the hash "
            "matches the previous run's. Stored as job-tag metadata on the run."
        ),
    )
    fail_on_catalog_error: bool = Field(
        default=True,
        description=(
            "If True, an unreachable / errored catalog fails the run. Default True — "
            "OpenMetadata PUTs are idempotent so failure usually indicates a real "
            "config issue worth surfacing rather than silently retrying next tick."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _walk_and_push(context: dg.OpExecutionContext):
            # Walk the live asset graph from the current code location.
            repo = getattr(context, "repository_def", None)
            if repo is None:
                raise RuntimeError(
                    "context.repository_def is unavailable — this job must run inside "
                    "a loaded code location."
                )
            payload = _build_payload_from_repo(repo)
            payload_hash = _hash_structural(payload)

            # Change-detection: skip when the structural hash matches the
            # previous successful run (stored as a run tag).
            if _self.only_export_on_change:
                last_hash = None
                try:
                    runs = context.instance.get_runs(
                        filters=dg.RunsFilter(
                            job_name=_self.job_name,
                            statuses=[dg.DagsterRunStatus.SUCCESS],
                        ),
                        limit=2,  # current + most recent prior
                    )
                    for r in runs:
                        if r.run_id == context.run_id:
                            continue
                        last_hash = (r.tags or {}).get("openmetadata_export/payload_hash")
                        if last_hash:
                            break
                except Exception:
                    pass  # best-effort

                if last_hash == payload_hash:
                    context.log.info(
                        f"Asset graph unchanged (hash={payload_hash}); skipping push of "
                        f"{payload['sync_metadata']['total_nodes']} nodes / "
                        f"{payload['sync_metadata']['total_edges']} edges."
                    )
                    context.instance.add_run_tags(
                        context.run_id, {"openmetadata_export/payload_hash": payload_hash}
                    )
                    return {
                        "pushed": False,
                        "skipped_unchanged": True,
                        "payload_hash": payload_hash,
                    }

            # Build the OpenMetadata entity plan and push.
            plan = _build_plan(payload, _self.service_name, _self.database_name)
            try:
                _push(context.log, plan, _self.catalog_url, _self.api_token_env, _self.verify_ssl)
            except Exception as exc:
                if _self.fail_on_catalog_error:
                    raise
                context.log.warning(
                    f"OpenMetadata push failed ({type(exc).__name__}: {exc}) — "
                    "continuing because fail_on_catalog_error=false"
                )
                return {
                    "pushed": False,
                    "error": str(exc),
                    "payload_hash": payload_hash,
                }

            try:
                context.instance.add_run_tags(
                    context.run_id, {"openmetadata_export/payload_hash": payload_hash}
                )
            except Exception:
                pass

            return {
                "pushed": True,
                "service": plan["service"]["name"],
                "schemas_pushed": len(plan["schemas"]),
                "tables_pushed": len(plan["tables"]),
                "edges_pushed": len(plan["edges"]),
                "payload_hash": payload_hash,
                "total_nodes": payload["sync_metadata"]["total_nodes"],
                "total_edges": payload["sync_metadata"]["total_edges"],
            }

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _walk_and_push()

        defs_kwargs: Dict[str, Any] = {"jobs": [_the_job]}
        if self.schedule:
            sched = dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule,
                job=_the_job,
                default_status=(
                    dg.DefaultScheduleStatus.STOPPED
                    if self.default_status.upper() == "STOPPED"
                    else dg.DefaultScheduleStatus.RUNNING
                ),
            )
            defs_kwargs["schedules"] = [sched]
        return dg.Definitions(**defs_kwargs)
