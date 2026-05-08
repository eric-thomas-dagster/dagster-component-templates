"""OpenLineageExportJobComponent.

Op-shaped job that walks the live asset graph and emits one OpenLineage
RunEvent per asset to a configured collector (Marquez, OpenLineage Proxy,
Datakin). Self-contained — no shared helpers; duplicates the asset-graph
walk inline so the file works dropped into any Dagster project as-is.

Complements (does not replace) the asset-shaped lineage_to_openlineage
sink. The asset version requires building lineage_graph_extractor +
lineage_graph + lineage_to_openlineage as a 3-asset chain, materialized
on its own schedule. This job is a single op that does the whole thing
in one tick — useful for teams that want a job-shaped, schedulable
"export Dagster lineage to my OpenLineage collector" without adding
three new assets to their graph.
"""

import hashlib
import json
import os
import time
import uuid
from typing import Any, Dict, List, Optional

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
        },
        "nodes": nodes,
        "edges": edges,
    }


# ── OpenLineage transform ────────────────────────────────────────────────
def _hash_structural(payload: Dict[str, Any]) -> str:
    structural = json.dumps(
        {"nodes": payload["nodes"], "edges": payload["edges"]},
        sort_keys=True,
    )
    return hashlib.sha256(structural.encode()).hexdigest()[:16]


def _datasets_for_asset(asset_key_str: str, namespace: str, node: Dict[str, Any]) -> Dict[str, Any]:
    """Build a single OpenLineage Dataset reference for an asset."""
    facets: Dict[str, Any] = {
        "dagster_metadata": {
            "_producer": namespace,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DagsterMetadataFacet.json",
            "group": node["group"],
            "kinds": node["kinds"],
            "freshness_policy": node["freshness_policy"],
        },
    }
    if node.get("metadata"):
        facets["schema"] = {
            "_producer": namespace,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SchemaDatasetFacet.json",
            "fields": [{"name": k, "type": str(type(v).__name__)} for k, v in node["metadata"].items()],
        }
    return {"namespace": namespace, "name": asset_key_str, "facets": facets, "inputFacets": {}}


def _build_per_asset_events(payload: Dict[str, Any], namespace: str, run_id: str, producer: str) -> List[Dict[str, Any]]:
    """Emit one OpenLineage RunEvent per asset, with this-asset as the output and parents as inputs.

    Each event is a self-contained COMPLETE for the asset's place in the
    graph. Collectors that index by job + run id will see the full
    deployment lineage in one batch.
    """
    nodes_by_key = {n["asset_key_string"]: n for n in payload["nodes"]}
    parents_by_key: Dict[str, List[str]] = {}
    for edge in payload["edges"]:
        parents_by_key.setdefault(edge["downstream"], []).append(edge["upstream"])

    events: List[Dict[str, Any]] = []
    event_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    for node in payload["nodes"]:
        key_str = node["asset_key_string"]
        outputs = [_datasets_for_asset(key_str, namespace, node)]
        inputs = [
            _datasets_for_asset(p, namespace, nodes_by_key[p])
            for p in parents_by_key.get(key_str, [])
            if p in nodes_by_key
        ]
        events.append({
            "eventType": "COMPLETE",
            "eventTime": event_time,
            "producer": producer,
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
            "run": {"runId": run_id},
            "job": {"namespace": namespace, "name": f"dagster.{key_str}"},
            "inputs": inputs,
            "outputs": outputs,
        })
    return events


# ── HTTP emit ────────────────────────────────────────────────────────────
def _post(endpoint: str, headers: Dict[str, str], body: Dict[str, Any], log) -> bool:
    """POST a single event. Returns True on success, False on failure (logs warning)."""
    import urllib.error
    import urllib.request

    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        endpoint,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json", **headers},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return 200 <= resp.status < 300
    except urllib.error.HTTPError as e:
        log.warning(f"OpenLineage collector returned {e.code}: {e.read()[:200]!r}")
        return False
    except urllib.error.URLError as e:
        log.warning(f"OpenLineage collector unreachable: {e.reason}")
        return False


# ── component ────────────────────────────────────────────────────────────
class OpenLineageExportJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Op-shaped job that walks the asset graph and emits OpenLineage events per asset.

    On each run, the op:
      1. Walks the live asset graph (current code location).
      2. Builds one OpenLineage RunEvent per asset, with the asset as the
         output dataset and its parents as input datasets.
      3. POSTs each event to the configured collector endpoint.

    Pair with `schedule:` for a recurring export, or trigger manually
    when the asset graph changes meaningfully.
    """

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = manual / sensor-triggered).")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Dagster job tags")

    # OpenLineage destination
    endpoint_env_var: str = Field(
        default="OPENLINEAGE_URL",
        description="Env var holding the collector base URL (e.g. http://marquez:5000). The job appends /api/v1/lineage if no path is set.",
    )
    api_key_env_var: Optional[str] = Field(
        default=None,
        description="Env var with a bearer token, if the collector requires auth.",
    )

    # OpenLineage event payload
    namespace: str = Field(
        default="dagster",
        description="OpenLineage namespace. All datasets and jobs from this code location group under this string.",
    )
    producer: str = Field(
        default="https://github.com/eric-thomas-dagster/dagster-component-templates/openlineage_export_job",
        description="OpenLineage 'producer' URI — identifies the emitter implementation.",
    )

    # Behavior
    only_export_on_change: bool = Field(
        default=True,
        description=(
            "If True, hash the structural payload and skip the export when the hash "
            "matches the previous run's. Stored as job-tag metadata on the run."
        ),
    )
    fail_on_collector_error: bool = Field(
        default=False,
        description="If True, an unreachable / errored collector fails the run. Default False (best-effort emit).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op
        def _walk_and_emit(context: dg.OpExecutionContext):
            endpoint_base = os.environ.get(_self.endpoint_env_var)
            if not endpoint_base:
                msg = f"{_self.endpoint_env_var} not set"
                if _self.fail_on_collector_error:
                    raise ValueError(msg)
                context.log.warning(f"{msg} — skipping export (set {_self.endpoint_env_var} to enable)")
                return {"emitted": 0, "skipped": True}

            # Append the standard /api/v1/lineage suffix if the user gave a base URL.
            endpoint = endpoint_base
            if "/api/v1/lineage" not in endpoint:
                endpoint = endpoint.rstrip("/") + "/api/v1/lineage"

            headers: Dict[str, str] = {}
            if _self.api_key_env_var:
                token = os.environ.get(_self.api_key_env_var, "")
                if token:
                    headers["Authorization"] = f"Bearer {token}"

            # Walk the live asset graph from the current code location.
            repo = getattr(context, "repository_def", None)
            if repo is None:
                raise RuntimeError(
                    "context.repository_def is unavailable — this job must run inside a "
                    "loaded code location."
                )
            payload = _build_payload_from_repo(repo)
            payload_hash = _hash_structural(payload)

            # Change-detection: skip when the structural hash matches the
            # previous successful run (stored as a run tag).
            if _self.only_export_on_change:
                last_hash = None
                try:
                    runs = context.instance.get_runs(
                        filters=dg.RunsFilter(job_name=_self.job_name, statuses=[dg.DagsterRunStatus.SUCCESS]),
                        limit=2,  # current + most recent prior
                    )
                    for r in runs:
                        if r.run_id == context.run_id:
                            continue
                        last_hash = (r.tags or {}).get("openlineage_export/payload_hash")
                        if last_hash:
                            break
                except Exception:
                    pass  # best-effort

                if last_hash == payload_hash:
                    context.log.info(
                        f"Asset graph unchanged (hash={payload_hash}); skipping export of "
                        f"{payload['sync_metadata']['total_nodes']} nodes / "
                        f"{payload['sync_metadata']['total_edges']} edges."
                    )
                    # Still tag the run so future change-detection sees this hash.
                    context.instance.add_run_tags(context.run_id, {"openlineage_export/payload_hash": payload_hash})
                    return {"emitted": 0, "skipped_unchanged": True, "payload_hash": payload_hash}

            # Build per-asset events and POST each.
            run_id = str(context.run_id) if context.run_id else str(uuid.uuid4())
            events = _build_per_asset_events(payload, _self.namespace, run_id, _self.producer)

            ok = 0
            failed = 0
            for ev in events:
                if _post(endpoint, headers, ev, context.log):
                    ok += 1
                else:
                    failed += 1
                    if _self.fail_on_collector_error:
                        raise RuntimeError(
                            f"OpenLineage emit failed at event {ok + failed}/{len(events)} — "
                            "fail_on_collector_error=true"
                        )

            context.log.info(
                f"OpenLineage export complete: {ok} emitted, {failed} failed, "
                f"hash={payload_hash}, "
                f"{payload['sync_metadata']['total_nodes']} nodes / "
                f"{payload['sync_metadata']['total_edges']} edges."
            )
            try:
                context.instance.add_run_tags(context.run_id, {"openlineage_export/payload_hash": payload_hash})
            except Exception:
                pass

            return {
                "emitted": ok,
                "failed": failed,
                "endpoint": endpoint,
                "payload_hash": payload_hash,
                "total_nodes": payload["sync_metadata"]["total_nodes"],
                "total_edges": payload["sync_metadata"]["total_edges"],
            }

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _walk_and_emit()

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
