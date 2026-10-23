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
from concurrent.futures import ThreadPoolExecutor
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


def _build_payload_from_dagster_plus(log, dagster_plus_token_env: str) -> Optional[Dict[str, Any]]:
    """Query Dagster+ GraphQL for the deployment-wide asset graph.

    Mirrors lineage_graph_extractor's identically-named helper so the two
    components' scope='deployment' behavior stays consistent. Returns None
    (never raises) when Dagster+ env vars aren't set or the query fails --
    callers fall back to the local code-location graph.
    """
    cloud_url = os.environ.get("DAGSTER_CLOUD_URL")
    deployment = os.environ.get("DAGSTER_CLOUD_DEPLOYMENT_NAME", "prod")
    token = os.environ.get(dagster_plus_token_env)
    if not (cloud_url and token):
        return None

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
                repository { name location { name } }
                freshnessPolicy { cronSchedule maximumLagMinutes }
            }
        }
        """
        resp = requests.post(
            f"{cloud_url}/{deployment}/graphql",
            json={"query": query},
            headers={"Dagster-Cloud-Api-Token": token, "Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        asset_nodes = resp.json().get("data", {}).get("assetNodes", [])
        if not asset_nodes:
            return None

        nodes: List[Dict[str, Any]] = []
        edges: List[Dict[str, str]] = []
        for an in asset_nodes:
            key_str = "/".join(an["assetKey"]["path"])
            code_location = an.get("repository", {}).get("location", {}).get("name", "unknown")
            nodes.append({
                "asset_key": an["assetKey"]["path"],
                "asset_key_string": key_str,
                "group": an.get("groupName") or "ungrouped",
                "kinds": [an["computeKind"]] if an.get("computeKind") else [],
                "description": (an.get("description") or "")[:500],
                "metadata": {"code_location": code_location},
                "freshness_policy": str(an["freshnessPolicy"]) if an.get("freshnessPolicy") else None,
            })
            for dep in an.get("dependencyKeys", []):
                edges.append({"upstream": "/".join(dep["path"]), "downstream": key_str})

        log.info(f"GraphQL: fetched {len(nodes)} assets across deployment")
        return {
            "sync_metadata": {
                "synced_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source": "dagster_plus_graphql",
                "total_nodes": len(nodes),
                "total_edges": len(edges),
            },
            "nodes": nodes,
            "edges": edges,
        }
    except Exception as e:
        log.warning(f"GraphQL query failed, falling back to local graph: {e}")
        return None


# ── column lineage ───────────────────────────────────────────────────────
def _column_lineage_facets(instance, asset_key_strings: List[str], namespace: str) -> Dict[str, Dict[str, Any]]:
    """Batched lookup of each asset's REAL `dagster/column_lineage` metadata (set via
    `context.add_output_metadata` on its most recent materialization -- this is NOT
    visible on the static asset-graph definition `_build_payload_from_repo` walks,
    since `TableColumnLineage` is runtime, per-materialization metadata, not a static
    asset property), converted into the OpenLineage `columnLineage` dataset facet
    (schema: https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json).

    Returns {asset_key_string: facet_dict}, omitting assets with no column lineage
    recorded, or whose `dagster/column_lineage` isn't a real TableColumnLineage value
    (e.g. a component that sets it to a plain dict directly).
    """
    from dagster import AssetKey, TableColumnLineageMetadataValue

    keys = [AssetKey.from_user_string(s) for s in asset_key_strings]
    try:
        records = instance.get_asset_records(keys)
    except Exception:
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for record in records:
        entry = record.asset_entry
        last_mat = entry.last_materialization_record
        if last_mat is None:
            continue
        try:
            metadata = last_mat.event_log_entry.dagster_event.event_specific_data.materialization.metadata
        except Exception:
            continue
        lineage_md = metadata.get("dagster/column_lineage") if metadata else None
        if not isinstance(lineage_md, TableColumnLineageMetadataValue):
            continue
        fields: Dict[str, Any] = {}
        for out_col, deps in lineage_md.value.deps_by_column.items():
            fields[out_col] = {
                "inputFields": [
                    {
                        "namespace": namespace,
                        "name": dep.asset_key.to_user_string(),
                        "field": dep.column_name,
                        "transformations": [{"type": "DIRECT"}],
                    }
                    for dep in deps
                ]
            }
        if fields:
            out[entry.asset_key.to_user_string()] = {
                "_producer": namespace,
                "_schemaURL": "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json",
                "fields": fields,
            }
    return out


# ── OpenLineage transform ────────────────────────────────────────────────
def _hash_structural(payload: Dict[str, Any], column_lineage: Optional[Dict[str, Dict[str, Any]]] = None) -> str:
    structural = json.dumps(
        {"nodes": payload["nodes"], "edges": payload["edges"], "column_lineage": column_lineage or {}},
        sort_keys=True,
    )
    return hashlib.sha256(structural.encode()).hexdigest()[:16]


def _datasets_for_asset(
    asset_key_str: str, namespace: str, node: Dict[str, Any],
    column_lineage_facet: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
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
    if column_lineage_facet:
        facets["columnLineage"] = column_lineage_facet
    return {"namespace": namespace, "name": asset_key_str, "facets": facets, "inputFacets": {}}


def _build_per_asset_events(
    payload: Dict[str, Any], namespace: str, run_id: str, producer: str,
    column_lineage: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Emit one OpenLineage RunEvent per asset, with this-asset as the output and parents as inputs.

    Each event is a self-contained COMPLETE for the asset's place in the
    graph. Collectors that index by job + run id will see the full
    deployment lineage in one batch. `column_lineage` (output asset key
    string -> columnLineage facet dict) attaches to the OUTPUT dataset only
    -- the facet's own `inputFields` already name the upstream asset+column,
    so it does not need to be duplicated onto input datasets.
    """
    nodes_by_key = {n["asset_key_string"]: n for n in payload["nodes"]}
    parents_by_key: Dict[str, List[str]] = {}
    for edge in payload["edges"]:
        parents_by_key.setdefault(edge["downstream"], []).append(edge["upstream"])
    column_lineage = column_lineage or {}

    events: List[Dict[str, Any]] = []
    event_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    for node in payload["nodes"]:
        key_str = node["asset_key_string"]
        outputs = [_datasets_for_asset(key_str, namespace, node, column_lineage.get(key_str))]
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


def _post_many(endpoint: str, headers: Dict[str, str], events: List[Dict[str, Any]], log, max_workers: int) -> int:
    """Dispatch every event's POST, up to `max_workers` concurrently, and return the
    failure count. The standard OpenLineage HTTP transport (Marquez's /api/v1/lineage,
    and the v2 spec generally) accepts exactly one RunEvent per request -- there is no
    standard batch/array endpoint to send many events in one call (confirmed against
    Marquez's own docs: "record a single lineage event" is the operation this path
    exposes). Concurrency is the real lever for export throughput here, not batching.
    max_workers=1 recovers the original strictly-sequential behavior.
    """
    if max_workers <= 1 or len(events) <= 1:
        return sum(0 if _post(endpoint, headers, ev, log) else 1 for ev in events)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        results = list(pool.map(lambda ev: _post(endpoint, headers, ev, log), events))
    return sum(0 if ok else 1 for ok in results)


# ── component ────────────────────────────────────────────────────────────

# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


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
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Dagster job tags")

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

    # Scope
    scope: str = Field(
        default="code_location",
        description=(
            "'code_location' (current location's asset graph) or 'deployment' (full "
            "Dagster+ graph via GraphQL, same mechanism as lineage_graph_extractor). "
            "Falls back to 'code_location' automatically if Dagster+ env vars aren't "
            "set or the GraphQL query fails."
        ),
    )
    dagster_plus_token_env: str = Field(
        default="DAGSTER_PLUS_TOKEN",
        description="Env var holding a Dagster+ user token (only used for scope='deployment').",
    )

    # Behavior
    only_export_on_change: bool = Field(
        default=True,
        description=(
            "If True, hash the structural payload (plus column lineage) and skip the "
            "export when the hash matches the previous run's. Stored as job-tag "
            "metadata on the run."
        ),
    )
    fail_on_collector_error: bool = Field(
        default=False,
        description=(
            "If True, any collector POST failure fails the run (checked after every "
            "event has been attempted, not at the first failure -- with "
            "max_concurrent_requests>1 there is no single 'first' failure to stop at). "
            "Default False (best-effort emit)."
        ),
    )
    max_concurrent_requests: int = Field(
        default=8,
        description=(
            "Max concurrent POSTs to the collector. The OpenLineage HTTP transport "
            "(Marquez's /api/v1/lineage) accepts one event per request -- there is no "
            "standard batch endpoint to send many events in one call -- so concurrency, "
            "not batching, is the real export-throughput lever for a large asset graph. "
            "Set to 1 for strictly sequential POSTs."
        ),
    )
    emit_column_lineage: bool = Field(
        default=True,
        description=(
            "Emit a real OpenLineage columnLineage facet on each asset's output "
            "dataset, built from its most recent materialization's "
            "`dagster/column_lineage` metadata (e.g. set by sql_transform's own "
            "column_lineage: field), when present. Requires one extra "
            "instance.get_asset_records(...) call per export; set False to skip it."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=(), csv=(), tags=('tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
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

            # Walk the asset graph: deployment-wide via Dagster+ GraphQL if scope
            # requests it (and env vars are set + the query succeeds), else fall
            # back to the current code location's live graph.
            payload = None
            if _self.scope == "deployment":
                payload = _build_payload_from_dagster_plus(context.log, _self.dagster_plus_token_env)
                if payload is not None:
                    context.log.info(
                        f"Deployment-wide graph via Dagster+ GraphQL "
                        f"({payload['sync_metadata']['total_nodes']} assets)"
                    )
                else:
                    context.log.info(
                        "Dagster+ GraphQL unavailable (scope='deployment' requires "
                        f"DAGSTER_CLOUD_URL + {_self.dagster_plus_token_env} set), "
                        "falling back to current code location"
                    )
            if payload is None:
                repo = getattr(context, "repository_def", None)
                if repo is None:
                    raise RuntimeError(
                        "context.repository_def is unavailable — this job must run inside a "
                        "loaded code location."
                    )
                payload = _build_payload_from_repo(repo)

            # Real OpenLineage columnLineage facet, derived from each asset's most
            # recent materialization's `dagster/column_lineage` metadata (NOT visible
            # on the static definition payload._build_payload_from_repo walked above).
            column_lineage: Dict[str, Dict[str, Any]] = {}
            if _self.emit_column_lineage:
                try:
                    column_lineage = _column_lineage_facets(
                        context.instance,
                        [n["asset_key_string"] for n in payload["nodes"]],
                        _self.namespace,
                    )
                except Exception as _exc:
                    context.log.warning(f"Column lineage lookup failed, continuing without it ({type(_exc).__name__}: {_exc})")

            payload_hash = _hash_structural(payload, column_lineage)

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
                except Exception as _exc:
                    context.log.warning(f"Could not read the previous payload hash; pushing anyway ({type(_exc).__name__}: {_exc})")

                if last_hash == payload_hash:
                    context.log.info(
                        f"Asset graph unchanged (hash={payload_hash}); skipping export of "
                        f"{payload['sync_metadata']['total_nodes']} nodes / "
                        f"{payload['sync_metadata']['total_edges']} edges."
                    )
                    # Still tag the run so future change-detection sees this hash.
                    context.instance.add_run_tags(context.run_id, {"openlineage_export/payload_hash": payload_hash})
                    return {"emitted": 0, "skipped_unchanged": True, "payload_hash": payload_hash}

            # Build per-asset events and POST them (up to max_concurrent_requests at
            # once -- there's no standard OpenLineage batch endpoint to send many
            # events in one request, so concurrency is the real throughput lever).
            run_id = str(context.run_id) if context.run_id else str(uuid.uuid4())
            events = _build_per_asset_events(payload, _self.namespace, run_id, _self.producer, column_lineage)

            failed = _post_many(endpoint, headers, events, context.log, _self.max_concurrent_requests)
            ok = len(events) - failed
            if failed and _self.fail_on_collector_error:
                raise RuntimeError(
                    f"OpenLineage emit failed for {failed}/{len(events)} events — "
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
            except Exception as _exc:
                context.log.warning(f"Could not persist run tag; the next run cannot dedupe against this one ({type(_exc).__name__}: {_exc})")

            return {
                "emitted": ok,
                "failed": failed,
                "endpoint": endpoint,
                "payload_hash": payload_hash,
                "total_nodes": payload["sync_metadata"]["total_nodes"],
                "total_edges": payload["sync_metadata"]["total_edges"],
                "assets_with_column_lineage": len(column_lineage),
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
