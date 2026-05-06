"""Lineage → OpenLineage component.

Sink that emits the upstream lineage_graph as OpenLineage RunEvents to any OL-compatible backend.
"""
import json
import os
from pathlib import Path
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field

from . import lineage_core


# ── catalog-specific transform + push ─────────────────────────────────
def _transform(payload):
    import uuid
    ss = payload.get("source_system", {})
    producer = ss.get("dagster_ui_url") or "https://github.com/dagster-io/dagster"
    deployment = ss.get("deployment", "local")
    namespace = f"dagster://{deployment}"
    if ss.get("organization"):
        namespace = f"dagster://{ss['organization']}/{deployment}"

    input_datasets = []
    for node in payload["nodes"]:
        facets = {
            "dagster_metadata": {
                "_producer": producer,
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DagsterMetadataFacet.json",
                "group": node["group"],
                "kinds": node["kinds"],
                "freshness_policy": node["freshness_policy"],
            },
        }
        if node.get("metadata"):
            facets["schema"] = {
                "_producer": producer,
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SchemaDatasetFacet.json",
                "fields": [{"name": k, "type": str(type(v).__name__)} for k, v in node["metadata"].items()],
            }
        input_datasets.append({"namespace": namespace, "name": node["asset_key_string"], "facets": facets, "inputFacets": {}})

    return {
        "_producer": producer,
        "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "eventType": "COMPLETE",
        "eventTime": payload["sync_metadata"]["synced_at"],
        "job": {"namespace": namespace, "name": "dagster_lineage_sync", "facets": {}},
        "inputs": input_datasets,
        "outputs": [],
        "run": {
            "runId": str(uuid.uuid4()),
            "facets": {
                "dagster_lineage": {
                    "_producer": producer,
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DagsterLineageFacet.json",
                    "edges": payload["edges"],
                    "total_nodes": payload["sync_metadata"]["total_nodes"],
                    "total_edges": payload["sync_metadata"]["total_edges"],
                },
            },
        },
    }


def _push(log, transformed, base_url, token_env):
    import requests
    headers = {"Content-Type": "application/json"}
    token = os.environ.get(token_env) if token_env else None
    if token:
        headers["Authorization"] = f"Bearer {token}"
    resp = requests.post(f"{base_url}/api/v1/lineage", json=transformed, headers=headers, timeout=30)
    resp.raise_for_status()
    log.info(f"OpenLineage: {resp.status_code}")


class LineageToOpenLineageComponent(dg.Component, dg.Model, dg.Resolvable):
    """Lineage → OpenLineage — sink asset that depends on lineage_graph and pushes to openlineage."""

    asset_name: str = Field(default="lineage_to_openlineage", description="Output sink asset name")
    upstream_asset_key: str = Field(
        default="lineage_graph",
        description="Upstream asset emitting the canonical lineage payload (typically from lineage_graph_extractor).",
    )
    catalog_url: str = Field(
        default="https://marquez.example.com",
        description="Catalog endpoint base URL.",
    )
    api_token_env: str = Field(
        default="OPENLINEAGE_API_TOKEN",
        description="Env var holding the API token / OAuth bearer.",
    )
    only_push_on_change: bool = Field(
        default=True,
        description=(
            "If true, skip the catalog POST when the upstream payload_hash matches "
            "the last successfully pushed hash. Stored as asset metadata across runs."
        ),
    )
    group_name: str = Field(default="lineage")
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        catalog_url = self.catalog_url
        token_env = self.api_token_env
        only_push_on_change = self.only_push_on_change
        upstream_key = dg.AssetKey.from_user_string(self.upstream_asset_key)
        kinds = ["lineage", "openlineage"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""
        description = self.description or "Push the upstream lineage_graph to openlineage."

        @dg.asset(
            name=self.asset_name,
            ins={"upstream": dg.AssetIn(key=upstream_key)},
            group_name=self.group_name,
            description=description,
            owners=self.owners or [],
            tags=tags,
        )
        def lineage_sink(context: dg.AssetExecutionContext, upstream: dict) -> dg.MaterializeResult:
            payload = upstream
            current_hash = payload.get("sync_metadata", {}).get("payload_hash") or lineage_core.hash_payload(payload)

            # Pull last-pushed hash from this asset's previous materialization metadata
            last_hash = None
            if only_push_on_change:
                try:
                    last_mat = context.instance.get_latest_materialization_event(context.asset_key)
                    if last_mat and last_mat.asset_materialization:
                        md = last_mat.asset_materialization.metadata or {}
                        for label in ("pushed_hash", "payload_hash"):
                            if label in md:
                                v = md[label]
                                last_hash = str(getattr(v, "value", v) or getattr(v, "text", "") or v)
                                break
                except Exception:
                    pass  # best-effort

            if only_push_on_change and last_hash and last_hash == current_hash:
                meta = payload.get("sync_metadata", {})
                context.log.info(
                    f"Lineage unchanged (hash={current_hash[:8]}), skipping push to openlineage. "
                    f"Graph: {meta.get('total_nodes', 0)} nodes, {meta.get('total_edges', 0)} edges."
                )
                return dg.MaterializeResult(metadata={
                    "skipped": dg.MetadataValue.bool(True),
                    "reason": dg.MetadataValue.text("payload unchanged"),
                    "payload_hash": dg.MetadataValue.text(current_hash),
                })

            transformed = _transform(payload)
            _push(context.log, transformed, catalog_url, token_env)
            meta = payload.get("sync_metadata", {})
            return dg.MaterializeResult(metadata={
                "pushed_hash": dg.MetadataValue.text(current_hash),
                "payload_hash": dg.MetadataValue.text(current_hash),
                "total_nodes": dg.MetadataValue.int(meta.get("total_nodes", 0)),
                "total_edges": dg.MetadataValue.int(meta.get("total_edges", 0)),
                "catalog": dg.MetadataValue.text("openlineage"),
                "catalog_url": dg.MetadataValue.text(catalog_url),
            })

        return dg.Definitions(assets=[lineage_sink])
