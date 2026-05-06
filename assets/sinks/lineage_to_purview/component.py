"""Lineage → Microsoft Purview component.

Sink that pushes the upstream lineage_graph to Microsoft Purview Data Map (Apache Atlas v2 entity bulk API).
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
        up = edge["upstream"]
        dn = edge["downstream"]
        up_qn = f"{qn_prefix}/{up}"
        dn_qn = f"{qn_prefix}/{dn}"
        edge_id = f"{up}->{dn}"
        entities.append({
            "typeName": "Process",
            "attributes": {
                "qualifiedName": f"{qn_prefix}/process/{up}__to__{dn}",
                "name": f"dagster_transform_{i}",
                "inputs": [{"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": up_qn}}],
                "outputs": [{"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": dn_qn}}],
            },
            "guid": f"-{abs(hash(edge_id)) % 10**12}",
        })

    return {"entities": entities}


def _push(log, transformed, base_url, token_env):
    import requests
    token = lineage_core.get_token(token_env)
    resp = requests.post(
        f"{base_url}/datamap/api/atlas/v2/entity/bulk",
        json=transformed,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    n = len(transformed.get("entities", []))
    log.info(f"Purview: ingested {n} Atlas entities (DataSets + Process lineage)")


class LineageToPurviewComponent(dg.Component, dg.Model, dg.Resolvable):
    """Lineage → Microsoft Purview — sink asset that depends on lineage_graph and pushes to purview."""

    asset_name: str = Field(default="lineage_to_purview", description="Output sink asset name")
    upstream_asset_key: str = Field(
        default="lineage_graph",
        description="Upstream asset emitting the canonical lineage payload (typically from lineage_graph_extractor).",
    )
    catalog_url: str = Field(
        default="https://my-account.purview.azure.com",
        description="Catalog endpoint base URL.",
    )
    api_token_env: str = Field(
        default="PURVIEW_ACCESS_TOKEN",
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
        kinds = ["lineage", "purview"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""
        description = self.description or "Push the upstream lineage_graph to purview."

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
                    f"Lineage unchanged (hash={current_hash[:8]}), skipping push to purview. "
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
                "catalog": dg.MetadataValue.text("purview"),
                "catalog_url": dg.MetadataValue.text(catalog_url),
            })

        return dg.Definitions(assets=[lineage_sink])
