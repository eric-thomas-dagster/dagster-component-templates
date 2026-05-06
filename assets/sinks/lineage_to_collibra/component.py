"""Lineage → Collibra component.

Sink that pushes the upstream lineage_graph to Collibra Data Intelligence Platform.
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
    org = ss.get("organization", "") or ss.get("platform_display_name", "Dagster")
    deployment = ss.get("deployment", "")
    community_name = f"{org} Data Platform" if org else "Data Platform"

    assets = []
    for node in payload["nodes"]:
        domain_name = node.get("group") or "Default"
        if deployment:
            domain_name = f"{domain_name} ({deployment})"
        assets.append({
            "identifier": {
                "name": node["asset_key_string"],
                "domain": {"name": domain_name, "community": {"name": community_name}},
            },
            "resourceType": "Asset",
            "type": {"name": "Data Asset"},
            "displayName": node["asset_key_string"],
            "attributes": {
                "Description": [{"value": node.get("description", "")}],
                "Source Platform": [{"value": ss.get("platform_display_name", "Dagster")}],
                "Deployment": [{"value": deployment}],
                "Dagster Group": [{"value": node.get("group", "")}],
                "Dagster Kinds": [{"value": ",".join(node.get("kinds", []))}],
            },
        })

    relations = []
    for edge in payload["edges"]:
        relations.append({
            "source": {"name": edge["upstream"], "domain": {"name": "Data Platform"}},
            "target": {"name": edge["downstream"], "domain": {"name": "Data Platform"}},
            "type": {"name": "Data Flow"},
        })
    return {"assets": assets, "relations": relations}


def _push(log, transformed, base_url, token_env):
    import requests
    token = lineage_core.get_token(token_env)
    resp = requests.post(
        f"{base_url}/rest/2.0/import/json-job",
        json=transformed,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    log.info(f"Collibra import: {resp.status_code}")


class LineageToCollibraComponent(dg.Component, dg.Model, dg.Resolvable):
    """Lineage → Collibra — sink asset that depends on lineage_graph and pushes to collibra."""

    asset_name: str = Field(default="lineage_to_collibra", description="Output sink asset name")
    upstream_asset_key: str = Field(
        default="lineage_graph",
        description="Upstream asset emitting the canonical lineage payload (typically from lineage_graph_extractor).",
    )
    catalog_url: str = Field(
        default="https://example.collibra.com",
        description="Catalog endpoint base URL.",
    )
    api_token_env: str = Field(
        default="COLLIBRA_API_TOKEN",
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
        kinds = ["lineage", "collibra"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""
        description = self.description or "Push the upstream lineage_graph to collibra."

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
                        for md_entry in last_mat.asset_materialization.metadata_entries:
                            if md_entry.label in ("pushed_hash", "payload_hash"):
                                last_hash = str(md_entry.value)
                                break
                except Exception:
                    pass  # best-effort

            if only_push_on_change and last_hash and last_hash == current_hash:
                meta = payload.get("sync_metadata", {})
                context.log.info(
                    f"Lineage unchanged (hash={current_hash[:8]}), skipping push to collibra. "
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
                "catalog": dg.MetadataValue.text("collibra"),
                "catalog_url": dg.MetadataValue.text(catalog_url),
            })

        return dg.Definitions(assets=[lineage_sink])
