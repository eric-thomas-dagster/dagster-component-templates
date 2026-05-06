"""Lineage → DataHub component.

Sink that pushes the upstream lineage_graph to DataHub via Rest.li ingestProposal.
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
    deployment = ss.get("deployment", "")
    env = "PROD" if deployment in ("prod", "production", "") else "DEV"
    ui_url = ss.get("dagster_ui_url", "")

    parent_map = {}
    for edge in payload["edges"]:
        parent_map.setdefault(edge["downstream"], []).append(edge["upstream"])

    proposals = []
    for node in payload["nodes"]:
        key_str = node["asset_key_string"]
        urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{key_str},{env})"
        custom_props = {
            "dagster_group": node["group"] or "",
            "dagster_kinds": ",".join(node["kinds"]),
            "dagster_deployment": deployment,
            **{k: str(v) for k, v in node.get("metadata", {}).items()},
        }
        properties_aspect = {
            "name": key_str,
            "description": node.get("description", ""),
            "externalUrl": f"{ui_url}/assets/{'/'.join(node['asset_key'])}" if ui_url else "",
            "customProperties": custom_props,
        }
        proposals.append({
            "proposal": {
                "entityUrn": urn,
                "entityType": "dataset",
                "aspectName": "datasetProperties",
                "changeType": "UPSERT",
                "aspect": {"value": json.dumps(properties_aspect), "contentType": "application/json"},
            }
        })

        parents = parent_map.get(key_str, [])
        if parents:
            lineage_aspect = {
                "upstreams": [
                    {"dataset": f"urn:li:dataset:(urn:li:dataPlatform:{platform},{p},{env})", "type": "TRANSFORMED"}
                    for p in parents
                ],
            }
            proposals.append({
                "proposal": {
                    "entityUrn": urn,
                    "entityType": "dataset",
                    "aspectName": "upstreamLineage",
                    "changeType": "UPSERT",
                    "aspect": {"value": json.dumps(lineage_aspect), "contentType": "application/json"},
                }
            })
    return proposals


def _push(log, transformed, base_url, token_env):
    import requests
    token = lineage_core.get_token(token_env)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-RestLi-Protocol-Version": "2.0.0",
    }
    for proposal in transformed:
        resp = requests.post(f"{base_url}/aspects?action=ingestProposal", json=proposal, headers=headers, timeout=30)
        resp.raise_for_status()
    log.info(f"DataHub: ingested {len(transformed)} aspect proposals")


class LineageToDataHubComponent(dg.Component, dg.Model, dg.Resolvable):
    """Lineage → DataHub — sink asset that depends on lineage_graph and pushes to datahub."""

    asset_name: str = Field(default="lineage_to_datahub", description="Output sink asset name")
    upstream_asset_key: str = Field(
        default="lineage_graph",
        description="Upstream asset emitting the canonical lineage payload (typically from lineage_graph_extractor).",
    )
    catalog_url: str = Field(
        default="https://datahub.example.com/api/gms",
        description="Catalog endpoint base URL.",
    )
    api_token_env: str = Field(
        default="DATAHUB_API_TOKEN",
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
        kinds = ["lineage", "datahub"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""
        description = self.description or "Push the upstream lineage_graph to datahub."

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
                    f"Lineage unchanged (hash={current_hash[:8]}), skipping push to datahub. "
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
                "catalog": dg.MetadataValue.text("datahub"),
                "catalog_url": dg.MetadataValue.text(catalog_url),
            })

        return dg.Definitions(assets=[lineage_sink])
