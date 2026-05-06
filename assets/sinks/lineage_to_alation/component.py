"""Lineage → Alation component.

Sink that pushes the upstream lineage_graph to Alation Data Catalog.
"""
import json
import os
from pathlib import Path
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field



# ── catalog-specific transform + push ─────────────────────────────────
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
                "code_location": node.get("code_location", ss.get("code_location", "")),
                "group": node["group"],
                "kinds": node["kinds"],
                "metadata": node["metadata"],
                "freshness_policy": node["freshness_policy"],
            }),
        })

    paths = []
    for edge in payload["edges"]:
        up = edge["upstream"]
        dn = edge["downstream"]
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


class LineageToAlationComponent(dg.Component, dg.Model, dg.Resolvable):
    """Lineage → Alation — sink asset that depends on lineage_graph and pushes to alation."""

    asset_name: str = Field(default="lineage_to_alation", description="Output sink asset name")
    upstream_asset_key: str = Field(
        default="lineage_graph",
        description="Upstream asset emitting the canonical lineage payload (typically from lineage_graph_extractor).",
    )
    catalog_url: str = Field(
        default="https://alation.example.com",
        description="Catalog endpoint base URL.",
    )
    api_token_env: str = Field(
        default="ALATION_API_TOKEN",
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
        kinds = ["lineage", "alation"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""
        description = self.description or "Push the upstream lineage_graph to alation."

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
            current_hash = payload.get("sync_metadata", {}).get("payload_hash", "")

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
                    f"Lineage unchanged (hash={current_hash[:8]}), skipping push to alation. "
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
                "catalog": dg.MetadataValue.text("alation"),
                "catalog_url": dg.MetadataValue.text(catalog_url),
            })

        return dg.Definitions(assets=[lineage_sink])
