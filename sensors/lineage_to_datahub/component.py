"""Lineage → DataHub component.

Sensor that exports the Dagster asset lineage graph to DataHub via Rest.li ingestProposal.

Same source-system identity + change-detection cursor as the other
lineage_to_<catalog> components — the only differences are the
catalog-specific transform + push at the bottom of this file.
"""
import json
import os
from pathlib import Path
from typing import Optional

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
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "X-RestLi-Protocol-Version": "2.0.0"}
    for proposal in transformed:
        resp = requests.post(f"{base_url}/aspects?action=ingestProposal", json=proposal, headers=headers, timeout=30)
        resp.raise_for_status()
    log.info(f"DataHub: ingested {len(transformed)} aspect proposals")


class LineageToDataHubComponent(dg.Component, dg.Model, dg.Resolvable):
    """Lineage → DataHub — sensor that pushes the Dagster asset lineage graph to datahub.

    Hashes the graph structure and only pushes when lineage actually changes.
    """

    catalog_url: str = Field(
        default="https://datahub.example.com/api/gms",
        description="Catalog endpoint base URL.",
    )
    api_token_env: str = Field(
        default="DATAHUB_API_TOKEN",
        description="Env var holding the API token / OAuth bearer.",
    )
    scope: str = Field(
        default="code_location",
        description="'code_location' (current location only) or 'deployment' (full Dagster+ graph via GraphQL).",
    )
    dagster_plus_token_env: str = Field(
        default="DAGSTER_PLUS_TOKEN",
        description="Env var for Dagster+ GraphQL (only used when scope='deployment').",
    )
    sensor_interval_seconds: int = Field(default=3600)
    sensor_name: str = Field(default="lineage_to_datahub_sync")
    sensor_default_status: str = Field(
        default="STOPPED",
        description="Set to 'RUNNING' after testing.",
    )
    demo_mode: bool = Field(
        default=False,
        description="If true, log + write JSON locally and skip the catalog POST. Lets you preview the payload.",
    )
    demo_export_path: Optional[str] = Field(
        default="data/exports/lineage_to_datahub.json",
        description="When demo_mode=true, write the transformed payload here.",
    )

    # Source system identity — tells the catalog WHO is sending this lineage
    platform_name: str = Field(default="dagster")
    platform_display_name: str = Field(default="Dagster")
    deployment_name: str = Field(default="", description="auto-filled from DAGSTER_CLOUD_DEPLOYMENT_NAME if empty")
    dagster_ui_url: str = Field(default="", description="e.g. https://myorg.dagster.cloud — for drill-down links")
    organization: str = Field(default="")
    code_location_name: str = Field(default="")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        catalog_url = self.catalog_url
        token_env = self.api_token_env
        sensor_name = self.sensor_name
        export_path = self.demo_export_path
        scope = self.scope
        dagster_plus_token_env = self.dagster_plus_token_env
        demo_mode = self.demo_mode
        source_system = lineage_core.build_source_system(self)

        default_status = (
            dg.DefaultSensorStatus.RUNNING
            if self.sensor_default_status == "RUNNING"
            else dg.DefaultSensorStatus.STOPPED
        )

        @dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=self.sensor_interval_seconds,
            default_status=default_status,
            description=f"Sync the Dagster asset lineage graph to datahub. Only pushes when the graph changes.",
        )
        def lineage_sensor(context: dg.SensorEvaluationContext):
            payload = None
            if scope == "deployment":
                payload = lineage_core.build_lineage_from_dagster_plus_graphql(context.log, dagster_plus_token_env)
                if payload:
                    code_locs = payload.get("sync_metadata", {}).get("code_locations", [])
                    context.log.info(f"Using deployment-wide graph via Dagster+ GraphQL ({len(code_locs)} code locations)")

            if payload is None:
                if scope == "deployment":
                    context.log.info("Dagster+ GraphQL unavailable, falling back to code location scope")
                payload = lineage_core.build_lineage_payload(context.repository_def)

            meta = payload["sync_metadata"]
            payload["sync_metadata"]["scope"] = scope

            _ss = dict(source_system)
            if not _ss["code_location"]:
                _ss["code_location"] = context.repository_name
            if _ss["dagster_ui_url"] and _ss["deployment"]:
                _ss["dagster_ui_url"] = f"{_ss['dagster_ui_url']}/{_ss['deployment']}"
            payload["source_system"] = _ss

            current_hash = lineage_core.hash_payload(payload)
            previous_hash = context.cursor

            if previous_hash == current_hash:
                context.log.info(
                    f"Lineage unchanged (hash={current_hash}), skipping sync. "
                    f"Graph: {meta['total_nodes']} nodes, {meta['total_edges']} edges."
                )
                return

            context.log.info(
                f"Lineage changed ({previous_hash or 'first run'} → {current_hash}): "
                f"{meta['total_nodes']} nodes, {meta['total_edges']} edges"
            )

            transformed = _transform(payload)

            if demo_mode and export_path:
                Path(export_path).parent.mkdir(parents=True, exist_ok=True)
                Path(export_path).write_text(json.dumps({
                    "internal_graph": payload,
                    "transformed_payload": transformed,
                }, indent=2))
                context.log.info(f"[DEMO] Wrote lineage to {export_path}; would have POSTed to {catalog_url}")
            else:
                _push(context.log, transformed, catalog_url, token_env)
                context.log.info(f"Pushed lineage to datahub")

            context.update_cursor(current_hash)

        return dg.Definitions(sensors=[lineage_sensor])
