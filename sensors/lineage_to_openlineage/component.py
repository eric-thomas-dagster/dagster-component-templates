"""Lineage → OpenLineage component.

Sensor that emits the Dagster asset lineage graph as OpenLineage RunEvents to any OL-compatible backend (Marquez, Atlan, Astronomer Observe, etc.).

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
    import uuid
    ss = payload.get("source_system", {})
    producer = ss.get("dagster_ui_url") or "https://github.com/dagster-io/dagster"
    namespace = f"dagster://{ss.get('deployment', 'local')}"
    if ss.get("organization"):
        namespace = f"dagster://{ss['organization']}/{ss.get('deployment', 'local')}"

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
    """Lineage → OpenLineage — sensor that pushes the Dagster asset lineage graph to openlineage.

    Hashes the graph structure and only pushes when lineage actually changes.
    """

    catalog_url: str = Field(
        default="https://marquez.example.com",
        description="Catalog endpoint base URL.",
    )
    api_token_env: str = Field(
        default="OPENLINEAGE_API_TOKEN",
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
    sensor_name: str = Field(default="lineage_to_openlineage_sync")
    sensor_default_status: str = Field(
        default="STOPPED",
        description="Set to 'RUNNING' after testing.",
    )
    demo_mode: bool = Field(
        default=False,
        description="If true, log + write JSON locally and skip the catalog POST. Lets you preview the payload.",
    )
    demo_export_path: Optional[str] = Field(
        default="data/exports/lineage_to_openlineage.json",
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
            description=f"Sync the Dagster asset lineage graph to openlineage. Only pushes when the graph changes.",
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
                context.log.info(f"Pushed lineage to openlineage")

            context.update_cursor(current_hash)

        return dg.Definitions(sensors=[lineage_sensor])
