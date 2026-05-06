"""Lineage Graph Extractor.

Build the canonical Dagster asset lineage payload (nodes + edges + source
system identity) and materialize it as an asset. Downstream
`lineage_to_<catalog>` components fan out from this single source — same
graph, multiple catalogs, in lock-step.

Hash of the structural payload is included in metadata for change
detection. Pair with a sensor (or auto-materialize policy) to only
re-push when lineage actually changes.
"""
import json
from typing import Optional

import dagster as dg
from pydantic import Field

from . import lineage_core


class LineageGraphExtractorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Materialize the canonical Dagster asset lineage graph as an asset."""

    asset_name: str = Field(default="lineage_graph", description="Output asset name")
    scope: str = Field(
        default="code_location",
        description="'code_location' (current location) or 'deployment' (full Dagster+ graph via GraphQL)",
    )
    dagster_plus_token_env: str = Field(
        default="DAGSTER_PLUS_TOKEN",
        description="Env var holding a Dagster+ user token (only for scope='deployment')",
    )
    group_name: str = Field(default="lineage")
    description: Optional[str] = Field(default=None)

    # Source system identity — propagated to the payload so downstream
    # transformers can attribute lineage to the right deployment / org / location
    platform_name: str = Field(default="dagster")
    platform_display_name: str = Field(default="Dagster")
    deployment_name: str = Field(default="")
    dagster_ui_url: str = Field(default="")
    organization: str = Field(default="")
    code_location_name: str = Field(default="")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        scope = self.scope
        dagster_plus_token_env = self.dagster_plus_token_env
        source_system = lineage_core.build_source_system(self)

        @dg.asset(
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or "Canonical Dagster asset lineage graph; downstream sinks fan out to data catalogs.",
            kinds={"lineage", "python"},
        )
        def lineage_graph_asset(context: dg.AssetExecutionContext) -> dg.Output:
            payload = None
            if scope == "deployment":
                payload = lineage_core.build_lineage_from_dagster_plus_graphql(context.log, dagster_plus_token_env)
                if payload:
                    n_locs = len(payload.get("sync_metadata", {}).get("code_locations", []))
                    context.log.info(f"Deployment-wide graph via Dagster+ GraphQL ({n_locs} code locations)")
            if payload is None:
                if scope == "deployment":
                    context.log.info("Dagster+ GraphQL unavailable, falling back to current code location")
                # No repository_def in asset context — use the asset graph from the run
                from dagster._core.definitions.repository_definition.repository_definition import RepositoryDefinition
                # Pull the current code location's repository via the Dagster+ instance
                # In a normal asset, we use context.dagster_run + the workspace
                # For simplicity, build from context.assets_def graph + manually walk
                try:
                    from dagster import AssetSelection
                    ag = context.repository_def.asset_graph
                except AttributeError:
                    # newer dagster: use context.instance and the run's graph
                    raise RuntimeError(
                        "lineage_graph_extractor: context.repository_def is unavailable in this Dagster version. "
                        "Please use scope='deployment' with a Dagster+ token, or upgrade Dagster."
                    )
                payload = lineage_core.build_lineage_payload(context.repository_def)

            meta = payload["sync_metadata"]
            payload["sync_metadata"]["scope"] = scope

            _ss = dict(source_system)
            if not _ss["code_location"]:
                _ss["code_location"] = getattr(context, "repository_name", "unknown")
            if _ss["dagster_ui_url"] and _ss["deployment"]:
                _ss["dagster_ui_url"] = f"{_ss['dagster_ui_url']}/{_ss['deployment']}"
            payload["source_system"] = _ss

            payload_hash = lineage_core.hash_payload(payload)
            payload["sync_metadata"]["payload_hash"] = payload_hash

            return dg.Output(
                payload,
                metadata={
                    "payload_hash": dg.MetadataValue.text(payload_hash),
                    "total_nodes": dg.MetadataValue.int(meta["total_nodes"]),
                    "total_edges": dg.MetadataValue.int(meta["total_edges"]),
                    "total_groups": dg.MetadataValue.int(meta["total_groups"]),
                    "scope": dg.MetadataValue.text(scope),
                    "groups": dg.MetadataValue.json(payload["group_summary"]),
                    "edges_preview": dg.MetadataValue.json(payload["edges"][:50]),
                },
            )

        return dg.Definitions(assets=[lineage_graph_asset])
