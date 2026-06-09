"""Lineage Graph Extractor.

Build the canonical Dagster asset lineage payload (nodes + edges + source
system identity) and materialize it as an asset. Downstream
`lineage_to_<catalog>` components fan out from this single source — same
graph, multiple catalogs, in lock-step.

Hash of the structural payload is included in metadata for change
detection. Pair with a sensor (or auto-materialize policy) to only
re-push when lineage actually changes.

Self-contained: no shared helper modules. Sinks read `payload_hash`
directly from the upstream payload's sync_metadata.
"""

import hashlib
import json
import os
import time
from typing import Optional

import dagster as dg
from pydantic import Field


def _build_payload_from_repo(repo_def) -> dict:
    """Walk the live asset graph and build a catalog-ready payload."""
    asset_graph = repo_def.asset_graph
    all_keys = list(asset_graph.toposorted_asset_keys)

    nodes = []
    edges = []
    group_summary: dict = {}

    for key in all_keys:
        node = asset_graph.get(key)
        key_str = key.to_user_string()

        raw_metadata = node.metadata or {}
        safe_metadata = {}
        for k, v in raw_metadata.items():
            if k.startswith(("dagster_dbt/", "dagster/")):
                continue
            try:
                json.dumps(v)
                safe_metadata[k] = v
            except Exception:
                safe_metadata[k] = str(v)

        fp = node.freshness_policy_or_from_metadata
        nodes.append({
            "asset_key": key.path,
            "asset_key_string": key_str,
            "group": node.group_name,
            "kinds": sorted(node.kinds) if node.kinds else [],
            "description": (node.description or "")[:500],
            "metadata": safe_metadata,
            "freshness_policy": str(fp) if fp else None,
            "parent_count": len(node.parent_keys),
            "child_count": len(node.child_keys),
        })

        grp = node.group_name or "ungrouped"
        group_summary.setdefault(grp, []).append(key_str)

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
            "total_groups": len(group_summary),
            "assets_with_freshness_policy": sum(1 for n in nodes if n["freshness_policy"]),
        },
        "nodes": nodes,
        "edges": edges,
        "group_summary": {
            grp: {"count": len(assets), "assets": assets}
            for grp, assets in sorted(group_summary.items())
        },
    }


def _build_payload_from_dagster_plus(log, dagster_plus_token_env: str) -> Optional[dict]:
    """Query Dagster+ GraphQL for the deployment-wide asset graph."""
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
                dependedByKeys { path }
                opNames
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

        nodes, edges, group_summary = [], [], {}
        for an in asset_nodes:
            key_str = "/".join(an["assetKey"]["path"])
            code_location = an.get("repository", {}).get("location", {}).get("name", "unknown")
            grp = an.get("groupName") or "ungrouped"
            nodes.append({
                "asset_key": an["assetKey"]["path"],
                "asset_key_string": key_str,
                "group": grp,
                "kinds": [an["computeKind"]] if an.get("computeKind") else [],
                "description": (an.get("description") or "")[:500],
                "metadata": {"code_location": code_location},
                "freshness_policy": str(an["freshnessPolicy"]) if an.get("freshnessPolicy") else None,
                "code_location": code_location,
                "parent_count": len(an.get("dependencyKeys", [])),
                "child_count": len(an.get("dependedByKeys", [])),
            })
            group_summary.setdefault(grp, []).append(key_str)
            for dep in an.get("dependencyKeys", []):
                edges.append({"upstream": "/".join(dep["path"]), "downstream": key_str})

        log.info(f"GraphQL: fetched {len(nodes)} assets across deployment")
        return {
            "sync_metadata": {
                "synced_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source": "dagster_plus_graphql",
                "total_nodes": len(nodes),
                "total_edges": len(edges),
                "total_groups": len(group_summary),
                "assets_with_freshness_policy": sum(1 for n in nodes if n["freshness_policy"]),
                "code_locations": list({n["code_location"] for n in nodes}),
            },
            "nodes": nodes,
            "edges": edges,
            "group_summary": {
                grp: {"count": len(assets), "assets": assets}
                for grp, assets in sorted(group_summary.items())
            },
        }
    except Exception as e:
        log.warning(f"GraphQL query failed, falling back to local graph: {e}")
        return None


def _hash_structural(payload: dict) -> str:
    """Hash nodes + edges (excluding timestamps) for change detection."""
    structural = json.dumps(
        {"nodes": payload["nodes"], "edges": payload["edges"]},
        sort_keys=True,
    )
    return hashlib.sha256(structural.encode()).hexdigest()[:16]


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

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _values:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_values)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        scope = self.scope
        dagster_plus_token_env = self.dagster_plus_token_env
        source_system = {
            "platform": self.platform_name,
            "platform_display_name": self.platform_display_name,
            "deployment": self.deployment_name or os.environ.get("DAGSTER_CLOUD_DEPLOYMENT_NAME", "local"),
            "dagster_ui_url": self.dagster_ui_url or os.environ.get("DAGSTER_CLOUD_URL", ""),
            "organization": self.organization,
            "code_location": self.code_location_name,
        }

        @dg.asset(
            key=AssetKey.from_user_string(self.asset_name),
            group_name=self.group_name,
            description=self.description or "Canonical Dagster asset lineage graph; downstream sinks fan out to data catalogs.",
            kinds={"lineage", "python"},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def lineage_graph_asset(context: dg.AssetExecutionContext) -> dg.Output:
            payload = None
            if scope == "deployment":
                payload = _build_payload_from_dagster_plus(context.log, dagster_plus_token_env)
                if payload:
                    n_locs = len(payload.get("sync_metadata", {}).get("code_locations", []))
                    context.log.info(f"Deployment-wide graph via Dagster+ GraphQL ({n_locs} code locations)")
            if payload is None:
                if scope == "deployment":
                    context.log.info("Dagster+ GraphQL unavailable, falling back to current code location")
                payload = _build_payload_from_repo(context.repository_def)

            payload["sync_metadata"]["scope"] = scope
            _ss = dict(source_system)
            if not _ss["code_location"]:
                _ss["code_location"] = getattr(context, "repository_name", "unknown")
            if _ss["dagster_ui_url"] and _ss["deployment"]:
                _ss["dagster_ui_url"] = f"{_ss['dagster_ui_url']}/{_ss['deployment']}"
            payload["source_system"] = _ss

            payload_hash = _hash_structural(payload)
            payload["sync_metadata"]["payload_hash"] = payload_hash
            meta = payload["sync_metadata"]

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
