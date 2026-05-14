"""Lineage → Microsoft Purview component.

Sink that pushes the upstream lineage_graph to Microsoft Purview Data Map (Apache Atlas v2 entity bulk API).
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
    token = os.environ.get(token_env)
    if not token:
        raise RuntimeError(f"Missing {token_env} environment variable")
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
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
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
