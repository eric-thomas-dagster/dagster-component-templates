"""Lineage → Castor (Coalesce Catalog) component.

Sink that pushes the upstream lineage_graph to Castor/CastorDoc — the data
catalog product behind Coalesce's "Catalog" offering (a confirmed, separate
product/acquisition; API served from api.castordoc.com or
api.us.castordoc.com, not Coalesce's Scheduler/v1 REST API).

Architecture note -- read before assuming this mirrors lineage_to_alation's
node-creation model:

Alation's lineage API lets you CREATE brand-new first-class "dataflow
objects" (arbitrary external nodes) plus lineage edges to them, so Dagster
shows up as its own node type in Alation's lineage graph. Castor's public
API does NOT support this. Confirmed two ways against Castor's real public
docs (https://docs.coalesce.io/docs/catalog/api/..., 2026-10-08):

  - `upsertLineages(data: [UpsertLineageInput!]!)` only accepts
    `parentTableId` / `childTableId` (or the dashboard equivalents) --
    IDs of TABLE/DASHBOARD records Castor's own warehouse/BI connectors
    already discovered. There is no "external"/custom node type.
  - The UI-based "Manual Lineage Importer" (a CSV upload flow, not a
    GraphQL call) is explicitly documented as working "exclusively with
    existing assets already known to Catalog" (TABLE/COLUMN/DASHBOARD/
    DASHBOARD_FIELD) and explicitly "cannot register brand-new external
    nodes like custom pipeline tools (Dagster, etc.)."

So this sink is honestly architected as ENRICHMENT of existing Castor
table records, not node creation:

  1. Each Dagster asset is resolved to an existing Castor table by an
     exact, case-insensitive name match on the asset key's last path
     segment (same name-resolution approach this repo's
     integrations/coalesce_workspace Catalog-enrichment code already
     uses and tests), via `getTables(scope: {nameContains})`.
  2. Matched tables get their description pushed via
     `updateTableDescriptions` and (optionally) `dagster`/group/kind
     tags attached via `attachTags`.
  3. Optionally, a lightweight best-effort "tracked by Dagster" presence
     record is pushed via `upsertDataQualities` (push_data_quality) --
     this is NOT a real asset-check test result (lineage_graph_extractor's
     payload carries no pass/fail data), just a provenance signal. Wire a
     real dg.asset_check result through if you want genuine pass/fail here.
  4. Optionally (push_lineage_edges, on by default), lineage edges from
     the Dagster asset graph are pushed via `upsertLineages` -- but ONLY
     for edges where BOTH endpoints already resolved to a Castor table.
     An edge with an unmatched endpoint (e.g. a pure Python/ML asset with
     no warehouse-table backing) is skipped and counted, never forced.

Unmatched Dagster assets (no corresponding Castor table at all) are
skipped entirely -- counted and logged, never silently dropped or raised.
"""
import os
import time
from typing import Dict, List, Optional

import dagster as dg
import requests
from pydantic import Field


# ── catalog-specific transform + push ─────────────────────────────────
def _transform(payload):
    """Pure: shape the canonical lineage payload into Castor-matchable candidates.

    No network calls here -- table-id resolution (which requires a
    getTables call per candidate name) happens in `_push`.
    """
    nodes_out = []
    for node in payload["nodes"]:
        asset_key = node.get("asset_key") or []
        match_name = asset_key[-1] if asset_key else node["asset_key_string"]
        nodes_out.append({
            "asset_key_string": node["asset_key_string"],
            "match_name": match_name,
            "description": node.get("description") or "",
            "group": node.get("group") or "",
            "kinds": node.get("kinds") or [],
        })
    edges_out = [
        {"upstream": e["upstream"], "downstream": e["downstream"]}
        for e in payload.get("edges", [])
    ]
    return {"nodes": nodes_out, "edges": edges_out}


def _castor_call(base_url, headers, op_name, query, variables):
    resp = requests.post(
        f"{base_url}?op={op_name}",
        headers=headers,
        json={"query": query, "variables": variables},
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("errors"):
        raise RuntimeError(f"Castor {op_name!r} returned errors: {body['errors']}")
    return (body.get("data") or {}).get(op_name)


def _resolve_table_id(base_url, headers, name):
    """getTables(scope: {nameContains}) is a server-side substring match, so
    results are filtered here to an exact, case-insensitive name match (a
    search for "orders" would also return "stg_orders"). Returns None if no
    exact match is found -- callers treat that as "skip this asset"."""
    query = """
    query GetTables($scope: GetTablesScope) {
      getTables(scope: $scope) { data { id name } }
    }
    """
    result = _castor_call(base_url, headers, "getTables", query, {"scope": {"nameContains": name}})
    for row in (result or {}).get("data") or []:
        if (row.get("name") or "").lower() == name.lower():
            return row.get("id")
    return None


def _push(log, transformed, base_url, token_env, push_tags=True, push_data_quality=False, push_lineage_edges=True):
    token = os.environ.get(token_env)
    if not token:
        raise RuntimeError(f"Missing {token_env} environment variable")
    headers = {"Authorization": f"Token {token}", "Content-Type": "application/json"}

    # Resolve match_name -> table_id once per distinct name (not once per node).
    match_name_to_id: Dict[str, Optional[str]] = {}
    for n in transformed["nodes"]:
        if n["match_name"] not in match_name_to_id:
            match_name_to_id[n["match_name"]] = _resolve_table_id(base_url, headers, n["match_name"])

    matched: Dict[str, Optional[str]] = {
        n["asset_key_string"]: match_name_to_id[n["match_name"]] for n in transformed["nodes"]
    }
    matched_count = sum(1 for v in matched.values() if v)
    unmatched_count = sum(1 for v in matched.values() if not v)

    description_inputs = []
    tag_inputs = []
    for n in transformed["nodes"]:
        table_id = matched.get(n["asset_key_string"])
        if not table_id:
            continue
        if n["description"]:
            description_inputs.append({"id": table_id, "externalDescription": n["description"]})
        if push_tags:
            tag_inputs.append({"entityType": "TABLE", "entityId": table_id, "label": "dagster"})
            if n["group"]:
                tag_inputs.append({"entityType": "TABLE", "entityId": table_id, "label": f"dagster:group:{n['group']}"})
            for kind in n["kinds"]:
                tag_inputs.append({"entityType": "TABLE", "entityId": table_id, "label": f"dagster:kind:{kind}"})

    if description_inputs:
        _castor_call(
            base_url, headers, "updateTableDescriptions",
            "mutation UpdateTableDescriptions($data: [UpdateTableDescriptionInput!]!) "
            "{ updateTableDescriptions(data: $data) { id } }",
            {"data": description_inputs},
        )

    if push_tags and tag_inputs:
        _castor_call(
            base_url, headers, "attachTags",
            "mutation AttachTags($data: [BaseTagEntityInput!]!) { attachTags(data: $data) }",
            {"data": tag_inputs},
        )

    quality_pushed = 0
    if push_data_quality:
        run_at = time.strftime("%Y-%m-%dT%H:%M:%SZ")
        for n in transformed["nodes"]:
            table_id = matched.get(n["asset_key_string"])
            if not table_id:
                continue
            _castor_call(
                base_url, headers, "upsertDataQualities",
                "mutation UpsertDataQualities($data: UpsertQualityChecksInput!) "
                "{ upsertDataQualities(data: $data) { id } }",
                {"data": {
                    "tableId": table_id,
                    "qualityChecks": [{
                        "externalId": f"dagster:{n['asset_key_string']}:tracked",
                        "name": "Tracked by Dagster",
                        "status": "SUCCESS",
                        "runAt": run_at,
                        "description": (
                            "This table is orchestrated/tracked by a Dagster asset. "
                            "Not a real test outcome -- see README Architecture notes."
                        ),
                    }],
                }},
            )
            quality_pushed += 1

    lineage_pushed = 0
    lineage_skipped = 0
    if push_lineage_edges:
        lineage_inputs = []
        for e in transformed["edges"]:
            parent_id = matched.get(e["upstream"])
            child_id = matched.get(e["downstream"])
            if parent_id and child_id:
                lineage_inputs.append({"parentTableId": parent_id, "childTableId": child_id})
            else:
                lineage_skipped += 1
        if lineage_inputs:
            _castor_call(
                base_url, headers, "upsertLineages",
                "mutation UpsertLineages($data: [UpsertLineageInput!]!) "
                "{ upsertLineages(data: $data) { id } }",
                {"data": lineage_inputs},
            )
        lineage_pushed = len(lineage_inputs)

    log.info(
        f"Castor sync: {matched_count} assets matched to existing tables "
        f"({unmatched_count} unmatched, skipped); {len(description_inputs)} descriptions, "
        f"{len(tag_inputs)} tags, {quality_pushed} quality records, "
        f"{lineage_pushed} lineage edges pushed ({lineage_skipped} edges skipped, endpoint(s) unmatched)."
    )
    return {
        "matched": matched_count,
        "unmatched": unmatched_count,
        "descriptions_pushed": len(description_inputs),
        "tags_attached": len(tag_inputs),
        "quality_records_pushed": quality_pushed,
        "lineage_edges_pushed": lineage_pushed,
        "lineage_edges_skipped": lineage_skipped,
    }


class LineageToCastordocComponent(dg.Component, dg.Model, dg.Resolvable):
    """Lineage → Castor — sink asset that depends on lineage_graph and enriches matching Castor tables."""

    asset_name: str = Field(default="lineage_to_castordoc", description="Output sink asset name")
    upstream_asset_key: str = Field(
        default="lineage_graph",
        description="Upstream asset emitting the canonical lineage payload (typically from lineage_graph_extractor).",
    )
    catalog_url: str = Field(
        default="https://api.castordoc.com/public/graphql",
        description=(
            "Castor/Coalesce Catalog GraphQL endpoint. EU tenants use api.castordoc.com "
            "(default); US tenants use https://api.us.castordoc.com/public/graphql."
        ),
    )
    api_token_env: str = Field(
        default="CASTORDOC_API_TOKEN",
        description="Env var holding the Castor API token (sent as `Token` header, not Bearer).",
    )
    only_push_on_change: bool = Field(
        default=True,
        description=(
            "If true, skip the Castor calls when the upstream payload_hash matches "
            "the last successfully pushed hash. Stored as asset metadata across runs."
        ),
    )
    push_tags: bool = Field(
        default=True,
        description="Attach `dagster` / `dagster:group:<group>` / `dagster:kind:<kind>` tags onto matched Castor tables via attachTags.",
    )
    push_data_quality: bool = Field(
        default=False,
        description=(
            "Push a best-effort 'tracked by Dagster' presence record via upsertDataQualities on each "
            "matched table. NOT a real test result -- lineage_graph_extractor carries no pass/fail data."
        ),
    )
    push_lineage_edges: bool = Field(
        default=True,
        description=(
            "Push lineage edges via upsertLineages for asset-graph edges where BOTH endpoints already "
            "resolve to an existing Castor table. Edges with an unmatched endpoint are skipped (Castor "
            "has no concept of an external/custom node)."
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
        push_tags = self.push_tags
        push_data_quality = self.push_data_quality
        push_lineage_edges = self.push_lineage_edges
        upstream_key = dg.AssetKey.from_user_string(self.upstream_asset_key)
        kinds = ["lineage", "castordoc"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""
        description = self.description or "Push the upstream lineage_graph to Castor (CastorDoc / Coalesce Catalog)."

        @dg.asset(
            key=dg.AssetKey.from_user_string(self.asset_name),
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
                    f"Lineage unchanged (hash={current_hash[:8]}), skipping push to Castor. "
                    f"Graph: {meta.get('total_nodes', 0)} nodes, {meta.get('total_edges', 0)} edges."
                )
                return dg.MaterializeResult(metadata={
                    "skipped": dg.MetadataValue.bool(True),
                    "reason": dg.MetadataValue.text("payload unchanged"),
                    "payload_hash": dg.MetadataValue.text(current_hash),
                })

            transformed = _transform(payload)
            result = _push(
                context.log, transformed, catalog_url, token_env,
                push_tags=push_tags, push_data_quality=push_data_quality,
                push_lineage_edges=push_lineage_edges,
            )
            meta = payload.get("sync_metadata", {})
            return dg.MaterializeResult(metadata={
                "pushed_hash": dg.MetadataValue.text(current_hash),
                "payload_hash": dg.MetadataValue.text(current_hash),
                "total_nodes": dg.MetadataValue.int(meta.get("total_nodes", 0)),
                "total_edges": dg.MetadataValue.int(meta.get("total_edges", 0)),
                "assets_matched": dg.MetadataValue.int(result["matched"]),
                "assets_unmatched": dg.MetadataValue.int(result["unmatched"]),
                "descriptions_pushed": dg.MetadataValue.int(result["descriptions_pushed"]),
                "tags_attached": dg.MetadataValue.int(result["tags_attached"]),
                "quality_records_pushed": dg.MetadataValue.int(result["quality_records_pushed"]),
                "lineage_edges_pushed": dg.MetadataValue.int(result["lineage_edges_pushed"]),
                "lineage_edges_skipped": dg.MetadataValue.int(result["lineage_edges_skipped"]),
                "catalog": dg.MetadataValue.text("castordoc"),
                "catalog_url": dg.MetadataValue.text(catalog_url),
            })

        return dg.Definitions(assets=[lineage_sink])
