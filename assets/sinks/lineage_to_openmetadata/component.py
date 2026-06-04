"""Lineage → OpenMetadata component.

Sink that pushes the upstream lineage_graph payload to OpenMetadata via the
REST API.

OpenMetadata's entity hierarchy is service → database → schema → table, so we
map Dagster's asset graph onto it:

  - One DatabaseService (configurable, default "dagster")
  - One Database (configurable, default "default")
  - One DatabaseSchema per Dagster `group` (collapses ungrouped assets into a
    "default" schema)
  - One Table per Dagster asset
  - One lineage edge per Dagster asset graph edge

All PUTs against OpenMetadata's API are idempotent — re-running with the same
payload upserts cleanly.

Distinct from `lineage_to_datahub` etc. — same `lineage_graph_extractor`
upstream, different downstream catalog.
"""
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import dagster as dg
from pydantic import Field


# ── catalog-specific transform + push ─────────────────────────────────


def _safe_part(name: str) -> str:
    """OpenMetadata FQN parts must be ASCII-safe. Slash-separated asset
    keys become dot-FQN-safe via underscores; non-alnum collapsed."""
    if not name:
        return "default"
    out = []
    for ch in name:
        if ch.isalnum() or ch in "_-":
            out.append(ch)
        else:
            out.append("_")
    return "".join(out) or "default"


def _build_plan(payload: dict, service_name: str, database_name: str) -> dict:
    """Walk the payload and emit a deterministic plan of OpenMetadata
    entities to ensure-exist + lineage edges to push.

    Returned shape:
        {
          "service":  {"name": str},
          "database": {"name": str, "service_fqn": str},
          "schemas":  [{"name": str, "database_fqn": str}, ...],
          "tables":   [{"name": str, "schema_fqn": str, "description": str, ...}, ...],
          "edges":    [{"from_fqn": str, "to_fqn": str}, ...],
        }
    """
    ss = payload.get("source_system", {})
    ui_url = ss.get("dagster_ui_url", "")
    deployment = ss.get("deployment", "")

    svc_part = _safe_part(service_name)
    db_part = _safe_part(database_name)
    service_fqn = svc_part
    database_fqn = f"{svc_part}.{db_part}"

    schemas_seen: Set[str] = set()
    schemas: List[dict] = []
    tables: List[dict] = []
    table_fqn_by_key: Dict[str, str] = {}

    for node in payload["nodes"]:
        key_str = node["asset_key_string"]
        group = node.get("group") or "default"
        schema_part = _safe_part(group)
        table_part = _safe_part(key_str)

        schema_fqn = f"{svc_part}.{db_part}.{schema_part}"
        table_fqn = f"{schema_fqn}.{table_part}"
        table_fqn_by_key[key_str] = table_fqn

        if schema_part not in schemas_seen:
            schemas_seen.add(schema_part)
            schemas.append({"name": schema_part, "database_fqn": database_fqn})

        external_url = ""
        if ui_url and node.get("asset_key"):
            external_url = f"{ui_url.rstrip('/')}/assets/{'/'.join(node['asset_key'])}"

        # OpenMetadata Table.columns requires at least one column; emit
        # one synthetic "_value" column when the asset has no schema.
        # Real schema-bearing assets get one column per metadata key.
        raw_metadata = node.get("metadata") or {}
        columns: List[dict] = []
        for k, v in raw_metadata.items():
            columns.append({
                "name": _safe_part(k),
                "dataType": "STRING",
                "description": f"From Dagster metadata: {k}",
            })
        if not columns:
            columns = [{"name": "_value", "dataType": "STRING"}]

        tables.append({
            "name": table_part,
            "schema_fqn": schema_fqn,
            "description": node.get("description") or "",
            "external_url": external_url,
            "columns": columns,
            "tags_props": {
                "dagster_group": group,
                "dagster_kinds": ",".join(node.get("kinds") or []),
                "dagster_deployment": deployment,
                "dagster_asset_key": key_str,
            },
        })

    edges: List[dict] = []
    for edge in payload["edges"]:
        upstream = edge["upstream"]
        downstream = edge["downstream"]
        u_fqn = table_fqn_by_key.get(upstream)
        d_fqn = table_fqn_by_key.get(downstream)
        if not (u_fqn and d_fqn):
            continue
        edges.append({"from_fqn": u_fqn, "to_fqn": d_fqn})

    return {
        "service": {"name": svc_part, "fqn": service_fqn},
        "database": {"name": db_part, "service_fqn": service_fqn, "fqn": database_fqn},
        "schemas": schemas,
        "tables": tables,
        "edges": edges,
    }


def _push(log, plan: dict, base_url: str, token_env: str, verify_ssl: bool):
    """Push the plan to OpenMetadata. Each PUT is idempotent (upsert).

    Order matters: service → database → schemas → tables → lineage edges.
    Lineage PUTs require both endpoints to already exist.
    """
    import requests

    token = os.environ.get(token_env)
    if not token:
        raise RuntimeError(f"Missing {token_env} environment variable")
    base = base_url.rstrip("/")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    def _put(path: str, body: dict, label: str):
        url = f"{base}/api/v1/{path}"
        resp = requests.put(url, json=body, headers=headers, timeout=30, verify=verify_ssl)
        if not resp.ok:
            raise RuntimeError(
                f"OpenMetadata PUT {path} failed ({resp.status_code}) for {label}: {resp.text[:300]}"
            )

    # 1. Database service. We pick MySQL as the type since OM requires a
    #    concrete service type; the asset graph is Dagster's, not MySQL's,
    #    but OM is happy as long as the service kind is valid. The
    #    `displayName` makes it clear in the UI.
    _put(
        "services/databaseServices",
        {
            "name": plan["service"]["name"],
            "displayName": "Dagster",
            "serviceType": "Mysql",
            "connection": {"config": {"type": "Mysql"}},
        },
        f"service {plan['service']['name']}",
    )

    # 2. Database.
    _put(
        "databases",
        {
            "name": plan["database"]["name"],
            "service": plan["database"]["service_fqn"],
        },
        f"database {plan['database']['name']}",
    )

    # 3. Schemas.
    for s in plan["schemas"]:
        _put(
            "databaseSchemas",
            {"name": s["name"], "database": s["database_fqn"]},
            f"schema {s['name']}",
        )

    # 4. Tables.
    for t in plan["tables"]:
        body = {
            "name": t["name"],
            "databaseSchema": t["schema_fqn"],
            "description": t["description"],
            "columns": t["columns"],
        }
        if t.get("external_url"):
            body["sourceUrl"] = t["external_url"]
        if t.get("tags_props"):
            body["extension"] = t["tags_props"]
        _put("tables", body, f"table {t['name']}")

    # 5. Lineage edges.
    edges = plan["edges"]
    for e in edges:
        _put(
            "lineage",
            {
                "edge": {
                    "fromEntity": {"id": e["from_fqn"], "type": "table"},
                    "toEntity": {"id": e["to_fqn"], "type": "table"},
                }
            },
            f"lineage {e['from_fqn']} -> {e['to_fqn']}",
        )

    log.info(
        f"OpenMetadata: ensured 1 service, 1 db, {len(plan['schemas'])} schemas, "
        f"{len(plan['tables'])} tables, {len(edges)} lineage edges"
    )


def _build_partitions_def(partition_type, partition_start, partition_values, dynamic_partition_name):
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, DynamicPartitionsDefinition,
    )
    _pt = partition_type
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
    if _pt in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
    if _pt == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if _pt == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if _pt == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if _pt == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if _pt == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if _pt == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"Unknown partition_type: {_pt!r}")


class LineageToOpenMetadataComponent(dg.Component, dg.Model, dg.Resolvable):
    """Lineage → OpenMetadata — sink asset that depends on lineage_graph and pushes to OpenMetadata."""

    asset_name: str = Field(default="lineage_to_openmetadata", description="Output sink asset name")
    upstream_asset_key: str = Field(
        default="lineage_graph",
        description="Upstream asset emitting the canonical lineage payload (from lineage_graph_extractor).",
    )
    catalog_url: str = Field(
        default="https://openmetadata.example.com",
        description="OpenMetadata host (e.g. https://openmetadata.acme.com). /api/v1/... is appended.",
    )
    api_token_env: str = Field(
        default="OPENMETADATA_API_TOKEN",
        description="Env var holding the JWT bearer token. Generate one in OM under Settings → Bots.",
    )
    service_name: str = Field(
        default="dagster",
        description="OpenMetadata DatabaseService name to ensure-exist + parent the asset graph under.",
    )
    database_name: str = Field(
        default="default",
        description="OpenMetadata Database name under the service. Default 'default'.",
    )
    verify_ssl: bool = Field(
        default=True,
        description="Verify TLS cert. Set False for self-signed dev OpenMetadata.",
    )
    only_push_on_change: bool = Field(
        default=True,
        description=(
            "If true, skip the catalog push when the upstream payload_hash matches "
            "the last successfully pushed hash. Stored as asset metadata across runs."
        ),
    )
    group_name: str = Field(default="lineage")
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries.")
    retry_policy_backoff: str = Field(default="exponential", description="'linear' or 'exponential'.")

    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)

    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[str] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        partitions_def = (
            _build_partitions_def(
                self.partition_type, self.partition_start,
                self.partition_values, self.dynamic_partition_name,
            )
            if self.partition_type
            else None
        )

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=(
                    Backoff.LINEAR
                    if self.retry_policy_backoff.lower() == "linear"
                    else Backoff.EXPONENTIAL
                ),
            )

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        _asset_kwargs = dict(
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or "Sink: pushes asset lineage to OpenMetadata.",
            owners=self.owners,
            tags=self.asset_tags,
            ins={"upstream": dg.AssetIn(dg.AssetKey.from_user_string(self.upstream_asset_key))},
            kinds={"openmetadata", "catalog"},
            retry_policy=_retry_policy,
            freshness_policy=_freshness_policy,
            partitions_def=partitions_def,
        )
        # Strip None-valued kwargs the @asset decorator doesn't accept as None.
        _asset_kwargs = {k: v for k, v in _asset_kwargs.items() if v is not None}

        cfg = self

        @dg.asset(**_asset_kwargs)
        def _sink(context: dg.AssetExecutionContext, upstream: dict) -> dg.MaterializeResult:
            payload = upstream
            payload_hash = (payload.get("sync_metadata") or {}).get("payload_hash") or hashlib.sha256(
                json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()

            # Look up the previous hash from this asset's last materialization.
            prev_hash = None
            try:
                inst = context.instance
                last = inst.get_latest_materialization_event(context.asset_key)
                if last and last.asset_materialization and last.asset_materialization.metadata:
                    raw = last.asset_materialization.metadata.get("last_pushed_hash")
                    if raw is not None:
                        prev_hash = getattr(raw, "text", None) or getattr(raw, "value", None) or str(raw)
            except Exception:
                pass

            if cfg.only_push_on_change and prev_hash == payload_hash:
                context.log.info(f"OpenMetadata: payload hash unchanged ({payload_hash[:12]}…) — skipping push.")
                return dg.MaterializeResult(metadata={
                    "last_pushed_hash": dg.MetadataValue.text(payload_hash),
                    "pushed": dg.MetadataValue.bool(False),
                    "skip_reason": dg.MetadataValue.text("payload_hash unchanged"),
                })

            plan = _build_plan(payload, cfg.service_name, cfg.database_name)
            _push(context.log, plan, cfg.catalog_url, cfg.api_token_env, cfg.verify_ssl)

            return dg.MaterializeResult(metadata={
                "last_pushed_hash": dg.MetadataValue.text(payload_hash),
                "pushed": dg.MetadataValue.bool(True),
                "service": dg.MetadataValue.text(plan["service"]["name"]),
                "schemas_pushed": dg.MetadataValue.int(len(plan["schemas"])),
                "tables_pushed": dg.MetadataValue.int(len(plan["tables"])),
                "edges_pushed": dg.MetadataValue.int(len(plan["edges"])),
            })

        return dg.Definitions(assets=[_sink])
