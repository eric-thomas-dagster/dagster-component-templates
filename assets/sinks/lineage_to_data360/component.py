"""Lineage → Precisely Data360 Govern component.

Sink that pushes the upstream ``lineage_graph`` into Precisely Data360
Govern (the data catalog formerly known as Infogix). Mirrors the shape
of every other ``lineage_to_*`` component in this registry; the only
catalog-specific code is ``_transform`` (Dagster lineage → Data360
object + lineage payloads) and ``_push`` (POST to the Data360 catalog
REST endpoint with OAuth2 client-credentials auth).

Auth follows the same DIS-style OAuth2 client-credentials flow as
``precisely_dis_dq_check``. Endpoints follow Data360 Govern's
documented Catalog REST API:

  - POST {catalog_base}/objects           — upsert asset
  - POST {catalog_base}/lineage           — upsert edge

Validation level: **code** — YAML loads, build_defs returns the sink
asset, runtime path requires a Data360 tenant. Promotes to ``live``
once a customer pilot exercises the catalog endpoint.

Docs: https://help.precisely.com/r/Data360-Govern
"""
import json
import os
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


DATA360_DEFAULT_TOKEN_URL = "https://api.data.precisely.com/oauth/token"


def _transform(payload):
    """Dagster lineage_graph → list of (object, edges) to upsert in Data360."""
    ss = payload.get("source_system", {})
    deployment = ss.get("deployment", "")
    ui_url = ss.get("dagster_ui_url", "")
    asset_type = ss.get("data360_asset_type", "DagsterAsset")

    parent_map = {}
    for edge in payload["edges"]:
        parent_map.setdefault(edge["downstream"], []).append(edge["upstream"])

    objects = []
    edges = []
    for node in payload["nodes"]:
        key_str = node["asset_key_string"]
        custom_props = {
            "dagster_group": node["group"] or "",
            "dagster_kinds": ",".join(node["kinds"]),
            "dagster_deployment": deployment,
        }
        for k, v in node.get("metadata", {}).items():
            custom_props[k] = str(v)

        objects.append({
            "objectIdentifier": key_str,
            "assetType": asset_type,
            "name": key_str,
            "description": node.get("description", ""),
            "externalUrl": f"{ui_url}/assets/{'/'.join(node['asset_key'])}" if ui_url else "",
            "customProperties": custom_props,
        })

        for upstream in parent_map.get(key_str, []):
            edges.append({
                "sourceIdentifier": upstream,
                "sourceAssetType": asset_type,
                "targetIdentifier": key_str,
                "targetAssetType": asset_type,
                "lineageType": "DATA_FLOW",
            })

    return objects, edges


def _push(log, objects, edges, catalog_base, token_url, client_id_env, client_secret_env, timeout_seconds):
    import requests
    client_id = os.environ.get(client_id_env, "")
    client_secret = os.environ.get(client_secret_env, "")
    if not client_id or not client_secret:
        raise RuntimeError(
            f"Data360 creds missing: set {client_id_env} and {client_secret_env}."
        )
    # OAuth2 client-credentials.
    token_resp = requests.post(
        token_url,
        data={"grant_type": "client_credentials"},
        auth=(client_id, client_secret),
        timeout=timeout_seconds,
    )
    token_resp.raise_for_status()
    token = token_resp.json()["access_token"]
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    base = catalog_base.rstrip("/")
    pushed_objects = 0
    pushed_edges = 0
    for obj in objects:
        resp = requests.post(f"{base}/objects", json=obj, headers=headers, timeout=timeout_seconds)
        resp.raise_for_status()
        pushed_objects += 1
    for edge in edges:
        resp = requests.post(f"{base}/lineage", json=edge, headers=headers, timeout=timeout_seconds)
        resp.raise_for_status()
        pushed_edges += 1
    log.info(f"Data360: upserted {pushed_objects} objects + {pushed_edges} lineage edges")
    return pushed_objects, pushed_edges


class LineageToData360Component(dg.Component, dg.Model, dg.Resolvable):
    """Lineage → Precisely Data360 Govern — sink asset that depends on lineage_graph.

    Example:
        ```yaml
        type: dagster_community_components.LineageToData360Component
        attributes:
          asset_name: lineage_to_data360
          upstream_asset_key: lineage_graph
          catalog_url: https://api.data.precisely.com/data360/catalog
          client_id_env: PRECISELY_DIS_CLIENT_ID
          client_secret_env: PRECISELY_DIS_CLIENT_SECRET
        ```

    Pair with the registry's ``lineage_graph_extractor`` component as
    the upstream — it produces the canonical lineage payload every
    ``lineage_to_*`` sink consumes.
    """

    asset_name: str = Field(default="lineage_to_data360", description="Output sink asset name.")
    upstream_asset_key: str = Field(
        default="lineage_graph",
        description="Upstream asset emitting the canonical lineage payload (lineage_graph_extractor).",
    )
    catalog_url: str = Field(
        default="https://api.data.precisely.com/data360/catalog",
        description="Data360 Govern Catalog REST base URL.",
    )
    token_url: str = Field(
        default=DATA360_DEFAULT_TOKEN_URL,
        description="OAuth2 token endpoint (override for non-global Data360 tenants).",
    )
    client_id_env: str = Field(
        default="PRECISELY_DIS_CLIENT_ID",
        description="Env var with the Data360 OAuth2 client_id.",
    )
    client_secret_env: str = Field(
        default="PRECISELY_DIS_CLIENT_SECRET",
        description="Env var with the Data360 OAuth2 client_secret.",
    )
    request_timeout_seconds: int = Field(
        default=30,
        ge=1,
        description="Per-request HTTP timeout.",
    )
    only_push_on_change: bool = Field(
        default=True,
        description=(
            "If true, skip the catalog POST when the upstream payload_hash "
            "matches the last successfully pushed hash. Stored as asset "
            "metadata across runs."
        ),
    )

    group_name: str = Field(default="lineage")
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        upstream_key = dg.AssetKey.from_user_string(self.upstream_asset_key)
        kinds = list(self.kinds) if self.kinds else ["lineage", "data360", "precisely"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            ins={"upstream": dg.AssetIn(key=upstream_key)},
            group_name=_self.group_name,
            description=_self.description or "Push Dagster lineage_graph to Precisely Data360 Govern.",
            owners=_self.owners or [],
            tags=tags,
        )
        def _asset(context: dg.AssetExecutionContext, upstream) -> dg.MaterializeResult:
            payload = upstream if isinstance(upstream, dict) else json.loads(upstream)

            # Hash-based dedup. We store the last pushed hash in materialization
            # metadata; if it matches the upstream's payload_hash, skip.
            payload_hash = payload.get("payload_hash") or ""
            if _self.only_push_on_change and payload_hash:
                # Best-effort fetch of the last materialization's metadata.
                # If the lookup fails, fall through to push.
                try:
                    instance = context.instance
                    last = instance.get_latest_materialization_event(context.asset_key)
                    last_meta = (last.dagster_event.event_specific_data.materialization.metadata
                                 if last and last.dagster_event and last.dagster_event.event_specific_data
                                 else {})
                    last_hash = last_meta.get("payload_hash")
                    if last_hash and getattr(last_hash, "value", None) == payload_hash:
                        return dg.MaterializeResult(
                            metadata={
                                "skipped": dg.MetadataValue.bool(True),
                                "reason": "payload_hash unchanged",
                                "payload_hash": payload_hash,
                            },
                        )
                except Exception:
                    pass  # fall through to push

            objects, edges = _transform(payload)
            pushed_objects, pushed_edges = _push(
                context.log, objects, edges,
                _self.catalog_url, _self.token_url,
                _self.client_id_env, _self.client_secret_env,
                _self.request_timeout_seconds,
            )

            return dg.MaterializeResult(
                metadata={
                    "objects_pushed": dg.MetadataValue.int(pushed_objects),
                    "lineage_edges_pushed": dg.MetadataValue.int(pushed_edges),
                    "data360/catalog_url": dg.MetadataValue.url(_self.catalog_url),
                    "payload_hash": payload_hash,
                },
            )

        return dg.Definitions(assets=[_asset])
