"""SiemEventNormalizerComponent.

Normalize heterogeneous audit-log events to a common schema (OCSF or ECS) before shipping to a SIEM.
"""

import json
import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


class SiemEventNormalizerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Normalize heterogeneous audit-log events to a common schema (OCSF or ECS) before shipping to a SIEM."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame to normalize")

    schema: str = Field(default="ocsf", description="Target schema: 'ocsf' (Open Cybersecurity Schema Framework) | 'ecs' (Elastic Common Schema)")
    source_kind: str = Field(default="generic", description="Source hint: 'cloudtrail' | 'okta' | 'github' | 'azure' | 'generic'")
    event_column: Optional[str] = Field(
        default=None,
        description=(
            "If set, treat this column as a JSON-encoded event payload and parse it into "
            "the source columns before normalization. Use when the upstream stores raw "
            "events as JSON strings in a single column."
        ),
    )
    timestamp_column: Optional[str] = Field(default=None, description="Source column to map to event timestamp (auto-detect if None)")
    actor_column: Optional[str] = Field(default=None, description="Source column for actor/user (auto-detect if None)")
    action_column: Optional[str] = Field(default=None, description="Source column for action/event name")
    drop_extras: bool = Field(default=False, description="Drop source columns that don't map to the target schema")

    description: Optional[str] = Field(default=None)
    group_name: str = Field(default="security_normalize")
    owners: Optional[list[str]] = Field(default=None)
    asset_tags: Optional[dict] = Field(default=None)
    kinds: Optional[list[str]] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.asset(
            name=self.asset_name,
            description=self.description or "Normalize heterogeneous audit-log events to a common schema (OCSF or ECS) before shipping to a SIEM.",
            group_name=self.group_name,
            kinds=set(self.kinds or ['security', 'ocsf', 'ecs']),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame) -> pd.DataFrame:
            # If event_column is set, parse JSON payloads out into individual columns first.
            if _self.event_column and _self.event_column in df.columns:
                parsed = df[_self.event_column].apply(
                    lambda v: json.loads(v) if isinstance(v, str) and v.startswith("{") else {}
                )
                expanded = pd.json_normalize(parsed.tolist())
                # Concat parsed columns alongside the originals so auto-detect can find them.
                df = pd.concat([df.reset_index(drop=True), expanded.reset_index(drop=True)], axis=1)
            # Auto-detect commonly-named fields from each source
            auto_ts = {
                "cloudtrail": "EventTime",
                "okta": "published",
                "github": "@timestamp",
                "azure": "event_timestamp",
                "generic": next((c for c in df.columns if c.lower() in ("timestamp", "@timestamp", "time", "event_time", "eventtime", "occurredat")), None),
            }
            auto_actor = {
                "cloudtrail": "Username",
                "okta": "actor.alternateId",
                "github": "actor",
                "azure": "caller",
                "generic": next((c for c in df.columns if c.lower() in ("user", "username", "actor", "principal", "email")), None),
            }
            auto_action = {
                "cloudtrail": "EventName",
                "okta": "eventType",
                "github": "action",
                "azure": "operation_name",
                "generic": next((c for c in df.columns if c.lower() in ("action", "event_name", "eventname", "eventtype", "operation_name")), None),
            }
            ts_col = _self.timestamp_column or auto_ts.get(_self.source_kind)
            actor_col = _self.actor_column or auto_actor.get(_self.source_kind)
            action_col = _self.action_column or auto_action.get(_self.source_kind)

            out = pd.DataFrame()
            if _self.schema == "ocsf":
                # OCSF Activity event: time, actor.user.name, activity_name, raw_data
                out["time"] = df[ts_col] if ts_col and ts_col in df.columns else None
                out["activity_name"] = df[action_col] if action_col and action_col in df.columns else None
                out["actor.user.name"] = df[actor_col] if actor_col and actor_col in df.columns else None
                out["metadata.product.vendor_name"] = _self.source_kind
                out["raw_data"] = df.apply(lambda r: r.to_json(), axis=1)
            elif _self.schema == "ecs":
                # ECS: @timestamp, event.action, user.name, source
                out["@timestamp"] = df[ts_col] if ts_col and ts_col in df.columns else None
                out["event.action"] = df[action_col] if action_col and action_col in df.columns else None
                out["user.name"] = df[actor_col] if actor_col and actor_col in df.columns else None
                out["event.dataset"] = _self.source_kind
                out["event.original"] = df.apply(lambda r: r.to_json(), axis=1)
            else:
                raise ValueError(f"Unknown schema: {_self.schema}")

            if not _self.drop_extras:
                # Pass-through original fields (prefixed) for debuggability
                for c in df.columns:
                    if c not in out.columns:
                        out[f"original.{c}"] = df[c]

            df = out
            context.add_output_metadata({
                "dagster/row_count": dg.MetadataValue.int(len(df)),
                "schema": dg.MetadataValue.text(_self.schema),
                "source_kind": dg.MetadataValue.text(_self.source_kind),
            })
            return df

        return dg.Definitions(assets=[_asset])
