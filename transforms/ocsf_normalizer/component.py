"""OcsfNormalizerComponent.

Maps source-specific event records to OCSF v1.1 (Open Cybersecurity Schema
Framework) — the open-standard schema adopted by AWS Security Lake, Splunk
Federated Search, Cisco, IBM, and others.

Per-source mapping tables ship for: dagster_plus, aws_cloudtrail, okta,
github, azure_activity, slack, generic. Override `event_class_uid` and
`activity_map` to extend.
"""

import json
import os
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


# OCSF v1.1 categories
CATEGORY_UIDS = {
    "system_activity": 1,
    "findings": 2,
    "iam": 3,
    "network_activity": 4,
    "discovery": 5,
    "application_activity": 6,
    "remediation": 7,
}

# (class_uid, category_uid, default activity_id)
SOURCE_MAPS = {
    "dagster_plus": {
        # event_type → (class_uid, category_uid, activity_id)
        "LOG_IN":              (3002, 3, 1),  # Authentication / Logon
        "LOG_OUT":             (3002, 3, 2),  # Authentication / Logoff
        "USER_INVITED":        (3005, 3, 1),  # Account Change / Create
        "USER_REMOVED":        (3005, 3, 3),  # Account Change / Delete
        "USER_DELETED":        (3005, 3, 3),
        "ROLE_GRANTED":        (3006, 3, 1),  # User Access / Assign Privileges
        "ROLE_REVOKED":        (3006, 3, 2),
        "DEPLOYMENT_CREATED":  (6002, 6, 1),  # Application Lifecycle / Install
        "DEPLOYMENT_UPDATED":  (6002, 6, 2),
        "DEPLOYMENT_DELETED":  (6002, 6, 4),
        "TOKEN_CREATED":       (3005, 3, 1),
        "TOKEN_REVOKED":       (3005, 3, 3),
        "AGENT_STARTED":       (6002, 6, 1),
        "AGENT_STOPPED":       (6002, 6, 4),
    },
    "aws_cloudtrail": {
        "ConsoleLogin":        (3002, 3, 1),
        "AssumeRole":          (3003, 3, 1),  # Authorize Session
        "CreateUser":          (3005, 3, 1),
        "DeleteUser":          (3005, 3, 3),
        "UpdateUser":          (3005, 3, 2),
        "AttachRolePolicy":    (3006, 3, 1),
        "DetachRolePolicy":    (3006, 3, 2),
        "CreateAccessKey":     (3005, 3, 1),
        "DeleteAccessKey":     (3005, 3, 3),
    },
    "okta": {
        "user.session.start":         (3002, 3, 1),
        "user.session.end":           (3002, 3, 2),
        "user.authentication.auth_via_mfa": (3002, 3, 1),
        "user.lifecycle.create":      (3005, 3, 1),
        "user.lifecycle.delete":      (3005, 3, 3),
        "user.lifecycle.activate":    (3005, 3, 2),
        "user.lifecycle.deactivate":  (3005, 3, 2),
        "policy.grant.add":           (3006, 3, 1),
        "policy.grant.remove":        (3006, 3, 2),
    },
    "github": {
        "org.add_member":             (3005, 3, 1),
        "org.remove_member":          (3005, 3, 3),
        "team.add_member":            (3006, 3, 1),
        "team.remove_member":         (3006, 3, 2),
        "repo.create":                (6002, 6, 1),
        "repo.destroy":               (6002, 6, 4),
        "workflows.completed_workflow_run": (6003, 6, 1),
    },
    "azure_activity": {
        # operation_name (best-guess prefixes)
        "Microsoft.Authorization/roleAssignments/write":  (3006, 3, 1),
        "Microsoft.Authorization/roleAssignments/delete": (3006, 3, 2),
    },
    "slack": {
        "user_login":              (3002, 3, 1),
        "user_login_failed":       (3002, 3, 1),
        "user_logout":             (3002, 3, 2),
        "user_created":            (3005, 3, 1),
        "user_deactivated":        (3005, 3, 3),
        "permissions_changed":     (3006, 3, 1),
    },
}


def _to_epoch_ms(value) -> Optional[int]:
    """Coerce a timestamp value (epoch s/ms or ISO string) to epoch ms."""
    import datetime as dt
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value) if value > 1e12 else int(value * 1000)
    try:
        s = str(value).replace("Z", "+00:00")
        parsed = dt.datetime.fromisoformat(s)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return int(parsed.timestamp() * 1000)
    except Exception:
        return None


class OcsfNormalizerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Normalize source events to OCSF v1.1 with class/category/activity IDs."""

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream DataFrame to normalize")
    source_kind: str = Field(
        default="generic",
        description="dagster_plus | aws_cloudtrail | okta | github | azure_activity | slack | generic",
    )

    # Field mappings — keep auto-detect defaults but allow override
    event_type_column: Optional[str] = Field(default=None, description="Source column holding the event type string (auto-detected when None)")
    timestamp_column: Optional[str] = Field(default=None, description="Source column for the event timestamp (auto-detected when None)")
    actor_email_column: Optional[str] = Field(default=None, description="Source column for actor email (auto-detected when None)")
    actor_name_column: Optional[str] = Field(default=None, description="Source column for actor display name (auto-detected when None)")

    # Identity
    vendor_name: str = Field(default="Dagster", description="metadata.product.vendor_name")
    product_name: str = Field(default="Dagster+", description="metadata.product.name")
    ocsf_version: str = Field(default="1.1.0", description="metadata.version")

    # Overrides
    activity_map: Optional[dict] = Field(
        default=None,
        description="Per-event-type override: {event_type: [class_uid, category_uid, activity_id]}",
    )
    default_class_uid: int = Field(
        default=0,
        description="Fallback class_uid for unmapped events (0 = Base Event)",
    )
    default_severity_id: int = Field(
        default=1,
        description="OCSF severity_id (0=Unknown, 1=Informational, 2=Low, 3=Medium, 4=High, 5=Critical, 6=Fatal)",
    )

    drop_unmapped: bool = Field(default=False, description="Drop rows whose event_type is not in the mapping table")
    keep_raw: bool = Field(default=True, description="Include the original event as `unmapped`")

    description: Optional[str] = Field(default=None)
    group_name: str = Field(default="ocsf")
    owners: Optional[list[str]] = Field(default=None)
    asset_tags: Optional[dict] = Field(default=None)
    kinds: Optional[list[str]] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.asset(
            name=self.asset_name,
            description=self.description or f"OCSF v{self.ocsf_version} normalized events",
            group_name=self.group_name,
            kinds=set(self.kinds or ["ocsf", "security"]),
            deps=[dg.AssetKey.from_user_string(self.upstream_asset_key)],
            ins={"df": dg.AssetIn(key=dg.AssetKey.from_user_string(self.upstream_asset_key))},
            owners=self.owners or None,
            tags=self.asset_tags or None,
        )
        def _asset(context: dg.AssetExecutionContext, df: pd.DataFrame) -> pd.DataFrame:
            mapping = dict(SOURCE_MAPS.get(_self.source_kind, {}))
            if _self.activity_map:
                for k, v in _self.activity_map.items():
                    mapping[k] = tuple(v)

            # Auto-detect columns when not explicitly set
            def _autodetect(*candidates):
                for c in candidates:
                    if c in df.columns:
                        return c
                return None

            ts_col = _self.timestamp_column or _autodetect(
                "timestamp", "@timestamp", "EventTime", "event_timestamp", "occurredAt", "creationTime",
            )
            event_col = _self.event_type_column or _autodetect(
                "eventType", "EventName", "event_type", "action", "operation_name", "operationName",
            )
            email_col = _self.actor_email_column or _autodetect(
                "userEmail", "actor.alternateId", "Username", "user.email", "actor",
            )
            name_col = _self.actor_name_column or _autodetect(
                "actor.displayName", "user.name", "userName",
            )

            rows = []
            unmapped = 0
            for _, row in df.iterrows():
                event_type = str(row.get(event_col, "")) if event_col else ""
                triple = mapping.get(event_type)
                if not triple:
                    if _self.drop_unmapped and event_type:
                        continue
                    triple = (_self.default_class_uid, _self.default_class_uid // 1000, 0)
                    unmapped += 1
                class_uid, category_uid, activity_id = triple

                rec = {
                    "time": _to_epoch_ms(row.get(ts_col)) if ts_col else None,
                    "class_uid": int(class_uid),
                    "category_uid": int(category_uid),
                    "activity_id": int(activity_id),
                    "severity_id": int(_self.default_severity_id),
                    "type_uid": int(class_uid) * 100 + int(activity_id),
                    "actor.user.email_addr": row.get(email_col) if email_col else None,
                    "actor.user.name": row.get(name_col) if name_col else (row.get(email_col) if email_col else None),
                    "metadata.product.vendor_name": _self.vendor_name,
                    "metadata.product.name": _self.product_name,
                    "metadata.version": _self.ocsf_version,
                    "raw_event_type": event_type,
                }
                if _self.keep_raw:
                    rec["unmapped"] = row.to_json()
                rows.append(rec)

            out = pd.DataFrame(rows)
            context.add_output_metadata({
                "dagster/row_count": dg.MetadataValue.int(len(out)),
                "ocsf_version": dg.MetadataValue.text(_self.ocsf_version),
                "source_kind": dg.MetadataValue.text(_self.source_kind),
                "unmapped_rows": dg.MetadataValue.int(unmapped),
                "mapping_entries": dg.MetadataValue.int(len(mapping)),
            })
            return out

        return dg.Definitions(assets=[_asset])
