"""ApprovalAuditAssetComponent.

Materializes the full who/when/why history of an approval workflow as a
first-class Dagster DataFrame asset. Reads every JSON approval token in
the shared `approval_dir` (written by HumanApprovalGate, SlackApprovalGate,
TeamsApprovalGate — or any external system dropping tokens in that dir)
and emits one row per token with columns:

    partition_key       str    (or default_approval_key when unpartitioned)
    approved            bool
    approver            str    (email / Slack user id / Teams user id)
    reason              str    (approver-supplied rationale)
    feedback            str    (optional — rejection feedback for re-run loops)
    decided_at          str    ISO timestamp — file mtime if not in token
    token_file          str    absolute path
    upstream_asset      str    (optional — from `upstream_asset` field if present)
    source              str    inferred: slack | teams | filesystem | external

Governance-grade audit surface: point Insights at it, join to run history,
export to your GRC system on a schedule, or wire an asset check that fails
if any approval is older than N days without a downstream materialization.

Complements the approval-gate components — read-only, additive, no
coupling. Runs on a schedule (default daily) or on-demand.
"""
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class ApprovalAuditAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Materialize approval-token history from `approval_dir` as a Dagster DataFrame asset.

    Reads every `*.json` in the directory (recursively if `recursive: true`),
    parses each as an approval token, emits one row per token. Independent
    of which component WROTE the token — audits Human / Slack / Teams /
    external-webhook tokens uniformly.
    """

    asset_name: str = Field(
        description="Dagster asset name for the audit DataFrame (e.g. `approval_audit`).",
    )
    approval_dir: str = Field(
        description=(
            "Directory of JSON approval tokens (same dir the approval-gate "
            "components write into). Absolute path recommended."
        ),
    )
    recursive: bool = Field(
        default=False,
        description="If true, walk subdirectories under `approval_dir`.",
    )
    token_glob: str = Field(
        default="*.json",
        description="Glob pattern for token files. Default `*.json`.",
    )

    # Catalog / governance
    group_name: Optional[str] = Field(
        default="governance",
        description="Asset group. Default: 'governance'.",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Default: ['audit', 'approval', 'governance'].",
    )
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    description: Optional[str] = Field(default=None)

    # Optional freshness policy
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Freshness policy: warn if audit is older than N minutes.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron for the freshness policy (paired with max_lag_minutes).",
    )

    # Optional schedule
    schedule_cron: Optional[str] = Field(
        default=None,
        description=(
            "Cron expression for auto-materialization (e.g. '0 * * * *' hourly). "
            "Omit for on-demand materialization only."
        ),
    )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Approval Audit Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        approval_dir = self.approval_dir
        recursive = self.recursive
        token_glob = self.token_glob

        kinds = self.kinds or ["audit", "approval", "governance"]
        tag_map = dict(self.tags or {})
        for k in kinds:
            tag_map[f"dagster/kind/{k}"] = ""

        freshness = None
        if self.freshness_max_lag_minutes is not None:
            freshness = dg.FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Approval audit — every token in {approval_dir}",
            group_name=self.group_name,
            tags=tag_map,
            owners=self.owners or [],
            freshness_policy=freshness,
        )
        def _audit(context: dg.AssetExecutionContext):
            import pandas as pd
            root = Path(approval_dir).expanduser().resolve()
            if not root.exists():
                context.log.warning(f"approval_dir does not exist yet: {root}")
                return dg.MaterializeResult(
                    metadata={
                        "n_tokens": 0,
                        "approval_dir": str(root),
                        "status": "no_dir_yet",
                    },
                )

            paths = list(root.rglob(token_glob)) if recursive else list(root.glob(token_glob))
            rows: List[Dict[str, Any]] = []
            for p in paths:
                if not p.is_file():
                    continue
                try:
                    token = json.loads(p.read_text())
                except (json.JSONDecodeError, OSError) as e:
                    context.log.warning(f"skip malformed token {p}: {e}")
                    continue
                partition_key = p.stem  # filename without extension
                decided_at = token.get("decided_at") or datetime.fromtimestamp(
                    p.stat().st_mtime, tz=timezone.utc
                ).isoformat()
                source = token.get("source") or _infer_source(token)
                rows.append({
                    "partition_key": partition_key,
                    "approved": bool(token.get("approved", False)),
                    "approver": str(token.get("approver") or ""),
                    "reason": str(token.get("reason") or ""),
                    "feedback": str(token.get("feedback") or ""),
                    "decided_at": decided_at,
                    "token_file": str(p),
                    "upstream_asset": str(token.get("upstream_asset") or ""),
                    "source": source,
                })
            df = pd.DataFrame(rows, columns=[
                "partition_key", "approved", "approver", "reason",
                "feedback", "decided_at", "token_file", "upstream_asset", "source",
            ])
            df = df.sort_values("decided_at", ascending=False).reset_index(drop=True)

            n_approved = int(df["approved"].sum()) if not df.empty else 0
            n_rejected = int((~df["approved"]).sum()) if not df.empty else 0
            n_with_feedback = int((df["feedback"].str.len() > 0).sum()) if not df.empty else 0
            preview = df.head(20).to_markdown(index=False) if not df.empty else "_(no tokens yet)_"

            context.log.info(
                f"audited {len(df)} token(s): {n_approved} approved, {n_rejected} rejected, "
                f"{n_with_feedback} with feedback"
            )

            return dg.MaterializeResult(
                value=df,
                metadata={
                    "n_tokens": len(df),
                    "n_approved": n_approved,
                    "n_rejected": n_rejected,
                    "n_with_feedback": n_with_feedback,
                    "approval_dir": str(root),
                    "recursive": recursive,
                    "polled_at": time.time(),
                    "preview_first_20": dg.MetadataValue.md(preview),
                },
            )

        defs_kwargs: Dict[str, Any] = {"assets": [_audit]}

        if self.schedule_cron:
            job = dg.define_asset_job(
                name=f"{asset_name}_job",
                selection=dg.AssetSelection.assets(dg.AssetKey.from_user_string(asset_name)),
            )
            schedule = dg.ScheduleDefinition(
                name=f"{asset_name}_schedule",
                cron_schedule=self.schedule_cron,
                job=job,
                default_status=dg.DefaultScheduleStatus.RUNNING,
            )
            defs_kwargs["jobs"] = [job]
            defs_kwargs["schedules"] = [schedule]

        return dg.Definitions(**defs_kwargs)


def _infer_source(token: Dict[str, Any]) -> str:
    """Heuristic: infer which component wrote the token from its fields.
    Preserves exact provenance when the writer sets `source:` explicitly."""
    approver = str(token.get("approver") or "")
    if approver.startswith("U") and len(approver) >= 9 and approver[1:].replace("-", "").isalnum():
        return "slack"
    if "@" in approver and "graph.microsoft.com" in str(token.get("via", "")):
        return "teams"
    if "@" in approver:
        return "filesystem"  # email-shaped, most likely a direct file drop
    return "external"
