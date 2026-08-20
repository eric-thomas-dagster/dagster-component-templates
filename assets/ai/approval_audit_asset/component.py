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


# ── Inline fs helper: local + cloud (s3://, gs://, abfs://) via fsspec ──
# Kept inline per DCC's self-contained convention — no cross-component imports.
class _ApprovalFS:
    """Uniform read/write/list over `approval_dir`. Plain paths use
    pathlib (no fsspec dependency); URIs (`s3://`, `gs://`, `abfs://`)
    route through fsspec + the appropriate driver (`s3fs` / `gcsfs` /
    `adlfs` — install what you need)."""
    def __init__(self, root: str):
        if "://" in root:
            import fsspec
            fs, rt = fsspec.core.url_to_fs(root)
            self.fs, self.root, self.is_uri = fs, rt.rstrip("/"), True
        else:
            self.fs, self.root, self.is_uri = None, str(Path(root).expanduser().resolve()), False
    def path(self, *parts: str) -> str:
        pieces = [self.root, *[p.strip("/") for p in parts if p]]
        return "/".join(pieces) if self.is_uri else str(Path(*pieces))
    def exists(self, p: str) -> bool:
        return bool(self.fs.exists(p)) if self.is_uri else Path(p).exists()
    def read_text(self, p: str) -> str:
        if self.is_uri:
            with self.fs.open(p, "r") as f:
                return f.read()
        return Path(p).read_text()
    def glob(self, pattern: str) -> List[str]:
        if self.is_uri:
            proto = self.fs.protocol if isinstance(self.fs.protocol, str) else self.fs.protocol[0]
            return [f"{proto}://{m}" for m in self.fs.glob(self.path(pattern))]
        return [str(p) for p in Path(self.root).glob(pattern)]
    def rglob(self, pattern: str) -> List[str]:
        if self.is_uri:
            import fnmatch
            proto = self.fs.protocol if isinstance(self.fs.protocol, str) else self.fs.protocol[0]
            return [f"{proto}://{m}" for m in self.fs.find(self.root)
                    if fnmatch.fnmatch(m.split("/")[-1], pattern)]
        return [str(p) for p in Path(self.root).rglob(pattern)]
    def mtime_iso(self, path: str) -> Optional[str]:
        try:
            if self.is_uri:
                info = self.fs.info(path)
                mt = info.get("LastModified") or info.get("mtime")
                if hasattr(mt, "isoformat"):
                    return mt.isoformat()
                if isinstance(mt, (int, float)):
                    return datetime.fromtimestamp(mt, tz=timezone.utc).isoformat()
                return None
            return datetime.fromtimestamp(Path(path).stat().st_mtime, tz=timezone.utc).isoformat()
        except Exception:  # noqa: BLE001
            return None
    def stem(self, path: str) -> str:
        base = path.rstrip("/").split("/")[-1]
        return base.rsplit(".", 1)[0] if "." in base else base


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
            fs = _ApprovalFS(approval_dir)
            root_display = fs.root
            # Only local paths have `exists()` on the root — cloud stores are
            # prefix-based; skipping the check on cloud is safe (an empty
            # bucket returns [] from glob).
            if not fs.is_uri and not Path(fs.root).exists():
                context.log.warning(f"approval_dir does not exist yet: {root_display}")
                return dg.MaterializeResult(
                    metadata={
                        "n_tokens": 0,
                        "approval_dir": root_display,
                        "status": "no_dir_yet",
                    },
                )

            paths = fs.rglob(token_glob) if recursive else fs.glob(token_glob)
            rows: List[Dict[str, Any]] = []
            for p in paths:
                try:
                    token = json.loads(fs.read_text(p))
                except (json.JSONDecodeError, OSError, IsADirectoryError) as e:
                    context.log.warning(f"skip malformed token {p}: {e}")
                    continue
                partition_key = fs.stem(p)
                decided_at = token.get("decided_at") or fs.mtime_iso(p) or _now_iso()
                source = token.get("source") or _infer_source(token)
                rows.append({
                    "partition_key": partition_key,
                    "approved": bool(token.get("approved", False)),
                    "approver": str(token.get("approver") or ""),
                    "reason": str(token.get("reason") or ""),
                    "feedback": str(token.get("feedback") or ""),
                    "decided_at": decided_at,
                    "token_file": p,
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
                    "approval_dir": root_display,
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


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
