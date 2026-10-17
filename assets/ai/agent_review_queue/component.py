"""AgentReviewQueueComponent -- the review queue for agentic pipelines.

`agentic_pipeline`'s `approve` op writes a pending record to `<approval_dir>/pending/<key>.json` whenever a human decision is
needed. This asset aggregates those records across one or more approval directories (local, or `s3://` / `gs://` / `abfs://` via
fsspec) into a DataFrame -- "what is waiting for a person, for how long, and what will happen if nobody answers" -- visible in the
Dagster UI, with an asset check that flags overdue reviews.

It is read-only: it never approves or rejects anything. Decisions are made by writing the token file named in each row's
`approve_path` (the same contract as `human_approval_gate`).
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class _Store:
    """Local directory or any fsspec URI (s3://, gs://, abfs://, memory://). Install the driver you need (s3fs/gcsfs/adlfs)."""

    def __init__(self, root: str):
        self.is_uri = "://" in root
        if self.is_uri:
            import fsspec
            self.fs, self.root = fsspec.core.url_to_fs(root)
            self.root = self.root.rstrip("/")
        else:
            self.fs, self.root = None, str(Path(root).expanduser().resolve())
        self.display = root.rstrip("/")

    def pending_files(self) -> List[str]:
        d = f"{self.root}/pending"
        if self.is_uri:
            return sorted(self.fs.glob(f"{d}/*.json")) if self.fs.exists(d) else []
        p = Path(d)
        return sorted(str(x) for x in p.glob("*.json")) if p.exists() else []

    def read_text(self, path: str) -> str:
        if self.is_uri:
            with self.fs.open(path, "r") as f:
                return f.read()
        return Path(path).read_text()


def _parse_ts(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        t = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return t if t.tzinfo else t.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _collect_pending(approval_dirs: List[str], overdue_after_minutes: float, now: Optional[datetime] = None) -> List[Dict[str, Any]]:
    now = now or datetime.now(timezone.utc)
    rows: List[Dict[str, Any]] = []
    for root in approval_dirs:
        store = _Store(root)
        for path in store.pending_files():
            try:
                rec = json.loads(store.read_text(path))
            except (OSError, json.JSONDecodeError):
                rows.append({"approval_dir": root, "key": Path(path).stem, "step": None, "summary": "(unreadable pending record)",
                             "requested_at": None, "expires_at": None, "age_minutes": None, "overdue": True,
                             "on_timeout": None, "approve_path": None})
                continue
            requested = _parse_ts(rec.get("requested_at"))
            expires = _parse_ts(rec.get("expires_at"))
            age = round((now - requested).total_seconds() / 60.0, 2) if requested else None
            overdue = bool((expires and now > expires) or (age is not None and age > overdue_after_minutes))
            rows.append({
                "approval_dir": root, "key": rec.get("key") or Path(path).stem, "step": rec.get("step"),
                "summary": rec.get("summary"), "requested_at": rec.get("requested_at"), "expires_at": rec.get("expires_at"),
                "age_minutes": age, "overdue": overdue, "on_timeout": rec.get("on_timeout"),
                "approve_path": (rec.get("approve_by_writing") or {}).get("path"),
            })
    return rows


class AgentReviewQueueComponent(dg.Component, dg.Model, dg.Resolvable):
    """List the reviews waiting on a person (written by `agentic_pipeline`'s `approve` op) as a DataFrame asset, with an
    asset check that flags overdue reviews. Read-only."""

    asset_name: str = Field(default="agent_review_queue", description="Dagster asset name")
    approval_dirs: List[str] = Field(
        description=(
            "Approval directories to scan -- the same `approval_dir:` values your `approve` steps use. Local paths or "
            "fsspec URIs (s3://, gs://, abfs://). Pending records live in `<dir>/pending/*.json`."
        ),
    )
    overdue_after_minutes: float = Field(
        default=60.0,
        description="A pending review older than this is overdue (it is also overdue once its own `expires_at` has passed).",
    )
    overdue_severity: str = Field(default="WARN", description="Asset check severity when reviews are overdue: 'WARN' or 'ERROR'.")
    cron_schedule: Optional[str] = Field(
        default=None,
        description="Optional cron expression; when set, a schedule re-materializes this asset so the queue stays current (e.g. '*/5 * * * *').",
    )
    group_name: Optional[str] = Field(default="agents", description="Asset group")
    description: Optional[str] = Field(default=None, description="Asset description")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners (team:name or emails)")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog (default: ['agent', 'review'])")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Tags added to the asset")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds'), csv=(), tags=('asset_tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.overdue_severity.upper() not in ("WARN", "ERROR"):
            raise ValueError("overdue_severity must be 'WARN' or 'ERROR'")
        if not self.approval_dirs:
            raise ValueError("approval_dirs needs at least one directory")
        severity = dg.AssetCheckSeverity.ERROR if self.overdue_severity.upper() == "ERROR" else dg.AssetCheckSeverity.WARN
        dirs, overdue_after = list(self.approval_dirs), float(self.overdue_after_minutes)
        asset_key = dg.AssetKey.from_user_string(self.asset_name)
        check_name = "no_overdue_reviews"
        tags = {**(self.asset_tags or {}), **{f"dagster/kind/{k}": "" for k in (self.kinds or ["agent", "review"])}}

        @dg.asset(
            key=asset_key, group_name=self.group_name, description=self.description or "Reviews waiting on a person.",
            owners=self.owners or None, tags=tags,
            check_specs=[dg.AssetCheckSpec(name=check_name, asset=asset_key, description="No pending review is overdue.")],
        )
        def _queue(context: dg.AssetExecutionContext):
            import pandas as pd
            rows = _collect_pending(dirs, overdue_after)
            df = pd.DataFrame(rows, columns=["approval_dir", "key", "step", "summary", "requested_at", "expires_at",
                                              "age_minutes", "overdue", "on_timeout", "approve_path"])
            overdue = df[df["overdue"]] if len(df) else df
            ages = df["age_minutes"].dropna() if len(df) else df.get("age_minutes", [])
            md: Dict[str, Any] = {
                "dagster/row_count": dg.MetadataValue.int(len(df)),
                "pending_count": dg.MetadataValue.int(len(df)),
                "overdue_count": dg.MetadataValue.int(len(overdue)),
                "oldest_age_minutes": dg.MetadataValue.float(float(max(ages)) if len(ages) else 0.0),
            }
            if len(df):
                md["queue"] = dg.MetadataValue.md(df[["key", "step", "age_minutes", "overdue", "summary"]].head(50).to_markdown(index=False))
            yield dg.Output(df, metadata=md)
            yield dg.AssetCheckResult(
                check_name=check_name, passed=len(overdue) == 0, severity=severity,
                metadata={"overdue_keys": dg.MetadataValue.json(overdue["key"].tolist() if len(overdue) else []),
                          "overdue_after_minutes": overdue_after},
                description=f"{len(overdue)} of {len(df)} pending review(s) overdue" if len(df) else "queue is empty",
            )

        schedules = []
        if self.cron_schedule:
            schedules.append(dg.ScheduleDefinition(
                name=f"{self.asset_name}_refresh", cron_schedule=self.cron_schedule,
                target=dg.AssetSelection.assets(asset_key) | dg.AssetSelection.checks_for_assets(asset_key),
            ))
        return dg.Definitions(assets=[_queue], schedules=schedules)
