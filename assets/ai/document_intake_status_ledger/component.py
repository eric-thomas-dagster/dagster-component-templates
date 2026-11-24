"""DocumentIntakeStatusLedgerComponent -- a durable, queryable per-file STATUS ledger for batch document
intake (hundreds to thousands of files ingested over many sensor ticks / runs): "how many of this batch's
files are done? which ones failed and why? which are stuck mid-processing?"

`sensors/s3_monitor` already detects new S3 objects and can fire a `RunRequest` per file carrying
`{bucket, key, size, etag, last_modified, prefix, region}` in run_config (or register each file as a dynamic
partition) -- but it has no durable per-file STATUS that survives across runs and sensor ticks. This
component is that store, kept deliberately separate (not a change to s3_monitor itself, which is generic
and widely used): a downstream asset's own code calls `record_status(etag, "discovered")` when it receives
a file, `"processing"` when it starts extracting, then `"extracted"` or `"failed"` with a `detail` message
-- see README.md for the full worked example. This component does not orchestrate that wiring itself.

Storage: a local JSONL file, append-only on disk (crash-safe, concurrent-writer-safe -- every write is one
line), "latest write per key wins" on read (duplicate `content_hash` lines collapse to the last one written).
This is a MUTABLE per-file STATUS store, not an event log -- contrast with `write_outcome_ledger_entry` /
`read_outcome_ledger_entries` in `assets/ai/github_pr_tools/component.py`, which is an append-only EVENT log
keyed by run_id that returns every entry; this collapses to one current status per key.

Two pieces:
  1. `DocumentIntakeStatusLedger` -- a `dg.ConfigurableResource` (same convention as `PlaidResource` /
     `AgenticPipelineAuthoringTools` etc. in this repo: the resource class holds the behavior, a Component
     registers an instance of it under `resource_key` so other components' asset/sensor code can depend on
     it via `required_resource_keys`). record_status / get_status / list_by_status / summary.
  2. `DocumentIntakeStatusLedgerComponent` -- registers that resource AND a summary asset
     (`{ledger_name}_intake_summary`) that materializes `summary()` with per-status counts as
     `MetadataValue`, plus an optional asset check flagging files stuck in `processing` too long.
"""
import hashlib
import json
import os
import threading
from datetime import datetime, timezone
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


VALID_STATUSES = ("discovered", "processing", "extracted", "failed", "reviewed")

_LEDGER_LOCK = threading.Lock()


def compute_identity_hash(source: Optional[str], identifier: Optional[str]) -> str:
    """Fallback key used only when the caller has no real content hash to give us (e.g. no S3 etag handy).
    This is an identity hash of `{source}:{identifier}` (typically a path or URI) -- it changes if the path
    changes, NOT if the file's bytes change. Prefer passing a real content hash (an S3 etag, a sha256 of the
    bytes, ...) as `content_hash` whenever one is available."""
    return hashlib.sha256(f"{source}:{identifier}".encode("utf-8")).hexdigest()


def _parse_ts(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        t = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return t if t.tzinfo else t.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def append_ledger_entry(ledger_path: str, entry: Dict[str, Any]) -> None:
    resolved = os.path.expanduser(ledger_path)
    parent = os.path.dirname(os.path.abspath(resolved))
    with _LEDGER_LOCK:
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(resolved, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str) + "\n")


def read_latest_entries(ledger_path: str) -> Dict[str, Dict[str, Any]]:
    """Collapse the append-only JSONL file to one entry per content_hash: whichever line was written last
    for that key wins (a later line in the file always overwrites an earlier one for the same key)."""
    resolved = os.path.expanduser(ledger_path)
    if not os.path.isfile(resolved):
        return {}
    latest: Dict[str, Dict[str, Any]] = {}
    with open(resolved, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            h = rec.get("content_hash")
            if not h:
                continue
            latest[h] = rec  # later line in the file always wins -- no need to compare timestamps
    return latest


class DocumentIntakeStatusLedger(dg.ConfigurableResource):
    """Durable per-file status store for batch document intake, backed by one local JSONL file. Each write
    is appended (crash-safe, concurrent-writer-safe); reads collapse duplicate `content_hash` keys to the
    last-written entry -- this is a mutable per-file STATUS, not an append-only event log.

    Five statuses, exactly: discovered, processing, extracted, failed, reviewed. No transition validation is
    enforced (any valid status can follow any other) -- this is a status store, not a state machine.
    """

    ledger_path: str = Field(description="Local JSONL file the ledger is persisted to. Created (with parent directories) on first write.")
    recent_failed_cap: int = Field(default=20, description="Default cap on the most-recent failed entries returned by summary() when recent_failed_cap isn't passed explicitly.")

    def record_status(
        self,
        content_hash: Optional[str],
        status: str,
        *,
        source: Optional[str] = None,
        identifier: Optional[str] = None,
        detail: Optional[str] = None,
    ) -> str:
        """Write one status entry for a file, keyed by `content_hash` (e.g. an S3 etag -- already a real
        per-object content hash). Pass `content_hash=None` with both `source` and `identifier` set to derive
        a fallback identity hash via `compute_identity_hash` (honestly: an identity hash of the path, not a
        content hash, when no real one is available). Returns the content_hash used."""
        if status not in VALID_STATUSES:
            raise ValueError(f"status must be one of {VALID_STATUSES}, got {status!r}")
        if not content_hash:
            if not source or not identifier:
                raise ValueError(
                    "content_hash is required unless both source and identifier are given, so a fallback "
                    "identity hash can be derived via compute_identity_hash(source, identifier)"
                )
            content_hash = compute_identity_hash(source, identifier)
        entry = {
            "content_hash": content_hash,
            "status": status,
            "source": source,
            "identifier": identifier,
            "detail": detail,
            # microsecond precision (not truncated to seconds) so entries recorded moments apart in a tight
            # loop still sort distinctly by recency -- summary()'s recent_failed ordering depends on it.
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        append_ledger_entry(self.ledger_path, entry)
        return content_hash

    def get_status(self, content_hash: str) -> Optional[Dict[str, Any]]:
        """The latest entry for one key, or None if it has never been recorded."""
        return read_latest_entries(self.ledger_path).get(content_hash)

    def list_by_status(self, status: str, *, stuck_after_minutes: Optional[float] = None) -> List[Dict[str, Any]]:
        """All keys currently (latest entry) in `status`. When `stuck_after_minutes` is given, filters to
        entries whose timestamp is older than that many minutes -- the "stuck" query, e.g.
        `list_by_status("processing", stuck_after_minutes=30)`. An entry with no parseable timestamp is
        treated as not-stuck (excluded) rather than guessed at."""
        if status not in VALID_STATUSES:
            raise ValueError(f"status must be one of {VALID_STATUSES}, got {status!r}")
        now = datetime.now(timezone.utc)
        out = []
        for rec in read_latest_entries(self.ledger_path).values():
            if rec.get("status") != status:
                continue
            if stuck_after_minutes is not None:
                ts = _parse_ts(rec.get("ts"))
                if ts is None or (now - ts).total_seconds() < stuck_after_minutes * 60:
                    continue
            out.append(rec)
        return out

    def summary(self, *, recent_failed_cap: Optional[int] = None) -> Dict[str, Any]:
        """Counts per status plus the most recent `failed` entries (capped) with their `detail`."""
        cap = self.recent_failed_cap if recent_failed_cap is None else recent_failed_cap
        latest = read_latest_entries(self.ledger_path)
        counts = {s: 0 for s in VALID_STATUSES}
        failed = []
        for rec in latest.values():
            status = rec.get("status")
            if status in counts:
                counts[status] += 1
            if status == "failed":
                failed.append(rec)
        failed.sort(key=lambda r: r.get("ts") or "", reverse=True)
        return {"counts": counts, "total": len(latest), "recent_failed": failed[:cap]}


class DocumentIntakeStatusLedgerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers a `DocumentIntakeStatusLedger` resource plus a summary asset that materializes its
    `summary()` output, with per-status counts surfaced as `MetadataValue` on the materialization.

    Example:
        ```yaml
        type: dagster_community_components.DocumentIntakeStatusLedgerComponent
        attributes:
          ledger_name: invoice_batch_intake
          ledger_path: ./state/invoice_batch_intake.jsonl
          stuck_after_minutes: 30
        ```

    Wiring (done in your OWN asset code, not by this component -- see README.md): a downstream asset
    receiving a `RunRequest` from `sensors/s3_monitor` (which already carries `etag` in run_config) calls
    `record_status(etag, "discovered")` on receipt, `"processing"` when it starts extracting, then
    `"extracted"` or `"failed"` with a `detail` message.
    """

    ledger_name: str = Field(
        default="document_intake_status_ledger",
        description="Used to derive the default ledger_path ('./{ledger_name}.jsonl') and the default summary asset name ('{ledger_name}_intake_summary').",
    )
    ledger_path: Optional[str] = Field(
        default=None,
        description="Path to the local JSONL ledger file. Default: './{ledger_name}.jsonl'. Created (with parent directories) on first write.",
    )
    resource_key: str = Field(default="document_intake_status_ledger", description="Dagster resource key the ledger is registered under -- reference it from your own asset/sensor code via required_resource_keys.")
    summary_asset_name: Optional[str] = Field(default=None, description="Dagster asset name for the summary asset. Default: '{ledger_name}_intake_summary'.")
    recent_failed_cap: int = Field(default=20, description="Max number of most-recent failed entries surfaced by the summary asset's metadata.")
    stuck_after_minutes: Optional[float] = Field(
        default=None,
        description="When set, the summary asset also emits an asset check that fails when any file has been in 'processing' longer than this many minutes. Unset (default): no check.",
    )
    stuck_severity: str = Field(default="WARN", description="Asset check severity when files are stuck: 'WARN' or 'ERROR'. Only used when stuck_after_minutes is set.")
    cron_schedule: Optional[str] = Field(default=None, description="Optional cron expression; when set, a schedule re-materializes the summary asset on this cadence (e.g. '*/5 * * * *').")
    group_name: Optional[str] = Field(default="ai", description="Asset group")
    description: Optional[str] = Field(default=None, description="Asset description")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners (team:name or emails)")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog (default: ['ledger', 'status'])")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Tags added to the asset")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "kinds"), csv=(), tags=("asset_tags",))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Document Intake Status Ledger", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.stuck_after_minutes is not None and self.stuck_severity.upper() not in ("WARN", "ERROR"):
            raise ValueError("stuck_severity must be 'WARN' or 'ERROR'")

        ledger_path = self.ledger_path or f"./{self.ledger_name}.jsonl"
        resource_key = self.resource_key
        recent_failed_cap = self.recent_failed_cap
        stuck_after_minutes = self.stuck_after_minutes
        stuck_severity = dg.AssetCheckSeverity.ERROR if self.stuck_severity.upper() == "ERROR" else dg.AssetCheckSeverity.WARN

        asset_key = dg.AssetKey.from_user_string(self.summary_asset_name or f"{self.ledger_name}_intake_summary")
        tags = {**(self.asset_tags or {}), **{f"dagster/kind/{k}": "" for k in (self.kinds or ["ledger", "status"])}}
        check_name = "no_stuck_processing"

        @dg.asset(
            key=asset_key,
            group_name=self.group_name,
            description=self.description or "Per-status counts for a batch document intake ledger (discovered/processing/extracted/failed/reviewed).",
            owners=self.owners or None,
            tags=tags,
            required_resource_keys={resource_key},
            check_specs=(
                [dg.AssetCheckSpec(name=check_name, asset=asset_key, description="No file has been stuck in 'processing' too long.")]
                if stuck_after_minutes is not None else None
            ),
        )
        def _intake_summary(context: dg.AssetExecutionContext):
            ledger: DocumentIntakeStatusLedger = getattr(context.resources, resource_key)
            data = ledger.summary(recent_failed_cap=recent_failed_cap)
            counts = data["counts"]
            md: Dict[str, Any] = {f"count_{status}": dg.MetadataValue.int(counts.get(status, 0)) for status in VALID_STATUSES}
            md["total_count"] = dg.MetadataValue.int(data["total"])
            md["recent_failed"] = dg.MetadataValue.json(data["recent_failed"])
            yield dg.Output(data, metadata=md)

            if stuck_after_minutes is not None:
                stuck = ledger.list_by_status("processing", stuck_after_minutes=stuck_after_minutes)
                yield dg.AssetCheckResult(
                    check_name=check_name,
                    passed=len(stuck) == 0,
                    severity=stuck_severity,
                    metadata={
                        "stuck_count": dg.MetadataValue.int(len(stuck)),
                        "stuck_content_hashes": dg.MetadataValue.json([r.get("content_hash") for r in stuck]),
                        "stuck_after_minutes": stuck_after_minutes,
                    },
                    description=(
                        f"{len(stuck)} file(s) stuck in 'processing' for over {stuck_after_minutes} minute(s)"
                        if stuck else "no files stuck in processing"
                    ),
                )

        schedules = []
        if self.cron_schedule:
            schedules.append(dg.ScheduleDefinition(
                name=f"{asset_key.to_user_string()}_refresh", cron_schedule=self.cron_schedule,
                target=dg.AssetSelection.assets(asset_key) | dg.AssetSelection.checks_for_assets(asset_key),
            ))

        return dg.Definitions(
            resources={resource_key: DocumentIntakeStatusLedger(ledger_path=ledger_path, recent_failed_cap=recent_failed_cap)},
            assets=[_intake_summary],
            schedules=schedules,
        )
