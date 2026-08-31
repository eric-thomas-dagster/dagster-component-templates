"""MultiApproverGateComponent.

Quorum-based approval gate. Extends the base "human approval" pattern
with N-of-M semantics: `required_approvers` votes from an
`approver_allowlist`, optional `timeout_hours` with `on_timeout` policy
(escalate / reject / approve). Works over local filesystem OR any
fsspec-supported cloud storage (`s3://`, `gs://`, `abfs://`) — the same
`approval_dir` abstraction the other approval gates use.

## Storage backend

`approval_dir` accepts:
  - Plain paths (`/tmp/approvals`) — local filesystem via pathlib.
  - Cloud URIs (`s3://bucket/approvals`, `gs://...`, `abfs://...`) —
    routed through fsspec. Install the driver: `s3fs`, `gcsfs`, or
    `adlfs`. On Dagster+ Serverless this is the recommended shape since
    the container filesystem is ephemeral.

## Vote-by-file-drop protocol

Each approver writes a single JSON file per partition:

    <approval_dir>/<safe_partition_key>.<approver_id>.json

with body:

    {"approved": true, "reason": "LGTM"}

At materialization time, the gate scans `<safe_partition_key>.*.json`
files, filters to `approver_allowlist`, and counts approvals + rejections
independently. Quorum semantics:

  - `len(approvers) >= required_approvers` → gate opens (approved).
  - `len(rejecters) >= required_approvers` → gate closes (rejected).
  - Otherwise → pending (asset_check fails soft, downstream blocks).

## Timeout policy

If `timeout_hours` is set and no quorum reached before then:

  - `on_timeout: escalate` — asset check emits an ERROR-severity
    result naming the escalation contact. Gate stays pending; a human
    still needs to write a vote file.
  - `on_timeout: reject` — treat as automatic rejection.
  - `on_timeout: approve` — treat as automatic approval (use with
    caution; documented as such).

Timeout clock starts at the first materialization attempt (stored in a
sidecar `<safe_key>.multi_state.json` written on first run).

## Composes with

- `slack_approval_gate` / `teams_approval_gate` — those write single-file
  quorum tokens (`<key>.json`). This component writes per-approver files
  (`<key>.<id>.json`). Different token shapes — use ONE or the OTHER for
  a given partition, not both.
- `filesystem_monitor` sensor — can trigger this gate's downstream on
  new vote-file drops. But since the gate re-evaluates on every
  materialization, a sensor isn't strictly required — just use
  `AutomationCondition.eager()` on the gate itself.
"""

import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


# ── Inline fs helper: local + cloud (s3://, gs://, abfs://) via fsspec ──
# Kept inline per DCC's self-contained convention. Backward-compat:
# plain paths (e.g. `/tmp/approvals`) use pathlib directly; only URIs
# require fsspec + the appropriate driver.
class _ApprovalFS:
    """Uniform read/write over `approval_dir`. Plain paths use pathlib
    (no fsspec dep); URIs (`s3://`, `gs://`, `abfs://`) route through
    fsspec + the appropriate driver (`s3fs` / `gcsfs` / `adlfs` —
    install what you need)."""
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

    def mkdir(self, subdir: str = "") -> None:
        target = self.path(subdir) if subdir else self.root
        if self.is_uri:
            try: self.fs.makedirs(target, exist_ok=True)
            except Exception: pass
        else:
            Path(target).mkdir(parents=True, exist_ok=True)

    def read_text(self, p: str) -> str:
        if self.is_uri:
            with self.fs.open(p, "r") as f: return f.read()
        return Path(p).read_text()

    def write_json(self, p: str, obj: Any) -> None:
        body = json.dumps(obj, indent=2, default=str)
        if self.is_uri:
            try: self.fs.makedirs("/".join(p.split("/")[:-1]), exist_ok=True)
            except Exception: pass
            with self.fs.open(p, "w") as f: f.write(body)
        else:
            Path(p).parent.mkdir(parents=True, exist_ok=True)
            Path(p).write_text(body)

    def glob(self, pattern: str) -> List[str]:
        if self.is_uri:
            proto = self.fs.protocol if isinstance(self.fs.protocol, str) else self.fs.protocol[0]
            return [f"{proto}://{m}" for m in self.fs.glob(self.path(pattern))]
        return [str(p) for p in Path(self.root).glob(pattern)]


# ── Partition helper (matches HumanApprovalGate/SlackApprovalGate shape) ──

def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
):
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily": return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly": return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly": return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("static partition requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("dynamic partition requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        return MultiPartitionsDefinition({d["name"]: _build_axis(d) for d in partition_dimensions})

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily": return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly": return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly": return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values: raise ValueError("static requires values")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("dynamic requires dynamic_partition_name")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _safe_partition_key(raw: str) -> str:
    return raw.replace("/", "_").replace("\\", "_")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


_STATE_SUFFIX = ".multi_state.json"


class MultiApproverGateComponent(dg.Component, dg.Model, dg.Resolvable):
    """Quorum-based human approval gate.

    Vote-by-file-drop: each approver writes `<approval_dir>/<key>.<id>.json`
    with `{approved: bool, reason: str}`. Gate counts allowlisted votes;
    opens when `required_approvers` votes agree (approve OR reject).

    Emits ONE asset (`asset_name`) with an asset check (`quorum`). Check
    fails while pending; downstream blocks via `AutomationCondition.eager()`.
    Works over local FS or any fsspec URI (s3://, gs://, abfs://) — on
    Dagster+ Serverless, point at cloud storage since the container FS
    is ephemeral.
    """

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(
        description=(
            "Upstream asset whose value passes through the gate when quorum "
            "approves. String (not AssetKey); slash notation for multi-part keys."
        )
    )
    approval_dir: str = Field(
        description=(
            "Where votes live. Plain path (`/tmp/approvals`) or fsspec URI "
            "(`s3://bucket/approvals`, `gs://...`, `abfs://...`). On Dagster+ "
            "Serverless, use cloud URIs — container FS is ephemeral."
        )
    )

    required_approvers: int = Field(
        default=1,
        description=(
            "How many allowlisted approvers must vote the same way for the "
            "gate to resolve. `len(approvers) >= required` → approved; "
            "`len(rejecters) >= required` → rejected; otherwise pending."
        ),
    )
    approver_allowlist: List[str] = Field(
        description=(
            "Approver IDs (email, username, whatever your org uses) allowed "
            "to vote. Files from other IDs are ignored. Vote filename is "
            "`<safe_partition_key>.<approver_id>.json`."
        ),
    )
    timeout_hours: Optional[float] = Field(
        default=None,
        description=(
            "Wall-clock hours before timeout policy kicks in. Clock starts on "
            "the FIRST materialization attempt for a partition (stored in a "
            "`.multi_state.json` sidecar). Optional — omit to wait forever."
        ),
    )
    on_timeout: str = Field(
        default="escalate",
        description=(
            "'escalate' (asset check emits ERROR naming escalation contact; "
            "gate stays pending until a vote lands) | 'reject' (treat as "
            "automatic rejection) | 'approve' (auto-approve — use with care)."
        ),
    )
    escalate_contact: Optional[str] = Field(
        default=None,
        description="Contact shown in the escalation-timeout asset check message. Free-form (email, Slack handle, PagerDuty rotation ID).",
    )

    default_approval_key: str = Field(
        default="default",
        description="Filename stem used when the asset is unpartitioned.",
    )

    # Catalog / governance
    group_name: Optional[str] = Field(default=None, description="Asset group")
    description: Optional[str] = Field(default=None, description="Asset description")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Extra tags")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (defaults to ['human', 'approval', 'quorum'])."
    )

    # Partitioning
    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Any] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Multi-Approver Gate", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        approval_dir = self.approval_dir
        required = self.required_approvers
        allowlist = list(self.approver_allowlist)
        if required > len(allowlist):
            raise ValueError(
                f"MultiApproverGateComponent: required_approvers={required} > "
                f"len(approver_allowlist)={len(allowlist)}. Impossible quorum."
            )
        if self.on_timeout not in ("escalate", "reject", "approve"):
            raise ValueError(
                f"on_timeout must be escalate|reject|approve; got {self.on_timeout!r}"
            )
        timeout_hours = self.timeout_hours
        on_timeout = self.on_timeout
        escalate_contact = self.escalate_contact
        default_approval_key = self.default_approval_key

        kinds = self.kinds or ["human", "approval", "quorum"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )

        _check_spec = dg.AssetCheckSpec(
            name="quorum",
            asset=dg.AssetKey.from_user_string(asset_name),
            description=(
                "Fails while quorum isn't reached (pending) or if quorum "
                "rejects. Blocks downstream via AutomationCondition.eager()."
            ),
        )

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Quorum approval gate for {upstream_asset_key}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tags,
            partitions_def=partitions_def,
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))},
            check_specs=[_check_spec],
        )
        def _gate(context: dg.AssetExecutionContext, upstream):
            import pandas as pd
            raw_key = context.partition_key if context.has_partition_key else default_approval_key
            safe_key = _safe_partition_key(raw_key)
            fs = _ApprovalFS(approval_dir)
            fs.mkdir()

            def _empty_passthrough_df():
                if isinstance(upstream, pd.DataFrame):
                    return upstream.iloc[0:0].copy()
                return pd.DataFrame()

            # Sidecar tracks first-materialization time for timeout clock.
            state_path = fs.path(f"{safe_key}{_STATE_SUFFIX}")
            if fs.exists(state_path):
                try:
                    state = json.loads(fs.read_text(state_path))
                except Exception:  # noqa: BLE001
                    state = {"first_seen_at": _now_iso()}
                    fs.write_json(state_path, state)
            else:
                state = {"first_seen_at": _now_iso()}
                fs.write_json(state_path, state)
            first_seen = datetime.fromisoformat(state["first_seen_at"])

            # Scan all vote files matching this partition.
            vote_pattern = f"{safe_key}.*.json"
            all_vote_paths = [
                p for p in fs.glob(vote_pattern)
                # Exclude the sidecar itself
                if not p.endswith(_STATE_SUFFIX)
                # Exclude quorum-tokens written by slack/teams gates
                and not p.endswith(f"{safe_key}.json")
            ]
            approvers: List[Dict[str, Any]] = []
            rejecters: List[Dict[str, Any]] = []
            malformed: List[str] = []

            for vote_path in all_vote_paths:
                # Filename: <safe_key>.<approver_id>.json — extract approver id
                base = vote_path.rsplit("/", 1)[-1] if fs.is_uri else Path(vote_path).name
                # Strip <safe_key>. prefix + .json suffix
                stem = base[len(safe_key) + 1:-len(".json")] if base.endswith(".json") else base
                # For safety: strip in case suffix parsing missed
                if not stem or stem == "":
                    continue
                approver_id = stem
                if approver_id not in allowlist:
                    context.log.info(
                        f"vote from {approver_id!r} ignored (not in allowlist)"
                    )
                    continue
                try:
                    body = json.loads(fs.read_text(vote_path))
                except Exception as e:  # noqa: BLE001
                    malformed.append(f"{vote_path}: {e}")
                    continue
                vote_entry = {
                    "approver": approver_id,
                    "reason": str(body.get("reason") or ""),
                    "timestamp": str(body.get("timestamp") or ""),
                }
                if body.get("approved"):
                    approvers.append(vote_entry)
                else:
                    rejecters.append(vote_entry)

            n_approve = len(approvers)
            n_reject = len(rejecters)
            context.log.info(
                f"quorum status: approvers={n_approve}/{required}, "
                f"rejecters={n_reject}/{required}, allowlist_size={len(allowlist)}, "
                f"malformed={len(malformed)}"
            )

            common_meta = {
                "required_approvers": required,
                "n_approvals": n_approve,
                "n_rejections": n_reject,
                "approvers": dg.MetadataValue.json([a["approver"] for a in approvers]),
                "rejecters": dg.MetadataValue.json([r["approver"] for r in rejecters]),
                "allowlist_size": len(allowlist),
                "partition_key": raw_key,
                "vote_dir": fs.root,
            }
            if malformed:
                common_meta["malformed_votes"] = dg.MetadataValue.json(malformed)

            # Quorum decisions.
            if n_approve >= required:
                context.log.info(
                    f"quorum APPROVED by {[a['approver'] for a in approvers]}"
                )
                yield dg.Output(
                    upstream,
                    metadata={
                        "status": "approved",
                        **common_meta,
                        "approval_details": dg.MetadataValue.json(approvers),
                    },
                )
                yield dg.AssetCheckResult(
                    check_name="quorum",
                    passed=True,
                    severity=dg.AssetCheckSeverity.WARN,
                    description=f"quorum ({n_approve}/{required}) approved",
                    metadata={"approvers": dg.MetadataValue.json([a["approver"] for a in approvers])},
                )
                return

            if n_reject >= required:
                context.log.info(
                    f"quorum REJECTED by {[r['approver'] for r in rejecters]}"
                )
                yield dg.Output(
                    _empty_passthrough_df(),
                    metadata={
                        "status": "rejected",
                        **common_meta,
                        "rejection_details": dg.MetadataValue.json(rejecters),
                    },
                )
                yield dg.AssetCheckResult(
                    check_name="quorum",
                    passed=False,
                    severity=dg.AssetCheckSeverity.ERROR,
                    description=f"quorum ({n_reject}/{required}) rejected",
                    metadata={"rejecters": dg.MetadataValue.json([r["approver"] for r in rejecters])},
                )
                return

            # Not yet at quorum — check timeout.
            timed_out = False
            if timeout_hours is not None:
                age_h = (datetime.now(timezone.utc) - first_seen).total_seconds() / 3600.0
                if age_h > timeout_hours:
                    timed_out = True

            if timed_out:
                if on_timeout == "reject":
                    context.log.info(
                        f"timeout after {timeout_hours}h → auto-REJECT (policy)"
                    )
                    yield dg.Output(
                        _empty_passthrough_df(),
                        metadata={
                            "status": "timeout_rejected",
                            **common_meta,
                            "timeout_hours": timeout_hours,
                            "first_seen_at": state["first_seen_at"],
                        },
                    )
                    yield dg.AssetCheckResult(
                        check_name="quorum",
                        passed=False,
                        severity=dg.AssetCheckSeverity.ERROR,
                        description=f"timeout after {timeout_hours}h — auto-rejected per on_timeout policy",
                    )
                    return
                if on_timeout == "approve":
                    context.log.warning(
                        f"timeout after {timeout_hours}h → auto-APPROVE (policy)"
                    )
                    yield dg.Output(
                        upstream,
                        metadata={
                            "status": "timeout_approved",
                            **common_meta,
                            "timeout_hours": timeout_hours,
                            "first_seen_at": state["first_seen_at"],
                        },
                    )
                    yield dg.AssetCheckResult(
                        check_name="quorum",
                        passed=True,
                        severity=dg.AssetCheckSeverity.WARN,
                        description=f"timeout after {timeout_hours}h — auto-approved per on_timeout policy",
                    )
                    return
                # escalate — emit ERROR check naming the contact, stay pending
                escalate_note = (
                    f"timeout after {timeout_hours}h; escalating to "
                    f"{escalate_contact or '(no escalate_contact set)'}"
                )
                context.log.error(escalate_note)
                yield dg.Output(
                    _empty_passthrough_df(),
                    metadata={
                        "status": "timeout_escalated",
                        **common_meta,
                        "escalate_contact": escalate_contact or "",
                        "timeout_hours": timeout_hours,
                        "first_seen_at": state["first_seen_at"],
                        "hint": dg.MetadataValue.md(
                            f"Quorum still pending after {timeout_hours}h. "
                            f"Escalation contact: **{escalate_contact or '(unset)'}**. "
                            f"Approvers drop votes at "
                            f"`{fs.path(f'{safe_key}.<approver_id>.json')}`."
                        ),
                    },
                )
                yield dg.AssetCheckResult(
                    check_name="quorum",
                    passed=False,
                    severity=dg.AssetCheckSeverity.ERROR,
                    description=escalate_note,
                    metadata={"escalate_contact": escalate_contact or ""},
                )
                return

            # Pending, no timeout yet.
            context.log.info("quorum pending")
            yield dg.Output(
                _empty_passthrough_df(),
                metadata={
                    "status": "pending",
                    **common_meta,
                    "first_seen_at": state["first_seen_at"],
                    "hint": dg.MetadataValue.md(
                        f"Waiting for quorum ({required} of {len(allowlist)}). "
                        f"Approvers drop votes at "
                        f"`{fs.path(f'{safe_key}.<approver_id>.json')}` with "
                        f"`{{'approved': true/false, 'reason': '...'}}`."
                    ),
                },
            )
            yield dg.AssetCheckResult(
                check_name="quorum",
                passed=False,
                severity=dg.AssetCheckSeverity.WARN,
                description=f"quorum pending ({n_approve}/{required} approvals)",
                metadata={"n_approvals": n_approve, "required": required},
            )

        return dg.Definitions(assets=[_gate])
