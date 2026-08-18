"""HumanApprovalGateComponent — the human-in-the-loop primitive.

The gate is an asset. When materialized it reads an approval token file at
`{approval_dir}/{partition_key or default_approval_key}.json`. Three outcomes:

  - **Token missing** → `dg.Failure` with reason=`approval_pending`. Downstream
    doesn't run. This is the "waiting on a human" state. Re-materializing
    later (via a sensor, cron, or manual click) picks up the token when it
    lands.
  - **Token present + `approved: true`** → the upstream payload is passed
    through (unchanged), and `approved_by` / `approval_reason` / `approved_at`
    land in materialization metadata. Downstream is unblocked.
  - **Token present + `approved: false`** → `dg.Failure` with
    reason=`approval_rejected`. Downstream doesn't run; the rejection is
    permanent state until someone edits the token or writes a new one.

Token format (JSON):

    {
      "approved": true|false,
      "approver": "eric@dagsterlabs.com",
      "reason": "reviewed, looks good",
      "timestamp": "2026-07-30T12:34:56Z"    (optional)
    }

Who writes the token is out of scope — anything works: a bash `echo > .json`,
a Slack bot, a Retool form, ServiceNow webhook, GitHub Action, another
Dagster asset. That's the point — the gate is stateless, the token file IS
the state.

Pair with `FilesystemMonitorSensorComponent` pointed at the approval
directory to auto-progress the graph the moment a token appears (no manual
re-materialize).
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


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
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
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
        raise ValueError(f"partition_type={partition_type!r} requires partition_start (ISO date).")
    if partition_type == "daily": return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly": return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly": return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values: raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values or not partition_start:
            raise ValueError("partition_type='multi' requires partition_values + partition_start.")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class HumanApprovalGateComponent(dg.Component, dg.Model, dg.Resolvable):
    """Gate an asset on a human-writable approval token file.

    Materialization reads `{approval_dir}/{partition_key or
    default_approval_key}.json`. Missing token = `Failure(approval_pending)`,
    `approved: false` = `Failure(approval_rejected)`, `approved: true` =
    passthrough of upstream payload + approver metadata.
    """

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: str = Field(
        description=(
            "Upstream asset whose value passes through the gate when approved. "
            "String (not an AssetKey object). For single-part asset keys use the "
            "bare name (e.g. `triage_report`). For multi-part keys use slash "
            "notation (e.g. `analytics/orders/daily_totals`) — this maps to "
            "`AssetKey.from_user_string()` at wiring time."
        )
    )
    approval_dir: str = Field(
        description=(
            "Absolute path to the directory of approval token JSON files. "
            "Filename is `<partition_key>.json` (or `<default_approval_key>.json` "
            "when unpartitioned)."
        )
    )
    default_approval_key: str = Field(
        default="default",
        description="Token filename stem used when the asset is unpartitioned.",
    )

    # Catalog / governance
    group_name: Optional[str] = Field(default=None, description="Asset group")
    description: Optional[str] = Field(default=None, description="Asset description")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Additional key-value tags")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds (defaults to ['human', 'approval']).",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None, description="Freshness policy: max lag before stale."
    )
    freshness_cron: Optional[str] = Field(
        default=None, description="Cron schedule string for the freshness policy."
    )

    # Partitioning
    partition_type: Optional[str] = Field(
        default=None,
        description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic' | 'multi' | None",
    )
    partition_start: Optional[str] = Field(default=None, description="ISO date for time-based partition types.")
    partition_values: Optional[str] = Field(
        default=None, description="Comma-separated values for static/multi partitioning."
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None, description="Name for DynamicPartitionsDefinition when partition_type='dynamic'."
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name}.",
    )

    # Retry policy
    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description=(
            "Max retries on failure. Rarely useful on this component — a pending "
            "approval isn't a retry-able failure. Leave unset unless you want the "
            "gate to poll a few times before giving up in a specific run."
        ),
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None, description="Seconds between retries (default 1)."
    )
    retry_policy_backoff: str = Field(
        default="exponential", description="Backoff strategy: 'linear' or 'exponential'."
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data loaded at runtime).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        approval_dir = self.approval_dir
        default_approval_key = self.default_approval_key

        kinds = self.kinds or ["human", "approval"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )

        freshness = None
        if self.freshness_max_lag_minutes is not None:
            freshness = dg.FreshnessPolicy(
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

        # An asset check named `approved` fails (severity=ERROR) when the token
        # is missing (pending) or explicitly rejected — that's what BLOCKS
        # downstream from materializing via automation conditions. The asset
        # itself still MATERIALIZES on every run — so in the UI the gate shows
        # green ("was evaluated") with a failing check badge (informative,
        # not alarming). Downstream sees the failed check and doesn't fire.
        _check_spec = dg.AssetCheckSpec(
            name="approved",
            asset=dg.AssetKey.from_user_string(asset_name),
            description=(
                "Fails when the approval token is missing (pending) or "
                "approved=false (rejected). Blocks downstream via "
                "AutomationCondition.eager()."
            ),
        )

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Approval gate for {upstream_asset_key}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tags,
            partitions_def=partitions_def,
            freshness_policy=freshness,
            retry_policy=retry_policy,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))},
            check_specs=[_check_spec],
        )
        def _gate(context: dg.AssetExecutionContext, upstream):
            import pandas as pd
            raw_key = context.partition_key if context.has_partition_key else default_approval_key
            # Sanitize `/` `\` so composite partition keys like
            # `dagster-io/dagster#30000` land on a single-segment filename
            # rather than a nested path (which would silently write to a
            # subdirectory the FilesystemMonitorSensor isn't watching).
            safe_key = raw_key.replace("/", "_").replace("\\", "_")
            token_file = Path(approval_dir).expanduser().resolve() / f"{safe_key}.json"

            if safe_key != raw_key:
                context.log.info(
                    f"Approval token: {token_file} (partition_key={raw_key!r} sanitized to {safe_key!r})"
                )
            else:
                context.log.info(f"Checking approval token: {token_file}")
            key = safe_key

            # Common failure payload — the gate always MATERIALIZES so the
            # asset stays green in the UI; the check_result signals the state.
            def _empty_passthrough_df():
                if isinstance(upstream, pd.DataFrame):
                    return upstream.iloc[0:0].copy()  # empty frame with same schema
                return pd.DataFrame()

            if not token_file.exists():
                context.log.info(f"approval_pending — no token at {token_file}")
                yield dg.Output(
                    _empty_passthrough_df(),
                    metadata={
                        "status": "approval_pending",
                        "token_file": str(token_file),
                        "partition_key": key,
                        "hint": dg.MetadataValue.md(
                            f"Drop a JSON at `{token_file}` with "
                            f"`{{'approved': true, 'approver': 'you@co', 'reason': '...'}}` "
                            f"then re-materialize this partition (or let the "
                            f"approval-token sensor pick it up)."
                        ),
                    },
                )
                yield dg.AssetCheckResult(
                    check_name="approved",
                    passed=False,
                    severity=dg.AssetCheckSeverity.WARN,
                    description=f"approval_pending — no token at {token_file}",
                    metadata={"status": "approval_pending", "partition_key": key},
                )
                return

            try:
                token = json.loads(token_file.read_text())
            except json.JSONDecodeError as e:
                context.log.error(f"approval_token_malformed — {token_file}: {e}")
                yield dg.Output(
                    _empty_passthrough_df(),
                    metadata={
                        "status": "approval_token_malformed",
                        "token_file": str(token_file),
                        "parse_error": str(e),
                    },
                )
                yield dg.AssetCheckResult(
                    check_name="approved",
                    passed=False,
                    severity=dg.AssetCheckSeverity.ERROR,
                    description=f"approval_token_malformed — not valid JSON: {e}",
                    metadata={"status": "approval_token_malformed"},
                )
                return

            approver = str(token.get("approver") or "unknown")
            reason = str(token.get("reason") or "")
            approved_at = str(token.get("timestamp") or datetime.now(timezone.utc).isoformat())

            if not token.get("approved"):
                context.log.info(f"approval_rejected by {approver}: {reason}")
                yield dg.Output(
                    _empty_passthrough_df(),
                    metadata={
                        "status": "approval_rejected",
                        "approver": approver,
                        "reason": reason,
                        "rejected_at": approved_at,
                        "token_file": str(token_file),
                    },
                )
                yield dg.AssetCheckResult(
                    check_name="approved",
                    passed=False,
                    severity=dg.AssetCheckSeverity.ERROR,
                    description=f"approval_rejected by {approver}: {reason}",
                    metadata={
                        "status": "approval_rejected",
                        "approver": approver,
                        "reason": reason,
                    },
                )
                return

            # Approved — passthrough of upstream.
            context.log.info(f"Approved by {approver}: {reason}")
            yield dg.Output(
                upstream,
                metadata={
                    "status": "approved",
                    "approver": approver,
                    "approval_reason": reason,
                    "approved_at": approved_at,
                    "token_file": str(token_file),
                    "partition_key": key,
                },
            )
            yield dg.AssetCheckResult(
                check_name="approved",
                passed=True,
                severity=dg.AssetCheckSeverity.WARN,  # only used on failures
                description=f"approved by {approver}",
                metadata={"approver": approver, "approval_reason": reason},
            )

        return dg.Definitions(assets=[_gate])
