"""SlackMessageSendComponent — post business-data-driven alerts to Slack.

Renders a templated message using upstream-row fields and posts it to a
Slack channel (or user, for a DM) — one message per row, or one digest
message summarizing the whole DataFrame.

Common use:
  - "Customer health score crossed threshold" -> ping #customer-success
  - "New high-value lead created" -> ping a sales channel with details
  - Daily digest: aggregated anomalies -> one summary message per run

Auth: reuse an existing `slack_resource` (SlackResourceComponent) via
`resource_key`, or authenticate directly with `token_env_var` (a Slack
bot token, xoxb-...). The bot needs the `chat:write` scope and must be
invited to the target channel (or share the IM with the target user).

!! Scope: business-data-driven alerts only, NOT run-status notifications !!
This component posts messages driven by WAREHOUSE DATA CONDITIONS
evaluated on an upstream asset's rows (e.g. "which customers just
crossed a health-score threshold") — it has no awareness of Dagster run
status, asset materialization success/failure, or freshness violations.

It is explicitly NOT a reimplementation of Dagster's own pipeline-health
notifications. For "this job failed" / "this asset failed to
materialize" / "this asset is stale" alerts, do NOT use this component —
instead pair the `slack_resource` (SlackResourceComponent) with a
Dagster run-status sensor or hook (`run_failure_sensor`,
`make_slack_on_run_failure_sensor`, etc.), or — if you're on Dagster+ —
use its built-in alert policies, which already cover run/check/freshness
events with zero code. Those two notification paths (run-health vs.
data-driven) are deliberately kept separate so teams don't confuse
"the pipeline broke" messages with "the data says something" messages.
"""
import re
from typing import Any, Dict, List, Optional

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    EnvVar,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


def _render(template: str, row: Dict[str, Any]) -> str:
    """Render `{column}` placeholders against a dict. Missing keys and
    pandas NaN (a float column of all-None values decays to NaN, not
    None) both yield empty string -- otherwise a NaN channel would render
    as the literal text "nan" and slip past a blank-channel check."""
    def sub(m):
        key = m.group(1)
        v = row.get(key)
        if v is None:
            return ""
        if isinstance(v, float) and pd.isna(v):
            return ""
        return str(v)
    return re.sub(r"\{(\w+)\}", sub, template)


def _post_slack_message(
    client: Any, channel: str, text: str, thread_ts: Optional[str] = None
) -> Dict[str, Any]:
    """Isolates the one external API call (Slack Web API `chat.postMessage`)
    so tests can monkeypatch this wholesale instead of hitting the real
    network."""
    kwargs: Dict[str, Any] = {"channel": channel, "text": text}
    if thread_ts:
        kwargs["thread_ts"] = thread_ts
    return client.chat_postMessage(**kwargs)


class SlackMessageSendComponent(Component, Model, Resolvable):
    """Post templated Slack messages, one per upstream DataFrame row (or one digest)."""

    asset_name: str = Field(description="Output asset name (summary row).")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Resource key registered by a SlackResourceComponent. Takes priority "
            "over token_env_var when both are set."
        ),
    )
    token_env_var: Optional[str] = Field(
        default=None,
        description="Env var with a Slack bot token (xoxb-...). Used when resource_key is not set.",
    )

    mode: str = Field(
        default="per_row",
        description=(
            "'per_row': one Slack message per upstream row, using row fields in templates. "
            "'summary': one message per run, with all rows rendered in the body via summary_template."
        ),
    )

    channel_template: str = Field(
        description=(
            "Target channel/user template — `{column}` placeholders against upstream row. "
            "Example: '#customer-success' (literal) or '{slack_channel}' (per-row routing). "
            "Accepts a channel ID, a '#name', or a user ID for a DM."
        ),
    )
    message_template: str = Field(
        default="",
        description="Message text, with `{column}` placeholders. Supports Slack mrkdwn formatting.",
    )
    summary_template: Optional[str] = Field(
        default=None,
        description="For mode='summary', a template rendered against the whole DataFrame's row "
                    "summary (use `{row_count}` / `{table_md}`, a markdown table of up to 50 rows).",
    )
    thread_ts_column: Optional[str] = Field(
        default=None,
        description="Optional upstream column holding a thread_ts to reply in an existing thread, per row.",
    )

    dry_run: bool = Field(
        default=False,
        description="If True, render and log every message but don't post to Slack. Use for staging.",
    )
    max_send: Optional[int] = Field(
        default=None,
        description="Hard cap on messages sent per run. None = no limit.",
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        if not self.resource_key and not self.token_env_var:
            raise ValueError(
                "SlackMessageSendComponent: supply resource_key (a registered "
                "SlackResourceComponent) OR token_env_var."
            )

        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _values:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_values)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
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

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        resource_key = self.resource_key
        token_env = self.token_env_var
        mode = self.mode
        chan_t = self.channel_template
        msg_t = self.message_template
        summ_t = self.summary_template
        thread_col = self.thread_ts_column
        dry = self.dry_run
        cap = self.max_send

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Slack message send (mode={mode}).",
            group_name=self.group_name,
            kinds={"slack"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            required_resource_keys={resource_key} if resource_key else set(),
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: Any):
            # Defensive Output/MaterializeResult unwrap — see smtp_send_asset
            # for the rationale. Tolerates upstream authors who annotate
            # `-> Output` or return `Output(value=df, ...)` / `MaterializeResult(value=df)`.
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            # partition bridge dict-concat: when an unpartitioned asset
            # consumes a partitioned upstream, Dagster's IO manager loads
            # ALL partitions as a dict; concat to a single DataFrame first.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()

            if resource_key:
                _res = getattr(context.resources, resource_key)
                client = _res.get_client()
            else:
                from slack_sdk.web.client import WebClient
                token = EnvVar(token_env).get_value()
                if not token:
                    raise ValueError(f"Slack token missing — set {token_env!r}.")
                client = WebClient(token=token)

            messages_built: List[Any] = []
            if mode == "per_row":
                for _, src in upstream.iterrows():
                    row = src.to_dict()
                    channel = _render(chan_t, row)
                    text = _render(msg_t, row) if msg_t else ""
                    thread_ts = None
                    if thread_col and row.get(thread_col) not in (None, ""):
                        thread_ts = str(row.get(thread_col))
                    messages_built.append((channel, text, thread_ts))
                    if cap is not None and len(messages_built) >= cap:
                        break
            elif mode == "summary":
                row = {"row_count": len(upstream)}
                if summ_t:
                    table_md = upstream.head(50).to_markdown(index=False) if not upstream.empty else "(empty)"
                    row["table_md"] = table_md
                channel = _render(chan_t, row)
                text = _render(msg_t, row) if msg_t else (summ_t or "")
                text = _render(text, row)
                messages_built.append((channel, text, None))
            else:
                raise ValueError(f"unknown mode={mode!r} (must be 'per_row' or 'summary')")

            sent, failed = 0, 0
            errors: List[str] = []
            if dry:
                for channel, text, thread_ts in messages_built:
                    context.log.info(
                        f"[DRY-RUN] channel={channel!r} text={text!r} thread_ts={thread_ts}"
                    )
                sent = len(messages_built)
            else:
                for channel, text, thread_ts in messages_built:
                    if not channel:
                        failed += 1
                        errors.append("missing channel after template render")
                        continue
                    try:
                        _post_slack_message(client, channel=channel, text=text, thread_ts=thread_ts)
                        sent += 1
                    except Exception as e:
                        failed += 1
                        errors.append(f"channel={channel}: {e}")
                        context.log.warning(f"Slack post failed channel={channel!r}: {e}")

            # Emit a one-row summary
            summary = pd.DataFrame([{
                "mode": mode,
                "messages_sent": sent,
                "messages_failed": failed,
                "dry_run": dry,
            }])
            metadata: Dict[str, Any] = {
                "messages_sent": MetadataValue.int(sent),
                "messages_failed": MetadataValue.int(failed),
                "dry_run": MetadataValue.bool(dry),
            }
            if errors:
                metadata["first_errors"] = MetadataValue.json(errors[:5])
            return Output(value=summary, metadata=metadata)

        return Definitions(assets=[_asset])
