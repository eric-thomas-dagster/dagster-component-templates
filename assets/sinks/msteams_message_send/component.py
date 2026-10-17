"""MSTeamsMessageSendComponent — post business-data-driven alerts to Microsoft Teams.

Renders a templated message using upstream-row fields and posts it to a
Microsoft Teams channel via an incoming webhook — one message per row, or
one digest message summarizing the whole DataFrame.

Common use:
  - "Customer health score crossed threshold" -> post to a Teams channel
  - "New high-value lead created" -> ping a sales channel with details
  - Daily digest: aggregated anomalies -> one summary message per run

Auth: reuse an existing `msteams_resource` (MSTeamsResourceComponent) via
`resource_key`, or authenticate directly with `webhook_url_env_var` (the
incoming-webhook URL for the target channel).

!! Scope: business-data-driven alerts only, NOT run-status notifications !!
This component posts messages driven by WAREHOUSE DATA CONDITIONS
evaluated on an upstream asset's rows (e.g. "which customers just
crossed a health-score threshold") — it has no awareness of Dagster run
status, asset materialization success/failure, or freshness violations.

It is explicitly NOT a reimplementation of Dagster's own pipeline-health
notifications. For "this job failed" / "this asset failed to
materialize" / "this asset is stale" alerts, do NOT use this component —
instead pair the `msteams_resource` (MSTeamsResourceComponent) with a
Dagster run-status sensor or hook (`run_failure_sensor`,
`make_teams_on_run_failure_sensor`, etc.), or — if you're on Dagster+ —
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
    None) both yield empty string -- otherwise a NaN value would render
    as the literal text "nan"."""
    def sub(m):
        key = m.group(1)
        v = row.get(key)
        if v is None:
            return ""
        if isinstance(v, float) and pd.isna(v):
            return ""
        return str(v)
    return re.sub(r"\{(\w+)\}", sub, template)


def _build_hero_card(title: Optional[str], text: str) -> Dict[str, Any]:
    """Builds a legacy Office 365 Connector "hero card" payload — the same
    shape `dagster_msteams.Card` produces, kept inline here so this
    component's own title-per-message templating doesn't depend on that
    class's hardcoded "Dagster Pipeline Alert" title."""
    content: Dict[str, Any] = {"text": text}
    if title:
        content["title"] = title
    return {
        "type": "message",
        "attachments": [
            {"contentType": "application/vnd.microsoft.card.hero", "content": content}
        ],
    }


def _post_teams_message(client: Any, payload: Dict[str, Any]) -> bool:
    """Isolates the one external API call (the MS Teams incoming-webhook
    POST, via `dagster_msteams.TeamsClient.post_message`) so tests can
    monkeypatch this wholesale instead of hitting the real network."""
    return client.post_message(payload=payload)



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization



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


class MSTeamsMessageSendComponent(Component, Model, Resolvable):
    """Post templated Microsoft Teams messages, one per upstream DataFrame row (or one digest)."""

    asset_name: str = Field(description="Output asset name (summary row).")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream DataFrame asset key.")

    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Resource key registered by an MSTeamsResourceComponent. Takes priority "
            "over webhook_url_env_var when both are set."
        ),
    )
    webhook_url_env_var: Optional[str] = Field(
        default=None,
        description="Env var with the Teams incoming-webhook URL. Used when resource_key is not set.",
    )

    mode: str = Field(
        default="per_row",
        description=(
            "'per_row': one Teams message per upstream row, using row fields in templates. "
            "'summary': one message per run, with all rows rendered in the body via summary_template."
        ),
    )

    title_template: Optional[str] = Field(
        default=None,
        description="Card title, with `{column}` placeholders. Optional — omitted if unset.",
    )
    message_template: str = Field(
        default="",
        description="Card body text, with `{column}` placeholders.",
    )
    summary_template: Optional[str] = Field(
        default=None,
        description="For mode='summary', a template rendered against the whole DataFrame's row "
                    "summary (use `{row_count}` / `{table_md}`, a markdown table of up to 50 rows).",
    )

    dry_run: bool = Field(
        default=False,
        description="If True, render and log every message but don't post to Teams. Use for staging.",
    )
    max_send: Optional[int] = Field(
        default=None,
        description="Hard cap on messages sent per run. None = no limit.",
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
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
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners',), csv=('partition_values',), tags=('tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        if not self.resource_key and not self.webhook_url_env_var:
            raise ValueError(
                "MSTeamsMessageSendComponent: supply resource_key (a registered "
                "MSTeamsResourceComponent) OR webhook_url_env_var."
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

            freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

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
        webhook_env = self.webhook_url_env_var
        mode = self.mode
        title_t = self.title_template
        msg_t = self.message_template
        summ_t = self.summary_template
        dry = self.dry_run
        cap = self.max_send

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"MS Teams message send (mode={mode}).",
            group_name=self.group_name,
            kinds={"msteams"},
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
                from dagster_msteams.client import TeamsClient
                webhook_url = EnvVar(webhook_env).get_value()
                if not webhook_url:
                    raise ValueError(f"Teams webhook URL missing — set {webhook_env!r}.")
                client = TeamsClient(hook_url=webhook_url)

            payloads_built: List[Any] = []
            if mode == "per_row":
                for _, src in upstream.iterrows():
                    row = src.to_dict()
                    title = _render(title_t, row) if title_t else None
                    text = _render(msg_t, row) if msg_t else ""
                    payloads_built.append(_build_hero_card(title, text))
                    if cap is not None and len(payloads_built) >= cap:
                        break
            elif mode == "summary":
                row = {"row_count": len(upstream)}
                if summ_t:
                    table_md = upstream.head(50).to_markdown(index=False) if not upstream.empty else "(empty)"
                    row["table_md"] = table_md
                title = _render(title_t, row) if title_t else None
                text = _render(msg_t, row) if msg_t else (summ_t or "")
                text = _render(text, row)
                payloads_built.append(_build_hero_card(title, text))
            else:
                raise ValueError(f"unknown mode={mode!r} (must be 'per_row' or 'summary')")

            sent, failed = 0, 0
            errors: List[str] = []
            if dry:
                for payload in payloads_built:
                    context.log.info(f"[DRY-RUN] payload={payload!r}")
                sent = len(payloads_built)
            else:
                for payload in payloads_built:
                    try:
                        _post_teams_message(client, payload)
                        sent += 1
                    except Exception as e:
                        failed += 1
                        errors.append(f"{e}")
                        context.log.warning(f"Teams post failed: {e}")

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


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat
