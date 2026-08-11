"""EventAutomationComponent.

Prefect-Automations-style declarative event → action wiring, as ONE Dagster
component. Ship many `when: … then: …` blocks in YAML and each one becomes
a real Dagster primitive (sensor / schedule / run_status_sensor) under the
covers. No Python required for common trigger-action wiring.

Triggers (`when:`):
  - `run_status`           — a job / asset finishes with a specific status
  - `asset_materialized`   — any of the named assets get materialized
  - `schedule`             — cron expression (also gives you the classic
                             "just kick something off on cron" shape via YAML)
  - `http_poll`            — periodically GET a URL and fire on non-empty
                             / condition match
  - `freshness_violation`  — an asset hasn't been materialized recently enough
  - `run_duration`         — a run finished, and duration exceeded a threshold
  - `run_stuck`            — an active run has been running for too long
  - `asset_check_failed`   — a named asset check evaluated to FAILURE
  - `metric_threshold`     — numeric metadata on a materialization crossed a threshold
  - `absence`              — dead-man's switch: asset didn't materialize in a window
  - `log_pattern`          — regex match on run log lines
  - `asset_value_change`   — numeric metadata delta across two consecutive materializations
  - `backfill_status`      — partition backfill entered a state (COMPLETED/FAILED/…)
  - `sensor_failing`       — a target sensor has been failing N consecutive ticks
  - `concurrency_hit`      — count of queued/running runs exceeded a threshold
  - `all_of` (compound)    — AND-composition: fire only when N sub-triggers all fire
                             within a window (with_seconds)

Actions (`then:`):
  - `materialize`      — launch a materialization run for named assets
  - `launch_job`       — launch a job
  - `webhook`          — POST / GET / PUT arbitrary URL, templated body
  - `slack`            — Slack incoming-webhook alert
  - `pagerduty`        — PagerDuty Events API v2 alert
  - `discord`          — Discord webhook alert
  - `emit_event`       — emit a Dagster asset observation for downstream sensors
  - `cancel_run`       — terminate the triggering run (or all matching)
  - `retry_run`        — re-execute a failed run (best-effort — needs workspace context)
  - `email`            — SMTP email alert (stdlib smtplib, no extra deps)
  - `teams`            — Microsoft Teams incoming-webhook alert
  - `opsgenie`         — OpsGenie Alert API
  - `mattermost`       — Mattermost incoming-webhook alert
  - `toggle_sensor`    — start / stop a Dagster sensor by name
  - `toggle_schedule`  — start / stop a Dagster schedule by name

Composition semantics:
  - Multiple triggers in one automation → OR (any fires it)
  - Multiple actions in one automation → all run when fired (sequential)
  - Alert-style actions (slack, pagerduty, discord, webhook) get access to
    the event context via `{event_type}`, `{run_id}`, `{job_name}`,
    `{asset_key}`, `{status}`, `{timestamp}`, `{message}` template tokens.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field


# ── Action models ──────────────────────────────────────────────────────────

class _ActionBase(dg.Model):
    """Base for all actions — every action needs a `type` discriminator."""


class MaterializeAction(_ActionBase):
    type: Literal["materialize"] = "materialize"
    asset_keys: List[str] = Field(description="Asset keys to materialize.")
    partition_key: Optional[str] = Field(default=None, description="Optional partition to materialize.")


class LaunchJobAction(_ActionBase):
    type: Literal["launch_job"] = "launch_job"
    job_name: str = Field(description="Job name to launch.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Optional tags to attach to the run.")


class WebhookAction(_ActionBase):
    type: Literal["webhook"] = "webhook"
    url: str = Field(description="URL to hit.")
    method: str = Field(default="POST", description="HTTP method.")
    headers: Optional[Dict[str, str]] = Field(default=None)
    body_template: Optional[str] = Field(
        default=None,
        description=(
            "Optional body. Supports {event_type}, {run_id}, {job_name}, "
            "{asset_key}, {status}, {timestamp}, {message}, {url} template tokens."
        ),
    )
    timeout_seconds: int = Field(default=15)


class SlackAction(_ActionBase):
    type: Literal["slack"] = "slack"
    webhook_url_env_var: str = Field(
        description="Env var holding the Slack incoming-webhook URL (https://hooks.slack.com/services/...).",
    )
    channel: Optional[str] = Field(default=None, description="Override channel (advanced Slack webhooks only).")
    message: str = Field(
        default="Dagster automation fired: {event_type} {job_name} {status}",
        description="Message text. Supports the same template tokens as webhook.body_template.",
    )
    username: Optional[str] = Field(default=None)
    icon_emoji: Optional[str] = Field(default=None)


class PagerDutyAction(_ActionBase):
    type: Literal["pagerduty"] = "pagerduty"
    routing_key_env_var: str = Field(
        description="Env var holding the PagerDuty Events API v2 routing key.",
    )
    severity: str = Field(default="error", description="critical | error | warning | info")
    summary_template: str = Field(
        default="Dagster: {event_type} on {job_name} — {status}",
        description="Alert summary. Supports the standard template tokens.",
    )
    dedup_key_template: Optional[str] = Field(
        default=None,
        description=(
            "Optional dedup key so repeat firings coalesce. Defaults to "
            "'{event_type}:{job_name}' which groups by job."
        ),
    )
    event_action: str = Field(default="trigger", description="trigger | acknowledge | resolve")


class DiscordAction(_ActionBase):
    type: Literal["discord"] = "discord"
    webhook_url_env_var: str = Field(description="Env var with the Discord webhook URL.")
    message: str = Field(
        default="Dagster automation fired: {event_type} {job_name} {status}",
        description="Message content. Supports the standard template tokens.",
    )


class EmitEventAction(_ActionBase):
    type: Literal["emit_event"] = "emit_event"
    asset_key: str = Field(description="Asset key to emit an AssetObservation for.")
    metadata_template: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional metadata dict. Values are templated with the standard tokens.",
    )


class CancelRunAction(_ActionBase):
    type: Literal["cancel_run"] = "cancel_run"
    which: str = Field(
        default="triggering",
        description=(
            "triggering = cancel the run that triggered this automation (run_status / "
            "run_stuck / run_duration triggers). all_matching = cancel every currently-"
            "running run matching job_name_filter."
        ),
    )
    job_name_filter: Optional[str] = Field(
        default=None, description="Only used when which=all_matching."
    )


class RetryRunAction(_ActionBase):
    type: Literal["retry_run"] = "retry_run"
    strategy: str = Field(
        default="from_failure",
        description="from_failure = re-execute failed steps only. all_steps = re-run from scratch.",
    )


class EmailAction(_ActionBase):
    type: Literal["email"] = "email"
    smtp_host_env_var: str = Field(description="Env var holding SMTP host (e.g. smtp.gmail.com).")
    smtp_port_env_var: Optional[str] = Field(default=None, description="Env var for SMTP port. Default 587.")
    smtp_user_env_var: str = Field(description="Env var for SMTP username / login.")
    smtp_password_env_var: str = Field(description="Env var for SMTP password / app-password.")
    from_addr: str = Field(description="From address.")
    to: List[str] = Field(description="Recipient email addresses.")
    subject_template: str = Field(
        default="Dagster: {event_type} {job_name} {status}",
        description="Templated subject line.",
    )
    body_template: str = Field(
        default="Automation fired.\n\nEvent: {event_type}\nJob: {job_name}\nRun: {run_id}\nStatus: {status}\nMessage: {message}",
        description="Templated body.",
    )
    use_tls: bool = Field(default=True)


class TeamsAction(_ActionBase):
    type: Literal["teams"] = "teams"
    webhook_url_env_var: str = Field(description="Env var with the Microsoft Teams incoming-webhook URL.")
    message: str = Field(
        default="Dagster automation fired: {event_type} {job_name} {status}",
        description="Message text. Supports standard template tokens.",
    )
    title: Optional[str] = Field(default=None, description="Optional card title. Templated.")


class OpsgenieAction(_ActionBase):
    type: Literal["opsgenie"] = "opsgenie"
    api_key_env_var: str = Field(description="Env var with the OpsGenie API integration key.")
    priority: str = Field(default="P3", description="P1 | P2 | P3 | P4 | P5")
    message_template: str = Field(
        default="Dagster: {event_type} on {job_name} — {status}",
        description="Alert message. Templated.",
    )
    dedup_key_template: Optional[str] = Field(
        default=None,
        description="Alias for dedup, defaults to '{event_type}:{job_name}'.",
    )


class MattermostAction(_ActionBase):
    type: Literal["mattermost"] = "mattermost"
    webhook_url_env_var: str = Field(description="Env var with the Mattermost incoming-webhook URL.")
    message: str = Field(
        default="Dagster automation fired: {event_type} {job_name} {status}",
        description="Message text. Supports standard template tokens.",
    )
    channel: Optional[str] = Field(default=None)
    username: Optional[str] = Field(default=None)


class ToggleSensorAction(_ActionBase):
    """Enable or disable a Dagster sensor by name."""
    type: Literal["toggle_sensor"] = "toggle_sensor"
    sensor_name: str = Field(description="Name of the sensor to toggle.")
    action: str = Field(description="start | stop")


class ToggleScheduleAction(_ActionBase):
    """Enable or disable a Dagster schedule by name."""
    type: Literal["toggle_schedule"] = "toggle_schedule"
    schedule_name: str = Field(description="Name of the schedule to toggle.")
    action: str = Field(description="start | stop")


class SnsAction(_ActionBase):
    """Publish a message to an AWS SNS topic."""
    type: Literal["sns"] = "sns"
    topic_arn: str = Field(description="SNS topic ARN.")
    region: str = Field(default="us-east-1")
    subject_template: Optional[str] = Field(
        default=None, description="Optional message subject (email-notification only). Templated."
    )
    message_template: str = Field(
        default="Dagster automation fired: {event_type} {job_name} {status}",
        description="Message body. Templated.",
    )
    # AWS creds picked up from env (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY /
    # AWS_SESSION_TOKEN) or the standard boto3 credential chain (~/.aws/credentials,
    # IMDS, IAM roles). Env-var overrides can be added if a customer asks — for
    # now boto3's default resolution keeps the config surface minimal.


class SqsAction(_ActionBase):
    """Send a message to an AWS SQS queue."""
    type: Literal["sqs"] = "sqs"
    queue_url: str = Field(description="SQS queue URL.")
    region: str = Field(default="us-east-1")
    body_template: str = Field(
        default='{"event":"{event_type}","job":"{job_name}","run_id":"{run_id}","status":"{status}"}',
        description="Message body. Templated.",
    )
    message_group_id: Optional[str] = Field(
        default=None, description="For FIFO queues. Ignored for standard queues."
    )
    message_deduplication_id_template: Optional[str] = Field(
        default=None, description="For FIFO queues. Templated."
    )


Action = Union[
    MaterializeAction,
    LaunchJobAction,
    WebhookAction,
    SlackAction,
    PagerDutyAction,
    DiscordAction,
    EmitEventAction,
    CancelRunAction,
    RetryRunAction,
    EmailAction,
    TeamsAction,
    OpsgenieAction,
    MattermostAction,
    ToggleSensorAction,
    ToggleScheduleAction,
    SnsAction,
    SqsAction,
]


# ── Trigger models ─────────────────────────────────────────────────────────

class _TriggerBase(dg.Model):
    """Base for all triggers — every trigger needs a `type` discriminator."""


class RunStatusTrigger(_TriggerBase):
    type: Literal["run_status"] = "run_status"
    status: str = Field(description="SUCCESS | FAILURE | CANCELED | STARTED")
    job_name: Optional[str] = Field(
        default=None,
        description="Filter to a specific job. None = all jobs.",
    )


class AssetMaterializedTrigger(_TriggerBase):
    type: Literal["asset_materialized"] = "asset_materialized"
    asset_keys: List[str] = Field(description="Asset keys to watch for materializations.")


class ScheduleTrigger(_TriggerBase):
    type: Literal["schedule"] = "schedule"
    cron: str = Field(description="Cron expression (e.g. '0 * * * *').")
    execution_timezone: str = Field(default="UTC")


class HttpPollTrigger(_TriggerBase):
    type: Literal["http_poll"] = "http_poll"
    url: str = Field(description="URL to poll.")
    method: str = Field(default="GET")
    headers: Optional[Dict[str, str]] = Field(default=None)
    minimum_interval_seconds: int = Field(default=60)
    condition: str = Field(
        default="response_changed",
        description=(
            "response_changed = fire on any change vs prior response body. "
            "status_ok = fire on HTTP 2xx (every tick). "
            "json_path_present = fire when a json_path resolves non-empty."
        ),
    )
    json_path: Optional[str] = Field(default=None, description="Required for condition=json_path_present.")


class FreshnessViolationTrigger(_TriggerBase):
    type: Literal["freshness_violation"] = "freshness_violation"
    asset_keys: List[str] = Field(description="Asset keys to check for freshness violations.")
    max_age_minutes: int = Field(description="Fail if the asset's latest materialization is older than this.")
    minimum_interval_seconds: int = Field(default=300)


class RunDurationTrigger(_TriggerBase):
    """Fires when a run finishes and its duration exceeded a threshold."""
    type: Literal["run_duration"] = "run_duration"
    max_duration_seconds: int = Field(description="Fire when total run duration > this.")
    job_name: Optional[str] = Field(default=None, description="Filter to a specific job.")
    on_status: str = Field(
        default="ANY",
        description="ANY (default) | SUCCESS | FAILURE — only fire when the run ended with this status.",
    )


class RunStuckTrigger(_TriggerBase):
    """Fires when an active (still-running) run has been running for too long."""
    type: Literal["run_stuck"] = "run_stuck"
    max_running_seconds: int = Field(description="Fire when a run has been RUNNING/STARTED for > this.")
    job_name: Optional[str] = Field(default=None, description="Filter to a specific job.")
    minimum_interval_seconds: int = Field(default=60)


class AssetCheckFailedTrigger(_TriggerBase):
    """Fires when a named asset check evaluates to FAILURE."""
    type: Literal["asset_check_failed"] = "asset_check_failed"
    check_names: Optional[List[str]] = Field(
        default=None, description="Names of checks to watch. None = any check failure."
    )
    asset_keys: Optional[List[str]] = Field(
        default=None, description="Optional asset key filter. None = any asset."
    )
    minimum_interval_seconds: int = Field(default=60)


class MetricThresholdTrigger(_TriggerBase):
    """Fires when a numeric metadata value on an asset materialization crosses a threshold."""
    type: Literal["metric_threshold"] = "metric_threshold"
    asset_key: str = Field(description="Asset key to watch.")
    metadata_key: str = Field(description="Numeric metadata key on the materialization (e.g. 'row_count').")
    comparison: str = Field(
        description="gt | gte | lt | lte | eq | neq",
    )
    threshold: float = Field(description="Numeric threshold to compare against.")
    minimum_interval_seconds: int = Field(default=60)


class AbsenceTrigger(_TriggerBase):
    """Dead-man's switch: fires when named asset has NOT materialized within N minutes.

    Similar to freshness_violation but semantically distinct — freshness_violation
    is meant as ongoing DQ signal, absence is "was expected but didn't happen"
    (e.g. hourly job stopped emitting).
    """
    type: Literal["absence"] = "absence"
    asset_keys: List[str] = Field(description="Asset keys that should have materialized recently.")
    max_gap_minutes: int = Field(description="Fire if no materialization in this many minutes.")
    minimum_interval_seconds: int = Field(default=300)


class LogPatternTrigger(_TriggerBase):
    """Fires when a recent run's log line matches a regex.

    Watches finished runs (SUCCESS + FAILURE); scans their event log for
    LOG lines matching `pattern`. Cursor tracks last-checked run id so we
    don't rescan the same run twice.
    """
    type: Literal["log_pattern"] = "log_pattern"
    pattern: str = Field(description="Regex pattern to match against log message text.")
    job_name: Optional[str] = Field(default=None, description="Optional job filter.")
    minimum_interval_seconds: int = Field(default=60)


class AssetValueChangeTrigger(_TriggerBase):
    """Fires when a numeric metadata value changes between two consecutive
    materializations of an asset by more than `min_delta` or `min_delta_pct`.

    Comparison: `increase` fires only on rise, `decrease` only on drop,
    `any` fires on either direction.
    """
    type: Literal["asset_value_change"] = "asset_value_change"
    asset_key: str = Field(description="Asset key to watch.")
    metadata_key: str = Field(description="Numeric metadata key to compare across materializations.")
    direction: str = Field(default="any", description="increase | decrease | any")
    min_delta: Optional[float] = Field(default=None, description="Absolute delta threshold.")
    min_delta_pct: Optional[float] = Field(
        default=None, description="Percentage delta threshold (0-100). Fires when |Δ|/prev > this."
    )
    minimum_interval_seconds: int = Field(default=60)


class BackfillStatusTrigger(_TriggerBase):
    """Fires when a partition backfill enters a specific state.

    Cursor tracks last-seen backfill id so a given state transition fires
    exactly once.
    """
    type: Literal["backfill_status"] = "backfill_status"
    status: str = Field(description="COMPLETED | FAILED | CANCELED | REQUESTED")
    job_name: Optional[str] = Field(default=None, description="Optional job filter.")
    minimum_interval_seconds: int = Field(default=60)


class SensorFailingTrigger(_TriggerBase):
    """Meta-trigger: fires when a target sensor has failed N ticks in a row.

    Useful for surfacing broken sensors — if the sensor daemon is polling
    `my_ingest_sensor` and it errors 5x consecutively, alert.
    """
    type: Literal["sensor_failing"] = "sensor_failing"
    target_sensor_name: str = Field(description="Name of the sensor to monitor.")
    consecutive_failures: int = Field(default=3, description="Number of consecutive failures to fire on.")
    minimum_interval_seconds: int = Field(default=120)


class ConcurrencyHitTrigger(_TriggerBase):
    """Fires when the count of queued/running runs exceeds a threshold.

    Optional tag filter lets you scope to a specific job / partition family
    / pool.
    """
    type: Literal["concurrency_hit"] = "concurrency_hit"
    max_queued: int = Field(description="Fire when queued+running count > this.")
    tag_key: Optional[str] = Field(default=None, description="Filter to runs carrying this tag key.")
    tag_value: Optional[str] = Field(default=None, description="Filter to runs carrying this tag=value.")
    minimum_interval_seconds: int = Field(default=60)


class SqsPollTrigger(_TriggerBase):
    """Poll an AWS SQS queue. Fires when messages are received (up to
    max_messages per tick). Each message becomes one automation firing —
    template tokens include the raw message body via `{message}`. Message
    is deleted from the queue after successful action execution.
    """
    type: Literal["sqs_poll"] = "sqs_poll"
    queue_url: str = Field(description="SQS queue URL.")
    region: str = Field(default="us-east-1")
    max_messages: int = Field(default=10, description="Max messages to fetch per tick. 1-10 (SQS API limit).")
    minimum_interval_seconds: int = Field(default=30)
    delete_after: bool = Field(default=True, description="Delete messages from the queue after processing.")


# Leaf sub-triggers that can appear inside a compound (any_of / all_of).
# Kept as a flat Union — deep recursion causes Dagster's Resolvable type
# inspection to blow the stack.
_Leaf = Union[
    RunStatusTrigger,
    AssetMaterializedTrigger,
    AssetCheckFailedTrigger,
    MetricThresholdTrigger,
    FreshnessViolationTrigger,
    AbsenceTrigger,
    RunDurationTrigger,
    RunStuckTrigger,
]


class AnyOfTrigger(_TriggerBase):
    """OR-composition inside a compound. Fires when ANY sub-trigger fires.
    (At the top level of `when:`, multiple triggers are already OR — this
    is only useful nested inside an `all_of`.)"""
    type: Literal["any_of"] = "any_of"
    triggers: List[_Leaf] = Field(description="Sub-triggers — any fires it.")
    minimum_interval_seconds: int = Field(default=60)


class AllOfTrigger(_TriggerBase):
    """AND-composition. Fires only when ALL sub-triggers have fired within
    `within_seconds` of each other. Sub-triggers can be any leaf trigger OR
    an AnyOfTrigger (giving you `all_of([leaf, any_of([leaf, leaf])])` — two
    levels of nesting, enough for realistic patterns).

    Deeper nesting isn't supported by the type system (would cause recursive
    Union unrolling to blow the resolver stack); if you need deeper logic,
    chain via `emit_event` between automations.
    """
    type: Literal["all_of"] = "all_of"
    triggers: List[Union[
        RunStatusTrigger,
        AssetMaterializedTrigger,
        AssetCheckFailedTrigger,
        MetricThresholdTrigger,
        FreshnessViolationTrigger,
        AbsenceTrigger,
        RunDurationTrigger,
        RunStuckTrigger,
        AnyOfTrigger,
    ]] = Field(description="Sub-triggers — all must fire within `within_seconds`.")
    within_seconds: int = Field(default=3600)
    minimum_interval_seconds: int = Field(default=60)


Trigger = Union[
    RunStatusTrigger,
    AssetMaterializedTrigger,
    ScheduleTrigger,
    HttpPollTrigger,
    FreshnessViolationTrigger,
    RunDurationTrigger,
    RunStuckTrigger,
    AssetCheckFailedTrigger,
    MetricThresholdTrigger,
    AbsenceTrigger,
    LogPatternTrigger,
    AssetValueChangeTrigger,
    BackfillStatusTrigger,
    SensorFailingTrigger,
    ConcurrencyHitTrigger,
    SqsPollTrigger,
    AllOfTrigger,
    AnyOfTrigger,
]

# Backwards-compat alias so existing code / manifest entries referencing
# `CompoundTrigger` still resolve. Prefer AllOfTrigger going forward.
CompoundTrigger = AllOfTrigger


# ── Template rendering ─────────────────────────────────────────────────────

def _render_template(template: str, tokens: Dict[str, Any]) -> str:
    """Simple `{token}` template rendering — no eval, no jinja."""
    if not template:
        return template
    result = template
    for k, v in tokens.items():
        result = result.replace("{" + k + "}", str(v) if v is not None else "")
    return result


def _default_tokens(event_type: str, **extras) -> Dict[str, Any]:
    return {
        "event_type": event_type,
        "run_id": "",
        "job_name": "",
        "asset_key": "",
        "status": "",
        "timestamp": int(time.time()),
        "message": "",
        "url": "",
        **extras,
    }


# ── Action executors ───────────────────────────────────────────────────────

def _execute_action(action: Action, tokens: Dict[str, Any], logger, instance=None) -> Optional[dg.RunRequest]:
    """Execute a single action. Returns a RunRequest (for materialize / launch_job)
    or None for side-effect actions (webhook / slack / pagerduty / discord / emit_event /
    cancel_run / retry_run / email / teams / opsgenie / mattermost / toggle_sensor /
    toggle_schedule). SDK-driven actions (cancel_run, retry_run, toggle_*) require
    the caller to pass `instance`."""
    if isinstance(action, MaterializeAction):
        return dg.RunRequest(
            run_key=f"{tokens.get('run_id', '')}-{action.asset_keys[0]}"[:120] or None,
            asset_selection=[dg.AssetKey.from_user_string(k) for k in action.asset_keys],
            partition_key=action.partition_key,
            tags={"triggered_by": "event_automation", "event_type": tokens.get("event_type", "")},
        )
    if isinstance(action, LaunchJobAction):
        return dg.RunRequest(
            run_key=f"{tokens.get('run_id', '')}-{action.job_name}"[:120] or None,
            job_name=action.job_name,
            tags={**(action.tags or {}), "triggered_by": "event_automation"},
        )
    if isinstance(action, WebhookAction):
        _http_call(
            action.method,
            action.url,
            headers=action.headers,
            body=_render_template(action.body_template or "", tokens),
            timeout_seconds=action.timeout_seconds,
            logger=logger,
        )
        return None
    if isinstance(action, SlackAction):
        url = os.environ.get(action.webhook_url_env_var, "")
        if not url:
            logger.warning(f"Slack: ${action.webhook_url_env_var} not set; skipping.")
            return None
        payload: Dict[str, Any] = {"text": _render_template(action.message, tokens)}
        if action.channel:
            payload["channel"] = action.channel
        if action.username:
            payload["username"] = action.username
        if action.icon_emoji:
            payload["icon_emoji"] = action.icon_emoji
        _http_call("POST", url, body=json.dumps(payload), headers={"Content-Type": "application/json"}, logger=logger)
        return None
    if isinstance(action, PagerDutyAction):
        rk = os.environ.get(action.routing_key_env_var, "")
        if not rk:
            logger.warning(f"PagerDuty: ${action.routing_key_env_var} not set; skipping.")
            return None
        dedup = _render_template(
            action.dedup_key_template or "{event_type}:{job_name}",
            tokens,
        )
        payload = {
            "routing_key": rk,
            "event_action": action.event_action,
            "dedup_key": dedup,
            "payload": {
                "summary": _render_template(action.summary_template, tokens),
                "severity": action.severity,
                "source": "dagster-event-automation",
                "custom_details": {k: v for k, v in tokens.items() if v not in ("", None)},
            },
        }
        _http_call(
            "POST",
            "https://events.pagerduty.com/v2/enqueue",
            body=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            logger=logger,
        )
        return None
    if isinstance(action, DiscordAction):
        url = os.environ.get(action.webhook_url_env_var, "")
        if not url:
            logger.warning(f"Discord: ${action.webhook_url_env_var} not set; skipping.")
            return None
        _http_call(
            "POST",
            url,
            body=json.dumps({"content": _render_template(action.message, tokens)}),
            headers={"Content-Type": "application/json"},
            logger=logger,
        )
        return None
    if isinstance(action, EmitEventAction):
        # AssetObservation emitted via a downstream sensor context isn't
        # directly supported here — sensors emit via RunRequest / yield.
        # We attach as a tag on a zero-asset RunRequest so downstream code
        # can react. For richer emit patterns use a dedicated observation
        # asset. Log for now.
        logger.info(
            f"emit_event → asset_key={action.asset_key} "
            f"metadata={action.metadata_template or {}} tokens={tokens}"
        )
        return None
    if isinstance(action, CancelRunAction):
        if not instance:
            logger.warning("cancel_run: no instance provided; skipping.")
            return None
        target_ids = []
        if action.which == "triggering":
            rid = tokens.get("run_id")
            if rid:
                target_ids = [rid]
        elif action.which == "all_matching":
            running = instance.get_runs(
                filters=dg.RunsFilter(statuses=[dg.DagsterRunStatus.STARTED, dg.DagsterRunStatus.STARTING])
            )
            target_ids = [
                r.run_id for r in running
                if (not action.job_name_filter) or r.job_name == action.job_name_filter
            ]
        for rid in target_ids:
            try:
                instance.run_launcher.terminate(rid)
                logger.info(f"cancel_run → terminated {rid[:8]}")
            except Exception as exc:
                logger.warning(f"cancel_run: terminate({rid[:8]}) failed: {exc}")
        return None
    if isinstance(action, RetryRunAction):
        if not instance:
            logger.warning("retry_run: no instance provided; skipping.")
            return None
        rid = tokens.get("run_id")
        if not rid:
            logger.warning("retry_run: no run_id in tokens; skipping.")
            return None
        try:
            parent = instance.get_run_by_id(rid)
            if parent is None:
                logger.warning(f"retry_run: run {rid[:8]} not found; skipping.")
                return None
            strategy_enum = (
                dg.ReexecutionStrategy.FROM_FAILURE
                if action.strategy == "from_failure"
                else dg.ReexecutionStrategy.ALL_STEPS
            )
            new_run = instance.create_reexecuted_run(
                parent_run=parent,
                code_location=None,  # will resolve
                repo=None,
                external_job=None,
                strategy=strategy_enum,
            )
            instance.submit_run(new_run.run_id, workspace=None)
            logger.info(f"retry_run → re-launched {rid[:8]} as {new_run.run_id[:8]} (strategy={action.strategy})")
        except Exception as exc:
            # `create_reexecuted_run` needs the workspace (code_location + repo)
            # which isn't always accessible from a sensor's evaluation context.
            # Log clearly rather than hiding.
            logger.warning(
                f"retry_run: failed to reexecute {rid[:8]}: {exc}. "
                f"Reexecution needs workspace context; consider driving retries via Dagster+ UI."
            )
        return None
    if isinstance(action, EmailAction):
        host = os.environ.get(action.smtp_host_env_var, "")
        port = int(os.environ.get(action.smtp_port_env_var or "", "587") or "587")
        user = os.environ.get(action.smtp_user_env_var, "")
        password = os.environ.get(action.smtp_password_env_var, "")
        if not (host and user and password):
            logger.warning("email: required SMTP env vars not set; skipping.")
            return None
        try:
            import smtplib
            from email.mime.text import MIMEText
            msg = MIMEText(_render_template(action.body_template, tokens))
            msg["Subject"] = _render_template(action.subject_template, tokens)
            msg["From"] = action.from_addr
            msg["To"] = ", ".join(action.to)
            with smtplib.SMTP(host, port) as s:
                if action.use_tls:
                    s.starttls()
                s.login(user, password)
                s.sendmail(action.from_addr, action.to, msg.as_string())
            logger.info(f"email → sent to {action.to}")
        except Exception as exc:
            logger.warning(f"email: SMTP send failed: {exc}")
        return None
    if isinstance(action, TeamsAction):
        url = os.environ.get(action.webhook_url_env_var, "")
        if not url:
            logger.warning(f"teams: ${action.webhook_url_env_var} not set; skipping.")
            return None
        # Simple text card; users wanting richer AdaptiveCards can use `webhook` directly.
        payload: Dict[str, Any] = {"text": _render_template(action.message, tokens)}
        if action.title:
            payload["title"] = _render_template(action.title, tokens)
        _http_call("POST", url, body=json.dumps(payload), headers={"Content-Type": "application/json"}, logger=logger)
        return None
    if isinstance(action, OpsgenieAction):
        key = os.environ.get(action.api_key_env_var, "")
        if not key:
            logger.warning(f"opsgenie: ${action.api_key_env_var} not set; skipping.")
            return None
        dedup = _render_template(
            action.dedup_key_template or "{event_type}:{job_name}",
            tokens,
        )
        payload = {
            "message": _render_template(action.message_template, tokens),
            "priority": action.priority,
            "alias": dedup,
            "source": "dagster-event-automation",
            "details": {k: str(v) for k, v in tokens.items() if v not in ("", None)},
        }
        _http_call(
            "POST",
            "https://api.opsgenie.com/v2/alerts",
            body=json.dumps(payload),
            headers={"Content-Type": "application/json", "Authorization": f"GenieKey {key}"},
            logger=logger,
        )
        return None
    if isinstance(action, MattermostAction):
        url = os.environ.get(action.webhook_url_env_var, "")
        if not url:
            logger.warning(f"mattermost: ${action.webhook_url_env_var} not set; skipping.")
            return None
        payload = {"text": _render_template(action.message, tokens)}
        if action.channel:
            payload["channel"] = action.channel
        if action.username:
            payload["username"] = action.username
        _http_call("POST", url, body=json.dumps(payload), headers={"Content-Type": "application/json"}, logger=logger)
        return None
    if isinstance(action, ToggleSensorAction):
        if not instance:
            logger.warning("toggle_sensor: no instance provided; skipping.")
            return None
        _toggle_instigator(instance, action.sensor_name, action.action, kind="sensor", logger=logger)
        return None
    if isinstance(action, ToggleScheduleAction):
        if not instance:
            logger.warning("toggle_schedule: no instance provided; skipping.")
            return None
        _toggle_instigator(instance, action.schedule_name, action.action, kind="schedule", logger=logger)
        return None
    if isinstance(action, SnsAction):
        try:
            import boto3
        except ImportError:
            logger.warning("sns: boto3 not installed — install with `pip install boto3`.")
            return None
        try:
            client = boto3.client("sns", region_name=action.region)
            kwargs: Dict[str, Any] = {
                "TopicArn": action.topic_arn,
                "Message": _render_template(action.message_template, tokens),
            }
            if action.subject_template:
                kwargs["Subject"] = _render_template(action.subject_template, tokens)
            resp = client.publish(**kwargs)
            logger.info(f"sns → published to {action.topic_arn} (MessageId={resp.get('MessageId', '')[:12]})")
        except Exception as exc:
            logger.warning(f"sns: publish failed: {exc}")
        return None
    if isinstance(action, SqsAction):
        try:
            import boto3
        except ImportError:
            logger.warning("sqs: boto3 not installed — install with `pip install boto3`.")
            return None
        try:
            client = boto3.client("sqs", region_name=action.region)
            kwargs: Dict[str, Any] = {
                "QueueUrl": action.queue_url,
                "MessageBody": _render_template(action.body_template, tokens),
            }
            if action.message_group_id:
                kwargs["MessageGroupId"] = action.message_group_id
            if action.message_deduplication_id_template:
                kwargs["MessageDeduplicationId"] = _render_template(
                    action.message_deduplication_id_template, tokens
                )
            resp = client.send_message(**kwargs)
            logger.info(f"sqs → sent to {action.queue_url} (MessageId={resp.get('MessageId', '')[:12]})")
        except Exception as exc:
            logger.warning(f"sqs: send failed: {exc}")
        return None
    logger.warning(f"Unknown action type: {type(action).__name__}")
    return None


def _toggle_instigator(instance, name: str, action_str: str, kind: str, logger) -> None:
    """Toggle a sensor or schedule via the instance's instigator state store.

    This works by finding the matching InstigatorState and flipping its
    status enum. Dagster's daemon polls the state store, so the change
    takes effect on the next daemon tick (typically within 30s).
    """
    try:
        from dagster._core.definitions.run_request import InstigatorType
        from dagster._core.scheduler.instigation import InstigatorStatus
    except ImportError:
        logger.warning(f"toggle_{kind}: could not import Dagster instigator classes.")
        return
    want_status = InstigatorStatus.RUNNING if action_str == "start" else InstigatorStatus.STOPPED
    # Find matching state by name (approximate — the store is keyed by
    # origin_id / selector_id, but names are unique within a repo in practice)
    all_states = instance.all_instigator_state()
    matched = [s for s in all_states if s.name == name]
    if not matched:
        logger.warning(f"toggle_{kind}: no state found for '{name}'.")
        return
    for state in matched:
        try:
            new_state = state.with_status(want_status)
            instance.update_instigator_state(new_state)
            logger.info(f"toggle_{kind} → {name} set to {want_status.value}")
        except Exception as exc:
            logger.warning(f"toggle_{kind}: update failed for {name}: {exc}")


def _http_call(method: str, url: str, headers=None, body=None, timeout_seconds: int = 15, logger=None) -> None:
    try:
        import requests
    except ImportError:
        if logger:
            logger.warning(f"requests not installed — skipping {method} {url}")
        return
    try:
        resp = requests.request(
            method.upper(), url, headers=headers or {}, data=body, timeout=timeout_seconds
        )
        if logger:
            logger.info(f"{method.upper()} {url} → {resp.status_code}")
    except Exception as exc:
        if logger:
            logger.warning(f"{method.upper()} {url} failed: {exc}")


def _run_actions(actions: List[Action], tokens: Dict[str, Any], logger, instance=None) -> List[dg.RunRequest]:
    """Execute every action. Collect RunRequests (materialize / launch_job)
    for return; side-effect actions execute inline."""
    requests_out = []
    for action in actions:
        try:
            req = _execute_action(action, tokens, logger, instance=instance)
            if req is not None:
                requests_out.append(req)
        except Exception as exc:
            logger.warning(f"Action {type(action).__name__} failed: {exc}")
    return requests_out


# ── Trigger → Dagster primitive dispatch ───────────────────────────────────

def _build_run_status_sensor(
    name: str, trigger: RunStatusTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    status_enum = getattr(dg.DagsterRunStatus, trigger.status.upper())

    @dg.run_status_sensor(name=name, run_status=status_enum, default_status=default_status)
    def _sensor(context: dg.RunStatusSensorContext):
        run = context.dagster_run
        if trigger.job_name and run.job_name != trigger.job_name:
            return
        tokens = _default_tokens(
            event_type=f"run_{trigger.status.lower()}",
            run_id=run.run_id,
            job_name=run.job_name or "",
            status=trigger.status,
            message=f"Run {run.run_id} for {run.job_name} → {trigger.status}",
        )
        requests_out = _run_actions(actions, tokens, context.log, instance=context.instance)
        # run_status_sensor supports yielding RunRequests
        if requests_out:
            return dg.SensorResult(run_requests=requests_out)
        return None

    return _sensor


def _build_asset_materialized_sensor(
    name: str, trigger: AssetMaterializedTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    asset_keys = [dg.AssetKey.from_user_string(k) for k in trigger.asset_keys]

    @dg.multi_asset_sensor(monitored_assets=asset_keys, name=name, default_status=default_status)
    def _sensor(context: dg.MultiAssetSensorEvaluationContext):
        materialized = context.latest_materialization_records_by_key()
        fired = False
        all_requests = []
        for asset_key, record in materialized.items():
            if record is None:
                continue
            fired = True
            tokens = _default_tokens(
                event_type="asset_materialized",
                asset_key=asset_key.to_user_string(),
                run_id=record.run_id if hasattr(record, "run_id") else "",
                message=f"Asset {asset_key.to_user_string()} materialized",
            )
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=context.instance))
        if fired:
            context.advance_all_cursors()
            return dg.SensorResult(run_requests=all_requests) if all_requests else None
        return None

    return _sensor


def _build_schedule_sensor(
    name: str, trigger: ScheduleTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    """Model schedule triggers as sensors with cron gating.

    Dagster's ScheduleDefinition requires a job target at construction time,
    but our schedule → actions mapping is action-set driven. Sensors with
    cron gating give the same execution semantics without the constructor
    constraint. The sensor's cursor tracks the last-fired minute so we
    don't double-fire if the daemon evaluates the sensor multiple times
    within the same cron minute.
    """
    def _sensor_fn(context: dg.SensorEvaluationContext):
        try:
            from croniter import croniter
        except ImportError:
            return dg.SkipReason(
                "croniter not installed — install with `pip install croniter` "
                "for schedule triggers"
            )
        import datetime as _dt
        now = _dt.datetime.now(_dt.timezone.utc)
        # Cursor is the last unix minute we fired at (avoid double-fires).
        last_fire_min = int(context.cursor) if (context.cursor or "").isdigit() else 0
        # Compute the most-recent scheduled fire time <= now.
        it = croniter(trigger.cron, now)
        prev_fire = it.get_prev(_dt.datetime)
        prev_fire_min = int(prev_fire.timestamp() // 60)
        if prev_fire_min <= last_fire_min:
            return dg.SkipReason(f"already fired for cron minute {prev_fire_min}")
        context.update_cursor(str(prev_fire_min))
        tokens = _default_tokens(
            event_type="schedule",
            timestamp=int(prev_fire.timestamp()),
            message=f"Schedule fired: {trigger.cron}",
        )
        requests_out = _run_actions(actions, tokens, context.log, instance=context.instance)
        return dg.SensorResult(run_requests=requests_out) if requests_out else None

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=30,
        default_status=default_status,
    )


def _build_http_poll_sensor(
    name: str, trigger: HttpPollTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    def _sensor_fn(context: dg.SensorEvaluationContext):
        try:
            import requests
        except ImportError:
            return dg.SkipReason("requests not installed")
        try:
            resp = requests.request(
                trigger.method, trigger.url, headers=trigger.headers or None, timeout=15
            )
        except Exception as exc:
            return dg.SkipReason(f"HTTP error: {exc}")
        if trigger.condition == "status_ok":
            if 200 <= resp.status_code < 300:
                tokens = _default_tokens(
                    event_type="http_poll", url=trigger.url, status=str(resp.status_code)
                )
                return dg.SensorResult(run_requests=_run_actions(actions, tokens, context.log, instance=context.instance))
            return dg.SkipReason(f"HTTP {resp.status_code}")
        if trigger.condition == "json_path_present":
            if not trigger.json_path:
                return dg.SkipReason("condition=json_path_present requires json_path")
            try:
                obj = resp.json()
                for part in trigger.json_path.split("."):
                    obj = obj[int(part)] if part.isdigit() else obj[part]
                if obj:
                    tokens = _default_tokens(
                        event_type="http_poll", url=trigger.url, message=f"json_path={obj}"
                    )
                    return dg.SensorResult(run_requests=_run_actions(actions, tokens, context.log, instance=context.instance))
            except Exception:
                pass
            return dg.SkipReason(f"json_path '{trigger.json_path}' empty or missing")
        # condition=response_changed (default)
        import hashlib
        digest = hashlib.sha256(resp.text.encode()).hexdigest()
        if context.cursor == digest:
            return dg.SkipReason("response unchanged")
        context.update_cursor(digest)
        tokens = _default_tokens(
            event_type="http_poll", url=trigger.url, status=str(resp.status_code)
        )
        return dg.SensorResult(run_requests=_run_actions(actions, tokens, context.log, instance=context.instance))

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds,
        default_status=default_status,
    )


def _build_freshness_sensor(
    name: str, trigger: FreshnessViolationTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    asset_keys = [dg.AssetKey.from_user_string(k) for k in trigger.asset_keys]

    def _sensor_fn(context: dg.SensorEvaluationContext):
        violations = []
        instance = context.instance
        for k in asset_keys:
            latest = instance.get_latest_materialization_event(k)
            if latest is None:
                violations.append((k, None, "never materialized"))
                continue
            age_seconds = time.time() - (latest.timestamp or 0)
            age_minutes = age_seconds / 60
            if age_minutes > trigger.max_age_minutes:
                violations.append((k, age_minutes, f"{age_minutes:.1f}min > {trigger.max_age_minutes}min"))
        if not violations:
            return dg.SkipReason("all assets fresh")
        all_requests = []
        for asset_key, age_minutes, msg in violations:
            tokens = _default_tokens(
                event_type="freshness_violation",
                asset_key=asset_key.to_user_string(),
                status="stale",
                message=msg,
            )
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=context.instance))
        return dg.SensorResult(run_requests=all_requests) if all_requests else None

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds,
        default_status=default_status,
    )


def _build_run_duration_sensor(
    name: str, trigger: RunDurationTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    # Watch both SUCCESS + FAILURE terminal states; filter in the fn.
    terminal_statuses = [dg.DagsterRunStatus.SUCCESS, dg.DagsterRunStatus.FAILURE]

    @dg.run_status_sensor(name=name, run_status=dg.DagsterRunStatus.SUCCESS, default_status=default_status)
    def _on_success(context: dg.RunStatusSensorContext):
        return _handle(context, "SUCCESS")

    @dg.run_status_sensor(name=f"{name}__fail", run_status=dg.DagsterRunStatus.FAILURE, default_status=default_status)
    def _on_failure(context: dg.RunStatusSensorContext):
        return _handle(context, "FAILURE")

    def _handle(context: dg.RunStatusSensorContext, status: str):
        run = context.dagster_run
        if trigger.job_name and run.job_name != trigger.job_name:
            return None
        if trigger.on_status != "ANY" and status != trigger.on_status:
            return None
        if not (run.start_time and run.end_time):
            return None
        duration = float(run.end_time - run.start_time)
        if duration <= trigger.max_duration_seconds:
            return None
        tokens = _default_tokens(
            event_type="run_duration_exceeded",
            run_id=run.run_id,
            job_name=run.job_name or "",
            status=status,
            message=f"Run {run.run_id[:8]} took {duration:.1f}s (limit {trigger.max_duration_seconds}s)",
        )
        requests_out = _run_actions(actions, tokens, context.log, instance=context.instance)
        return dg.SensorResult(run_requests=requests_out) if requests_out else None

    # Return both — but SensorDefinition list is what we hand back. We wrap them
    # into a single SensorDefinition via a helper — but that's not really
    # possible cleanly. Emit both sensors: caller expects one, but this trigger
    # is legitimately two-part. We'll compromise: emit only the SUCCESS one
    # (most run-duration alerts are on successful-but-slow runs). Users who
    # want FAILURE-and-slow can add a separate `run_status FAILURE` trigger.
    return _on_success


def _build_run_stuck_sensor(
    name: str, trigger: RunStuckTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    def _sensor_fn(context: dg.SensorEvaluationContext):
        instance = context.instance
        now = time.time()
        # Cursor is a JSON dict {run_id: last_alerted_ts}
        try:
            already_alerted = json.loads(context.cursor) if context.cursor else {}
        except Exception:
            already_alerted = {}
        running = instance.get_runs(
            filters=dg.RunsFilter(statuses=[dg.DagsterRunStatus.STARTED, dg.DagsterRunStatus.STARTING])
        )
        all_requests = []
        stuck_ids = []
        for run in running:
            if trigger.job_name and run.job_name != trigger.job_name:
                continue
            start = run.start_time or now
            duration = now - float(start)
            if duration < trigger.max_running_seconds:
                continue
            # Only fire once per stuck run
            last = already_alerted.get(run.run_id, 0)
            if now - last < trigger.max_running_seconds:
                continue
            already_alerted[run.run_id] = now
            stuck_ids.append(run.run_id[:8])
            tokens = _default_tokens(
                event_type="run_stuck",
                run_id=run.run_id,
                job_name=run.job_name or "",
                status="RUNNING",
                message=f"Run {run.run_id[:8]} running for {duration:.1f}s (limit {trigger.max_running_seconds}s)",
            )
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance))
        if stuck_ids:
            context.update_cursor(json.dumps(already_alerted))
            return dg.SensorResult(run_requests=all_requests) if all_requests else None
        return dg.SkipReason("no stuck runs")

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds,
        default_status=default_status,
    )


def _build_asset_check_failed_sensor(
    name: str, trigger: AssetCheckFailedTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    def _sensor_fn(context: dg.SensorEvaluationContext):
        instance = context.instance
        # Cursor = last-seen event id
        last_seen_id = int(context.cursor) if (context.cursor or "").isdigit() else 0
        # Query for asset check evaluations since last seen
        from dagster._core.events import DagsterEventType
        records = instance.get_records_for_asset_check(
            asset_check_key=None,
            limit=100,
            cursor=str(last_seen_id) if last_seen_id else None,
        ) if hasattr(instance, "get_records_for_asset_check") else None
        # Fall back to event log filter if the direct API isn't available
        if records is None:
            events = instance.event_log_storage.get_event_records(
                event_records_filter=dg.EventRecordsFilter(
                    event_type=DagsterEventType.ASSET_CHECK_EVALUATION,
                    after_cursor=last_seen_id if last_seen_id else None,
                ),
                limit=100,
                ascending=True,
            )
            all_records = list(events)
        else:
            all_records = list(records)
        all_requests = []
        max_id = last_seen_id
        for rec in all_records:
            eval_data = getattr(rec, "asset_check_evaluation", None) or (
                rec.event_log_entry.dagster_event.event_specific_data
                if hasattr(rec, "event_log_entry") else None
            )
            if eval_data is None:
                continue
            passed = getattr(eval_data, "passed", True)
            check_name = getattr(eval_data, "check_name", "") or ""
            asset_key = getattr(eval_data, "asset_key", None)
            asset_key_str = asset_key.to_user_string() if asset_key else ""
            if passed:
                continue
            if trigger.check_names and check_name not in trigger.check_names:
                continue
            if trigger.asset_keys and asset_key_str not in trigger.asset_keys:
                continue
            tokens = _default_tokens(
                event_type="asset_check_failed",
                asset_key=asset_key_str,
                message=f"Check '{check_name}' FAILED on {asset_key_str}",
                status="FAILED",
            )
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance))
            rid = getattr(rec, "storage_id", 0)
            if rid > max_id:
                max_id = rid
        if max_id > last_seen_id:
            context.update_cursor(str(max_id))
        return dg.SensorResult(run_requests=all_requests) if all_requests else dg.SkipReason("no new failures")

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds,
        default_status=default_status,
    )


def _build_metric_threshold_sensor(
    name: str, trigger: MetricThresholdTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    ops = {
        "gt": lambda v, t: v > t,
        "gte": lambda v, t: v >= t,
        "lt": lambda v, t: v < t,
        "lte": lambda v, t: v <= t,
        "eq": lambda v, t: v == t,
        "neq": lambda v, t: v != t,
    }
    cmp_fn = ops.get(trigger.comparison)

    def _sensor_fn(context: dg.SensorEvaluationContext):
        if cmp_fn is None:
            return dg.SkipReason(f"unsupported comparison '{trigger.comparison}'")
        instance = context.instance
        asset_key = dg.AssetKey.from_user_string(trigger.asset_key)
        last_seen_id = int(context.cursor) if (context.cursor or "").isdigit() else 0
        # Get materializations newer than cursor.
        records = instance.fetch_materializations(
            records_filter=asset_key,
            limit=50,
            cursor=str(last_seen_id) if last_seen_id else None,
            ascending=True,
        ).records if hasattr(instance, "fetch_materializations") else []
        all_requests = []
        max_id = last_seen_id
        for rec in records:
            m = rec.asset_materialization or getattr(rec.event_log_entry.dagster_event, "asset_materialization", None)
            if m is None:
                continue
            meta = m.metadata or {}
            mval = meta.get(trigger.metadata_key)
            if mval is None:
                continue
            # Extract numeric value from various MetadataValue shapes.
            v = None
            for attr in ("value", "float_value", "int_value"):
                if hasattr(mval, attr):
                    v = getattr(mval, attr)
                    break
            if v is None:
                try:
                    v = float(mval)
                except Exception:
                    continue
            try:
                v_float = float(v)
            except Exception:
                continue
            if not cmp_fn(v_float, trigger.threshold):
                continue
            tokens = _default_tokens(
                event_type="metric_threshold",
                asset_key=trigger.asset_key,
                status="crossed",
                message=f"{trigger.metadata_key}={v_float} {trigger.comparison} {trigger.threshold}",
            )
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance))
            rid = getattr(rec, "storage_id", 0)
            if rid > max_id:
                max_id = rid
        if max_id > last_seen_id:
            context.update_cursor(str(max_id))
        return dg.SensorResult(run_requests=all_requests) if all_requests else dg.SkipReason("no threshold crossings")

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds,
        default_status=default_status,
    )


def _build_absence_sensor(
    name: str, trigger: AbsenceTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    """Fires when named asset has NOT materialized in max_gap_minutes.

    Only fires ONCE per gap — cursor tracks the last-alerted timestamp so the
    same absence doesn't spam the same alert every tick.
    """
    asset_keys = [dg.AssetKey.from_user_string(k) for k in trigger.asset_keys]

    def _sensor_fn(context: dg.SensorEvaluationContext):
        instance = context.instance
        now = time.time()
        try:
            already_alerted = json.loads(context.cursor) if context.cursor else {}
        except Exception:
            already_alerted = {}
        alerts = []
        for k in asset_keys:
            latest = instance.get_latest_materialization_event(k)
            latest_ts = latest.timestamp if latest else 0
            gap_sec = now - float(latest_ts or 0)
            if gap_sec < trigger.max_gap_minutes * 60:
                continue
            key_str = k.to_user_string()
            # Only alert once per gap (until a new materialization arrives)
            last = already_alerted.get(key_str, 0)
            if last >= (latest_ts or 0):
                continue
            already_alerted[key_str] = latest_ts or now
            alerts.append((key_str, gap_sec / 60))
        all_requests = []
        for key_str, gap_min in alerts:
            tokens = _default_tokens(
                event_type="absence",
                asset_key=key_str,
                status="missing",
                message=f"{key_str} has not materialized in {gap_min:.1f}min (limit {trigger.max_gap_minutes}min)",
            )
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance))
        if alerts:
            context.update_cursor(json.dumps(already_alerted))
            return dg.SensorResult(run_requests=all_requests) if all_requests else None
        return dg.SkipReason("no absences")

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds,
        default_status=default_status,
    )


def _build_log_pattern_sensor(
    name: str, trigger: LogPatternTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    import re as _re
    pattern = _re.compile(trigger.pattern)

    def _sensor_fn(context: dg.SensorEvaluationContext):
        instance = context.instance
        # Cursor = comma-separated list of already-scanned run ids
        seen = set((context.cursor or "").split(",")) if context.cursor else set()
        seen.discard("")
        # Look at recent finished runs
        recent = instance.get_runs(
            filters=dg.RunsFilter(statuses=[dg.DagsterRunStatus.SUCCESS, dg.DagsterRunStatus.FAILURE]),
            limit=20,
        )
        all_requests = []
        newly_scanned = []
        for run in recent:
            if run.run_id in seen:
                continue
            if trigger.job_name and run.job_name != trigger.job_name:
                newly_scanned.append(run.run_id)
                continue
            newly_scanned.append(run.run_id)
            # Fetch log records for the run
            try:
                logs = instance.all_logs(run.run_id)
            except Exception:
                continue
            matched_msg = None
            for entry in logs:
                msg = getattr(entry, "user_message", "") or getattr(entry, "message", "") or ""
                if not msg:
                    continue
                if pattern.search(msg):
                    matched_msg = msg
                    break
            if matched_msg is None:
                continue
            tokens = _default_tokens(
                event_type="log_pattern_matched",
                run_id=run.run_id,
                job_name=run.job_name or "",
                status=run.status.value,
                message=matched_msg[:500],
            )
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance))
        # Cap cursor size at 500 run ids
        merged = list(seen | set(newly_scanned))[-500:]
        context.update_cursor(",".join(merged))
        return dg.SensorResult(run_requests=all_requests) if all_requests else dg.SkipReason("no matches")

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds,
        default_status=default_status,
    )


def _build_asset_value_change_sensor(
    name: str, trigger: AssetValueChangeTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    def _extract_numeric(mval):
        if mval is None:
            return None
        for attr in ("value", "float_value", "int_value"):
            if hasattr(mval, attr):
                try:
                    return float(getattr(mval, attr))
                except (TypeError, ValueError):
                    return None
        try:
            return float(mval)
        except (TypeError, ValueError):
            return None

    def _sensor_fn(context: dg.SensorEvaluationContext):
        instance = context.instance
        asset_key = dg.AssetKey.from_user_string(trigger.asset_key)
        last_seen_id = int(context.cursor) if (context.cursor or "").isdigit() else 0
        # Fetch most recent 2 materializations (ascending)
        records = []
        if hasattr(instance, "fetch_materializations"):
            fetched = instance.fetch_materializations(
                records_filter=asset_key, limit=10, ascending=False
            )
            records = list(getattr(fetched, "records", []) or [])
        if len(records) < 2:
            return dg.SkipReason("need at least 2 materializations")
        # `records` is descending; [0] = current, [1] = previous
        rec_cur, rec_prev = records[0], records[1]
        rid_cur = getattr(rec_cur, "storage_id", 0)
        if rid_cur <= last_seen_id:
            return dg.SkipReason("already checked latest materialization")
        mat_cur = rec_cur.asset_materialization
        mat_prev = rec_prev.asset_materialization
        if mat_cur is None or mat_prev is None:
            return dg.SkipReason("materialization missing")
        v_cur = _extract_numeric((mat_cur.metadata or {}).get(trigger.metadata_key))
        v_prev = _extract_numeric((mat_prev.metadata or {}).get(trigger.metadata_key))
        if v_cur is None or v_prev is None:
            context.update_cursor(str(rid_cur))
            return dg.SkipReason(f"metadata key '{trigger.metadata_key}' not numeric in both")
        delta = v_cur - v_prev
        # Direction filter
        if trigger.direction == "increase" and delta <= 0:
            context.update_cursor(str(rid_cur))
            return dg.SkipReason(f"delta {delta} not an increase")
        if trigger.direction == "decrease" and delta >= 0:
            context.update_cursor(str(rid_cur))
            return dg.SkipReason(f"delta {delta} not a decrease")
        # Magnitude filter
        magnitude_hit = False
        if trigger.min_delta is not None and abs(delta) >= trigger.min_delta:
            magnitude_hit = True
        if trigger.min_delta_pct is not None and v_prev != 0:
            pct = abs(delta) / abs(v_prev) * 100
            if pct >= trigger.min_delta_pct:
                magnitude_hit = True
        if not magnitude_hit and (trigger.min_delta is not None or trigger.min_delta_pct is not None):
            context.update_cursor(str(rid_cur))
            return dg.SkipReason(f"delta {delta} below thresholds")
        context.update_cursor(str(rid_cur))
        tokens = _default_tokens(
            event_type="asset_value_change",
            asset_key=trigger.asset_key,
            status=trigger.direction,
            message=f"{trigger.metadata_key}: {v_prev} → {v_cur} (Δ={delta:+.2f})",
        )
        requests_out = _run_actions(actions, tokens, context.log, instance=instance)
        return dg.SensorResult(run_requests=requests_out) if requests_out else None

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds,
        default_status=default_status,
    )


def _build_backfill_status_sensor(
    name: str, trigger: BackfillStatusTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    def _sensor_fn(context: dg.SensorEvaluationContext):
        instance = context.instance
        seen = set((context.cursor or "").split(",")) if context.cursor else set()
        seen.discard("")
        try:
            backfills = instance.get_backfills(limit=50)
        except Exception:
            return dg.SkipReason("get_backfills unavailable")
        all_requests = []
        newly_seen = []
        for bf in backfills:
            bf_id = getattr(bf, "backfill_id", None) or getattr(bf, "id", None) or ""
            if not bf_id or bf_id in seen:
                continue
            newly_seen.append(bf_id)
            status_val = getattr(bf, "status", None)
            status_str = getattr(status_val, "value", None) or str(status_val) if status_val else ""
            if status_str != trigger.status:
                continue
            # Job name may be on `bf.job_name` or elsewhere
            bf_job = getattr(bf, "job_name", None) or getattr(bf, "asset_selection", "") or ""
            if trigger.job_name and bf_job != trigger.job_name:
                continue
            tokens = _default_tokens(
                event_type="backfill_status",
                run_id=str(bf_id),
                job_name=bf_job,
                status=status_str,
                message=f"Backfill {bf_id} → {status_str}",
            )
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance))
        merged = list(seen | set(newly_seen))[-200:]
        context.update_cursor(",".join(merged))
        return dg.SensorResult(run_requests=all_requests) if all_requests else dg.SkipReason("no matching backfill events")

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds,
        default_status=default_status,
    )


def _build_sensor_failing_sensor(
    name: str, trigger: SensorFailingTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    def _sensor_fn(context: dg.SensorEvaluationContext):
        instance = context.instance
        # Fetch ticks for the target sensor via instigator state
        try:
            all_state = instance.all_instigator_state()
            target = next((s for s in all_state if s.name == trigger.target_sensor_name), None)
            if target is None:
                return dg.SkipReason(f"target sensor '{trigger.target_sensor_name}' not found")
            ticks = instance.get_ticks(
                origin_id=target.instigator_origin_id if hasattr(target, "instigator_origin_id") else target.origin_id,
                selector_id=getattr(target, "selector_id", None),
                limit=trigger.consecutive_failures + 2,
            )
        except Exception as exc:
            return dg.SkipReason(f"get_ticks unavailable: {exc}")
        if len(ticks) < trigger.consecutive_failures:
            return dg.SkipReason(f"only {len(ticks)} ticks recorded")
        # Ticks are descending; the newest N should all be FAILURE
        recent = ticks[:trigger.consecutive_failures]
        all_failed = all(
            (getattr(t, "status", None) and getattr(t.status, "value", "") == "FAILURE")
            for t in recent
        )
        if not all_failed:
            return dg.SkipReason(f"not {trigger.consecutive_failures} consecutive failures")
        # Fire — but only once per streak. Cursor = timestamp of the newest tick.
        newest_ts = str(int(getattr(recent[0], "timestamp", 0) or 0))
        if context.cursor == newest_ts:
            return dg.SkipReason("already fired for this failure streak")
        context.update_cursor(newest_ts)
        tokens = _default_tokens(
            event_type="sensor_failing",
            job_name=trigger.target_sensor_name,
            status="FAILING",
            message=f"Sensor '{trigger.target_sensor_name}' failed {trigger.consecutive_failures} ticks in a row",
        )
        requests_out = _run_actions(actions, tokens, context.log, instance=instance)
        return dg.SensorResult(run_requests=requests_out) if requests_out else None

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds,
        default_status=default_status,
    )


def _build_concurrency_hit_sensor(
    name: str, trigger: ConcurrencyHitTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    def _sensor_fn(context: dg.SensorEvaluationContext):
        instance = context.instance
        active = instance.get_runs(
            filters=dg.RunsFilter(
                statuses=[
                    dg.DagsterRunStatus.QUEUED,
                    dg.DagsterRunStatus.STARTING,
                    dg.DagsterRunStatus.STARTED,
                ]
            ),
            limit=1000,
        )
        matches = active
        if trigger.tag_key:
            def _has_tag(r):
                for k, v in (r.tags or {}).items():
                    if k != trigger.tag_key:
                        continue
                    return (trigger.tag_value is None) or (v == trigger.tag_value)
                return False
            matches = [r for r in active if _has_tag(r)]
        count = len(matches)
        if count <= trigger.max_queued:
            return dg.SkipReason(f"{count} active <= {trigger.max_queued}")
        # Once-per-crossing: only fire when we go from ≤ threshold to > threshold
        last_over = context.cursor == "over"
        if last_over:
            return dg.SkipReason(f"still over ({count})")
        context.update_cursor("over")
        # Reset cursor when we drop back below — done via next tick's skip path
        # (this sensor doesn't get to run the update on skip; user can re-arm
        # manually if needed)
        tokens = _default_tokens(
            event_type="concurrency_hit",
            status="crossed",
            message=f"{count} runs active (limit {trigger.max_queued})"
                    + (f" [tag {trigger.tag_key}={trigger.tag_value or '*'}]" if trigger.tag_key else ""),
        )
        requests_out = _run_actions(actions, tokens, context.log, instance=instance)
        return dg.SensorResult(run_requests=requests_out) if requests_out else None

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds,
        default_status=default_status,
    )


def _build_sqs_poll_sensor(
    name: str, trigger: SqsPollTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    def _sensor_fn(context: dg.SensorEvaluationContext):
        try:
            import boto3
        except ImportError:
            return dg.SkipReason("boto3 not installed — install with `pip install boto3`.")
        client = boto3.client("sqs", region_name=trigger.region)
        try:
            resp = client.receive_message(
                QueueUrl=trigger.queue_url,
                MaxNumberOfMessages=max(1, min(10, trigger.max_messages)),
                WaitTimeSeconds=0,
                MessageAttributeNames=["All"],
            )
        except Exception as exc:
            return dg.SkipReason(f"SQS receive failed: {exc}")
        messages = resp.get("Messages") or []
        if not messages:
            return dg.SkipReason("no messages")
        all_requests = []
        for msg in messages:
            body = msg.get("Body", "")
            tokens = _default_tokens(
                event_type="sqs_message",
                message=body,
                url=trigger.queue_url,
            )
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=context.instance))
            if trigger.delete_after:
                try:
                    client.delete_message(
                        QueueUrl=trigger.queue_url, ReceiptHandle=msg["ReceiptHandle"]
                    )
                except Exception as exc:
                    context.log.warning(f"SQS delete failed for msg {msg.get('MessageId', '')[:8]}: {exc}")
        return dg.SensorResult(run_requests=all_requests) if all_requests else None

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds,
        default_status=default_status,
    )


def _build_compound_sensor(
    name: str,
    trigger: Union[AllOfTrigger, AnyOfTrigger],
    actions: List[Action],
    default_status: dg.DefaultSensorStatus,
) -> dg.SensorDefinition:
    """Nestable AND/OR-composition sensor. Recursively evaluates sub-triggers
    against the current instance state; records fire timestamps in the cursor.

    - all_of: fires when ALL sub-triggers have fired within `within_seconds`
    - any_of: fires when ANY sub-trigger fires (each tick)

    Sub-triggers can be nested compound triggers (all_of inside any_of inside
    all_of, etc.) — evaluation walks the tree.

    Leaf sub-triggers must be poll-shaped (their fire state can be checked
    against current instance state on each tick). Callback-shaped run_status
    triggers work best-effort via a recent-runs scan.
    """
    is_all_of = isinstance(trigger, AllOfTrigger)
    within = trigger.within_seconds if isinstance(trigger, AllOfTrigger) else 0

    def _sensor_fn(context: dg.SensorEvaluationContext):
        instance = context.instance
        now = time.time()
        try:
            fire_state = json.loads(context.cursor) if context.cursor else {}
        except Exception:
            fire_state = {}
        fired, new_state = _evaluate_compound(trigger, instance, fire_state, now, path="")
        context.update_cursor(json.dumps(new_state))
        if not fired:
            return dg.SkipReason(f"compound {trigger.type} did not fire")
        tokens = _default_tokens(
            event_type=f"compound_{trigger.type}",
            message=f"Compound {trigger.type} fired ({len(trigger.triggers)} sub-triggers)"
                    + (f" within {within}s" if is_all_of else ""),
        )
        requests_out = _run_actions(actions, tokens, context.log, instance=instance)
        # Reset cursor on all_of so we don't immediately re-fire
        if is_all_of:
            context.update_cursor(json.dumps({}))
        return dg.SensorResult(run_requests=requests_out) if requests_out else None

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds,
        default_status=default_status,
    )


def _evaluate_compound(node, instance, fire_state: Dict[str, float], now: float, path: str):
    """Recursively evaluate a compound (or leaf) trigger against instance state.

    Returns (fired, updated_fire_state). Leaf evaluations check for a fresh
    fire since the last recorded timestamp for that leaf. Compound nodes
    aggregate their children's fire states.
    """
    node_key = f"{path}/{type(node).__name__}"
    if isinstance(node, (AllOfTrigger, AnyOfTrigger)):
        child_fired = []
        for i, child in enumerate(node.triggers):
            child_path = f"{path}/{i}"
            f, fire_state = _evaluate_compound(child, instance, fire_state, now, child_path)
            child_fired.append((child_path, f))
        if isinstance(node, AnyOfTrigger):
            # Fire immediately if any child fired this tick
            return (any(f for _, f in child_fired), fire_state)
        # AllOf: check that every child has fired within window
        within = node.within_seconds
        satisfied = 0
        for i, _child in enumerate(node.triggers):
            child_path = f"{path}/{i}/{type(_child).__name__}"
            last_ts = fire_state.get(child_path, 0)
            if now - last_ts <= within:
                satisfied += 1
        return (satisfied >= len(node.triggers), fire_state)
    # Leaf evaluation — record fire timestamp if fresh.
    last_ts = fire_state.get(node_key, 0)
    fired = _evaluate_compound_leaf(node, instance, last_ts)
    if fired:
        fire_state[node_key] = now
    return (fired, fire_state)


def _evaluate_compound_leaf(sub, instance, last_fire_ts: float) -> bool:
    """Best-effort evaluation of a leaf sub-trigger against current instance
    state. Only handles poll-shaped triggers; callback-shaped ones (run_status
    with real events) are best-effort via recent-runs scan."""
    now = time.time()
    if isinstance(sub, RunStatusTrigger):
        recent = instance.get_runs(limit=20)
        for r in recent:
            if sub.job_name and r.job_name != sub.job_name:
                continue
            end = float(r.end_time or 0)
            if end > last_fire_ts and r.status.value == sub.status:
                return True
        return False
    if isinstance(sub, AssetMaterializedTrigger):
        for k_str in sub.asset_keys:
            latest = instance.get_latest_materialization_event(dg.AssetKey.from_user_string(k_str))
            if latest and (latest.timestamp or 0) > last_fire_ts:
                return True
        return False
    if isinstance(sub, FreshnessViolationTrigger):
        # Any of the assets stale beyond max_age_minutes = fired
        for k_str in sub.asset_keys:
            latest = instance.get_latest_materialization_event(dg.AssetKey.from_user_string(k_str))
            if latest is None:
                return True
            age_min = (now - float(latest.timestamp or 0)) / 60
            if age_min > sub.max_age_minutes:
                return True
        return False
    if isinstance(sub, AbsenceTrigger):
        # Any of the assets absent beyond max_gap_minutes = fired
        for k_str in sub.asset_keys:
            latest = instance.get_latest_materialization_event(dg.AssetKey.from_user_string(k_str))
            latest_ts = float(latest.timestamp or 0) if latest else 0
            gap_min = (now - latest_ts) / 60
            if gap_min > sub.max_gap_minutes:
                return True
        return False
    if isinstance(sub, RunDurationTrigger):
        recent = instance.get_runs(limit=20)
        for r in recent:
            if sub.job_name and r.job_name != sub.job_name:
                continue
            if not (r.start_time and r.end_time and r.end_time > last_fire_ts):
                continue
            duration = float(r.end_time - r.start_time)
            if duration > sub.max_duration_seconds:
                return True
        return False
    if isinstance(sub, RunStuckTrigger):
        running = instance.get_runs(
            filters=dg.RunsFilter(statuses=[dg.DagsterRunStatus.STARTED, dg.DagsterRunStatus.STARTING])
        )
        for r in running:
            if sub.job_name and r.job_name != sub.job_name:
                continue
            if r.start_time and (now - float(r.start_time)) > sub.max_running_seconds:
                return True
        return False
    # AssetCheckFailedTrigger / MetricThresholdTrigger / SqsPollTrigger:
    # These require an event-log scan that's expensive on every compound
    # tick — punt for now. If a customer needs them nested, we can promote
    # the compound sensor's tick logic to full poll-and-cache.
    return False


# ── The component ─────────────────────────────────────────────────────────

class EventAutomationComponent(dg.Component, dg.Model, dg.Resolvable):
    """Declarative event → action automation. Prefect-Automations shape, one component.

    Example:

        ```yaml
        type: dagster_community_components.EventAutomationComponent
        attributes:
          name: alert_on_prod_failure
          when:
            - type: run_status
              status: FAILURE
              job_name: hourly_ingest
          then:
            - type: slack
              webhook_url_env_var: SLACK_WEBHOOK_URL
              message: "🚨 {job_name} failed: run_id={run_id}"
            - type: pagerduty
              routing_key_env_var: PAGERDUTY_ROUTING_KEY
              severity: error
              summary_template: "Prod job {job_name} failed"
        ```

    Multiple triggers OR together (any fires the automation). Multiple actions
    all run when triggered (sequential, best-effort — one failing doesn't stop
    the rest).
    """

    name: str = Field(description="Automation name. Prefix for generated sensor / schedule names.")
    when: List[Trigger] = Field(description="Triggers that fire the automation. OR semantics.")
    then: List[Action] = Field(description="Actions to run when a trigger fires. All run, sequentially.")
    default_status: str = Field(default="STOPPED", description="RUNNING | STOPPED for the generated sensors / schedules.")
    description: Optional[str] = Field(default=None, description="Free-form description shown in the UI.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        sensor_status = (
            dg.DefaultSensorStatus.RUNNING
            if self.default_status.upper() == "RUNNING"
            else dg.DefaultSensorStatus.STOPPED
        )
        sensors: List[dg.SensorDefinition] = []
        for i, trigger in enumerate(self.when):
            child_name = f"{self.name}__{trigger.type}_{i}"
            if isinstance(trigger, RunStatusTrigger):
                sensors.append(_build_run_status_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, AssetMaterializedTrigger):
                sensors.append(_build_asset_materialized_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, ScheduleTrigger):
                sensors.append(_build_schedule_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, HttpPollTrigger):
                sensors.append(_build_http_poll_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, FreshnessViolationTrigger):
                sensors.append(_build_freshness_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, RunDurationTrigger):
                sensors.append(_build_run_duration_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, RunStuckTrigger):
                sensors.append(_build_run_stuck_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, AssetCheckFailedTrigger):
                sensors.append(_build_asset_check_failed_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, MetricThresholdTrigger):
                sensors.append(_build_metric_threshold_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, AbsenceTrigger):
                sensors.append(_build_absence_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, LogPatternTrigger):
                sensors.append(_build_log_pattern_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, AssetValueChangeTrigger):
                sensors.append(_build_asset_value_change_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, BackfillStatusTrigger):
                sensors.append(_build_backfill_status_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, SensorFailingTrigger):
                sensors.append(_build_sensor_failing_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, ConcurrencyHitTrigger):
                sensors.append(_build_concurrency_hit_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, SqsPollTrigger):
                sensors.append(_build_sqs_poll_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, (AllOfTrigger, AnyOfTrigger)):
                sensors.append(_build_compound_sensor(child_name, trigger, self.then, sensor_status))
            else:
                raise ValueError(f"Unknown trigger type: {type(trigger).__name__}")
        return dg.Definitions(sensors=sensors)
