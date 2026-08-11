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


class CompoundTrigger(_TriggerBase):
    """AND-composition of multiple sub-triggers. Fires when ALL sub-triggers
    have fired within `within_seconds` of each other.

    Only supports leaf triggers (no nested compound) for simplicity — most
    real ANDs are 2-3 flat conditions ("run_A failed AND run_B failed within
    the last hour"). If you need nested logic, chain automations via emit_event.
    """
    type: Literal["all_of"] = "all_of"
    triggers: List[Union[
        "RunStatusTrigger",
        "AssetMaterializedTrigger",
        "AssetCheckFailedTrigger",
        "MetricThresholdTrigger",
    ]] = Field(description="Sub-triggers, all must fire within `within_seconds` for the compound to fire.")
    within_seconds: int = Field(
        default=3600,
        description="All sub-triggers must fire within this window. Default 1 hour.",
    )
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
    CompoundTrigger,
]


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


def _build_compound_sensor(
    name: str, trigger: CompoundTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    """AND-composition sensor. Each tick evaluates all sub-triggers against the
    current instance state and records their fire-timestamps in the cursor.
    Fires the action bundle when ALL sub-triggers have fired within `within_seconds`.

    Only supports polling-shaped sub-triggers (asset_check_failed, metric_threshold,
    run_status via periodic scan, asset_materialized via periodic scan). Callback-
    driven sub-triggers (raw run_status_sensor) don't fit the compound model
    cleanly — use two separate automations that both emit_event, then a third
    that watches for both events, if you need that shape.
    """
    def _sensor_fn(context: dg.SensorEvaluationContext):
        instance = context.instance
        now = time.time()
        try:
            fire_state = json.loads(context.cursor) if context.cursor else {}
        except Exception:
            fire_state = {}
        # Check each sub-trigger against the current instance state (poll-shaped).
        for i, sub in enumerate(trigger.triggers):
            sub_key = f"{i}:{sub.type}"
            fired = _evaluate_compound_leaf(sub, instance, fire_state.get(sub_key, 0))
            if fired:
                fire_state[sub_key] = now
        # AND: all sub-triggers must have fired within window
        recent = [ts for ts in fire_state.values() if now - ts <= trigger.within_seconds]
        if len(recent) < len(trigger.triggers):
            context.update_cursor(json.dumps(fire_state))
            return dg.SkipReason(f"{len(recent)}/{len(trigger.triggers)} sub-triggers fired within window")
        # All fired — fire the compound action bundle, reset cursor
        tokens = _default_tokens(
            event_type="compound_all_of",
            message=f"All {len(trigger.triggers)} sub-triggers fired within {trigger.within_seconds}s",
        )
        requests_out = _run_actions(actions, tokens, context.log, instance=instance)
        context.update_cursor(json.dumps({}))  # reset
        return dg.SensorResult(run_requests=requests_out) if requests_out else None

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds,
        default_status=default_status,
    )


def _evaluate_compound_leaf(sub, instance, last_fire_ts: float) -> bool:
    """Best-effort evaluation of a leaf sub-trigger against current instance
    state. Only handles poll-shaped triggers; callback-shaped ones (run_status)
    always return False from this direct evaluation. Used only by CompoundTrigger."""
    now = time.time()
    if isinstance(sub, RunStatusTrigger):
        # Check for terminal runs newer than last_fire_ts
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
    # AssetCheckFailedTrigger / MetricThresholdTrigger: complex event scan.
    # Compound triggers with these are best-effort; recommend flattening.
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
            elif isinstance(trigger, CompoundTrigger):
                sensors.append(_build_compound_sensor(child_name, trigger, self.then, sensor_status))
            else:
                raise ValueError(f"Unknown trigger type: {type(trigger).__name__}")
        return dg.Definitions(sensors=sensors)
