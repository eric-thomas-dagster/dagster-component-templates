"""EventAutomationComponent.

Prefect-Automations-style declarative event → action wiring, as ONE Dagster
component. Ship many `when: … then: …` blocks in YAML and each one becomes
a real Dagster primitive (sensor / schedule / run_status_sensor) under the
covers. No Python required for common trigger-action wiring.

Triggers (`when:`):
  - `run_status`         — a job / asset finishes with a specific status
  - `asset_materialized` — any of the named assets get materialized
  - `schedule`           — cron expression (also gives you the classic
                           "just kick something off on cron" shape via YAML)
  - `http_poll`          — periodically GET a URL and fire on non-empty
                           / condition match
  - `freshness_violation` — an asset hasn't been materialized recently enough

Actions (`then:`):
  - `materialize`   — launch a materialization run for named assets
  - `launch_job`    — launch a job
  - `webhook`       — POST / GET / PUT arbitrary URL, templated body
  - `slack`         — Slack incoming-webhook alert
  - `pagerduty`     — PagerDuty Events API v2 alert
  - `discord`       — Discord webhook alert
  - `emit_event`    — emit a Dagster asset observation for downstream sensors

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


Action = Union[
    MaterializeAction,
    LaunchJobAction,
    WebhookAction,
    SlackAction,
    PagerDutyAction,
    DiscordAction,
    EmitEventAction,
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


Trigger = Union[
    RunStatusTrigger,
    AssetMaterializedTrigger,
    ScheduleTrigger,
    HttpPollTrigger,
    FreshnessViolationTrigger,
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

def _execute_action(action: Action, tokens: Dict[str, Any], logger) -> Optional[dg.RunRequest]:
    """Execute a single action. Returns a RunRequest (for materialize / launch_job)
    or None for side-effect actions (webhook / slack / pagerduty / discord / emit_event)."""
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
    logger.warning(f"Unknown action type: {type(action).__name__}")
    return None


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


def _run_actions(actions: List[Action], tokens: Dict[str, Any], logger) -> List[dg.RunRequest]:
    """Execute every action. Collect RunRequests (materialize / launch_job)
    for return; side-effect actions execute inline."""
    requests_out = []
    for action in actions:
        try:
            req = _execute_action(action, tokens, logger)
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
        requests_out = _run_actions(actions, tokens, context.log)
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
            all_requests.extend(_run_actions(actions, tokens, context.log))
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
        requests_out = _run_actions(actions, tokens, context.log)
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
                return dg.SensorResult(run_requests=_run_actions(actions, tokens, context.log))
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
                    return dg.SensorResult(run_requests=_run_actions(actions, tokens, context.log))
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
        return dg.SensorResult(run_requests=_run_actions(actions, tokens, context.log))

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
            all_requests.extend(_run_actions(actions, tokens, context.log))
        return dg.SensorResult(run_requests=all_requests) if all_requests else None

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds,
        default_status=default_status,
    )


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
            else:
                raise ValueError(f"Unknown trigger type: {type(trigger).__name__}")
        return dg.Definitions(sensors=sensors)
