"""EventAutomationComponent — 35 declarative triggers → 27 actions in one YAML component.

Prefect-Automations analog on top of real Dagster sensors. Full trigger + action
catalog, compound AND/OR semantics, and recipes are in the README:
https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/sensors/event_automation/README.md
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import dagster as dg
from pydantic import Field


# ── Action models ──────────────────────────────────────────────────────────

class _ActionBase(dg.Model):
    """Base for all actions — every action needs a `type` discriminator."""


class MaterializeAction(_ActionBase):
    type: Literal["materialize"] = "materialize"
    asset_keys: List[str] = Field(description="Asset keys to materialize.")
    partition_key: Optional[Union[str, Dict[str, str]]] = Field(
        default=None,
        description=(
            "Optional partition to materialize. String form for single-dim "
            "partitions (e.g. '2024-01-15') or a template like "
            "'{partition_key}' to pull from the triggering event. Dict form "
            "for MultiPartitionsDefinition — one entry per dimension (e.g. "
            "{date: '{partition_date}', region: '{partition_region}'}). "
            "Values run through template rendering."
        ),
    )


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


# ── Tier 1 ops / self-healing actions ─────────────────────────────────────
# These wrap Dagster+ GraphQL mutations or the instance API. Verified live
# against Dagster+ prod GraphQL introspection (see 42-mutation catalog).


class ReloadCodeLocationAction(_ActionBase):
    """Dagster+ only. Reload a specific code location or the whole workspace.

    Pair with `code_location_status: ERROR` for auto-recovery — a bad deploy
    that broke the location gets automatically reloaded (which retries load
    and can recover if the root cause was a transient dep-install issue).
    """
    type: Literal["reload_code_location"] = "reload_code_location"
    location_name: Optional[str] = Field(
        default=None,
        description=(
            "Location to reload. Templated (use '{location_name}' to pull from "
            "trigger tokens). If unset, reloads the whole workspace."
        ),
    )
    org_env_var: str = Field(default="DAGSTER_CLOUD_ORGANIZATION")
    token_env_var: str = Field(default="DAGSTER_CLOUD_API_TOKEN")
    deployment: str = Field(default="prod")


class RefreshDefsStateAction(_ActionBase):
    """Dagster+ only. Refresh definitions state for a code location.

    Use with StateBackedComponent-shaped assets (planned catalogs, LLM plans,
    dynamic asset graphs) that cache derived state — trigger a refresh on
    schedule or when upstream state signals staleness.
    """
    type: Literal["refresh_defs_state"] = "refresh_defs_state"
    location_name: str = Field(description="Code location to refresh. Templated.")
    defs_state_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional specific state key. If set, calls `refreshComponentState`; "
            "if unset, calls `refreshDefsState` (refresh everything in the location)."
        ),
    )
    org_env_var: str = Field(default="DAGSTER_CLOUD_ORGANIZATION")
    token_env_var: str = Field(default="DAGSTER_CLOUD_API_TOKEN")
    deployment: str = Field(default="prod")


class SetConcurrencyLimitAction(_ActionBase):
    """Adjust a concurrency pool limit dynamically.

    Works on OSS (uses `instance.event_log_storage.set_concurrency_slots`).
    Common use: scheduled scaling (bump warehouse pool during business hours,
    drop overnight) or reactive (concurrency_hit fires → auto-bump).
    """
    type: Literal["set_concurrency_limit"] = "set_concurrency_limit"
    concurrency_key: str = Field(description="Pool name. Templated.")
    limit: int = Field(description="New slot count (integer).")


class FreeConcurrencySlotsAction(_ActionBase):
    """Release concurrency slots stuck by a crashed run/step.

    Works on OSS (uses `instance.event_log_storage.free_concurrency_slots_for_run`
    or `.free_concurrency_slots`). Pair with `run_stuck` trigger for auto-recovery.
    """
    type: Literal["free_concurrency_slots"] = "free_concurrency_slots"
    run_id: str = Field(
        description="Run whose slots to release. Templated (use '{run_id}' from trigger).",
    )
    step_key: Optional[str] = Field(
        default=None, description="Optional specific step key to release. Templated.",
    )


class SetAutoMaterializePausedAction(_ActionBase):
    """Dagster+ only. Globally pause / unpause AutoMaterialize (Declarative Automation).

    Use for maintenance windows, incident freezes, cost-spike breakers.
    """
    type: Literal["set_auto_materialize_paused"] = "set_auto_materialize_paused"
    paused: bool = Field(description="True = pause, False = unpause.")
    org_env_var: str = Field(default="DAGSTER_CLOUD_ORGANIZATION")
    token_env_var: str = Field(default="DAGSTER_CLOUD_API_TOKEN")
    deployment: str = Field(default="prod")


class MuteAlertPolicyAction(_ActionBase):
    """Dagster+ only. Temporarily mute a Dagster+ Alerts policy.

    Pair with `maintenance_windows` throttle or `schedule` trigger to silence
    noisy alerts during planned quiet periods — cross-cuts across Dagster+
    Alerts + event_automation for full-suite silencing.
    """
    type: Literal["mute_alert_policy"] = "mute_alert_policy"
    alert_policy_id: str = Field(description="Dagster+ alert policy ID.")
    mute_for_seconds: int = Field(description="Duration to mute (seconds).")
    org_env_var: str = Field(default="DAGSTER_CLOUD_ORGANIZATION")
    token_env_var: str = Field(default="DAGSTER_CLOUD_API_TOKEN")
    deployment: str = Field(default="prod")


class ResumeBackfillAction(_ActionBase):
    """Resume a paused partition backfill.

    Pair with `backfill_status: FAILED` + a resolver trigger for auto-retry loops.
    """
    type: Literal["resume_backfill"] = "resume_backfill"
    backfill_id: str = Field(description="Backfill ID. Templated (use '{run_id}' — backfill_status trigger emits the backfill id there).")
    org_env_var: str = Field(default="DAGSTER_CLOUD_ORGANIZATION")
    token_env_var: str = Field(default="DAGSTER_CLOUD_API_TOKEN")
    deployment: str = Field(default="prod")


class CancelBackfillAction(_ActionBase):
    """Cancel a partition backfill in progress."""
    type: Literal["cancel_backfill"] = "cancel_backfill"
    backfill_id: str = Field(description="Backfill ID. Templated.")
    org_env_var: str = Field(default="DAGSTER_CLOUD_ORGANIZATION")
    token_env_var: str = Field(default="DAGSTER_CLOUD_API_TOKEN")
    deployment: str = Field(default="prod")


class ReexecuteBackfillAction(_ActionBase):
    """Dagster+ only. Re-execute a failed partition backfill (`reexecutePartitionBackfill`)."""
    type: Literal["reexecute_backfill"] = "reexecute_backfill"
    backfill_id: str = Field(description="Parent backfill ID. Templated.")
    from_failure: bool = Field(
        default=True,
        description="True = re-run only failed partitions; False = re-run everything.",
    )
    org_env_var: str = Field(default="DAGSTER_CLOUD_ORGANIZATION")
    token_env_var: str = Field(default="DAGSTER_CLOUD_API_TOKEN")
    deployment: str = Field(default="prod")


class AddDynamicPartitionAction(_ActionBase):
    """Add a new dynamic partition to a DynamicPartitionsDefinition.

    Works on OSS via `instance.add_dynamic_partitions()`. Pair with any
    trigger emitting a natural partition key (S3 file drop, Kafka message,
    HTTP poll response) to programmatically register the partition without
    a separate sensor.
    """
    type: Literal["add_dynamic_partition"] = "add_dynamic_partition"
    partitions_def_name: str = Field(description="DynamicPartitionsDefinition name.")
    partition_key: str = Field(
        description="Partition key to add. Templated ('{partition_key}' or a custom token).",
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
    # Tier 1 ops / self-healing
    ReloadCodeLocationAction,
    RefreshDefsStateAction,
    SetConcurrencyLimitAction,
    FreeConcurrencySlotsAction,
    SetAutoMaterializePausedAction,
    MuteAlertPolicyAction,
    ResumeBackfillAction,
    CancelBackfillAction,
    ReexecuteBackfillAction,
    AddDynamicPartitionAction,
]


# ── Trigger models ─────────────────────────────────────────────────────────

class MaintenanceWindow(dg.Model):
    """A scheduled quiet period. During this window, matching fires are suppressed."""
    from_ts: str = Field(description="ISO8601 start (e.g. '2024-01-15T02:00:00Z').")
    to_ts: str = Field(description="ISO8601 end.")
    reason: Optional[str] = Field(default=None, description="Free-form reason shown in logs.")


class EscalationTier(dg.Model):
    """One tier of an escalation ladder. When fire count crosses `after_fires`,
    ONLY the actions at these indices in the `then:` list fire."""
    after_fires: int = Field(description="Fire count threshold (0 = first fire).")
    action_indices: List[int] = Field(description="Indices into the automation's `then:` list to execute.")


class ThrottleConfig(dg.Model):
    """Rate-limit + suppression rules for a trigger.

    Keeps sensors from spamming downstream endpoints when the same event
    fires repeatedly. Attach `throttle:` to any trigger; the trigger's
    fire path checks it before running actions.

    Strategies:
      - `silence` (default) — drop fires within cooldown / over max_per_hour
      - `summarize` — buffer fires; flush a single "N events, first at X,
                     last at Y" alert after `flush_after_seconds`
      - `first_last` — fire first + last of a burst, drop middle
      - `llm` — ask an LLM whether to fire, given the current event +
               recent event history. Cached briefly to avoid LLM spam.

    Scoping via `dedup_key_template` — each rendered dedup key gets its own
    throttle state. Default: throttle applies globally per trigger.
    """
    min_seconds_between_fires: Optional[int] = Field(
        default=None,
        description="Cooldown: drop fires within this many seconds of the last fire.",
    )
    max_per_hour: Optional[int] = Field(
        default=None,
        description="Rolling-window cap: drop fires that would exceed N per hour.",
    )
    dedup_key_template: Optional[str] = Field(
        default=None,
        description=(
            "Template for the dedup key — throttle state is per-key. "
            "Supports the standard template tokens. Default: whole trigger."
        ),
    )
    strategy: str = Field(
        default="silence",
        description="silence | summarize | first_last | llm | escalate | auto_resolve",
    )
    flush_after_seconds: int = Field(
        default=600,
        description="For summarize / first_last: how long to accumulate before flushing.",
    )
    # LLM strategy fields (only used when strategy=llm)
    llm_provider: str = Field(default="openai", description="openai | anthropic")
    llm_model: str = Field(default="gpt-4o-mini")
    llm_api_key_env_var: str = Field(default="OPENAI_API_KEY")
    llm_prompt_template: str = Field(
        default=(
            "You are an on-call paging engineer. Given this incoming alert and "
            "recent alert history for the same dedup key, decide whether to page.\n\n"
            "Current alert:\n{message}\n\nRecent alerts (last hour):\n{recent}\n\n"
            "Answer strictly with 'YES: <one-line reason>' or 'NO: <one-line reason>'."
        ),
        description="Prompt template. {message} + {recent} are substituted with alert info.",
    )
    llm_decision_cache_seconds: int = Field(
        default=60,
        description="Cache the LLM's YES/NO decision for this dedup key for this many seconds.",
    )
    # Business-hours gating (single window, day-of-week filter optional)
    business_hours_only: Optional[str] = Field(
        default=None,
        description=(
            "Only fire during this daily window. Format: 'HH:MM-HH:MM tz' or "
            "'HH:MM-HH:MM tz mon,tue,wed,thu,fri'. Timezone must be a valid "
            "IANA name (e.g. America/New_York, UTC)."
        ),
    )
    # Maintenance windows — list of scheduled quiet periods
    maintenance_windows: Optional[List[MaintenanceWindow]] = Field(
        default=None,
        description="Scheduled quiet periods. Fires within any listed window are suppressed.",
    )
    # Correlation suppression — drop this trigger if any of these sensor names
    # fired within `correlation_within_seconds`. Uses cross-sensor state.
    correlation_suppress_sensors: Optional[List[str]] = Field(
        default=None,
        description=(
            "Sensor names whose recent fires suppress THIS trigger. Substring match "
            "on the emitted sensor name (e.g. 'daemon_heartbeat' matches "
            "'ops_alerts__daemon_heartbeat_0'). Use for root-cause suppression."
        ),
    )
    correlation_within_seconds: int = Field(
        default=300,
        description="Correlation lookback window.",
    )
    # Escalation ladder — strategy=escalate. Each tier picks WHICH actions fire.
    escalation_ladder: Optional[List[EscalationTier]] = Field(
        default=None,
        description="For strategy=escalate: fire count → which action indices execute.",
    )
    # Auto-resolve — strategy=auto_resolve. On subsequent tick where the
    # underlying condition CLEARS, emit a paired 'resolved' event with tokens
    # {event_type=..._resolved, message='Resolved after N sec'}.
    auto_resolve_message: str = Field(
        default="✅ Resolved after {duration_seconds}s ({fire_count} fires)",
        description="Message template for the paired resolve fire.",
    )


class _TriggerBase(dg.Model):
    """Base for all triggers — every trigger needs a `type` discriminator."""
    # Optional per-trigger throttle. All triggers inherit this — it's opt-in
    # via presence of `throttle:` in the YAML.
    throttle: Optional[ThrottleConfig] = Field(
        default=None,
        description="Optional rate-limit + suppression rules. See ThrottleConfig.",
    )


class _RunFilterMixin(dg.Model):
    """Adds glob + tag filters to run-based triggers.

    Composed alongside `_TriggerBase`; run-based sensors call
    `_run_matches_filters()` at event time to apply these on top of the
    existing `job_name` exact-match.
    """
    job_name_pattern: Optional[str] = Field(
        default=None,
        description=(
            "Optional fnmatch glob on the run's job_name (e.g. 'prod_*'). "
            "Composes with `job_name` — both must match if both are set."
        ),
    )
    run_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Optional run-tag filter. Every listed key=value must be present "
            "in the run's tags (e.g. {priority: P0, team: data-platform})."
        ),
    )


class MonitoredJob(dg.Model):
    """One entry in a `monitored_jobs` list — references a job in another
    code location. The default repository name matches the `create-dagster`
    convention (`__repository__`) so most users only need to supply
    `location` and `job`."""
    location: str = Field(description="Code location name.")
    job: str = Field(description="Job name.")
    repository: str = Field(
        default="__repository__",
        description="Repository name. Defaults to __repository__ (create-dagster convention).",
    )


class _MonitoredJobsMixin(dg.Model):
    """Adds cross-code-location targeting to triggers that use
    `@run_status_sensor`. Without either field, the sensor watches jobs in
    its OWN code location only. With them, the sensor subscribes to run
    events from the named locations — the natural pattern is a dedicated
    'alerts' code location that observes every prod location's runs."""
    monitored_locations: Optional[List[str]] = Field(
        default=None,
        description=(
            "Watch every job in these code locations. E.g. ['prod_ingest', "
            "'prod_analytics']. Composes with `monitored_jobs` (union)."
        ),
    )
    monitored_jobs: Optional[List[MonitoredJob]] = Field(
        default=None,
        description=(
            "Watch specific jobs by (location, repository, job). Use when "
            "you want a narrower target than a whole location."
        ),
    )


def _build_monitored_jobs_arg(
    monitored_locations: Optional[List[str]],
    monitored_jobs: Optional[List[MonitoredJob]],
):
    """Convert component fields to the list expected by `@run_status_sensor`'s
    `monitored_jobs` param. Returns None when nothing is set (sensor watches
    its own code location by default)."""
    if not monitored_locations and not monitored_jobs:
        return None
    from dagster import CodeLocationSelector, JobSelector
    result: List[Any] = []
    for loc in monitored_locations or []:
        result.append(CodeLocationSelector(location_name=loc))
    for m in monitored_jobs or []:
        result.append(
            JobSelector(
                location_name=m.location,
                repository_name=m.repository,
                job_name=m.job,
            )
        )
    return result or None


class RunStatusTrigger(_MonitoredJobsMixin, _RunFilterMixin, _TriggerBase):
    type: Literal["run_status"] = "run_status"
    status: str = Field(description="SUCCESS | FAILURE | CANCELED | STARTED")
    job_name: Optional[str] = Field(
        default=None,
        description="Filter to a specific job. None = all jobs.",
    )


class AssetMaterializedTrigger(_TriggerBase):
    type: Literal["asset_materialized"] = "asset_materialized"
    asset_keys: Union[List[str], str] = Field(
        description=(
            "Asset keys to watch. Accepts a list of keys or a Dagster "
            "asset-selection string (`group:X`, `tag:foo=bar`, `kind:Y`, "
            "`is:external`, boolean composition, or fnmatch glob like "
            "`marts/*`). Resolved against sibling assets at build_defs time."
        )
    )


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
    asset_keys: Union[List[str], str] = Field(
        description=(
            "Asset keys to check. Accepts a list of keys or a Dagster "
            "asset-selection string (`group:X`, `tag:foo=bar`, `kind:Y`, "
            "boolean composition, or `marts/*` glob)."
        )
    )
    max_age_minutes: int = Field(description="Fail if the asset's latest materialization is older than this.")
    minimum_interval_seconds: int = Field(default=300)


class RunDurationTrigger(_MonitoredJobsMixin, _RunFilterMixin, _TriggerBase):
    """Fires when a run finishes and its duration exceeded a threshold."""
    type: Literal["run_duration"] = "run_duration"
    max_duration_seconds: int = Field(description="Fire when total run duration > this.")
    job_name: Optional[str] = Field(default=None, description="Filter to a specific job.")
    on_status: str = Field(
        default="ANY",
        description="ANY (default) | SUCCESS | FAILURE — only fire when the run ended with this status.",
    )


class RunStuckTrigger(_RunFilterMixin, _TriggerBase):
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
    asset_keys: Optional[Union[List[str], str]] = Field(
        default=None,
        description=(
            "Optional asset key filter. None = any asset. Accepts a list or "
            "a Dagster asset-selection string / fnmatch glob."
        ),
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
    asset_keys: Union[List[str], str] = Field(
        description=(
            "Asset keys expected to have materialized recently. Accepts a "
            "list or a Dagster asset-selection string / fnmatch glob."
        )
    )
    max_gap_minutes: int = Field(description="Fire if no materialization in this many minutes.")
    minimum_interval_seconds: int = Field(default=300)


class LogPatternTrigger(_TriggerBase):
    """Fires when a recent run's log line matches a regex.

    Watches finished runs (SUCCESS + FAILURE); scans logs for `pattern`.
    Cursor tracks last-checked run ids so we don't rescan the same run twice.

    `sources` controls which log streams get scanned:
      - `events` (default) — dagster event log entries (context.log.info/warning/
        error calls inside ops, framework messages like STEP_FAILURE + tracebacks)
      - `stdout` / `stderr` — raw compute log manager output (K8s / ECS / Docker
        container stdout+stderr from your ops). Catches OOMKilled / kernel panics
        / oomkill traces that never made it to the dagster logger. Works against
        whatever compute_log_manager the deployment configures (Dagster+ managed
        for Serverless, S3/GCS/Azure for Hybrid, local files for OSS).
    """
    type: Literal["log_pattern"] = "log_pattern"
    pattern: str = Field(description="Regex pattern to match against log message text.")
    job_name: Optional[str] = Field(default=None, description="Optional job filter.")
    sources: List[str] = Field(
        default=["events"],
        description="Which log streams to scan. events | stdout | stderr (any combination).",
    )
    minimum_interval_seconds: int = Field(default=60)


class AssetObservationTrigger(_TriggerBase):
    """Fires when an AssetObservation event is emitted for named asset(s).

    Distinct from `asset_materialized` — observations record signals about an
    asset without producing a new materialization (freshness updates, external
    system state, quality checks). Ideal for reacting to observation-driven
    workflows (e.g., an external_asset that a sensor observes hourly).
    """
    type: Literal["asset_observation"] = "asset_observation"
    asset_keys: Union[List[str], str] = Field(
        description=(
            "Asset keys whose observations trigger. Accepts a list or a "
            "Dagster asset-selection string / fnmatch glob."
        )
    )
    minimum_interval_seconds: int = Field(default=60)


class StepErrorTrigger(_RunFilterMixin, _TriggerBase):
    """Fires when an op step raises an exception (STEP_FAILURE event).

    Distinct from `run_status FAILURE` — catches errors at the step level even
    when the run overall succeeds (retries, hooks, downstream steps that
    recover). Also fires MULTIPLE times per run if multiple steps fail.

    When ops have a `RetryPolicy` (or the job has one), STEP_FAILURE events
    fire on every intermediate attempt. Set `only_final_failures: true` to
    silence intermediate attempts and only fire when Dagster has stopped
    retrying (step is in terminal FAILURE with no further attempts pending).
    """
    type: Literal["step_error"] = "step_error"
    job_name: Optional[str] = Field(default=None, description="Optional job filter.")
    step_key_pattern: Optional[str] = Field(
        default=None,
        description="Optional regex on step key (e.g. '.*etl.*'). None = any step.",
    )
    exception_pattern: Optional[str] = Field(
        default=None,
        description="Optional regex on exception message. None = any exception.",
    )
    only_final_failures: bool = Field(
        default=False,
        description=(
            "When True, filter out failures that will be retried. Checks the "
            "step's current status via instance.get_run_step_stats — only "
            "fires when the step is definitively FAILURE and no retry is "
            "pending. Recommended for ops with a RetryPolicy — prevents "
            "alerting on transient failures that a retry recovers."
        ),
    )
    minimum_interval_seconds: int = Field(default=60)


class MetadataMatchTrigger(_TriggerBase):
    """Fires when a materialization or observation event carries specific metadata.

    Three shapes:
      - `metadata_key` alone → fires when the key is present (any value)
      - `metadata_key` + `equals` → fires when key == equals (string comparison)
      - `metadata_key` + `regex` → fires when str(value) matches regex

    Useful for reacting to categorical metadata (`status=stale`, `env=prod`,
    `severity=high`) that numeric-comparison triggers (`metric_threshold`,
    `asset_value_change`) can't capture.
    """
    type: Literal["metadata_match"] = "metadata_match"
    asset_key: str = Field(description="Asset key to watch.")
    metadata_key: str = Field(description="Metadata key to match on.")
    equals: Optional[str] = Field(default=None, description="Exact-match string value.")
    regex: Optional[str] = Field(default=None, description="Regex on str(value). Mutually exclusive with equals.")
    include_observations: bool = Field(
        default=True,
        description="True = match on materializations AND observations. False = materializations only.",
    )
    minimum_interval_seconds: int = Field(default=60)


class HookFiredTrigger(_RunFilterMixin, _TriggerBase):
    """Fires when a Dagster hook (@success_hook / @failure_hook) executes.

    Distinct from step_error — hooks capture success paths too, and are per-op
    not per-step. Filters by hook_name (regex) and status.
    """
    type: Literal["hook_fired"] = "hook_fired"
    hook_name_pattern: Optional[str] = Field(default=None, description="Regex on hook name.")
    on_status: str = Field(default="ANY", description="ANY | SUCCESS | FAILURE — which hook completions fire.")
    job_name: Optional[str] = Field(default=None)
    minimum_interval_seconds: int = Field(default=60)


class AssetPartitionMaterializedTrigger(_TriggerBase):
    """Fires when a specific asset PARTITION is materialized.

    Distinct from asset_materialized — filters to a partition key or partition
    key regex (partition family). Common use: "alert only when the 2024-Q1
    partition of revenue lands", or "notify when any Snowflake partition lands
    for the customer_events asset".
    """
    type: Literal["asset_partition_materialized"] = "asset_partition_materialized"
    asset_keys: Union[List[str], str] = Field(
        description=(
            "Asset keys to watch. Accepts a list or a Dagster asset-selection "
            "string / fnmatch glob."
        )
    )
    partition_key: Optional[Union[str, Dict[str, str]]] = Field(
        default=None,
        description=(
            "Exact partition match. String form for single-dim partitions "
            "('2024-01-15'). Dict form for MultiPartitionsDefinition — one "
            "entry per dimension, unspecified dims wildcard "
            "(e.g. {region: 'us'} matches every date for region=us)."
        ),
    )
    partition_key_pattern: Optional[str] = Field(default=None, description="Regex on partition key (string form).")
    minimum_interval_seconds: int = Field(default=60)


class RunReexecutionTrigger(_TriggerBase):
    """Fires when a run is re-executed (retry) — audit trail for retry actions.

    Detects runs with `parent_run_id` set (marker that this is a re-execution).
    Optional job_name filter + strategy filter (from_failure / all_steps).
    """
    type: Literal["run_reexecution"] = "run_reexecution"
    job_name: Optional[str] = Field(default=None)
    strategy: Optional[str] = Field(default=None, description="Filter to from_failure | all_steps.")
    minimum_interval_seconds: int = Field(default=60)


class AssetWipeTrigger(_TriggerBase):
    """Fires on ASSET_WIPED events — destructive audit signal.

    Someone deleted materialization history for an asset. Rare + important.
    """
    type: Literal["asset_wipe"] = "asset_wipe"
    asset_keys: Optional[Union[List[str], str]] = Field(
        default=None,
        description=(
            "Watch these asset keys. None = any wipe. Accepts a list or a "
            "Dagster asset-selection string / fnmatch glob."
        ),
    )
    minimum_interval_seconds: int = Field(default=60)


class ConfigOverrideTrigger(_TriggerBase):
    """Fires when a run is launched with a non-default config override.

    Change-tracking signal: someone launched with `run_config` that differs from
    the default. Useful for audit + change-control workflows.
    """
    type: Literal["config_override"] = "config_override"
    job_name: Optional[str] = Field(default=None)
    minimum_interval_seconds: int = Field(default=60)


class TagSetTrigger(_TriggerBase):
    """Fires when a run carries a specific tag key/value.

    Useful for audit + routing: `env=prod-hotfix`, `owner=user123`, `priority=P0`.
    """
    type: Literal["tag_set"] = "tag_set"
    tag_key: str = Field(description="Tag key to watch for.")
    tag_value: Optional[str] = Field(default=None, description="Optional exact value. None = any value.")
    tag_value_pattern: Optional[str] = Field(default=None, description="Optional regex on value.")
    on_status: str = Field(default="STARTED", description="Which run status transitions to check.")
    minimum_interval_seconds: int = Field(default=60)


class UnhandledExceptionTrigger(_TriggerBase):
    """Fires on run-level unhandled exceptions distinct from step failures.

    RunFailure with the `failure_reason` marked as unexpected — captures
    infrastructure crashes / process kills that aren't step-level errors.
    """
    type: Literal["unhandled_exception"] = "unhandled_exception"
    job_name: Optional[str] = Field(default=None)
    minimum_interval_seconds: int = Field(default=60)


class AssetCheckSeverityTrigger(_TriggerBase):
    """Fires on asset check evaluations at a specific severity level.

    Variant of asset_check_failed with severity filter — lets you separate
    WARN from ERROR handling in different automations.
    """
    type: Literal["asset_check_severity"] = "asset_check_severity"
    severity: str = Field(description="WARN | ERROR")
    check_names: Optional[List[str]] = Field(default=None)
    asset_keys: Optional[Union[List[str], str]] = Field(
        default=None,
        description=(
            "Optional asset filter. Accepts a list or a Dagster "
            "asset-selection string / fnmatch glob."
        ),
    )
    minimum_interval_seconds: int = Field(default=60)


class OpOutputTrigger(_TriggerBase):
    """Fires when a specific op yields output (STEP_OUTPUT event).

    Fine-grained — most people want asset_materialized instead. Useful for
    non-asset op-based workflows.
    """
    type: Literal["op_output"] = "op_output"
    step_key_pattern: str = Field(description="Regex on step key.")
    output_name: Optional[str] = Field(default=None, description="Optional output name filter.")
    minimum_interval_seconds: int = Field(default=60)


class MaterializationPlannedTrigger(_TriggerBase):
    """Fires on ASSET_MATERIALIZATION_PLANNED events (before materialization).

    Useful for pre-materialization side-effects (warm caches, pre-provision
    downstream resources, notify observers before the write lands).
    """
    type: Literal["materialization_planned"] = "materialization_planned"
    asset_keys: Union[List[str], str] = Field(
        description=(
            "Asset keys to watch. Accepts a list or a Dagster asset-selection "
            "string / fnmatch glob."
        )
    )
    minimum_interval_seconds: int = Field(default=60)


class AssetCheckStartedTrigger(_TriggerBase):
    """Fires on ASSET_CHECK_EVALUATION_STARTED events.

    Mirror of asset_check_failed but on start — useful for "check is running
    slowly" scenarios (pair with an all_of + timer to alert if not completed).
    """
    type: Literal["asset_check_started"] = "asset_check_started"
    check_names: Optional[List[str]] = Field(default=None)
    asset_keys: Optional[Union[List[str], str]] = Field(
        default=None,
        description=(
            "Optional asset filter. Accepts a list or a Dagster "
            "asset-selection string / fnmatch glob."
        ),
    )
    minimum_interval_seconds: int = Field(default=60)


class InsightsMetricThresholdTrigger(_TriggerBase):
    """Dagster+ only. Fires when a time-window-aggregated Insights metric crosses a threshold.

    Differentiated from `metric_threshold`: `metric_threshold` reads a single
    materialization event's metadata and fires on that per-event value. This
    trigger queries Dagster+ Insights (Victoria Metrics under the hood) for
    a *time-window aggregate* — so you can alert on trend shape rather than
    single-event crossings:

      - "Daily AVG of row_count over the last 7 days dropped below 100"
      - "Weekly SUM of Snowflake credits used exceeded 1000"
      - "Hourly MAX of run duration crossed 300 seconds"

    Also the right entry point for platform-computed metrics that don't exist
    as raw materialization metadata: run counts, credit spend, storage bytes,
    freshness pass %, etc.

    Scope:
      - `asset_selection` — Dagster selection string, resolved server-side by
        `reportingMetricsByAssetSelection`. Covers group / tag / kind /
        asset-key targeting via one field ('group:marts and tag:tier=gold',
        'kind:dbt', 'is:external', 'key:"marts/orders"'). Verified live
        against the Dagster+ prod GraphQL.
      - (default, no scope set) — deployment-wide via
        `reportingMetricsByDeployment`.

    Requires:
      - DAGSTER_CLOUD_ORGANIZATION env var (auto-injected in Dagster+ runtime)
      - DAGSTER_CLOUD_API_TOKEN env var with metrics-read permission
      - Deployment name (defaults to prod)
    """
    type: Literal["insights_metric"] = "insights_metric"
    metric_name: str = Field(description="Dagster+ Insights metric name (custom or built-in).")
    comparison: str = Field(description="gt | gte | lt | lte | eq | neq")
    threshold: float = Field()
    granularity: str = Field(
        default="DAILY",
        description="HOURLY | DAILY | WEEKLY | MONTHLY — the bucket size Victoria Metrics aggregates into.",
    )
    aggregation: str = Field(
        default="SUM",
        description=(
            "SUM | AVERAGE | MIN | MAX — how values within a bucket are combined. "
            "For 'trend shape' use AVERAGE; for 'total this window' use SUM."
        ),
    )
    lookback_hours: int = Field(
        default=24,
        description="How many hours of history to fetch. Tune to your granularity (24 for HOURLY, 168 for DAILY, etc.).",
    )
    asset_selection: Optional[str] = Field(
        default=None,
        description=(
            "Optional Dagster asset-selection string, resolved server-side. "
            "E.g. 'group:marts and tag:tier=gold', 'kind:dbt', 'is:external', "
            "'key:\"marts/orders\"'. Unset = deployment-wide."
        ),
    )
    deployment: str = Field(default="prod")
    org_env_var: str = Field(default="DAGSTER_CLOUD_ORGANIZATION")
    token_env_var: str = Field(default="DAGSTER_CLOUD_API_TOKEN")
    minimum_interval_seconds: int = Field(default=300)


class DagsterPlusAuditTrigger(_TriggerBase):
    """Dagster+ only. Fires on audit-log events matching a filter.

    Dagster+ Alerts does NOT cover audit-log events — this trigger fills the
    gap. Compliance + security workflows can react programmatically: RBAC
    changes → Slack the security team, secret rotations → SIEM webhook,
    deployment changes → Splunk, etc.

    Verified against live Dagster+ GraphQL. Filters push down server-side
    where possible (`event_types`, `user_emails`, `deployment_names`,
    `is_branch_deployment`); regex patterns run client-side after fetch.

    Real audit event types (42 total, grouped):

    - **RBAC / users**: CHANGE_USER_PERMISSIONS, CREATE_SERVICE_USER,
      UPDATE_SERVICE_USER, DELETE_SERVICE_USER, CHANGE_SERVICE_USER_PERMISSIONS
    - **Tokens**: CREATE_USER_TOKEN, REVOKE_USER_TOKEN, CREATE_AGENT_TOKEN,
      REVOKE_AGENT_TOKEN, UPDATE_AGENT_TOKEN_PERMISSIONS,
      CREATE_SERVICE_TOKEN, REVOKE_SERVICE_TOKEN, PUT_REVOKE_TOKEN
    - **Secrets**: CREATE_SECRET, UPDATE_SECRET, DELETE_SECRET
    - **Deployments**: CREATE_DEPLOYMENT, DELETE_DEPLOYMENT,
      UPDATE_DEPLOYMENT_SETTINGS
    - **Code locations**: CREATE_CODE_LOCATION, UPDATE_CODE_LOCATION,
      DELETE_CODE_LOCATION, REDEPLOY_SERVERLESS_AGENT
    - **Automation**: UPDATE_SCHEDULE, UPDATE_SENSOR, SET_AUTO_MATERIALIZE_PAUSED,
      LAUNCH_RUN, LAUNCH_BACKFILL
    - **Alerts (meta!)**: MODIFY_ALERT_POLICIES, SET_ALERT_POLICY_MUTE_UNTIL
    - **Org**: CREATE_ORGANIZATION_SUBDOMAIN, DELETE_ORGANIZATION_SUBDOMAIN,
      UPDATE_SUBSCRIPTION_PLAN, UPDATE_SUBSCRIPTION_TYPE
    - **Auth**: LOG_IN, IFRAME_LOG_IN

    Prefer `event_types` (server-side, exact enum) over `event_type_pattern`
    (client-side regex) when your target set is fixed.
    """
    type: Literal["dagster_plus_audit"] = "dagster_plus_audit"
    # Server-side push-down filters (verified against live GraphQL schema)
    event_types: Optional[List[str]] = Field(
        default=None,
        description=(
            "Server-side filter: list of AuditLogEventType enum values to include "
            "(e.g. ['CREATE_SECRET', 'UPDATE_SECRET', 'DELETE_SECRET']). "
            "Prefer this over event_type_pattern when your target set is fixed."
        ),
    )
    user_emails: Optional[List[str]] = Field(
        default=None,
        description="Server-side filter: list of actor emails to include.",
    )
    deployment_names: Optional[List[str]] = Field(
        default=None,
        description="Server-side filter: which deployments to include (e.g. ['prod']).",
    )
    is_branch_deployment: Optional[bool] = Field(
        default=None,
        description="Server-side filter: True = branch deployments only, False = main only, None = both.",
    )
    # Client-side regex filters (kept for pattern flexibility)
    event_type_pattern: Optional[str] = Field(
        default=None,
        description=(
            "Optional regex on audit event type (e.g. 'permission.*grant', "
            "'secret.*'). Applied client-side AFTER event_types push-down."
        ),
    )
    actor_pattern: Optional[str] = Field(
        default=None,
        description="Optional regex on actor email / user id (client-side).",
    )
    deployment: str = Field(default="prod")
    org_env_var: str = Field(default="DAGSTER_CLOUD_ORGANIZATION")
    token_env_var: str = Field(default="DAGSTER_CLOUD_API_TOKEN")
    minimum_interval_seconds: int = Field(default=300)


class CodeLocationStatusTrigger(_TriggerBase):
    """Fires when a code location enters an unhealthy state.

    States:
      - `ERROR` — the location failed to load (Python import error, dependency
        conflict, resource init failure)
      - `LOADING` — location is stuck loading for > `max_seconds_loading`
      - `TIMED_OUT` — location load exceeded the deployment's timeout

    Once-per-transition semantics via cursor. Reliable signal for `dg plus
    deploy` failures, dependency drift, or long-tail load times.
    """
    type: Literal["code_location_status"] = "code_location_status"
    on_status: str = Field(description="ERROR | LOADING | TIMED_OUT | UNHEALTHY (any of ERROR / TIMED_OUT)")
    max_seconds_loading: int = Field(
        default=300,
        description="For on_status=LOADING: fire when a location has been LOADING > this.",
    )
    location_name_pattern: Optional[str] = Field(
        default=None,
        description="Optional regex on location name (e.g. 'prod-*'). None = all locations.",
    )
    minimum_interval_seconds: int = Field(default=60)


class RunStartupSlowTrigger(_RunFilterMixin, _TriggerBase):
    """Fires when a run stayed in QUEUED / STARTING state for too long before
    reaching STARTED — captures "compute took too long to spin up" (pex load
    on Serverless, docker pull + container start on Hybrid, K8s pod scheduling
    delays, ECS task placement waits, resource-init hangs).

    Distinct from `run_stuck` which watches active RUNNING duration. This one
    catches startup latency specifically.
    """
    type: Literal["run_startup_slow"] = "run_startup_slow"
    max_startup_seconds: int = Field(
        description="Fire when time from run creation to STARTED > this."
    )
    job_name: Optional[str] = Field(default=None, description="Optional job filter.")
    minimum_interval_seconds: int = Field(default=60)


class DaemonHeartbeatTrigger(_TriggerBase):
    """Fires when a Dagster daemon / Dagster+ agent has stopped heartbeating.

    Watches `instance.get_daemon_statuses()` for the named daemon type (or all).
    A daemon is considered stale when its last heartbeat is older than
    `max_seconds_since_heartbeat`. Once-per-outage semantics via cursor.

    Common daemon types to watch: SENSOR / SCHEDULER / QUEUED_RUN_COORDINATOR /
    BACKFILL / ASSET / FRESHNESS. Dagster+ Hybrid agents also report through the
    daemon interface — this catches "the K8s / ECS / Docker agent died" via the
    same primitive that catches "the OSS sensor daemon died".
    """
    type: Literal["daemon_heartbeat"] = "daemon_heartbeat"
    daemon_type: Optional[str] = Field(
        default=None,
        description="Filter to a specific daemon type (e.g. SENSOR, SCHEDULER). None = all daemons.",
    )
    max_seconds_since_heartbeat: int = Field(
        default=120,
        description="Fire when a daemon hasn't heartbeat in this many seconds.",
    )
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
    DaemonHeartbeatTrigger,
    CodeLocationStatusTrigger,
    RunStartupSlowTrigger,
    AssetObservationTrigger,
    StepErrorTrigger,
    MetadataMatchTrigger,
    HookFiredTrigger,
    AssetPartitionMaterializedTrigger,
    RunReexecutionTrigger,
    AssetWipeTrigger,
    ConfigOverrideTrigger,
    TagSetTrigger,
    UnhandledExceptionTrigger,
    AssetCheckSeverityTrigger,
    OpOutputTrigger,
    MaterializationPlannedTrigger,
    AssetCheckStartedTrigger,
    InsightsMetricThresholdTrigger,
    DagsterPlusAuditTrigger,
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


def _split_multi_partition(partition) -> Optional[Dict[str, str]]:
    """Return a {dim: value} dict for a MultiPartitionKey, else None.

    Handles both real `MultiPartitionKey` (has `.keys_by_dimension`) and the
    pipe-delimited string form dagster uses when serialized. Returns None
    for single-dim / missing partitions."""
    if partition is None:
        return None
    # Real MultiPartitionKey exposes .keys_by_dimension
    keys_by_dim = getattr(partition, "keys_by_dimension", None)
    if keys_by_dim:
        return dict(keys_by_dim)
    # Fallback: pipe-delimited string form. We can't recover dim names from
    # the string alone — caller may still want the raw pieces, but without
    # names they're not useful, so we return None.
    return None


def _resolve_partition_key(spec, tokens: Dict[str, Any]):
    """Resolve a partition_key spec against the current tokens.

    Accepts:
      - None → returns None (no partition)
      - str → rendered via `_render_template` (so '{partition_key}' pulls
        from the triggering event's tokens)
      - Dict[str, str] → each value rendered, then wrapped in
        `MultiPartitionKey({dim: value, ...})` for
        MultiPartitionsDefinition targets

    Templating uses the standard trigger tokens plus per-dimension tokens
    like `partition_<dim>` that partition-aware triggers emit.
    """
    if spec is None:
        return None
    if isinstance(spec, dict):
        rendered = {k: _render_template(str(v), tokens) for k, v in spec.items()}
        try:
            return dg.MultiPartitionKey(rendered)
        except Exception:
            # Older dagster path or unexpected shape — fall back to a
            # pipe-joined string in dim-sorted order (dagster's internal form)
            return "|".join(rendered[k] for k in sorted(rendered))
    if isinstance(spec, str):
        return _render_template(spec, tokens)
    return spec


def _default_tokens(event_type: str, **extras) -> Dict[str, Any]:
    return {
        "event_type": event_type,
        "run_id": "",
        "job_name": "",
        "asset_key": "",
        "partition_key": "",
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
        partition_key = _resolve_partition_key(action.partition_key, tokens)
        return dg.RunRequest(
            run_key=f"{tokens.get('run_id', '')}-{action.asset_keys[0]}"[:120] or None,
            asset_selection=[dg.AssetKey.from_user_string(k) for k in action.asset_keys],
            partition_key=partition_key,
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
    # ── Tier 1 ops / self-healing actions ─────────────────────────────
    if isinstance(action, ReloadCodeLocationAction):
        loc = _render_template(action.location_name, tokens) if action.location_name else None
        if loc:
            _dagster_plus_mutation(
                action, f'mutation {{ reloadRepositoryLocation(repositoryLocationName: "{loc}") '
                        f'{{ __typename }} }}', logger, f"reload_code_location({loc})",
            )
        else:
            _dagster_plus_mutation(
                action, 'mutation { reloadWorkspace { __typename } }', logger, "reload_workspace",
            )
        return None
    if isinstance(action, RefreshDefsStateAction):
        loc = _render_template(action.location_name, tokens)
        if action.defs_state_key:
            key = _render_template(action.defs_state_key, tokens)
            m = (f'mutation {{ refreshComponentState(defsStateKey: "{key}", '
                 f'locationName: "{loc}") {{ __typename }} }}')
            label = f"refresh_component_state({loc}/{key})"
        else:
            m = f'mutation {{ refreshDefsState(locationName: "{loc}") {{ __typename }} }}'
            label = f"refresh_defs_state({loc})"
        _dagster_plus_mutation(action, m, logger, label)
        return None
    if isinstance(action, SetConcurrencyLimitAction):
        if instance is None:
            logger.warning("set_concurrency_limit: no instance provided; skipping.")
            return None
        key = _render_template(action.concurrency_key, tokens)
        try:
            instance.event_log_storage.set_concurrency_slots(key, action.limit)
            logger.info(f"set_concurrency_limit → {key} = {action.limit}")
        except Exception as exc:
            logger.warning(f"set_concurrency_limit failed: {exc}")
        return None
    if isinstance(action, FreeConcurrencySlotsAction):
        if instance is None:
            logger.warning("free_concurrency_slots: no instance provided; skipping.")
            return None
        rid = _render_template(action.run_id, tokens)
        step = _render_template(action.step_key, tokens) if action.step_key else None
        try:
            if step:
                instance.event_log_storage.free_concurrency_slots(run_id=rid, step_key=step)
                logger.info(f"free_concurrency_slots → run={rid[:8]} step={step}")
            else:
                instance.event_log_storage.free_concurrency_slots_for_run(run_id=rid)
                logger.info(f"free_concurrency_slots → run={rid[:8]}")
        except Exception as exc:
            logger.warning(f"free_concurrency_slots failed: {exc}")
        return None
    if isinstance(action, SetAutoMaterializePausedAction):
        m = (f'mutation {{ setAutoMaterializePaused(paused: '
             f'{"true" if action.paused else "false"}) }}')
        _dagster_plus_mutation(action, m, logger,
                               f"set_auto_materialize_paused({action.paused})")
        return None
    if isinstance(action, MuteAlertPolicyAction):
        m = (f'mutation {{ setAlertPolicyMuteUntil('
             f'alertPolicyId: "{action.alert_policy_id}", '
             f'muteForSeconds: {action.mute_for_seconds}) {{ __typename }} }}')
        _dagster_plus_mutation(action, m, logger,
                               f"mute_alert_policy({action.alert_policy_id}, {action.mute_for_seconds}s)")
        return None
    if isinstance(action, ResumeBackfillAction):
        bid = _render_template(action.backfill_id, tokens)
        m = f'mutation {{ resumePartitionBackfill(backfillId: "{bid}") {{ __typename }} }}'
        _dagster_plus_mutation(action, m, logger, f"resume_backfill({bid})")
        return None
    if isinstance(action, CancelBackfillAction):
        bid = _render_template(action.backfill_id, tokens)
        m = f'mutation {{ cancelPartitionBackfill(backfillId: "{bid}") {{ __typename }} }}'
        _dagster_plus_mutation(action, m, logger, f"cancel_backfill({bid})")
        return None
    if isinstance(action, ReexecuteBackfillAction):
        bid = _render_template(action.backfill_id, tokens)
        strategy = "FROM_FAILURE" if action.from_failure else "ALL_STEPS"
        m = (f'mutation {{ reexecutePartitionBackfill(reexecutionParams: '
             f'{{ parentRunId: "{bid}", strategy: {strategy} }}) {{ __typename }} }}')
        _dagster_plus_mutation(action, m, logger, f"reexecute_backfill({bid}, {strategy})")
        return None
    if isinstance(action, AddDynamicPartitionAction):
        if instance is None:
            logger.warning("add_dynamic_partition: no instance provided; skipping.")
            return None
        key = _render_template(action.partition_key, tokens)
        try:
            instance.add_dynamic_partitions(action.partitions_def_name, [key])
            logger.info(f"add_dynamic_partition → {action.partitions_def_name}={key}")
        except Exception as exc:
            logger.warning(f"add_dynamic_partition failed: {exc}")
        return None
    logger.warning(f"Unknown action type: {type(action).__name__}")
    return None


def _dagster_plus_mutation(action, mutation: str, logger, label: str) -> None:
    """Fire a Dagster+ GraphQL mutation. Non-fatal on failure — logs a warning.

    Actions that use this: reload_code_location, refresh_defs_state,
    set_auto_materialize_paused, mute_alert_policy, backfill controls.
    """
    org = os.environ.get(action.org_env_var, "")
    token = os.environ.get(action.token_env_var, "")
    if not org or not token:
        logger.warning(
            f"{label}: {action.org_env_var} or {action.token_env_var} not set — skipping."
        )
        return
    try:
        resp = _dagster_plus_graphql(mutation, org, token, action.deployment)
        if "errors" in resp:
            logger.warning(f"{label}: mutation returned errors: {resp['errors']}")
        else:
            logger.info(f"{label} → OK")
    except Exception as exc:
        logger.warning(f"{label}: mutation failed: {exc}")


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


# ── Selection resolution ──────────────────────────────────────────────
# Fields like `asset_keys` accept `Union[List[str], str]` where the string
# form is either:
#   - a Dagster asset-selection expression (`group:X`, `tag:foo=bar`,
#     `kind:Y`, `is:external`, boolean composition), resolved via
#     `AssetSelection.from_string()` against the sibling asset graph
#   - a bare fnmatch glob (`marts/*`) matched against discovered keys
#   - a single literal (backward compat)
# For run-based triggers, `job_name_pattern` is fnmatch on the run's
# `job_name`; `run_tags` is exact-match on the run's tags.

def _discover_sibling_assets(
    context,
) -> "Tuple[Optional[Any], List[str]]":
    """Discover assets defined in the same defs folder as this component.

    Returns (sibling_defs, discovered_key_strings). Used to resolve
    asset-selection strings into concrete asset keys at build_defs time.
    Matches the enhanced_data_quality_checks pattern.
    """
    try:
        parent_path = getattr(getattr(context, "path", None), "parent", None)
        if parent_path is None:
            return (None, [])
        sibling_defs = context.build_defs(parent_path)
        if sibling_defs is None:
            return (None, [])
        discovered: List[str] = []
        if getattr(sibling_defs, "assets", None):
            for assets_def in sibling_defs.assets:
                for k in assets_def.keys:
                    discovered.append(k.to_user_string())
        return (sibling_defs, discovered)
    except Exception:
        return (None, [])


def _resolve_asset_keys(
    target: "Union[List[str], str]",
    sibling_defs=None,
    discovered_keys: Optional[List[str]] = None,
) -> List[str]:
    """Resolve target into a concrete list of asset-key strings.

    - `List[str]` → returned as-is
    - `str` → try `AssetSelection.from_string()` against sibling asset
      graph; then fnmatch fallback against discovered keys; else return
      `[target]` as a single literal (backward compat for single-key strings).
    """
    if isinstance(target, list):
        return list(target)
    if not isinstance(target, str):
        return []
    # 1. Dagster selection language
    if sibling_defs is not None:
        try:
            graph = sibling_defs.resolve_asset_graph()
            matched = dg.AssetSelection.from_string(target).resolve(graph)
            if matched:
                return sorted(k.to_user_string() for k in matched)
        except Exception:
            pass
    # 2. Fnmatch against discovered keys
    if discovered_keys:
        import fnmatch as _fnmatch
        matched_glob = [k for k in discovered_keys if _fnmatch.fnmatch(k, target)]
        if matched_glob:
            return sorted(matched_glob)
    # 3. Single-literal fallback
    return [target]


def _run_matches_filters(
    run,
    job_name: Optional[str],
    job_name_pattern: Optional[str],
    run_tags: Optional[Dict[str, str]],
) -> bool:
    """Return True if run matches all specified filters.

    - `job_name` → exact-match on run.job_name
    - `job_name_pattern` → fnmatch (e.g. 'prod_*') on run.job_name
    - `run_tags` → every key/value must be present in run.tags
    """
    if job_name and getattr(run, "job_name", None) != job_name:
        return False
    if job_name_pattern:
        import fnmatch as _fnmatch
        if not _fnmatch.fnmatch(getattr(run, "job_name", "") or "", job_name_pattern):
            return False
    if run_tags:
        actual_tags = getattr(run, "tags", None) or {}
        for k, v in run_tags.items():
            if actual_tags.get(k) != v:
                return False
    return True


# Module-level throttle state — persists across ticks in the same daemon
# process, resets on process restart. Keyed by "sensor_name:dedup_key".
# Each value: {"last_ts": float, "count_hour": int, "hour_start": float,
#              "buffer": [tokens...], "buffer_first_ts": float,
#              "llm_last": {"decision": bool, "ts": float}}
_THROTTLE_STATE: Dict[str, Dict[str, Any]] = {}
# Cross-sensor fire log for correlation-based suppression.
# Each entry: (unix_ts, sensor_name). Bounded via _GLOBAL_FIRE_LOG_MAX.
_GLOBAL_FIRE_LOG: List[Tuple[float, str]] = []
_GLOBAL_FIRE_LOG_MAX = 500


def _record_global_fire(sensor_name: str, now: float) -> None:
    """Append a fire to the cross-sensor log; truncate to bound."""
    _GLOBAL_FIRE_LOG.append((now, sensor_name))
    if len(_GLOBAL_FIRE_LOG) > _GLOBAL_FIRE_LOG_MAX:
        del _GLOBAL_FIRE_LOG[: len(_GLOBAL_FIRE_LOG) - _GLOBAL_FIRE_LOG_MAX]


def _throttle_key(sensor_name: str, throttle: ThrottleConfig, tokens: Dict[str, Any]) -> str:
    """Build a state key for this trigger + dedup_key_template combo."""
    if throttle.dedup_key_template:
        dedup = _render_template(throttle.dedup_key_template, tokens)
    else:
        dedup = "*"
    return f"{sensor_name}:{dedup}"


_WEEKDAY_MAP = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


def _parse_business_hours(spec: str) -> Tuple[int, int, str, Optional[List[int]]]:
    """Parse 'HH:MM-HH:MM tz [day,day,...]' into (start_min, end_min, tz, weekdays).

    Raises ValueError on malformed input.
    """
    parts = spec.strip().split()
    if len(parts) < 2:
        raise ValueError(f"business_hours_only needs 'HH:MM-HH:MM tz [days]', got '{spec}'")
    window, tz_name = parts[0], parts[1]
    start_str, _, end_str = window.partition("-")
    sh, _, sm = start_str.partition(":")
    eh, _, em = end_str.partition(":")
    start_min = int(sh) * 60 + int(sm or 0)
    end_min = int(eh) * 60 + int(em or 0)
    weekdays: Optional[List[int]] = None
    if len(parts) >= 3:
        weekdays = []
        for day in parts[2].split(","):
            key = day.strip().lower()[:3]
            if key in _WEEKDAY_MAP:
                weekdays.append(_WEEKDAY_MAP[key])
    return start_min, end_min, tz_name, weekdays


def _is_in_business_hours(now_ts: float, spec: str) -> bool:
    """True if now_ts falls in the daily window (and optional day-of-week list)."""
    try:
        start_min, end_min, tz_name, weekdays = _parse_business_hours(spec)
    except Exception:
        return True  # malformed spec — fail open
    try:
        from datetime import datetime, timezone
        try:
            from zoneinfo import ZoneInfo
            tz = ZoneInfo(tz_name) if tz_name.upper() != "UTC" else timezone.utc
        except Exception:
            tz = timezone.utc
        local = datetime.fromtimestamp(now_ts, tz=tz)
        cur_min = local.hour * 60 + local.minute
        if weekdays is not None and local.weekday() not in weekdays:
            return False
        if start_min <= end_min:
            return start_min <= cur_min < end_min
        # Overnight window (e.g. 22:00-06:00)
        return cur_min >= start_min or cur_min < end_min
    except Exception:
        return True


def _is_in_maintenance_window(now_ts: float, windows: List[MaintenanceWindow]) -> Optional[MaintenanceWindow]:
    """Return the first window that now_ts falls inside, or None."""
    from datetime import datetime
    for w in windows:
        try:
            start = datetime.fromisoformat(w.from_ts.replace("Z", "+00:00")).timestamp()
            end = datetime.fromisoformat(w.to_ts.replace("Z", "+00:00")).timestamp()
            if start <= now_ts < end:
                return w
        except Exception:
            continue
    return None


def _correlated_fire_exists(
    throttle: ThrottleConfig, now: float
) -> Optional[str]:
    """Return the name of a recently-fired correlated sensor (substring match), or None."""
    if not throttle.correlation_suppress_sensors:
        return None
    cutoff = now - throttle.correlation_within_seconds
    for ts, name in reversed(_GLOBAL_FIRE_LOG):
        if ts < cutoff:
            break
        for needle in throttle.correlation_suppress_sensors:
            if needle in name:
                return name
    return None


def _escalation_indices(
    ladder: List[EscalationTier], fire_count: int
) -> Optional[List[int]]:
    """Given fire_count (0-indexed), return the action indices for the highest
    tier whose `after_fires` is <= fire_count. None means 'no tier matched'."""
    matched: Optional[EscalationTier] = None
    for tier in sorted(ladder, key=lambda t: t.after_fires):
        if fire_count >= tier.after_fires:
            matched = tier
        else:
            break
    return matched.action_indices if matched else None


def _apply_throttle(
    throttle: ThrottleConfig,
    tokens: Dict[str, Any],
    sensor_name: str,
    logger,
) -> "Tuple[bool, Dict[str, Any], Optional[List[int]]]":
    """Decide whether this fire should proceed given throttle rules.

    Returns (should_fire, tokens, action_indices_filter). When strategy is
    `summarize` or `first_last`, `tokens` may be replaced with an aggregate
    summary. When strategy is `escalate`, `action_indices_filter` is a list
    of indices into the trigger's action list to execute (None = all).
    """
    now = time.time()
    key = _throttle_key(sensor_name, throttle, tokens)
    state = _THROTTLE_STATE.setdefault(key, {})

    # ── Business hours gate ───────────────────────────────────────
    if throttle.business_hours_only and not _is_in_business_hours(now, throttle.business_hours_only):
        logger.info(f"[throttle:{key}] outside business_hours ({throttle.business_hours_only}), dropping")
        return (False, tokens, None)

    # ── Maintenance window gate ───────────────────────────────────
    if throttle.maintenance_windows:
        window = _is_in_maintenance_window(now, throttle.maintenance_windows)
        if window is not None:
            reason = window.reason or f"{window.from_ts} → {window.to_ts}"
            logger.info(f"[throttle:{key}] in maintenance window ({reason}), dropping")
            return (False, tokens, None)

    # ── Correlation suppression ───────────────────────────────────
    correlated = _correlated_fire_exists(throttle, now)
    if correlated is not None:
        logger.info(f"[throttle:{key}] correlated fire from '{correlated}' within {throttle.correlation_within_seconds}s, dropping")
        return (False, tokens, None)

    # ── Auto-resolve: emit paired resolve when condition returns ──
    # For auto_resolve, the CALLER is expected to invoke _apply_throttle
    # only when the underlying condition is TRUE (fires). If we were in a
    # fired state and the caller drops back to False, we emit a resolve.
    # Since throttle is checked only on real fires, the resolve pathway
    # is handled at the sensor level via _check_auto_resolve() below.
    if throttle.strategy == "auto_resolve":
        # Mark the condition as active; reset "resolved_sent" flag.
        state["auto_resolve_active"] = True
        state["auto_resolve_first_fire_ts"] = state.get("auto_resolve_first_fire_ts") or now
        state["auto_resolve_last_fire_ts"] = now
        state["auto_resolve_fire_count"] = state.get("auto_resolve_fire_count", 0) + 1
        state["resolved_sent"] = False
        # Cooldown between fires for auto_resolve
        if throttle.min_seconds_between_fires:
            last_ts = state.get("last_ts", 0.0)
            if now - last_ts < throttle.min_seconds_between_fires:
                logger.info(f"[throttle:{key}] auto_resolve in cooldown, dropping duplicate fire")
                return (False, tokens, None)
        state["last_ts"] = now
        state["count_hour"] = state.get("count_hour", 0) + 1
        return (True, tokens, None)

    # ── max_per_hour rolling window ────────────────────────────────
    if throttle.max_per_hour:
        hour_start = state.get("hour_start", 0.0)
        if now - hour_start > 3600:
            state["hour_start"] = now
            state["count_hour"] = 0
        if state.get("count_hour", 0) >= throttle.max_per_hour:
            logger.info(f"[throttle:{key}] max_per_hour={throttle.max_per_hour} hit, dropping")
            return (False, tokens, None)

    # ── min_seconds_between_fires cooldown ─────────────────────────
    if throttle.min_seconds_between_fires:
        last_ts = state.get("last_ts", 0.0)
        if now - last_ts < throttle.min_seconds_between_fires:
            # In cooldown — behavior depends on strategy
            if throttle.strategy == "silence":
                logger.info(f"[throttle:{key}] cooldown, dropping (silence)")
                return (False, tokens, None)
            elif throttle.strategy in ("summarize", "first_last"):
                # Buffer this event; flush later
                buf = state.setdefault("buffer", [])
                buf.append({"ts": now, "tokens": dict(tokens)})
                state.setdefault("buffer_first_ts", now)
                logger.info(f"[throttle:{key}] cooldown, buffered ({len(buf)} pending)")
                return (False, tokens, None)
            elif throttle.strategy in ("llm", "escalate"):
                # Fall through: LLM checks + escalation tier bumping happen below
                pass

    # ── summarize/first_last: check if buffer is ready to flush ────
    if throttle.strategy in ("summarize", "first_last") and state.get("buffer"):
        first_ts = state.get("buffer_first_ts", now)
        if now - first_ts >= throttle.flush_after_seconds:
            buf = state["buffer"]
            summarized = _summarize_tokens(buf, tokens, throttle.strategy)
            state["buffer"] = []
            state["buffer_first_ts"] = 0.0
            state["last_ts"] = now
            state["count_hour"] = state.get("count_hour", 0) + 1
            logger.info(f"[throttle:{key}] flushing {len(buf)}-event summary")
            _record_global_fire(sensor_name, now)
            return (True, summarized, None)

    # ── LLM strategy ────────────────────────────────────────────────
    if throttle.strategy == "llm":
        cached = state.get("llm_last")
        if cached and (now - cached["ts"]) < throttle.llm_decision_cache_seconds:
            if not cached["decision"]:
                logger.info(f"[throttle:{key}] LLM cached NO, dropping")
                return (False, tokens, None)
        else:
            recent = state.get("recent_alerts", [])
            recent = [r for r in recent if now - r["ts"] < 3600][-10:]
            state["recent_alerts"] = recent + [{"ts": now, "msg": tokens.get("message", "")}]
            decision = _ask_llm_should_fire(throttle, tokens, recent, logger)
            state["llm_last"] = {"decision": decision, "ts": now}
            if not decision:
                logger.info(f"[throttle:{key}] LLM said NO, dropping")
                return (False, tokens, None)

    # ── Escalation strategy — pick tier by fire count ──────────────
    action_indices: Optional[List[int]] = None
    if throttle.strategy == "escalate":
        prior_fires = state.get("escalate_fire_count", 0)
        if throttle.escalation_ladder:
            action_indices = _escalation_indices(throttle.escalation_ladder, prior_fires)
            if action_indices is None:
                logger.info(f"[throttle:{key}] escalate: no tier matched at fire_count={prior_fires}, dropping")
                return (False, tokens, None)
            logger.info(f"[throttle:{key}] escalate: fire #{prior_fires} → action indices {action_indices}")
        state["escalate_fire_count"] = prior_fires + 1

    # ── Fire — update state ─────────────────────────────────────────
    state["last_ts"] = now
    state["count_hour"] = state.get("count_hour", 0) + 1
    _record_global_fire(sensor_name, now)
    return (True, tokens, action_indices)


def _check_auto_resolve(
    throttle: Optional[ThrottleConfig],
    sensor_name: str,
    logger,
) -> Optional[Dict[str, Any]]:
    """Called at the top of a sensor tick BEFORE checking the underlying
    condition. If the condition was previously fired (auto_resolve_active)
    but has been stale (no fire this tick after cooldown), emit a paired
    resolve event and clear the active flag. Returns tokens for the
    resolve fire, or None.

    NOTE: This is a best-effort heuristic — since sensor logic can't know
    "condition is no longer true", we key off staleness: if the last fire
    was more than 2× cooldown ago and we haven't yet sent a resolve, emit.
    """
    if throttle is None or throttle.strategy != "auto_resolve":
        return None
    now = time.time()
    key = f"{sensor_name}:*"  # global for the sensor
    state = _THROTTLE_STATE.get(key)
    if not state or not state.get("auto_resolve_active"):
        return None
    if state.get("resolved_sent"):
        return None
    stale_after = max((throttle.min_seconds_between_fires or 60) * 2, 120)
    last_fire = state.get("auto_resolve_last_fire_ts", 0.0)
    if now - last_fire < stale_after:
        return None
    first_fire = state.get("auto_resolve_first_fire_ts", last_fire)
    fire_count = state.get("auto_resolve_fire_count", 1)
    duration = int(last_fire - first_fire)
    resolve_tokens = _default_tokens(
        event_type="auto_resolved",
        message=_render_template(
            throttle.auto_resolve_message,
            {"duration_seconds": duration, "fire_count": fire_count},
        ),
        duration_seconds=duration,
        fire_count=fire_count,
    )
    state["auto_resolve_active"] = False
    state["auto_resolve_first_fire_ts"] = 0.0
    state["auto_resolve_fire_count"] = 0
    state["resolved_sent"] = True
    logger.info(f"[throttle:{key}] auto_resolve: emitting paired resolve after {duration}s / {fire_count} fires")
    return resolve_tokens


def _summarize_tokens(buffer: List[Dict[str, Any]], current: Dict[str, Any], strategy: str) -> Dict[str, Any]:
    """Fold a buffer of accumulated events into a single token set for
    the summary fire."""
    if not buffer:
        return current
    count = len(buffer) + 1  # buffer + current
    first = buffer[0]["tokens"]
    last = current
    summary = dict(last)
    summary["event_type"] = f"{summary.get('event_type', '')}_summary_{strategy}"
    summary["message"] = (
        f"[{count}× in the last window] "
        f"First: {first.get('message', '')[:100]} @ ts={int(buffer[0]['ts'])}. "
        f"Last: {last.get('message', '')[:100]} @ ts={int(time.time())}."
    )
    return summary


def _ask_llm_should_fire(
    throttle: ThrottleConfig,
    tokens: Dict[str, Any],
    recent: List[Dict[str, Any]],
    logger,
) -> bool:
    """Ask an LLM to decide YES/NO. Falls back to True on any error."""
    api_key = os.environ.get(throttle.llm_api_key_env_var, "")
    if not api_key:
        logger.warning(f"[throttle:llm] {throttle.llm_api_key_env_var} not set — defaulting to YES")
        return True
    recent_str = "\n".join(f"  - {r['ts']}: {r['msg'][:200]}" for r in recent) or "(none)"
    prompt = _render_template(
        throttle.llm_prompt_template,
        {**tokens, "recent": recent_str, "message": tokens.get("message", "")},
    )
    try:
        if throttle.llm_provider == "openai":
            return _openai_yes_no(throttle.llm_model, api_key, prompt, logger)
        elif throttle.llm_provider == "anthropic":
            return _anthropic_yes_no(throttle.llm_model, api_key, prompt, logger)
        else:
            logger.warning(f"[throttle:llm] unknown provider '{throttle.llm_provider}' — defaulting to YES")
            return True
    except Exception as exc:
        logger.warning(f"[throttle:llm] LLM call failed: {exc} — defaulting to YES")
        return True


def _openai_yes_no(model: str, api_key: str, prompt: str, logger) -> bool:
    import urllib.request
    import json as _json
    body = _json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 100,
        "temperature": 0.0,
    }).encode()
    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=body,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
    )
    resp = _json.loads(urllib.request.urlopen(req, timeout=15).read())
    answer = (resp.get("choices", [{}])[0].get("message", {}).get("content", "") or "").strip()
    logger.info(f"[throttle:llm/openai] decision: {answer[:200]}")
    return answer.upper().startswith("YES")


def _anthropic_yes_no(model: str, api_key: str, prompt: str, logger) -> bool:
    import urllib.request
    import json as _json
    body = _json.dumps({
        "model": model,
        "max_tokens": 100,
        "messages": [{"role": "user", "content": prompt}],
    }).encode()
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
    )
    resp = _json.loads(urllib.request.urlopen(req, timeout=15).read())
    answer = (resp.get("content", [{}])[0].get("text", "") or "").strip()
    logger.info(f"[throttle:llm/anthropic] decision: {answer[:200]}")
    return answer.upper().startswith("YES")


def _run_actions(
    actions: List[Action],
    tokens: Dict[str, Any],
    logger,
    instance=None,
    throttle: Optional[ThrottleConfig] = None,
    sensor_name: str = "",
) -> List[dg.RunRequest]:
    """Execute every action. Collect RunRequests (materialize / launch_job)
    for return; side-effect actions execute inline. If `throttle` is set,
    apply the throttle gate before firing. When escalate strategy selects
    a subset of actions, only those indices execute."""
    action_indices_filter: Optional[List[int]] = None
    if throttle:
        should_fire, tokens, action_indices_filter = _apply_throttle(
            throttle, tokens, sensor_name or "unnamed", logger
        )
        if not should_fire:
            return []
    requests_out = []
    for idx, action in enumerate(actions):
        if action_indices_filter is not None and idx not in action_indices_filter:
            continue
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
    monitored = _build_monitored_jobs_arg(trigger.monitored_locations, trigger.monitored_jobs)

    decorator_kwargs = {
        "name": name,
        "run_status": status_enum,
        "default_status": default_status,
    }
    if monitored is not None:
        decorator_kwargs["monitored_jobs"] = monitored

    @dg.run_status_sensor(**decorator_kwargs)
    def _sensor(context: dg.RunStatusSensorContext):
        run = context.dagster_run
        if not _run_matches_filters(
            run, trigger.job_name, trigger.job_name_pattern, trigger.run_tags
        ):
            return
        tokens = _default_tokens(
            event_type=f"run_{trigger.status.lower()}",
            run_id=run.run_id,
            job_name=run.job_name or "",
            status=trigger.status,
            message=f"Run {run.run_id} for {run.job_name} → {trigger.status}",
        )
        requests_out = _run_actions(actions, tokens, context.log, instance=context.instance, throttle=trigger.throttle, sensor_name=name)
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
            partition = getattr(record, "partition_key", None) or getattr(record, "partition", None) or ""
            tokens = _default_tokens(
                event_type="asset_materialized",
                asset_key=asset_key.to_user_string(),
                run_id=record.run_id if hasattr(record, "run_id") else "",
                partition_key=str(partition),
                message=f"Asset {asset_key.to_user_string()} materialized",
            )
            for dim, val in (_split_multi_partition(partition) or {}).items():
                tokens[f"partition_{dim}"] = val
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=context.instance, throttle=trigger.throttle, sensor_name=name))
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
        requests_out = _run_actions(actions, tokens, context.log, instance=context.instance, throttle=trigger.throttle, sensor_name=name)
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
                return dg.SensorResult(run_requests=_run_actions(actions, tokens, context.log, instance=context.instance, throttle=trigger.throttle, sensor_name=name))
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
                    return dg.SensorResult(run_requests=_run_actions(actions, tokens, context.log, instance=context.instance, throttle=trigger.throttle, sensor_name=name))
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
        return dg.SensorResult(run_requests=_run_actions(actions, tokens, context.log, instance=context.instance, throttle=trigger.throttle, sensor_name=name))

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
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=context.instance, throttle=trigger.throttle, sensor_name=name))
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
    monitored = _build_monitored_jobs_arg(trigger.monitored_locations, trigger.monitored_jobs)

    success_kwargs = {"name": name, "run_status": dg.DagsterRunStatus.SUCCESS, "default_status": default_status}
    fail_kwargs = {"name": f"{name}__fail", "run_status": dg.DagsterRunStatus.FAILURE, "default_status": default_status}
    if monitored is not None:
        success_kwargs["monitored_jobs"] = monitored
        fail_kwargs["monitored_jobs"] = monitored

    @dg.run_status_sensor(**success_kwargs)
    def _on_success(context: dg.RunStatusSensorContext):
        return _handle(context, "SUCCESS")

    @dg.run_status_sensor(**fail_kwargs)
    def _on_failure(context: dg.RunStatusSensorContext):
        return _handle(context, "FAILURE")

    def _handle(context: dg.RunStatusSensorContext, status: str):
        run = context.dagster_run
        if not _run_matches_filters(
            run, trigger.job_name, trigger.job_name_pattern, trigger.run_tags
        ):
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
        requests_out = _run_actions(actions, tokens, context.log, instance=context.instance, throttle=trigger.throttle, sensor_name=name)
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
            if not _run_matches_filters(
                run, trigger.job_name, trigger.job_name_pattern, trigger.run_tags
            ):
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
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance, throttle=trigger.throttle, sensor_name=name))
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
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance, throttle=trigger.throttle, sensor_name=name))
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
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance, throttle=trigger.throttle, sensor_name=name))
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
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance, throttle=trigger.throttle, sensor_name=name))
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
    scan_events = "events" in trigger.sources
    scan_stdout = "stdout" in trigger.sources
    scan_stderr = "stderr" in trigger.sources

    def _scan_events(instance, run_id: str):
        """Scan dagster event log (user_message field)."""
        try:
            logs = instance.all_logs(run_id)
        except Exception:
            return None
        for entry in logs:
            msg = getattr(entry, "user_message", "") or getattr(entry, "message", "") or ""
            if msg and pattern.search(msg):
                return ("events", msg[:500])
        return None

    def _scan_compute(instance, run_id: str, io_type: str):
        """Scan compute_log_manager output (raw stdout/stderr from ops).

        API surface varies by Dagster version + compute log manager backend
        (Dagster+ managed / S3 / GCS / Azure / local files). We try the modern
        `get_log_data` API first; fall back to `read_logs` shape if needed.
        Silently no-ops on any exception — a broken compute log fetch shouldn't
        block the whole sensor tick.
        """
        try:
            from dagster._core.storage.captured_log_manager import ComputeIOType
            io_enum = getattr(ComputeIOType, io_type.upper(), None)
            if io_enum is None:
                return None
        except ImportError:
            return None
        # Fetch step keys for this run to build log_keys
        try:
            step_stats = instance.get_run_step_stats(run_id)
        except Exception:
            return None
        for step in step_stats:
            step_key = getattr(step, "step_key", None)
            if not step_key:
                continue
            log_key = [run_id, "compute_logs", step_key]
            try:
                clm = instance.compute_log_manager
                if hasattr(clm, "get_log_data"):
                    log_data = clm.get_log_data(log_key)
                    stream = getattr(log_data, io_type, b"") or b""
                elif hasattr(clm, "read_logs_file"):
                    log_data = clm.read_logs_file(run_id, step_key, io_enum)
                    stream = getattr(log_data, "data", b"") or b""
                else:
                    continue
                text = stream.decode("utf-8", errors="replace") if isinstance(stream, (bytes, bytearray)) else str(stream)
                if pattern.search(text):
                    # Return a short excerpt around the first match
                    m = pattern.search(text)
                    if m:
                        start = max(0, m.start() - 80)
                        end = min(len(text), m.end() + 80)
                        return (io_type, text[start:end])
            except Exception:
                continue
        return None

    def _sensor_fn(context: dg.SensorEvaluationContext):
        instance = context.instance
        seen = set((context.cursor or "").split(",")) if context.cursor else set()
        seen.discard("")
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
            hit = None
            if scan_events:
                hit = _scan_events(instance, run.run_id)
            if hit is None and scan_stdout:
                hit = _scan_compute(instance, run.run_id, "stdout")
            if hit is None and scan_stderr:
                hit = _scan_compute(instance, run.run_id, "stderr")
            if hit is None:
                continue
            source, matched_msg = hit
            tokens = _default_tokens(
                event_type=f"log_pattern_matched_{source}",
                run_id=run.run_id,
                job_name=run.job_name or "",
                status=run.status.value,
                message=matched_msg,
            )
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance, throttle=trigger.throttle, sensor_name=name))
        merged = list(seen | set(newly_scanned))[-500:]
        context.update_cursor(",".join(merged))
        return dg.SensorResult(run_requests=all_requests) if all_requests else dg.SkipReason("no matches")

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds,
        default_status=default_status,
    )


def _event_log_scan(instance, event_type, last_seen_id, limit=100):
    """Shared helper: scan event log for a specific event type after cursor.
    Returns (records, max_storage_id_seen)."""
    from dagster._core.events import DagsterEventType
    try:
        etype = event_type if isinstance(event_type, DagsterEventType) else getattr(DagsterEventType, event_type)
        records = instance.event_log_storage.get_event_records(
            event_records_filter=dg.EventRecordsFilter(
                event_type=etype,
                after_cursor=last_seen_id if last_seen_id else None,
            ),
            limit=limit,
            ascending=True,
        )
        return list(records)
    except Exception:
        return []


def _build_generic_event_sensor(
    name: str,
    event_type: str,
    filter_fn,
    token_builder,
    actions: List[Action],
    minimum_interval_seconds: int,
    default_status: dg.DefaultSensorStatus,
) -> dg.SensorDefinition:
    """Factory for the common shape: scan event log for `event_type`, apply
    `filter_fn(record) -> bool`, build tokens via `token_builder(record)`,
    fire actions on match."""
    def _sensor_fn(context: dg.SensorEvaluationContext):
        instance = context.instance
        last_seen_id = int(context.cursor) if (context.cursor or "").isdigit() else 0
        records = _event_log_scan(instance, event_type, last_seen_id)
        all_requests = []
        max_id = last_seen_id
        for rec in records:
            rid = getattr(rec, "storage_id", 0)
            if rid > max_id:
                max_id = rid
            try:
                # filter_fn may take (rec) or (rec, instance) — try 2-arg first
                try:
                    match = filter_fn(rec, instance)
                except TypeError:
                    match = filter_fn(rec)
                if not match:
                    continue
                tokens = token_builder(rec)
            except Exception as exc:
                context.log.warning(f"filter/token error: {exc}")
                continue
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance, throttle=trigger.throttle, sensor_name=name))
        if max_id > last_seen_id:
            context.update_cursor(str(max_id))
        return dg.SensorResult(run_requests=all_requests) if all_requests else dg.SkipReason("no matches")

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=minimum_interval_seconds,
        default_status=default_status,
    )


def _build_hook_fired_sensor(name, trigger, actions, default_status):
    import re as _re
    hook_re = _re.compile(trigger.hook_name_pattern) if trigger.hook_name_pattern else None
    def filter_fn(rec, instance):
        entry = getattr(rec, "event_log_entry", None) or rec
        evt = getattr(entry, "dagster_event", None)
        if evt is None:
            return False
        hook_data = evt.event_specific_data
        hook_name = getattr(hook_data, "hook_name", "") or ""
        # HOOK_COMPLETED events carry status implicitly; HOOK_ERRORED for failures
        etype_name = getattr(evt, "event_type_value", "")
        if hook_re and not hook_re.search(hook_name):
            return False
        if trigger.on_status == "SUCCESS" and "ERROR" in etype_name:
            return False
        if trigger.on_status == "FAILURE" and "ERROR" not in etype_name:
            return False
        # Apply run-based filters if any are set
        if trigger.job_name or trigger.job_name_pattern or trigger.run_tags:
            try:
                run = instance.get_run_by_id(getattr(entry, "run_id", "") or "")
                if run is None or not _run_matches_filters(
                    run, trigger.job_name, trigger.job_name_pattern, trigger.run_tags
                ):
                    return False
            except Exception:
                pass
        return True
    def token_builder(rec):
        entry = getattr(rec, "event_log_entry", None) or rec
        evt = entry.dagster_event
        hook_data = evt.event_specific_data
        return _default_tokens(
            event_type="hook_fired",
            run_id=entry.run_id or "",
            job_name=getattr(hook_data, "hook_name", "") or "",
            status=getattr(evt, "event_type_value", ""),
            message=f"Hook {getattr(hook_data, 'hook_name', '')} fired",
        )
    # Scan HOOK_COMPLETED (dagster ships both HOOK_COMPLETED / HOOK_ERRORED)
    return _build_generic_event_sensor(
        name, "HOOK_COMPLETED", filter_fn, token_builder, actions, trigger.minimum_interval_seconds, default_status
    )


def _build_asset_partition_materialized_sensor(name, trigger, actions, default_status):
    import re as _re
    watched = {dg.AssetKey.from_user_string(k) for k in trigger.asset_keys}
    partition_re = _re.compile(trigger.partition_key_pattern) if trigger.partition_key_pattern else None
    def filter_fn(rec):
        entry = getattr(rec, "event_log_entry", None) or rec
        evt = entry.dagster_event
        if evt is None:
            return False
        mat = evt.event_specific_data
        asset_key = getattr(mat, "asset_key", None)
        if asset_key not in watched:
            return False
        partition = getattr(mat, "partition", None)
        if trigger.partition_key is not None:
            if isinstance(trigger.partition_key, dict):
                # Multi-dim filter: every specified dim must match; unspecified dims wildcard
                dims = _split_multi_partition(partition)
                if dims is None:
                    return False
                for k, v in trigger.partition_key.items():
                    if dims.get(k) != v:
                        return False
            else:
                if partition != trigger.partition_key:
                    return False
        if partition_re and (partition is None or not partition_re.search(str(partition))):
            return False
        return True
    def token_builder(rec):
        entry = getattr(rec, "event_log_entry", None) or rec
        evt = entry.dagster_event
        mat = evt.event_specific_data
        asset_key = getattr(mat, "asset_key")
        partition = getattr(mat, "partition", None) or ""
        tokens = _default_tokens(
            event_type="asset_partition_materialized",
            asset_key=asset_key.to_user_string(),
            status=str(partition),
            partition_key=str(partition),
            message=f"{asset_key.to_user_string()} partition '{partition}' materialized",
        )
        # Expose each dimension as its own token so downstream actions can
        # template `partition_date` / `partition_region` / etc.
        for dim, val in (_split_multi_partition(partition) or {}).items():
            tokens[f"partition_{dim}"] = val
        return tokens
    return _build_generic_event_sensor(
        name, "ASSET_MATERIALIZATION", filter_fn, token_builder, actions, trigger.minimum_interval_seconds, default_status
    )


def _build_run_reexecution_sensor(name, trigger, actions, default_status):
    def _sensor_fn(context: dg.SensorEvaluationContext):
        instance = context.instance
        seen = set((context.cursor or "").split(",")) if context.cursor else set()
        seen.discard("")
        recent = instance.get_runs(limit=50)
        all_requests = []
        newly_seen = []
        for run in recent:
            if run.run_id in seen:
                continue
            newly_seen.append(run.run_id)
            parent = getattr(run, "parent_run_id", None) or getattr(run, "root_run_id", None)
            if not parent:
                continue
            if trigger.job_name and run.job_name != trigger.job_name:
                continue
            if trigger.strategy:
                # Strategy is typically stored in tags
                tag = (run.tags or {}).get("dagster/resume_retry", "")
                if trigger.strategy == "from_failure" and tag != "from_failure":
                    continue
            tokens = _default_tokens(
                event_type="run_reexecution",
                run_id=run.run_id,
                job_name=run.job_name or "",
                message=f"Re-execution of {parent[:8]} → {run.run_id[:8]}",
            )
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance, throttle=trigger.throttle, sensor_name=name))
        merged = list(seen | set(newly_seen))[-500:]
        context.update_cursor(",".join(merged))
        return dg.SensorResult(run_requests=all_requests) if all_requests else dg.SkipReason("no reexecutions")

    return dg.SensorDefinition(
        name=name, evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds, default_status=default_status,
    )


def _build_asset_wipe_sensor(name, trigger, actions, default_status):
    watched = {dg.AssetKey.from_user_string(k) for k in (trigger.asset_keys or [])} if trigger.asset_keys else None
    def filter_fn(rec):
        entry = getattr(rec, "event_log_entry", None) or rec
        evt = entry.dagster_event
        if evt is None:
            return False
        if watched is None:
            return True
        wipe_data = evt.event_specific_data
        asset_key = getattr(wipe_data, "asset_key", None)
        return asset_key in watched
    def token_builder(rec):
        entry = getattr(rec, "event_log_entry", None) or rec
        evt = entry.dagster_event
        wipe_data = evt.event_specific_data
        asset_key = getattr(wipe_data, "asset_key", None)
        return _default_tokens(
            event_type="asset_wipe",
            asset_key=asset_key.to_user_string() if asset_key else "",
            status="WIPED",
            message="Asset materialization history wiped",
        )
    return _build_generic_event_sensor(
        name, "ASSET_WIPED", filter_fn, token_builder, actions, trigger.minimum_interval_seconds, default_status
    )


def _build_config_override_sensor(name, trigger, actions, default_status):
    def _sensor_fn(context: dg.SensorEvaluationContext):
        instance = context.instance
        seen = set((context.cursor or "").split(",")) if context.cursor else set()
        seen.discard("")
        recent = instance.get_runs(limit=50)
        all_requests = []
        newly_seen = []
        for run in recent:
            if run.run_id in seen:
                continue
            newly_seen.append(run.run_id)
            if trigger.job_name and run.job_name != trigger.job_name:
                continue
            run_config = getattr(run, "run_config", None) or {}
            if not run_config:
                continue
            # Heuristic: any non-empty run_config likely means an override
            tokens = _default_tokens(
                event_type="config_override",
                run_id=run.run_id,
                job_name=run.job_name or "",
                message=f"Run {run.run_id[:8]} launched with config override ({len(run_config)} keys)",
            )
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance, throttle=trigger.throttle, sensor_name=name))
        merged = list(seen | set(newly_seen))[-500:]
        context.update_cursor(",".join(merged))
        return dg.SensorResult(run_requests=all_requests) if all_requests else dg.SkipReason("no overrides")

    return dg.SensorDefinition(
        name=name, evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds, default_status=default_status,
    )


def _build_tag_set_sensor(name, trigger, actions, default_status):
    import re as _re
    val_re = _re.compile(trigger.tag_value_pattern) if trigger.tag_value_pattern else None

    def _sensor_fn(context: dg.SensorEvaluationContext):
        instance = context.instance
        seen = set((context.cursor or "").split(",")) if context.cursor else set()
        seen.discard("")
        recent = instance.get_runs(limit=100)
        all_requests = []
        newly_seen = []
        for run in recent:
            if run.run_id in seen:
                continue
            newly_seen.append(run.run_id)
            tags = run.tags or {}
            if trigger.tag_key not in tags:
                continue
            v = tags[trigger.tag_key]
            if trigger.tag_value is not None and v != trigger.tag_value:
                continue
            if val_re and not val_re.search(v):
                continue
            tokens = _default_tokens(
                event_type="tag_set",
                run_id=run.run_id,
                job_name=run.job_name or "",
                status=v,
                message=f"Run {run.run_id[:8]} has {trigger.tag_key}={v}",
            )
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance, throttle=trigger.throttle, sensor_name=name))
        merged = list(seen | set(newly_seen))[-500:]
        context.update_cursor(",".join(merged))
        return dg.SensorResult(run_requests=all_requests) if all_requests else dg.SkipReason("no matching tags")

    return dg.SensorDefinition(
        name=name, evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds, default_status=default_status,
    )


def _build_unhandled_exception_sensor(name, trigger, actions, default_status):
    def filter_fn(rec):
        entry = getattr(rec, "event_log_entry", None) or rec
        evt = getattr(entry, "dagster_event", None)
        if evt is None:
            return False
        # Run failure with unexpected error (not step-level)
        failure_data = evt.event_specific_data
        error = getattr(failure_data, "error", None) or getattr(failure_data, "failure_error", None)
        # Only fire if there's no step_key (i.e., it's a run-level unhandled)
        if getattr(evt, "step_key", None):
            return False
        return error is not None
    def token_builder(rec):
        entry = getattr(rec, "event_log_entry", None) or rec
        evt = entry.dagster_event
        failure_data = evt.event_specific_data
        error = getattr(failure_data, "error", None) or getattr(failure_data, "failure_error", None)
        msg = getattr(error, "message", "") if error else ""
        return _default_tokens(
            event_type="unhandled_exception",
            run_id=entry.run_id or "",
            status="FAILURE",
            message=(msg or "Run failed with unhandled exception")[:500],
        )
    return _build_generic_event_sensor(
        name, "PIPELINE_FAILURE", filter_fn, token_builder, actions, trigger.minimum_interval_seconds, default_status
    )


def _build_asset_check_severity_sensor(name, trigger, actions, default_status):
    watched_checks = set(trigger.check_names or [])
    watched_assets = {dg.AssetKey.from_user_string(k) for k in (trigger.asset_keys or [])} if trigger.asset_keys else None
    def filter_fn(rec):
        entry = getattr(rec, "event_log_entry", None) or rec
        evt = getattr(entry, "dagster_event", None)
        if evt is None:
            return False
        eval_data = evt.event_specific_data
        severity = getattr(eval_data, "severity", None)
        sev_str = getattr(severity, "value", None) or str(severity or "")
        if sev_str != trigger.severity:
            return False
        check_name = getattr(eval_data, "check_name", "") or ""
        if watched_checks and check_name not in watched_checks:
            return False
        asset_key = getattr(eval_data, "asset_key", None)
        if watched_assets and asset_key not in watched_assets:
            return False
        return True
    def token_builder(rec):
        entry = getattr(rec, "event_log_entry", None) or rec
        evt = entry.dagster_event
        eval_data = evt.event_specific_data
        check_name = getattr(eval_data, "check_name", "") or ""
        asset_key = getattr(eval_data, "asset_key", None)
        return _default_tokens(
            event_type="asset_check_severity",
            asset_key=asset_key.to_user_string() if asset_key else "",
            status=trigger.severity,
            message=f"Check '{check_name}' at severity {trigger.severity}",
        )
    return _build_generic_event_sensor(
        name, "ASSET_CHECK_EVALUATION", filter_fn, token_builder, actions, trigger.minimum_interval_seconds, default_status
    )


def _build_op_output_sensor(name, trigger, actions, default_status):
    import re as _re
    step_re = _re.compile(trigger.step_key_pattern)
    def filter_fn(rec):
        entry = getattr(rec, "event_log_entry", None) or rec
        evt = getattr(entry, "dagster_event", None)
        if evt is None:
            return False
        step_key = getattr(evt, "step_key", "") or ""
        if not step_re.search(step_key):
            return False
        if trigger.output_name:
            out = evt.event_specific_data
            output_name = getattr(out, "output_name", None) or ""
            if output_name != trigger.output_name:
                return False
        return True
    def token_builder(rec):
        entry = getattr(rec, "event_log_entry", None) or rec
        evt = entry.dagster_event
        return _default_tokens(
            event_type="op_output",
            run_id=entry.run_id or "",
            asset_key=getattr(evt, "step_key", "") or "",
            message=f"Step {getattr(evt, 'step_key', '')} yielded output",
        )
    return _build_generic_event_sensor(
        name, "STEP_OUTPUT", filter_fn, token_builder, actions, trigger.minimum_interval_seconds, default_status
    )


def _build_materialization_planned_sensor(name, trigger, actions, default_status):
    watched = {dg.AssetKey.from_user_string(k) for k in trigger.asset_keys}
    def filter_fn(rec):
        entry = getattr(rec, "event_log_entry", None) or rec
        evt = getattr(entry, "dagster_event", None)
        if evt is None:
            return False
        plan_data = evt.event_specific_data
        asset_key = getattr(plan_data, "asset_key", None)
        return asset_key in watched
    def token_builder(rec):
        entry = getattr(rec, "event_log_entry", None) or rec
        evt = entry.dagster_event
        plan_data = evt.event_specific_data
        asset_key = getattr(plan_data, "asset_key", None)
        partition = getattr(plan_data, "partition", None) or ""
        tokens = _default_tokens(
            event_type="materialization_planned",
            asset_key=asset_key.to_user_string() if asset_key else "",
            partition_key=str(partition),
            message=f"Planned materialization for {asset_key.to_user_string() if asset_key else ''}",
        )
        for dim, val in (_split_multi_partition(partition) or {}).items():
            tokens[f"partition_{dim}"] = val
        return tokens
    return _build_generic_event_sensor(
        name, "ASSET_MATERIALIZATION_PLANNED", filter_fn, token_builder, actions, trigger.minimum_interval_seconds, default_status
    )


def _build_asset_check_started_sensor(name, trigger, actions, default_status):
    watched_checks = set(trigger.check_names or [])
    watched_assets = {dg.AssetKey.from_user_string(k) for k in (trigger.asset_keys or [])} if trigger.asset_keys else None
    def filter_fn(rec):
        entry = getattr(rec, "event_log_entry", None) or rec
        evt = getattr(entry, "dagster_event", None)
        if evt is None:
            return False
        planned_data = evt.event_specific_data
        check_name = getattr(planned_data, "check_name", "") or ""
        if watched_checks and check_name not in watched_checks:
            return False
        asset_key = getattr(planned_data, "asset_key", None)
        if watched_assets and asset_key not in watched_assets:
            return False
        return True
    def token_builder(rec):
        entry = getattr(rec, "event_log_entry", None) or rec
        evt = entry.dagster_event
        planned_data = evt.event_specific_data
        return _default_tokens(
            event_type="asset_check_started",
            asset_key=getattr(planned_data.asset_key, "to_user_string", lambda: "")() if getattr(planned_data, "asset_key", None) else "",
            message=f"Check '{getattr(planned_data, 'check_name', '')}' started",
        )
    return _build_generic_event_sensor(
        name, "ASSET_CHECK_EVALUATION_PLANNED", filter_fn, token_builder, actions, trigger.minimum_interval_seconds, default_status
    )


def _dagster_plus_graphql(query: str, org: str, token: str, deployment: str = "prod"):
    """Shared helper for Dagster+ GraphQL queries. Returns response dict or raises."""
    import json as _json
    import urllib.request
    url = f"https://{org}.dagster.plus/{deployment}/graphql"
    req = urllib.request.Request(
        url,
        data=_json.dumps({"query": query}).encode(),
        headers={"Content-Type": "application/json", "Dagster-Cloud-Api-Token": token},
    )
    return _json.loads(urllib.request.urlopen(req, timeout=10).read())


def _resolve_dagster_plus_context(trigger):
    """Return (org, deployment, token) or None if any missing.

    Dagster+ auto-injects DAGSTER_CLOUD_ORGANIZATION + DAGSTER_CLOUD_DEPLOYMENT_NAME
    into every sensor evaluation environment (both Serverless and Hybrid). The
    API token has to be provisioned by the customer as a Dagster+ secret
    scoped to the code location — that's the one required setup step.
    """
    org = os.environ.get(trigger.org_env_var, "") or os.environ.get("DAGSTER_CLOUD_ORGANIZATION", "")
    deployment = trigger.deployment or os.environ.get("DAGSTER_CLOUD_DEPLOYMENT_NAME", "prod")
    token = os.environ.get(trigger.token_env_var, "")
    if not (org and token):
        return None
    return (org, deployment, token)


def _build_insights_metric_sensor(name, trigger, actions, default_status):
    """Dagster+ Insights metric threshold — VERIFIED against live prod GraphQL.

    Query: `reportingMetricsByDeployment(metricsSelector: {after, before, metricName,
    granularity, aggregationFunction})` returning `{timestamps, metrics: [{values}]}`.

    Metric names are the internal Dagster+ names (e.g. `__DAGSTER_CREDITS_USED_MINUTES`,
    `dagster_cloud.freshness_pass_percentage`) — pull the exact list via the Insights
    UI or `queryableMetrics` GraphQL introspection.
    """
    ops = {
        "gt": lambda v, t: v > t, "gte": lambda v, t: v >= t,
        "lt": lambda v, t: v < t, "lte": lambda v, t: v <= t,
        "eq": lambda v, t: v == t, "neq": lambda v, t: v != t,
    }
    cmp_fn = ops.get(trigger.comparison)

    def _sensor_fn(context: dg.SensorEvaluationContext):
        if cmp_fn is None:
            return dg.SkipReason(f"unsupported comparison '{trigger.comparison}'")
        ctx = _resolve_dagster_plus_context(trigger)
        if ctx is None:
            return dg.SkipReason(
                f"Dagster+ credentials missing — set {trigger.token_env_var} "
                f"as a code-location secret. Org is auto-detected from DAGSTER_CLOUD_ORGANIZATION."
            )
        org, deployment, token = ctx
        # User-configured granularity + aggregation + lookback — the whole
        # point of this trigger vs metric_threshold is time-window aggregate
        # comparison via Insights (Victoria Metrics under the hood).
        now = int(time.time())
        after = now - max(1, trigger.lookback_hours) * 3600
        selector = (
            f'metricsSelector: {{ after: {after}.0, before: {now}.0, '
            f'metricName: "{trigger.metric_name}", '
            f'granularity: {trigger.granularity}, '
            f'aggregationFunction: {trigger.aggregation} }}'
        )
        # Pick the right scoped query. Sibling queries all return the same
        # union response shape (ReportingMetrics / ReportingInputError / etc.),
        # so downstream parsing is identical.
        response_fragment = '''{
            ... on ReportingMetrics { timestamps metrics { values } }
            ... on ReportingInputError { message }
            ... on UnauthorizedError { message }
            ... on PythonError { message }
          }'''
        if trigger.asset_selection:
            _sel = trigger.asset_selection.replace('"', '\\"')
            q = (f'query {{ reportingMetricsByAssetSelection('
                 f'metricsFilter: {{ assetSelection: "{_sel}" }}, {selector}) '
                 f'{response_fragment} }}')
        else:
            q = (f'query {{ reportingMetricsByDeployment({selector}) '
                 f'{response_fragment} }}')
        try:
            resp = _dagster_plus_graphql(q, org, token, deployment)
        except Exception as exc:
            return dg.SkipReason(f"Insights GraphQL query failed: {exc}")
        if "errors" in resp:
            return dg.SkipReason(f"Insights query returned errors: {resp['errors']}")
        # Which top-level query key we used depends on the scope
        if trigger.asset_selection:
            query_key = "reportingMetricsByAssetSelection"
        else:
            query_key = "reportingMetricsByDeployment"
        data = (resp.get("data") or {}).get(query_key) or {}
        # Union result — check for error shape first
        if "message" in data:
            return dg.SkipReason(f"Insights: {data['message']}")
        metrics = data.get("metrics") or []
        if not metrics:
            return dg.SkipReason(f"metric '{trigger.metric_name}' has no data for this scope")
        # Take the most recent non-null value from the first metric result
        values = metrics[0].get("values") or []
        latest = None
        for v in reversed(values):
            if v is not None:
                latest = v
                break
        if latest is None:
            return dg.SkipReason(f"metric '{trigger.metric_name}' returned all-null values")
        try:
            val_f = float(latest)
        except (TypeError, ValueError):
            return dg.SkipReason(f"metric value '{latest}' not numeric")
        if not cmp_fn(val_f, trigger.threshold):
            return dg.SkipReason(f"{val_f} not {trigger.comparison} {trigger.threshold}")
        # Once-per-crossing cursor (only re-fire when value moves back below then re-crosses)
        crossed_key = f"crossed:{val_f}"
        if context.cursor == crossed_key:
            return dg.SkipReason("already alerted for this value")
        context.update_cursor(crossed_key)
        tokens = _default_tokens(
            event_type="insights_metric",
            status=trigger.metric_name,
            message=f"{trigger.metric_name}={val_f} {trigger.comparison} {trigger.threshold}",
        )
        requests_out = _run_actions(actions, tokens, context.log, instance=context.instance, throttle=trigger.throttle, sensor_name=name)
        return dg.SensorResult(run_requests=requests_out) if requests_out else None

    return dg.SensorDefinition(
        name=name, evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds, default_status=default_status,
    )


def _build_dagster_plus_audit_sensor(name, trigger, actions, default_status):
    """Dagster+ audit log — VERIFIED against live prod GraphQL.

    Query: `auditLog.auditLogEntries(filters: {afterDatetime, beforeDatetime, ...},
    limit)` returning entries with `{id, eventType, authorUserEmail,
    authorAgentTokenId, eventMetadata, timestamp, deploymentName,
    branchDeploymentName}`.

    Audit log is a Dagster+ Pro feature; sensor returns SkipReason cleanly if
    it's not enabled or accessible.
    """
    import re as _re
    etype_re = _re.compile(trigger.event_type_pattern) if trigger.event_type_pattern else None
    actor_re = _re.compile(trigger.actor_pattern) if trigger.actor_pattern else None

    def _sensor_fn(context: dg.SensorEvaluationContext):
        ctx = _resolve_dagster_plus_context(trigger)
        if ctx is None:
            return dg.SkipReason(
                f"Dagster+ credentials missing — set {trigger.token_env_var} "
                f"as a code-location secret. Org is auto-detected from DAGSTER_CLOUD_ORGANIZATION."
            )
        org, deployment, token = ctx
        # Look back to the last-seen timestamp (or 1 hour if starting fresh)
        try:
            last_ts = float(context.cursor) if context.cursor else 0.0
        except (TypeError, ValueError):
            last_ts = 0.0
        now = float(int(time.time()))
        after = last_ts if last_ts > 0 else (now - 3600)
        # Build push-down filter list — GraphQL enum values are unquoted;
        # string list values are quoted. Skip any filter that isn't set.
        filter_parts = [f"afterDatetime: {after}", f"beforeDatetime: {now}"]
        if trigger.event_types:
            enums = ", ".join(str(e) for e in trigger.event_types)
            filter_parts.append(f"eventTypes: [{enums}]")
        if trigger.user_emails:
            emails = ", ".join(f'"{e}"' for e in trigger.user_emails)
            filter_parts.append(f"userEmails: [{emails}]")
        if trigger.deployment_names:
            deps = ", ".join(f'"{d}"' for d in trigger.deployment_names)
            filter_parts.append(f"deploymentNames: [{deps}]")
        if trigger.is_branch_deployment is not None:
            filter_parts.append(f"isBranchDeployment: {str(trigger.is_branch_deployment).lower()}")
        filter_str = ", ".join(filter_parts)
        q = f'''query {{
          auditLog {{
            enabled
            auditLogEntries(filters: {{ {filter_str} }}, limit: 100) {{
              id eventType authorUserEmail authorAgentTokenId timestamp
              deploymentName branchDeploymentName eventMetadata
            }}
          }}
        }}'''
        try:
            resp = _dagster_plus_graphql(q, org, token, deployment)
        except Exception as exc:
            return dg.SkipReason(f"audit log GraphQL query failed: {exc}")
        if "errors" in resp:
            return dg.SkipReason(f"audit log query returned errors: {resp['errors']}")
        audit = (resp.get("data") or {}).get("auditLog") or {}
        if not audit.get("enabled"):
            return dg.SkipReason("audit log not enabled on this Dagster+ org")
        entries = audit.get("auditLogEntries") or []
        all_requests = []
        max_ts = last_ts
        # Sort ascending by timestamp so we fire in chronological order
        entries.sort(key=lambda e: e.get("timestamp") or 0)
        for entry in entries:
            etype = entry.get("eventType") or ""
            actor = entry.get("authorUserEmail") or entry.get("authorAgentTokenId") or ""
            ts = float(entry.get("timestamp") or 0)
            if etype_re and not etype_re.search(etype):
                continue
            if actor_re and not actor_re.search(actor):
                continue
            deployment_name = entry.get("deploymentName") or ""
            tokens = _default_tokens(
                event_type="dagster_plus_audit",
                # Dedicated audit tokens (natural naming for templates)
                audit_event_type=etype,
                actor=actor,
                deployment=deployment_name,
                # Legacy token-squatting kept for backward compat
                status=etype,
                job_name=actor,
                timestamp=int(ts),
                message=f"Audit: {actor} → {etype} (deployment={deployment_name})",
            )
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=context.instance, throttle=trigger.throttle, sensor_name=name))
            if ts > max_ts:
                max_ts = ts
        if max_ts > last_ts:
            context.update_cursor(str(max_ts))
        return dg.SensorResult(run_requests=all_requests) if all_requests else dg.SkipReason("no matching audit entries")

    return dg.SensorDefinition(
        name=name, evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds, default_status=default_status,
    )


def _build_code_location_status_sensor(
    name: str, trigger: CodeLocationStatusTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    import re as _re
    loc_re = _re.compile(trigger.location_name_pattern) if trigger.location_name_pattern else None

    def _sensor_fn(context: dg.SensorEvaluationContext):
        instance = context.instance
        # Instance API for workspace / code location snapshots varies across
        # Dagster versions. Try multiple candidates and no-op on failure.
        snapshots = None
        for candidate in ("get_code_location_snapshots", "all_code_location_snapshots", "workspace_snapshot"):
            fn = getattr(instance, candidate, None)
            if fn:
                try:
                    snapshots = fn()
                    break
                except Exception:
                    continue
        if snapshots is None:
            return dg.SkipReason("code location snapshot API unavailable on this instance")
        try:
            already_alerted = json.loads(context.cursor) if context.cursor else {}
        except Exception:
            already_alerted = {}
        now = time.time()
        alerts = []
        # Snapshots shape varies — try dict-of-name→snapshot or list-of-snapshots
        items = snapshots.items() if hasattr(snapshots, "items") else enumerate(snapshots)
        want_error = trigger.on_status in ("ERROR", "UNHEALTHY", "TIMED_OUT")
        want_loading = trigger.on_status == "LOADING"
        for _key, snap in items:
            loc_name = getattr(snap, "location_name", None) or getattr(snap, "name", None) or str(_key)
            if loc_re and not loc_re.search(loc_name):
                continue
            load_status = getattr(snap, "load_status", None) or getattr(snap, "status", None)
            status_str = getattr(load_status, "value", None) or str(load_status) if load_status else ""
            load_ts = getattr(snap, "load_timestamp", None) or getattr(snap, "start_time", None) or 0
            error = getattr(snap, "load_error", None) or getattr(snap, "error", None)
            fire = False
            reason = ""
            if want_error and (status_str in ("ERROR", "TIMED_OUT") or error):
                fire = True
                reason = str(error)[:300] if error else status_str
            elif want_loading and status_str == "LOADING":
                age = now - float(load_ts or 0)
                if age > trigger.max_seconds_loading:
                    fire = True
                    reason = f"loading for {age:.1f}s (limit {trigger.max_seconds_loading}s)"
            if not fire:
                # Reset once-alerted so a future transition can fire again
                already_alerted.pop(loc_name, None)
                continue
            key = f"{loc_name}:{status_str}"
            if already_alerted.get(key):
                continue
            already_alerted[key] = now
            alerts.append((loc_name, status_str, reason))
        all_requests = []
        for loc_name, status_str, reason in alerts:
            tokens = _default_tokens(
                event_type="code_location_status",
                status=status_str,
                job_name=loc_name,
                message=f"Code location {loc_name} → {status_str}: {reason}",
            )
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance, throttle=trigger.throttle, sensor_name=name))
        context.update_cursor(json.dumps(already_alerted))
        return dg.SensorResult(run_requests=all_requests) if all_requests else dg.SkipReason("all locations healthy")

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds,
        default_status=default_status,
    )


def _build_run_startup_slow_sensor(
    name: str, trigger: RunStartupSlowTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    def _sensor_fn(context: dg.SensorEvaluationContext):
        instance = context.instance
        try:
            already_alerted = set(json.loads(context.cursor)) if context.cursor else set()
        except Exception:
            already_alerted = set()
        # Look at runs that have started recently — check the gap between
        # creation timestamp and start_time
        recent = instance.get_runs(
            filters=dg.RunsFilter(
                statuses=[dg.DagsterRunStatus.STARTED, dg.DagsterRunStatus.SUCCESS, dg.DagsterRunStatus.FAILURE]
            ),
            limit=50,
        )
        alerts = []
        for run in recent:
            if run.run_id in already_alerted:
                continue
            if not _run_matches_filters(
                run, trigger.job_name, trigger.job_name_pattern, trigger.run_tags
            ):
                continue
            create_ts = getattr(run, "create_timestamp", None) or getattr(run, "creation_timestamp", None)
            start_ts = getattr(run, "start_time", None)
            if not (create_ts and start_ts):
                continue
            try:
                startup = float(start_ts) - float(create_ts)
            except (TypeError, ValueError):
                continue
            if startup <= trigger.max_startup_seconds:
                continue
            alerts.append((run, startup))
            already_alerted.add(run.run_id)
        all_requests = []
        for run, startup in alerts:
            tokens = _default_tokens(
                event_type="run_startup_slow",
                run_id=run.run_id,
                job_name=run.job_name or "",
                message=f"Run {run.run_id[:8]} took {startup:.1f}s to start (limit {trigger.max_startup_seconds}s)",
            )
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance, throttle=trigger.throttle, sensor_name=name))
        # Cap cursor size at 200 run ids
        capped = list(already_alerted)[-200:]
        context.update_cursor(json.dumps(capped))
        return dg.SensorResult(run_requests=all_requests) if all_requests else dg.SkipReason("no slow startups")

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds,
        default_status=default_status,
    )


def _build_asset_observation_sensor(
    name: str, trigger: AssetObservationTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    def _sensor_fn(context: dg.SensorEvaluationContext):
        from dagster._core.events import DagsterEventType
        instance = context.instance
        last_seen_id = int(context.cursor) if (context.cursor or "").isdigit() else 0
        watched = {dg.AssetKey.from_user_string(k) for k in trigger.asset_keys}
        # Fetch recent ASSET_OBSERVATION events
        try:
            records = instance.event_log_storage.get_event_records(
                event_records_filter=dg.EventRecordsFilter(
                    event_type=DagsterEventType.ASSET_OBSERVATION,
                    after_cursor=last_seen_id if last_seen_id else None,
                ),
                limit=100,
                ascending=True,
            )
        except Exception as exc:
            return dg.SkipReason(f"event fetch failed: {exc}")
        all_requests = []
        max_id = last_seen_id
        for rec in records:
            entry = getattr(rec, "event_log_entry", None) or rec
            evt = getattr(entry, "dagster_event", None)
            if evt is None:
                continue
            obs = evt.event_specific_data
            asset_key = getattr(obs, "asset_key", None)
            if asset_key is None or asset_key not in watched:
                continue
            tokens = _default_tokens(
                event_type="asset_observation",
                asset_key=asset_key.to_user_string(),
                message=f"Observation on {asset_key.to_user_string()}",
            )
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance, throttle=trigger.throttle, sensor_name=name))
            rid = getattr(rec, "storage_id", 0)
            if rid > max_id:
                max_id = rid
        if max_id > last_seen_id:
            context.update_cursor(str(max_id))
        return dg.SensorResult(run_requests=all_requests) if all_requests else dg.SkipReason("no observations")

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds,
        default_status=default_status,
    )


def _build_step_error_sensor(
    name: str, trigger: StepErrorTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    import re as _re
    step_re = _re.compile(trigger.step_key_pattern) if trigger.step_key_pattern else None
    exc_re = _re.compile(trigger.exception_pattern) if trigger.exception_pattern else None

    def _sensor_fn(context: dg.SensorEvaluationContext):
        from dagster._core.events import DagsterEventType
        instance = context.instance
        last_seen_id = int(context.cursor) if (context.cursor or "").isdigit() else 0
        try:
            records = instance.event_log_storage.get_event_records(
                event_records_filter=dg.EventRecordsFilter(
                    event_type=DagsterEventType.STEP_FAILURE,
                    after_cursor=last_seen_id if last_seen_id else None,
                ),
                limit=100,
                ascending=True,
            )
        except Exception as exc:
            return dg.SkipReason(f"event fetch failed: {exc}")
        all_requests = []
        max_id = last_seen_id
        for rec in records:
            entry = getattr(rec, "event_log_entry", None) or rec
            evt = getattr(entry, "dagster_event", None)
            if evt is None:
                continue
            step_key = getattr(evt, "step_key", None) or ""
            run_id = getattr(entry, "run_id", "") or ""
            # Filter by run's job_name / job_name_pattern / run_tags
            if trigger.job_name or trigger.job_name_pattern or trigger.run_tags:
                try:
                    run = instance.get_run_by_id(run_id)
                    if run is None or not _run_matches_filters(
                        run, trigger.job_name, trigger.job_name_pattern, trigger.run_tags
                    ):
                        rid = getattr(rec, "storage_id", 0)
                        max_id = max(max_id, rid)
                        continue
                except Exception:
                    continue
            if step_re and not step_re.search(step_key):
                rid = getattr(rec, "storage_id", 0)
                max_id = max(max_id, rid)
                continue
            # Extract exception message
            failure_data = evt.event_specific_data
            error = getattr(failure_data, "error", None)
            exc_msg = getattr(error, "message", "") if error else ""
            if exc_re and not exc_re.search(exc_msg or ""):
                rid = getattr(rec, "storage_id", 0)
                max_id = max(max_id, rid)
                continue
            # Retry-aware: skip attempts that will be retried. We check the
            # step's *current* status — if Dagster shows it as anything other
            # than FAILURE (e.g. RETRY_REQUESTED, IN_PROGRESS, SUCCESS), a
            # retry is pending or has already recovered the step.
            if trigger.only_final_failures:
                try:
                    stats = instance.get_run_step_stats(run_id, step_keys=[step_key])
                    if not stats:
                        # No stats yet — retry may still be pending; skip
                        # this cursor so we re-evaluate next tick.
                        continue
                    stat_status = getattr(stats[0], "status", None)
                    status_name = getattr(stat_status, "name", str(stat_status)).upper()
                    if status_name != "FAILURE":
                        # Step is running, succeeded via retry, or awaiting
                        # retry — this attempt isn't final; skip. Don't
                        # advance cursor so we re-check next tick.
                        context.log.info(
                            f"[step_error:{step_key}] skipping non-final failure "
                            f"(current status: {status_name}) — waiting for retries to finish"
                        )
                        continue
                except Exception as exc:
                    context.log.warning(
                        f"[step_error:{step_key}] could not verify final status "
                        f"({exc}) — firing anyway"
                    )
            tokens = _default_tokens(
                event_type="step_error",
                run_id=run_id,
                asset_key=step_key,
                status="FAILURE",
                message=(exc_msg or f"Step {step_key} failed")[:500],
            )
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance, throttle=trigger.throttle, sensor_name=name))
            rid = getattr(rec, "storage_id", 0)
            if rid > max_id:
                max_id = rid
        if max_id > last_seen_id:
            context.update_cursor(str(max_id))
        return dg.SensorResult(run_requests=all_requests) if all_requests else dg.SkipReason("no step errors")

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds,
        default_status=default_status,
    )


def _build_metadata_match_sensor(
    name: str, trigger: MetadataMatchTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    import re as _re
    value_re = _re.compile(trigger.regex) if trigger.regex else None
    equals_val = trigger.equals

    def _extract(mval):
        if mval is None:
            return None
        for attr in ("value", "text", "url", "path", "float_value", "int_value"):
            if hasattr(mval, attr):
                v = getattr(mval, attr)
                if v is not None:
                    return v
        return mval

    def _sensor_fn(context: dg.SensorEvaluationContext):
        from dagster._core.events import DagsterEventType
        instance = context.instance
        last_seen_id = int(context.cursor) if (context.cursor or "").isdigit() else 0
        watched = dg.AssetKey.from_user_string(trigger.asset_key)
        event_types = [DagsterEventType.ASSET_MATERIALIZATION]
        if trigger.include_observations:
            event_types.append(DagsterEventType.ASSET_OBSERVATION)
        all_requests = []
        max_id = last_seen_id
        for etype in event_types:
            try:
                records = instance.event_log_storage.get_event_records(
                    event_records_filter=dg.EventRecordsFilter(
                        event_type=etype,
                        after_cursor=last_seen_id if last_seen_id else None,
                    ),
                    limit=50,
                    ascending=True,
                )
            except Exception:
                continue
            for rec in records:
                entry = getattr(rec, "event_log_entry", None) or rec
                evt = getattr(entry, "dagster_event", None)
                if evt is None:
                    continue
                data = evt.event_specific_data
                asset_key = getattr(data, "asset_key", None)
                if asset_key != watched:
                    continue
                # Find the materialization / observation object
                mat = getattr(data, "materialization", None) or getattr(data, "asset_observation", None) or data
                meta = getattr(mat, "metadata", None) or {}
                mval = meta.get(trigger.metadata_key)
                if mval is None:
                    continue
                extracted = _extract(mval)
                if extracted is None:
                    continue
                val_str = str(extracted)
                if equals_val is not None and val_str != equals_val:
                    continue
                if value_re is not None and not value_re.search(val_str):
                    continue
                tokens = _default_tokens(
                    event_type="metadata_match",
                    asset_key=trigger.asset_key,
                    status=val_str[:100],
                    message=f"{trigger.metadata_key}={val_str}",
                )
                all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance, throttle=trigger.throttle, sensor_name=name))
                rid = getattr(rec, "storage_id", 0)
                if rid > max_id:
                    max_id = rid
        if max_id > last_seen_id:
            context.update_cursor(str(max_id))
        return dg.SensorResult(run_requests=all_requests) if all_requests else dg.SkipReason("no metadata matches")

    return dg.SensorDefinition(
        name=name,
        evaluation_fn=_sensor_fn,
        minimum_interval_seconds=trigger.minimum_interval_seconds,
        default_status=default_status,
    )


def _build_daemon_heartbeat_sensor(
    name: str, trigger: DaemonHeartbeatTrigger, actions: List[Action], default_status: dg.DefaultSensorStatus
) -> dg.SensorDefinition:
    def _sensor_fn(context: dg.SensorEvaluationContext):
        instance = context.instance
        try:
            statuses = instance.get_daemon_statuses()
        except Exception as exc:
            return dg.SkipReason(f"get_daemon_statuses unavailable: {exc}")
        # statuses is dict {daemon_type: DaemonStatus}
        try:
            already_alerted = json.loads(context.cursor) if context.cursor else {}
        except Exception:
            already_alerted = {}
        now = time.time()
        stale = []
        for daemon_type, status in (statuses.items() if hasattr(statuses, "items") else []):
            if trigger.daemon_type and daemon_type != trigger.daemon_type:
                continue
            last_hb = getattr(status, "last_heartbeat", None)
            if last_hb is None:
                continue
            ts = getattr(last_hb, "timestamp", None) or getattr(last_hb, "run_timestamp", None) or 0
            try:
                ts_float = float(ts)
            except (TypeError, ValueError):
                continue
            age = now - ts_float
            if age > trigger.max_seconds_since_heartbeat:
                # Once-per-outage: only fire if last-alerted timestamp is older
                if already_alerted.get(daemon_type, 0) >= ts_float:
                    continue
                stale.append((daemon_type, age))
                already_alerted[daemon_type] = now
        all_requests = []
        for daemon_type, age in stale:
            tokens = _default_tokens(
                event_type="daemon_stale",
                status=daemon_type,
                message=f"{daemon_type} daemon has not heartbeat in {age:.1f}s (limit {trigger.max_seconds_since_heartbeat}s)",
            )
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance, throttle=trigger.throttle, sensor_name=name))
        context.update_cursor(json.dumps(already_alerted))
        return dg.SensorResult(run_requests=all_requests) if all_requests else dg.SkipReason("all daemons healthy")

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
        requests_out = _run_actions(actions, tokens, context.log, instance=instance, throttle=trigger.throttle, sensor_name=name)
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
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=instance, throttle=trigger.throttle, sensor_name=name))
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
        requests_out = _run_actions(actions, tokens, context.log, instance=instance, throttle=trigger.throttle, sensor_name=name)
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
        requests_out = _run_actions(actions, tokens, context.log, instance=instance, throttle=trigger.throttle, sensor_name=name)
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
            all_requests.extend(_run_actions(actions, tokens, context.log, instance=context.instance, throttle=trigger.throttle, sensor_name=name))
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
        requests_out = _run_actions(actions, tokens, context.log, instance=instance, throttle=trigger.throttle, sensor_name=name)
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

    def _normalize_asset_selectors(self, triggers, sibling_defs, discovered_keys):
        """Walk triggers (including nested compounds) and replace any
        `asset_keys: str` value with the resolved concrete list. Backward
        compat: existing `List[str]` values pass through untouched."""
        for trig in triggers:
            if isinstance(trig, (AllOfTrigger, AnyOfTrigger)):
                self._normalize_asset_selectors(trig.triggers, sibling_defs, discovered_keys)
                continue
            keys = getattr(trig, "asset_keys", None)
            if isinstance(keys, str):
                trig.asset_keys = _resolve_asset_keys(keys, sibling_defs, discovered_keys)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        sensor_status = (
            dg.DefaultSensorStatus.RUNNING
            if self.default_status.upper() == "RUNNING"
            else dg.DefaultSensorStatus.STOPPED
        )
        # Resolve asset-selection strings on every trigger to concrete key
        # lists, using the sibling assets in the same defs folder. Backward
        # compat: list values pass through untouched.
        sibling_defs, discovered_keys = _discover_sibling_assets(context)
        self._normalize_asset_selectors(self.when, sibling_defs, discovered_keys)
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
            elif isinstance(trigger, DaemonHeartbeatTrigger):
                sensors.append(_build_daemon_heartbeat_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, CodeLocationStatusTrigger):
                sensors.append(_build_code_location_status_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, RunStartupSlowTrigger):
                sensors.append(_build_run_startup_slow_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, AssetObservationTrigger):
                sensors.append(_build_asset_observation_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, StepErrorTrigger):
                sensors.append(_build_step_error_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, MetadataMatchTrigger):
                sensors.append(_build_metadata_match_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, HookFiredTrigger):
                sensors.append(_build_hook_fired_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, AssetPartitionMaterializedTrigger):
                sensors.append(_build_asset_partition_materialized_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, RunReexecutionTrigger):
                sensors.append(_build_run_reexecution_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, AssetWipeTrigger):
                sensors.append(_build_asset_wipe_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, ConfigOverrideTrigger):
                sensors.append(_build_config_override_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, TagSetTrigger):
                sensors.append(_build_tag_set_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, UnhandledExceptionTrigger):
                sensors.append(_build_unhandled_exception_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, AssetCheckSeverityTrigger):
                sensors.append(_build_asset_check_severity_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, OpOutputTrigger):
                sensors.append(_build_op_output_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, MaterializationPlannedTrigger):
                sensors.append(_build_materialization_planned_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, AssetCheckStartedTrigger):
                sensors.append(_build_asset_check_started_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, InsightsMetricThresholdTrigger):
                sensors.append(_build_insights_metric_sensor(child_name, trigger, self.then, sensor_status))
            elif isinstance(trigger, DagsterPlusAuditTrigger):
                sensors.append(_build_dagster_plus_audit_sensor(child_name, trigger, self.then, sensor_status))
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
