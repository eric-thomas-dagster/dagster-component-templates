"""SentryMonitorCheckinJobComponent.

Fire a Sentry Cron Monitoring check-in as a job. Sentry watches for missing
check-ins and pages on-call when the schedule silently drops a beat.

Use case: register a Dagster scheduled job under a Sentry monitor. Every
run reports 'in_progress' at start and 'ok' at end. If Sentry doesn't
see the expected check-in within the schedule window (+ margin), it
raises an alert automatically.

Docs: https://docs.sentry.io/product/crons/
"""

import os
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field


class SentryMonitorCheckinJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Fire a Sentry Cron Monitoring check-in as a scheduled job."""

    job_name: str = Field(description="Dagster job name.")
    schedule: Optional[str] = Field(
        default=None,
        description="Cron schedule (None = no schedule; can still trigger manually).",
    )
    default_status: str = Field(
        default="STOPPED",
        description="'STOPPED' or 'RUNNING' — controls the Dagster schedule's default status.",
    )
    tags: Optional[Dict[str, str]] = Field(
        default=None, description="Dagster job tags.",
    )

    # --- Sentry Cron Monitor fields -----------------------------------------

    dsn_env_var: str = Field(
        default="SENTRY_DSN",
        description="Env var holding the Sentry DSN.",
    )
    monitor_slug: str = Field(
        description=(
            "The monitor slug (as configured in Sentry or auto-registered via "
            "monitor_config on first check-in). Keep it stable across runs."
        ),
    )
    environment: Optional[str] = Field(
        default=None,
        description="Sentry environment tag (e.g. 'prod', 'staging').",
    )
    release: Optional[str] = Field(
        default=None,
        description="Sentry release tag.",
    )

    # Optional: define the monitor's schedule + policies so Sentry auto-creates
    # the monitor on first check-in. If omitted, the monitor must be pre-created
    # in the Sentry UI.
    monitor_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Sentry monitor_config dict — see "
            "https://docs.sentry.io/product/crons/getting-started/http/. "
            "Example: "
            "{schedule: {type: 'crontab', value: '0 3 * * *'}, checkin_margin: 5, "
            "max_runtime: 15, timezone: 'America/New_York'}. When set, Sentry "
            "auto-creates or updates the monitor on the first check-in."
        ),
    )
    fail_on_error: bool = Field(
        default=False,
        description=(
            "If True, the job step fails when the Sentry check-in raises. Default "
            "False for cron monitors — you don't want a Sentry outage to make "
            "your scheduler flap."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_checkin_op")
        def _checkin_op(context: dg.OpExecutionContext):
            try:
                import sentry_sdk
                from sentry_sdk.crons import capture_checkin
            except ImportError as e:
                if _self.fail_on_error:
                    raise ImportError(
                        "sentry-sdk not installed. `uv add sentry-sdk` first."
                    ) from e
                context.log.warning("sentry-sdk not installed; skipping check-in")
                return

            dsn = os.environ.get(_self.dsn_env_var)
            if not dsn:
                _err = f"{_self.dsn_env_var!r} env var not set."
                if _self.fail_on_error:
                    raise RuntimeError(_err)
                context.log.warning(_err + " (fail_on_error=False, skipping)")
                return

            init_kwargs: Dict[str, Any] = {"dsn": dsn}
            if _self.environment:
                init_kwargs["environment"] = _self.environment
            if _self.release:
                init_kwargs["release"] = _self.release
            sentry_sdk.init(**init_kwargs)

            try:
                # in_progress → ok (successful completion)
                _checkin_id = capture_checkin(
                    monitor_slug=_self.monitor_slug,
                    status="in_progress",
                    monitor_config=_self.monitor_config,
                )
                context.log.info(
                    f"Sentry check-in in_progress for monitor {_self.monitor_slug!r} "
                    f"(id={_checkin_id})"
                )
                capture_checkin(
                    monitor_slug=_self.monitor_slug,
                    check_in_id=_checkin_id,
                    status="ok",
                )
                context.log.info(
                    f"Sentry check-in OK for monitor {_self.monitor_slug!r}"
                )
                # Flush to ensure the check-in leaves the SDK before the process ends.
                _client = sentry_sdk.Hub.current.client
                if _client is not None:
                    _client.flush(timeout=5)
            except Exception as _e:  # noqa: BLE001
                if _self.fail_on_error:
                    raise
                context.log.warning(
                    f"Sentry check-in failed (fail_on_error=False): {_e}"
                )

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _checkin_op()

        defs_kwargs: Dict[str, Any] = {"jobs": [_the_job]}
        if self.schedule:
            _default = (
                dg.DefaultScheduleStatus.RUNNING
                if str(self.default_status).upper() == "RUNNING"
                else dg.DefaultScheduleStatus.STOPPED
            )
            sched = dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule,
                job=_the_job,
                default_status=_default,
            )
            defs_kwargs["schedules"] = [sched]
        return dg.Definitions(**defs_kwargs)
