"""SentryAlertJobComponent.

Fire a Sentry event on a schedule (or on-demand) — no asset materialized.
Typical use: heartbeat that Sentry watches; or a scheduled report event
that on-call sees in the Sentry inbox.
"""

import os
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


_VALID_LEVELS = {"debug", "info", "warning", "error", "fatal"}



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


class SentryAlertJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Fire a Sentry event as a job — heartbeats, scheduled status reports,
    fire-and-forget notifications. No asset materialized."""

    job_name: str = Field(description="Dagster job name.")
    schedule: Optional[str] = Field(
        default=None,
        description="Cron schedule (None = no schedule; can still trigger manually).",
    )
    default_status: str = Field(
        default="STOPPED",
        description="'STOPPED' or 'RUNNING' — controls the schedule's default status on load.",
    )
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Dagster job tags (not Sentry tags — see sentry_tags for that).",
    )

    # --- Sentry event fields -------------------------------------------------

    dsn_env_var: str = Field(
        default="SENTRY_DSN",
        description="Env var holding the Sentry DSN.",
    )
    message: str = Field(
        description=(
            "Event message text. Supports simple string.format placeholders — "
            "'{now}' is replaced with the current UTC ISO timestamp before send."
        ),
    )
    level: str = Field(
        default="info",
        description="Sentry level: one of debug|info|warning|error|fatal.",
    )
    environment: Optional[str] = Field(
        default=None,
        description="Sentry environment tag (e.g. 'prod').",
    )
    release: Optional[str] = Field(
        default=None,
        description="Sentry release tag (e.g. app version or git SHA).",
    )
    sentry_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Searchable Sentry tags applied to the event.",
    )
    sentry_extras: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Extra context (non-indexed) attached to the event.",
    )
    fingerprint: Optional[List[str]] = Field(
        default=None,
        description="Sentry fingerprint (grouping key). Default is Sentry's auto-grouping.",
    )
    fail_on_error: bool = Field(
        default=True,
        description=(
            "If True (default) the job step fails when sentry-sdk raises. Set False for "
            "fire-and-forget heartbeats that must not break the schedule if Sentry is down."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=(), csv=(), tags=('tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _sentry_op(context: dg.OpExecutionContext):
            from datetime import datetime, timezone
            try:
                import sentry_sdk
            except ImportError as e:
                if _self.fail_on_error:
                    raise ImportError(
                        "sentry-sdk not installed. `uv add sentry-sdk` or `pip install sentry-sdk`."
                    ) from e
                context.log.warning("sentry-sdk not installed; skipping send")
                return

            dsn = os.environ.get(_self.dsn_env_var)
            if not dsn:
                _err = (
                    f"{_self.dsn_env_var!r} env var not set. Get your DSN at "
                    f"Sentry → Project Settings → Client Keys (DSN)."
                )
                if _self.fail_on_error:
                    raise RuntimeError(_err)
                context.log.warning(_err + " (fail_on_error=False, continuing)")
                return

            _lvl = (_self.level or "info").lower()
            if _lvl not in _VALID_LEVELS:
                _lvl = "info"

            _now = datetime.now(timezone.utc).isoformat()
            _msg = str(_self.message).format(now=_now)

            init_kwargs: Dict[str, Any] = {"dsn": dsn}
            if _self.environment:
                init_kwargs["environment"] = _self.environment
            if _self.release:
                init_kwargs["release"] = _self.release
            client = sentry_sdk.Client(**init_kwargs)
            hub = sentry_sdk.Hub(client)

            try:
                with hub.push_scope() as scope:
                    scope.level = _lvl
                    if _self.sentry_tags:
                        for _k, _v in _self.sentry_tags.items():
                            scope.set_tag(str(_k), str(_v))
                    if _self.sentry_extras:
                        for _k, _v in _self.sentry_extras.items():
                            scope.set_extra(str(_k), _v)
                    if _self.fingerprint:
                        scope.fingerprint = list(_self.fingerprint)
                    hub.capture_message(_msg, level=_lvl)
                client.flush(timeout=5)
                context.log.info(f"Sent Sentry event: level={_lvl} msg={_msg[:120]!r}")
            except Exception as _e:  # noqa: BLE001
                if _self.fail_on_error:
                    raise
                context.log.warning(f"Sentry send failed (fail_on_error=False): {_e}")

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _sentry_op()

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
