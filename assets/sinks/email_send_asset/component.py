"""Email Send Asset Component.

Send one email as a Dagster asset materialization -- a mid-chain notification
step (e.g. RunMyJobs' `System_Mail_Send`: To, Subject, Text, JobFile), not a
bulk per-row campaign tool (that's `smtp_send_asset`'s job). Goes through an
already-registered `smtp_resource` or `sendgrid_resource` -- this is the first
real consumer of either resource's registered key in this repo (both
`smtp_send_asset` and `email_approval_gate` reimplement SMTP sending from
scratch instead of referencing a shared resource).

Self-contained: nothing is inlined here beyond the standard shared boilerplate
blocks -- `resource.send_email(...)` is called through `context.resources`,
never by importing `SMTPResource`/`SendGridResource` directly (a component.py
must not import another component).
"""
import mimetypes
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import dagster as dg
from pydantic import Field, model_validator


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


# BEGIN run_window (inlined per component; keep copies identical — ported verbatim from
# sensors/event_automation's own business_hours_only parser/checker, same format and same
# fail-open-on-malformed-input behavior, just renamed for this field's own name)
_WEEKDAY_MAP = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


def _parse_run_window(spec: str) -> Tuple[int, int, str, Optional[List[int]]]:
    """Parse 'HH:MM-HH:MM tz [day,day,...]' into (start_min, end_min, tz, weekdays). Raises
    ValueError on malformed input."""
    parts = spec.strip().split()
    if len(parts) < 2:
        raise ValueError(f"run_window needs 'HH:MM-HH:MM tz [days]', got '{spec}'")
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


def _is_in_run_window(now_ts: float, spec: str) -> bool:
    """True if now_ts falls in the daily window (and optional day-of-week list). Fails open
    (returns True -- never blocks) on a malformed spec or any unexpected error."""
    try:
        start_min, end_min, tz_name, weekdays = _parse_run_window(spec)
    except Exception:
        return True
    try:
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
        return cur_min >= start_min or cur_min < end_min  # overnight window, e.g. 22:00-06:00
    except Exception:
        return True
# END run_window


class EmailSendAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Send one email through a registered `smtp_resource` or `sendgrid_resource`.

    ```yaml
    type: dagster_community_components.EmailSendAssetComponent
    attributes:
      asset_key: notifications/nightly_close_complete
      provider: smtp
      resource_key: smtp_resource
      to: [ops-team@example.com]
      subject: "Nightly close complete"
      body: "The nightly close chain finished successfully."
      deps: [sap/nightly_close]
    ```

    With attachments (the RunMyJobs `System_Mail_Send` shape: To, Subject, Text, JobFile):

    ```yaml
    type: dagster_community_components.EmailSendAssetComponent
    attributes:
      asset_key: notifications/report_ready
      provider: sendgrid
      resource_key: sendgrid
      from_addr: reports@example.com
      to: [finance@example.com]
      cc: [controller@example.com]
      subject: "Monthly report"
      body: "See attached."
      attachment_paths: ["/data/out/monthly_report.csv"]
    ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'notifications/nightly_close_complete').")
    provider: Literal["smtp", "sendgrid"] = Field(
        default="smtp",
        description="Which registered resource's send_email(...) to call -- an SMTPResourceComponent's or a SendGridResourceComponent's resource_key.",
    )
    resource_key: str = Field(description="Key of the registered smtp_resource / sendgrid_resource (matching `provider`).")

    to: List[str] = Field(description="Recipient email addresses.")
    cc: Optional[List[str]] = Field(default=None, description="Cc recipient email addresses.")
    bcc: Optional[List[str]] = Field(default=None, description="Bcc recipient email addresses -- receive the message, never added as a header.")
    subject: str = Field(description="Email subject.")
    body: str = Field(description="Plain-text email body.")
    html_body: Optional[str] = Field(default=None, description="Optional HTML alternative body (multipart/alternative alongside `body`).")
    reply_to: Optional[str] = Field(default=None, description="Reply-To address.")
    from_addr: Optional[str] = Field(
        default=None,
        description="Sender address. For provider='smtp', defaults to the resource's own configured username when unset. For provider='sendgrid', required (SendGrid has no notion of a default sender tied to API-key auth).",
    )
    attachment_paths: Optional[List[str]] = Field(
        default=None,
        description="Local file paths to attach, read and attached at materialization time. MIME type is guessed from the file extension.",
    )

    dry_run: bool = Field(
        default=False,
        description=(
            "Log what would be sent (recipients, subject, attachment paths) and skip the actual "
            "send -- no resource is looked up, nothing goes over the network. Output metadata "
            "always includes dry_run so a materialization history shows which runs were real."
        ),
    )
    run_window: Optional[str] = Field(
        default=None,
        description=(
            "'HH:MM-HH:MM tz [day,day,...]' (e.g. '09:00-17:00 America/New_York "
            "mon,tue,wed,thu,fri'). Outside this window, the asset SKIPS cleanly instead of "
            "sending or failing -- no email goes out. Checked once per materialization, before "
            "any send. A malformed spec fails open (never blocks). Unset (default): always runs."
        ),
    )
    pool: Optional[str] = Field(
        default=None,
        description=(
            "Dagster's native named concurrency pool (e.g. Airflow's `pool` -- a fixed number "
            "of slots shared across every asset/op using the same pool name, e.g. an SMTP-server "
            "connection-rate cap). The pool's own slot count is configured at the deployment "
            "level (dagster.yaml's concurrency: pools:, or the Dagster+ UI), not here."
        ),
    )

    group_name: Optional[str] = Field(default="notifications", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys for lineage (e.g. the chain step(s) this notification follows).")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog. Auto-includes 'email' if empty.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(
            data, lists=("to", "cc", "bcc", "deps", "owners", "kinds", "attachment_paths"), tags=("asset_tags",),
        )

    @model_validator(mode="after")
    def _validate_sender(self):
        if self.provider == "sendgrid" and not self.from_addr and not self.dry_run:
            raise ValueError(
                "EmailSendAssetComponent: provider='sendgrid' requires from_addr (SendGrid has no default sender)."
            )
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Email Send Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        kinds = set(component.kinds) if component.kinds else {"email"}
        dry_run = component.dry_run
        run_window = component.run_window
        rk = component.resource_key
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (component.deps or [])]

        _retry_policy = None
        if component.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=component.retry_policy_max_retries,
                delay=component.retry_policy_delay_seconds or 1,
                backoff=Backoff[component.retry_policy_backoff.upper()],
            )

        outs = {
            "result": dg.AssetOut(
                key=dg.AssetKey(component.asset_key.split("/")),
                description=component.description or f"Send an email ({component.subject!r}) via {component.provider}",
                is_required=False,
                kinds=kinds,
                owners=component.owners or None,
                tags=component.asset_tags or None,
            ),
        }

        @dg.multi_asset(
            name=dg.AssetKey(component.asset_key.split("/")).to_python_identifier(),
            outs=outs,
            group_name=component.group_name,
            deps=deps_keys,
            required_resource_keys={rk} if not dry_run else None,
            retry_policy=_retry_policy,
            pool=component.pool,
        )
        def _email_send_asset(context: dg.AssetExecutionContext):
            if run_window and not _is_in_run_window(time.time(), run_window):
                context.log.info(f"run_window={run_window!r}: outside the configured window -- skipping (not failing).")
                return
            yield _execute(context)

        def _read_attachments() -> List[Tuple[str, bytes, str]]:
            attachments = []
            for path in component.attachment_paths or []:
                with open(path, "rb") as f:
                    content = f.read()
                filename = os.path.basename(path)
                mime_type = mimetypes.guess_type(path)[0] or "application/octet-stream"
                attachments.append((filename, content, mime_type))
            return attachments

        def _execute(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            if dry_run:
                context.log.info(
                    f"[dry_run] would send via {component.provider}: to={component.to} "
                    f"cc={component.cc or []} bcc={component.bcc or []} subject={component.subject!r} "
                    f"attachments={component.attachment_paths or []}"
                )
                return dg.MaterializeResult(metadata={
                    "dry_run": dg.MetadataValue.bool(True),
                    "provider": dg.MetadataValue.text(component.provider),
                    "to": dg.MetadataValue.json(component.to),
                    "subject": dg.MetadataValue.text(component.subject),
                })

            resource = getattr(context.resources, rk)
            attachments = _read_attachments()
            resource.send_email(
                to=", ".join(component.to),
                subject=component.subject,
                body=component.body,
                from_addr=component.from_addr or "",
                cc=component.cc or None,
                bcc=component.bcc or None,
                html_body=component.html_body,
                reply_to=component.reply_to,
                attachments=attachments or None,
            )
            context.log.info(f"Email sent via {component.provider}: to={component.to} subject={component.subject!r}")
            return dg.MaterializeResult(metadata={
                "dry_run": dg.MetadataValue.bool(False),
                "provider": dg.MetadataValue.text(component.provider),
                "to": dg.MetadataValue.json(component.to),
                "cc": dg.MetadataValue.json(component.cc or []),
                "bcc_count": dg.MetadataValue.int(len(component.bcc or [])),
                "subject": dg.MetadataValue.text(component.subject),
                "attachment_count": dg.MetadataValue.int(len(attachments)),
            })

        return dg.Definitions(assets=[_email_send_asset])
