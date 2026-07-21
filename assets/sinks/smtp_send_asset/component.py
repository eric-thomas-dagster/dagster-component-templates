"""SmtpSendAssetComponent — send emails via SMTP.

Renders a templated subject/body using upstream-row fields, sends one email
per row (or one summary email per run). Supports plain-text + HTML bodies,
attachments referenced by file path, CC/BCC, and STARTTLS / SSL transports.

Common use:
  - Notifications from a pipeline: failed rows → on-call alert email
  - Outbound automation: invoice rendering → mail to customers
  - Daily digest: aggregated report → distribution list

Auth: SMTP server-specific. Gmail SMTP requires an app password
(smtp.gmail.com:587 with STARTTLS). Outlook/O365 SMTP requires
admin opt-in for basic auth. Self-hosted Postfix usually just works.

Not a replacement for Dagster+ notifications — for run-level alerts use
Dagster+'s built-in alerting. This is for data-row-driven outbound mail.
"""
import re
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import formataddr
from pathlib import Path
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
    """Render `{column}` placeholders against a dict. Missing keys yield empty string."""
    def sub(m):
        key = m.group(1)
        v = row.get(key)
        return "" if v is None else str(v)
    return re.sub(r"\{(\w+)\}", sub, template)


class SmtpSendAssetComponent(Component, Model, Resolvable):
    """Send emails via SMTP, one per upstream DataFrame row (or one summary)."""

    asset_name: str = Field(description="Output asset name (summary row).")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    host: str = Field(description="SMTP server hostname.")
    port: int = Field(default=587, description="SMTP port (587 STARTTLS, 465 SSL, 25 plain).")
    use_ssl: bool = Field(
        default=False,
        description="True for implicit SSL (port 465). For STARTTLS (587), leave False.",
    )
    use_starttls: bool = Field(
        default=True,
        description="Issue STARTTLS upgrade on a plain connection (port 587).",
    )

    username_env_var: str = Field(description="Env var with SMTP username.")
    password_env_var: str = Field(description="Env var with SMTP password (Gmail: app password).")

    sender: str = Field(
        description=(
            "From address. Either bare 'alerts@me.com' or display form "
            "'Pipeline Bot <alerts@me.com>'. Many providers reject mismatch with the auth user."
        )
    )

    mode: str = Field(
        default="per_row",
        description=(
            "'per_row': one email per upstream row, using row fields in templates. "
            "'summary': one email per run, with all rows rendered in the body via summary_template."
        ),
    )

    to_template: str = Field(
        description=(
            "Recipient template — `{column}` placeholders against upstream row. "
            "Example: '{user_email}' or 'alerts@me.com,team@me.com' (literal list)."
        ),
    )
    cc_template: Optional[str] = Field(default=None, description="CC template (optional).")
    bcc_template: Optional[str] = Field(default=None, description="BCC template (optional).")

    subject_template: str = Field(
        default="Pipeline alert",
        description="Subject line, with `{column}` placeholders.",
    )
    body_template: str = Field(
        default="",
        description="Plain-text body, with `{column}` placeholders.",
    )
    html_body_template: Optional[str] = Field(
        default=None,
        description="Optional HTML body. If both are set, recipient sees text/html alternative.",
    )

    summary_template: Optional[str] = Field(
        default=None,
        description="For mode='summary', a template rendered against the whole DataFrame's row "
                    "summary (use Markdown — gets rendered into the body as text).",
    )

    attachment_path_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "Columns whose values are local file paths to attach. Per row in per_row mode; "
            "in summary mode, deduplicated across all rows."
        ),
    )

    dry_run: bool = Field(
        default=False,
        description="If True, render and log every message but don't SMTP-send. Use for staging.",
    )

    max_send: Optional[int] = Field(
        default=None,
        description="Hard cap on emails sent per run. None = no limit.",
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
        host = self.host
        port = self.port
        use_ssl = self.use_ssl
        use_starttls = self.use_starttls
        user_env = self.username_env_var
        pass_env = self.password_env_var
        sender = self.sender
        mode = self.mode
        to_t = self.to_template
        cc_t = self.cc_template
        bcc_t = self.bcc_template
        subj_t = self.subject_template
        body_t = self.body_template
        html_t = self.html_body_template
        summ_t = self.summary_template
        att_cols = self.attachment_path_columns or []
        dry = self.dry_run
        cap = self.max_send

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"SMTP send via {host}:{port} (mode={mode}).",
            group_name=self.group_name,
            kinds={"smtp", "email"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: Any):
            # Defensive Output/MaterializeResult unwrap — see summarize for the rationale.
            # Tolerates upstream authors who annotate `-> Output` or
            # return `Output(value=df, ...)` / `MaterializeResult(value=df)`.
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            username = EnvVar(user_env).get_value()
            password = EnvVar(pass_env).get_value()
            if not (username and password):
                raise ValueError(f"SMTP creds missing — set {user_env!r} and {pass_env!r}.")

            def build_message(row: Dict[str, Any], body_text: str, body_html: Optional[str],
                              extra_attachments: List[str]) -> EmailMessage:
                m = EmailMessage()
                m["From"] = sender
                m["To"] = _render(to_t, row)
                if cc_t:
                    cc = _render(cc_t, row)
                    if cc:
                        m["Cc"] = cc
                if bcc_t:
                    bcc = _render(bcc_t, row)
                    if bcc:
                        m["Bcc"] = bcc
                m["Subject"] = _render(subj_t, row)
                m.set_content(body_text or "")
                if body_html:
                    m.add_alternative(body_html, subtype="html")
                for path in extra_attachments:
                    if not path:
                        continue
                    p = Path(path)
                    if not p.exists():
                        context.log.warning(f"attachment not found: {p} — skipping")
                        continue
                    data = p.read_bytes()
                    # crude MIME inference
                    ext = p.suffix.lower().lstrip(".")
                    maintype, subtype = (
                        ("image", ext) if ext in ("png", "jpg", "jpeg", "gif") else
                        ("application", "pdf") if ext == "pdf" else
                        ("application", "octet-stream")
                    )
                    m.add_attachment(data, maintype=maintype, subtype=subtype, filename=p.name)
                return m

            messages: List[EmailMessage] = []
            if mode == "per_row":
                for _, src in upstream.iterrows():
                    row = src.to_dict()
                    text = _render(body_t, row) if body_t else ""
                    html = _render(html_t, row) if html_t else None
                    extras = [str(row.get(c) or "") for c in att_cols]
                    messages.append(build_message(row, text, html, extras))
                    if cap is not None and len(messages) >= cap:
                        break
            elif mode == "summary":
                row = {"row_count": len(upstream)}
                # Render summary_template against the entire DataFrame as markdown
                if summ_t:
                    # Provide table_md slot
                    table_md = upstream.head(50).to_markdown(index=False) if not upstream.empty else "(empty)"
                    row["table_md"] = table_md
                text = _render(body_t, row) if body_t else (summ_t or "")
                text = _render(text, row)
                html = _render(html_t, row) if html_t else None
                # Deduplicated attachments across all rows
                extras = []
                seen = set()
                for c in att_cols:
                    for v in upstream[c].dropna().tolist() if c in upstream.columns else []:
                        if v and v not in seen:
                            extras.append(str(v))
                            seen.add(v)
                messages.append(build_message(row, text, html, extras))
            else:
                raise ValueError(f"unknown mode={mode!r} (must be 'per_row' or 'summary')")

            sent, failed = 0, 0
            if dry:
                for m in messages:
                    context.log.info(
                        f"[DRY-RUN] To={m['To']} Subj={m['Subject']!r} bytes={len(bytes(m))}"
                    )
                sent = len(messages)
            else:
                ctx = ssl.create_default_context()
                conn_cls = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
                with conn_cls(host, port) as server:
                    if not use_ssl and use_starttls:
                        server.starttls(context=ctx)
                    # Skip LOGIN if the server doesn't advertise AUTH —
                    # supports auth-less local/dev relays (aiosmtpd, mailpit, etc.)
                    server.ehlo_or_helo_if_needed()
                    if "auth" in server.esmtp_features:
                        server.login(username, password)
                    else:
                        context.log.info(
                            f"SMTP server at {host}:{port} did not advertise AUTH — skipping login."
                        )
                    for m in messages:
                        try:
                            server.send_message(m)
                            sent += 1
                        except Exception as e:
                            failed += 1
                            context.log.warning(
                                f"send failed To={m['To']} Subj={m['Subject']!r}: {e}"
                            )

            # Emit a one-row summary
            summary = pd.DataFrame([{
                "host": host,
                "port": port,
                "mode": mode,
                "messages_sent": sent,
                "messages_failed": failed,
                "dry_run": dry,
            }])
            return Output(
                value=summary,
                metadata={
                    "messages_sent": MetadataValue.int(sent),
                    "messages_failed": MetadataValue.int(failed),
                    "dry_run": MetadataValue.bool(dry),
                    "host": MetadataValue.text(f"{host}:{port}"),
                },
            )

        return Definitions(assets=[_asset])
