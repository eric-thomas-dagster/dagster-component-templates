"""ImapInboxSourceComponent — fetch emails from an IMAP server.

Works with any IMAP-compliant mailbox: Gmail (app password), Outlook/O365,
Yahoo Mail, FastMail, self-hosted Dovecot/Cyrus, etc. Emits one row per
message with subject/from/to/date/body/attachments_count and the raw
RFC-822 source for downstream processing.

Common use:
  - Inbound automation: process forwarded reports, ticketing, contract
    intake — read messages from a shared mailbox, mark them read, write
    to a database / warehouse / vector store.
  - Email-to-data ingestion: customer feedback, vendor invoices,
    monitoring alerts that land in a mailbox.

Auth: server-specific. Gmail requires an app password (or OAuth — for
OAuth, prefer `gmail_inbox_source`). Outlook/O365 supports basic auth
for IMAP only with admin opt-in; otherwise use Microsoft Graph.

Body decoding handles multipart/alternative (prefers text/plain over
text/html), 7-bit, quoted-printable, base64. Falls back to raw bytes
when decoding fails.
"""
import email
import imaplib
import re
from datetime import datetime, timezone
from email.header import decode_header
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from dagster import (
    AssetExecutionContext,
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


def _decode_header_value(raw: Optional[str]) -> str:
    """Decode RFC-2047 encoded headers (=?utf-8?B?...?=) into plain text."""
    if not raw:
        return ""
    parts = decode_header(raw)
    out = []
    for text, charset in parts:
        if isinstance(text, bytes):
            try:
                out.append(text.decode(charset or "utf-8", errors="replace"))
            except (LookupError, AttributeError):
                out.append(text.decode("utf-8", errors="replace"))
        else:
            out.append(text)
    return "".join(out)


def _extract_body(msg: email.message.Message) -> Dict[str, Any]:
    """Pull plain-text body, html body, and attachment count from a message."""
    plain, html, attachments = None, None, 0
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            disp = (part.get("Content-Disposition") or "").lower()
            if "attachment" in disp or part.get_filename():
                attachments += 1
                continue
            if ctype == "text/plain" and plain is None:
                try:
                    plain = part.get_payload(decode=True).decode(
                        part.get_content_charset() or "utf-8", errors="replace"
                    )
                except Exception:
                    plain = None
            elif ctype == "text/html" and html is None:
                try:
                    html = part.get_payload(decode=True).decode(
                        part.get_content_charset() or "utf-8", errors="replace"
                    )
                except Exception:
                    html = None
    else:
        ctype = msg.get_content_type()
        try:
            payload = msg.get_payload(decode=True).decode(
                msg.get_content_charset() or "utf-8", errors="replace"
            )
        except Exception:
            payload = None
        if ctype == "text/plain":
            plain = payload
        elif ctype == "text/html":
            html = payload
    return {"body_text": plain, "body_html": html, "attachments_count": attachments}


def _strip_html(html: Optional[str]) -> Optional[str]:
    """Cheap HTML stripper for when only HTML is present and a plain body
    is wanted as fallback. Not a full HTML renderer — just tag/entity strip."""
    if not html:
        return None
    text = re.sub(r"<\s*br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"</\s*p\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = (
        text.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", '"')
        .replace("&#39;", "'")
    )
    return re.sub(r"\n{3,}", "\n\n", text).strip()


class ImapInboxSourceComponent(Component, Model, Resolvable):
    """Fetch messages from an IMAP mailbox and emit one row per message."""

    asset_name: str = Field(description="Output asset name.")

    host: str = Field(
        description="IMAP server hostname (imap.gmail.com / outlook.office365.com / etc.)."
    )
    port: int = Field(default=993, description="IMAP SSL port (default 993).")
    use_ssl: bool = Field(default=True, description="Use IMAPS (TLS).")

    username_env_var: str = Field(
        description="Env var containing the mailbox username (typically the email address)."
    )
    password_env_var: str = Field(
        description="Env var containing the password (for Gmail use an app password)."
    )

    mailbox: str = Field(
        default="INBOX",
        description="IMAP folder to read from. Common: INBOX, '[Gmail]/All Mail', Archive.",
    )
    search_criteria: str = Field(
        default="UNSEEN",
        description=(
            "IMAP search expression. Examples: 'UNSEEN', 'ALL', "
            "'(FROM \"alerts@\")', 'SINCE 01-Jan-2025', 'SUBJECT \"invoice\"'."
        ),
    )

    max_messages: int = Field(
        default=100,
        description="Cap on how many messages to fetch in a single run.",
    )

    mark_read: bool = Field(
        default=False,
        description=(
            "After fetching, set the \\Seen flag on each message so the next "
            "UNSEEN run skips them. Set False to leave unread (useful for testing)."
        ),
    )

    include_body_html: bool = Field(
        default=False,
        description="Emit the raw HTML body too (in addition to text). Off by default — adds bulk.",
    )
    fallback_html_to_text: bool = Field(
        default=True,
        description="If a message has only HTML body, derive a text body by stripping tags.",
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
        host = self.host
        port = self.port
        use_ssl = self.use_ssl
        user_env = self.username_env_var
        pass_env = self.password_env_var
        mailbox = self.mailbox
        criteria = self.search_criteria
        cap = self.max_messages
        mark_read = self.mark_read
        emit_html = self.include_body_html
        fallback = self.fallback_html_to_text

        @asset(
            name=asset_name,
            description=self.description or f"IMAP inbox source: {host}/{mailbox} ({criteria}).",
            group_name=self.group_name,
            kinds={"imap", "email", "pandas"},
            tags=self.tags or None,
            owners=self.owners or None,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext) -> Output:
            username = EnvVar(user_env).get_value()
            password = EnvVar(pass_env).get_value()
            if not username or not password:
                raise ValueError(
                    f"IMAP credentials missing — set {user_env!r} and {pass_env!r}."
                )

            conn_cls = imaplib.IMAP4_SSL if use_ssl else imaplib.IMAP4
            conn = conn_cls(host, port)
            try:
                conn.login(username, password)
                conn.select(mailbox)
                typ, msgnums = conn.search(None, criteria)
                if typ != "OK":
                    raise RuntimeError(f"IMAP search failed: {typ}")
                ids = msgnums[0].split() if msgnums and msgnums[0] else []
                # Newest first — IMAP returns oldest first
                ids = list(reversed(ids))[:cap]
                context.log.info(
                    f"Found {len(ids)} matching messages in {host}/{mailbox} "
                    f"(criteria={criteria!r}, cap={cap})"
                )

                rows: List[Dict[str, Any]] = []
                for msg_id in ids:
                    typ, data = conn.fetch(msg_id, "(RFC822)")
                    if typ != "OK" or not data or not data[0]:
                        continue
                    raw_bytes = data[0][1]
                    msg = email.message_from_bytes(raw_bytes)

                    subj = _decode_header_value(msg.get("Subject"))
                    from_addr = _decode_header_value(msg.get("From"))
                    to_addr = _decode_header_value(msg.get("To"))
                    cc_addr = _decode_header_value(msg.get("Cc"))
                    reply_to = _decode_header_value(msg.get("Reply-To"))
                    message_id = (msg.get("Message-ID") or "").strip("<>")
                    in_reply_to = (msg.get("In-Reply-To") or "").strip("<>")

                    date_str = msg.get("Date")
                    try:
                        sent_at = parsedate_to_datetime(date_str) if date_str else None
                        if sent_at and sent_at.tzinfo is None:
                            sent_at = sent_at.replace(tzinfo=timezone.utc)
                    except Exception:
                        sent_at = None

                    body = _extract_body(msg)
                    text = body["body_text"]
                    if not text and fallback:
                        text = _strip_html(body["body_html"])

                    row = {
                        "uid": int(msg_id),
                        "message_id": message_id or None,
                        "in_reply_to": in_reply_to or None,
                        "subject": subj,
                        "from": from_addr,
                        "to": to_addr,
                        "cc": cc_addr or None,
                        "reply_to": reply_to or None,
                        "sent_at": sent_at,
                        "fetched_at": datetime.now(timezone.utc),
                        "body_text": text,
                        "attachments_count": body["attachments_count"],
                    }
                    if emit_html:
                        row["body_html"] = body["body_html"]
                    rows.append(row)

                    if mark_read:
                        conn.store(msg_id, "+FLAGS", "\\Seen")
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
                conn.logout()

            df = pd.DataFrame(rows)
            preview = df.head(5)[
                [c for c in ("subject", "from", "sent_at", "attachments_count") if c in df.columns]
            ].to_markdown(index=False) if not df.empty else "(no rows)"
            return Output(
                value=df,
                metadata={
                    "message_count": MetadataValue.int(len(df)),
                    "mailbox":       MetadataValue.text(mailbox),
                    "search":        MetadataValue.text(criteria),
                    "marked_read":   MetadataValue.bool(mark_read),
                    "preview":       MetadataValue.md(preview),
                },
            )

        return Definitions(assets=[_asset])
