"""EmailApprovalGateComponent.

Human-in-the-loop approval via email reply. Composes with
`HumanApprovalGateComponent` — this component posts an approval-request
email via SMTP and polls a shared mailbox via IMAP for replies from
allowlisted approvers. When quorum is reached, writes the same JSON
token file `HumanApprovalGateComponent` reads. Everything downstream of
the gate is unchanged.

## Why IMAP polling (not SES-inbound / webhook)

Interactive email delivery notifications (SES inbound → S3 → Lambda,
Postmark webhooks, SendGrid inbound parse) require public HTTPS endpoints
and provider-specific setup. IMAP polling works against any mailbox —
Gmail, Office 365, Zoho, self-hosted Postfix — with just credentials,
and is completely Dagster+ Serverless-safe (pure outbound + inbound
polling; no incoming webhooks).

Trade-off: 30-60s approval-detection latency vs. instant push. Fine for
humans.

## Emits (per YAML instance)

1. **`{asset_name}_posted` asset** (partitioned). Materialization sends
   the outbound approval-request email(s) to `to_emails`. Stores
   `message_id + subject + sent_at + partition_key` in a sidecar file
   under `approval_dir` for the sensor to correlate replies against.
2. **`{asset_name}_watcher` sensor** (default_status: RUNNING). Polls
   `imap_server` every `poll_interval_seconds`. For each posted-but-not-
   tokened partition, scans unread messages in `imap_mailbox`:
   - `In-Reply-To` header matches the outbound message_id → thread reply.
   - `Subject` starts with `[APPROVE ...]` or `[REJECT ...]` → subject-tagged.
   - Body first non-blank line is `APPROVE` / `REJECT` → body-tagged.
   Filters replies to `approver_allowlist`; on quorum, writes the
   standard JSON token `HumanApprovalGateComponent` reads.

Pair with a `HumanApprovalGateComponent` (`upstream_asset_key:
{same_report_asset_key}` + `approval_dir: {same_dir}`) to complete the
gate — email handles the human side, the existing gate handles the
downstream asset-check + blocking.

## Storage backend

Same fsspec-based abstraction as `SlackApprovalGate` / `TeamsApprovalGate`
/ `HumanApprovalGate`: `approval_dir` accepts local paths OR cloud URIs
(`s3://`, `gs://`, `abfs://`).

## Common auth patterns

- **Gmail**: enable IMAP (Settings → Forwarding and POP/IMAP), create an
  App Password (Account → Security → App passwords — 2FA required),
  use `imap_server: imap.gmail.com`, port 993, SMTP `smtp.gmail.com`
  port 587 TLS.
- **Office 365 / Outlook**: `imap-mail.outlook.com` / `smtp-mail.outlook.com`.
  Basic auth is being deprecated; enterprise deployments should use
  modern-auth-tenant-app-passwords or delegate to a service that fronts
  Graph API (out of scope for this component's v1).
- **Zoho**: `imap.zoho.com` / `smtp.zoho.com` (both port 465 SSL).
- **Self-hosted Postfix + Dovecot**: standard IMAPS 993 / SMTP 587.
"""

import email
import imaplib
import json
import os
import re
import smtplib
from datetime import datetime, timezone, timedelta
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


# ── Inline fs helper (same shape as SlackApprovalGate / HumanApprovalGate) ──
class _ApprovalFS:
    """Uniform read/write over `approval_dir`. Plain paths use pathlib
    (no fsspec dep); URIs (`s3://`, `gs://`, `abfs://`) route through
    fsspec + the appropriate driver (`s3fs` / `gcsfs` / `adlfs`)."""
    def __init__(self, root: str):
        if "://" in root:
            import fsspec
            fs, rt = fsspec.core.url_to_fs(root)
            self.fs, self.root, self.is_uri = fs, rt.rstrip("/"), True
        else:
            self.fs, self.root, self.is_uri = None, str(Path(root).expanduser().resolve()), False

    def path(self, *parts: str) -> str:
        pieces = [self.root, *[p.strip("/") for p in parts if p]]
        return "/".join(pieces) if self.is_uri else str(Path(*pieces))

    def exists(self, p: str) -> bool:
        return bool(self.fs.exists(p)) if self.is_uri else Path(p).exists()

    def mkdir(self, subdir: str = "") -> None:
        target = self.path(subdir) if subdir else self.root
        if self.is_uri:
            try: self.fs.makedirs(target, exist_ok=True)
            except Exception: pass
        else:
            Path(target).mkdir(parents=True, exist_ok=True)

    def read_json(self, p: str) -> Any:
        if self.is_uri:
            with self.fs.open(p, "r") as f: return json.loads(f.read())
        return json.loads(Path(p).read_text())

    def write_json(self, p: str, obj: Any) -> None:
        body = json.dumps(obj, indent=2, default=str)
        if self.is_uri:
            try: self.fs.makedirs("/".join(p.split("/")[:-1]), exist_ok=True)
            except Exception: pass
            with self.fs.open(p, "w") as f: f.write(body)
        else:
            Path(p).parent.mkdir(parents=True, exist_ok=True)
            Path(p).write_text(body)

    def glob(self, pattern: str) -> List[str]:
        if self.is_uri:
            proto = self.fs.protocol if isinstance(self.fs.protocol, str) else self.fs.protocol[0]
            return [f"{proto}://{m}" for m in self.fs.glob(self.path(pattern))]
        return [str(p) for p in Path(self.root).glob(pattern)]


# ── Partition helper (matches sibling approval-gate shape) ──

def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
):
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily": return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly": return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly": return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("static partition requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("dynamic partition requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        return MultiPartitionsDefinition({d["name"]: _build_axis(d) for d in partition_dimensions})

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily": return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly": return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly": return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values: raise ValueError("static requires values")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("dynamic requires dynamic_partition_name")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _safe_partition_key(raw: str) -> str:
    return raw.replace("/", "_").replace("\\", "_")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _extract_upstream_text(upstream: Any) -> str:
    """Prefer `text` / `content` / str-coerce."""
    if isinstance(upstream, dict):
        for k in ("text", "content", "value"):
            v = upstream.get(k)
            if isinstance(v, str):
                return v
        return json.dumps(upstream, default=str)[:4000]
    if isinstance(upstream, str):
        return upstream
    return str(upstream)


# ── Email helpers ─────────────────────────────────────────────────────

_EMAIL_STATE_SUFFIX = ".email_state.json"


def _make_message_id(from_email: str, safe_key: str) -> str:
    """Generate a deterministic-ish RFC 5322 message-id. Includes the
    safe_partition_key so replies can correlate even without In-Reply-To."""
    import hashlib
    import time as _time
    domain = from_email.split("@", 1)[1] if "@" in from_email else "dagster.local"
    stamp = str(int(_time.time() * 1000))
    seed = f"{safe_key}:{stamp}".encode()
    short = hashlib.sha256(seed).hexdigest()[:16]
    return f"<{safe_key}.{short}@{domain}>"


def _send_email(
    *,
    smtp_server: str,
    smtp_port: int,
    username: str,
    password: str,
    use_tls: bool,
    from_email: str,
    to_emails: List[str],
    cc_emails: List[str],
    subject: str,
    body: str,
    message_id: str,
) -> None:
    msg = EmailMessage()
    msg["From"] = from_email
    msg["To"] = ", ".join(to_emails)
    if cc_emails:
        msg["Cc"] = ", ".join(cc_emails)
    msg["Subject"] = subject
    msg["Message-ID"] = message_id
    msg.set_content(body)

    with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as smtp:
        smtp.ehlo()
        if use_tls:
            smtp.starttls()
            smtp.ehlo()
        smtp.login(username, password)
        recipients = list(to_emails) + list(cc_emails)
        smtp.send_message(msg, from_addr=from_email, to_addrs=recipients)


def _connect_imap(
    *,
    imap_server: str,
    imap_port: int,
    username: str,
    password: str,
    use_ssl: bool,
    mailbox: str,
):
    """Return a logged-in IMAP4 (or IMAP4_SSL) client selected on mailbox."""
    if use_ssl:
        conn = imaplib.IMAP4_SSL(imap_server, imap_port, timeout=30)
    else:
        conn = imaplib.IMAP4(imap_server, imap_port, timeout=30)
    conn.login(username, password)
    typ, _ = conn.select(mailbox, readonly=True)
    if typ != "OK":
        conn.logout()
        raise RuntimeError(f"IMAP select({mailbox!r}) failed: {typ}")
    return conn


def _extract_body_text(msg: email.message.EmailMessage) -> str:
    """Return the plain-text body of an email message. Prefers text/plain
    parts; falls back to text/html stripped of tags."""
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            if ctype == "text/plain":
                try:
                    return part.get_content()
                except Exception:  # noqa: BLE001
                    try:
                        return part.get_payload(decode=True).decode("utf-8", errors="replace")
                    except Exception:  # noqa: BLE001
                        continue
        # No text/plain — fall through to HTML strip
        for part in msg.walk():
            ctype = part.get_content_type()
            if ctype == "text/html":
                try:
                    html = part.get_content()
                except Exception:  # noqa: BLE001
                    html = part.get_payload(decode=True).decode("utf-8", errors="replace")
                return re.sub(r"<[^>]+>", "", html or "").strip()
    else:
        try:
            return msg.get_content()
        except Exception:  # noqa: BLE001
            payload = msg.get_payload(decode=True)
            if isinstance(payload, bytes):
                return payload.decode("utf-8", errors="replace")
            return str(payload or "")
    return ""


def _classify_reply(
    subject: str, body: str, safe_key: str,
    approve_keyword: str, reject_keyword: str,
) -> Optional[str]:
    """Return 'approve' | 'reject' | None. Match order:
    1. Subject contains `[APPROVE ...]` or `[REJECT ...]` (case-insensitive,
       allow trailing text like `[APPROVE gpt-4o]`, and any prefix like
       `Re: Fwd:` from replies/forwards — we search anywhere in the subject).
    2. Body first non-blank non-quoted line startswith approve_keyword /
       reject_keyword.
    Otherwise None.
    """
    # Search anywhere in subject — replies prepend `Re: ` (or i18n variants
    # like `AW:`, `SV:`), forwards prepend `Fwd:`. `re.search` handles them
    # all without needing an anchor.
    subj_pat_approve = rf"\[?\b{re.escape(approve_keyword)}\b"
    subj_pat_reject = rf"\[?\b{re.escape(reject_keyword)}\b"
    if re.search(subj_pat_approve, subject or "", re.IGNORECASE):
        return "approve"
    if re.search(subj_pat_reject, subject or "", re.IGNORECASE):
        return "reject"
    for line in (body or "").splitlines():
        line = line.strip()
        if not line or line.startswith(">"):
            continue
        upper = line.upper()
        if upper.startswith(approve_keyword.upper()):
            return "approve"
        if upper.startswith(reject_keyword.upper()):
            return "reject"
        # Only inspect first meaningful line.
        break
    return None


def _extract_sender_email(from_header: str) -> str:
    """Pull email out of `Name <foo@bar.com>` or return the raw string."""
    m = re.search(r"[\w\.\-\+]+@[\w\.\-]+\.\w+", from_header or "")
    return (m.group(0) if m else (from_header or "")).lower().strip()


class EmailApprovalGateComponent(dg.Component, dg.Model, dg.Resolvable):
    """Email-native HITL approval gate. Sends approval-request emails via
    SMTP and polls IMAP for replies from allowlisted approvers. Composes
    with `HumanApprovalGateComponent` — email side handles posting +
    reply polling + quorum; the existing gate consumes the resulting
    token unchanged.

    Emits one asset (`{asset_name}_posted`) + one sensor
    (`{asset_name}_watcher`). Every partition that hasn't been tokened
    yet is polled every `poll_interval_seconds`.
    """

    asset_name: str = Field(
        description="Base name. Emits `{asset_name}_posted` asset + `{asset_name}_watcher` sensor."
    )
    upstream_asset_key: str = Field(
        description=(
            "Upstream asset whose text is emailed for approval. "
            "String (not AssetKey object); multi-part keys use slash notation."
        )
    )
    approval_dir: str = Field(
        description=(
            "Where to write the standard JSON approval token on quorum, and "
            "where the outbound message sidecar lives. Plain path or fsspec URI."
        )
    )

    # SMTP (outbound)
    smtp_server: str = Field(description="SMTP host, e.g. `smtp.gmail.com`.")
    smtp_port: int = Field(default=587, description="SMTP port. 587=TLS, 465=SSL.")
    smtp_use_tls: bool = Field(default=True, description="Use STARTTLS after connect.")
    smtp_username_env_var: str = Field(
        description="Env var holding the SMTP username (usually the from-email)."
    )
    smtp_password_env_var: str = Field(
        description="Env var holding the SMTP password (app password for Gmail; account password elsewhere)."
    )
    from_email: str = Field(
        description="`From:` address on the outbound approval request."
    )
    to_emails: List[str] = Field(
        description="Recipient emails. Everyone here gets the request."
    )
    cc_emails: Optional[List[str]] = Field(
        default=None,
        description="Optional CC list (typically escalation contacts or auditors).",
    )
    subject_template: Optional[str] = Field(
        default=None,
        description=(
            "Outbound Subject template. Placeholders: `{partition_key}`. "
            "Default: `[Dagster] Approval needed: {partition_key}`."
        ),
    )
    body_template: Optional[str] = Field(
        default=None,
        description=(
            "Outbound plain-text body template. Placeholders: "
            "`{upstream_text}` (truncated to 4000 chars), `{approve_keyword}`, "
            "`{reject_keyword}`, `{required_approvers}`, `{n_allowlisted}`, "
            "`{partition_key}`. Default explains how to reply."
        ),
    )

    # IMAP (inbound polling)
    imap_server: str = Field(description="IMAP host, e.g. `imap.gmail.com`.")
    imap_port: int = Field(default=993, description="IMAP port (993=SSL, 143=cleartext).")
    imap_use_ssl: bool = Field(default=True, description="Use IMAPS (SSL/TLS from connect).")
    imap_username_env_var: str = Field(
        description="Env var holding the IMAP username (often same as SMTP username)."
    )
    imap_password_env_var: str = Field(
        description="Env var holding the IMAP password (often same as SMTP password)."
    )
    imap_mailbox: str = Field(
        default="INBOX",
        description="Mailbox to poll (folder name). Default `INBOX`.",
    )

    approve_keyword: str = Field(
        default="APPROVE",
        description="Case-insensitive. Match in subject (`[APPROVE ...]`) or first body line.",
    )
    reject_keyword: str = Field(
        default="REJECT",
        description="Case-insensitive. Match in subject (`[REJECT ...]`) or first body line.",
    )

    # Quorum + timeout (same shape as SlackApprovalGate)
    required_approvers: int = Field(
        default=1,
        description="How many allowlisted approvers must reply approve before the token is written.",
    )
    approver_allowlist: List[str] = Field(
        description=(
            "Email addresses allowed to vote. Replies from anyone else are ignored. "
            "Sender email is extracted from the `From:` header (case-insensitive)."
        ),
    )
    timeout_hours: Optional[float] = Field(
        default=None,
        description="Optional. If quorum isn't reached within this many hours, applies `on_timeout` policy.",
    )
    on_timeout: str = Field(
        default="escalate",
        description="`escalate` (default — CC escalation contact on a follow-up email, keep waiting) | `reject` (auto-reject) | `approve` (auto-approve — use with caution).",
    )
    escalate_email: Optional[str] = Field(
        default=None,
        description="Email address CC'd on the escalation follow-up. Only used when `on_timeout: escalate`.",
    )

    poll_interval_seconds: int = Field(
        default=60,
        description="Sensor cadence — how often to poll IMAP for new replies. 60s default; lower for demos, higher for shared corporate mailboxes with rate limits.",
    )

    # Catalog / governance
    group_name: Optional[str] = Field(default=None, description="Asset group.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds. Default: ['human', 'approval', 'email']."
    )
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Extra tags.")
    description: Optional[str] = Field(default=None, description="Asset description.")

    # Partitioning — must match upstream asset's partitions_def.
    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Any] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Email Approval Gate", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        posted_asset_name = f"{asset_name}_posted"
        upstream_asset_key = self.upstream_asset_key
        approval_dir = self.approval_dir

        smtp_server = self.smtp_server
        smtp_port = self.smtp_port
        smtp_use_tls = self.smtp_use_tls
        smtp_user_env = self.smtp_username_env_var
        smtp_pass_env = self.smtp_password_env_var
        from_email = self.from_email
        to_emails = list(self.to_emails)
        cc_emails = list(self.cc_emails or [])

        imap_server = self.imap_server
        imap_port = self.imap_port
        imap_use_ssl = self.imap_use_ssl
        imap_user_env = self.imap_username_env_var
        imap_pass_env = self.imap_password_env_var
        imap_mailbox = self.imap_mailbox

        approve_keyword = self.approve_keyword
        reject_keyword = self.reject_keyword

        subject_template = self.subject_template or "[Dagster] Approval needed: {partition_key}"
        body_template = self.body_template or (
            "An approval is requested for partition `{partition_key}`.\n\n"
            "--- Content ---\n{upstream_text}\n\n"
            "--- How to respond ---\n"
            "Reply with `{approve_keyword}` on the first line (or in the subject "
            "as `[{approve_keyword}]`) to approve. Reply with `{reject_keyword}` "
            "to reject.\n\n"
            "Requires {required_approvers} of {n_allowlisted} allowlisted "
            "approvers.\n"
        )

        required_approvers = self.required_approvers
        approver_allowlist = [e.lower().strip() for e in self.approver_allowlist]
        if required_approvers > len(approver_allowlist):
            raise ValueError(
                f"required_approvers={required_approvers} > "
                f"len(approver_allowlist)={len(approver_allowlist)}. Impossible quorum."
            )
        timeout_hours = self.timeout_hours
        on_timeout = self.on_timeout
        if on_timeout not in ("escalate", "reject", "approve"):
            raise ValueError(f"on_timeout must be escalate|reject|approve; got {on_timeout!r}")
        escalate_email = self.escalate_email
        poll_interval_seconds = self.poll_interval_seconds

        kinds = self.kinds or ["human", "approval", "email"]
        tag_map = dict(self.tags or {})
        for k in kinds:
            tag_map[f"dagster/kind/{k}"] = ""

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )

        @dg.asset(
            key=dg.AssetKey.from_user_string(posted_asset_name),
            description=self.description or f"Email approval request for {upstream_asset_key}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            partitions_def=partitions_def,
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))},
        )
        def _posted_asset(context: dg.AssetExecutionContext, upstream):
            raw_key = context.partition_key if context.has_partition_key else "default"
            safe_key = _safe_partition_key(raw_key)
            fs = _ApprovalFS(approval_dir)
            fs.mkdir()
            sidecar_path = fs.path(f"{safe_key}{_EMAIL_STATE_SUFFIX}")

            if fs.exists(sidecar_path):
                state = fs.read_json(sidecar_path)
                context.log.info(
                    f"[email_approval] partition {raw_key!r} already posted "
                    f"(message_id={state.get('message_id')}); not resending."
                )
                return state

            username = os.environ.get(smtp_user_env, "")
            password = os.environ.get(smtp_pass_env, "")
            if not username or not password:
                raise RuntimeError(
                    f"SMTP creds not set: {smtp_user_env!r}={'SET' if username else 'MISSING'}, "
                    f"{smtp_pass_env!r}={'SET' if password else 'MISSING'}"
                )

            upstream_text = _extract_upstream_text(upstream)[:4000]
            subject = subject_template.format(partition_key=raw_key)
            body = body_template.format(
                upstream_text=upstream_text,
                approve_keyword=approve_keyword,
                reject_keyword=reject_keyword,
                required_approvers=required_approvers,
                n_allowlisted=len(approver_allowlist),
                partition_key=raw_key,
            )
            message_id = _make_message_id(from_email, safe_key)

            _send_email(
                smtp_server=smtp_server, smtp_port=smtp_port,
                username=username, password=password, use_tls=smtp_use_tls,
                from_email=from_email, to_emails=to_emails, cc_emails=cc_emails,
                subject=subject, body=body, message_id=message_id,
            )

            state = {
                "message_id": message_id,
                "subject": subject,
                "from_email": from_email,
                "to_emails": to_emails,
                "cc_emails": cc_emails,
                "partition_key": raw_key,
                "safe_partition_key": safe_key,
                "sent_at": _now_iso(),
                "required_approvers": required_approvers,
                "approver_allowlist": approver_allowlist,
                "approve_keyword": approve_keyword,
                "reject_keyword": reject_keyword,
                "timeout_hours": timeout_hours,
                "on_timeout": on_timeout,
                "escalate_email": escalate_email,
                "imap_server": imap_server,
                "imap_port": imap_port,
                "imap_use_ssl": imap_use_ssl,
                "imap_user_env": imap_user_env,
                "imap_pass_env": imap_pass_env,
                "imap_mailbox": imap_mailbox,
                "smtp_server": smtp_server,
                "smtp_port": smtp_port,
                "smtp_use_tls": smtp_use_tls,
                "smtp_user_env": smtp_user_env,
                "smtp_pass_env": smtp_pass_env,
                "approval_dir": fs.root,
                "escalated_at": None,
            }
            fs.write_json(sidecar_path, state)
            context.log.info(
                f"[email_approval] sent to {to_emails} for partition "
                f"{raw_key!r} (message_id={message_id})"
            )
            context.add_output_metadata({
                "smtp_server": smtp_server,
                "message_id": message_id,
                "to_emails": ",".join(to_emails),
                "sidecar_path": sidecar_path,
                "partition_key": raw_key,
                "required_approvers": required_approvers,
                "allowlisted": ",".join(approver_allowlist),
            })
            return state

        # ── Sensor ────────────────────────────────────────────────────
        sensor_name = f"{asset_name}_watcher"

        @dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=poll_interval_seconds,
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def _watcher(context: dg.SensorEvaluationContext):
            fs = _ApprovalFS(approval_dir)
            if not fs.is_uri and not Path(fs.root).exists():
                return dg.SensorResult(skip_reason=f"approval_dir does not exist yet: {fs.root}")

            sidecars = fs.glob(f"*{_EMAIL_STATE_SUFFIX}")
            if not sidecars:
                return dg.SensorResult(skip_reason="no partitions posted yet")

            actions: List[str] = []
            for sidecar in sidecars:
                try:
                    state = fs.read_json(sidecar)
                except Exception as e:  # noqa: BLE001
                    context.log.warning(f"skipping malformed sidecar {sidecar}: {e}")
                    continue

                safe_key = state["safe_partition_key"]
                token_path = fs.path(f"{safe_key}.json")
                if fs.exists(token_path):
                    continue  # already resolved

                imap_user = os.environ.get(state["imap_user_env"], "")
                imap_pass = os.environ.get(state["imap_pass_env"], "")
                if not imap_user or not imap_pass:
                    context.log.error(
                        f"IMAP creds missing for {safe_key} "
                        f"({state['imap_user_env']!r}/{state['imap_pass_env']!r}); "
                        f"skipping this tick."
                    )
                    continue

                # Scan mailbox for candidate replies.
                approvers: List[str] = []
                rejecters: List[str] = []
                try:
                    conn = _connect_imap(
                        imap_server=state["imap_server"],
                        imap_port=state["imap_port"],
                        username=imap_user, password=imap_pass,
                        use_ssl=state["imap_use_ssl"],
                        mailbox=state["imap_mailbox"],
                    )
                except Exception as e:  # noqa: BLE001
                    context.log.error(
                        f"IMAP connect/select failed for {safe_key}: {e}. Skipping this tick."
                    )
                    continue
                try:
                    orig_msgid = state["message_id"]
                    # Search: messages with In-Reply-To matching, OR subject
                    # containing the approve/reject keywords + partition key
                    # for older mail servers that don't index In-Reply-To.
                    typ, data = conn.uid(
                        "search", None,
                        "OR",
                        f'(HEADER In-Reply-To "{orig_msgid}")',
                        f'(HEADER References "{orig_msgid}")',
                    )
                    if typ != "OK":
                        context.log.warning(f"IMAP search failed for {safe_key}: {typ}")
                        continue
                    uids = (data[0].split() if data and data[0] else [])
                    for uid in uids:
                        typ, msg_data = conn.uid("fetch", uid, "(RFC822)")
                        if typ != "OK" or not msg_data:
                            continue
                        raw = msg_data[0][1] if isinstance(msg_data[0], tuple) else None
                        if not raw:
                            continue
                        msg = email.message_from_bytes(raw)
                        sender = _extract_sender_email(msg.get("From", ""))
                        if sender not in state["approver_allowlist"]:
                            continue
                        subject = msg.get("Subject", "")
                        body_text = _extract_body_text(msg)
                        classification = _classify_reply(
                            subject, body_text, safe_key,
                            state["approve_keyword"], state["reject_keyword"],
                        )
                        if classification == "approve" and sender not in approvers:
                            approvers.append(sender)
                        elif classification == "reject" and sender not in rejecters:
                            rejecters.append(sender)
                finally:
                    try:
                        conn.logout()
                    except Exception:  # noqa: BLE001
                        pass

                required = state["required_approvers"]
                token_body: Optional[Dict[str, Any]] = None
                if len(approvers) >= required:
                    token_body = {
                        "approved": True,
                        "approver": ",".join(approvers),
                        "reason": f"Email quorum reached: {len(approvers)}/{required} approved",
                        "timestamp": _now_iso(),
                        "source": "email_approval_gate",
                        "message_id": state["message_id"],
                    }
                elif len(rejecters) >= required:
                    token_body = {
                        "approved": False,
                        "approver": ",".join(rejecters),
                        "reason": f"Email rejection quorum reached: {len(rejecters)}/{required} rejected",
                        "timestamp": _now_iso(),
                        "source": "email_approval_gate",
                        "message_id": state["message_id"],
                    }
                else:
                    # Timeout check.
                    timeout = state.get("timeout_hours")
                    if timeout is not None:
                        sent = datetime.fromisoformat(state["sent_at"])
                        if datetime.now(timezone.utc) - sent > timedelta(hours=timeout):
                            on_to = state["on_timeout"]
                            if on_to == "reject":
                                token_body = {
                                    "approved": False,
                                    "approver": "timeout",
                                    "reason": f"Timed out after {timeout}h with no quorum; auto-rejected.",
                                    "timestamp": _now_iso(),
                                    "source": "email_approval_gate",
                                }
                            elif on_to == "approve":
                                token_body = {
                                    "approved": True,
                                    "approver": "timeout",
                                    "reason": f"Timed out after {timeout}h with no quorum; auto-approved (per policy).",
                                    "timestamp": _now_iso(),
                                    "source": "email_approval_gate",
                                }
                            elif (
                                on_to == "escalate"
                                and state.get("escalate_email")
                                and not state.get("escalated_at")
                            ):
                                # Send an escalation follow-up ONCE, keep waiting.
                                try:
                                    escalation_subject = f"[Dagster ESCALATION] Approval pending: {state['partition_key']}"
                                    escalation_body = (
                                        f"Approval on partition `{state['partition_key']}` "
                                        f"has been pending for {timeout}h. Please review "
                                        f"(replying `{state['approve_keyword']}` / "
                                        f"`{state['reject_keyword']}` still works)."
                                    )
                                    smtp_u = os.environ.get(state["smtp_user_env"], "")
                                    smtp_p = os.environ.get(state["smtp_pass_env"], "")
                                    _send_email(
                                        smtp_server=state["smtp_server"],
                                        smtp_port=state["smtp_port"],
                                        username=smtp_u, password=smtp_p,
                                        use_tls=state["smtp_use_tls"],
                                        from_email=state["from_email"],
                                        to_emails=state["to_emails"],
                                        cc_emails=[state["escalate_email"]],
                                        subject=escalation_subject,
                                        body=escalation_body,
                                        message_id=_make_message_id(
                                            state["from_email"], f"{safe_key}.esc"
                                        ),
                                    )
                                    state["escalated_at"] = _now_iso()
                                    fs.write_json(sidecar, state)
                                    actions.append(f"{safe_key}: escalated")
                                except Exception as e:  # noqa: BLE001
                                    context.log.warning(f"escalation send failed for {safe_key}: {e}")

                if token_body is not None:
                    fs.write_json(token_path, token_body)
                    actions.append(f"{safe_key}: wrote token ({'approved' if token_body['approved'] else 'rejected'})")

            if not actions:
                return dg.SensorResult(skip_reason=f"polled {len(sidecars)} partition(s); no quorum changes")
            return dg.SensorResult(skip_reason="; ".join(actions))

        return dg.Definitions(assets=[_posted_asset], sensors=[_watcher])
