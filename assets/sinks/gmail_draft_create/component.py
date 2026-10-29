"""GmailDraftCreateComponent — create a Gmail DRAFT (never sends) via the real Gmail API.

Templates a subject/body from an upstream asset's materialized value (a dict-shaped
record, or one row of a pandas DataFrame, selected via `row_index`) and creates a
Gmail draft addressed to `to_email` using Google's Gmail REST API
(`POST /gmail/v1/users/me/drafts`). This is the write-capable sibling of the
read-only `assets/ingestion/gmail_ingestion` component, and shares its exact
OAuth2 convention: a bearer access token in the `access_token` field (set it to
a literal token, or `${GMAIL_ACCESS_TOKEN}` to pull from an env var).

!! This component ONLY creates a draft -- it never sends mail. !!
The Gmail API's `drafts.create` endpoint cannot send on a user's behalf; a
human still has to open Gmail and click Send (or Gmail's own scheduled-send /
"undo send" safety nets apply if they do). There is no config flag here that
sends instead -- if you need to actually send mail autonomously, that's a
different (and riskier) API surface (`users.messages.send`), not what this
component does.

Motivating use case: a nightly automated brief-generation pipeline (e.g. an
LLM-written daily summary, an anomaly digest, a sales/ops report) that should
land as a reviewable draft in the user's own Gmail — addressed to their own
address — rather than auto-send unreviewed, LLM-authored content. A human
reviews, edits if needed, and clicks Send themselves.
"""
import base64
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import requests
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field

GMAIL_DRAFTS_URL = "https://gmail.googleapis.com/gmail/v1/users/me/drafts"


def _render(template: str, row: Dict[str, Any]) -> str:
    """Render `{column}` placeholders against a dict. Missing keys and
    pandas NaN (a float column of all-None values decays to NaN, not
    None) both yield empty string -- otherwise a NaN value would render
    as the literal text "nan". Unmatched/non-identifier braces (e.g.
    literal '{' in a report body) are left untouched since the pattern
    only matches `\\w+` identifiers."""
    import re

    def sub(m):
        key = m.group(1)
        v = row.get(key)
        if v is None:
            return ""
        if isinstance(v, float) and pd.isna(v):
            return ""
        return str(v)

    return re.sub(r"\{(\w+)\}", sub, template)


def _resolve_template_fields(upstream: Any, row_index: int = 0) -> Dict[str, Any]:
    """Normalize the upstream asset's materialized value into a plain dict of
    template fields, for either shape this component accepts:

      - dict-like (a single record, with named fields directly usable)
      - pandas DataFrame (one row, selected by `row_index`, default 0)

    Also unwraps Output/MaterializeResult (an upstream author may annotate
    `-> Output` or return `Output(value=..., ...)`), and the partition-bridge
    shape (an unpartitioned asset consuming a partitioned upstream loads ALL
    partitions as {partition_key: DataFrame} -- these are concatenated into
    one DataFrame first, same as this repo's other single-upstream sinks).
    """
    if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
        upstream = upstream.value

    if isinstance(upstream, dict) and upstream and all(isinstance(v, pd.DataFrame) for v in upstream.values()):
        frames = list(upstream.values())
        upstream = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    if isinstance(upstream, pd.DataFrame):
        if upstream.empty:
            raise ValueError("upstream DataFrame is empty -- no row available to draft from")
        if row_index < 0 or row_index >= len(upstream):
            raise ValueError(
                f"row_index={row_index} out of range for upstream DataFrame with {len(upstream)} row(s)"
            )
        return upstream.iloc[row_index].to_dict()

    if isinstance(upstream, pd.Series):
        return upstream.to_dict()

    if isinstance(upstream, dict):
        return dict(upstream)

    raise ValueError(
        f"upstream value for GmailDraftCreateComponent must be a dict or pandas DataFrame, got {type(upstream)!r}"
    )


def _build_mime_message(
    to_email: str,
    subject: str,
    body: str,
    cc_emails: Optional[List[str]] = None,
    bcc_emails: Optional[List[str]] = None,
) -> MIMEText:
    """Build a real RFC 2822 MIME message (UTF-8 plain text), per Gmail's
    draft-creation contract. `MIMEText` is itself an `email.message.Message`,
    so `To`/`Cc`/`Bcc`/`Subject` headers are set directly on it -- no
    `MIMEMultipart` wrapper is needed for a plain-text-only body."""
    msg = MIMEText(body, "plain", "utf-8")
    msg["To"] = to_email
    msg["Subject"] = subject
    if cc_emails:
        msg["Cc"] = ", ".join(cc_emails)
    if bcc_emails:
        msg["Bcc"] = ", ".join(bcc_emails)
    return msg


def _encode_mime_message(msg: MIMEText) -> str:
    """Serialize to raw bytes and base64url-encode, per Gmail API's `raw` field contract."""
    return base64.urlsafe_b64encode(msg.as_bytes()).decode()


def _create_gmail_draft(access_token: str, encoded_raw: str) -> Dict[str, Any]:
    """Isolates the one external API call (Gmail `drafts.create`) so tests
    can monkeypatch `requests.post` wholesale instead of hitting the real
    network."""
    response = requests.post(
        GMAIL_DRAFTS_URL,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        json={"message": {"raw": encoded_raw}},
    )
    if not (200 <= response.status_code < 300):
        raise RuntimeError(
            f"Gmail draft creation failed: HTTP {response.status_code} -- {response.text}"
        )
    return response.json()


# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization


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


class GmailDraftCreateComponent(Component, Model, Resolvable):
    """Create a Gmail DRAFT (never sends) templated from an upstream asset's row.

    Example:

        ```yaml
        type: dagster_component_templates.GmailDraftCreateComponent
        attributes:
          asset_name: nightly_brief_gmail_draft
          access_token: "${GMAIL_ACCESS_TOKEN}"
          upstream_asset_key: nightly_brief
          to_email: me@example.com
          subject_template: "Nightly brief -- {report_date}"
          body_template: |
            {summary_text}
        ```
    """

    asset_name: str = Field(description="Output asset name (draft-creation result).")
    access_token: str = Field(
        description='Google OAuth2 access token scoped for gmail.compose (or gmail.modify). Use ${GMAIL_ACCESS_TOKEN} for env vars.'
    )
    upstream_asset_key: Union[str, List[str]] = Field(
        description="Upstream asset whose materialized value supplies the template fields -- a dict-shaped record, or a pandas DataFrame (one row is selected via row_index)."
    )
    row_index: int = Field(
        default=0,
        description="When the upstream value is a DataFrame, the 0-based row position to draft from. Ignored for a dict-shaped upstream value.",
    )

    to_email: str = Field(
        description="Address the draft is addressed to. This is almost always the pipeline-running user's own address (a reviewable draft lands in their own mailbox), not a third party -- the Gmail API can only draft/send as the authenticated account. Supports `{field}` placeholders."
    )
    subject_template: str = Field(
        description="Draft subject line, with `{column}` placeholders templated against the upstream row/record."
    )
    body_template: str = Field(
        description="Draft body (plain text), with `{column}` placeholders templated against the upstream row/record."
    )
    cc_emails: Optional[List[str]] = Field(
        default=None, description="Optional list of Cc recipient addresses."
    )
    bcc_emails: Optional[List[str]] = Field(
        default=None, description="Optional list of Bcc recipient addresses."
    )

    dry_run: bool = Field(
        default=False,
        description="If True, render and log the MIME message but don't call the Gmail API or create a draft. Use for staging/testing templates.",
    )

    description: Optional[str] = Field(default=None, description="Asset description.")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or Gmail API rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None, description="Seconds between retries (default 1)."
    )
    retry_policy_backoff: str = Field(
        default="exponential", description="Backoff strategy: 'linear' or 'exponential'."
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "cc_emails", "bcc_emails"), tags=("tags",))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
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
        access_token = self.access_token
        row_index = self.row_index
        to_email_t = self.to_email
        subject_t = self.subject_template
        body_t = self.body_template
        cc_emails = self.cc_emails or []
        bcc_emails = self.bcc_emails or []
        dry = self.dry_run

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or "Create a Gmail draft (never sends) templated from an upstream asset's row.",
            group_name=self.group_name,
            kinds={"gmail"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
        )
        def _asset(context: AssetExecutionContext, upstream: Any):
            row = _resolve_template_fields(upstream, row_index)

            to_email = _render(to_email_t, row)
            subject = _render(subject_t, row)
            body = _render(body_t, row)
            rendered_cc = [_render(c, row) for c in cc_emails]
            rendered_bcc = [_render(c, row) for c in bcc_emails]

            if not to_email:
                raise ValueError("to_email rendered to an empty string -- check to_email / the upstream row")

            msg = _build_mime_message(to_email, subject, body, rendered_cc, rendered_bcc)
            encoded_raw = _encode_mime_message(msg)

            metadata: Dict[str, Any] = {
                "to": MetadataValue.text(to_email),
                "subject": MetadataValue.text(subject),
                "dry_run": MetadataValue.bool(dry),
            }

            if dry:
                context.log.info(
                    f"[DRY-RUN] Gmail draft to={to_email!r} cc={rendered_cc!r} bcc={rendered_bcc!r} subject={subject!r}"
                )
                return MaterializeResult(metadata=metadata)

            response_json = _create_gmail_draft(access_token, encoded_raw)
            draft_id = response_json.get("id")
            message_id = (response_json.get("message") or {}).get("id")
            context.log.info(f"Created Gmail draft id={draft_id!r} message_id={message_id!r} to={to_email!r}")

            metadata["draft_id"] = MetadataValue.text(str(draft_id) if draft_id else "")
            metadata["message_id"] = MetadataValue.text(str(message_id) if message_id else "")
            return MaterializeResult(metadata=metadata)

        return Definitions(assets=[_asset])
