"""SlackReplyComponent -- deterministically posts a chat agent's answer back into the Slack
thread it's answering. A model is never trusted to remember to call a "post" tool -- if it
forgets, or the run fails outright, the human sees nothing. This component is the opposite of
that: a plain downstream asset for the success path, PLUS a run-failure sensor that fires
whenever any failed run carries the same `slack/channel`/`slack/thread_ts` tags
`slack_agent_request_sensor` sets, so a failed run still gets a reply (a short error + a run
link) instead of silence.

Both halves read channel/thread straight off the RUN's own tags (`context.run.tags` for the
success-path asset, `context.dagster_run.tags` for the failure sensor) -- the same identifiers
`slack_agent_request_sensor` set, never re-derived or guessed.
"""
import json
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Union

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

# BEGIN receipt-message-links (inlined per component; keep copies in slack_reply and
# slack_chat_tools identical). Links a run's receipt ids (from a receipts-db-path-configured
# knowledge_search_server/hybrid_knowledge_search_server) to the Slack message that carried
# the answer built from them, so a later reaction (or slack_chat_tools.get_thread()) can find
# them back without parsing anything out of the reply text. Not part of the canonical
# receipt-warehouse block (resources/context_receipt_store) -- this table is SQLite-only and
# Slack-specific, never synced to a warehouse, so it stays out of that multi-dialect system.
def _ensure_message_links_table(conn) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS context_receipt_message_links ("
        "receipt_id TEXT NOT NULL, platform TEXT NOT NULL, channel TEXT NOT NULL, "
        "message_ts TEXT NOT NULL, created_at TEXT NOT NULL)"
    )


def record_message_links(db_path: str, receipt_ids, *, platform: str, channel: str, message_ts: str) -> None:
    if not receipt_ids:
        return
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        _ensure_message_links_table(conn)
        created_at = datetime.now(timezone.utc).isoformat()
        conn.executemany(
            "INSERT INTO context_receipt_message_links (receipt_id, platform, channel, message_ts, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            [(rid, platform, channel, message_ts, created_at) for rid in receipt_ids],
        )
        conn.commit()
    finally:
        conn.close()


def receipt_ids_for_message(db_path: str, *, platform: str, channel: str, message_ts: str) -> List[str]:
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        _ensure_message_links_table(conn)
        rows = conn.execute(
            "SELECT DISTINCT receipt_id FROM context_receipt_message_links WHERE platform = ? AND channel = ? AND message_ts = ?",
            (platform, channel, message_ts),
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def recent_message_links(db_path: str, *, platform: str, limit: int) -> List[Dict[str, str]]:
    """Distinct (channel, message_ts) pairs this platform has linked, most-recently-linked
    first -- lets a rating sensor discover which bot messages to poll for reactions without a
    separate per-partition sidecar file."""
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        _ensure_message_links_table(conn)
        rows = conn.execute(
            "SELECT channel, message_ts, MAX(created_at) AS latest FROM context_receipt_message_links "
            "WHERE platform = ? GROUP BY channel, message_ts ORDER BY latest DESC LIMIT ?",
            (platform, limit),
        ).fetchall()
        return [{"channel": r[0], "message_ts": r[1]} for r in rows]
    finally:
        conn.close()


def receipt_ids_for_run(db_path: str, dagster_run_id: str) -> List[str]:
    """Which receipts (from context_receipts, the canonical table knowledge_search_server/
    hybrid_knowledge_search_server write to when configured with a matching run_id_template)
    this Dagster run produced. Returns [] (not an error) if the canonical table doesn't exist
    yet -- a receipts_db_path misconfiguration or an agent that made no searches must never
    fail the reply post itself."""
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        rows = conn.execute("SELECT receipt_id FROM context_receipts WHERE dagster_run_id = ?", (dagster_run_id,)).fetchall()
        return [r[0] for r in rows]
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()


# Same vocabulary as context_receipt_store's own OUTCOME_SIGNALS / knowledge_search_server's
# own copy of it -- "+1"/"-1" (Slack's own reaction names for 👍/👎) map onto thumbs_up/thumbs_down.
OUTCOME_SIGNALS = {
    "thumbs_up": 1.0, "thumbs_down": -1.0, "accepted": 0.5, "edited": -0.3, "escalated": -0.7,
    "reopened": -0.7, "closed_won": 1.0, "closed_lost": -0.5, "reformulated": -0.2,
}


def record_outcome(db_path: str, receipt_id: str, *, signal: str, outcome_ref: Optional[str] = None) -> None:
    """Same canonical shape as knowledge_search_server's own _ReceiptWriter.record_outcome --
    copied, not imported, per this repo's self-contained-component convention. Assumes
    context_receipts/context_receipt_events already exist (created by whichever
    knowledge_search_server/hybrid_knowledge_search_server wrote receipts for this run) --
    no-ops rather than crashing the rating sensor if they don't, or if receipt_id is unknown."""
    value = OUTCOME_SIGNALS.get(signal)
    if value is None:
        raise ValueError(f"unknown signal {signal!r}")
    payload = {"signal": signal, "value": float(value), "source": "slack_reaction", "outcome_ref": outcome_ref}
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        exists = conn.execute("SELECT 1 FROM context_receipts WHERE receipt_id = ?", (receipt_id,)).fetchone()
        if exists is None:
            return
        conn.execute(
            "INSERT INTO context_receipt_events (event_id, receipt_id, event_type, occurred_at, payload_json) "
            "VALUES (?, ?, 'outcome', ?, ?)",
            (str(uuid.uuid4()), receipt_id, datetime.now(timezone.utc).isoformat(),
             json.dumps(payload, sort_keys=True, separators=(",", ":"))),
        )
        conn.commit()
    except sqlite3.OperationalError:
        return
    finally:
        conn.close()
# END receipt-message-links

# BEGIN report-manifest (read-only; inlined per component, kept byte-identical to the shared
# reader functions in report_tools_server's own writer copy -- see tests/test_report_manifest_drift.py)
import json as _rm_json  # noqa: E402,F401
import os as _rm_os  # noqa: E402,F401


def _report_run_dir(files_dir: str, run_id: Optional[str]) -> str:
    return _rm_os.path.join(_rm_os.path.expanduser(files_dir), run_id or "_unscoped")


def _report_manifest_path(files_dir: str, run_id: Optional[str]) -> str:
    return _rm_os.path.join(_report_run_dir(files_dir, run_id), "manifest.json")


def read_run_manifest(files_dir: str, run_id: Optional[str]) -> List[Dict[str, Any]]:
    """Every file {file_id, name, media_type, path, size_bytes, tool, created_at} report_tools_server has
    written for this run so far. Trusted, run-scoped lookup -- same reasoning as receipts_db_path's own
    receipt_ids_for_run: a caller only ever reads its OWN run's files by the run id Dagster itself put on the
    run (never an argument an agent could spoof)."""
    path = _report_manifest_path(files_dir, run_id)
    if not _rm_os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = _rm_json.load(f)
        return data if isinstance(data, list) else []
    except (OSError, ValueError):
        return []


def lookup_report_file(files_dir: str, run_id: Optional[str], file_id: str) -> Optional[Dict[str, Any]]:
    for entry in read_run_manifest(files_dir, run_id):
        if entry.get("file_id") == file_id:
            return entry
    return None
# END report-manifest (read-only)


def _resolve_user_email(client, user_id: str) -> Optional[str]:
    """`users.info` -> profile.email -- same shape as slack_agent_request_sensor's own copy.
    Needs `users:read.email`; any failure degrades to None."""
    try:
        resp = client.users_info(user=user_id)
        if not resp.get("ok"):
            return None
        return ((resp.get("user") or {}).get("profile") or {}).get("email")
    except Exception:  # noqa: BLE001
        return None


_REDACTED = "[REDACTED]"
_REDACT_PATTERNS = [
    (re.compile(r"(?i)\b(bearer)\s+[A-Za-z0-9._~+/=-]{8,}"), lambda m: f"{m.group(1)} {_REDACTED}"),
    (re.compile(r"\bsk-[A-Za-z0-9_-]{16,}"), lambda m: _REDACTED),
    (re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr|github_pat)_[A-Za-z0-9_]{16,}"), lambda m: _REDACTED),
    (re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}"), lambda m: _REDACTED),
    (re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"), lambda m: _REDACTED),
]


def _redact(text: str) -> str:
    s = str(text)
    for pattern, repl in _REDACT_PATTERNS:
        s = pattern.sub(repl, s)
    return s


def _get_slack_client(bot_token_env_var: str):
    """Same shape as slack_run_request_sensor's / slack_chat_tools' own `_get_slack_client`."""
    import os
    try:
        from slack_sdk import WebClient
    except ImportError as e:
        raise ImportError("slack_reply requires slack_sdk: pip install 'slack_sdk>=3.0'") from e

    token = os.environ.get(bot_token_env_var)
    if not token:
        raise RuntimeError(f"Slack bot token env var {bot_token_env_var!r} not set.")
    return WebClient(token=token)


def _extract_answer(value: Any, answer_field: Optional[str]) -> str:
    """The agent components' own documented output shape is {final_answer, iterations,
    tool_calls_made, stopped_reason, ...} -- default to `final_answer`; answer_field overrides
    for a differently-shaped upstream."""
    if answer_field:
        if isinstance(value, dict):
            return str(value.get(answer_field, ""))
        return str(getattr(value, answer_field, ""))
    if isinstance(value, dict):
        if "final_answer" in value:
            return str(value.get("final_answer") or "_(empty)_")
        return str(value)
    return str(value)


def _split_or_truncate(text: str, max_chars: int, mode: str) -> List[str]:
    text = text or "_(empty)_"
    if len(text) <= max_chars:
        return [text]
    if mode == "truncate":
        return [text[: max(0, max_chars - 20)].rstrip() + "… (truncated)"]
    parts = [text[i: i + max_chars] for i in range(0, len(text), max_chars)]
    return parts or [text]


def _run_url(webserver_base_url: Optional[str], run_id: str) -> Optional[str]:
    if not webserver_base_url:
        return None
    return f"{webserver_base_url.rstrip('/')}/runs/{run_id}"


class SlackReplyComponent(dg.Component, dg.Model, dg.Resolvable):
    """Posts a chat agent's answer into the Slack thread named by the run's own
    `slack/channel`/`slack/thread_ts` tags. Emits TWO things: `{asset_name}` (downstream of
    `upstream_asset_key`, the success path) and `{asset_name}_failure_watcher` (a run-status
    sensor that posts a short error instead, for ANY failed run carrying those same tags --
    not just one where this asset itself ran)."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: Union[str, List[str]] = Field(description="The chat agent asset to depend on and read the answer from (slash-delimited). Exactly one -- a list of more than one is a validation error.")

    slack_bot_token_env_var: str = Field(default="SLACK_BOT_TOKEN", description="Env var holding the bot's 'xoxb-...' token. Needs chat:write.")
    answer_field: Optional[str] = Field(
        default=None,
        description="Which key of the upstream asset's output dict holds the answer text. Defaults to 'final_answer' (the agent components' own documented output shape) when the upstream value is a dict; used verbatim (str()'d) otherwise.",
    )
    max_chars_per_message: int = Field(default=3000, description="Split or truncate (see on_long_answer) an answer longer than this many characters.")
    on_long_answer: Literal["split", "truncate"] = Field(default="split", description="'split' (default): post as multiple sequential thread replies, each <= max_chars_per_message. 'truncate': post one reply, cut short with an ellipsis.")
    include_run_link: bool = Field(default=True, description="Append a link to the Dagster run (requires webserver_base_url) to the last reply posted.")
    run_link_label: str = Field(default="View run", description="Link text for include_run_link.")
    webserver_base_url: Optional[str] = Field(default=None, description="Base URL of the Dagster webserver (e.g. 'http://localhost:3000' or your Dagster+ deployment URL) -- used to build the run link. Leave unset to omit the link entirely.")

    monitor_failures: bool = Field(default=True, description="Also emit {asset_name}_failure_watcher, a run-status sensor that posts a short error reply for ANY failed run carrying slack/channel + slack/thread_ts tags -- so a crashed run is never silent in the channel. Set false to handle failure notification some other way.")
    error_message_template: str = Field(default="⚠️ Something went wrong answering this: {error}", description="Template for the failure-path reply. Placeholder: {error}.")

    update_reaction_on_completion: bool = Field(
        default=True,
        description=(
            "On success, remove ack_reaction and add done_reaction to the ORIGINAL triggering "
            "message (the slack/message_ts tag slack_agent_request_sensor sets) -- so a 20-30s "
            "answer doesn't just look stuck at its acknowledgement reaction forever. On a "
            "failed run, {asset_name}_failure_watcher adds error_reaction instead (ack_reaction "
            "is still removed first). Needs reactions:write (same scope watch_reactions reads "
            "with, just the write half). Best-effort: a reaction-swap failure never fails the "
            "reply itself."
        ),
    )
    ack_reaction: str = Field(default="eyes", description="The reaction slack_agent_request_sensor posted on pickup (must match its own acknowledgement_reaction) -- removed on completion. Only used when update_reaction_on_completion is true.")
    done_reaction: str = Field(default="white_check_mark", description="Reaction added to the original message once the reply posts successfully. Only used when update_reaction_on_completion is true.")
    error_reaction: str = Field(default="warning", description="Reaction added to the original message when the run fails (added by {asset_name}_failure_watcher). Only used when update_reaction_on_completion is true.")

    receipts_db_path: Optional[str] = Field(
        default=None,
        description=(
            "SQLite file path -- the SAME receipts_db_path a knowledge_search_server/"
            "hybrid_knowledge_search_server the agent calls is configured with (and which must "
            "also have run_id_template set, the default, so its receipts carry this run's "
            "dagster_run_id). When set, after posting, this asset looks up every receipt this "
            "run produced and links them to the posted Slack message(s) -- so a later rating "
            "(watch_reactions below, or any other feedback path) can find the exact receipt_id "
            "to record an outcome against, instead of an agent having to end its reply with "
            "'ref: <receipt_id>' and have that parsed back out of the text. Best-effort: a "
            "lookup failure here never fails the reply post itself."
        ),
    )
    report_files_dir: Optional[str] = Field(
        default=None,
        description=(
            "Local directory root -- the SAME files_dir a report_tools_server this agent calls "
            "is configured with. When set, after posting the answer, this asset reads this "
            "run's manifest (written by report_tools_server's make_chart/make_pdf/make_docx/"
            "make_xlsx) and uploads every file it recorded into the same thread with "
            "files_upload_v2 (needs the `files:write` bot scope). Files may contain data the "
            "agent read, so the same people who can see the channel can see them -- posted "
            "filenames are recorded in this asset's own materialization metadata. Best-effort: "
            "an upload failure here never fails the already-posted answer."
        ),
    )
    watch_reactions: bool = Field(
        default=False,
        description=(
            "Also emit {asset_name}_rating_watcher, a sensor that polls reactions on every "
            "Slack message this asset has linked to a receipt (via receipts_db_path, which "
            "must also be set) and records a thumbs_up/thumbs_down outcome against each linked "
            "receipt_id -- the reactor's resolved email (see resolve_rater_email) becomes the "
            "outcome's `outcome_ref`, so changing a reaction later supersedes the earlier rating "
            "instead of double-counting (the same outcome_ref-supersede behavior "
            "record_feedback already has). Needs the `reactions:read` bot scope."
        ),
    )
    positive_reaction: str = Field(default="+1", description="Slack reaction name (as reactions.get returns it, e.g. '+1' for 👍) that records a thumbs_up outcome. Only used when watch_reactions is true.")
    negative_reaction: str = Field(default="-1", description="Slack reaction name (e.g. '-1' for 👎) that records a thumbs_down outcome. Only used when watch_reactions is true.")
    rating_poll_interval_seconds: int = Field(default=30, description="Minimum seconds between rating_watcher ticks. Only used when watch_reactions is true.")
    rating_lookback_messages: int = Field(default=200, description="Most-recently-linked (channel, message_ts) pairs to keep polling for reactions, and the cursor's bound. Only used when watch_reactions is true.")
    resolve_rater_email: bool = Field(default=True, description="Resolve the reacting user's Slack user id to an email address (needs users:read.email) before using it as the outcome's outcome_ref. When false, the raw Slack user id is used instead. Only used when watch_reactions is true.")

    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys, beyond upstream_asset_key -- no value loaded.")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("deps", "owners", "kinds"), csv=(), tags=("asset_tags",))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Slack Reply", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_key = dg.AssetKey(self.asset_name.split("/"))
        upstream_key = dg.AssetKey(self.upstream_asset_key.split("/"))
        bot_token_env_var = self.slack_bot_token_env_var
        answer_field = self.answer_field
        max_chars = self.max_chars_per_message
        on_long_answer = self.on_long_answer
        include_run_link = self.include_run_link
        run_link_label = self.run_link_label
        webserver_base_url = self.webserver_base_url
        error_message_template = self.error_message_template
        receipts_db_path = self.receipts_db_path
        report_files_dir = self.report_files_dir
        update_reaction_on_completion = self.update_reaction_on_completion
        ack_reaction = self.ack_reaction
        done_reaction = self.done_reaction
        error_reaction = self.error_reaction

        @dg.asset(
            key=asset_key,
            ins={"agent_output": dg.AssetIn(key=upstream_key)},
            deps=[dg.AssetKey(k.split("/")) for k in (self.deps or [])],
            group_name=self.group_name,
            description=self.description,
            owners=self.owners or [],
            tags=self.asset_tags or {},
            kinds=frozenset(self.kinds) if self.kinds else frozenset(),
        )
        def _reply(context: dg.AssetExecutionContext, agent_output: Any):
            tags = context.run.tags or {}
            channel, thread_ts = tags.get("slack/channel"), tags.get("slack/thread_ts")
            message_ts = tags.get("slack/message_ts")
            placeholder_ts = tags.get("slack/placeholder_ts")
            if not channel or not thread_ts:
                raise dg.Failure(
                    "slack_reply requires 'slack/channel' and 'slack/thread_ts' run tags "
                    "(normally set by slack_agent_request_sensor) -- this asset should only "
                    "ever be materialized as part of that chain, not run standalone."
                )
            text = _redact(_extract_answer(agent_output, answer_field))
            parts = _split_or_truncate(text, max_chars, on_long_answer)
            link = _run_url(webserver_base_url, context.run_id) if include_run_link else None
            if link:
                parts[-1] = f"{parts[-1]}\n\n<{link}|{run_link_label}>"
            client = _get_slack_client(bot_token_env_var)
            posted_ts: List[str] = []
            for i, part in enumerate(parts):
                if i == 0 and placeholder_ts:
                    # Updates the sensor's own "Looking into it…" message in place rather than
                    # posting a new one -- falls back to a fresh post if the placeholder is gone
                    # (e.g. deleted) or the update otherwise fails.
                    resp = client.chat_update(channel=channel, ts=placeholder_ts, text=part)
                    if resp.get("ok"):
                        posted_ts.append(placeholder_ts)
                        continue
                resp = client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=part)
                if not resp.get("ok"):
                    raise dg.Failure(f"Slack chat_postMessage failed: {resp.get('error')}")
                posted_ts.append(resp.get("ts"))
            if update_reaction_on_completion and message_ts:
                try:
                    client.reactions_remove(channel=channel, timestamp=message_ts, name=ack_reaction)
                except Exception:  # noqa: BLE001 -- may already be gone; never fatal
                    pass
                try:
                    client.reactions_add(channel=channel, timestamp=message_ts, name=done_reaction)
                except Exception as e:  # noqa: BLE001 -- a cosmetic reaction must never fail an already-posted reply
                    context.log.warning(f"slack_reply: could not add {done_reaction!r} to the original message: {e}")
            if receipts_db_path:
                try:
                    receipt_ids = receipt_ids_for_run(receipts_db_path, context.run.run_id)
                    for ts in posted_ts:
                        if ts:
                            record_message_links(receipts_db_path, receipt_ids, platform="slack", channel=channel, message_ts=ts)
                except Exception as e:  # noqa: BLE001 -- linking receipts must never fail an already-posted reply
                    context.log.warning(f"slack_reply: could not link receipts to the posted message(s): {e}")
            files_posted: List[str] = []
            if report_files_dir:
                try:
                    manifest = read_run_manifest(report_files_dir, context.run.run_id)
                except Exception as e:  # noqa: BLE001 -- a manifest read failure must never fail an already-posted reply
                    manifest = []
                    context.log.warning(f"slack_reply: could not read report_tools_server's manifest: {e}")
                for entry in manifest:
                    try:
                        client.files_upload_v2(channel=channel, thread_ts=thread_ts, file=entry["path"], filename=entry.get("name"))
                        files_posted.append(entry["name"])
                    except Exception as e:  # noqa: BLE001 -- one bad file upload must never block the others or the reply
                        context.log.warning(f"slack_reply: could not post generated file {entry.get('name')!r}: {e}")
            return dg.Output(
                {"posted_parts": len(parts), "channel": channel, "thread_ts": thread_ts, "files_posted": files_posted},
                metadata={
                    "posted_parts": dg.MetadataValue.int(len(parts)), "channel": dg.MetadataValue.text(channel),
                    "files_posted": dg.MetadataValue.json(files_posted),
                },
            )

        defs = dg.Definitions(assets=[_reply])

        if self.monitor_failures:
            failure_sensor_name = f"{self.asset_name.replace('/', '_')}_failure_watcher"

            @dg.run_status_sensor(
                name=failure_sensor_name,
                run_status=dg.DagsterRunStatus.FAILURE,
                monitor_all_code_locations=True,
                default_status=dg.DefaultSensorStatus.RUNNING,
            )
            def _failure_watcher(sensor_context: dg.RunStatusSensorContext):
                run_tags = sensor_context.dagster_run.tags or {}
                channel, thread_ts = run_tags.get("slack/channel"), run_tags.get("slack/thread_ts")
                message_ts = run_tags.get("slack/message_ts")
                placeholder_ts = run_tags.get("slack/placeholder_ts")
                if not channel or not thread_ts:
                    return  # not a chat-agent run -- nothing to do, same tag-based identity check as everywhere else here
                error = _redact(getattr(sensor_context.dagster_event, "message", None) or "the run failed with no further detail.")
                link = _run_url(webserver_base_url, sensor_context.dagster_run.run_id) if include_run_link else None
                text = error_message_template.format(error=error)
                if link:
                    text = f"{text}\n<{link}|{run_link_label}>"
                try:
                    client = _get_slack_client(bot_token_env_var)
                    posted = False
                    if placeholder_ts:
                        resp = client.chat_update(channel=channel, ts=placeholder_ts, text=text)
                        posted = bool(resp.get("ok"))
                    if not posted:
                        client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=text)
                    if update_reaction_on_completion and message_ts:
                        try:
                            client.reactions_remove(channel=channel, timestamp=message_ts, name=ack_reaction)
                        except Exception:  # noqa: BLE001 -- may already be gone; never fatal
                            pass
                        client.reactions_add(channel=channel, timestamp=message_ts, name=error_reaction)
                except Exception as e:  # noqa: BLE001 -- a failed failure-notification must not crash the sensor loop
                    sensor_context.log.warning(f"slack_reply failure_watcher: could not post error reply: {e}")

            defs = dg.Definitions.merge(defs, dg.Definitions(sensors=[_failure_watcher]))

        if self.watch_reactions:
            if not receipts_db_path:
                raise ValueError(f"SlackReplyComponent {self.asset_name!r}: watch_reactions requires receipts_db_path.")
            rating_sensor_name = f"{self.asset_name.replace('/', '_')}_rating_watcher"
            positive_reaction = self.positive_reaction
            negative_reaction = self.negative_reaction
            resolve_rater_email = self.resolve_rater_email
            lookback = max(1, self.rating_lookback_messages)
            signal_by_reaction = {positive_reaction: "thumbs_up", negative_reaction: "thumbs_down"}

            @dg.sensor(
                name=rating_sensor_name,
                minimum_interval_seconds=self.rating_poll_interval_seconds,
                default_status=dg.DefaultSensorStatus.RUNNING,
            )
            def _rating_watcher(sensor_context: dg.SensorEvaluationContext):
                try:
                    client = _get_slack_client(bot_token_env_var)
                except Exception as e:  # noqa: BLE001
                    return dg.SkipReason(f"Slack auth failed: {e}")
                try:
                    links = recent_message_links(receipts_db_path, platform="slack", limit=lookback)
                except Exception as e:  # noqa: BLE001
                    return dg.SkipReason(f"could not read receipt message links: {e}")
                if not links:
                    return dg.SkipReason("no receipt-linked messages to poll yet.")
                try:
                    seen: Dict[str, Dict[str, str]] = json.loads(sensor_context.cursor) if sensor_context.cursor else {}
                except Exception:  # noqa: BLE001 -- a corrupt cursor restarts clean rather than crashing the sensor forever
                    seen = {}
                email_cache: Dict[str, Optional[str]] = {}
                changed = 0
                for link in links:
                    key = f"{link['channel']}:{link['message_ts']}"
                    last_seen_for_message = dict(seen.get(key) or {})
                    try:
                        resp = client.reactions_get(channel=link["channel"], timestamp=link["message_ts"])
                    except Exception as e:  # noqa: BLE001 -- one bad message must never block the others
                        sensor_context.log.warning(f"rating_watcher: reactions_get failed for {key}: {e}")
                        continue
                    reactions = (resp.get("message") or {}).get("reactions") or []
                    for r in reactions:
                        signal = signal_by_reaction.get(r.get("name"))
                        if signal is None:
                            continue
                        for uid in r.get("users") or []:
                            if last_seen_for_message.get(uid) == signal:
                                continue  # already recorded and unchanged -- each reaction counted once
                            last_seen_for_message[uid] = signal
                            changed += 1
                            if uid not in email_cache:
                                email_cache[uid] = _resolve_user_email(client, uid) if resolve_rater_email else None
                            rater = email_cache[uid] or uid
                            try:
                                for receipt_id in receipt_ids_for_message(
                                    receipts_db_path, platform="slack", channel=link["channel"], message_ts=link["message_ts"],
                                ):
                                    record_outcome(receipts_db_path, receipt_id, signal=signal, outcome_ref=rater)
                            except Exception as e:  # noqa: BLE001 -- one bad outcome write must never block the others
                                sensor_context.log.warning(f"rating_watcher: could not record outcome for {key}/{uid}: {e}")
                    seen[key] = last_seen_for_message
                bounded = dict(list(seen.items())[-lookback:])
                return dg.SensorResult(
                    cursor=json.dumps(bounded),
                    skip_reason=None if changed else "no new or changed reactions this tick.",
                )

            defs = dg.Definitions.merge(defs, dg.Definitions(sensors=[_rating_watcher]))

        return defs
