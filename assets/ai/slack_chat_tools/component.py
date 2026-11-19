"""SlackChatToolsComponent -- the chat agent's only window onto the conversation it's answering.

A chat-triggered agent run (see `slack_agent_request_sensor`) knows almost nothing on its own:
the model's own prompt only carries `{tag.<key>}` identifiers (channel, thread, requester), never
the actual message text -- see that sensor's own README for why stuffing chat text into a Dagster
run tag is the wrong design. `get_thread()` is how the agent actually reads the conversation: it
fetches the live thread from Slack, fresh, every time it's called, using the channel/thread
identifiers this component reads from its OWN process env (set via `{tag.<key>}` substitution in
the calling agent's `mcp_servers[].env` -- same trusted, server-side mechanism
`dagster_workflow_tools`' `launch_job` uses for its requester identity, never a tool argument).
No database, no caching -- there is nothing here to go stale or leak across runs.

How agents consume it (same shape as dagster_instance_tools / dagster_workflow_tools, verified
against agentic_pipeline in this repo): `tool_use_loop` / `mcp_call` / `agent_call` / the agent
components obtain tools ONLY from MCP servers. So this file is BOTH
  1. an importable toolset (`SlackChatTools`, plus `SlackChatToolsComponent` that registers it
     as a Dagster resource), and
  2. a self-contained stdio MCP server: `python component.py` (or `python -m
     <dotted.path>` for the `mcp_server_resource_key` path).

Tool: `get_thread()` -- no arguments. Returns the last `max_messages` messages in the thread
(or just the single message, if it isn't part of one) as `{user, text, ts}` rows, oldest first.
Uses `conversations.replies`, which Slack's own docs confirm works for a solo, non-threaded
message too (it just returns that one message) -- so this component never needs to know whether
the triggering message was actually in a thread; `slack_agent_request_sensor` always sets the
`slack/thread_ts` tag to the real thread root if there is one, else the message's own `ts`.

Safety: this tool only ever reads the ONE thread its own process env names -- there is no
"channel" or "thread_ts" tool argument the model could use to read a different conversation.
Secrets accidentally pasted into a Slack message pass through `redact_text` (same patterns
`dagster_instance_tools` uses) before ever reaching the model.
"""
import json
import logging
import os
import re
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

import dagster as dg
from pydantic import Field

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

_AUDIT = logging.getLogger("slack_chat_tools.audit")

ALL_TOOLS = ("get_thread", "upload_files", "create_canvas")


# ── redaction -- same patterns dagster_instance_tools uses (duplicated, not imported, per this
# repo's self-contained-component convention) ────────────────────────────────────────────────
_REDACTED = "[REDACTED]"
_SECRET_KEY = r"(?:[\w.-]*(?:token|secret|passw(?:or)?d|passwd|pwd|api[_-]?key|apikey|access[_-]?key|private[_-]?key|client[_-]?secret|credential|authorization|auth|cookie|session[_-]?id)[\w.-]*)"
_REDACT_PATTERNS = [
    (re.compile(r"(?i)\b(authorization\s*[:=]\s*)(?:(bearer|basic|token|digest|negotiate)\s+)?[^\s,;'\"]+(?:\s+[^\s,;'\"]{8,})?"),
     lambda m: f"{m.group(1)}{(m.group(2) + ' ') if m.group(2) else ''}{_REDACTED}"),
    (re.compile(r"(?i)\b(bearer)\s+[A-Za-z0-9._~+/=-]{8,}"), lambda m: f"{m.group(1)} {_REDACTED}"),
    (re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://[^\s:/@]+:)[^\s@/]+(@)"), lambda m: f"{m.group(1)}{_REDACTED}{m.group(2)}"),
    (re.compile(r"(?i)(['\"]?" + _SECRET_KEY + r"['\"]?\s*[:=]\s*)(['\"]?)(?!\[REDACTED\]|(?:bearer|basic|token|digest|negotiate)\s)([^\s,;'\"}\]]+)(\2)"),
     lambda m: f"{m.group(1)}{m.group(2)}{_REDACTED}{m.group(4)}"),
    (re.compile(r"\bsk-[A-Za-z0-9_-]{16,}"), lambda m: _REDACTED),
    (re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr|github_pat)_[A-Za-z0-9_]{16,}"), lambda m: _REDACTED),
    (re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}"), lambda m: _REDACTED),
    (re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"), lambda m: _REDACTED),
    (re.compile(r"\bAIza[0-9A-Za-z_-]{30,}"), lambda m: _REDACTED),
]


def redact_text(text: Any) -> str:
    s = str(text)
    for pattern, repl in _REDACT_PATTERNS:
        s = pattern.sub(repl, s)
    return s


def redact_obj(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: redact_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [redact_obj(v) for v in obj]
    if isinstance(obj, str):
        return redact_text(obj)
    return obj


class ToolError(Exception):
    pass


def _get_slack_client(bot_token_env_var: str):
    """Same shape as slack_run_request_sensor's / slack_approval_gate's own `_get_slack_client`
    -- inlined fresh copy, not a shared import, per this repo's convention."""
    try:
        from slack_sdk import WebClient
    except ImportError as e:
        raise ImportError("slack_chat_tools requires slack_sdk: pip install 'slack_sdk>=3.0'") from e

    token = os.environ.get(bot_token_env_var)
    if not token:
        raise RuntimeError(
            f"Slack bot token env var {bot_token_env_var!r} not set. Create a bot at "
            f"https://api.slack.com/apps with the `channels:history` (or `groups:history`) "
            f"scope, install it to your workspace, invite it to the target channel, and export "
            f"the `xoxb-...` token."
        )
    return WebClient(token=token)


class SlackChatTools:
    """`get_thread()` is the one safe entry point (audit, size cap, redaction, error shaping).
    Reads the SAME channel/thread every call -- there is no per-call argument that could read a
    different conversation."""

    def __init__(
        self,
        *,
        bot_token_env_var: str = "SLACK_BOT_TOKEN",
        channel: Optional[str] = None,
        thread_ts: Optional[str] = None,
        max_messages: int = 20,
        max_result_chars: int = 8000,
        receipts_db_path: Optional[str] = None,
        report_files_dir: Optional[str] = None,
        run_id: Optional[str] = None,
        enable_canvas_tool: bool = False,
        audit=None,
    ):
        self.bot_token_env_var = bot_token_env_var
        self.channel = channel
        self.thread_ts = thread_ts
        self.max_messages = max(1, int(max_messages))
        self.max_result_chars = max(200, int(max_result_chars))
        self.receipts_db_path = receipts_db_path
        self.report_files_dir = report_files_dir
        self.run_id = run_id
        self.enable_canvas_tool = enable_canvas_tool
        self._audit = audit or (lambda line: _AUDIT.info(line))

    def tool_names(self) -> List[str]:
        return list(ALL_TOOLS)

    def call(self, tool: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        args = dict(args or {})
        try:
            self._audit(f"slack_chat_tools call tool={tool} args={redact_text(json.dumps(args, default=str))[:300]}")
        except Exception:  # an audit sink must never break a call
            pass
        if tool not in ALL_TOOLS:
            return {"error": f"unknown tool {tool!r}; available: {self.tool_names()}"}
        try:
            result = getattr(self, f"t_{tool}")(**args)
        except ToolError as e:
            return {"error": redact_text(str(e))}
        except TypeError as e:
            return {"error": f"bad arguments for {tool}: {redact_text(str(e))}"}
        except Exception as e:  # noqa: BLE001 -- surface as data, the agent loop must not crash
            return {"error": f"{tool} failed: {type(e).__name__}: {redact_text(str(e))[:300]}"}
        return self._fit(redact_obj(result))

    def _fit(self, result: Dict[str, Any]) -> Dict[str, Any]:
        cap = self.max_result_chars
        if len(json.dumps(result, default=str)) <= cap:
            return result
        messages = result.get("messages")
        while isinstance(messages, list) and len(messages) > 1 and len(json.dumps(result, default=str)) > cap:
            messages.pop(0)  # drop oldest first -- the most recent turns matter most to the agent
        if len(json.dumps(result, default=str)) <= cap:
            result["truncated"] = True
            return result
        text = json.dumps(result, default=str)[: max(0, cap - 120)]
        return {"truncated": True, "preview": text}

    def t_get_thread(self) -> Dict[str, Any]:
        """The conversation so far: the last `max_messages` messages in the thread this run is
        answering (or the single message, if it isn't part of a thread), oldest first, as
        {user, text, ts} rows. Always reads THIS run's own channel/thread -- there is no
        channel/thread_ts argument, so the model can never point this at a different
        conversation."""
        if not self.channel:
            raise ToolError(
                "get_thread has no channel configured (SLACK_CHAT_TOOLS_CHANNEL env var unset) "
                "-- normally set via a '{tag.slack/channel}' placeholder in the calling agent's "
                "mcp_servers[].env; see README."
            )
        if not self.thread_ts:
            raise ToolError(
                "get_thread has no thread/message identifier configured "
                "(SLACK_CHAT_TOOLS_THREAD_TS env var unset) -- normally set via a "
                "'{tag.slack/thread_ts}' placeholder in the calling agent's mcp_servers[].env; "
                "see README."
            )
        client = _get_slack_client(self.bot_token_env_var)
        resp = client.conversations_replies(channel=self.channel, ts=self.thread_ts, limit=self.max_messages)
        if not resp.get("ok"):
            raise ToolError(f"Slack conversations_replies failed: {resp.get('error')}")
        messages = (resp.get("messages") or [])[-self.max_messages:]
        rows = []
        for m in messages:
            row = {"user": m.get("user"), "text": m.get("text") or "", "ts": m.get("ts")}
            if self.receipts_db_path and m.get("bot_id"):
                # A bot-authored message is a prior reply from this same chat agent -- look up
                # which receipt(s) (knowledge-base searches) it was built from, so the model
                # can cite or reference them directly instead of parsing its own past text.
                try:
                    row["receipt_ids"] = receipt_ids_for_message(
                        self.receipts_db_path, platform="slack", channel=self.channel, message_ts=m["ts"],
                    )
                except Exception:  # noqa: BLE001 -- a lookup failure must never break get_thread itself
                    row["receipt_ids"] = []
            rows.append(row)
        return {"channel": self.channel, "thread_ts": self.thread_ts, "message_count": len(rows), "messages": rows}

    def t_upload_files(self, file_ids: List[str]) -> Dict[str, Any]:
        """Posts one or more files report_tools_server already generated (by file_id) into THIS
        thread -- same channel/thread_ts get_thread() reads, there is no channel/thread
        argument. Use this when the person wants a chart/PDF/doc/spreadsheet posted mid-
        conversation rather than waiting for the run to finish (slack_reply posts every
        generated file automatically at the end of the run regardless)."""
        if not self.report_files_dir:
            raise ToolError(
                "upload_files needs report_files_dir configured "
                "(SLACK_CHAT_TOOLS_REPORT_FILES_DIR env var unset); see README."
            )
        if not self.channel or not self.thread_ts:
            raise ToolError(
                "upload_files has no channel/thread configured -- same cause as get_thread's "
                "own error; see README."
            )
        if not file_ids:
            raise ToolError("file_ids must be a non-empty list of file_id values returned by report_tools_server.")
        client = _get_slack_client(self.bot_token_env_var)
        posted, missing = [], []
        for file_id in file_ids:
            entry = lookup_report_file(self.report_files_dir, self.run_id, file_id)
            if entry is None:
                missing.append(file_id)
                continue
            client.files_upload_v2(channel=self.channel, thread_ts=self.thread_ts, file=entry["path"], filename=entry.get("name"))
            posted.append(entry["name"])
        result: Dict[str, Any] = {"posted": posted}
        if missing:
            result["missing_file_ids"] = missing
        return result

    def t_create_canvas(self, title: str, markdown: str) -> Dict[str, Any]:
        """Creates a Slack canvas from markdown -- better than a PDF for a long document people
        will keep and edit -- tabs it into THIS channel, and returns its link. Include the link
        in your own answer text; slack_reply posts that normally, there is no separate upload
        step for a canvas."""
        if not self.enable_canvas_tool:
            raise ToolError("create_canvas is not enabled (enable_canvas_tool is false); see README.")
        if not self.channel:
            raise ToolError("create_canvas has no channel configured -- same cause as get_thread's own error; see README.")
        if not title or not markdown:
            raise ToolError("title and markdown are both required.")
        client = _get_slack_client(self.bot_token_env_var)
        resp = client.canvases_create(title=title, document_content={"type": "markdown", "markdown": markdown}, channel_id=self.channel)
        if not resp.get("ok"):
            raise ToolError(f"canvases.create failed: {resp.get('error')}")
        canvas_id = resp["canvas_id"]
        link = None
        try:
            team = client.team_info()
            if team.get("ok"):
                link = f"https://{team['team']['domain']}.slack.com/docs/{team['team']['id']}/{canvas_id}"
        except Exception:  # noqa: BLE001 -- a missing link is not fatal, the canvas itself was already created
            pass
        return {"canvas_id": canvas_id, "link": link}


# ── definitions loading not needed here -- unlike dagster_instance_tools/dagster_workflow_tools,
# this tool never touches a Dagster instance at all, only the Slack API. ──────────────────────


class SlackChatToolsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers `get_thread()` as a resource (default key `slack_chat_tools`).

    Use inside assets/ops: `context.resources.slack_chat_tools.call("get_thread", {})`.
    For an agent's `mcp_servers` (MCP only) run THIS FILE as the stdio MCP server -- see
    README.md."""

    resource_key: str = Field(default="slack_chat_tools", description="Resource key the toolset is registered under.")
    slack_bot_token_env_var: str = Field(default="SLACK_BOT_TOKEN", description="Env var holding the bot's 'xoxb-...' token. Needs channels:history (or groups:history).")
    channel_template: Optional[str] = Field(
        default="{tag.slack/channel}",
        description="Template string for this spawned server's SLACK_CHAT_TOOLS_CHANNEL env value -- the calling agent's own {tag.<key>} substitution resolves it at run time (see README). Set to a literal channel id instead if every run should always read the same fixed channel.",
    )
    thread_ts_template: Optional[str] = Field(
        default="{tag.slack/thread_ts}",
        description="Template string for this spawned server's SLACK_CHAT_TOOLS_THREAD_TS env value, same mechanism as channel_template.",
    )
    max_messages: int = Field(default=20, description="Most recent messages get_thread returns.")
    max_result_chars: int = Field(default=8000, description="Upper bound on the serialized size of any tool result.")
    receipts_db_path: Optional[str] = Field(
        default=None,
        description=(
            "SQLite file path -- the SAME receipts_db_path a slack_reply instance (and the "
            "knowledge_search_server/hybrid_knowledge_search_server the agent calls) are "
            "configured with. When set, get_thread() attaches receipt_ids to any BOT-authored "
            "message in the thread (a prior reply from this same chat agent, looked up via "
            "slack_reply's own receipt-message link table) -- so the model can see which "
            "knowledge-base searches a past answer was built from, without parsing its own "
            "text for a 'ref: <id>' marker."
        ),
    )
    report_files_dir: Optional[str] = Field(
        default=None,
        description=(
            "Local directory root -- the SAME files_dir a report_tools_server this agent calls "
            "is configured with. When set, ALSO registers an upload_files tool so the agent can "
            "post a generated chart/PDF/doc/spreadsheet (by file_id) into the thread mid-"
            "conversation, instead of waiting for slack_reply's own end-of-run delivery. Needs "
            "run_id_template too (left at its default) so this run's own files can be found."
        ),
    )
    run_id_template: Optional[str] = Field(
        default="{run_id}",
        description=(
            "Template for the spawned server's SLACK_CHAT_TOOLS_RUN_ID env var -- left as the "
            "literal string '{run_id}' here; the CALLING agent's own generic {run_id}/{tag.*} "
            "substitution resolves it to the real run id before the subprocess is spawned (same "
            "mechanism as report_tools_server's own run_id_template). Only used by upload_files "
            "(report_files_dir)."
        ),
    )
    enable_canvas_tool: bool = Field(
        default=False,
        description=(
            "Also registers create_canvas(title, markdown): creates a Slack canvas (better "
            "than a PDF for a long document people will keep and edit), tabs it into this "
            "channel, and returns its link for the agent to include in its own answer. Needs "
            "canvases:write -- a NEW scope beyond what this component otherwise needs."
        ),
    )
    mcp_server_resource_key: Optional[str] = Field(
        default=None,
        description=(
            "If set, ALSO registers a stdio MCP-server-shaped resource (same name/type/command/"
            "env shape as MCPServerResource) under this key, built from this component's own "
            "settings. channel_template/thread_ts_template are passed through VERBATIM as env "
            "values -- resolved by the calling agent's own {tag.*} substitution at run time, not "
            "by this component. Any mcp_servers-aware component (the agent components, "
            "agentic_pipeline) can then reference it via server_resource_key instead of "
            "hand-writing an inline command+env entry. Leave unset to skip this -- existing "
            "resource_key-only behavior is unchanged."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=(), csv=(), tags=())

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Slack Chat Tools", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        bot_token_env_var, max_messages, max_result_chars = self.slack_bot_token_env_var, self.max_messages, self.max_result_chars
        receipts_db_path = self.receipts_db_path
        report_files_dir = self.report_files_dir
        enable_canvas_tool = self.enable_canvas_tool

        def _init(init_context):
            # The in-process resource_key path has no per-run tag substitution to resolve
            # channel/thread_ts from (that mechanism lives in the calling agent component's
            # own compute function) -- a direct caller using this path must set them itself
            # by constructing SlackChatTools directly, same as dagster_workflow_tools'
            # resource_key path has no `requester` either.
            return SlackChatTools(
                bot_token_env_var=bot_token_env_var, max_messages=max_messages, max_result_chars=max_result_chars,
                receipts_db_path=receipts_db_path, report_files_dir=report_files_dir, enable_canvas_tool=enable_canvas_tool,
            )

        res = dg.ResourceDefinition(resource_fn=_init, description="Reads the Slack thread a chat-triggered agent run is answering.")
        resources: Dict[str, Any] = {self.resource_key: res}

        if self.mcp_server_resource_key:
            mcp_env: Dict[str, str] = {
                "SLACK_CHAT_TOOLS_MAX_MESSAGES": str(max_messages),
                "SLACK_CHAT_TOOLS_MAX_CHARS": str(max_result_chars),
            }
            if self.channel_template:
                mcp_env["SLACK_CHAT_TOOLS_CHANNEL"] = self.channel_template
            if self.thread_ts_template:
                mcp_env["SLACK_CHAT_TOOLS_THREAD_TS"] = self.thread_ts_template
            if receipts_db_path:
                mcp_env["SLACK_CHAT_TOOLS_RECEIPTS_DB_PATH"] = receipts_db_path
            if report_files_dir:
                mcp_env["SLACK_CHAT_TOOLS_REPORT_FILES_DIR"] = report_files_dir
                if self.run_id_template:
                    mcp_env["SLACK_CHAT_TOOLS_RUN_ID"] = self.run_id_template
            if enable_canvas_tool:
                mcp_env["SLACK_CHAT_TOOLS_ENABLE_CANVAS"] = "1"
            resources[self.mcp_server_resource_key] = _McpServerShapeResource(
                name=self.mcp_server_resource_key,
                type="stdio",
                command=[sys.executable, "-m", _this_module_name()],
                env=mcp_env,
                live_env_passthrough=[bot_token_env_var],
            )

        return dg.Definitions(resources=resources)


# ── stdio MCP server entrypoint: python component.py (or python -m <dotted.path> for the
# mcp_server_resource_key path) ─────────────────────────────────────────────────────────────
class _McpServerShapeResource(dg.ConfigurableResource):
    """Minimal MCPServerResource-shaped resource -- inlined here rather than imported from
    resources/mcp_server_resource (confirmed not importable across components; same duck-typed
    copy every mcp_servers-aware consumer already expects). Same shape as dagster_instance_tools'
    / dagster_workflow_tools' own copies."""

    name: str
    type: Literal["http", "sse", "stdio"] = "stdio"
    command: Optional[List[str]] = None
    url: Optional[str] = None
    env: Optional[Dict[str, str]] = None
    headers: Optional[Dict[str, str]] = None
    headers_env: Optional[Dict[str, str]] = None
    live_env_passthrough: Optional[List[str]] = None

    def model_dump(self, *args, **kwargs):
        dumped = super().model_dump(*args, **kwargs)
        passthrough = dumped.pop("live_env_passthrough", None)
        if dumped.get("type") == "stdio" and passthrough:
            env = dict(dumped.get("env") or {})
            for var_name in passthrough:
                value = os.environ.get(var_name)
                if value:
                    env[var_name] = value
            dumped["env"] = env
        return dumped


def _this_module_name() -> str:
    """The real importable dotted path for `python -m <this>` -- only needed for the
    mcp_server_resource_key path (a dotted `-m` invocation works wherever the package is
    installed, independent of knowing this file's absolute path). This tool doesn't need job
    reconstruction, so a bare `python component.py` invocation works fine too."""
    if __spec__ is not None:
        return __spec__.name
    raise RuntimeError(
        "slack_chat_tools's mcp_server_resource_key path must be run as `python -m "
        "<dotted.path.to.this.module>` (e.g. `python -m "
        "dagster_community_components.assets.ai.slack_chat_tools.component`), not "
        "`python component.py` directly."
    )


def build_mcp_server(tools: SlackChatTools):
    """MCP server (official `mcp` package) exposing get_thread. Works on mcp 1.x and 2.x (same
    compat shim as dagster_instance_tools'/dagster_workflow_tools' own build_mcp_server)."""
    try:  # mcp 2.x renamed FastMCP -> MCPServer (mcp.server.fastmcp now raises ModuleNotFoundError)
        from mcp.server.mcpserver import MCPServer as _Server
    except ImportError:  # mcp 1.x
        from mcp.server.fastmcp import FastMCP as _Server

    mcp = _Server("slack-chat-tools")

    @mcp.tool()
    def get_thread() -> dict:
        """The conversation so far: the last messages in the thread this run is answering (or the single message, if not in a thread), oldest first, as {user, text, ts} rows."""
        return tools.call("get_thread", {})

    @mcp.tool()
    def upload_files(file_ids: list) -> dict:
        """Posts one or more files report_tools_server already generated (by file_id, from its make_chart/make_pdf/make_docx/make_xlsx results) into the thread this run is answering, right now -- use this when the person wants a file posted mid-conversation rather than waiting for the run to finish."""
        return tools.call("upload_files", {"file_ids": file_ids})

    @mcp.tool()
    def create_canvas(title: str, markdown: str) -> dict:
        """Creates a Slack canvas from markdown -- use this for a long document the person wants to keep and edit, instead of a PDF. Tabs it into this channel and returns {canvas_id, link}; include the link in your own answer text."""
        return tools.call("create_canvas", {"title": title, "markdown": markdown})

    return mcp


def main(argv: Optional[List[str]] = None) -> None:
    """Env: SLACK_BOT_TOKEN (or whatever slack_bot_token_env_var names), SLACK_CHAT_TOOLS_CHANNEL,
    SLACK_CHAT_TOOLS_THREAD_TS (both normally resolved per-run by the CALLING agent's own
    {tag.*} substitution -- see README), SLACK_CHAT_TOOLS_MAX_MESSAGES (default 20),
    SLACK_CHAT_TOOLS_MAX_CHARS (default 8000),
    SLACK_CHAT_TOOLS_RECEIPTS_DB_PATH (optional -- attaches receipt_ids to bot messages),
    SLACK_CHAT_TOOLS_REPORT_FILES_DIR + SLACK_CHAT_TOOLS_RUN_ID (optional -- enables upload_files),
    SLACK_CHAT_TOOLS_ENABLE_CANVAS (optional, any truthy value -- enables create_canvas)."""
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(message)s")  # stdout is the MCP channel
    tools = SlackChatTools(
        bot_token_env_var=os.environ.get("SLACK_CHAT_TOOLS_BOT_TOKEN_ENV_VAR", "SLACK_BOT_TOKEN"),
        channel=os.environ.get("SLACK_CHAT_TOOLS_CHANNEL") or None,
        thread_ts=os.environ.get("SLACK_CHAT_TOOLS_THREAD_TS") or None,
        max_messages=int(os.environ.get("SLACK_CHAT_TOOLS_MAX_MESSAGES", "20")),
        max_result_chars=int(os.environ.get("SLACK_CHAT_TOOLS_MAX_CHARS", "8000")),
        receipts_db_path=os.environ.get("SLACK_CHAT_TOOLS_RECEIPTS_DB_PATH") or None,
        report_files_dir=os.environ.get("SLACK_CHAT_TOOLS_REPORT_FILES_DIR") or None,
        run_id=os.environ.get("SLACK_CHAT_TOOLS_RUN_ID") or None,
        enable_canvas_tool=bool(os.environ.get("SLACK_CHAT_TOOLS_ENABLE_CANVAS")),
    )
    build_mcp_server(tools).run(transport="stdio")


if __name__ == "__main__":
    main()
