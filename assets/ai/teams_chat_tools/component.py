"""TeamsChatToolsComponent -- the chat agent's only window onto the Teams conversation it's
answering. Teams analog of `slack_chat_tools` -- no shared code (per this repo's convention),
but the same design.

A chat-triggered agent run (see `teams_agent_request_sensor`) knows almost nothing on its own:
the model's own prompt only carries `{tag.<key>}` identifiers (team, channel, thread,
requester), never the actual message text. `get_thread()` is how the agent actually reads the
conversation: it fetches the live thread from Microsoft Graph, fresh, every time it's called,
using the team/channel/thread identifiers this component reads from its OWN process env (set
via `{tag.<key>}` substitution in the calling agent's `mcp_servers[].env`, the same trusted,
server-side mechanism `dagster_workflow_tools`' `launch_job` uses for its requester identity,
never a tool argument). No database, no caching -- there is nothing here to go stale or leak
across runs.

Tool: `get_thread()` -- no arguments. Returns the last `max_messages` messages in the thread
(root message + replies, merged and sorted) as `{user, text, id, created_at}` rows, oldest
first, HTML stripped to plain text.

**A real design difference from Slack, not a copy-paste**: Slack's `conversations.replies`
returns the thread's root message AND its replies in one call, so `slack_chat_tools` never
needs to special-case "is this message the root." Microsoft Graph's
`.../messages/{id}/replies` returns ONLY replies (confirmed against Microsoft's own
`chatmessage-list-replies` reference) -- the root is a separate resource. So this component
makes two calls (`GET .../messages/{thread_id}` for the root, `GET .../messages/{thread_id}/
replies` for the rest) and merges them client-side, sorted by `createdDateTime`.

Safety: this tool only ever reads the ONE thread its own process env names -- there is no
team/channel/thread-id tool argument the model could use to read a different conversation.
Secrets accidentally pasted into a Teams message pass through `redact_text` (same patterns
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
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import dagster as dg
from pydantic import Field, model_validator

_GRAPH = "https://graph.microsoft.com/v1.0"

# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=(), strs=()):
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
    for k in strs:  # str (or Dict[str, str]) fields: a numeric-looking YAML scalar resolves to int/float/bool
        v = out.get(k)
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            out[k] = str(v)
        elif isinstance(v, dict):
            out[k] = {str(a): (("true" if b is True else "false") if isinstance(b, bool) else (str(b) if isinstance(b, (int, float)) else b)) for a, b in v.items()}
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

_AUDIT = logging.getLogger("teams_chat_tools.audit")

ALL_TOOLS = ("get_thread",)


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


# ── Inline fs helper: local + cloud (s3://, gs://, abfs://) via fsspec. Same shape as
# teams_run_request_sensor's / teams_agent_request_sensor's own copy, not a shared import.
# Only used to persist a rotated OAuth refresh token. ──
class _StateFS:
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

    def read_json(self, p: str) -> Any:
        import json as _json
        if self.is_uri:
            with self.fs.open(p, "r") as f:
                return _json.loads(f.read())
        return _json.loads(Path(p).read_text())

    def write_json(self, p: str, obj: Any) -> None:
        import json as _json
        body = _json.dumps(obj, indent=2, default=str)
        if self.is_uri:
            try:
                self.fs.makedirs("/".join(p.split("/")[:-1]), exist_ok=True)
            except Exception:
                pass
            with self.fs.open(p, "w") as f:
                f.write(body)
        else:
            Path(p).parent.mkdir(parents=True, exist_ok=True)
            Path(p).write_text(body)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _oauth_state_path(fs: "_StateFS") -> str:
    return fs.path(".teams_oauth_state.json")


def _get_teams_bearer(fs: "_StateFS", tenant_id_env: str, client_id_env: str, client_secret_env: str, refresh_token_env: str) -> str:
    """OAuth2 refresh-token grant against Azure AD -- a DELEGATED token, not app-only. Same
    shape as teams_run_request_sensor's own copy -- see that component's README ("Auth") for
    why app-only has no working path here."""
    import requests
    tenant_id = os.environ.get(tenant_id_env)
    client_id = os.environ.get(client_id_env)
    client_secret = os.environ.get(client_secret_env)
    seed_refresh_token = os.environ.get(refresh_token_env)
    if not (tenant_id and client_id and client_secret and seed_refresh_token):
        raise RuntimeError(
            f"Teams auth env vars not all set: "
            f"{tenant_id_env}={'✓' if tenant_id else '✗'}, "
            f"{client_id_env}={'✓' if client_id else '✗'}, "
            f"{client_secret_env}={'✓' if client_secret else '✗'}, "
            f"{refresh_token_env}={'✓' if seed_refresh_token else '✗'}."
        )
    refresh_token = seed_refresh_token
    state_path = _oauth_state_path(fs)
    if fs.exists(state_path):
        try:
            saved = fs.read_json(state_path)
            if saved.get("refresh_token"):
                refresh_token = saved["refresh_token"]
        except Exception:  # noqa: BLE001 -- a corrupt state file falls back to the seed token, not a crash
            pass

    r = requests.post(
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://graph.microsoft.com/.default offline_access",
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        timeout=30,
    )
    r.raise_for_status()
    body = r.json()
    new_refresh_token = body.get("refresh_token")
    if new_refresh_token and new_refresh_token != refresh_token:
        fs.write_json(state_path, {"refresh_token": new_refresh_token, "rotated_at": _now_iso()})
    return body["access_token"]


def _get_teams_message(bearer: str, team_id: str, channel_id: str, message_id: str) -> Dict[str, Any]:
    import requests
    r = requests.get(
        f"{_GRAPH}/teams/{team_id}/channels/{channel_id}/messages/{message_id}",
        headers={"Authorization": f"Bearer {bearer}"}, timeout=30,
    )
    r.raise_for_status()
    return r.json()


def _get_teams_replies(bearer: str, team_id: str, channel_id: str, message_id: str, top: int, max_pages: int) -> List[Dict[str, Any]]:
    """Lists ALL current replies to one message -- Graph's replies endpoint has no delta/
    incremental query (confirmed against Microsoft's own `chatmessage-list-replies`
    reference); only `$top` (hard-capped at 50) and `@odata.nextLink` pagination."""
    import requests
    url = f"{_GRAPH}/teams/{team_id}/channels/{channel_id}/messages/{message_id}/replies"
    params: Optional[Dict[str, Any]] = {"$top": min(max(top, 1), 50)}
    out: List[Dict[str, Any]] = []
    pages = 0
    while url and pages < max(max_pages, 1):
        r = requests.get(url, headers={"Authorization": f"Bearer {bearer}"}, params=params, timeout=30)
        r.raise_for_status()
        body = r.json()
        out.extend(body.get("value", []) or [])
        url = body.get("@odata.nextLink")
        params = None
        pages += 1
    return out


def _strip_html(html: str) -> str:
    return re.sub(r"<[^>]+>", " ", html or "").strip()


class TeamsChatTools:
    """`get_thread()` is the one safe entry point (audit, size cap, redaction, error shaping).
    Reads the SAME team/channel/thread every call -- there is no per-call argument that could
    read a different conversation."""

    def __init__(
        self,
        *,
        tenant_id_env: str = "TEAMS_TENANT_ID",
        client_id_env: str = "TEAMS_CLIENT_ID",
        client_secret_env: str = "TEAMS_CLIENT_SECRET",
        refresh_token_env: str = "TEAMS_REFRESH_TOKEN",
        oauth_state_dir: str = ".",
        team_id: Optional[str] = None,
        channel_id: Optional[str] = None,
        thread_id: Optional[str] = None,
        max_messages: int = 20,
        max_result_chars: int = 8000,
        receipts_db_path: Optional[str] = None,
        audit=None,
    ):
        self.tenant_id_env = tenant_id_env
        self.client_id_env = client_id_env
        self.client_secret_env = client_secret_env
        self.refresh_token_env = refresh_token_env
        self.oauth_state_dir = oauth_state_dir
        self.team_id = team_id
        self.channel_id = channel_id
        self.thread_id = thread_id
        self.max_messages = max(1, int(max_messages))
        self.max_result_chars = max(200, int(max_result_chars))
        self.receipts_db_path = receipts_db_path
        self._audit = audit or (lambda line: _AUDIT.info(line))

    def tool_names(self) -> List[str]:
        return list(ALL_TOOLS)

    def call(self, tool: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        args = dict(args or {})
        try:
            self._audit(f"teams_chat_tools call tool={tool} args={redact_text(json.dumps(args, default=str))[:300]}")
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
        answering (root message + replies, merged and sorted by createdDateTime), oldest
        first, as {user, text, id, created_at} rows. Always reads THIS run's own team/channel/
        thread -- there is no argument, so the model can never point this at a different
        conversation."""
        if not self.team_id:
            raise ToolError("get_thread has no team configured (TEAMS_CHAT_TOOLS_TEAM_ID env var unset) -- normally set via a '{tag.teams/team_id}' placeholder in the calling agent's mcp_servers[].env; see README.")
        if not self.channel_id:
            raise ToolError("get_thread has no channel configured (TEAMS_CHAT_TOOLS_CHANNEL_ID env var unset) -- normally set via a '{tag.teams/channel_id}' placeholder in the calling agent's mcp_servers[].env; see README.")
        if not self.thread_id:
            raise ToolError("get_thread has no thread configured (TEAMS_CHAT_TOOLS_THREAD_ID env var unset) -- normally set via a '{tag.teams/thread_id}' placeholder in the calling agent's mcp_servers[].env; see README.")

        fs = _StateFS(self.oauth_state_dir)
        bearer = _get_teams_bearer(fs, self.tenant_id_env, self.client_id_env, self.client_secret_env, self.refresh_token_env)
        root = _get_teams_message(bearer, self.team_id, self.channel_id, self.thread_id)
        replies = _get_teams_replies(bearer, self.team_id, self.channel_id, self.thread_id, self.max_messages, max_pages=3)
        combined = sorted([root, *replies], key=lambda m: m.get("createdDateTime", ""))[-self.max_messages:]

        rows = []
        for m in combined:
            sender = (m.get("from") or {}).get("user")
            is_bot = not sender and bool((m.get("from") or {}).get("application"))
            row = {
                "user": (sender or {}).get("id"),
                "text": _strip_html((m.get("body") or {}).get("content") or ""),
                "id": m.get("id"),
                "created_at": m.get("createdDateTime"),
            }
            if self.receipts_db_path and is_bot:
                # A bot-authored message is a prior reply from this same chat agent -- look up
                # which receipt(s) (knowledge-base searches) it was built from, so the model
                # can cite or reference them directly instead of parsing its own past text.
                # channel encodes "{team_id}|{channel_id}" and message_ts encodes
                # "{thread_id}|{reply_id}" (not just the bare ids) -- teams_reply writes the
                # SAME composites, since addressing a Graph call (team+channel) and a specific
                # reply's own reactions (thread+reply) later (the rating watcher) both need
                # more than Slack's flat channel/ts addressing. See teams_reply's own module
                # docstring.
                try:
                    row["receipt_ids"] = receipt_ids_for_message(
                        self.receipts_db_path, platform="teams", channel=f"{self.team_id}|{self.channel_id}",
                        message_ts=f"{self.thread_id}|{m['id']}",
                    )
                except Exception:  # noqa: BLE001 -- a lookup failure must never break get_thread itself
                    row["receipt_ids"] = []
            rows.append(row)
        return {"team_id": self.team_id, "channel_id": self.channel_id, "thread_id": self.thread_id, "message_count": len(rows), "messages": rows}


class TeamsChatToolsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers `get_thread()` as a resource (default key `teams_chat_tools`).

    Use inside assets/ops: `context.resources.teams_chat_tools.call("get_thread", {})`.
    For an agent's `mcp_servers` (MCP only) run THIS FILE as the stdio MCP server -- see
    README.md."""

    resource_key: str = Field(default="teams_chat_tools", description="Resource key the toolset is registered under.")
    teams_tenant_id_env_var: str = Field(default="TEAMS_TENANT_ID", description="Env var holding the Azure AD tenant id.")
    teams_client_id_env_var: str = Field(default="TEAMS_CLIENT_ID", description="Env var holding the Azure AD app registration client id.")
    teams_client_secret_env_var: str = Field(default="TEAMS_CLIENT_SECRET", description="Env var holding the Azure AD app registration client secret.")
    teams_refresh_token_env_var: str = Field(default="TEAMS_REFRESH_TOKEN", description="Env var holding the INITIAL OAuth refresh token (see teams_run_request_sensor's README, 'Auth').")
    oauth_state_dir: str = Field(description="Local path or fsspec URI dedicated to this server's '.teams_oauth_state.json' (a rotated refresh token, if Microsoft ever issues one). Can be the SAME directory teams_agent_request_sensor uses -- they share nothing in code, but rotation state for the same Entra app/account is safe to share.")
    team_id_template: Optional[str] = Field(
        default="{tag.teams/team_id}",
        description="Template string for this spawned server's TEAMS_CHAT_TOOLS_TEAM_ID env value -- the calling agent's own {tag.<key>} substitution resolves it at run time. Set to a literal team id instead if every run should always read the same fixed team.",
    )
    channel_id_template: Optional[str] = Field(
        default="{tag.teams/channel_id}",
        description="Template string for this spawned server's TEAMS_CHAT_TOOLS_CHANNEL_ID env value, same mechanism as team_id_template.",
    )
    thread_id_template: Optional[str] = Field(
        default="{tag.teams/thread_id}",
        description="Template string for this spawned server's TEAMS_CHAT_TOOLS_THREAD_ID env value, same mechanism as team_id_template.",
    )
    max_messages: int = Field(default=20, description="Most recent messages get_thread returns.")
    max_result_chars: int = Field(default=8000, description="Upper bound on the serialized size of any tool result.")
    receipts_db_path: Optional[str] = Field(
        default=None,
        description=(
            "SQLite file path -- the SAME receipts_db_path a teams_reply instance (and the "
            "knowledge_search_server/hybrid_knowledge_search_server the agent calls) are "
            "configured with. When set, get_thread() attaches receipt_ids to any BOT-authored "
            "message in the thread (a prior reply from this same chat agent, looked up via "
            "teams_reply's own receipt-message link table)."
        ),
    )
    mcp_server_resource_key: Optional[str] = Field(
        default=None,
        description=(
            "If set, ALSO registers a stdio MCP-server-shaped resource (same name/type/command/"
            "env shape as MCPServerResource) under this key, built from this component's own "
            "settings. *_template fields are passed through VERBATIM as env values -- resolved "
            "by the calling agent's own {tag.*} substitution at run time, not by this "
            "component. Leave unset to skip this -- existing resource_key-only behavior is "
            "unchanged."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=(), csv=(), tags=())

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Teams Chat Tools", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        tenant_env, client_env, secret_env, refresh_env = (
            self.teams_tenant_id_env_var, self.teams_client_id_env_var,
            self.teams_client_secret_env_var, self.teams_refresh_token_env_var,
        )
        oauth_state_dir, max_messages, max_result_chars = self.oauth_state_dir, self.max_messages, self.max_result_chars
        receipts_db_path = self.receipts_db_path

        def _init(init_context):
            # The in-process resource_key path has no per-run tag substitution to resolve
            # team/channel/thread from (that mechanism lives in the calling agent component's
            # own compute function) -- a direct caller using this path must set them itself by
            # constructing TeamsChatTools directly, same as slack_chat_tools' resource_key path.
            return TeamsChatTools(
                tenant_id_env=tenant_env, client_id_env=client_env, client_secret_env=secret_env, refresh_token_env=refresh_env,
                oauth_state_dir=oauth_state_dir, max_messages=max_messages, max_result_chars=max_result_chars,
                receipts_db_path=receipts_db_path,
            )

        res = dg.ResourceDefinition(resource_fn=_init, description="Reads the Teams thread a chat-triggered agent run is answering.")
        resources: Dict[str, Any] = {self.resource_key: res}

        if self.mcp_server_resource_key:
            mcp_env: Dict[str, str] = {
                "TEAMS_CHAT_TOOLS_MAX_MESSAGES": str(max_messages),
                "TEAMS_CHAT_TOOLS_MAX_CHARS": str(max_result_chars),
                "TEAMS_CHAT_TOOLS_OAUTH_STATE_DIR": oauth_state_dir,
            }
            if self.team_id_template:
                mcp_env["TEAMS_CHAT_TOOLS_TEAM_ID"] = self.team_id_template
            if self.channel_id_template:
                mcp_env["TEAMS_CHAT_TOOLS_CHANNEL_ID"] = self.channel_id_template
            if self.thread_id_template:
                mcp_env["TEAMS_CHAT_TOOLS_THREAD_ID"] = self.thread_id_template
            if receipts_db_path:
                mcp_env["TEAMS_CHAT_TOOLS_RECEIPTS_DB_PATH"] = receipts_db_path
            resources[self.mcp_server_resource_key] = _McpServerShapeResource(
                name=self.mcp_server_resource_key,
                type="stdio",
                command=[sys.executable, "-m", _this_module_name()],
                env=mcp_env,
                live_env_passthrough=[client_env, secret_env, tenant_env, refresh_env],
            )

        return dg.Definitions(resources=resources)


# ── stdio MCP server entrypoint: python component.py (or python -m <dotted.path> for the
# mcp_server_resource_key path) ─────────────────────────────────────────────────────────────
class _McpServerShapeResource(dg.ConfigurableResource):
    """Minimal MCPServerResource-shaped resource -- inlined here rather than imported from
    resources/mcp_server_resource (confirmed not importable across components; same duck-typed
    copy every mcp_servers-aware consumer already expects). Same shape as slack_chat_tools' /
    dagster_instance_tools' own copies."""

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
    mcp_server_resource_key path. This tool doesn't need job reconstruction, so a bare
    `python component.py` invocation works fine too."""
    if __spec__ is not None:
        return __spec__.name
    raise RuntimeError(
        "teams_chat_tools's mcp_server_resource_key path must be run as `python -m "
        "<dotted.path.to.this.module>` (e.g. `python -m "
        "dagster_community_components.assets.ai.teams_chat_tools.component`), not "
        "`python component.py` directly."
    )


def build_mcp_server(tools: TeamsChatTools):
    """MCP server (official `mcp` package) exposing get_thread. Works on mcp 1.x and 2.x (same
    compat shim as slack_chat_tools'/dagster_instance_tools' own build_mcp_server)."""
    try:  # mcp 2.x renamed FastMCP -> MCPServer (mcp.server.fastmcp now raises ModuleNotFoundError)
        from mcp.server.mcpserver import MCPServer as _Server
    except ImportError:  # mcp 1.x
        from mcp.server.fastmcp import FastMCP as _Server

    try:  # the SDK's own ToolError, not this module's -- see _mcp_call below for why that distinction matters.
        from mcp.server.mcpserver.exceptions import ToolError as _SdkToolError  # mcp 2.x (confirmed live, 2.3.0)
    except ImportError:
        from mcp.server.fastmcp.exceptions import ToolError as _SdkToolError  # mcp 1.x (stable FastMCP API, not live-verified here)

    mcp = _Server("teams-chat-tools")

    def _mcp_call(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """tools.call() never raises (see module docstring) -- a bad-arguments or failure comes back as a plain
        {"error": ...} dict, which the MCP transport would otherwise hand back as a NORMAL (isError=False) tool
        result indistinguishable from success. Re-raising here lets the `mcp` package's own exception handling
        (both fastmcp 1.x and mcp.server.mcpserver 2.x catch any exception from a registered tool function and
        turn it into a real isError=True CallToolResult) make the failure visible to whatever's counting failed
        calls in the agent loop. MUST be the SDK's own ToolError (_SdkToolError), not this module's: the SDK's
        tool dispatch (mcp/server/mcpserver/tools/base.py `Tool.run`) only keeps a raised exception's own message
        when it recognizes the exception as ITS OWN ToolError/ResourceError -- anything else (including a
        same-named ToolError class defined elsewhere) is treated as an unexpected crash and replaced with a
        generic 'Error executing tool <name>' before it reaches the client, throwing away the reason the agent
        needs to act on."""
        result = tools.call(name, args)
        if isinstance(result, dict) and "error" in result:
            raise _SdkToolError(result["error"])
        return result

    @mcp.tool()
    def get_thread() -> dict:
        """The conversation so far: the last messages in the thread this run is answering (root + replies, merged and sorted), oldest first, as {user, text, id, created_at} rows."""
        return _mcp_call("get_thread", {})

    return mcp


def main(argv: Optional[List[str]] = None) -> None:
    """Env: TEAMS_TENANT_ID/TEAMS_CLIENT_ID/TEAMS_CLIENT_SECRET/TEAMS_REFRESH_TOKEN (delegated
    OAuth -- see teams_run_request_sensor's README, 'Auth'), TEAMS_CHAT_TOOLS_OAUTH_STATE_DIR,
    TEAMS_CHAT_TOOLS_TEAM_ID / _CHANNEL_ID / _THREAD_ID (normally resolved per-run by the
    CALLING agent's own {tag.*} substitution -- see README), TEAMS_CHAT_TOOLS_MAX_MESSAGES
    (default 20), TEAMS_CHAT_TOOLS_MAX_CHARS (default 8000),
    TEAMS_CHAT_TOOLS_RECEIPTS_DB_PATH (optional -- attaches receipt_ids to bot messages)."""
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(message)s")  # stdout is the MCP channel
    tools = TeamsChatTools(
        tenant_id_env="TEAMS_TENANT_ID", client_id_env="TEAMS_CLIENT_ID",
        client_secret_env="TEAMS_CLIENT_SECRET", refresh_token_env="TEAMS_REFRESH_TOKEN",
        oauth_state_dir=os.environ.get("TEAMS_CHAT_TOOLS_OAUTH_STATE_DIR", "."),
        team_id=os.environ.get("TEAMS_CHAT_TOOLS_TEAM_ID") or None,
        channel_id=os.environ.get("TEAMS_CHAT_TOOLS_CHANNEL_ID") or None,
        thread_id=os.environ.get("TEAMS_CHAT_TOOLS_THREAD_ID") or None,
        max_messages=int(os.environ.get("TEAMS_CHAT_TOOLS_MAX_MESSAGES", "20")),
        max_result_chars=int(os.environ.get("TEAMS_CHAT_TOOLS_MAX_CHARS", "8000")),
        receipts_db_path=os.environ.get("TEAMS_CHAT_TOOLS_RECEIPTS_DB_PATH") or None,
    )
    build_mcp_server(tools).run(transport="stdio")


if __name__ == "__main__":
    main()
