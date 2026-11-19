"""TeamsReplyComponent -- deterministically posts a chat agent's answer back into the Teams
thread it's answering. Teams analog of `slack_reply` -- no shared code (per this repo's
convention), but the same design: a model is never trusted to remember to call a "post" tool --
if it forgets, or the run fails outright, the human sees nothing. This component is the
opposite of that: a plain downstream asset for the success path, PLUS a run-failure sensor
that fires whenever any failed run carries the same `teams/team_id`/`teams/channel_id`/
`teams/thread_id` tags `teams_agent_request_sensor` sets, so a failed run still gets a reply
(a short error + a run link) instead of silence.

Both halves read team/channel/thread straight off the RUN's own tags (`context.run.tags` for
the success-path asset, `context.dagster_run.tags` for the failure sensor) -- the same
identifiers `teams_agent_request_sensor` set, never re-derived or guessed.

## Addressing a specific reply's reactions needs more than Slack's flat ts

Slack's `reactions.get(channel, timestamp)` addresses ANY message (root or reply) with just a
channel id + its own ts. Microsoft Graph has no such flat addressing: reading or reacting to a
specific reply needs its PARENT thread id too (`.../messages/{threadId}/replies/{replyId}/...`),
and every Graph call needs a `team_id` the channel id alone doesn't carry. So the
`context_receipt_message_links` table's generic `channel`/`message_ts` string columns are used
here as `channel = "{team_id}|{channel_id}"` and `message_ts = "{thread_id}|{reply_id}"` --
composites, not literal ids -- so the same byte-identical, platform-agnostic table and
functions `slack_reply`/`slack_chat_tools` use can still address a Teams reply precisely.
`teams_chat_tools` writes/reads the exact same composite shape.
"""
import json
import os
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field, model_validator

_GRAPH = "https://graph.microsoft.com/v1.0"

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


def _post_teams_reply(bearer: str, team_id: str, channel_id: str, thread_id: str, html_body: str) -> Dict[str, Any]:
    """Posts a reply and returns the created chatMessage (unlike teams_run_request_sensor's/
    teams_approval_gate's own copy, which discards the response -- this component needs the
    new reply's own `id` to link receipts and later read its reactions)."""
    import requests
    r = requests.post(
        f"{_GRAPH}/teams/{team_id}/channels/{channel_id}/messages/{thread_id}/replies",
        headers={"Authorization": f"Bearer {bearer}", "Content-Type": "application/json"},
        json={"body": {"contentType": "html", "content": html_body}},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def _get_teams_replies(bearer: str, team_id: str, channel_id: str, thread_id: str, top: int, max_pages: int) -> List[Dict[str, Any]]:
    import requests
    url = f"{_GRAPH}/teams/{team_id}/channels/{channel_id}/messages/{thread_id}/replies"
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


def _get_channel_files_folder(bearer: str, team_id: str, channel_id: str) -> Dict[str, str]:
    """GET /teams/{id}/channels/{id}/filesFolder -- the driveItem for this channel's SharePoint
    folder. Confirmed against Microsoft's own Graph reference: least-privileged delegated scope
    is Files.Read.All (Files.ReadWrite/.All also works); the response's own `id` is the PARENT
    item id uploads below address, and `parentReference.driveId` is the drive uploads need."""
    import requests
    r = requests.get(
        f"{_GRAPH}/teams/{team_id}/channels/{channel_id}/filesFolder",
        headers={"Authorization": f"Bearer {bearer}"}, timeout=30,
    )
    r.raise_for_status()
    body = r.json()
    return {"drive_id": (body.get("parentReference") or {}).get("driveId"), "item_id": body.get("id")}


def _upload_file_to_channel_folder(bearer: str, drive_id: str, folder_item_id: str, filename: str, local_path: str, content_type: str) -> Dict[str, Any]:
    """PUT /drives/{drive-id}/items/{parent-id}:/{filename}:/content -- Graph's "upload small
    file" call (confirmed against Microsoft's own reference: up to 250 MB in one request, well
    above report_tools_server's own max_file_bytes default). Least-privileged delegated scope is
    Files.ReadWrite (Files.ReadWrite.All also works) -- a DIFFERENT scope than ChannelMessage.Send
    (posting replies/reactions), so this is new consent beyond what teams_reply otherwise needs."""
    import requests
    with open(local_path, "rb") as f:
        data = f.read()
    r = requests.put(
        f"{_GRAPH}/drives/{drive_id}/items/{folder_item_id}:/{filename}:/content",
        headers={"Authorization": f"Bearer {bearer}", "Content-Type": content_type or "application/octet-stream"},
        data=data, timeout=60,
    )
    r.raise_for_status()
    return r.json()


def _set_teams_reaction(bearer: str, team_id: str, channel_id: str, message_id: str, reaction_type: str) -> None:
    """Adds a reaction to a (root) channel message -- same shape as teams_agent_request_sensor's
    own `_set_teams_reaction`, confirmed against Microsoft's own `chatmessage-setreaction`
    reference: delegated ChannelMessage.Send is sufficient. teams_reply only ever reacts to the
    triggering ROOT message (teams/message_id), never a reply, so no parent_id parameter here."""
    import requests
    r = requests.post(
        f"{_GRAPH}/teams/{team_id}/channels/{channel_id}/messages/{message_id}/setReaction",
        headers={"Authorization": f"Bearer {bearer}", "Content-Type": "application/json"},
        json={"reactionType": reaction_type}, timeout=30,
    )
    r.raise_for_status()


def _unset_teams_reaction(bearer: str, team_id: str, channel_id: str, message_id: str, reaction_type: str) -> None:
    """Removes a reaction from a (root) channel message. Confirmed against Microsoft's own
    `chatmessage-unsetreaction` reference (v1.0, not beta-only): same endpoint shape and
    delegated ChannelMessage.Send scope as setReaction -- no separate permission for removing."""
    import requests
    r = requests.post(
        f"{_GRAPH}/teams/{team_id}/channels/{channel_id}/messages/{message_id}/unsetReaction",
        headers={"Authorization": f"Bearer {bearer}", "Content-Type": "application/json"},
        json={"reactionType": reaction_type}, timeout=30,
    )
    r.raise_for_status()


def _update_teams_reply(bearer: str, team_id: str, channel_id: str, thread_id: str, reply_id: str, html_body: str) -> None:
    """PATCH .../replies/{id} -- edits an existing reply's content in place (used for the
    placeholder-message update path). Confirmed against Microsoft's own `chatmessage-update`
    reference: delegated ChannelMessage.ReadWrite is required -- a DIFFERENT, broader scope than
    ChannelMessage.Send (posting new replies/reactions), so this is new consent beyond what
    teams_reply otherwise needs. Returns 204 No Content on success (delegated); raises on any
    other status via raise_for_status()."""
    import requests
    r = requests.patch(
        f"{_GRAPH}/teams/{team_id}/channels/{channel_id}/messages/{thread_id}/replies/{reply_id}",
        headers={"Authorization": f"Bearer {bearer}", "Content-Type": "application/json"},
        json={"body": {"contentType": "html", "content": html_body}},
        timeout=30,
    )
    r.raise_for_status()


def _resolve_user_email(bearer: str, aad_id: str) -> Optional[str]:
    """`from.user` on a chatMessage carries only an Azure AD object id, never an email -- same
    shape as teams_run_request_sensor's own `_resolve_user_email`. Any failure degrades to
    None rather than raising."""
    import requests
    try:
        r = requests.get(
            f"{_GRAPH}/users/{aad_id}", headers={"Authorization": f"Bearer {bearer}"},
            params={"$select": "mail,userPrincipalName"}, timeout=30,
        )
        r.raise_for_status()
        body = r.json()
        return body.get("mail") or body.get("userPrincipalName")
    except Exception:  # noqa: BLE001 -- see docstring
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


def _to_html(text: str) -> str:
    """Teams message bodies are HTML (contentType: 'html'), unlike Slack's plain mrkdwn text --
    this is the minimal escape this component needs: & < > escaped, newlines to <br>."""
    escaped = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return escaped.replace("\n", "<br>")


class TeamsReplyComponent(dg.Component, dg.Model, dg.Resolvable):
    """Posts a chat agent's answer into the Teams thread named by the run's own
    `teams/team_id`/`teams/channel_id`/`teams/thread_id` tags. Emits TWO things: `{asset_name}`
    (downstream of `upstream_asset_key`, the success path) and `{asset_name}_failure_watcher`
    (a run-status sensor that posts a short error instead, for ANY failed run carrying those
    same tags -- not just one where this asset itself ran)."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: Union[str, List[str]] = Field(description="The chat agent asset to depend on and read the answer from (slash-delimited). Exactly one -- a list of more than one is a validation error.")

    teams_tenant_id_env_var: str = Field(default="TEAMS_TENANT_ID", description="Env var holding the Azure AD tenant id.")
    teams_client_id_env_var: str = Field(default="TEAMS_CLIENT_ID", description="Env var holding the Azure AD app registration client id.")
    teams_client_secret_env_var: str = Field(default="TEAMS_CLIENT_SECRET", description="Env var holding the Azure AD app registration client secret.")
    teams_refresh_token_env_var: str = Field(default="TEAMS_REFRESH_TOKEN", description="Env var holding the INITIAL OAuth refresh token (see teams_run_request_sensor's README, 'Auth').")
    oauth_state_dir: str = Field(description="Local path or fsspec URI dedicated to this asset's '.teams_oauth_state.json' (a rotated refresh token, if Microsoft ever issues one). Can be the SAME directory teams_agent_request_sensor uses.")

    answer_field: Optional[str] = Field(
        default=None,
        description="Which key of the upstream asset's output dict holds the answer text. Defaults to 'final_answer' (the agent components' own documented output shape) when the upstream value is a dict; used verbatim (str()'d) otherwise.",
    )
    max_chars_per_message: int = Field(default=3000, description="Split or truncate (see on_long_answer) an answer longer than this many characters.")
    on_long_answer: Literal["split", "truncate"] = Field(default="split", description="'split' (default): post as multiple sequential thread replies, each <= max_chars_per_message. 'truncate': post one reply, cut short with an ellipsis.")
    include_run_link: bool = Field(default=True, description="Append a link to the Dagster run (requires webserver_base_url) to the last reply posted.")
    run_link_label: str = Field(default="View run", description="Link text for include_run_link.")
    webserver_base_url: Optional[str] = Field(default=None, description="Base URL of the Dagster webserver (e.g. 'http://localhost:3000' or your Dagster+ deployment URL) -- used to build the run link. Leave unset to omit the link entirely.")

    monitor_failures: bool = Field(default=True, description="Also emit {asset_name}_failure_watcher, a run-status sensor that posts a short error reply for ANY failed run carrying teams/team_id + teams/channel_id + teams/thread_id tags -- so a crashed run is never silent in the channel. Set false to handle failure notification some other way.")
    error_message_template: str = Field(default="⚠️ Something went wrong answering this: {error}", description="Template for the failure-path reply. Placeholder: {error}.")

    update_reaction_on_completion: bool = Field(
        default=True,
        description=(
            "On success, remove ack_reaction and add done_reaction to the ORIGINAL triggering "
            "message (the teams/message_id tag teams_agent_request_sensor sets) -- so a 20-30s "
            "answer doesn't just look stuck at its acknowledgement reaction forever. On a "
            "failed run, {asset_name}_failure_watcher adds error_reaction instead (ack_reaction "
            "is still removed first). Same delegated ChannelMessage.Send scope watch_reactions "
            "already needs -- confirmed no separate scope for removing a reaction either, same "
            "as adding one. Best-effort: a reaction-swap failure never fails the reply itself."
        ),
    )
    ack_reaction: str = Field(default="👀", description="The reaction teams_agent_request_sensor posted on pickup (must match its own acknowledgement_reaction) -- removed on completion. Only used when update_reaction_on_completion is true.")
    done_reaction: str = Field(default="✅", description="Reaction added to the original message once the reply posts successfully. Only used when update_reaction_on_completion is true.")
    error_reaction: str = Field(default="⚠️", description="Reaction added to the original message when the run fails (added by {asset_name}_failure_watcher). Only used when update_reaction_on_completion is true.")

    receipts_db_path: Optional[str] = Field(
        default=None,
        description=(
            "SQLite file path -- the SAME receipts_db_path a knowledge_search_server/"
            "hybrid_knowledge_search_server the agent calls is configured with (and which must "
            "also have run_id_template set, the default, so its receipts carry this run's "
            "dagster_run_id). When set, after posting, this asset looks up every receipt this "
            "run produced and links them to the posted Teams reply/replies -- so a later "
            "rating (watch_reactions below) can find the exact receipt_id to record an outcome "
            "against. Best-effort: a lookup failure here never fails the reply post itself."
        ),
    )
    report_files_dir: Optional[str] = Field(
        default=None,
        description=(
            "Local directory root -- the SAME files_dir a report_tools_server this agent calls "
            "is configured with. When set, after posting the answer, this asset reads this "
            "run's manifest (written by report_tools_server's make_chart/make_pdf/make_docx/"
            "make_xlsx), uploads each file to the channel's own Files folder (Graph has no "
            "inline file attachment on a channel message, unlike Slack), and posts a reply "
            "linking to it. Needs delegated Files.ReadWrite (or Files.ReadWrite.All) -- a "
            "DIFFERENT scope than ChannelMessage.Send, which only covers posting/reacting. "
            "Files may contain data the agent read, so the same people who can already see the "
            "channel's Files tab can see them. Best-effort: an upload failure here never fails "
            "the already-posted answer."
        ),
    )
    watch_reactions: bool = Field(
        default=False,
        description=(
            "Also emit {asset_name}_rating_watcher, a sensor that polls reactions on every "
            "Teams reply this asset has linked to a receipt (via receipts_db_path, which must "
            "also be set) and records a thumbs_up/thumbs_down outcome against each linked "
            "receipt_id -- the reactor's resolved email (see resolve_rater_email) becomes the "
            "outcome's `outcome_ref`, so changing a reaction later supersedes the earlier "
            "rating instead of double-counting. Needs delegated ChannelMessage.Send (the same "
            "scope already provisioned for posting) -- confirmed against Microsoft's own Graph "
            "reference, reading a message's own `reactions[]` needs no separate scope at all."
        ),
    )
    positive_reaction: str = Field(default="👍", description="Reaction (any Unicode emoji character, or a legacy name like 'like') that records a thumbs_up outcome. Only used when watch_reactions is true.")
    negative_reaction: str = Field(default="👎", description="Reaction that records a thumbs_down outcome. Only used when watch_reactions is true.")
    rating_poll_interval_seconds: int = Field(default=30, description="Minimum seconds between rating_watcher ticks. Only used when watch_reactions is true.")
    rating_lookback_messages: int = Field(default=200, description="Most-recently-linked replies to keep polling for reactions, and the cursor's bound. Only used when watch_reactions is true.")
    resolve_rater_email: bool = Field(default=True, description="Resolve the reacting user's Azure AD object id to an email address (needs delegated User.Read.All) before using it as the outcome's outcome_ref. When false, the raw Azure AD object id is used instead. Only used when watch_reactions is true.")

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
        return ComponentFormConfig(label="Teams Reply", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_key = dg.AssetKey(self.asset_name.split("/"))
        upstream_key = dg.AssetKey(self.upstream_asset_key.split("/"))
        tenant_env, client_env, secret_env, refresh_env = (
            self.teams_tenant_id_env_var, self.teams_client_id_env_var,
            self.teams_client_secret_env_var, self.teams_refresh_token_env_var,
        )
        oauth_state_dir = self.oauth_state_dir
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
            team_id, channel_id, thread_id = tags.get("teams/team_id"), tags.get("teams/channel_id"), tags.get("teams/thread_id")
            message_id = tags.get("teams/message_id")
            placeholder_id = tags.get("teams/placeholder_id")
            if not team_id or not channel_id or not thread_id:
                raise dg.Failure(
                    "teams_reply requires 'teams/team_id', 'teams/channel_id' and "
                    "'teams/thread_id' run tags (normally set by teams_agent_request_sensor) "
                    "-- this asset should only ever be materialized as part of that chain, "
                    "not run standalone."
                )
            text = _redact(_extract_answer(agent_output, answer_field))
            parts = _split_or_truncate(text, max_chars, on_long_answer)
            link = _run_url(webserver_base_url, context.run.run_id) if include_run_link else None
            fs = _StateFS(oauth_state_dir)
            bearer = _get_teams_bearer(fs, tenant_env, client_env, secret_env, refresh_env)
            posted_ids: List[str] = []
            for i, part in enumerate(parts):
                html_part = _to_html(part)
                if i == len(parts) - 1 and link:
                    # Appended AFTER escaping the plain-text part -- _to_html would otherwise
                    # escape this markup's own angle brackets right along with the text.
                    html_part = f'{html_part}<br><br><a href="{link}">{run_link_label}</a>'
                if i == 0 and placeholder_id:
                    # Edits the sensor's own "Looking into it…" reply in place rather than
                    # posting a new one -- falls back to a fresh post if the edit fails (e.g.
                    # the placeholder was deleted).
                    try:
                        _update_teams_reply(bearer, team_id, channel_id, thread_id, placeholder_id, html_part)
                        posted_ids.append(placeholder_id)
                        continue
                    except Exception as e:  # noqa: BLE001 -- fall back to a new post below
                        context.log.warning(f"teams_reply: could not edit the placeholder reply, posting a new one instead: {e}")
                resp = _post_teams_reply(bearer, team_id, channel_id, thread_id, html_part)
                reply_id = resp.get("id")
                if reply_id:
                    posted_ids.append(reply_id)
            if update_reaction_on_completion and message_id:
                try:
                    _unset_teams_reaction(bearer, team_id, channel_id, message_id, ack_reaction)
                except Exception:  # noqa: BLE001 -- may already be gone; never fatal
                    pass
                try:
                    _set_teams_reaction(bearer, team_id, channel_id, message_id, done_reaction)
                except Exception as e:  # noqa: BLE001 -- a cosmetic reaction must never fail an already-posted reply
                    context.log.warning(f"teams_reply: could not add {done_reaction!r} to the original message: {e}")
            if receipts_db_path:
                try:
                    receipt_ids = receipt_ids_for_run(receipts_db_path, context.run.run_id)
                    for reply_id in posted_ids:
                        record_message_links(
                            receipts_db_path, receipt_ids, platform="teams",
                            channel=f"{team_id}|{channel_id}", message_ts=f"{thread_id}|{reply_id}",
                        )
                except Exception as e:  # noqa: BLE001 -- linking receipts must never fail an already-posted reply
                    context.log.warning(f"teams_reply: could not link receipts to the posted message(s): {e}")
            files_posted: List[str] = []
            if report_files_dir:
                try:
                    manifest = read_run_manifest(report_files_dir, context.run.run_id)
                except Exception as e:  # noqa: BLE001 -- a manifest read failure must never fail an already-posted reply
                    manifest = []
                    context.log.warning(f"teams_reply: could not read report_tools_server's manifest: {e}")
                if manifest:
                    try:
                        folder = _get_channel_files_folder(bearer, team_id, channel_id)
                    except Exception as e:  # noqa: BLE001 -- see above
                        folder = None
                        context.log.warning(f"teams_reply: could not resolve the channel's Files folder: {e}")
                    for entry in manifest:
                        if not folder or not folder.get("drive_id") or not folder.get("item_id"):
                            break
                        try:
                            item = _upload_file_to_channel_folder(
                                bearer, folder["drive_id"], folder["item_id"], entry["name"], entry["path"],
                                entry.get("media_type", "application/octet-stream"),
                            )
                            web_url = item.get("webUrl")
                            link_html = f'<a href="{web_url}">{_to_html(entry["name"])}</a>' if web_url else _to_html(entry["name"])
                            _post_teams_reply(bearer, team_id, channel_id, thread_id, f"📎 {link_html}")
                            files_posted.append(entry["name"])
                        except Exception as e:  # noqa: BLE001 -- one bad file upload must never block the others or the reply
                            context.log.warning(f"teams_reply: could not post generated file {entry.get('name')!r}: {e}")
            return dg.Output(
                {"posted_parts": len(parts), "team_id": team_id, "channel_id": channel_id, "thread_id": thread_id, "files_posted": files_posted},
                metadata={
                    "posted_parts": dg.MetadataValue.int(len(parts)), "channel_id": dg.MetadataValue.text(channel_id),
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
                team_id, channel_id, thread_id = run_tags.get("teams/team_id"), run_tags.get("teams/channel_id"), run_tags.get("teams/thread_id")
                message_id = run_tags.get("teams/message_id")
                placeholder_id = run_tags.get("teams/placeholder_id")
                if not team_id or not channel_id or not thread_id:
                    return  # not a chat-agent run -- nothing to do, same tag-based identity check as everywhere else here
                error = _redact(getattr(sensor_context.dagster_event, "message", None) or "the run failed with no further detail.")
                link = _run_url(webserver_base_url, sensor_context.dagster_run.run_id) if include_run_link else None
                text = error_message_template.format(error=error)
                html_text = _to_html(text)
                if link:
                    # Appended AFTER escaping -- see the success-path asset's own comment.
                    html_text = f'{html_text}<br><a href="{link}">{run_link_label}</a>'
                try:
                    fs = _StateFS(oauth_state_dir)
                    bearer = _get_teams_bearer(fs, tenant_env, client_env, secret_env, refresh_env)
                    posted = False
                    if placeholder_id:
                        try:
                            _update_teams_reply(bearer, team_id, channel_id, thread_id, placeholder_id, html_text)
                            posted = True
                        except Exception as e:  # noqa: BLE001 -- fall back to a new post below
                            sensor_context.log.warning(f"teams_reply failure_watcher: could not edit the placeholder reply, posting a new one instead: {e}")
                    if not posted:
                        _post_teams_reply(bearer, team_id, channel_id, thread_id, html_text)
                    if update_reaction_on_completion and message_id:
                        try:
                            _unset_teams_reaction(bearer, team_id, channel_id, message_id, ack_reaction)
                        except Exception:  # noqa: BLE001 -- may already be gone; never fatal
                            pass
                        _set_teams_reaction(bearer, team_id, channel_id, message_id, error_reaction)
                except Exception as e:  # noqa: BLE001 -- a failed failure-notification must not crash the sensor loop
                    sensor_context.log.warning(f"teams_reply failure_watcher: could not post error reply: {e}")

            defs = dg.Definitions.merge(defs, dg.Definitions(sensors=[_failure_watcher]))

        if self.watch_reactions:
            if not receipts_db_path:
                raise ValueError(f"TeamsReplyComponent {self.asset_name!r}: watch_reactions requires receipts_db_path.")
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
                fs = _StateFS(oauth_state_dir)
                try:
                    bearer = _get_teams_bearer(fs, tenant_env, client_env, secret_env, refresh_env)
                except Exception as e:  # noqa: BLE001
                    return dg.SkipReason(f"Teams auth failed: {e}")
                try:
                    links = recent_message_links(receipts_db_path, platform="teams", limit=lookback)
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
                # Group by (team_id, channel_id, thread_id) so threads with several linked
                # replies only cost one replies-list call, not one per reply.
                by_thread: Dict[str, List[str]] = {}
                for link in links:
                    team_id, channel_id = link["channel"].split("|", 1)
                    thread_id, reply_id = link["message_ts"].split("|", 1)
                    by_thread.setdefault(f"{team_id}|{channel_id}|{thread_id}", []).append(reply_id)
                for thread_key, reply_ids in by_thread.items():
                    team_id, channel_id, thread_id = thread_key.split("|", 2)
                    try:
                        replies = _get_teams_replies(bearer, team_id, channel_id, thread_id, lookback, max_pages=3)
                    except Exception as e:  # noqa: BLE001 -- one bad thread must never block the others
                        sensor_context.log.warning(f"rating_watcher: replies fetch failed for thread {thread_id}: {e}")
                        continue
                    replies_by_id = {r.get("id"): r for r in replies}
                    for reply_id in reply_ids:
                        key = f"{thread_key}|{reply_id}"
                        last_seen_for_message = dict(seen.get(key) or {})
                        reply = replies_by_id.get(reply_id)
                        if reply is None:
                            continue
                        for r in reply.get("reactions") or []:
                            signal = signal_by_reaction.get(r.get("reactionType"))
                            if signal is None:
                                continue
                            uid = ((r.get("user") or {}).get("user") or {}).get("id")
                            if not uid:
                                continue
                            if last_seen_for_message.get(uid) == signal:
                                continue  # already recorded and unchanged -- each reaction counted once
                            last_seen_for_message[uid] = signal
                            changed += 1
                            if uid not in email_cache:
                                email_cache[uid] = _resolve_user_email(bearer, uid) if resolve_rater_email else None
                            rater = email_cache[uid] or uid
                            try:
                                for receipt_id in receipt_ids_for_message(
                                    receipts_db_path, platform="teams",
                                    channel=f"{team_id}|{channel_id}", message_ts=f"{thread_id}|{reply_id}",
                                ):
                                    record_outcome(receipts_db_path, receipt_id, signal=signal, outcome_ref=rater)
                            except Exception as e:  # noqa: BLE001 -- one bad outcome write must never block the others
                                sensor_context.log.warning(f"rating_watcher: could not record outcome for {key}: {e}")
                        seen[key] = last_seen_for_message
                bounded = dict(list(seen.items())[-lookback:])
                return dg.SensorResult(
                    cursor=json.dumps(bounded),
                    skip_reason=None if changed else "no new or changed reactions this tick.",
                )

            defs = dg.Definitions.merge(defs, dg.Definitions(sensors=[_rating_watcher]))

        return defs
