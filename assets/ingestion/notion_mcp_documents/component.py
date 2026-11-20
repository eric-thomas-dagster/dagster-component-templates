"""NotionMcpDocumentsComponent -- extract Notion page body text via Notion's HOSTED MCP server.

`notion_documents` (a separate, pre-existing component) reads Notion through a self-created
integration token -- creating one requires Notion workspace-admin access, which many users don't
have. This component reads the SAME kind of data (full page body text for RAG pipelines) through
Notion's hosted MCP server (`https://mcp.notion.com/mcp`) instead: OAuth as the SIGNED-IN PERSON
(no admin token needed), with the tradeoff that it sees exactly what that person can see -- not a
scoped service-account view. Prefer `notion_documents` with a real integration token once an admin
can create one; use this component when that isn't available yet.

Verified against a real Notion hosted-MCP OAuth session + live crawl (treated as given facts, not
re-verified here -- see this session's own research notes):
  - OAuth: Authorization Code + PKCE + dynamic client registration, token endpoint
    `https://mcp.notion.com/token`. Access tokens live ~8h; the refresh token is REPLACED on every
    refresh (a stale copy elsewhere dies after first use) -- see the `_nmo_get_access_token` /
    `_nmo_write_tokens` helpers below, which take a file lock around every read-refresh-write cycle
    so concurrent workers never race on the same tokens.json. Sign in once via the companion CLI:
    `python -m dagster_community_components.resources.notion_mcp_login --oauth-dir <dir>`.
  - `notion-fetch`'s tool result is a JSON ENVELOPE, not plain text: `{"metadata": {"type":
    "page"|"database"|...}, "title": "...", "url": "...", "text": "..."}`. `_parse_fetch_envelope`
    decodes it; `title`/`url` come straight from the envelope's own top-level fields (real values,
    confirmed against a live crawl -- an earlier version of this component guessed these weren't
    present and fell back to regex/constructed values; that fallback is KEPT for a tool result that
    isn't valid JSON, but the envelope's own fields are now used whenever present). The envelope's
    `text` field is markdown-ish XML: page text in `<content>` (or `<wiki-home-content>` for a
    database/wiki page), child pages as `<page url="...">title</page>` tags (NOT followed
    automatically -- this component follows them itself, see `_crawl_notion_pages`), links as
    `<mention url="...">` (never followed), and some top-level wiki pages have an empty
    `<ancestor-path>`. Page ids embedded in a Notion URL have no dashes -- `_normalize_notion_page_id`
    strips any dashes and takes the last 32 hex characters before comparing ids anywhere in this
    component (crawl scope checks, dedup, doc_id).
  - `redact_secrets` (on by default): Slack/GitHub/AWS/JWT-shaped tokens and PEM private keys are
    replaced with `[redacted]` in `content` before it's hashed or stored -- a real runbook page in
    a live crawl contained a pasted GitHub token. `redact_secrets_extra_patterns` adds more regexes.
    `secrets_redacted` (a count) and `pages_excluded_titles` (the titles skipped by
    `exclude_title_contains`, for visibility into what got dropped) are reported in materialization
    metadata.
  - Notion's hosted MCP server does NOT return HTTP 429 for rate limiting -- a rate-limited call
    comes back as a normal/successful tool result whose TEXT reads "The Notion API rate limit was
    reached. Wait at least N seconds before retrying this tool." A naive reader stores that text as
    if it were real page content (a real incident: one user's own crawl indexed 211 pages with
    exactly this string as their "content" before anyone noticed). `_NOTION_RATE_LIMIT_RE` detects
    this pattern specifically; a matching result is NEVER returned as content -- `_nmo_call_tool`
    sleeps the requested time and retries, up to `max_retries`, raising a clear terminal failure if
    it never clears. Every worker shares ONE `NotionMcpRateLimiter` instance (default 2 req/s,
    `concurrency` default 2) -- a per-worker sleep (already tried and insufficient against a real
    workspace) still exceeds the aggregate limit when several workers each pace independently.

NOT independently verified this session (called out honestly rather than guessed as fact):
  - `notion-search`'s exact semantics for enumerating every page in a workspace (vs. a scoped text
    query) were not confirmed against a live server. Incremental sync here is therefore
    CONTENT-HASH-based (`state_path`), not `notion-search` timestamp-based -- the spec's own
    fallback option ("...or a stored per-page hash"). Every crawl still re-fetches every in-scope
    page every run (fetching is also how child pages are discovered, so there's no cheaper partial
    crawl); `state_path` only keeps `updated_at` accurate (unchanged content keeps its prior
    timestamp) and reports how many previously-seen pages dropped out of scope this run --
    pruning those from a vector store / knowledge graph is `prune_missing_sources`'s job on
    `vector_store_writer` / `knowledge_graph_builder`, downstream of this asset, not this component's.
  - A child page's title, as the inner text of its own `<page url="...">` tag inside a PARENT
    page's `text`, is confirmed. The page BEING fetched's own title/url now come from the
    envelope's top-level fields (see above) -- confirmed against a live crawl, superseding this
    component's earlier (wrong) assumption that they weren't present.

Document contract (same as every document-source component in this repo -- see
`notion_documents`'s own `_row_from_notion_page` / `firecrawl_ingestion`'s `_row_from_page`):
`doc_id`, `content`, `title`, `url`, `source_type` (`"notion"`), `updated_at`, `content_hash`.
"""
import concurrent.futures
import hashlib
import json
import os
import re
import threading
import time
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import dlt
import pandas as pd
import dagster as dg
from pydantic import Field, model_validator


# BEGIN notion mcp oauth (inlined per component; keep copies identical — see tests/test_notion_mcp_oauth_drift.py)
import fcntl as _nmo_fcntl  # noqa: E402,F401
import json as _nmo_json  # noqa: E402,F401
import os as _nmo_os  # noqa: E402,F401
import re as _nmo_re  # noqa: E402,F401
import threading as _nmo_threading  # noqa: E402,F401
import time as _nmo_time  # noqa: E402,F401

NOTION_MCP_URL = "https://mcp.notion.com/mcp"
NOTION_MCP_TOKEN_URL = "https://mcp.notion.com/token"
_NOTION_RATE_LIMIT_RE = _nmo_re.compile(
    r"rate limit was reached\.?\s*Wait at least (\d+(?:\.\d+)?) seconds", _nmo_re.IGNORECASE
)


class NotionMcpAuthError(Exception):
    """Sign-in is missing or the refresh token was rejected -- surfaced distinctly from a generic
    network/API error so a caller can give an actionable 'sign in again' message."""


class NotionMcpRateLimited(Exception):
    """Notion's hosted MCP rate-limit text signal (never HTTP 429) persisted past max_retries."""

    def __init__(self, wait_seconds: float):
        self.wait_seconds = wait_seconds
        super().__init__(f"Notion MCP rate limit exceeded max_retries (last asked to wait {wait_seconds}s)")


def _nmo_tokens_path(oauth_dir: str) -> str:
    return _nmo_os.path.join(_nmo_os.path.expanduser(oauth_dir), "tokens.json")


def _nmo_client_info_path(oauth_dir: str) -> str:
    return _nmo_os.path.join(_nmo_os.path.expanduser(oauth_dir), "client_info.json")


def _nmo_read_json(path: str):
    if not _nmo_os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return _nmo_json.load(f)


def _nmo_atomic_write(path: str, data) -> None:
    """Atomic, 0600-permissioned write. No locking here -- callers that need a read-modify-write
    critical section (see _nmo_get_access_token) take the lock themselves and call this directly,
    to avoid a self-deadlock from acquiring the same flock twice in one process."""
    _nmo_os.makedirs(_nmo_os.path.dirname(path) or ".", exist_ok=True)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        _nmo_json.dump(data, f)
    _nmo_os.chmod(tmp_path, 0o600)
    _nmo_os.replace(tmp_path, path)


def _nmo_write_tokens(oauth_dir: str, data) -> None:
    """Locked single-shot write -- used by the sign-in CLI after the initial PKCE exchange."""
    lock_path = _nmo_tokens_path(oauth_dir) + ".lock"
    lock_fd = _nmo_os.open(lock_path, _nmo_os.O_CREAT | _nmo_os.O_RDWR, 0o600)
    try:
        _nmo_fcntl.flock(lock_fd, _nmo_fcntl.LOCK_EX)
        _nmo_atomic_write(_nmo_tokens_path(oauth_dir), data)
    finally:
        _nmo_fcntl.flock(lock_fd, _nmo_fcntl.LOCK_UN)
        _nmo_os.close(lock_fd)


def _nmo_get_access_token(oauth_dir: str) -> str:
    """Returns a live access token, refreshing under a file lock (re-checking after acquiring it,
    in case a concurrent process already refreshed while this one waited) when the cached one is
    expired or about to expire. Persists the NEW refresh token Notion issues on every refresh."""
    import requests

    tokens = _nmo_read_json(_nmo_tokens_path(oauth_dir))
    if not tokens:
        raise NotionMcpAuthError(
            f"No tokens found under oauth_dir={oauth_dir!r}. Sign in first: python -m "
            "dagster_community_components.resources.notion_mcp_login --oauth-dir " + repr(oauth_dir)
        )
    if tokens.get("expires_at", 0) > _nmo_time.time() + 60:
        return tokens["access_token"]

    client_info = _nmo_read_json(_nmo_client_info_path(oauth_dir))
    if not client_info:
        raise NotionMcpAuthError(f"No client_info found under oauth_dir={oauth_dir!r}. Sign in again.")

    lock_path = _nmo_tokens_path(oauth_dir) + ".lock"
    lock_fd = _nmo_os.open(lock_path, _nmo_os.O_CREAT | _nmo_os.O_RDWR, 0o600)
    try:
        _nmo_fcntl.flock(lock_fd, _nmo_fcntl.LOCK_EX)
        tokens = _nmo_read_json(_nmo_tokens_path(oauth_dir)) or tokens
        if tokens.get("expires_at", 0) > _nmo_time.time() + 60:
            return tokens["access_token"]
        resp = requests.post(
            NOTION_MCP_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": tokens["refresh_token"],
                "client_id": client_info["client_id"],
                "client_secret": client_info.get("client_secret") or "",
            },
            headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        if resp.status_code in (400, 401):
            raise NotionMcpAuthError(
                f"Notion MCP refresh token was rejected (HTTP {resp.status_code}). Sign in again: "
                f"python -m dagster_community_components.resources.notion_mcp_login --oauth-dir {oauth_dir!r}"
            )
        resp.raise_for_status()
        data = resp.json()
        new_tokens = {
            "access_token": data["access_token"],
            "refresh_token": data.get("refresh_token") or tokens["refresh_token"],
            "expires_at": _nmo_time.time() + data.get("expires_in", 8 * 3600),
        }
        _nmo_atomic_write(_nmo_tokens_path(oauth_dir), new_tokens)
        return new_tokens["access_token"]
    finally:
        _nmo_fcntl.flock(lock_fd, _nmo_fcntl.LOCK_UN)
        _nmo_os.close(lock_fd)


class NotionMcpRateLimiter:
    """ONE instance shared by every worker -- a per-worker sleep still exceeds Notion's real
    aggregate limit when several workers each pace independently (a real, previously-hit problem);
    only a single shared gate enforces the combined rate across all of them."""

    def __init__(self, max_requests_per_second: float):
        self._interval = 1.0 / max(float(max_requests_per_second), 0.01)
        self._lock = _nmo_threading.Lock()
        self._next_allowed = 0.0

    def acquire(self) -> None:
        with self._lock:
            now = _nmo_time.time()
            wait = self._next_allowed - now
            if wait > 0:
                _nmo_time.sleep(wait)
                now = _nmo_time.time()
            self._next_allowed = now + self._interval


async def _nmo_open_mcp_session(stack, access_token: str):
    """Streamable-HTTP MCP client against NOTION_MCP_URL with a bearer token. Works on mcp 1.x and
    2.x -- mcp 2.x moved header support onto an httpx-like client object instead of a `headers=`
    kwarg (same shim this repo's mcp_server_resource/agentic_pipeline components already use for
    their own streamable-HTTP connections, duplicated here per this repo's self-contained-component
    convention rather than cross-importing).

    `max_sse_event_size=None` disables the mcp SDK's own default 1 MiB-per-SSE-event cap
    (confirmed: `mcp.client.streamable_http.DEFAULT_MAX_SSE_EVENT_SIZE`), which otherwise raises
    an SSEError -- surfacing as a silently-dropped page, not a visible error -- the moment a
    single notion-fetch result (a large page's JSON envelope) exceeds it. Wrapped in try/except
    TypeError since an older mcp minor version may not accept the kwarg yet, in which case the
    SDK's own default cap still applies (better than crashing)."""
    headers = {"Authorization": f"Bearer {access_token}"}
    from mcp import ClientSession
    try:
        from mcp.client.streamable_http import streamable_http_client
        from mcp.shared._httpx_utils import create_mcp_http_client
        http_client = await stack.enter_async_context(create_mcp_http_client(headers=headers))
        try:
            streams = await stack.enter_async_context(
                streamable_http_client(NOTION_MCP_URL, http_client=http_client, max_sse_event_size=None)
            )
        except TypeError:
            streams = await stack.enter_async_context(streamable_http_client(NOTION_MCP_URL, http_client=http_client))
    except ImportError:
        from mcp.client.streamable_http import streamablehttp_client
        try:
            streams = await stack.enter_async_context(
                streamablehttp_client(NOTION_MCP_URL, headers=headers, max_sse_event_size=None)
            )
        except TypeError:
            streams = await stack.enter_async_context(streamablehttp_client(NOTION_MCP_URL, headers=headers))
    read, write = streams[0], streams[1]
    session = await stack.enter_async_context(ClientSession(read, write))
    await session.initialize()
    return session


async def _nmo_call_tool_async(oauth_dir: str, tool_name: str, arguments: Optional[Dict[str, Any]], timeout_seconds: float = 60.0) -> str:
    import anyio
    from contextlib import AsyncExitStack

    access_token = _nmo_get_access_token(oauth_dir)
    async with AsyncExitStack() as stack:
        session = await _nmo_open_mcp_session(stack, access_token)
        with anyio.fail_after(timeout_seconds):
            result = await session.call_tool(tool_name, arguments or {})
        parts = [getattr(item, "text", None) for item in (result.content or [])]
        return "\n".join(p for p in parts if p)


def _nmo_call_tool(
    oauth_dir: str, tool_name: str, arguments: Optional[Dict[str, Any]], rate_limiter: "NotionMcpRateLimiter",
    max_retries: int = 5, on_rate_limit=None, timeout_seconds: float = 60.0,
) -> str:
    """Sync entry point used by every caller: enforces `rate_limiter`, retries on Notion's own
    text-based rate-limit signal (never HTTP 429 -- see module docstring) and on transient
    errors (connection resets, 5xx), and NEVER returns the rate-limit text as if it were real
    content. `on_rate_limit(wait_seconds)`, if given, is invoked once per hit (hit-count/
    total-wait-time bookkeeping)."""
    import asyncio

    attempt = 0
    while True:
        rate_limiter.acquire()
        try:
            text = asyncio.run(_nmo_call_tool_async(oauth_dir, tool_name, arguments, timeout_seconds))
        except NotionMcpAuthError:
            raise
        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                raise RuntimeError(f"Notion MCP tool {tool_name!r} failed after {max_retries} retries: {e}") from e
            _nmo_time.sleep(min(2 ** attempt, 30))
            continue
        match = _NOTION_RATE_LIMIT_RE.search(text or "")
        if not match:
            return text
        wait_seconds = float(match.group(1))
        if on_rate_limit:
            on_rate_limit(wait_seconds)
        attempt += 1
        if attempt > max_retries:
            raise NotionMcpRateLimited(wait_seconds)
        _nmo_time.sleep(wait_seconds)
# END notion mcp oauth


# BEGIN preview-safe metadata (inlined per component; keep copies identical — see tests/test_preview_safe_metadata_drift.py)
def _safe_preview_markdown(df, preview_rows: int, max_cell_chars: int = 200) -> str:
    """Row-sample/head to preview_rows, then truncate every cell's text representation to
    max_cell_chars before rendering to markdown. Without this, a text/content column embeds a
    document's FULL text in materialization metadata -- confirmed real: a single large page's
    full body alone pushed a Dagster+ materialization event past its 5 MB per-event limit,
    failing the run. Truncation happens on the already-sampled rows only (cheap)."""
    sample = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)

    def _truncate(v):
        s = "" if v is None else str(v)
        return s if len(s) <= max_cell_chars else s[:max_cell_chars] + "…"

    sample = sample.map(_truncate) if hasattr(sample, "map") else sample.applymap(_truncate)
    return sample.to_markdown(index=False)
# END preview-safe metadata


def _normalize_notion_page_id(s: str) -> str:
    """Page ids embedded in a Notion URL have no dashes; a page's own canonical UUID commonly
    does. Strip anything non-hex and take the last 32 characters so both forms compare equal."""
    hexonly = re.sub(r"[^0-9a-fA-F]", "", s or "")
    return hexonly[-32:].lower()


_FETCH_PREAMBLE_RE = re.compile(r'^\s*Here is the result of "fetch" for the \w+[^\n]*\n?', re.IGNORECASE)


def _parse_fetch_envelope(raw_text: str) -> Dict[str, Any]:
    """notion-fetch's real tool result is a JSON envelope -- `{"metadata": {"type": ...},
    "title": ..., "url": ..., "text": ...}` -- NOT plain markdown. An earlier version of this
    component ran its XML-tag regexes directly against the raw envelope string, which never
    matched (the tags are JSON-escaped inside `text`): no children were ever discovered past the
    roots, title/url were always empty/guessed, and `content` carried literal `\\"`/`\\n` escape
    sequences plus the envelope's own "Here is the result of 'fetch' for the Page/Database ..."
    wrapper line. `json.loads` unescapes `text` back to literal `<page url="...">`/`<content>`
    tags, so the existing regex-based extractors below work unmodified once pointed at the
    DECODED `text` -- `_FETCH_PREAMBLE_RE` additionally strips that wrapper line (confirmed
    present against a live crawl), since it precedes the real content/tags, not inside any tag,
    and would otherwise leak into a page's content. Falls back to treating the whole raw string
    as `text` (title/url left empty) if it isn't valid JSON, so an unexpected tool-result shape
    degrades gracefully instead of crashing the crawl."""
    try:
        envelope = json.loads(raw_text)
    except (TypeError, ValueError):
        envelope = None
    if not isinstance(envelope, dict):
        return {"title": "", "url": None, "text": _FETCH_PREAMBLE_RE.sub("", raw_text or "", count=1), "type": None}
    return {
        "title": envelope.get("title") or "",
        "url": envelope.get("url"),
        "text": _FETCH_PREAMBLE_RE.sub("", envelope.get("text") or "", count=1),
        "type": (envelope.get("metadata") or {}).get("type"),
    }


def _extract_tag(text: str, tag: str) -> Optional[str]:
    m = re.search(rf"<{tag}>(.*?)</{tag}>", text or "", re.DOTALL | re.IGNORECASE)
    return m.group(1).strip() if m else None


def _extract_child_pages(text: str) -> List[Tuple[str, str]]:
    """[(url, title), ...] for every `<page url="...">title</page>` tag -- NOT followed
    automatically by notion-fetch itself; this component's own crawl decides whether to."""
    return re.findall(r'<page url="([^"]+)">(.*?)</page>', text or "", re.DOTALL)


def _extract_title(text: str, content: str) -> str:
    """Fallback only -- the envelope's own top-level `title` (see _parse_fetch_envelope) is used
    whenever present. Kept for a tool result that isn't a JSON envelope."""
    title = _extract_tag(text, "title")
    if title:
        return title
    m = re.search(r"^#{1,6}\s+(.+)$", content or "", re.MULTILINE)
    return m.group(1).strip() if m else ""


# Secrets that have no business ending up in a vector index / knowledge graph -- a real runbook
# page in a live crawl contained a pasted GitHub token. Checked before content is hashed/stored.
_SECRET_PATTERNS = [
    re.compile(r"xox[baprs]-[0-9A-Za-z-]{10,}"),  # Slack bot/user/app/refresh/single-channel tokens
    re.compile(r"gh[pousr]_[A-Za-z0-9]{36,}"),  # GitHub personal/oauth/user-to-server/server-to-server/refresh tokens
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),  # AWS access key id
    re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b"),  # JWT (header.payload.signature)
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----"),  # PEM private keys
]


def _redact_secrets(text: str, extra_patterns: Optional[List[str]] = None) -> Tuple[str, int]:
    """Replaces every match of every built-in pattern (plus `extra_patterns`, additional regex
    strings) with `[redacted]`. Returns (redacted_text, total_replacement_count)."""
    count = 0
    for pattern in list(_SECRET_PATTERNS) + [re.compile(p) for p in (extra_patterns or [])]:
        text, n = pattern.subn("[redacted]", text)
        count += n
    return text, count


def _clean_notion_markup(text: str) -> str:
    """Strips/converts Notion's own markup tags out of extracted `content` -- real examples from
    a live crawl's chunks: `<br />`, `<page url="...">Title</page>` (a child-page reference
    inline in prose, separate from the top-level ones _extract_child_pages follows for the
    crawl), `<mention url="...">Title</mention>`, `<span color="...">...</span>`,
    `<b>`/`<i>`/`<code>`, `<columns>`/`<column>`, `<table>`/`<tr>`/`<td>`, `<unknown url="...#..."
    alt="..." />`, `<table-of-contents />`. Called AFTER `_extract_child_pages`/the crawl's own
    traversal already consumed the unmodified envelope text -- converting `<page>`/`<mention>`
    tags to markdown links here would otherwise destroy the very tags the crawl depends on."""
    if not text:
        return text

    # void/self-closing elements with nothing worth keeping
    text = re.sub(r"<table-of-contents\s*/?>", "", text)
    text = re.sub(r"<unknown\b[^>]*/?>", "", text)
    text = re.sub(r"<br\s*/?>", "\n", text)

    # link-shaped tags -> markdown links (keeps a path back to the original page/mention)
    text = re.sub(r'<page url="([^"]*)">(.*?)</page>', r"[\2](\1)", text, flags=re.DOTALL)
    text = re.sub(r'<mention url="([^"]*)">(.*?)</mention>', r"[\2](\1)", text, flags=re.DOTALL)

    # tables: one "row: cell | cell" line per <tr>, processed before the outer tag is stripped
    def _table_sub(m: "re.Match") -> str:
        rows = re.findall(r"<tr>(.*?)</tr>", m.group(1), flags=re.DOTALL)
        lines = []
        for row in rows:
            cells = [c.strip() for c in re.findall(r"<td>(.*?)</td>", row, flags=re.DOTALL)]
            lines.append("row: " + " | ".join(cells))
        return "\n".join(lines)
    text = re.sub(r"<table>(.*?)</table>", _table_sub, text, flags=re.DOTALL)

    # inline formatting -> markdown equivalents (keeps the text, not the tag)
    text = re.sub(r"<b>(.*?)</b>", r"**\1**", text, flags=re.DOTALL)
    text = re.sub(r"<i>(.*?)</i>", r"*\1*", text, flags=re.DOTALL)
    text = re.sub(r"<code>(.*?)</code>", r"`\1`", text, flags=re.DOTALL)
    text = re.sub(r"<span[^>]*>(.*?)</span>", r"\1", text, flags=re.DOTALL)

    # layout-only wrappers -> removed, keeping their content
    text = re.sub(r"</?columns>", "", text)
    text = re.sub(r"</?column>", "", text)

    # catch-all for any stray tag left by a nesting shape the single-pass substitutions above
    # didn't fully resolve (e.g. a <span> nested inside another <span>) -- never leaves a raw
    # tag in the output, even in an edge case this function didn't anticipate by name.
    text = re.sub(r"</?(?:span|columns|column|b|i|code|page|mention)\b[^>]*>", "", text)
    return text


def _row_from_notion_mcp_page(doc_id: str, content: str, title: str, updated_at: Optional[str], url: Optional[str] = None) -> Dict[str, Any]:
    return {
        "doc_id": doc_id,
        "content": content,
        "title": title,
        "url": url or f"https://www.notion.so/{doc_id}",
        "source_type": "notion",
        "updated_at": updated_at,
        "content_hash": hashlib.sha256((content or "").encode("utf-8")).hexdigest(),
    }


def _crawl_notion_pages(
    root_page_ids: List[str], oauth_dir: str, exclude_title_contains: List[str], max_pages: Optional[int],
    concurrency: int, rate_limiter: "NotionMcpRateLimiter", prior_state: Dict[str, Dict[str, Any]],
    on_rate_limit=None, redact_secrets: bool = True, redact_secrets_extra_patterns: Optional[List[str]] = None,
    clean_markup: bool = True,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Dict[str, Any]]]:
    """Concurrent BFS over root_page_ids, following only `<page url="...">` child tags discovered
    in each fetch's decoded envelope (see _parse_fetch_envelope). Returns (rows, skip_info,
    new_state). `prior_state` ({doc_id: {content_hash, updated_at}}) lets an unchanged page keep
    its prior updated_at instead of bumping it every run. `skip_info` is
    {"excluded": int, "out_of_scope": int, "errored": int, "excluded_titles": [...],
    "secrets_redacted": int, "errored_examples": [...]} -- the last is up to 20 "{ref}: {error}"
    strings, since "errored" alone gave no way to tell a transient network blip from (say) a page
    consistently hitting the MCP SDK's SSE size cap. An auth failure (expired/rejected refresh
    token) is NOT counted here at all -- it propagates as NotionMcpAuthError and fails the whole
    crawl, since a per-page "errored" count would otherwise hide a total auth failure as if only
    a few pages had transient problems."""
    normalized_roots = {_normalize_notion_page_id(r) for r in root_page_ids}
    seen: set = set()
    rows: List[Dict[str, Any]] = []
    new_state: Dict[str, Dict[str, Any]] = {}
    skipped: Dict[str, Any] = {
        "excluded": 0, "out_of_scope": 0, "errored": 0, "excluded_titles": [], "secrets_redacted": 0,
        "errored_examples": [],
    }
    lock = threading.Lock()

    def _at_cap() -> bool:
        return bool(max_pages) and len(rows) >= max_pages

    def _process(ref: str) -> Tuple[List[str], Optional[Dict[str, Any]]]:
        doc_id = _normalize_notion_page_id(ref)
        try:
            raw = _nmo_call_tool(oauth_dir, "notion-fetch", {"id": ref}, rate_limiter, on_rate_limit=on_rate_limit)
        except NotionMcpAuthError:
            # Fatal, not a per-page skip -- propagates out of the whole crawl (and this asset) so
            # the run fails with a clear "sign in again" message instead of silently reporting
            # pages_extracted=0 with the real cause buried in an opaque per-page error count.
            raise
        except Exception as e:
            with lock:
                skipped["errored"] += 1
                if len(skipped["errored_examples"]) < 20:
                    skipped["errored_examples"].append(f"{ref}: {e}")
            return [], None

        envelope = _parse_fetch_envelope(raw)
        text = envelope["text"]

        ancestor_path = _extract_tag(text, "ancestor-path") or ""
        is_root = doc_id in normalized_roots
        if not is_root and ancestor_path and not any(r in ancestor_path for r in normalized_roots):
            with lock:
                skipped["out_of_scope"] += 1
            return [], None

        content = _extract_tag(text, "content") or _extract_tag(text, "wiki-home-content") or ""
        title = envelope["title"] or _extract_title(text, content)
        if exclude_title_contains and any(s.lower() in title.lower() for s in exclude_title_contains):
            with lock:
                skipped["excluded"] += 1
                skipped["excluded_titles"].append(title)
            return [], None  # skips this page AND its sub-pages (children are not returned either)

        if clean_markup:
            content = _clean_notion_markup(content)

        if redact_secrets:
            content, n_redacted = _redact_secrets(content, redact_secrets_extra_patterns)
            if n_redacted:
                with lock:
                    skipped["secrets_redacted"] += n_redacted

        prior = prior_state.get(doc_id) or {}
        content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
        now_iso = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
        updated_at = prior.get("updated_at") if prior.get("content_hash") == content_hash else now_iso
        row = _row_from_notion_mcp_page(doc_id, content, title, updated_at, url=envelope["url"])
        with lock:
            new_state[doc_id] = {"content_hash": content_hash, "updated_at": updated_at}
        children = [url for url, _title in _extract_child_pages(text)]
        return children, row

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
        futures = {}
        for rid in root_page_ids:
            norm = _normalize_notion_page_id(rid)
            if norm in seen:
                continue
            seen.add(norm)
            futures[executor.submit(_process, rid)] = rid

        while futures:
            done, _pending = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
            for fut in done:
                futures.pop(fut)
                children, row = fut.result()
                if row is not None:
                    with lock:
                        if not _at_cap():
                            rows.append(row)
                if _at_cap():
                    continue
                for child_ref in children:
                    norm_child = _normalize_notion_page_id(child_ref)
                    with lock:
                        if norm_child in seen or _at_cap():
                            continue
                        seen.add(norm_child)
                    futures[executor.submit(_process, child_ref)] = child_ref

    return rows, skipped, new_state


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat


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


class NotionMcpDocumentsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Extract Notion page body text via Notion's hosted MCP server (OAuth as the signed-in
    person -- no admin-created integration token needed). See module docstring for the full
    verified-facts list and this component's honestly-scoped assumptions."""

    asset_name: str = Field(description="Name of the asset that will hold the data")
    oauth_dir: str = Field(
        description=(
            "Directory holding tokens.json + client_info.json from the sign-in CLI (python -m "
            "dagster_community_components.resources.notion_mcp_login --oauth-dir <dir>). Treat "
            "as a secret -- it grants everything the signed-in person can see in Notion."
        ),
    )
    root_page_ids: List[str] = Field(
        description="Notion page ids (or URLs) to crawl as page trees -- each is walked, and any child_page link found while walking its content is recursively walked too, bounded to pages whose ancestor path traces back to one of these roots.",
    )
    exclude_title_contains: Optional[List[str]] = Field(
        default=None,
        description="Case-insensitive substrings; a page whose title contains any of these is skipped, along with all of its sub-pages (its children are never even visited).",
    )
    max_pages: Optional[int] = Field(default=None, description="Caps how many pages this run emits. Unset: no cap.")
    max_requests_per_second: float = Field(
        default=2.0,
        description="Shared across every concurrent worker through ONE rate limiter -- a per-worker sleep still exceeds Notion's real aggregate limit (a previously-hit problem). Default 2, below Notion's own ~3 req/s average.",
    )
    concurrency: int = Field(default=2, description="Number of pages crawled in parallel. Default 2 -- paired with max_requests_per_second=2, not a separate per-worker throttle.")
    max_retries: int = Field(default=5, description="Retries on Notion's own text-based rate-limit signal (never HTTP 429) or a transient/5xx-shaped error, before failing the run with a clear terminal error.")
    clean_markup: bool = Field(
        default=True,
        description=(
            "Strip/convert Notion's own markup tags out of page content before it's hashed or "
            "stored -- real examples from a live crawl: <br />, <page url=...>T</page> and "
            "<mention url=...>T</mention> (converted to markdown links [T](url), not dropped), "
            "<span>/<b>/<i>/<code> (converted to markdown emphasis or plain text), "
            "<columns>/<column> (removed, keeping their content), <table>/<tr>/<td> (converted "
            "to 'row: cell | cell' lines), and <unknown .../>/<table-of-contents /> (dropped). "
            "On by default -- raw markup otherwise pollutes chunks and the snippets an agent "
            "quotes. Applied AFTER the crawl's own child-page discovery, which needs the "
            "unmodified <page>/<mention> tags to traverse -- this only affects stored content."
        ),
    )
    redact_secrets: bool = Field(
        default=True,
        description=(
            "Replace Slack ('xox...'), GitHub ('gh[pousr]_...'), AWS ('AKIA...'), JWT-shaped, and "
            "PEM private-key strings found in page content with '[redacted]' before it's hashed or "
            "stored -- on by default, since a real runbook page can easily contain a pasted "
            "credential. The redaction count is reported as secrets_redacted in materialization "
            "metadata."
        ),
    )
    redact_secrets_extra_patterns: Optional[List[str]] = Field(
        default=None,
        description="Additional regex patterns (strings) to redact alongside the built-in ones. Only used when redact_secrets is True.",
    )
    state_path: Optional[str] = Field(
        default=None,
        description=(
            "Local JSON file tracking {doc_id: {content_hash, updated_at}} across runs, so an "
            "unchanged page keeps its prior updated_at instead of bumping it every run. Also used "
            "to report how many previously-seen pages fell out of scope this run (pages_pruned "
            "metadata) -- actually removing them from a vector store / knowledge graph is "
            "prune_missing_sources's job downstream, not this component's. Unset: every page's "
            "updated_at is 'now' on every run, and pages_pruned is never computed."
        ),
    )
    allow_empty_crawl: bool = Field(
        default=False,
        description=(
            "By default, a crawl that extracts 0 pages FAILS the run instead of succeeding with "
            "an empty frame -- a silent 0-page 'success' (e.g. from an expired login or a deleted "
            "root page) would otherwise look like real data downstream, and (with state_path set) "
            "would report every previously-seen page as pruned. Set True only if an empty result "
            "is genuinely expected for this crawl."
        ),
    )

    write_disposition: Literal["replace", "append", "merge"] = Field(
        default="replace",
        description="dlt write disposition. 'replace' (default): each run fully replaces the table (matches notion_documents's own default and reasoning -- a stale page tree would otherwise just keep accumulating). 'merge': upserts keyed on doc_id. 'append': a historical log of every extraction.",
    )
    destination: Optional[str] = Field(default=None, description="dlt destination identifier (e.g. 'snowflake', 'bigquery', 'postgres', 'filesystem', 'duckdb'). Leave empty for in-memory DuckDB -> DataFrame mode.")
    dataset_name: Optional[str] = Field(default=None, description="Target dataset/schema in the destination. Defaults to the asset name.")
    persist_only: bool = Field(default=False, description="If True with destination set: emit a MaterializeResult and skip the DataFrame return.")
    destination_credentials_url: Optional[str] = Field(default=None, description="Inline destination connection string. If unset, dlt resolves credentials from env vars.")
    destination_credentials_env_var: Optional[str] = Field(default=None, description="Alternative to destination_credentials_url: env var holding the connection string.")
    bucket_url: Optional[str] = Field(default=None, description="Bucket/path URL for filesystem-shaped storage, or the staging area for destination='databricks'/'athena'.")

    group_name: Optional[str] = Field(default="notion_mcp_documents", description="Asset group for organization")
    description: Optional[str] = Field(default=None, description="Asset description")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog. Auto-inferred if unset.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream Dagster asset keys for lineage.")
    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="Maximum acceptable lag in minutes before the asset is considered stale.")
    freshness_cron: Optional[str] = Field(default=None, description="Cron schedule string for the freshness policy.")
    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure (distinct from max_retries, which retries individual MCP tool calls).")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between asset-level retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")
    include_preview_metadata: bool = Field(default=True, description="Include a sample data preview in metadata. Every cell is truncated to 200 chars (see _safe_preview_markdown) so a large page's full content can't blow past a materialization event's size limit.")
    preview_rows: int = Field(default=25, ge=1, le=500, description="Rows to include in the preview metadata.")

    partition_type: Optional[str] = Field(default=None, description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'cron', 'static', 'multi', 'dynamic', or None for unpartitioned.")
    partition_cron_schedule: Optional[str] = Field(default=None, description="Required when partition_type='cron'.")
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d", description="strftime format for partition keys produced by partition_type='cron'.")
    partition_end_offset: int = Field(default=0, description="0 (default): a time-based partition set only ever holds COMPLETED windows. 1: include the current, still-in-progress window.")
    partition_timezone: Optional[str] = Field(default=None, description="IANA timezone for the partitions definition (defaults to UTC).")
    partition_start: Optional[str] = Field(default=None, description="Partition start date in ISO format. Required for time-based partition types.")
    partition_values: Optional[Union[str, List[str]]] = Field(default=None, description="Comma-separated values for static or multi partitioning.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition (partition_type='dynamic').")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None, description="Multi-axis partition spec. Overrides flat fields when set.")
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(default=None, description="Backfill execution strategy for a partitioned asset.")
    backfill_policy_max_partitions_per_run: Optional[int] = Field(default=None, description="For backfill_policy='multi_run' only.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('root_page_ids', 'exclude_title_contains', 'owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Notion MCP Documents', editable=True)

    def _resolve_destination(self):
        if not self.destination:
            return "duckdb"
        creds = None
        if self.destination_credentials_url:
            creds = self.destination_credentials_url
        elif self.destination_credentials_env_var:
            creds = dg.EnvVar(self.destination_credentials_env_var).get_value()
        if creds:
            factory = getattr(dlt.destinations, self.destination, None)
            if factory is not None:
                return factory(credentials=creds)
        return self.destination

    def _resolve_destination_with_bucket(self):
        if self.destination == "filesystem" and self.bucket_url:
            return dlt.destinations.filesystem(bucket_url=self.bucket_url)
        return self._resolve_destination()

    def _resolve_staging(self):
        if self.destination not in ("databricks", "athena"):
            return None
        if self.bucket_url:
            return dlt.destinations.filesystem(bucket_url=self.bucket_url)
        return "filesystem"

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        asset_name = self.asset_name
        oauth_dir = self.oauth_dir
        root_page_ids = list(self.root_page_ids)
        exclude_title_contains = self.exclude_title_contains or []
        max_pages = self.max_pages
        max_requests_per_second = self.max_requests_per_second
        concurrency = self.concurrency
        max_retries = self.max_retries
        clean_markup = self.clean_markup
        redact_secrets = self.redact_secrets
        redact_secrets_extra_patterns = self.redact_secrets_extra_patterns
        state_path = self.state_path
        allow_empty_crawl = self.allow_empty_crawl
        write_disposition = self.write_disposition
        destination = self.destination
        dataset_name = self.dataset_name or asset_name
        persist_only = self.persist_only
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (self.deps or [])]

        _inferred_kinds = list(self.kinds or []) or ["notion", "python"]
        if "rag" not in _inferred_kinds:
            _inferred_kinds.append("rag")
        _inferred_kinds = list(dict.fromkeys(_inferred_kinds))
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = (
            _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
            if self.freshness_max_lag_minutes is not None else None
        )
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule, cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
            end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )
        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            backfill_policy = dg.BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            backfill_policy = dg.BackfillPolicy.multi_run(max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1)

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or "Extract Notion page body text via Notion's hosted MCP server (OAuth as the signed-in person).",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=_all_tags,
            deps=deps_keys,
            partitions_def=partitions_def,
            backfill_policy=backfill_policy,
            retry_policy=_retry_policy,
            freshness_policy=_freshness_policy,
        )
        def notion_mcp_documents_asset(context: dg.AssetExecutionContext):
            prior_state: Dict[str, Dict[str, Any]] = {}
            if state_path and os.path.isfile(state_path):
                try:
                    with open(state_path, "r", encoding="utf-8") as f:
                        prior_state = __import__("json").load(f)
                except (OSError, ValueError) as e:
                    context.log.warning(f"Could not read state_path={state_path!r} (treating as empty): {e}")

            rate_limiter = NotionMcpRateLimiter(max_requests_per_second)
            rate_limit_stats = {"hits": 0, "total_wait_seconds": 0.0}
            rl_lock = threading.Lock()

            def _on_rate_limit(wait_seconds: float):
                with rl_lock:
                    rate_limit_stats["hits"] += 1
                    rate_limit_stats["total_wait_seconds"] += wait_seconds
                context.log.info(f"Notion MCP rate limit hit; waiting {wait_seconds}s before retrying.")

            context.log.info(f"Crawling {len(root_page_ids)} root page(s) via Notion's hosted MCP server...")
            rows, skipped, new_state = _crawl_notion_pages(
                root_page_ids, oauth_dir, exclude_title_contains, max_pages, concurrency,
                rate_limiter, prior_state, on_rate_limit=_on_rate_limit,
                redact_secrets=redact_secrets, redact_secrets_extra_patterns=redact_secrets_extra_patterns,
                clean_markup=clean_markup,
            )
            if skipped["excluded_titles"]:
                context.log.info(f"Excluded {len(skipped['excluded_titles'])} page(s) by title: {skipped['excluded_titles']}")
            if skipped["errored_examples"]:
                context.log.warning(f"{skipped['errored']} page(s) errored during crawl: {skipped['errored_examples']}")

            if not rows and not allow_empty_crawl:
                # Refuse to overwrite state_path or hand an empty frame downstream -- either would
                # otherwise look like "0 real pages" (and, with state_path set, pages_pruned would
                # wrongly list EVERY previously-seen page as pruned, since new_state stays empty --
                # a real risk if that metadata ever feeds a downstream prune_missing_sources step).
                raise dg.Failure(
                    description=(
                        f"Notion MCP crawl extracted 0 pages from {len(root_page_ids)} root page(s) "
                        f"(excluded={skipped['excluded']}, out_of_scope={skipped['out_of_scope']}, "
                        f"errored={skipped['errored']}). Set allow_empty_crawl=True if an empty "
                        "result is genuinely expected (e.g. a brand-new, still-empty root page)."
                    ),
                    metadata={
                        "pages_skipped_excluded": dg.MetadataValue.int(skipped["excluded"]),
                        "pages_skipped_out_of_scope": dg.MetadataValue.int(skipped["out_of_scope"]),
                        "pages_skipped_errored": dg.MetadataValue.int(skipped["errored"]),
                        "pages_errored_examples": dg.MetadataValue.json(skipped["errored_examples"]),
                    },
                )

            pages_pruned = sorted(set(prior_state) - set(new_state)) if state_path else []
            if state_path:
                try:
                    os.makedirs(os.path.dirname(state_path) or ".", exist_ok=True)
                    tmp_path = state_path + ".tmp"
                    with open(tmp_path, "w", encoding="utf-8") as f:
                        __import__("json").dump(new_state, f)
                    os.replace(tmp_path, state_path)
                except OSError as e:
                    context.log.warning(f"Could not write state_path={state_path!r}: {e}")

            @dlt.resource(name="pages")
            def _pages_resource():
                yield from rows

            resource = _pages_resource()
            hint_kwargs: Dict[str, Any] = {"write_disposition": write_disposition}
            if write_disposition == "merge":
                hint_kwargs["primary_key"] = "doc_id"
            resource.apply_hints(**hint_kwargs)

            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination=component._resolve_destination_with_bucket(),
                staging=component._resolve_staging(),
                dataset_name=dataset_name,
            )
            context.log.info(f"Loading {len(rows)} Notion page(s) into destination...")
            load_info = pipeline.run([resource])
            load_id = load_info.loads_ids[0] if getattr(load_info, "loads_ids", None) else None

            base_metadata = {
                "destination": dg.MetadataValue.text(destination or "duckdb (in-memory)"),
                "pages_extracted": dg.MetadataValue.int(len(rows)),
                "pages_skipped_excluded": dg.MetadataValue.int(skipped["excluded"]),
                "pages_skipped_out_of_scope": dg.MetadataValue.int(skipped["out_of_scope"]),
                "pages_skipped_errored": dg.MetadataValue.int(skipped["errored"]),
                "pages_errored_examples": dg.MetadataValue.json(skipped["errored_examples"]),
                "pages_excluded_titles": dg.MetadataValue.json([t[:200] for t in skipped["excluded_titles"][:50]]),
                "secrets_redacted": dg.MetadataValue.int(skipped["secrets_redacted"]),
                "pages_pruned_count": dg.MetadataValue.int(len(pages_pruned)),
                "pages_pruned": dg.MetadataValue.json(pages_pruned[:500]),
                "rate_limit_hits": dg.MetadataValue.int(rate_limit_stats["hits"]),
                "rate_limit_total_wait_seconds": dg.MetadataValue.float(rate_limit_stats["total_wait_seconds"]),
                "write_disposition": dg.MetadataValue.text(write_disposition),
                "load_id": dg.MetadataValue.text(load_id) if load_id else dg.MetadataValue.null(),
            }

            non_sql_destinations = {"filesystem", "delta", "iceberg"}
            if persist_only or destination in non_sql_destinations:
                return dg.MaterializeResult(metadata=base_metadata)

            with pipeline.sql_client() as client:
                try:
                    with client.execute_query(f"SELECT * FROM {dataset_name}.pages") as cur:
                        df = cur.df()
                except Exception as e:
                    context.log.warning(f"Could not read back pages table: {e}")
                    df = pd.DataFrame(rows)

            metadata = {**base_metadata, "row_count": dg.MetadataValue.int(len(df))}
            if include_preview and len(df) > 0:
                try:
                    metadata["preview"] = dg.MetadataValue.md(_safe_preview_markdown(df, preview_rows))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return dg.Output(value=df, metadata=metadata)

        return dg.Definitions(assets=[notion_mcp_documents_asset])


def _build_partitions_def(
    partition_type, partition_start, partition_values, dynamic_partition_name, partition_dimensions,
    cron_schedule=None, cron_fmt="%Y-%m-%d", end_offset=0, timezone=None,
):
    """Construct a Dagster partitions_def from the canonical partition fields (same shape as
    every other component in this repo's own copy of this helper)."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type (flat-fields shape) or partition_dimensions (multi-axis shape), not both.")

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01').")
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError("partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only.")
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")
