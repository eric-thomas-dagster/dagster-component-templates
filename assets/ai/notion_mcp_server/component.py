"""NotionMcpServerComponent -- live Notion search/fetch for a chat agent, via Notion's HOSTED MCP server.

Same "OAuth as the signed-in person, no admin-created integration token needed" tradeoff as
`notion_mcp_documents` (see that component's module docstring for the full verified-facts list),
but for LIVE lookups from a chat agent instead of a batch extraction asset.

Why a local stdio proxy instead of pointing `mcp_server_resource`/an agent's inline `server:` spec
straight at `https://mcp.notion.com/mcp`: that resource's `http` transport sends a STATIC
`Authorization` header (from `headers_env`, read once at call time) -- fine for a token that never
expires, wrong here, since Notion's access token lives ~8h and needs refreshing through a stored,
file-locked, auto-rotating refresh token (see `notion_mcp_documents`'s `_nmo_get_access_token`).
This component instead ships a small LOCAL stdio MCP server (`python -m <this module>`, same
pattern as `report_tools_server`) that, on every tool call, refreshes the token if needed and
proxies the call to Notion's real hosted server -- the agent framework never sees a token at all,
just a normal stdio MCP server exposing `notion-search`/`notion-fetch` (an `allow` list; default
both).

Rate limiting and the rate-limit-TEXT-not-HTTP-429 detection are the exact same mechanism as
`notion_mcp_documents` (`NotionMcpRateLimiter` / `_nmo_call_tool`), inlined here byte-identically
-- see tests/test_notion_mcp_oauth_drift.py. One `NotionMcpRateLimiter` instance per server
process (every concurrent tool call from the agent shares it), not per call.
"""
import os
import sys
from typing import Any, Dict, List, Literal, Optional

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


_ALL_TOOLS = ("notion-search", "notion-fetch")


class NotionMcpProxyTools:
    """`call(tool, args)` is the safe entry point an in-process caller (or the stdio server body
    below) uses -- allow-list + rate limiting + rate-limit-text retry + error shaping, never a
    raw exception escaping to an agent loop."""

    def __init__(self, oauth_dir: str, *, allow: Optional[List[str]] = None, max_requests_per_second: float = 2.0, max_retries: int = 5):
        self.oauth_dir = oauth_dir
        if allow is None:
            allow = list(_ALL_TOOLS)
        unknown = sorted(set(allow) - set(_ALL_TOOLS))
        if unknown:
            raise ValueError(f"unknown tool(s) in allow: {unknown}; valid: {list(_ALL_TOOLS)}")
        self.allow = frozenset(allow)
        self.max_retries = max_retries
        self.rate_limiter = NotionMcpRateLimiter(max_requests_per_second)
        self.rate_limit_hits = 0
        self.rate_limit_total_wait_seconds = 0.0

    def tool_names(self) -> List[str]:
        return [t for t in _ALL_TOOLS if t in self.allow]

    def _on_rate_limit(self, wait_seconds: float) -> None:
        self.rate_limit_hits += 1
        self.rate_limit_total_wait_seconds += wait_seconds

    def call(self, tool: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        args = dict(args or {})
        if tool not in _ALL_TOOLS:
            return {"error": f"unknown tool {tool!r}; available: {self.tool_names()}"}
        if tool not in self.allow:
            return {"error": f"tool {tool!r} is not enabled (allowed: {self.tool_names()})"}
        try:
            text = _nmo_call_tool(
                self.oauth_dir, tool, args, self.rate_limiter,
                max_retries=self.max_retries, on_rate_limit=self._on_rate_limit,
            )
        except NotionMcpAuthError as e:
            return {"error": str(e)}
        except NotionMcpRateLimited as e:
            return {"error": str(e)}
        except Exception as e:  # noqa: BLE001 -- surface as data, the agent loop must not crash
            return {"error": f"{tool} failed: {type(e).__name__}: {e}"}
        return {"text": text}


def _this_module_name() -> str:
    """The real importable dotted path for `python -m <this>` -- `__name__` is literally
    `"__main__"` for both a bare `python component.py` and `python -m dotted.path`, so
    `__spec__.name` is used instead (same fix as report_tools_server's own `_this_module_name`,
    duplicated here per this repo's self-contained-component convention)."""
    if __spec__ is not None:
        return __spec__.name
    raise RuntimeError(
        "notion_mcp_server's mcp_server_resource_key path must be run as `python -m "
        "<dotted.path.to.this.module>`, not `python component.py` directly. See README.md."
    )


class _McpServerShapeResource(dg.ConfigurableResource):
    """Minimal MCPServerResource-shaped resource -- inlined rather than imported, same shape as
    report_tools_server's own copy."""

    name: str
    type: Literal["http", "sse", "stdio"] = "stdio"
    command: Optional[List[str]] = None
    url: Optional[str] = None
    env: Optional[Dict[str, str]] = None
    headers: Optional[Dict[str, str]] = None
    headers_env: Optional[Dict[str, str]] = None
    instructions: Optional[str] = None


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


class NotionMcpServerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers a local stdio MCP server proxying `notion-search`/`notion-fetch` to Notion's
    hosted MCP server, refreshing the signed-in person's OAuth token as needed. Any
    mcp_servers-aware agent component references it via `server_resource_key`."""

    resource_key: str = Field(default="notion_mcp", description="Dagster resource key other components reference via server_resource_key.")
    oauth_dir: str = Field(description="Directory holding tokens.json + client_info.json from the sign-in CLI. Treat as a secret.")
    allow: Optional[List[str]] = Field(default=None, description=f"Hard allow-list of tools (default: both). Valid: {', '.join(_ALL_TOOLS)}.")
    max_requests_per_second: float = Field(default=2.0, description="Shared by every tool call this server process makes -- see NotionMcpRateLimiter.")
    max_retries: int = Field(default=5, description="Retries on Notion's own text-based rate-limit signal or a transient error before returning {'error': ...} to the agent.")
    instructions: Optional[str] = Field(
        default=None,
        description=(
            "Free-text usage guidance for this server -- read by the litellm/openai/anthropic/"
            "gemini agent components and appended to their system prompt under a heading with "
            "this resource's name, so every agent referencing it via server_resource_key gets "
            "the same guidance with no copy-paste."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("allow",), csv=(), tags=())

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Notion MCP Server', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        allow = list(self.allow) if self.allow else None
        if allow is not None:
            bad = sorted(set(allow) - set(_ALL_TOOLS))
            if bad:
                raise ValueError(f"unknown tool(s) in allow: {bad}; valid: {list(_ALL_TOOLS)}")

        mcp_env: Dict[str, str] = {
            "NOTION_MCP_OAUTH_DIR": self.oauth_dir,
            "NOTION_MCP_MAX_REQUESTS_PER_SECOND": str(self.max_requests_per_second),
            "NOTION_MCP_MAX_RETRIES": str(self.max_retries),
        }
        if allow:
            mcp_env["NOTION_MCP_ALLOW"] = ",".join(allow)

        resource = _McpServerShapeResource(
            name=self.resource_key,
            type="stdio",
            command=[sys.executable, "-m", _this_module_name()],
            env=mcp_env,
            instructions=self.instructions,
        )
        return dg.Definitions(resources={self.resource_key: resource})

    @classmethod
    def get_description(cls) -> str:
        return cls.__doc__ or ""


# ── stdio MCP server entrypoint:  python -m <this module>  ──────────────────────────────────────
def build_mcp_server(tools: "NotionMcpProxyTools"):
    """MCP server (official `mcp` package) exposing exactly the allowed tools. Works on mcp 1.x and 2.x."""
    try:  # mcp 2.x renamed FastMCP -> MCPServer (mcp.server.fastmcp now raises ModuleNotFoundError)
        from mcp.server.mcpserver import MCPServer as _Server
    except ImportError:  # mcp 1.x
        from mcp.server.fastmcp import FastMCP as _Server

    try:  # the SDK's own ToolError, not this module's -- see _mcp_call below for why that distinction matters.
        from mcp.server.mcpserver.exceptions import ToolError as _SdkToolError  # mcp 2.x (confirmed live, 2.3.0)
    except ImportError:
        from mcp.server.fastmcp.exceptions import ToolError as _SdkToolError  # mcp 1.x (stable FastMCP API, not live-verified here)

    mcp = _Server("notion-mcp-proxy")
    allowed = set(tools.tool_names())

    def _tool(fn):
        if fn.__name__.replace("_", "-") in allowed:
            mcp.tool(name=fn.__name__.replace("_", "-"))(fn)
        return fn

    def _mcp_call(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """tools.call() never raises (see NotionMcpProxyTools.call) -- a bad-arguments or failure comes back as a
        plain {"error": ...} dict, which the MCP transport would otherwise hand back as a NORMAL (isError=False)
        tool result indistinguishable from success. Re-raising here lets the `mcp` package's own exception
        handling (both fastmcp 1.x and mcp.server.mcpserver 2.x catch any exception from a registered tool
        function and turn it into a real isError=True CallToolResult) make the failure visible to whatever's
        counting failed calls in the agent loop. MUST be the SDK's own ToolError (_SdkToolError), not a local
        one: the SDK's tool dispatch (mcp/server/mcpserver/tools/base.py `Tool.run`) only keeps a raised
        exception's own message when it recognizes the exception as ITS OWN ToolError/ResourceError -- anything
        else (including a same-named ToolError class defined elsewhere) is treated as an unexpected crash and
        replaced with a generic 'Error executing tool <name>' before it reaches the client, throwing away the
        reason the agent needs to act on."""
        result = tools.call(name, args)
        if isinstance(result, dict) and "error" in result:
            raise _SdkToolError(result["error"])
        return result

    @_tool
    def notion_search(query: str) -> dict:
        """Search the signed-in person's Notion workspace (everything they can see -- this is
        NOT scoped to a service account). Returns Notion's own MCP result as {"text": ...}, or
        {"error": ...}."""
        return _mcp_call("notion-search", {"query": query})

    @_tool
    def notion_fetch(id: str) -> dict:
        """Fetch one Notion page/database by id or URL. Returns markdown-ish XML ({"text": ...}:
        page body in <content>, child pages as <page url="...">, links as <mention url="...">) or
        {"error": ...}. NEVER returns Notion's own rate-limit message as if it were content --
        that is detected and retried internally (see module docstring)."""
        return _mcp_call("notion-fetch", {"id": id})

    return mcp


def main(argv: Optional[List[str]] = None) -> None:
    """Env: NOTION_MCP_OAUTH_DIR (required), NOTION_MCP_ALLOW (csv), _MAX_REQUESTS_PER_SECOND, _MAX_RETRIES."""
    import logging
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(message)s")  # stdout is the MCP channel
    oauth_dir = os.environ.get("NOTION_MCP_OAUTH_DIR")
    if not oauth_dir:
        raise SystemExit("NOTION_MCP_OAUTH_DIR must be set")
    allow_env = os.environ.get("NOTION_MCP_ALLOW")
    allow = [t.strip() for t in allow_env.split(",") if t.strip()] if allow_env else None

    tools = NotionMcpProxyTools(
        oauth_dir,
        allow=allow,
        max_requests_per_second=float(os.environ.get("NOTION_MCP_MAX_REQUESTS_PER_SECOND", "2.0")),
        max_retries=int(os.environ.get("NOTION_MCP_MAX_RETRIES", "5")),
    )
    build_mcp_server(tools).run(transport="stdio")


if __name__ == "__main__":
    main()
