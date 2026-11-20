"""HostedMcpServerComponent -- a stdio proxy for ANY hosted MCP server that uses OAuth, generalizing
`NotionMcpServerComponent` (which can become a preset of this component -- see that component's
README; kept separate for now rather than refactored, to avoid risking a working integration).

Sign in first with the companion CLI:

    python -m dagster_community_components.resources.mcp_oauth_login --url <mcp url> --oauth-dir <dir>

Why a local stdio proxy instead of pointing `mcp_server_resource`'s `http` transport straight at
the hosted URL: that resource's `headers_env` sends a STATIC `Authorization` header, read once --
fine for a server that takes a permanent static token (GitHub's hosted MCP server:
`Authorization: Bearer <personal access token>`; Linear's: `Authorization: Bearer <API key>` --
note this differs from Linear's OWN REST API, which takes the raw key with no `Bearer` prefix;
use `MCPServerResourceComponent` directly for either, no OAuth needed at all), wrong for a server
whose OAuth access token expires and needs refreshing through a stored, file-locked,
auto-rotating refresh token (same mechanism `mcp_oauth_login`/this component share with
`notion_mcp_documents`/`notion_mcp_server`).

Tool interface shape -- stated plainly, not hidden: each name in `allow` is exposed as
`<tool_name>(arguments: dict)`, a single nested object parameter carrying the upstream tool's
own real argument names (e.g. `get_issue(arguments={"id": "ABC-123"})`), NOT flattened kwargs
matching that tool's own JSON schema directly. Mirroring the upstream server's exact per-tool
schema would require MCP's low-level `Server` API (`on_list_tools`/`on_call_tool` callbacks on
mcp 2.x; decorator-based `@server.list_tools()`/`@server.call_tool()` on 1.x -- confirmed these
are genuinely different, incompatible APIs across the SDK's 1.x/2.x split), which isn't something
this repo can verify against a live third-party server in this environment. The `arguments: dict`
wrapper is simple, identical on both SDK majors (confirmed: FastMCP/MCPServer's own schema
inference turns a `Dict[str, Any]` parameter into a free-form `additionalProperties: true` object
schema on both), and testable without a live server -- a deliberate, documented tradeoff, not an
oversight.

`allow` defaults to EMPTY (unlike `NotionMcpServerComponent`'s "both tools by default"): an
arbitrary hosted server's tool surface is unknown ahead of time and may include destructive
tools (delete/write) -- defaulting to "nothing exposed" means a user must explicitly name every
tool they want an agent to call.

Rate limiting here handles a REAL HTTP 429 (if the transport surfaces one as an exception -- the
common case for most servers) and an optional, configurable SOFT text-based signal
(`soft_rate_limit_pattern`, a regex) for a server that signals rate limits as ordinary tool-result
text instead of HTTP 429 (Notion's hosted server is the confirmed example of that -- see
`notion_mcp_documents`); unset, a generic default pattern (`rate limit|too many requests|retry
...after`) is used, which may not match every server's own wording.
"""
import os
import sys
from typing import Any, Dict, List, Literal, Optional

import dagster as dg
from pydantic import Field, model_validator


# BEGIN hosted mcp oauth (inlined per component; keep copies identical — see tests/test_hosted_mcp_oauth_drift.py)
import fcntl as _hmo_fcntl  # noqa: E402,F401
import json as _hmo_json  # noqa: E402,F401
import os as _hmo_os  # noqa: E402,F401
import re as _hmo_re  # noqa: E402,F401
import threading as _hmo_threading  # noqa: E402,F401
import time as _hmo_time  # noqa: E402,F401

_HTTP_429_RE = _hmo_re.compile(r"\b429\b")
_SOFT_RATE_LIMIT_RE = _hmo_re.compile(r"rate limit|too many requests|retry.{0,15}after", _hmo_re.IGNORECASE)


class HostedMcpAuthError(Exception):
    """Sign-in is missing or the refresh token was rejected -- surfaced distinctly from a
    generic network/API error so a caller can give an actionable 'sign in again' message."""


class HostedMcpRateLimited(Exception):
    """A rate-limit signal (real HTTP 429, or a soft text match -- see _SOFT_RATE_LIMIT_RE)
    persisted past max_retries."""

    def __init__(self, detail: str):
        self.detail = detail
        super().__init__(f"MCP server rate limit exceeded max_retries ({detail})")


def _hmo_tokens_path(oauth_dir: str) -> str:
    return _hmo_os.path.join(_hmo_os.path.expanduser(oauth_dir), "tokens.json")


def _hmo_client_info_path(oauth_dir: str) -> str:
    return _hmo_os.path.join(_hmo_os.path.expanduser(oauth_dir), "client_info.json")


def _hmo_read_json(path: str):
    if not _hmo_os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return _hmo_json.load(f)


def _hmo_atomic_write(path: str, data) -> None:
    """Atomic, 0600-permissioned write. No locking here -- callers that need a read-modify-write
    critical section (see _hmo_get_access_token) take the lock themselves and call this directly,
    to avoid a self-deadlock from acquiring the same flock twice in one process."""
    _hmo_os.makedirs(_hmo_os.path.dirname(path) or ".", exist_ok=True)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        _hmo_json.dump(data, f)
    _hmo_os.chmod(tmp_path, 0o600)
    _hmo_os.replace(tmp_path, path)


def _hmo_write_tokens(oauth_dir: str, data) -> None:
    """Locked single-shot write -- used by the sign-in CLI after the initial PKCE exchange."""
    lock_path = _hmo_tokens_path(oauth_dir) + ".lock"
    lock_fd = _hmo_os.open(lock_path, _hmo_os.O_CREAT | _hmo_os.O_RDWR, 0o600)
    try:
        _hmo_fcntl.flock(lock_fd, _hmo_fcntl.LOCK_EX)
        _hmo_atomic_write(_hmo_tokens_path(oauth_dir), data)
    finally:
        _hmo_fcntl.flock(lock_fd, _hmo_fcntl.LOCK_UN)
        _hmo_os.close(lock_fd)


def _hmo_get_access_token(oauth_dir: str, token_endpoint: str) -> str:
    """Returns a live access token, refreshing under a file lock (re-checking after acquiring
    it, in case a concurrent process already refreshed while this one waited) when the cached
    one is expired or about to expire. Persists the NEW refresh token if the server rotates it."""
    import requests

    tokens = _hmo_read_json(_hmo_tokens_path(oauth_dir))
    if not tokens:
        raise HostedMcpAuthError(
            f"No tokens found under oauth_dir={oauth_dir!r}. Sign in first: python -m "
            "dagster_community_components.resources.mcp_oauth_login --url <mcp url> --oauth-dir "
            + repr(oauth_dir)
        )
    if tokens.get("expires_at", 0) > _hmo_time.time() + 60:
        return tokens["access_token"]

    client_info = _hmo_read_json(_hmo_client_info_path(oauth_dir))
    if not client_info:
        raise HostedMcpAuthError(f"No client_info found under oauth_dir={oauth_dir!r}. Sign in again.")

    lock_path = _hmo_tokens_path(oauth_dir) + ".lock"
    lock_fd = _hmo_os.open(lock_path, _hmo_os.O_CREAT | _hmo_os.O_RDWR, 0o600)
    try:
        _hmo_fcntl.flock(lock_fd, _hmo_fcntl.LOCK_EX)
        tokens = _hmo_read_json(_hmo_tokens_path(oauth_dir)) or tokens
        if tokens.get("expires_at", 0) > _hmo_time.time() + 60:
            return tokens["access_token"]
        resp = requests.post(
            token_endpoint,
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
            raise HostedMcpAuthError(
                f"Refresh token was rejected (HTTP {resp.status_code}). Sign in again: python -m "
                f"dagster_community_components.resources.mcp_oauth_login --url <mcp url> "
                f"--oauth-dir {oauth_dir!r}"
            )
        resp.raise_for_status()
        data = resp.json()
        new_tokens = {
            "access_token": data["access_token"],
            "refresh_token": data.get("refresh_token") or tokens["refresh_token"],
            "expires_at": _hmo_time.time() + data.get("expires_in", 3600),
        }
        _hmo_atomic_write(_hmo_tokens_path(oauth_dir), new_tokens)
        return new_tokens["access_token"]
    finally:
        _hmo_fcntl.flock(lock_fd, _hmo_fcntl.LOCK_UN)
        _hmo_os.close(lock_fd)


class HostedMcpRateLimiter:
    """ONE instance shared by every worker -- a per-worker sleep still exceeds a server's real
    aggregate limit when several workers each pace independently."""

    def __init__(self, max_requests_per_second: float):
        self._interval = 1.0 / max(float(max_requests_per_second), 0.01)
        self._lock = _hmo_threading.Lock()
        self._next_allowed = 0.0

    def acquire(self) -> None:
        with self._lock:
            now = _hmo_time.time()
            wait = self._next_allowed - now
            if wait > 0:
                _hmo_time.sleep(wait)
                now = _hmo_time.time()
            self._next_allowed = now + self._interval


async def _hmo_open_mcp_session(stack, mcp_url: str, access_token: str):
    """Streamable-HTTP MCP client with a bearer token. Works on mcp 1.x and 2.x -- mcp 2.x moved
    header support onto an httpx-like client object instead of a `headers=` kwarg (same shim
    this repo's mcp_server_resource/agentic_pipeline components already use for their own
    streamable-HTTP connections, duplicated here per this repo's self-contained-component
    convention rather than cross-importing)."""
    headers = {"Authorization": f"Bearer {access_token}"}
    from mcp import ClientSession
    try:
        from mcp.client.streamable_http import streamable_http_client
        from mcp.shared._httpx_utils import create_mcp_http_client
        http_client = await stack.enter_async_context(create_mcp_http_client(headers=headers))
        streams = await stack.enter_async_context(streamable_http_client(mcp_url, http_client=http_client))
    except ImportError:
        from mcp.client.streamable_http import streamablehttp_client
        streams = await stack.enter_async_context(streamablehttp_client(mcp_url, headers=headers))
    read, write = streams[0], streams[1]
    session = await stack.enter_async_context(ClientSession(read, write))
    await session.initialize()
    return session


async def _hmo_call_tool_async(mcp_url: str, oauth_dir: str, token_endpoint: str, tool_name: str, arguments: Optional[Dict[str, Any]], timeout_seconds: float = 60.0) -> str:
    import anyio
    from contextlib import AsyncExitStack

    access_token = _hmo_get_access_token(oauth_dir, token_endpoint)
    async with AsyncExitStack() as stack:
        session = await _hmo_open_mcp_session(stack, mcp_url, access_token)
        with anyio.fail_after(timeout_seconds):
            result = await session.call_tool(tool_name, arguments or {})
        parts = [getattr(item, "text", None) for item in (result.content or [])]
        return "\n".join(p for p in parts if p)


async def _hmo_list_tools_async(mcp_url: str, oauth_dir: str, token_endpoint: str, timeout_seconds: float = 60.0) -> List[str]:
    import anyio
    from contextlib import AsyncExitStack

    access_token = _hmo_get_access_token(oauth_dir, token_endpoint)
    async with AsyncExitStack() as stack:
        session = await _hmo_open_mcp_session(stack, mcp_url, access_token)
        with anyio.fail_after(timeout_seconds):
            result = await session.list_tools()
        return [t.name for t in result.tools]


def _hmo_call_tool(
    mcp_url: str, oauth_dir: str, token_endpoint: str, tool_name: str, arguments: Optional[Dict[str, Any]],
    rate_limiter: "HostedMcpRateLimiter", max_retries: int = 5, on_rate_limit=None, timeout_seconds: float = 60.0,
    soft_rate_limit_pattern: Optional[str] = None,
) -> str:
    """Sync entry point used by every caller: enforces `rate_limiter`, retries on a real HTTP 429
    (surfaced by the underlying transport as an exception) or a soft text-based rate-limit signal
    (configurable via soft_rate_limit_pattern -- different servers phrase this differently;
    Notion's own hosted server is the confirmed example of text-only, never-429 signaling, see
    notion_mcp_documents), and on other transient errors, up to max_retries."""
    import asyncio

    pattern = _hmo_re.compile(soft_rate_limit_pattern, _hmo_re.IGNORECASE) if soft_rate_limit_pattern else _SOFT_RATE_LIMIT_RE
    attempt = 0
    while True:
        rate_limiter.acquire()
        try:
            text = asyncio.run(_hmo_call_tool_async(mcp_url, oauth_dir, token_endpoint, tool_name, arguments, timeout_seconds))
        except HostedMcpAuthError:
            raise
        except Exception as e:
            is_rate_limit = bool(_HTTP_429_RE.search(str(e)))
            attempt += 1
            if is_rate_limit:
                if on_rate_limit:
                    on_rate_limit(str(e))
                if attempt > max_retries:
                    raise HostedMcpRateLimited(str(e))
                _hmo_time.sleep(min(2 ** attempt, 30))
                continue
            if attempt > max_retries:
                raise RuntimeError(f"MCP tool {tool_name!r} failed after {max_retries} retries: {e}") from e
            _hmo_time.sleep(min(2 ** attempt, 30))
            continue
        if pattern.search(text or ""):
            attempt += 1
            if on_rate_limit:
                on_rate_limit(text)
            if attempt > max_retries:
                raise HostedMcpRateLimited(text)
            _hmo_time.sleep(min(2 ** attempt, 30))
            continue
        return text
# END hosted mcp oauth


def check_token_health(oauth_dir: str) -> Dict[str, Any]:
    """Confirms the refresh token still works, without making a real MCP tool call -- just a
    token refresh/validity check. Returns {"healthy": bool, "message": str}. 'sign in again' in
    the message (not a raw auth-library exception) is what a caller should show a user."""
    client_info = _hmo_read_json(_hmo_client_info_path(oauth_dir))
    if not client_info or not client_info.get("token_endpoint"):
        return {"healthy": False, "message": f"No client_info found under oauth_dir={oauth_dir!r}. Sign in first."}
    try:
        _hmo_get_access_token(oauth_dir, client_info["token_endpoint"])
    except HostedMcpAuthError as e:
        return {"healthy": False, "message": str(e)}
    except Exception as e:  # noqa: BLE001 -- a health check must never crash, just report
        return {"healthy": False, "message": f"Token health check failed: {type(e).__name__}: {e}"}
    return {"healthy": True, "message": "Token is valid."}


class HostedMcpProxyTools:
    """`call(tool, args)` is the safe entry point an in-process caller (or the stdio server body
    below) uses -- allow-list + rate limiting + retry + error shaping, never a raw exception
    escaping to an agent loop."""

    def __init__(
        self, mcp_url: str, oauth_dir: str, token_endpoint: str, *, allow: Optional[List[str]] = None,
        max_requests_per_second: float = 2.0, max_retries: int = 5, soft_rate_limit_pattern: Optional[str] = None,
    ):
        self.mcp_url = mcp_url
        self.oauth_dir = oauth_dir
        self.token_endpoint = token_endpoint
        self.allow = frozenset(allow or [])
        self.max_retries = max_retries
        self.soft_rate_limit_pattern = soft_rate_limit_pattern
        self.rate_limiter = HostedMcpRateLimiter(max_requests_per_second)
        self.rate_limit_hits = 0

    def tool_names(self) -> List[str]:
        return sorted(self.allow)

    def _on_rate_limit(self, detail: str) -> None:
        self.rate_limit_hits += 1

    def call(self, tool: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        args = dict(args or {})
        if tool not in self.allow:
            return {"error": f"tool {tool!r} is not enabled (allowed: {self.tool_names()})"}
        try:
            text = _hmo_call_tool(
                self.mcp_url, self.oauth_dir, self.token_endpoint, tool, args, self.rate_limiter,
                max_retries=self.max_retries, on_rate_limit=self._on_rate_limit,
                soft_rate_limit_pattern=self.soft_rate_limit_pattern,
            )
        except HostedMcpAuthError as e:
            return {"error": str(e)}
        except HostedMcpRateLimited as e:
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
        "hosted_mcp_server's mcp_server_resource_key path must be run as `python -m "
        "<dotted.path.to.this.module>`, not `python component.py` directly. See README.md."
    )


class _McpServerShapeResource(dg.ConfigurableResource):
    """Minimal MCPServerResource-shaped resource -- inlined rather than imported, same shape as
    report_tools_server's / notion_mcp_server's own copies."""

    name: str
    type: Literal["http", "sse", "stdio"] = "stdio"
    command: Optional[List[str]] = None
    url: Optional[str] = None
    env: Optional[Dict[str, str]] = None
    headers: Optional[Dict[str, str]] = None
    headers_env: Optional[Dict[str, str]] = None


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


class HostedMcpServerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers a local stdio MCP server proxying an allow-listed subset of tools from ANY
    hosted MCP server that uses OAuth, refreshing the signed-in person's token as needed. Any
    mcp_servers-aware agent component references it via `server_resource_key`. See the module
    docstring for the `<tool_name>(arguments: dict)` interface shape and why."""

    resource_key: str = Field(default="mcp", description="Dagster resource key other components reference via server_resource_key.")
    url: str = Field(description="The hosted MCP server's URL (e.g. 'https://mcp.example.com/mcp').")
    oauth_dir: str = Field(description="Directory holding tokens.json + client_info.json from `python -m dagster_community_components.resources.mcp_oauth_login`. Treat as a secret.")
    allow: List[str] = Field(
        default_factory=list,
        description=(
            "Tool allow-list -- default EMPTY, so no tools are exposed until you explicitly "
            "choose some (unlike notion_mcp_server's 'both tools by default': an arbitrary "
            "hosted server's tool surface is unknown ahead of time and may include destructive "
            "write/delete tools)."
        ),
    )
    max_requests_per_second: float = Field(default=2.0, description="Shared by every tool call this server process makes -- see HostedMcpRateLimiter.")
    max_retries: int = Field(default=5, description="Retries on a real HTTP 429, a soft text-based rate-limit signal, or a transient error before returning {'error': ...} to the agent.")
    soft_rate_limit_pattern: Optional[str] = Field(
        default=None,
        description=(
            "Regex matched against a tool result's TEXT to detect a server that signals rate "
            "limits as ordinary text instead of HTTP 429 (Notion's hosted server is the "
            "confirmed example -- see notion_mcp_documents). Unset: a generic default pattern "
            "('rate limit', 'too many requests', 'retry...after') is used, which may not match "
            "every server's own wording."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("allow",), csv=(), tags=())

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Hosted MCP Server', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        mcp_env: Dict[str, str] = {
            "HOSTED_MCP_URL": self.url,
            "HOSTED_MCP_OAUTH_DIR": self.oauth_dir,
            "HOSTED_MCP_MAX_REQUESTS_PER_SECOND": str(self.max_requests_per_second),
            "HOSTED_MCP_MAX_RETRIES": str(self.max_retries),
        }
        if self.allow:
            mcp_env["HOSTED_MCP_ALLOW"] = ",".join(self.allow)
        if self.soft_rate_limit_pattern:
            mcp_env["HOSTED_MCP_SOFT_RATE_LIMIT_PATTERN"] = self.soft_rate_limit_pattern

        resource = _McpServerShapeResource(
            name=self.resource_key,
            type="stdio",
            command=[sys.executable, "-m", _this_module_name()],
            env=mcp_env,
        )
        return dg.Definitions(resources={self.resource_key: resource})

    @classmethod
    def get_description(cls) -> str:
        return cls.__doc__ or ""


# ── stdio MCP server entrypoint:  python -m <this module>  ──────────────────────────────────────
def build_mcp_server(tools: "HostedMcpProxyTools"):
    """MCP server (official `mcp` package, FastMCP/MCPServer) exposing one `<name>(arguments:
    dict)` tool per name in `tools.allow` -- see module docstring for why this shape, not a
    schema-matching proxy. Works on mcp 1.x and 2.x."""
    try:  # mcp 2.x renamed FastMCP -> MCPServer (mcp.server.fastmcp now raises ModuleNotFoundError)
        from mcp.server.mcpserver import MCPServer as _Server
    except ImportError:  # mcp 1.x
        from mcp.server.fastmcp import FastMCP as _Server

    mcp = _Server("hosted-mcp-proxy")

    def _make_proxy_tool(tool_name: str):
        def _proxy(arguments: Optional[Dict[str, Any]] = None) -> dict:
            return tools.call(tool_name, arguments)
        _proxy.__name__ = tool_name
        _proxy.__doc__ = (
            f"Proxies the upstream MCP tool {tool_name!r}. Call with `arguments` set to a dict "
            f"matching that tool's own real parameters (e.g. arguments={{'id': '...'}}) -- this "
            f"wrapper does not itself validate them, the upstream server does."
        )
        return _proxy

    for name in tools.tool_names():
        mcp.tool(name=name)(_make_proxy_tool(name))

    return mcp


def main(argv: Optional[List[str]] = None) -> None:
    """Env: HOSTED_MCP_URL, HOSTED_MCP_OAUTH_DIR (required), HOSTED_MCP_ALLOW (csv),
    _MAX_REQUESTS_PER_SECOND, _MAX_RETRIES, _SOFT_RATE_LIMIT_PATTERN."""
    import logging
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(message)s")  # stdout is the MCP channel
    mcp_url = os.environ.get("HOSTED_MCP_URL")
    oauth_dir = os.environ.get("HOSTED_MCP_OAUTH_DIR")
    if not mcp_url or not oauth_dir:
        raise SystemExit("HOSTED_MCP_URL and HOSTED_MCP_OAUTH_DIR must both be set")
    allow_env = os.environ.get("HOSTED_MCP_ALLOW")
    allow = [t.strip() for t in allow_env.split(",") if t.strip()] if allow_env else []

    client_info = _hmo_read_json(_hmo_client_info_path(oauth_dir)) or {}
    token_endpoint = client_info.get("token_endpoint")
    if not token_endpoint:
        raise SystemExit(
            f"No token_endpoint found under oauth_dir={oauth_dir!r}. Sign in first: python -m "
            f"dagster_community_components.resources.mcp_oauth_login --url {mcp_url!r} "
            f"--oauth-dir {oauth_dir!r}"
        )

    tools = HostedMcpProxyTools(
        mcp_url, oauth_dir, token_endpoint,
        allow=allow,
        max_requests_per_second=float(os.environ.get("HOSTED_MCP_MAX_REQUESTS_PER_SECOND", "2.0")),
        max_retries=int(os.environ.get("HOSTED_MCP_MAX_RETRIES", "5")),
        soft_rate_limit_pattern=os.environ.get("HOSTED_MCP_SOFT_RATE_LIMIT_PATTERN"),
    )
    build_mcp_server(tools).run(transport="stdio")


if __name__ == "__main__":
    main()
