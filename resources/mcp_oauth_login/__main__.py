"""General sign-in CLI for any hosted MCP server that uses OAuth (generalizes
notion_mcp_login -- see HostedMcpServerComponent's README for why this exists alongside the
Notion-specific components rather than replacing them outright).

    python -m dagster_community_components.resources.mcp_oauth_login --url <mcp url> --oauth-dir <dir>

Runs the OAuth Authorization Code + PKCE flow once (the user approves in their browser), writes
tokens.json + client_info.json to --oauth-dir (mode 0600, via the same locked-write helpers
notion_mcp_login uses) -- HostedMcpServerComponent reads the same two files at run time.

Endpoint discovery follows the Model Context Protocol authorization spec's documented two-step
flow (confirmed against the spec + RFC text, not guessed):
  1. RFC 9728 (OAuth 2.0 Protected Resource Metadata), fetched from the MCP server's own origin
     at `/.well-known/oauth-protected-resource<path>` (the well-known segment is inserted BETWEEN
     the host and the url's own path, per RFC 9728 section 3.1 -- for `https://mcp.example.com/
     mcp` that's `https://mcp.example.com/.well-known/oauth-protected-resource/mcp`, NOT
     `.../mcp/.well-known/...`). Its `authorization_servers` field names which server(s) issue
     tokens for this resource.
  2. RFC 8414 (OAuth 2.0 Authorization Server Metadata), fetched from the FIRST listed
     authorization server using the same well-known-segment-insertion rule, at
     `/.well-known/oauth-authorization-server<path>`.
  If step 1 fails (a server that doesn't publish RFC 9728 metadata -- confirmed this is real:
  Notion's own hosted MCP server is exactly this case, see notion_mcp_login), this CLI falls
  back to trying RFC 8414 directly against the MCP server's own origin instead.

  Every authorization and token request also carries the RFC 8707 `resource` parameter (the MCP
  server's own URL), so the issued token is scoped to that one resource -- part of the same spec
  revision that made RFC 9728 discovery mandatory.

For a server with NO dynamic client registration (`registration_endpoint` absent from its
metadata) -- Salesforce is the confirmed example: it requires an admin to create an External
Client App with the `mcp_api` scope first -- pass `--client-id` (and `--client-secret-env`, the
name of an env var holding the app's client secret, if it has one) instead of relying on DCR.

This CLI tries to confirm WHO it signed in as by listing the server's tools and calling the
first one whose name looks identity-shaped (whoami/get_self/current_user/viewer/me/...) with no
arguments -- a best-effort convenience, not every server has one, and this prints a plain
"signed in" message when none matches rather than guessing.

Servers that take a STATIC token instead of OAuth (GitHub's hosted MCP server:
`Authorization: Bearer <personal access token>`; Linear's hosted MCP server:
`Authorization: Bearer <API key>` -- note this differs from Linear's own REST API, which takes
the raw key with NO `Bearer` prefix) don't need this CLI at all -- register them directly via
MCPServerResourceComponent's `headers`/`headers_env` fields instead.
"""
import argparse
import base64
import hashlib
import http.server
import json
import os
import secrets
import sys
import threading
import time
import urllib.parse
import urllib.request
import webbrowser
from typing import Any, Dict, List, Optional, Tuple

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


def _hmo_write_client_info(oauth_dir: str, data) -> None:
    """Locked single-shot write for client_info.json -- CLI-only (HostedMcpServerComponent only
    ever reads it), so not part of the shared inlined block above."""
    lock_path = _hmo_client_info_path(oauth_dir) + ".lock"
    lock_fd = _hmo_os.open(lock_path, _hmo_os.O_CREAT | _hmo_os.O_RDWR, 0o600)
    try:
        _hmo_fcntl.flock(lock_fd, _hmo_fcntl.LOCK_EX)
        _hmo_atomic_write(_hmo_client_info_path(oauth_dir), data)
    finally:
        _hmo_fcntl.flock(lock_fd, _hmo_fcntl.LOCK_UN)
        _hmo_os.close(lock_fd)


def generate_pkce_pair() -> Tuple[str, str]:
    """(code_verifier, code_challenge) per RFC 7636 -- a 64-byte URL-safe random verifier and its
    S256 challenge."""
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(64)).rstrip(b"=").decode("ascii")
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest()).rstrip(b"=").decode("ascii")
    return verifier, challenge


def _well_known_url(base_url: str, suffix: str) -> str:
    """RFC 9728 / RFC 8414's shared well-known-segment-insertion rule: the `/.well-known/<suffix>`
    segment goes BETWEEN the host and the url's own path (with any trailing slash on that path
    stripped first) -- NOT appended after it. 'https://mcp.example.com/mcp' ->
    'https://mcp.example.com/.well-known/<suffix>/mcp', not '.../mcp/.well-known/<suffix>'."""
    parsed = urllib.parse.urlparse(base_url)
    path = parsed.path.rstrip("/")
    new_path = f"/.well-known/{suffix}{path}"
    return urllib.parse.urlunparse((parsed.scheme, parsed.netloc, new_path, "", "", ""))


def _fetch_json(url: str) -> Optional[Dict[str, Any]]:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None


def discover_oauth_metadata(mcp_url: str) -> Dict[str, Any]:
    """RFC 9728 (protected resource metadata, fetched from the MCP server itself) naming the
    authorization server(s), then RFC 8414 (authorization server metadata) fetched from the
    first one -- the MCP authorization spec's own documented two-step discovery flow. Falls back
    (in order) to RFC 8414 directly against the MCP server's own origin with the RFC's own
    path-insertion rule, then to the bare origin root with NO path insertion -- some real
    servers publish their metadata at the plain origin even when their own endpoint URL has a
    path, for simplicity (this is the shape notion_mcp_login's own hardcoded lookup assumed;
    kept here as a fallback rather than the only attempt, since it isn't the RFC's own rule)."""
    resource_metadata = _fetch_json(_well_known_url(mcp_url, "oauth-protected-resource"))
    auth_servers = (resource_metadata or {}).get("authorization_servers") or []
    if auth_servers:
        auth_server_metadata = _fetch_json(_well_known_url(auth_servers[0], "oauth-authorization-server"))
        if auth_server_metadata:
            return auth_server_metadata

    path_inserted_metadata = _fetch_json(_well_known_url(mcp_url, "oauth-authorization-server"))
    if path_inserted_metadata:
        return path_inserted_metadata

    parsed = urllib.parse.urlparse(mcp_url)
    origin_root_metadata = _fetch_json(urllib.parse.urlunparse((parsed.scheme, parsed.netloc, "/.well-known/oauth-authorization-server", "", "", "")))
    if origin_root_metadata:
        return origin_root_metadata

    raise RuntimeError(
        f"Could not discover OAuth metadata for {mcp_url!r} via RFC 9728 (protected resource "
        "metadata), RFC 8414 (authorization server metadata, path-inserted or at the bare "
        "origin). Check the server's own docs for its sign-in flow."
    )


def register_client(registration_endpoint: str, redirect_uri: str, mcp_url: str) -> Dict[str, Any]:
    """Dynamic Client Registration (RFC 7591) -- public client (no stored secret required to be
    kept confidential; `token_endpoint_auth_method: none` per PKCE's own public-client model).
    Returns the full registration response (at minimum client_id; client_secret if issued)."""
    body = json.dumps({
        "client_name": "dagster-community-components mcp_oauth_login",
        "redirect_uris": [redirect_uri],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "none",
    }).encode("utf-8")
    req = urllib.request.Request(
        registration_endpoint, data=body, method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def build_authorization_url(authorization_endpoint: str, client_id: str, redirect_uri: str, code_challenge: str, state: str, resource: str) -> str:
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "state": state,
        "resource": resource,  # RFC 8707 -- scopes the issued token to this one MCP server
    }
    return f"{authorization_endpoint}?{urllib.parse.urlencode(params)}"


def exchange_code_for_tokens(token_endpoint: str, code: str, redirect_uri: str, client_id: str, code_verifier: str, resource: str, client_secret: Optional[str] = None) -> Dict[str, Any]:
    body = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
        "code_verifier": code_verifier,
        "resource": resource,
    }
    if client_secret:
        body["client_secret"] = client_secret
    req = urllib.request.Request(
        token_endpoint, data=urllib.parse.urlencode(body).encode("utf-8"), method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


_WHOAMI_KEYWORDS = ("whoami", "who_am_i", "get_self", "getself", "get_me", "getme", "current_user", "identity", "viewer", "me", "self")


def find_whoami_tool(tool_names: List[str]) -> Optional[str]:
    """Best-effort: the first tool name that looks identity-shaped, checked in priority order
    (an exact/near match like 'whoami' beats a loose substring like 'me' matching 'remember').
    Returns None if nothing matches -- not every server has one, and guessing wrong is worse
    than saying so."""
    lowered = {name: name.lower() for name in tool_names}
    for keyword in _WHOAMI_KEYWORDS:
        for name, low in lowered.items():
            if low == keyword or low.replace("-", "_") == keyword:
                return name
    for keyword in _WHOAMI_KEYWORDS:
        for name, low in lowered.items():
            if keyword in low:
                return name
    return None


class _CallbackResult:
    def __init__(self):
        self.code: Optional[str] = None
        self.error: Optional[str] = None
        self.event = threading.Event()


def _make_callback_handler(expected_state: str, result: "_CallbackResult"):
    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urllib.parse.urlparse(self.path)
            qs = urllib.parse.parse_qs(parsed.query)
            state = (qs.get("state") or [None])[0]
            if state != expected_state:
                result.error = "state mismatch -- possible CSRF, aborting"
            elif "error" in qs:
                result.error = (qs.get("error_description") or qs.get("error"))[0]
            else:
                result.code = (qs.get("code") or [None])[0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            msg = "Sign-in failed -- you can close this tab and check the terminal." if result.error else "Signed in -- you can close this tab."
            self.wfile.write(f"<html><body>{msg}</body></html>".encode("utf-8"))
            result.event.set()

        def log_message(self, *args):  # silence BaseHTTPRequestHandler's default stderr logging
            pass

    return _Handler


def run_interactive_authorization(authorization_url: str, redirect_port: int, timeout_seconds: float = 300.0) -> _CallbackResult:
    """Opens `authorization_url` in the user's browser and waits for the redirect carrying the
    authorization code on localhost:redirect_port/callback."""
    state = urllib.parse.parse_qs(urllib.parse.urlparse(authorization_url).query).get("state", [""])[0]
    result = _CallbackResult()
    server = http.server.HTTPServer(("localhost", redirect_port), _make_callback_handler(state, result))
    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()
    print(f"Opening your browser to sign in...\n{authorization_url}\n")
    webbrowser.open(authorization_url)
    if not result.event.wait(timeout_seconds):
        result.error = f"Timed out after {timeout_seconds}s waiting for the browser redirect."
    return result


def sign_in(
    mcp_url: str, oauth_dir: str, *, redirect_port: int = 8765, client_id: Optional[str] = None,
    client_secret: Optional[str] = None, open_browser=run_interactive_authorization,
) -> Dict[str, Any]:
    """Full flow: discover metadata, register a client (or reuse --client-id, or one already
    saved under oauth_dir), PKCE + browser consent, exchange the code, persist tokens, try to
    confirm identity via a best-effort who-am-I tool call. Returns {"whoami": str | None,
    "metadata": the discovered server metadata} for the CLI to print."""
    metadata = discover_oauth_metadata(mcp_url)
    redirect_uri = f"http://localhost:{redirect_port}/callback"
    os.makedirs(os.path.expanduser(oauth_dir), exist_ok=True)

    if client_id:
        client_info = {"client_id": client_id, "client_secret": client_secret}
    else:
        client_info = _hmo_read_json(_hmo_client_info_path(oauth_dir))
        if not client_info:
            if not metadata.get("registration_endpoint"):
                raise RuntimeError(
                    f"{mcp_url!r} has no registration_endpoint (no dynamic client registration) "
                    "and no --client-id was given. Create a client with this server's own admin "
                    "tooling (e.g. Salesforce's External Client App with the mcp_api scope) and "
                    "pass --client-id (and --client-secret-env if it issues a secret)."
                )
            client_info = register_client(metadata["registration_endpoint"], redirect_uri, mcp_url)

    # client_info.json also carries mcp_url/token_endpoint -- HostedMcpServerComponent's later
    # token refreshes read these back (no OAuth metadata re-discovery needed on every run).
    client_info = {**client_info, "mcp_url": mcp_url, "token_endpoint": metadata["token_endpoint"]}
    _hmo_write_client_info(oauth_dir, client_info)

    verifier, challenge = generate_pkce_pair()
    state = secrets.token_urlsafe(24)
    auth_url = build_authorization_url(metadata["authorization_endpoint"], client_info["client_id"], redirect_uri, challenge, state, mcp_url)

    result = open_browser(auth_url, redirect_port)
    if result.error:
        raise RuntimeError(f"Sign-in failed: {result.error}")

    token_data = exchange_code_for_tokens(
        metadata["token_endpoint"], result.code, redirect_uri, client_info["client_id"], verifier, mcp_url,
        client_secret=client_info.get("client_secret"),
    )
    tokens = {
        "access_token": token_data["access_token"],
        "refresh_token": token_data.get("refresh_token"),
        "expires_at": time.time() + token_data.get("expires_in", 3600),
    }
    _hmo_write_tokens(oauth_dir, tokens)

    whoami = None
    try:
        rate_limiter = HostedMcpRateLimiter(max_requests_per_second=2.0)
        tool_names = __import__("asyncio").run(_hmo_list_tools_async(mcp_url, oauth_dir, metadata["token_endpoint"]))
        whoami_tool = find_whoami_tool(tool_names)
        if whoami_tool:
            whoami = _hmo_call_tool(mcp_url, oauth_dir, metadata["token_endpoint"], whoami_tool, {}, rate_limiter)
    except Exception as e:  # noqa: BLE001 -- identity confirmation is a courtesy, never fatal to sign-in
        whoami = f"(signed in, but could not confirm identity: {e})"

    return {"whoami": whoami, "metadata": metadata}


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", required=True, help="The hosted MCP server's URL (e.g. 'https://mcp.example.com/mcp').")
    parser.add_argument("--oauth-dir", required=True, help="Directory to write tokens.json + client_info.json into (mode 0600). Treat as a secret.")
    parser.add_argument("--redirect-port", type=int, default=8765, help="Local port for the OAuth redirect callback (default 8765).")
    parser.add_argument("--client-id", default=None, help="Use this client id instead of dynamic client registration -- required for a server with no registration_endpoint (e.g. Salesforce's External Client App).")
    parser.add_argument("--client-secret-env", default=None, help="Name of an env var holding --client-id's client secret, if it has one.")
    args = parser.parse_args(argv)

    oauth_dir = os.path.expanduser(args.oauth_dir)
    os.makedirs(oauth_dir, exist_ok=True)
    client_secret = os.environ.get(args.client_secret_env) if args.client_secret_env else None
    try:
        info = sign_in(args.url, oauth_dir, redirect_port=args.redirect_port, client_id=args.client_id, client_secret=client_secret)
    except Exception as e:
        print(f"Sign-in failed: {e}", file=sys.stderr)
        raise SystemExit(1)

    print(f"Signed in to {args.url!r}. oauth_dir={oauth_dir!r}")
    if info["whoami"]:
        print(info["whoami"])
    else:
        print("(no who-am-I tool found on this server to confirm identity)")
    print("\nSign in again (to switch accounts, or if the refresh token is ever rejected) by re-running this command.")


if __name__ == "__main__":
    main()
