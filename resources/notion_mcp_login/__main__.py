"""Sign-in CLI for Notion's hosted MCP server (notion_mcp_documents / notion_mcp_server).

    python -m dagster_community_components.resources.notion_mcp_login --oauth-dir <dir>

Runs the OAuth Authorization Code + PKCE flow once (the user approves in their browser), writes
tokens.json + client_info.json to --oauth-dir (mode 0600, via the SAME locked-write helper
notion_mcp_documents/notion_mcp_server use to read/refresh them later), and prints which Notion
workspace + user it signed in as.

Endpoint discovery -- called out explicitly, not guessed: the token endpoint
(`https://mcp.notion.com/token`) was confirmed directly against a live server. The
AUTHORIZATION and DYNAMIC-CLIENT-REGISTRATION endpoints were NOT independently hardcoded here --
this CLI discovers them via OAuth 2.0 Authorization Server Metadata
(`{server}/.well-known/oauth-authorization-server`, RFC 8414), which the Model Context Protocol's
own authorization specification requires every compliant MCP server to publish for exactly this
reason (so a client never has to hardcode per-server endpoint paths). If Notion's hosted server
ever stops publishing that document at the standard path, this CLI fails with a clear message
rather than silently hitting a wrong, hardcoded URL.

This reads Notion as the SIGNED-IN PERSON -- it sees everything they can see in their workspace,
not a scoped service-account view. Prefer `notion_documents` with a real integration token once a
workspace admin can create one for you; use this only when that isn't available yet. --oauth-dir
is a secret (tokens.json's refresh token grants that same access) and should have exactly one
writer -- concurrent sign-ins to the same directory are not supported.
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
from typing import Any, Dict, Optional, Tuple

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
    convention rather than cross-importing)."""
    headers = {"Authorization": f"Bearer {access_token}"}
    from mcp import ClientSession
    try:
        from mcp.client.streamable_http import streamable_http_client
        from mcp.shared._httpx_utils import create_mcp_http_client
        http_client = await stack.enter_async_context(create_mcp_http_client(headers=headers))
        streams = await stack.enter_async_context(streamable_http_client(NOTION_MCP_URL, http_client=http_client))
    except ImportError:
        from mcp.client.streamable_http import streamablehttp_client
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


_MCP_ORIGIN = "https://mcp.notion.com"
_METADATA_URL = f"{_MCP_ORIGIN}/.well-known/oauth-authorization-server"
_DEFAULT_REDIRECT_PORT = 8765


def generate_pkce_pair() -> Tuple[str, str]:
    """(code_verifier, code_challenge) per RFC 7636 -- a 64-byte URL-safe random verifier and its
    S256 challenge."""
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(64)).rstrip(b"=").decode("ascii")
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest()).rstrip(b"=").decode("ascii")
    return verifier, challenge


def fetch_server_metadata() -> Dict[str, Any]:
    """OAuth 2.0 Authorization Server Metadata (RFC 8414) -- see module docstring for why this is
    discovered rather than hardcoded."""
    req = urllib.request.Request(_METADATA_URL, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        raise RuntimeError(
            f"Could not fetch OAuth server metadata from {_METADATA_URL}: {e}. Notion's hosted "
            "MCP server may have moved this; check https://developers.notion.com for the current "
            "sign-in flow."
        ) from e


def register_client(registration_endpoint: str, redirect_uri: str) -> Dict[str, Any]:
    """Dynamic Client Registration (RFC 7591) -- public client (no stored secret required to be
    kept confidential; `token_endpoint_auth_method: none` per PKCE's own public-client model).
    Returns the full registration response (at minimum client_id; client_secret if issued)."""
    body = json.dumps({
        "client_name": "dagster-community-components notion_mcp_login",
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


def build_authorization_url(authorization_endpoint: str, client_id: str, redirect_uri: str, code_challenge: str, state: str) -> str:
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "state": state,
    }
    return f"{authorization_endpoint}?{urllib.parse.urlencode(params)}"


def exchange_code_for_tokens(token_endpoint: str, code: str, redirect_uri: str, client_id: str, code_verifier: str) -> Dict[str, Any]:
    body = urllib.parse.urlencode({
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
        "code_verifier": code_verifier,
    }).encode("utf-8")
    req = urllib.request.Request(
        token_endpoint, data=body, method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


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
    print(f"Opening your browser to sign in to Notion...\n{authorization_url}\n")
    webbrowser.open(authorization_url)
    if not result.event.wait(timeout_seconds):
        result.error = f"Timed out after {timeout_seconds}s waiting for the browser redirect."
    return result


def sign_in(oauth_dir: str, redirect_port: int = _DEFAULT_REDIRECT_PORT, open_browser=run_interactive_authorization) -> Dict[str, Any]:
    """Full flow: discover metadata, register a client (reusing one already saved under
    oauth_dir, if present), PKCE + browser consent, exchange the code, persist tokens, confirm
    identity via notion-get-self. Returns the notion-get-self result for the CLI to print."""
    metadata = fetch_server_metadata()
    redirect_uri = f"http://localhost:{redirect_port}/callback"

    os.makedirs(os.path.expanduser(oauth_dir), exist_ok=True)
    client_info = _nmo_read_json(_nmo_client_info_path(oauth_dir))
    if not client_info:
        client_info = register_client(metadata["registration_endpoint"], redirect_uri)
        lock_path = _nmo_client_info_path(oauth_dir) + ".lock"
        lock_fd = _nmo_os.open(lock_path, _nmo_os.O_CREAT | _nmo_os.O_RDWR, 0o600)
        try:
            _nmo_fcntl.flock(lock_fd, _nmo_fcntl.LOCK_EX)
            _nmo_atomic_write(_nmo_client_info_path(oauth_dir), client_info)
        finally:
            _nmo_fcntl.flock(lock_fd, _nmo_fcntl.LOCK_UN)
            _nmo_os.close(lock_fd)

    verifier, challenge = generate_pkce_pair()
    state = secrets.token_urlsafe(24)
    auth_url = build_authorization_url(metadata["authorization_endpoint"], client_info["client_id"], redirect_uri, challenge, state)

    result = open_browser(auth_url, redirect_port)
    if result.error:
        raise RuntimeError(f"Notion sign-in failed: {result.error}")

    token_data = exchange_code_for_tokens(metadata["token_endpoint"], result.code, redirect_uri, client_info["client_id"], verifier)
    tokens = {
        "access_token": token_data["access_token"],
        "refresh_token": token_data["refresh_token"],
        "expires_at": time.time() + token_data.get("expires_in", 8 * 3600),
    }
    _nmo_write_tokens(oauth_dir, tokens)

    rate_limiter = NotionMcpRateLimiter(max_requests_per_second=2.0)
    whoami_text = _nmo_call_tool(oauth_dir, "notion-get-self", {}, rate_limiter)
    return {"whoami": whoami_text}


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--oauth-dir", required=True, help="Directory to write tokens.json + client_info.json into (mode 0600). Treat as a secret.")
    parser.add_argument("--redirect-port", type=int, default=_DEFAULT_REDIRECT_PORT, help=f"Local port for the OAuth redirect callback (default {_DEFAULT_REDIRECT_PORT}).")
    args = parser.parse_args(argv)

    oauth_dir = os.path.expanduser(args.oauth_dir)
    os.makedirs(oauth_dir, exist_ok=True)
    try:
        info = sign_in(oauth_dir, redirect_port=args.redirect_port)
    except Exception as e:
        print(f"Sign-in failed: {e}", file=sys.stderr)
        raise SystemExit(1)

    print(f"Signed in. oauth_dir={oauth_dir!r}")
    print(info["whoami"])
    print("\nSign in again (to switch workspace, or if the refresh token is ever rejected) by re-running this command.")


if __name__ == "__main__":
    main()
