"""Real, committed tests for the general mcp_oauth_login sign-in CLI.

Mocks the external calls (metadata discovery / client registration / token exchange / tool
calls) and `webbrowser.open`; the local OAuth-redirect HTTP server itself runs for real.
"""
import json
import threading
import time
import urllib.request

import pytest


# --- _well_known_url (RFC 9728 / RFC 8414 shared path-insertion rule) --------------------------

def test_well_known_url_inserts_segment_between_host_and_path(mod):
    assert mod._well_known_url("https://resource.example.com/resource1", "oauth-protected-resource") == \
        "https://resource.example.com/.well-known/oauth-protected-resource/resource1"


def test_well_known_url_with_no_path_has_no_trailing_segment(mod):
    assert mod._well_known_url("https://resource.example.com", "oauth-protected-resource") == \
        "https://resource.example.com/.well-known/oauth-protected-resource"


def test_well_known_url_strips_trailing_slash_before_inserting(mod):
    assert mod._well_known_url("https://resource.example.com/mcp/", "oauth-authorization-server") == \
        "https://resource.example.com/.well-known/oauth-authorization-server/mcp"


# --- PKCE ---------------------------------------------------------------------------------------

def test_generate_pkce_pair_shape_and_uniqueness(mod):
    v1, c1 = mod.generate_pkce_pair()
    v2, c2 = mod.generate_pkce_pair()
    assert v1 != v2 and c1 != c2
    assert 43 <= len(v1) <= 128
    assert "=" not in v1 and "=" not in c1


# --- find_whoami_tool -----------------------------------------------------------------------

def test_find_whoami_tool_exact_match_wins_over_substring(mod):
    # "notion-search" contains no keyword; "get_self" is an exact match.
    assert mod.find_whoami_tool(["notion-search", "get_self"]) == "get_self"


def test_find_whoami_tool_substring_fallback(mod):
    assert mod.find_whoami_tool(["list_issues", "viewer"]) == "viewer"


def test_find_whoami_tool_returns_none_when_nothing_matches(mod):
    assert mod.find_whoami_tool(["notion-search", "notion-fetch"]) is None


def test_find_whoami_tool_priority_order(mod):
    # "whoami" is earlier in _WHOAMI_KEYWORDS than "me" -- an exact "whoami" wins even if a
    # looser "me"-substring tool appears first in the list.
    assert mod.find_whoami_tool(["remember_this", "whoami"]) == "whoami"


# --- discover_oauth_metadata, layered fallback ---------------------------------------------------

def test_discover_via_rfc9728_then_rfc8414_on_auth_server(mod, monkeypatch):
    calls = []

    def fake_fetch(url):
        calls.append(url)
        if url.endswith("/.well-known/oauth-protected-resource/mcp"):
            return {"authorization_servers": ["https://auth.example.com"]}
        if url == "https://auth.example.com/.well-known/oauth-authorization-server":
            return {"token_endpoint": "https://auth.example.com/token"}
        return None

    monkeypatch.setattr(mod, "_fetch_json", fake_fetch)
    meta = mod.discover_oauth_metadata("https://mcp.example.com/mcp")
    assert meta["token_endpoint"] == "https://auth.example.com/token"


def test_discover_falls_back_to_path_inserted_rfc8414_direct(mod, monkeypatch):
    def fake_fetch(url):
        if "oauth-protected-resource" in url:
            return None  # not published
        if url == "https://mcp.example.com/.well-known/oauth-authorization-server/mcp":
            return {"token_endpoint": "direct-path-inserted"}
        return None

    monkeypatch.setattr(mod, "_fetch_json", fake_fetch)
    meta = mod.discover_oauth_metadata("https://mcp.example.com/mcp")
    assert meta["token_endpoint"] == "direct-path-inserted"


def test_discover_falls_back_to_bare_origin_root(mod, monkeypatch):
    def fake_fetch(url):
        if url == "https://mcp.example.com/.well-known/oauth-authorization-server":
            return {"token_endpoint": "bare-origin"}
        return None

    monkeypatch.setattr(mod, "_fetch_json", fake_fetch)
    meta = mod.discover_oauth_metadata("https://mcp.example.com/mcp")
    assert meta["token_endpoint"] == "bare-origin"


def test_discover_raises_clear_error_when_nothing_found(mod, monkeypatch):
    monkeypatch.setattr(mod, "_fetch_json", lambda url: None)
    with pytest.raises(RuntimeError, match="Could not discover OAuth metadata"):
        mod.discover_oauth_metadata("https://mcp.example.com/mcp")


# --- client registration / authorization url / token exchange (RFC 8707 resource param) --------

class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return json.dumps(self._payload).encode()


def test_register_client_posts_public_client_shape(mod, monkeypatch):
    captured = {}

    def fake_urlopen(req, timeout=30):
        captured["body"] = json.loads(req.data.decode())
        captured["url"] = req.full_url
        return _FakeResp({"client_id": "newclient"})

    monkeypatch.setattr(mod.urllib.request, "urlopen", fake_urlopen)
    result = mod.register_client("https://auth.example.com/register", "http://localhost:8765/callback", "https://mcp.example.com/mcp")
    assert result["client_id"] == "newclient"
    assert captured["body"]["token_endpoint_auth_method"] == "none"
    assert captured["body"]["redirect_uris"] == ["http://localhost:8765/callback"]


def test_build_authorization_url_contains_pkce_and_resource_params(mod):
    url = mod.build_authorization_url("https://auth.example.com/authorize", "cid", "http://localhost:8765/callback", "chal", "state1", "https://mcp.example.com/mcp")
    assert "code_challenge=chal" in url
    assert "code_challenge_method=S256" in url
    assert "resource=https%3A%2F%2Fmcp.example.com%2Fmcp" in url


def test_exchange_code_for_tokens_posts_resource_and_optional_secret(mod, monkeypatch):
    captured = {}

    def fake_urlopen(req, timeout=30):
        captured["body"] = req.data.decode()
        return _FakeResp({"access_token": "at", "refresh_token": "rt", "expires_in": 3600})

    monkeypatch.setattr(mod.urllib.request, "urlopen", fake_urlopen)
    result = mod.exchange_code_for_tokens("https://auth.example.com/token", "code1", "http://localhost:8765/callback", "cid", "verifier1", "https://mcp.example.com/mcp")
    assert result["access_token"] == "at"
    assert "resource=https%3A%2F%2Fmcp.example.com%2Fmcp" in captured["body"]
    assert "client_secret" not in captured["body"]

    result2 = mod.exchange_code_for_tokens("https://auth.example.com/token", "code1", "http://localhost:8765/callback", "cid", "verifier1", "https://mcp.example.com/mcp", client_secret="shh")
    assert "client_secret=shh" in captured["body"]


# --- token refresh / rotation --------------------------------------------------------------

class _FakeTokenResp:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


def test_cached_token_short_circuits_refresh(mod, tmp_path, monkeypatch):
    oauth_dir = str(tmp_path)
    mod._hmo_atomic_write(mod._hmo_tokens_path(oauth_dir), {"access_token": "cached", "refresh_token": "r0", "expires_at": time.time() + 3600})
    calls = []
    monkeypatch.setattr("requests.post", lambda *a, **k: calls.append((a, k)) or _FakeTokenResp())
    assert mod._hmo_get_access_token(oauth_dir, "https://auth.example.com/token") == "cached"
    assert calls == []


def test_expired_token_refreshes_and_rotates(mod, tmp_path, monkeypatch):
    oauth_dir = str(tmp_path)
    tokens_path = mod._hmo_tokens_path(oauth_dir)
    mod._hmo_atomic_write(tokens_path, {"access_token": "old", "refresh_token": "r1", "expires_at": time.time() - 10})
    mod._hmo_atomic_write(mod._hmo_client_info_path(oauth_dir), {"client_id": "cid"})

    def fake_post(url, data=None, headers=None, timeout=None):
        assert url == "https://auth.example.com/token"
        return _FakeTokenResp(payload={"access_token": "new", "refresh_token": "r2", "expires_in": 3600})

    monkeypatch.setattr("requests.post", fake_post)
    token = mod._hmo_get_access_token(oauth_dir, "https://auth.example.com/token")
    assert token == "new"
    assert json.loads(open(tokens_path).read())["refresh_token"] == "r2"


def test_missing_tokens_raises_auth_error(mod, tmp_path):
    with pytest.raises(mod.HostedMcpAuthError, match="Sign in first"):
        mod._hmo_get_access_token(str(tmp_path), "https://auth.example.com/token")


def test_rejected_refresh_token_raises_auth_error(mod, tmp_path, monkeypatch):
    oauth_dir = str(tmp_path)
    mod._hmo_atomic_write(mod._hmo_tokens_path(oauth_dir), {"access_token": "old", "refresh_token": "bad", "expires_at": 0})
    mod._hmo_atomic_write(mod._hmo_client_info_path(oauth_dir), {"client_id": "cid"})
    monkeypatch.setattr("requests.post", lambda *a, **k: _FakeTokenResp(status_code=401))
    with pytest.raises(mod.HostedMcpAuthError, match="Sign in again"):
        mod._hmo_get_access_token(oauth_dir, "https://auth.example.com/token")


# --- _hmo_call_tool: HTTP 429 / soft text rate limits / generic errors --------------------------

def test_call_tool_retries_on_http_429_like_exception(mod, monkeypatch):
    monkeypatch.setattr(mod._hmo_time, "sleep", lambda s: None)
    attempts = {"n": 0}

    async def fake_async(mcp_url, oauth_dir, token_endpoint, tool_name, arguments, timeout_seconds=60.0):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("HTTP 429 Too Many Requests")
        return "real content"

    monkeypatch.setattr(mod, "_hmo_call_tool_async", fake_async)
    rl = mod.HostedMcpRateLimiter(max_requests_per_second=1000)
    hits = []
    text = mod._hmo_call_tool("https://mcp.example.com/mcp", "dir", "https://auth.example.com/token", "search", {}, rl, on_rate_limit=hits.append)
    assert text == "real content"
    assert hits


def test_call_tool_retries_on_soft_rate_limit_text(mod, monkeypatch):
    monkeypatch.setattr(mod._hmo_time, "sleep", lambda s: None)
    responses = iter(["Rate limit exceeded, please retry after 2 seconds", "real content"])

    async def fake_async(mcp_url, oauth_dir, token_endpoint, tool_name, arguments, timeout_seconds=60.0):
        return next(responses)

    monkeypatch.setattr(mod, "_hmo_call_tool_async", fake_async)
    rl = mod.HostedMcpRateLimiter(max_requests_per_second=1000)
    text = mod._hmo_call_tool("https://mcp.example.com/mcp", "dir", "https://auth.example.com/token", "search", {}, rl)
    assert text == "real content"


def test_call_tool_raises_rate_limited_after_max_retries(mod, monkeypatch):
    monkeypatch.setattr(mod._hmo_time, "sleep", lambda s: None)

    async def fake_async(mcp_url, oauth_dir, token_endpoint, tool_name, arguments, timeout_seconds=60.0):
        raise RuntimeError("429")

    monkeypatch.setattr(mod, "_hmo_call_tool_async", fake_async)
    rl = mod.HostedMcpRateLimiter(max_requests_per_second=1000)
    with pytest.raises(mod.HostedMcpRateLimited):
        mod._hmo_call_tool("https://mcp.example.com/mcp", "dir", "https://auth.example.com/token", "search", {}, rl, max_retries=2)


def test_call_tool_generic_error_retries_then_raises_runtime_error(mod, monkeypatch):
    monkeypatch.setattr(mod._hmo_time, "sleep", lambda s: None)

    async def fake_async(mcp_url, oauth_dir, token_endpoint, tool_name, arguments, timeout_seconds=60.0):
        raise ConnectionError("boom")

    monkeypatch.setattr(mod, "_hmo_call_tool_async", fake_async)
    rl = mod.HostedMcpRateLimiter(max_requests_per_second=1000)
    with pytest.raises(RuntimeError, match="failed after 2 retries"):
        mod._hmo_call_tool("https://mcp.example.com/mcp", "dir", "https://auth.example.com/token", "search", {}, rl, max_retries=2)


def test_call_tool_auth_error_is_not_retried(mod, monkeypatch):
    calls = {"n": 0}

    async def fake_async(mcp_url, oauth_dir, token_endpoint, tool_name, arguments, timeout_seconds=60.0):
        calls["n"] += 1
        raise mod.HostedMcpAuthError("sign in again")

    monkeypatch.setattr(mod, "_hmo_call_tool_async", fake_async)
    rl = mod.HostedMcpRateLimiter(max_requests_per_second=1000)
    with pytest.raises(mod.HostedMcpAuthError):
        mod._hmo_call_tool("https://mcp.example.com/mcp", "dir", "https://auth.example.com/token", "search", {}, rl, max_retries=5)
    assert calls["n"] == 1


# --- real local redirect server ------------------------------------------------------------

def test_callback_server_captures_code_on_matching_state(mod):
    result = mod._CallbackResult()
    handler_cls = mod._make_callback_handler("expected-state", result)
    server = mod.http.server.HTTPServer(("localhost", 0), handler_cls)
    port = server.server_address[1]
    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()
    time.sleep(0.05)
    urllib.request.urlopen(f"http://localhost:{port}/callback?code=realcode&state=expected-state", timeout=5)
    thread.join(timeout=5)
    assert result.code == "realcode"
    assert result.error is None


def test_callback_server_rejects_state_mismatch(mod):
    result = mod._CallbackResult()
    handler_cls = mod._make_callback_handler("expected-state", result)
    server = mod.http.server.HTTPServer(("localhost", 0), handler_cls)
    port = server.server_address[1]
    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()
    time.sleep(0.05)
    urllib.request.urlopen(f"http://localhost:{port}/callback?code=realcode&state=WRONG", timeout=5)
    thread.join(timeout=5)
    assert result.code is None
    assert "mismatch" in result.error


# --- full sign_in() flow --------------------------------------------------------------------

_METADATA = {
    "authorization_endpoint": "https://auth.example.com/authorize",
    "token_endpoint": "https://auth.example.com/token",
    "registration_endpoint": "https://auth.example.com/register",
}


def _fake_open_browser(mod, code="fake-code"):
    def _open(auth_url, redirect_port):
        result = mod._CallbackResult()
        result.code = code
        return result
    return _open


def test_sign_in_uses_dcr_when_no_client_id_and_no_existing_client_info(mod, monkeypatch, tmp_path):
    oauth_dir = str(tmp_path)
    monkeypatch.setattr(mod, "discover_oauth_metadata", lambda url: dict(_METADATA))
    register_calls = []
    monkeypatch.setattr(mod, "register_client", lambda endpoint, redirect_uri, mcp_url: register_calls.append(1) or {"client_id": "dcr-client"})
    monkeypatch.setattr(mod, "exchange_code_for_tokens", lambda *a, **k: {"access_token": "AT", "refresh_token": "RT", "expires_in": 3600})

    async def fake_list_tools(mcp_url, oauth_dir_, token_endpoint, timeout_seconds=60.0):
        return ["get_self"]

    async def fake_call_tool_async(mcp_url, oauth_dir_, token_endpoint, tool_name, arguments, timeout_seconds=60.0):
        return "signed in as person@example.com"

    monkeypatch.setattr(mod, "_hmo_list_tools_async", fake_list_tools)
    monkeypatch.setattr(mod, "_hmo_call_tool_async", fake_call_tool_async)

    info = mod.sign_in("https://mcp.example.com/mcp", oauth_dir, open_browser=_fake_open_browser(mod))
    assert register_calls == [1]
    assert "signed in as" in info["whoami"]
    client_info = json.loads(open(mod._hmo_client_info_path(oauth_dir)).read())
    assert client_info["client_id"] == "dcr-client"


def test_sign_in_reuses_existing_client_info(mod, monkeypatch, tmp_path):
    oauth_dir = str(tmp_path)
    mod._hmo_atomic_write(mod._hmo_client_info_path(oauth_dir), {"client_id": "existing"})
    monkeypatch.setattr(mod, "discover_oauth_metadata", lambda url: dict(_METADATA))
    register_calls = []
    monkeypatch.setattr(mod, "register_client", lambda *a, **k: register_calls.append(1) or {"client_id": "SHOULD_NOT_BE_USED"})
    monkeypatch.setattr(mod, "exchange_code_for_tokens", lambda *a, **k: {"access_token": "AT", "refresh_token": "RT", "expires_in": 3600})

    async def fake_list_tools(mcp_url, oauth_dir_, token_endpoint, timeout_seconds=60.0):
        return []

    monkeypatch.setattr(mod, "_hmo_list_tools_async", fake_list_tools)

    mod.sign_in("https://mcp.example.com/mcp", oauth_dir, open_browser=_fake_open_browser(mod))
    assert register_calls == []


def test_sign_in_with_explicit_client_id_skips_dcr_and_persists_it(mod, monkeypatch, tmp_path):
    oauth_dir = str(tmp_path)
    monkeypatch.setattr(mod, "discover_oauth_metadata", lambda url: {**_METADATA, "registration_endpoint": None})
    register_calls = []
    monkeypatch.setattr(mod, "register_client", lambda *a, **k: register_calls.append(1))
    monkeypatch.setattr(mod, "exchange_code_for_tokens", lambda *a, **k: {"access_token": "AT", "refresh_token": "RT", "expires_in": 3600})

    async def fake_list_tools(mcp_url, oauth_dir_, token_endpoint, timeout_seconds=60.0):
        return []

    monkeypatch.setattr(mod, "_hmo_list_tools_async", fake_list_tools)

    mod.sign_in("https://mcp.example.com/mcp", oauth_dir, client_id="explicit-cid", client_secret="sshh", open_browser=_fake_open_browser(mod))
    assert register_calls == []
    client_info = json.loads(open(mod._hmo_client_info_path(oauth_dir)).read())
    assert client_info["client_id"] == "explicit-cid"
    assert client_info["client_secret"] == "sshh"


# --- sign_in with --state-key (token_store='state') ---------------------------------------

def test_sign_in_requires_exactly_one_of_oauth_dir_or_state_key(mod, tmp_path):
    with pytest.raises(ValueError, match="exactly one"):
        mod.sign_in("https://mcp.example.com/mcp")
    with pytest.raises(ValueError, match="exactly one"):
        mod.sign_in("https://mcp.example.com/mcp", str(tmp_path), state_key="linear")


def test_sign_in_with_state_key_writes_to_state_not_a_folder(mod, monkeypatch, tmp_path):
    import dagster._core.storage.defs_state.base as defs_state_base

    class _FakeStorage:
        def __init__(self):
            self.uploaded = None

        def upload_state_from_path(self, key, version, path):
            import json
            self.uploaded = (key, json.loads(path.read_text()))

    fake_storage = _FakeStorage()
    monkeypatch.setattr(defs_state_base.DefsStateStorage, "get", classmethod(lambda cls: fake_storage))
    monkeypatch.setattr(mod, "discover_oauth_metadata", lambda url: dict(_METADATA))
    monkeypatch.setattr(mod, "register_client", lambda *a, **k: {"client_id": "cid"})
    monkeypatch.setattr(mod, "exchange_code_for_tokens", lambda *a, **k: {"access_token": "AT", "refresh_token": "RT", "expires_in": 3600})

    async def fake_list_tools(mcp_url, oauth_dir_, token_endpoint, timeout_seconds=60.0):
        return []

    monkeypatch.setattr(mod, "_hmo_list_tools_async", fake_list_tools)

    mod.sign_in("https://mcp.example.com/mcp", state_key="linear", open_browser=_fake_open_browser(mod))
    key, blob = fake_storage.uploaded
    assert key == "linear"
    assert blob["access_token"] == "AT"
    assert blob["client_id"] == "cid"
    assert blob["token_endpoint"] == _METADATA["token_endpoint"]


def test_main_state_key_and_oauth_dir_are_mutually_exclusive(mod, capsys):
    with pytest.raises(SystemExit) as exc_info:
        mod.main(["--url", "https://mcp.example.com/mcp"])
    assert exc_info.value.code == 1
    assert "exactly one" in capsys.readouterr().err


def test_main_with_state_key_calls_sign_in_with_state_key(mod, monkeypatch):
    captured = {}

    def fake_sign_in(url, oauth_dir, **k):
        captured["oauth_dir"] = oauth_dir
        captured.update(k)
        return {"whoami": None, "metadata": {}}

    monkeypatch.setattr(mod, "sign_in", fake_sign_in)
    mod.main(["--url", "https://mcp.example.com/mcp", "--state-key", "linear"])
    assert captured["oauth_dir"] is None
    assert captured["state_key"] == "linear"


def test_sign_in_no_dcr_and_no_client_id_raises_clear_error(mod, monkeypatch, tmp_path):
    oauth_dir = str(tmp_path)
    monkeypatch.setattr(mod, "discover_oauth_metadata", lambda url: {**_METADATA, "registration_endpoint": None})
    with pytest.raises(RuntimeError, match="no dynamic client registration"):
        mod.sign_in("https://mcp.example.com/mcp", oauth_dir)


def test_sign_in_whoami_absent_returns_none(mod, monkeypatch, tmp_path):
    oauth_dir = str(tmp_path)
    monkeypatch.setattr(mod, "discover_oauth_metadata", lambda url: dict(_METADATA))
    monkeypatch.setattr(mod, "register_client", lambda *a, **k: {"client_id": "cid"})
    monkeypatch.setattr(mod, "exchange_code_for_tokens", lambda *a, **k: {"access_token": "AT", "refresh_token": "RT", "expires_in": 3600})

    async def fake_list_tools(mcp_url, oauth_dir_, token_endpoint, timeout_seconds=60.0):
        return ["notion-search", "notion-fetch"]

    monkeypatch.setattr(mod, "_hmo_list_tools_async", fake_list_tools)
    info = mod.sign_in("https://mcp.example.com/mcp", oauth_dir, open_browser=_fake_open_browser(mod))
    assert info["whoami"] is None


def test_sign_in_raises_clearly_on_browser_error(mod, monkeypatch, tmp_path):
    oauth_dir = str(tmp_path)
    monkeypatch.setattr(mod, "discover_oauth_metadata", lambda url: dict(_METADATA))
    monkeypatch.setattr(mod, "register_client", lambda *a, **k: {"client_id": "cid"})

    def fake_open_browser(auth_url, redirect_port):
        result = mod._CallbackResult()
        result.error = "user declined"
        return result

    with pytest.raises(RuntimeError, match="user declined"):
        mod.sign_in("https://mcp.example.com/mcp", oauth_dir, open_browser=fake_open_browser)


# --- main() -----------------------------------------------------------------------------------

def test_main_exits_nonzero_with_message_on_failure(mod, monkeypatch, tmp_path, capsys):
    def fake_sign_in(*a, **k):
        raise RuntimeError("sign-in blew up")

    monkeypatch.setattr(mod, "sign_in", fake_sign_in)
    with pytest.raises(SystemExit) as exc_info:
        mod.main(["--url", "https://mcp.example.com/mcp", "--oauth-dir", str(tmp_path)])
    assert exc_info.value.code == 1
    assert "sign-in blew up" in capsys.readouterr().err


def test_main_prints_whoami_on_success(mod, monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(mod, "sign_in", lambda url, oauth_dir, **k: {"whoami": "Signed in as X", "metadata": {}})
    mod.main(["--url", "https://mcp.example.com/mcp", "--oauth-dir", str(tmp_path)])
    out = capsys.readouterr().out
    assert "Signed in as X" in out


def test_main_resolves_client_secret_env(mod, monkeypatch, tmp_path):
    monkeypatch.setenv("MY_SECRET", "topsecret")
    captured = {}

    def fake_sign_in(url, oauth_dir, **k):
        captured.update(k)
        return {"whoami": None, "metadata": {}}

    monkeypatch.setattr(mod, "sign_in", fake_sign_in)
    mod.main(["--url", "https://mcp.example.com/mcp", "--oauth-dir", str(tmp_path), "--client-id", "cid", "--client-secret-env", "MY_SECRET"])
    assert captured["client_id"] == "cid"
    assert captured["client_secret"] == "topsecret"
