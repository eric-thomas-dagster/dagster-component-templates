"""Real, committed tests for the Notion MCP sign-in CLI.

Mocks the external calls (metadata discovery / client registration / token exchange / the
notion-get-self confirmation) and `webbrowser.open` (never actually pop a browser in CI); the
local OAuth-redirect HTTP server itself runs for real, exercised with a real HTTP request.
"""
import json
import threading
import time
import urllib.request

import pytest


def test_generate_pkce_pair_shape_and_uniqueness(mod):
    v1, c1 = mod.generate_pkce_pair()
    v2, c2 = mod.generate_pkce_pair()
    assert v1 != v2 and c1 != c2
    assert 43 <= len(v1) <= 128  # RFC 7636 code_verifier length bounds
    assert "=" not in v1 and "=" not in c1  # base64url, no padding


def test_build_authorization_url_contains_pkce_params(mod):
    url = mod.build_authorization_url("https://mcp.notion.com/authorize", "cid123", "http://localhost:8765/callback", "chal", "state1")
    assert url.startswith("https://mcp.notion.com/authorize?")
    assert "client_id=cid123" in url
    assert "code_challenge=chal" in url
    assert "code_challenge_method=S256" in url
    assert "state=state1" in url


def test_fetch_server_metadata_parses_json(mod, monkeypatch):
    class _FakeResp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return json.dumps({"token_endpoint": "https://mcp.notion.com/token", "authorization_endpoint": "a", "registration_endpoint": "r"}).encode()

    monkeypatch.setattr(mod.urllib.request, "urlopen", lambda req, timeout=30: _FakeResp())
    meta = mod.fetch_server_metadata()
    assert meta["token_endpoint"] == "https://mcp.notion.com/token"


def test_fetch_server_metadata_raises_clear_error_on_failure(mod, monkeypatch):
    def fail(req, timeout=30):
        raise OSError("connection refused")

    monkeypatch.setattr(mod.urllib.request, "urlopen", fail)
    with pytest.raises(RuntimeError, match="Could not fetch OAuth server metadata"):
        mod.fetch_server_metadata()


def test_register_client_posts_public_client_shape(mod, monkeypatch):
    captured = {}

    class _FakeResp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return json.dumps({"client_id": "newclient"}).encode()

    def fake_urlopen(req, timeout=30):
        captured["body"] = json.loads(req.data.decode())
        captured["url"] = req.full_url
        return _FakeResp()

    monkeypatch.setattr(mod.urllib.request, "urlopen", fake_urlopen)
    result = mod.register_client("https://mcp.notion.com/register", "http://localhost:8765/callback")
    assert result["client_id"] == "newclient"
    assert captured["body"]["token_endpoint_auth_method"] == "none"
    assert captured["body"]["redirect_uris"] == ["http://localhost:8765/callback"]
    assert captured["url"] == "https://mcp.notion.com/register"


def test_exchange_code_for_tokens_posts_pkce_verifier(mod, monkeypatch):
    captured = {}

    class _FakeResp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return json.dumps({"access_token": "at", "refresh_token": "rt", "expires_in": 28800}).encode()

    def fake_urlopen(req, timeout=30):
        captured["body"] = req.data.decode()
        return _FakeResp()

    monkeypatch.setattr(mod.urllib.request, "urlopen", fake_urlopen)
    result = mod.exchange_code_for_tokens("https://mcp.notion.com/token", "codeXYZ", "http://localhost:8765/callback", "cid", "verifierABC")
    assert result["access_token"] == "at"
    assert "code=codeXYZ" in captured["body"]
    assert "code_verifier=verifierABC" in captured["body"]


# --- the real local redirect server -----------------------------------------------------------

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


def test_callback_server_surfaces_oauth_error(mod):
    result = mod._CallbackResult()
    handler_cls = mod._make_callback_handler("expected-state", result)
    server = mod.http.server.HTTPServer(("localhost", 0), handler_cls)
    port = server.server_address[1]
    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()
    time.sleep(0.05)
    urllib.request.urlopen(f"http://localhost:{port}/callback?error=access_denied&error_description=user+declined&state=expected-state", timeout=5)
    thread.join(timeout=5)
    assert result.code is None
    assert result.error == "user declined"


# --- full sign_in() flow, with the interactive pieces faked ------------------------------------

def test_sign_in_end_to_end_with_faked_browser_step(mod, monkeypatch, tmp_path):
    oauth_dir = str(tmp_path)
    monkeypatch.setattr(mod, "fetch_server_metadata", lambda: {
        "authorization_endpoint": "https://mcp.notion.com/authorize",
        "token_endpoint": "https://mcp.notion.com/token",
        "registration_endpoint": "https://mcp.notion.com/register",
    })
    monkeypatch.setattr(mod, "register_client", lambda endpoint, redirect_uri: {"client_id": "cid1", "client_secret": "secret1"})

    def fake_open_browser(auth_url, redirect_port):
        result = mod._CallbackResult()
        result.code = "fake-code"
        return result

    monkeypatch.setattr(mod, "exchange_code_for_tokens", lambda *a, **k: {"access_token": "AT", "refresh_token": "RT", "expires_in": 28800})

    async def fake_async(oauth_dir_, tool_name, arguments, timeout_seconds=60.0):
        assert tool_name == "notion-get-self"
        return "signed in as person@example.com in Workspace X"

    monkeypatch.setattr(mod, "_nmo_call_tool_async", fake_async)

    info = mod.sign_in(oauth_dir, open_browser=fake_open_browser)
    assert "Workspace X" in info["whoami"]

    tokens = json.loads(open(mod._nmo_tokens_path(oauth_dir)).read())
    assert tokens["access_token"] == "AT"
    assert tokens["refresh_token"] == "RT"
    client_info = json.loads(open(mod._nmo_client_info_path(oauth_dir)).read())
    assert client_info["client_id"] == "cid1"


def test_sign_in_reuses_existing_client_info(mod, monkeypatch, tmp_path):
    oauth_dir = str(tmp_path)
    mod._nmo_atomic_write(mod._nmo_client_info_path(oauth_dir), {"client_id": "existing"})
    monkeypatch.setattr(mod, "fetch_server_metadata", lambda: {
        "authorization_endpoint": "https://mcp.notion.com/authorize",
        "token_endpoint": "https://mcp.notion.com/token",
        "registration_endpoint": "https://mcp.notion.com/register",
    })
    register_calls = []
    monkeypatch.setattr(mod, "register_client", lambda *a, **k: register_calls.append(1) or {"client_id": "SHOULD_NOT_BE_USED"})
    monkeypatch.setattr(mod, "exchange_code_for_tokens", lambda *a, **k: {"access_token": "AT", "refresh_token": "RT", "expires_in": 28800})

    async def fake_async(oauth_dir_, tool_name, arguments, timeout_seconds=60.0):
        return "ok"

    monkeypatch.setattr(mod, "_nmo_call_tool_async", fake_async)

    def fake_open_browser(auth_url, redirect_port):
        assert "client_id=existing" in auth_url
        result = mod._CallbackResult()
        result.code = "fake-code"
        return result

    mod.sign_in(oauth_dir, open_browser=fake_open_browser)
    assert register_calls == []  # never re-registered


def test_sign_in_raises_clearly_on_browser_error(mod, monkeypatch, tmp_path):
    oauth_dir = str(tmp_path)
    monkeypatch.setattr(mod, "fetch_server_metadata", lambda: {
        "authorization_endpoint": "https://mcp.notion.com/authorize",
        "token_endpoint": "https://mcp.notion.com/token",
        "registration_endpoint": "https://mcp.notion.com/register",
    })
    monkeypatch.setattr(mod, "register_client", lambda *a, **k: {"client_id": "cid1"})

    def fake_open_browser(auth_url, redirect_port):
        result = mod._CallbackResult()
        result.error = "user declined"
        return result

    with pytest.raises(RuntimeError, match="user declined"):
        mod.sign_in(oauth_dir, open_browser=fake_open_browser)


def test_main_exits_nonzero_with_message_on_failure(mod, monkeypatch, tmp_path, capsys):
    def fake_sign_in(oauth_dir, redirect_port=mod._DEFAULT_REDIRECT_PORT, open_browser=None):
        raise RuntimeError("sign-in blew up")

    monkeypatch.setattr(mod, "sign_in", fake_sign_in)
    with pytest.raises(SystemExit) as exc_info:
        mod.main(["--oauth-dir", str(tmp_path)])
    assert exc_info.value.code == 1
    assert "sign-in blew up" in capsys.readouterr().err


def test_main_prints_whoami_on_success(mod, monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(mod, "sign_in", lambda oauth_dir, redirect_port=mod._DEFAULT_REDIRECT_PORT, open_browser=None: {"whoami": "Signed in as X"})
    mod.main(["--oauth-dir", str(tmp_path)])
    out = capsys.readouterr().out
    assert "Signed in as X" in out
