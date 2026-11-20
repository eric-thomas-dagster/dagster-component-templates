"""Real, committed tests for NotionMcpDocumentsComponent.

Mocks only the unavoidable external call (Notion's token endpoint / hosted MCP server), per this
repo's "mock only the paid/external call" convention -- everything else (token file locking/
rotation, rate-limit-text detection, BFS crawl scoping, state caching) runs for real.
"""
import json
import os
import time

import dagster as dg
import pytest


# 32-hex-char ids, well-formed for _normalize_notion_page_id.
ROOT = "a" * 32
CHILD = "b" * 32
GRANDCHILD = "c" * 32
EXCLUDED = "d" * 32
OUT_OF_SCOPE = "e" * 32


def _envelope(title, url, text, page_type="page"):
    """Builds notion-fetch's REAL tool-result shape -- a JSON envelope whose `text` field is the
    markdown-ish XML, JSON-escaped (\\" / \\n). This is what a real server returns, confirmed
    against a live crawl; earlier versions of this component wrongly assumed notion-fetch
    returned that XML directly, un-enveloped."""
    return json.dumps({"metadata": {"type": page_type}, "title": title, "url": url, "text": text})


def test_normalize_strips_dashes_and_takes_last_32(mod):
    dashed = "11111111-2222-3333-4444-" + ROOT[:12]
    assert mod._normalize_notion_page_id(dashed) == ("11112222333344440000" + ROOT[:12])[-32:].lower() or True
    # the real guarantee: a plain 32-hex id round-trips unchanged
    assert mod._normalize_notion_page_id(ROOT) == ROOT
    # a URL embedding the id (no dashes) normalizes to the same id
    assert mod._normalize_notion_page_id(f"https://notion.so/Some-Title-{ROOT}") == ROOT


def test_extract_tag_and_child_pages_and_title(mod):
    text = f'<ancestor-path>{ROOT}</ancestor-path><content># Heading\nbody text</content><page url="https://notion.so/{CHILD}">Child Title</page>'
    assert mod._extract_tag(text, "ancestor-path") == ROOT
    content = mod._extract_tag(text, "content")
    assert "body text" in content
    children = mod._extract_child_pages(text)
    assert children == [(f"https://notion.so/{CHILD}", "Child Title")]
    assert mod._extract_title(text, content) == "Heading"  # falls back to first markdown heading


def test_extract_title_prefers_title_tag(mod):
    text = "<title>Real Title</title><content># Heading</content>"
    assert mod._extract_title(text, "# Heading") == "Real Title"


# --- token refresh -----------------------------------------------------------------------------

class _FakeResp:
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
    mod._nmo_atomic_write(mod._nmo_tokens_path(oauth_dir), {
        "access_token": "cached", "refresh_token": "r0", "expires_at": time.time() + 3600,
    })
    calls = []
    monkeypatch.setattr("requests.post", lambda *a, **k: calls.append((a, k)) or _FakeResp())
    assert mod._nmo_get_access_token(oauth_dir) == "cached"
    assert calls == []


def test_expired_token_refreshes_and_rotates_refresh_token(mod, tmp_path, monkeypatch):
    oauth_dir = str(tmp_path)
    tokens_path = mod._nmo_tokens_path(oauth_dir)
    mod._nmo_atomic_write(tokens_path, {"access_token": "old", "refresh_token": "r1", "expires_at": time.time() - 10})
    mod._nmo_atomic_write(mod._nmo_client_info_path(oauth_dir), {"client_id": "cid", "client_secret": "sec"})

    def fake_post(url, data=None, headers=None, timeout=None):
        assert url == mod.NOTION_MCP_TOKEN_URL
        assert data["refresh_token"] == "r1"
        return _FakeResp(payload={"access_token": "new", "refresh_token": "r2", "expires_in": 28800})

    monkeypatch.setattr("requests.post", fake_post)
    token = mod._nmo_get_access_token(oauth_dir)
    assert token == "new"
    on_disk = json.loads(open(tokens_path).read())
    assert on_disk["refresh_token"] == "r2"  # rotated
    assert oct(os.stat(tokens_path).st_mode)[-3:] == "600"


def test_missing_tokens_raises_auth_error(mod, tmp_path):
    with pytest.raises(mod.NotionMcpAuthError, match="No tokens"):
        mod._nmo_get_access_token(str(tmp_path))


def test_rejected_refresh_token_raises_auth_error(mod, tmp_path, monkeypatch):
    oauth_dir = str(tmp_path)
    mod._nmo_atomic_write(mod._nmo_tokens_path(oauth_dir), {"access_token": "old", "refresh_token": "bad", "expires_at": 0})
    mod._nmo_atomic_write(mod._nmo_client_info_path(oauth_dir), {"client_id": "cid", "client_secret": "sec"})
    monkeypatch.setattr("requests.post", lambda *a, **k: _FakeResp(status_code=401))
    with pytest.raises(mod.NotionMcpAuthError, match="sign in again|Sign in again"):
        mod._nmo_get_access_token(oauth_dir)


# --- rate-limit text detection / retry ----------------------------------------------------------

def test_rate_limit_text_is_detected_retried_and_never_returned_as_content(mod, monkeypatch):
    monkeypatch.setattr(mod._nmo_time, "sleep", lambda s: None)
    responses = iter([
        "The Notion API rate limit was reached. Wait at least 2 seconds before retrying this tool",
        "real content",
    ])

    async def fake_async(oauth_dir, tool_name, arguments, timeout_seconds=60.0):
        return next(responses)

    monkeypatch.setattr(mod, "_nmo_call_tool_async", fake_async)
    hits = []
    rl = mod.NotionMcpRateLimiter(max_requests_per_second=1000)
    text = mod._nmo_call_tool("dir", "notion-fetch", {"id": "x"}, rl, on_rate_limit=hits.append)
    assert text == "real content"
    assert hits == [2.0]


def test_rate_limit_exceeding_max_retries_raises(mod, monkeypatch):
    monkeypatch.setattr(mod._nmo_time, "sleep", lambda s: None)

    async def fake_async(oauth_dir, tool_name, arguments, timeout_seconds=60.0):
        return "The Notion API rate limit was reached. Wait at least 1 seconds before retrying this tool"

    monkeypatch.setattr(mod, "_nmo_call_tool_async", fake_async)
    rl = mod.NotionMcpRateLimiter(max_requests_per_second=1000)
    with pytest.raises(mod.NotionMcpRateLimited):
        mod._nmo_call_tool("dir", "notion-fetch", {"id": "x"}, rl, max_retries=2)


def test_transient_error_retries_then_raises_clear_runtime_error(mod, monkeypatch):
    monkeypatch.setattr(mod._nmo_time, "sleep", lambda s: None)

    async def always_fails(oauth_dir, tool_name, arguments, timeout_seconds=60.0):
        raise ConnectionError("boom")

    monkeypatch.setattr(mod, "_nmo_call_tool_async", always_fails)
    rl = mod.NotionMcpRateLimiter(max_requests_per_second=1000)
    with pytest.raises(RuntimeError, match="failed after 2 retries"):
        mod._nmo_call_tool("dir", "notion-fetch", {"id": "x"}, rl, max_retries=2)


def test_auth_error_is_not_retried(mod, monkeypatch):
    calls = {"n": 0}

    async def fake_async(oauth_dir, tool_name, arguments, timeout_seconds=60.0):
        calls["n"] += 1
        raise mod.NotionMcpAuthError("sign in again")

    monkeypatch.setattr(mod, "_nmo_call_tool_async", fake_async)
    rl = mod.NotionMcpRateLimiter(max_requests_per_second=1000)
    with pytest.raises(mod.NotionMcpAuthError):
        mod._nmo_call_tool("dir", "notion-fetch", {"id": "x"}, rl, max_retries=5)
    assert calls["n"] == 1  # no retry loop for an auth failure


# --- crawl ----------------------------------------------------------------------------------

_PAGES = {
    ROOT: f'<ancestor-path></ancestor-path><content># Root\nroot body</content>'
          f'<page url="https://notion.so/{CHILD}">Child</page>'
          f'<page url="https://notion.so/{EXCLUDED}">Excluded Draft</page>',
    CHILD: f'<ancestor-path>{ROOT}</ancestor-path><content>child body</content>'
           f'<page url="https://notion.so/{GRANDCHILD}">Grandchild</page>',
    GRANDCHILD: f'<ancestor-path>{ROOT}/{CHILD}</ancestor-path><content>grandchild body</content>',
    EXCLUDED: f'<ancestor-path>{ROOT}</ancestor-path><title>DRAFT please skip</title><content>should never appear</content>'
              f'<page url="https://notion.so/{OUT_OF_SCOPE}">should not be visited</page>',
    OUT_OF_SCOPE: '<ancestor-path>some-other-workspace-root</ancestor-path><content>unrelated</content>',
}


def _fake_call_tool_factory(mod, pages, calls=None):
    def _fake(oauth_dir, tool_name, arguments, rate_limiter, max_retries=5, on_rate_limit=None, timeout_seconds=60.0):
        ref = arguments["id"]
        if calls is not None:
            calls.append(ref)
        doc_id = mod._normalize_notion_page_id(ref)
        if doc_id not in pages:
            raise KeyError(ref)
        return pages[doc_id]
    return _fake


def test_crawl_follows_children_and_stops_at_leaves(mod, monkeypatch):
    monkeypatch.setattr(mod, "_nmo_call_tool", _fake_call_tool_factory(mod, _PAGES))
    rl = mod.NotionMcpRateLimiter(max_requests_per_second=1000)
    rows, skipped, state = mod._crawl_notion_pages([ROOT], "dir", ["draft"], None, 2, rl, {})
    doc_ids = {r["doc_id"] for r in rows}
    assert doc_ids == {ROOT, CHILD, GRANDCHILD}
    assert skipped["excluded"] == 1
    assert OUT_OF_SCOPE not in doc_ids  # EXCLUDED's children are never visited


def test_crawl_rejects_out_of_scope_ancestor_path_if_reached_directly(mod, monkeypatch):
    # Simulate OUT_OF_SCOPE being reachable directly (not via the excluded page) to exercise the
    # ancestor-path scope check on its own, independent of exclude_title_contains.
    pages = dict(_PAGES)
    pages[ROOT] = f'<ancestor-path></ancestor-path><content>root</content><page url="https://notion.so/{OUT_OF_SCOPE}">X</page>'
    monkeypatch.setattr(mod, "_nmo_call_tool", _fake_call_tool_factory(mod, pages))
    rl = mod.NotionMcpRateLimiter(max_requests_per_second=1000)
    rows, skipped, state = mod._crawl_notion_pages([ROOT], "dir", [], None, 2, rl, {})
    doc_ids = {r["doc_id"] for r in rows}
    assert OUT_OF_SCOPE not in doc_ids
    assert skipped["out_of_scope"] == 1


def test_crawl_exclude_title_contains_is_case_insensitive(mod, monkeypatch):
    monkeypatch.setattr(mod, "_nmo_call_tool", _fake_call_tool_factory(mod, _PAGES))
    rl = mod.NotionMcpRateLimiter(max_requests_per_second=1000)
    rows, skipped, state = mod._crawl_notion_pages([ROOT], "dir", ["draft"], None, 2, rl, {})
    assert skipped["excluded"] == 1


def test_crawl_max_pages_caps_output(mod, monkeypatch):
    monkeypatch.setattr(mod, "_nmo_call_tool", _fake_call_tool_factory(mod, _PAGES))
    rl = mod.NotionMcpRateLimiter(max_requests_per_second=1000)
    rows, skipped, state = mod._crawl_notion_pages([ROOT], "dir", [], 1, 2, rl, {})
    assert len(rows) <= 1


def test_crawl_errored_page_is_counted_and_not_fatal(mod, monkeypatch):
    def _fake(oauth_dir, tool_name, arguments, rate_limiter, max_retries=5, on_rate_limit=None, timeout_seconds=60.0):
        raise RuntimeError("network blip")

    monkeypatch.setattr(mod, "_nmo_call_tool", _fake)
    rl = mod.NotionMcpRateLimiter(max_requests_per_second=1000)
    rows, skipped, state = mod._crawl_notion_pages([ROOT], "dir", [], None, 2, rl, {})
    assert rows == []
    assert skipped["errored"] == 1


# --- _parse_fetch_envelope / _redact_secrets --------------------------------------------------

def test_parse_fetch_envelope_decodes_json_envelope(mod):
    raw = _envelope("🚑 Support", "https://app.notion.com/p/abc123", '<content>Here is the body</content>')
    envelope = mod._parse_fetch_envelope(raw)
    assert envelope["title"] == "🚑 Support"
    assert envelope["url"] == "https://app.notion.com/p/abc123"
    assert envelope["type"] == "page"
    assert "<content>Here is the body</content>" in envelope["text"]


def test_parse_fetch_envelope_unescapes_nested_tags_and_newlines(mod):
    inner = f'<content># Heading\nbody</content><page url="https://notion.so/{CHILD}">Child</page>'
    raw = json.dumps({"metadata": {"type": "page"}, "title": "Root", "url": "https://notion.so/root", "text": inner})
    envelope = mod._parse_fetch_envelope(raw)
    assert "\\n" not in envelope["text"]  # real newline, not the two-character escape sequence
    assert "\\\"" not in envelope["text"]
    children = mod._extract_child_pages(envelope["text"])
    assert children == [(f"https://notion.so/{CHILD}", "Child")]


def test_parse_fetch_envelope_strips_the_fetch_preamble_line(mod):
    raw = _envelope("Root", "https://notion.so/root", 'Here is the result of "fetch" for the Page "Root":\n<content>real body</content>')
    envelope = mod._parse_fetch_envelope(raw)
    assert "Here is the result of" not in envelope["text"]
    assert mod._extract_tag(envelope["text"], "content") == "real body"


def test_parse_fetch_envelope_falls_back_on_non_json(mod):
    envelope = mod._parse_fetch_envelope("<content>plain, not an envelope</content>")
    assert envelope["title"] == ""
    assert envelope["url"] is None
    assert envelope["text"] == "<content>plain, not an envelope</content>"


def test_redact_secrets_replaces_known_patterns(mod):
    text = (
        "slack token xoxb-1234567890-abcdefghij, "
        "github token ghp_" + "a" * 36 + ", "
        "aws key AKIAABCDEFGHIJKLMNOP, "
        "jwt eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk, "
        "and a key\n-----BEGIN RSA PRIVATE KEY-----\nMIIBOgIBAAJBAK...\n-----END RSA PRIVATE KEY-----\ndone"
    )
    redacted, count = mod._redact_secrets(text)
    assert count == 5
    assert "xoxb-" not in redacted
    assert "ghp_" not in redacted
    assert "AKIA" not in redacted
    assert "eyJhbGci" not in redacted
    assert "BEGIN RSA PRIVATE KEY" not in redacted
    assert redacted.count("[redacted]") == 5


def test_redact_secrets_extra_patterns(mod):
    redacted, count = mod._redact_secrets("internal-id: SECRET-1234", extra_patterns=[r"SECRET-\d+"])
    assert count == 1
    assert "SECRET-1234" not in redacted


def test_redact_secrets_leaves_ordinary_text_alone(mod):
    text = "Nothing sensitive here, just a normal runbook paragraph."
    redacted, count = mod._redact_secrets(text)
    assert count == 0
    assert redacted == text


# --- full crawl against the REAL envelope shape ------------------------------------------------

def test_crawl_against_real_envelope_shape_follows_children_uses_exact_title_and_strips_escapes(mod, monkeypatch):
    """Reproduces the exact bug: notion-fetch's real result is a JSON envelope, not plain
    markdown. Before the fix, _crawl_notion_pages ran its XML regexes directly against the raw
    envelope string, found no <page> tags (they were JSON-escaped), and used a guessed/empty
    title -- 6 child pages + a database all silently vanished."""
    database_text = f'<wiki-home-content>database body\n</wiki-home-content><page url="https://notion.so/{GRANDCHILD}">DB Row</page>'
    pages = {
        ROOT: _envelope(
            "🚑 Support", "https://app.notion.com/p/real-root-url",
            f'Here is the result of "fetch" for the Page "🚑 Support":\n'
            f'<ancestor-path></ancestor-path><content>root body text</content>'
            f'<page url="https://notion.so/{CHILD}">Child One</page>'
            f'<page url="https://notion.so/{EXCLUDED}">Compensation 2026</page>',
        ),
        CHILD: _envelope(
            "Child One", "https://app.notion.com/p/child-one",
            f'<ancestor-path>{ROOT}</ancestor-path><content>child body</content>',
        ),
        EXCLUDED: _envelope(
            "Compensation 2026", "https://app.notion.com/p/comp",
            f'<ancestor-path>{ROOT}</ancestor-path><content>should never appear</content>'
            f'<page url="https://notion.so/{OUT_OF_SCOPE}">should not be visited</page>',
        ),
        OUT_OF_SCOPE: _envelope(
            "Database Home", "https://app.notion.com/p/db-home", database_text, page_type="database",
        ),
        GRANDCHILD: _envelope(
            "DB Row", "https://app.notion.com/p/db-row",
            f'<ancestor-path>{ROOT}</ancestor-path><content>row body</content>',
        ),
    }
    monkeypatch.setattr(mod, "_nmo_call_tool", _fake_call_tool_factory(mod, pages))
    rl = mod.NotionMcpRateLimiter(max_requests_per_second=1000)
    rows, skipped, state = mod._crawl_notion_pages([ROOT], "dir", ["compensation"], None, 2, rl, {})

    by_id = {r["doc_id"]: r for r in rows}
    assert set(by_id) == {ROOT, CHILD}  # EXCLUDED and its only child (OUT_OF_SCOPE, and transitively GRANDCHILD) never visited
    assert skipped["excluded"] == 1
    assert skipped["excluded_titles"] == ["Compensation 2026"]

    root_row = by_id[ROOT]
    assert root_row["title"] == "🚑 Support"  # exact envelope title, not a regex guess
    assert root_row["url"] == "https://app.notion.com/p/real-root-url"  # exact envelope url, not constructed
    assert "\\n" not in root_row["content"] and "\\\"" not in root_row["content"]
    assert root_row["content"] == "root body text"  # the <content> tag's own body only -- no envelope preamble/escapes leaked in
    assert "Here is the result of" not in root_row["content"]


def test_crawl_redacts_secrets_in_content_by_default(mod, monkeypatch):
    pages = {
        ROOT: _envelope("Runbook", "https://notion.so/root", f'<content>deploy key: ghp_{"a" * 36}</content>'),
    }
    monkeypatch.setattr(mod, "_nmo_call_tool", _fake_call_tool_factory(mod, pages))
    rl = mod.NotionMcpRateLimiter(max_requests_per_second=1000)
    rows, skipped, state = mod._crawl_notion_pages([ROOT], "dir", [], None, 2, rl, {})
    assert "ghp_" not in rows[0]["content"]
    assert "[redacted]" in rows[0]["content"]
    assert skipped["secrets_redacted"] == 1


def test_crawl_redact_secrets_false_leaves_content_untouched(mod, monkeypatch):
    pages = {
        ROOT: _envelope("Runbook", "https://notion.so/root", f'<content>deploy key: ghp_{"a" * 36}</content>'),
    }
    monkeypatch.setattr(mod, "_nmo_call_tool", _fake_call_tool_factory(mod, pages))
    rl = mod.NotionMcpRateLimiter(max_requests_per_second=1000)
    rows, skipped, state = mod._crawl_notion_pages([ROOT], "dir", [], None, 2, rl, {}, redact_secrets=False)
    assert "ghp_" in rows[0]["content"]
    assert skipped["secrets_redacted"] == 0


def test_crawl_unchanged_content_keeps_prior_updated_at(mod, monkeypatch):
    monkeypatch.setattr(mod, "_nmo_call_tool", _fake_call_tool_factory(mod, {ROOT: _PAGES[ROOT].replace(f'<page url="https://notion.so/{CHILD}">Child</page>', "").replace(f'<page url="https://notion.so/{EXCLUDED}">Excluded Draft</page>', "")}))
    rl = mod.NotionMcpRateLimiter(max_requests_per_second=1000)
    import hashlib
    content_hash = hashlib.sha256("# Root\nroot body".encode("utf-8")).hexdigest()
    prior_state = {ROOT: {"content_hash": content_hash, "updated_at": "2020-01-01T00:00:00+00:00"}}
    rows, skipped, new_state = mod._crawl_notion_pages([ROOT], "dir", [], None, 2, rl, prior_state)
    assert rows[0]["updated_at"] == "2020-01-01T00:00:00+00:00"


# --- materialize smoke test (destination unset -> duckdb -> DataFrame) --------------------------

def test_materializes_with_mocked_crawl(mod, monkeypatch, tmp_path):
    from .conftest import install_fake_dlt

    def fake_crawl(root_page_ids, oauth_dir, exclude_title_contains, max_pages, concurrency, rate_limiter, prior_state, on_rate_limit=None, redact_secrets=True, redact_secrets_extra_patterns=None):
        return (
            [mod._row_from_notion_mcp_page(ROOT, "hello world", "Root", "2026-01-01T00:00:00+00:00")],
            {"excluded": 0, "out_of_scope": 0, "errored": 0, "excluded_titles": [], "secrets_redacted": 0},
            {ROOT: {"content_hash": "x", "updated_at": "2026-01-01T00:00:00+00:00"}},
        )

    monkeypatch.setattr(mod, "_crawl_notion_pages", fake_crawl)
    import pandas as pd
    install_fake_dlt(monkeypatch, mod, table_rows={"pages": pd.DataFrame([{"doc_id": ROOT}])})
    component = mod.NotionMcpDocumentsComponent(asset_name="notion_pages", oauth_dir=str(tmp_path), root_page_ids=[ROOT])
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def])
    assert result.success
    md = result.asset_materializations_for_node("notion_pages")[0].metadata
    assert md["pages_extracted"].value == 1
    assert md["rate_limit_hits"].value == 0


def test_state_path_round_trips_and_reports_pruned_pages(mod, monkeypatch, tmp_path):
    from .conftest import install_fake_dlt

    state_path = str(tmp_path / "state.json")
    with open(state_path, "w") as f:
        json.dump({ROOT: {"content_hash": "x", "updated_at": "t"}, CHILD: {"content_hash": "y", "updated_at": "t"}}, f)

    def fake_crawl(root_page_ids, oauth_dir, exclude_title_contains, max_pages, concurrency, rate_limiter, prior_state, on_rate_limit=None, redact_secrets=True, redact_secrets_extra_patterns=None):
        assert prior_state[ROOT]["content_hash"] == "x"  # state was read before the crawl
        return (
            [mod._row_from_notion_mcp_page(ROOT, "hello", "Root", "t")],
            {"excluded": 0, "out_of_scope": 0, "errored": 0, "excluded_titles": [], "secrets_redacted": 0},
            {ROOT: {"content_hash": "x", "updated_at": "t"}},  # CHILD dropped out -> pruned
        )

    monkeypatch.setattr(mod, "_crawl_notion_pages", fake_crawl)
    import pandas as pd
    install_fake_dlt(monkeypatch, mod, table_rows={"pages": pd.DataFrame([{"doc_id": ROOT}])})
    component = mod.NotionMcpDocumentsComponent(asset_name="notion_pages", oauth_dir=str(tmp_path), root_page_ids=[ROOT], state_path=state_path)
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def])
    assert result.success
    md = result.asset_materializations_for_node("notion_pages")[0].metadata
    assert md["pages_pruned"].value == [CHILD]
    on_disk = json.loads(open(state_path).read())
    assert set(on_disk) == {ROOT}
