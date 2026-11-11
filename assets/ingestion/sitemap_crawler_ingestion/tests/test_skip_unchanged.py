"""skip_unchanged: persists a per-URL {etag, last_modified, content_hash} fingerprint to
state_path between runs. A later run sends If-None-Match/If-Modified-Since from the stored
fingerprint; a 304 (or an unchanged content_hash after a real fetch) reuses the cached row
instead of reparsing, while the output still covers every page every run. Off by default.
"""
import json

import dagster as dg
import pytest

from .conftest import FakeResponse, install_fake_dlt, install_fake_requests, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})


def _robots_allow_all(url, headers=None, timeout=None):
    return FakeResponse(status_code=404)


LEAF_SITEMAP = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://example.com/a</loc><lastmod>2024-01-01</lastmod></url>
</urlset>
"""


def _page_html(title):
    return f"<html><head><title>{title}</title></head><body><p>Body for {title}.</p></body></html>"


def test_first_run_fetches_everything_and_writes_state(mod, monkeypatch, tmp_path):
    state_path = str(tmp_path / "state.json")
    calls = {"page": 0}

    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return _robots_allow_all(url)
        if url == "https://example.com/sitemap.xml":
            return FakeResponse(content=LEAF_SITEMAP.encode("utf-8"))
        calls["page"] += 1
        return FakeResponse(text=_page_html("Page A"), headers={"ETag": '"v1"', "Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT"})

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url="https://example.com/sitemap.xml",
        include_preview_metadata=False, request_delay_seconds=0,
        skip_unchanged=True, state_path=state_path,
    )
    result = _materialize(component)
    assert result.success
    assert calls["page"] == 1
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1

    state = json.loads(open(state_path).read())
    assert state["https://example.com/a"]["etag"] == '"v1"'


def test_second_run_sends_conditional_headers_and_reuses_cached_row_on_304(mod, monkeypatch, tmp_path):
    state_path = str(tmp_path / "state.json")
    state_path_obj = tmp_path / "state.json"
    state_path_obj.write_text(json.dumps({
        "https://example.com/a": {
            "etag": '"v1"', "last_modified": "Mon, 01 Jan 2024 00:00:00 GMT",
            "content_hash": "irrelevant",
            "row": {
                "doc_id": "https://example.com/a", "content": "cached body", "title": "Cached Title",
                "url": "https://example.com/a", "source_type": "sitemap_crawler",
                "updated_at": "2024-01-01", "content_hash": "cached-hash",
            },
        }
    }))

    sent_headers = {}

    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return _robots_allow_all(url)
        if url == "https://example.com/sitemap.xml":
            return FakeResponse(content=LEAF_SITEMAP.encode("utf-8"))
        sent_headers.update(headers or {})
        return FakeResponse(status_code=304)

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url="https://example.com/sitemap.xml",
        include_preview_metadata=False, request_delay_seconds=0,
        skip_unchanged=True, state_path=state_path,
    )
    result = _materialize(component)
    assert result.success
    assert sent_headers.get("If-None-Match") == '"v1"'
    assert sent_headers.get("If-Modified-Since") == "Mon, 01 Jan 2024 00:00:00 GMT"

    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1
    assert rows[0]["content"] == "cached body"  # reused, not reparsed


def test_pages_fetched_skipped_failed_metadata(mod, monkeypatch, tmp_path):
    state_path = str(tmp_path / "state.json")

    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return _robots_allow_all(url)
        if url == "https://example.com/sitemap.xml":
            return FakeResponse(content=LEAF_SITEMAP.encode("utf-8"))
        return FakeResponse(text=_page_html("Page A"), headers={"ETag": '"v1"'})

    install_fake_requests(monkeypatch, mod, get=fake_get)
    install_fake_dlt(monkeypatch, mod)
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url="https://example.com/sitemap.xml",
        include_preview_metadata=False, request_delay_seconds=0,
        skip_unchanged=True, state_path=state_path,
    )
    result = _materialize(component)
    assert result.success
    md = result.asset_materializations_for_node("test_asset")[0].metadata
    assert md["pages_fetched"].value == 1
    assert md["pages_skipped"].value == 0
    assert md["pages_failed"].value == 0


def test_skip_unchanged_off_by_default_no_state_file_written(mod, monkeypatch, tmp_path):
    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return _robots_allow_all(url)
        if url == "https://example.com/sitemap.xml":
            return FakeResponse(content=LEAF_SITEMAP.encode("utf-8"))
        return FakeResponse(text=_page_html("Page A"))

    install_fake_requests(monkeypatch, mod, get=fake_get)
    install_fake_dlt(monkeypatch, mod)
    monkeypatch.chdir(tmp_path)
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url="https://example.com/sitemap.xml",
        include_preview_metadata=False, request_delay_seconds=0,
    )
    result = _materialize(component)
    assert result.success
    assert not (tmp_path / "test_asset_crawler_state.json").exists()
