"""Tests for SitemapCrawlerIngestionComponent.

`dlt.pipeline(...)` is faked (see conftest.py); the REAL `@dlt.resource` object dlt
builds is still exercised, so hash_columns/exclude_columns/max_rows_per_resource/
on_schema_change are verified by iterating the real resource the component built
(`fake_pipeline.run_calls[-1]`). The one real network surface this component has --
`requests.get` (sitemap XML, page HTML, robots.txt; no API key, no paid call) -- is
mocked via `install_fake_requests` with real, hand-crafted XML/HTML fixtures.
"""
import hashlib

import dagster as dg
import pydantic
import pytest

from .conftest import FakeResponse, install_fake_dlt, install_fake_requests, load_component_module

LEAF_SITEMAP = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://example.com/a</loc><lastmod>2024-01-01</lastmod></url>
  <url><loc>https://example.com/b</loc><lastmod>2024-01-02</lastmod></url>
</urlset>
"""

INDEX_SITEMAP = """<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>https://example.com/sitemap-1.xml</loc></sitemap>
  <sitemap><loc>https://example.com/sitemap-2.xml</loc></sitemap>
</sitemapindex>
"""

CHILD_SITEMAP_1 = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://example.com/c1</loc><lastmod>2024-02-01</lastmod></url>
</urlset>
"""

CHILD_SITEMAP_2 = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://example.com/c2</loc><lastmod>2024-02-02</lastmod></url>
</urlset>
"""

# A child sitemap that is itself an index -- should be logged and skipped, not recursed into.
NESTED_INDEX_CHILD = """<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>https://example.com/sitemap-grandchild.xml</loc></sitemap>
</sitemapindex>
"""


def _page_html(title, links=()):
    link_tags = "".join(f'<a href="{link}">link</a>' for link in links)
    return (
        f"<html><head><title>{title}</title><style>.x {{ color: red; }}</style></head>"
        f"<body><script>var tracked = 1;</script>"
        f"<h1>{title}</h1><p>Real body content for {title}.</p>{link_tags}</body></html>"
    )


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})


def _robots_allow_all(url, headers=None, timeout=None):
    assert url.endswith("/robots.txt")
    return FakeResponse(status_code=404)


# --- validation ------------------------------------------------------------------------

def test_requires_exactly_one_of_sitemap_url_or_start_urls(mod):
    with pytest.raises(pydantic.ValidationError, match="exactly one"):
        mod.SitemapCrawlerIngestionComponent(asset_name="test_asset")


def test_rejects_both_sitemap_url_and_start_urls_set(mod):
    with pytest.raises(pydantic.ValidationError, match="exactly one"):
        mod.SitemapCrawlerIngestionComponent(
            asset_name="test_asset",
            sitemap_url="https://example.com/sitemap.xml",
            start_urls="https://example.com/",
        )


# --- sitemap mode: leaf sitemap ---------------------------------------------------------

def test_leaf_sitemap_parsing_builds_one_row_per_url(mod, monkeypatch):
    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return _robots_allow_all(url)
        if url == "https://example.com/sitemap.xml":
            return FakeResponse(content=LEAF_SITEMAP.encode("utf-8"))
        if url == "https://example.com/a":
            return FakeResponse(text=_page_html("Page A"))
        if url == "https://example.com/b":
            return FakeResponse(text=_page_html("Page B"))
        raise AssertionError(f"unexpected URL: {url}")

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url="https://example.com/sitemap.xml",
        include_preview_metadata=False, request_delay_seconds=0,
    )
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 2
    urls = {r["url"] for r in rows}
    assert urls == {"https://example.com/a", "https://example.com/b"}
    by_url = {r["url"]: r for r in rows}
    assert by_url["https://example.com/a"]["updated_at"] == "2024-01-01"
    assert by_url["https://example.com/b"]["updated_at"] == "2024-01-02"


def test_leaf_sitemap_title_and_content_extracted_without_script_or_style(mod, monkeypatch):
    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return _robots_allow_all(url)
        if url == "https://example.com/sitemap.xml":
            return FakeResponse(
                content=b'<?xml version="1.0"?><urlset><url><loc>https://example.com/a</loc></url></urlset>'
            )
        return FakeResponse(text=_page_html("Page A"))

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url="https://example.com/sitemap.xml",
        include_preview_metadata=False, request_delay_seconds=0,
    )
    result = _materialize(component)
    assert result.success
    row = list(fake_pipeline.run_calls[-1][0])[0]
    assert row["title"] == "Page A"
    assert "Real body content for Page A" in row["content"]
    assert "tracked" not in row["content"]
    assert "color: red" not in row["content"]


# --- sitemap mode: sitemap index (recursion one level) ----------------------------------

def test_sitemap_index_recurses_one_level_into_child_sitemaps(mod, monkeypatch):
    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return _robots_allow_all(url)
        if url == "https://example.com/sitemap.xml":
            return FakeResponse(content=INDEX_SITEMAP.encode("utf-8"))
        if url == "https://example.com/sitemap-1.xml":
            return FakeResponse(content=CHILD_SITEMAP_1.encode("utf-8"))
        if url == "https://example.com/sitemap-2.xml":
            return FakeResponse(content=CHILD_SITEMAP_2.encode("utf-8"))
        if url == "https://example.com/c1":
            return FakeResponse(text=_page_html("C1"))
        if url == "https://example.com/c2":
            return FakeResponse(text=_page_html("C2"))
        raise AssertionError(f"unexpected URL: {url}")

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url="https://example.com/sitemap.xml",
        include_preview_metadata=False, request_delay_seconds=0,
    )
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 2
    assert {r["url"] for r in rows} == {"https://example.com/c1", "https://example.com/c2"}


def test_sitemap_index_nested_index_child_is_skipped_not_recursed(mod, monkeypatch):
    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return _robots_allow_all(url)
        if url == "https://example.com/sitemap.xml":
            return FakeResponse(content=INDEX_SITEMAP.encode("utf-8"))
        if url == "https://example.com/sitemap-1.xml":
            # This child is itself an index -- should be logged and skipped.
            return FakeResponse(content=NESTED_INDEX_CHILD.encode("utf-8"))
        if url == "https://example.com/sitemap-2.xml":
            return FakeResponse(content=CHILD_SITEMAP_2.encode("utf-8"))
        if url == "https://example.com/c2":
            return FakeResponse(text=_page_html("C2"))
        raise AssertionError(f"unexpected URL fetched: {url} (grandchild sitemap should not be followed)")

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url="https://example.com/sitemap.xml",
        include_preview_metadata=False, request_delay_seconds=0,
    )
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1
    assert rows[0]["url"] == "https://example.com/c2"


def test_sitemap_mode_ignores_max_pages_and_max_depth(mod, monkeypatch):
    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return _robots_allow_all(url)
        if url == "https://example.com/sitemap.xml":
            return FakeResponse(content=LEAF_SITEMAP.encode("utf-8"))
        return FakeResponse(text=_page_html("Page"))

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    # Non-default max_pages/max_depth set alongside sitemap_url -- both urls still fetched.
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url="https://example.com/sitemap.xml",
        max_pages=1, max_depth=0, include_preview_metadata=False, request_delay_seconds=0,
    )
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 2  # not capped to max_pages=1 -- max_pages is plain-crawl-only


# --- plain-crawl mode --------------------------------------------------------------------

def test_plain_crawl_follows_same_domain_links_breadth_first(mod, monkeypatch):
    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return _robots_allow_all(url)
        if url == "https://example.com/":
            return FakeResponse(text=_page_html(
                "Home", links=["/docs/a", "https://other.com/off-domain"]
            ))
        if url == "https://example.com/docs/a":
            return FakeResponse(text=_page_html("Docs A", links=["/docs/b"]))
        if url == "https://example.com/docs/b":
            return FakeResponse(text=_page_html("Docs B"))
        raise AssertionError(f"unexpected URL fetched: {url} (off-domain link should not be followed)")

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url=None, start_urls="https://example.com/",
        max_pages=10, max_depth=3, include_preview_metadata=False, request_delay_seconds=0,
    )
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert {r["url"] for r in rows} == {
        "https://example.com/", "https://example.com/docs/a", "https://example.com/docs/b",
    }


def test_plain_crawl_respects_max_pages(mod, monkeypatch):
    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return _robots_allow_all(url)
        if url == "https://example.com/":
            return FakeResponse(text=_page_html("Home", links=["/a", "/b", "/c"]))
        return FakeResponse(text=_page_html(url))

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url=None, start_urls="https://example.com/",
        max_pages=2, max_depth=3, include_preview_metadata=False, request_delay_seconds=0,
    )
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 2


def test_plain_crawl_respects_max_depth(mod, monkeypatch):
    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return _robots_allow_all(url)
        if url == "https://example.com/":
            return FakeResponse(text=_page_html("Home", links=["/depth1"]))
        if url == "https://example.com/depth1":
            return FakeResponse(text=_page_html("Depth1", links=["/depth2"]))
        if url == "https://example.com/depth2":
            raise AssertionError("depth2 should not be fetched -- beyond max_depth=1")
        raise AssertionError(f"unexpected URL: {url}")

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url=None, start_urls="https://example.com/",
        max_pages=10, max_depth=1, include_preview_metadata=False, request_delay_seconds=0,
    )
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert {r["url"] for r in rows} == {"https://example.com/", "https://example.com/depth1"}


# --- include/exclude pattern filtering (substring match, both modes) --------------------

def test_matches_patterns_substring_semantics(mod):
    assert mod._matches_patterns("https://example.com/docs/a", ["/docs/"], []) is True
    assert mod._matches_patterns("https://example.com/blog/a", ["/docs/"], []) is False
    assert mod._matches_patterns("https://example.com/docs/a", [], ["/docs/"]) is False
    assert mod._matches_patterns("https://example.com/docs/a", [], []) is True
    # exclude wins when a URL matches both an include and an exclude pattern
    assert mod._matches_patterns("https://example.com/docs/archive/a", ["/docs/"], ["/archive/"]) is False


def test_include_exclude_patterns_filter_sitemap_urls(mod, monkeypatch):
    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return _robots_allow_all(url)
        if url == "https://example.com/sitemap.xml":
            return FakeResponse(
                content=(
                    '<?xml version="1.0"?><urlset>'
                    "<url><loc>https://example.com/docs/a</loc></url>"
                    "<url><loc>https://example.com/blog/a</loc></url>"
                    "</urlset>"
                ).encode("utf-8")
            )
        if url == "https://example.com/docs/a":
            return FakeResponse(text=_page_html("Docs A"))
        raise AssertionError(f"unexpected URL fetched: {url} (should have been excluded by include_patterns)")

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url="https://example.com/sitemap.xml",
        include_patterns="/docs/", include_preview_metadata=False, request_delay_seconds=0,
    )
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1
    assert rows[0]["url"] == "https://example.com/docs/a"


# --- robots.txt --------------------------------------------------------------------------

def test_robots_txt_disallow_is_respected(mod, monkeypatch):
    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return FakeResponse(text="User-agent: *\nDisallow: /private/\n")
        if url == "https://example.com/sitemap.xml":
            return FakeResponse(
                content=(
                    '<?xml version="1.0"?><urlset>'
                    "<url><loc>https://example.com/public/a</loc></url>"
                    "<url><loc>https://example.com/private/b</loc></url>"
                    "</urlset>"
                ).encode("utf-8")
            )
        if url == "https://example.com/public/a":
            return FakeResponse(text=_page_html("Public A"))
        raise AssertionError(f"unexpected URL fetched: {url} (disallowed by robots.txt)")

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url="https://example.com/sitemap.xml",
        include_preview_metadata=False, request_delay_seconds=0,
    )
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1
    assert rows[0]["url"] == "https://example.com/public/a"


def test_respect_robots_txt_false_fetches_disallowed_url_anyway(mod, monkeypatch):
    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            raise AssertionError("robots.txt should not be fetched when respect_robots_txt=False")
        if url == "https://example.com/sitemap.xml":
            return FakeResponse(
                content=(
                    '<?xml version="1.0"?><urlset>'
                    "<url><loc>https://example.com/private/b</loc></url>"
                    "</urlset>"
                ).encode("utf-8")
            )
        if url == "https://example.com/private/b":
            return FakeResponse(text=_page_html("Private B"))
        raise AssertionError(f"unexpected URL: {url}")

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url="https://example.com/sitemap.xml",
        respect_robots_txt=False, include_preview_metadata=False, request_delay_seconds=0,
    )
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1
    assert rows[0]["url"] == "https://example.com/private/b"


def test_robots_txt_unreachable_treated_as_allow_all(mod, monkeypatch):
    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return FakeResponse(status_code=500)
        if url == "https://example.com/sitemap.xml":
            return FakeResponse(
                content=(
                    '<?xml version="1.0"?><urlset>'
                    "<url><loc>https://example.com/a</loc></url>"
                    "</urlset>"
                ).encode("utf-8")
            )
        return FakeResponse(text=_page_html("A"))

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url="https://example.com/sitemap.xml",
        include_preview_metadata=False, request_delay_seconds=0,
    )
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1


# --- document source contract ------------------------------------------------------------

def test_document_source_contract_columns(mod, monkeypatch):
    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return _robots_allow_all(url)
        if url == "https://example.com/sitemap.xml":
            return FakeResponse(
                content=(
                    '<?xml version="1.0"?><urlset>'
                    "<url><loc>https://example.com/a</loc><lastmod>2024-05-01</lastmod></url>"
                    "</urlset>"
                ).encode("utf-8")
            )
        return FakeResponse(text=_page_html("Page A"))

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url="https://example.com/sitemap.xml",
        include_preview_metadata=False, request_delay_seconds=0,
    )
    result = _materialize(component)
    assert result.success
    row = list(fake_pipeline.run_calls[-1][0])[0]
    assert set(row.keys()) == {
        "doc_id", "content", "title", "url", "source_type", "updated_at", "content_hash",
    }
    assert row["doc_id"] == "https://example.com/a"
    assert row["url"] == "https://example.com/a"
    assert row["title"] == "Page A"
    assert row["source_type"] == "sitemap_crawler"
    assert row["updated_at"] == "2024-05-01"
    assert row["content_hash"] == hashlib.sha256(row["content"].encode("utf-8")).hexdigest()


def test_document_source_contract_updated_at_falls_back_to_fetch_timestamp_without_lastmod(mod, monkeypatch):
    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return _robots_allow_all(url)
        if url == "https://example.com/sitemap.xml":
            return FakeResponse(
                content='<?xml version="1.0"?><urlset><url><loc>https://example.com/a</loc></url></urlset>'.encode("utf-8")
            )
        return FakeResponse(text=_page_html("Page A"))

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url="https://example.com/sitemap.xml",
        include_preview_metadata=False, request_delay_seconds=0,
    )
    result = _materialize(component)
    assert result.success
    row = list(fake_pipeline.run_calls[-1][0])[0]
    # No <lastmod> in the sitemap entry -- falls back to an ISO fetch timestamp, not None/empty.
    assert row["updated_at"]
    assert row["updated_at"] != "2024-05-01"
    assert "T" in row["updated_at"]
