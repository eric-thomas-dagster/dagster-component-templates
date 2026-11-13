"""Tests for SitemapCrawlerIngestionComponent.

`dlt.pipeline(...)` is faked (see conftest.py); the REAL `@dlt.resource` object dlt
builds is still exercised, so hash_columns/exclude_columns/max_rows_per_resource/
on_schema_change are verified by iterating the real resource the component built
(`fake_pipeline.run_calls[-1]`). The one real network surface this component has --
`requests.get` (sitemap XML, page HTML, robots.txt; no API key, no paid call) -- is
mocked via `install_fake_requests` with real, hand-crafted XML/HTML fixtures.
"""
import hashlib
import pathlib

import dagster as dg
import pydantic
import pytest

from .conftest import FakeResponse, install_fake_dlt, install_fake_requests, load_component_module, requires_duckdb

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


# --- strip_boilerplate ------------------------------------------------------------------

_PAGE_WITH_CHROME = (
    "<html><head><title>{title}</title></head><body>"
    "<nav>Home | Docs | Blog</nav>"
    "<header>Site Header</header>"
    "<div role='banner'>Promo banner</div>"
    "<main><h1>{title}</h1><p>Real body content for {title}.</p></main>"
    "<aside>Related links sidebar</aside>"
    "<div role='contentinfo'>Copyright 2026</div>"
    "<footer>Footer links</footer>"
    "</body></html>"
)


def test_extract_page_content_strips_nav_header_footer_aside_and_landmark_roles(mod):
    html = _PAGE_WITH_CHROME.format(title="Page")
    title, content = mod._extract_page_content(html, strip_boilerplate=True)
    assert title == "Page"
    assert "Real body content for Page." in content
    for chrome in ("Home | Docs | Blog", "Site Header", "Promo banner", "Related links sidebar", "Copyright 2026", "Footer links"):
        assert chrome not in content


def test_extract_page_content_strip_boilerplate_false_keeps_everything(mod):
    html = _PAGE_WITH_CHROME.format(title="Page")
    _, content = mod._extract_page_content(html, strip_boilerplate=False)
    assert "Site Header" in content
    assert "Footer links" in content


def test_extract_page_content_prefers_article_when_no_main(mod):
    html = (
        "<html><head><title>T</title></head><body>"
        "<nav>Home</nav>"
        "<article>Article-only content.</article>"
        "<div>Sidebar text not inside any landmark tag.</div>"
        "</body></html>"
    )
    _, content = mod._extract_page_content(html, strip_boilerplate=True)
    assert "Article-only content." in content
    assert "Sidebar text not inside any landmark tag." not in content  # outside <article>, dropped


def test_strip_repeated_boilerplate_lines_drops_lines_common_to_most_pages(mod):
    pages = [
        "Sign in\nReal content A\nFooter copyright",
        "Sign in\nReal content B\nFooter copyright",
        "Sign in\nReal content C\nFooter copyright",
    ]
    cleaned, removed_chars = mod._strip_repeated_boilerplate_lines(pages)
    assert removed_chars > 0
    for c, unique in zip(cleaned, ("Real content A", "Real content B", "Real content C")):
        assert unique in c
        assert "Sign in" not in c
        assert "Footer copyright" not in c


def test_strip_repeated_boilerplate_lines_keeps_content_unique_to_one_page(mod):
    pages = ["Sign in\nUnique to page 1", "Sign in\nUnique to page 2"]
    cleaned, _ = mod._strip_repeated_boilerplate_lines(pages)
    assert "Unique to page 1" in cleaned[0]
    assert "Unique to page 2" in cleaned[1]


def test_strip_repeated_boilerplate_lines_single_page_is_a_no_op(mod):
    cleaned, removed_chars = mod._strip_repeated_boilerplate_lines(["Sign in\nOnly page"])
    assert cleaned == ["Sign in\nOnly page"]
    assert removed_chars == 0


def test_crawl_strips_repeated_chrome_line_with_no_semantic_wrapper(mod, monkeypatch):
    """End-to-end: a 'Sign in' line baked directly into <body> (no nav/header tag, so
    structural stripping alone wouldn't catch it) still gets removed because it repeats
    across every crawled page -- and the run reports removed_chars."""
    def _page(title):
        return f"<html><head><title>{title}</title></head><body><div>Sign in</div><p>Unique content for {title}.</p></body></html>"

    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return _robots_allow_all(url)
        if url == "https://example.com/sitemap.xml":
            return FakeResponse(content=LEAF_SITEMAP.encode())
        if url.endswith("/a"):
            return FakeResponse(text=_page("Page A"))
        return FakeResponse(text=_page("Page B"))

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url="https://example.com/sitemap.xml",
        include_preview_metadata=False, request_delay_seconds=0,
    )
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    for row in rows:
        assert "Sign in" not in row["content"]
    assert "Unique content for Page A." in rows[0]["content"]
    metadata = result.get_asset_materialization_events()[-1].materialization.metadata
    assert metadata["removed_chars"].value > 0


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


# --- write_disposition: resource hints (fake-dlt, real .apply_hints()) -----------------

def test_default_write_disposition_is_replace(mod, monkeypatch):
    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return _robots_allow_all(url)
        if url == "https://example.com/sitemap.xml":
            return FakeResponse(content=LEAF_SITEMAP.encode("utf-8"))
        return FakeResponse(text=_page_html("Page"))

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url="https://example.com/sitemap.xml",
        include_preview_metadata=False, request_delay_seconds=0,
    )
    result = _materialize(component)
    assert result.success
    resource = fake_pipeline.run_calls[-1][0]
    assert resource.write_disposition == "replace"
    assert "write_disposition" in result.asset_materializations_for_node("test_asset")[0].metadata
    assert result.asset_materializations_for_node("test_asset")[0].metadata["write_disposition"].value == "replace"


def test_merge_write_disposition_sets_primary_key(mod, monkeypatch):
    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return _robots_allow_all(url)
        if url == "https://example.com/sitemap.xml":
            return FakeResponse(content=LEAF_SITEMAP.encode("utf-8"))
        return FakeResponse(text=_page_html("Page"))

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url="https://example.com/sitemap.xml",
        include_preview_metadata=False, request_delay_seconds=0, write_disposition="merge",
    )
    result = _materialize(component)
    assert result.success
    resource = fake_pipeline.run_calls[-1][0]
    assert resource.write_disposition == "merge"
    assert resource.compute_table_schema()["columns"]["doc_id"]["primary_key"] is True


def test_append_write_disposition_is_explicit_opt_in(mod, monkeypatch):
    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return _robots_allow_all(url)
        if url == "https://example.com/sitemap.xml":
            return FakeResponse(content=LEAF_SITEMAP.encode("utf-8"))
        return FakeResponse(text=_page_html("Page"))

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="test_asset", sitemap_url="https://example.com/sitemap.xml",
        include_preview_metadata=False, request_delay_seconds=0, write_disposition="append",
    )
    result = _materialize(component)
    assert result.success
    resource = fake_pipeline.run_calls[-1][0]
    assert resource.write_disposition == "append"


# --- _dedupe_by_doc_id: pure-function safety net ---------------------------------------

def test_dedupe_by_doc_id_keeps_latest_updated_at(mod):
    import pandas as pd
    df = pd.DataFrame([
        {"doc_id": "https://a", "updated_at": "2024-01-01", "content": "old"},
        {"doc_id": "https://a", "updated_at": "2024-02-01", "content": "new"},
        {"doc_id": "https://b", "updated_at": "2024-01-01", "content": "b"},
    ])
    deduped, removed = mod._dedupe_by_doc_id(df)
    assert removed == 1
    assert len(deduped) == 2
    assert deduped.set_index("doc_id").loc["https://a", "content"] == "new"


def test_dedupe_by_doc_id_no_doc_id_column_is_a_no_op(mod):
    import pandas as pd
    df = pd.DataFrame([{"a": 1}, {"a": 2}])
    deduped, removed = mod._dedupe_by_doc_id(df)
    assert removed == 0
    assert len(deduped) == 2


def test_dedupe_by_doc_id_without_updated_at_column_still_dedupes(mod):
    import pandas as pd
    df = pd.DataFrame([{"doc_id": "x", "v": 1}, {"doc_id": "x", "v": 2}])
    deduped, removed = mod._dedupe_by_doc_id(df)
    assert removed == 1
    assert len(deduped) == 1


# --- real dlt + real (file-backed) duckdb: the actual regression this was fixing ------
# Faking dlt.pipeline() (as every test above does) can't prove accumulation is actually
# prevented at the destination -- it never persists anything between two separate
# FakePipeline instances. These two tests use the REAL dlt.pipeline()/duckdb destination
# (the component's own default when `destination` is unset) so a real regression of the
# exact bug this fixes -- row_count silently doubling on every rerun -- would be caught.
#
# dlt also keeps its OWN local pipeline-sync state in ~/.dlt/pipelines/<pipeline_name>/ --
# a GLOBAL directory, independent of cwd/tmp_path (confirmed directly: deleting a test's
# tmp_path after the fact and re-running a DIFFERENT test reusing the same pipeline_name
# makes dlt try to sync against the first test's now-deleted path). Each test below uses a
# pipeline_name unique to IT, and tears down that global directory before/after, so no run
# of this file -- in any order, on any host that has run it before -- can see stale state
# from another run.

def _reset_dlt_pipeline_dir(pipeline_name: str) -> None:
    import shutil
    shutil.rmtree(pathlib.Path.home() / ".dlt" / "pipelines" / pipeline_name, ignore_errors=True)


@requires_duckdb
def test_rerun_with_default_write_disposition_does_not_duplicate_rows(mod, monkeypatch, tmp_path):
    _reset_dlt_pipeline_dir("rerun_test_asset_pipeline")
    monkeypatch.chdir(tmp_path)

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
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="rerun_test_asset", sitemap_url="https://example.com/sitemap.xml",
        include_preview_metadata=False, request_delay_seconds=0,
    )
    result1 = _materialize(component)
    assert result1.success
    md1 = result1.asset_materializations_for_node("rerun_test_asset")[0].metadata
    assert md1["row_count"].value == 2

    result2 = _materialize(component)
    assert result2.success
    md2 = result2.asset_materializations_for_node("rerun_test_asset")[0].metadata
    assert md2["row_count"].value == 2  # NOT 4 -- this is the exact bug being fixed
    assert md2["duplicate_rows_removed"].value == 0


@requires_duckdb
def test_rerun_with_append_write_disposition_accumulates_at_the_destination_but_the_dedupe_safety_net_still_catches_it(mod, monkeypatch, tmp_path):
    """'append' is a real, working opt-in: the destination TABLE really does accumulate every
    run's rows (unlike 'replace'/'merge'). But the per-row dedupe safety net (asked for
    independently of write_disposition) still catches it before the DataFrame is returned, so
    row_count stays correct either way -- duplicate_rows_removed is what actually reveals that
    'append' is accumulating underneath."""
    _reset_dlt_pipeline_dir("rerun_append_asset_pipeline")
    monkeypatch.chdir(tmp_path)

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
    component = mod.SitemapCrawlerIngestionComponent(
        asset_name="rerun_append_asset", sitemap_url="https://example.com/sitemap.xml",
        include_preview_metadata=False, request_delay_seconds=0, write_disposition="append",
    )
    result1 = _materialize(component)
    md1 = result1.asset_materializations_for_node("rerun_append_asset")[0].metadata
    assert md1["row_count"].value == 2
    assert md1["duplicate_rows_removed"].value == 0

    result2 = _materialize(component)
    md2 = result2.asset_materializations_for_node("rerun_append_asset")[0].metadata
    assert md2["row_count"].value == 2  # the safety net still returns the current 2 pages...
    assert md2["duplicate_rows_removed"].value == 2  # ...but reports that the destination had accumulated to 4
