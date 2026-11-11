"""Tests for FirecrawlIngestionComponent.

`dlt.pipeline(...)` is faked (see conftest.py -- dlt 1.30.0's duckdb destination
rejects a bare `credentials=":memory:"` string, so a real in-memory pipeline isn't
viable here); the REAL `@dlt.resource` object dlt builds is still exercised, so
hash_columns/exclude_columns/max_rows_per_resource/on_schema_change are verified by
iterating the real resource the component built (`fake_pipeline.run_calls[-1]`), not a
hand-rolled stand-in. The one real "paid call" this component makes -- `requests.post`/
`requests.get` against the Firecrawl API -- is mocked via `install_fake_requests`.
"""
import hashlib

import dagster as dg
import pandas as pd
import pydantic
import pytest

from .conftest import FakeResponse, install_fake_dlt, install_fake_requests, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture(autouse=True)
def _api_key_env(monkeypatch):
    monkeypatch.setenv("FIRECRAWL_API_KEY", "test-key-123")


def _build_component(mod, **overrides):
    defaults = dict(
        asset_name="test_asset",
        urls="https://example.com/a",
        api_key_env_var="FIRECRAWL_API_KEY",
        include_preview_metadata=False,
    )
    defaults.update(overrides)
    return mod.FirecrawlIngestionComponent(**defaults)


def _materialize(component):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})


# --- validation ----------------------------------------------------------------------

def test_requires_urls_or_crawl_url(mod):
    with pytest.raises(pydantic.ValidationError, match="urls.*crawl_url|crawl_url.*urls"):
        mod.FirecrawlIngestionComponent(asset_name="test_asset")


def test_missing_api_key_raises(mod, monkeypatch):
    monkeypatch.delenv("FIRECRAWL_API_KEY", raising=False)
    install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod)
    with pytest.raises(Exception, match="FIRECRAWL_API_KEY"):
        _materialize(component)


# --- scrape mode -----------------------------------------------------------------------

def test_scrape_mode_single_url_builds_expected_row(mod, monkeypatch):
    def fake_post(url, json=None, headers=None, timeout=None):
        assert url.endswith("/scrape")
        assert json["url"] == "https://example.com/a"
        assert json["formats"] == ["markdown"]
        assert headers["Authorization"] == "Bearer test-key-123"
        return FakeResponse(json_body={
            "success": True,
            "data": {
                "markdown": "# Hello\n\nworld",
                "metadata": {"title": "Hello Page", "sourceURL": "https://example.com/a"},
            },
        })

    install_fake_requests(monkeypatch, mod, post=fake_post)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, urls="https://example.com/a")
    result = _materialize(component)
    assert result.success

    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1
    assert rows[0]["url"] == "https://example.com/a"
    assert rows[0]["title"] == "Hello Page"
    assert "world" in rows[0]["markdown"]
    assert rows[0]["scraped_at"]


def test_scrape_mode_emits_the_document_source_contract_columns(mod, monkeypatch):
    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse(json_body={
            "success": True,
            "data": {
                "markdown": "# Hello\n\nworld",
                "metadata": {"title": "Hello Page", "sourceURL": "https://example.com/a"},
            },
        })

    install_fake_requests(monkeypatch, mod, post=fake_post)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, urls="https://example.com/a")
    result = _materialize(component)
    assert result.success

    row = list(fake_pipeline.run_calls[-1][0])[0]
    assert row["doc_id"] == "https://example.com/a"
    assert "world" in row["content"]
    assert row["source_type"] == "firecrawl"
    assert row["updated_at"]
    assert row["content_hash"]


def test_scrape_mode_multiple_urls_returns_one_row_each(mod, monkeypatch):
    def fake_post(url, json=None, headers=None, timeout=None):
        requested = json["url"]
        return FakeResponse(json_body={
            "success": True,
            "data": {
                "markdown": f"content for {requested}",
                "metadata": {"title": f"Title for {requested}", "sourceURL": requested},
            },
        })

    install_fake_requests(monkeypatch, mod, post=fake_post)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, urls="https://example.com/a,https://example.com/b")
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 2
    assert {r["url"] for r in rows} == {"https://example.com/a", "https://example.com/b"}


def test_scrape_failure_response_raises(mod, monkeypatch):
    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse(json_body={"success": False, "error": "no such page"})

    install_fake_requests(monkeypatch, mod, post=fake_post)
    install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, urls="https://example.com/a")
    with pytest.raises(Exception, match="scrape failed"):
        _materialize(component)


# --- crawl mode ------------------------------------------------------------------------

def test_crawl_mode_immediate_completion_builds_rows(mod, monkeypatch):
    def fake_post(url, json=None, headers=None, timeout=None):
        assert url.endswith("/crawl")
        assert json["url"] == "https://docs.example.com"
        assert json["limit"] == 50
        return FakeResponse(json_body={"success": True, "id": "crawl-1", "url": json["url"]})

    def fake_get(url, headers=None, timeout=None):
        assert url.endswith("/crawl/crawl-1")
        return FakeResponse(json_body={
            "status": "completed",
            "total": 2,
            "completed": 2,
            "data": [
                {"markdown": "# Page 1", "metadata": {"title": "P1", "sourceURL": "https://docs.example.com/p1"}},
                {"markdown": "# Page 2", "metadata": {"title": "P2", "sourceURL": "https://docs.example.com/p2"}},
            ],
            "next": None,
        })

    install_fake_requests(monkeypatch, mod, post=fake_post, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, urls=None, crawl_url="https://docs.example.com", crawl_limit=50)
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 2
    assert {r["url"] for r in rows} == {"https://docs.example.com/p1", "https://docs.example.com/p2"}


def test_crawl_mode_polls_until_completed(mod, monkeypatch):
    calls = {"n": 0}

    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse(json_body={"success": True, "id": "crawl-2", "url": json["url"]})

    def fake_get(url, headers=None, timeout=None):
        calls["n"] += 1
        if calls["n"] < 3:
            return FakeResponse(json_body={"status": "scraping", "total": 1, "completed": 0})
        return FakeResponse(json_body={
            "status": "completed",
            "total": 1,
            "completed": 1,
            "data": [{"markdown": "# Done", "metadata": {"sourceURL": "https://docs.example.com/x"}}],
            "next": None,
        })

    install_fake_requests(monkeypatch, mod, post=fake_post, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    component = _build_component(mod, urls=None, crawl_url="https://docs.example.com", poll_interval_seconds=1)
    result = _materialize(component)
    assert result.success
    assert calls["n"] == 3
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1


def test_crawl_mode_follows_next_pagination(mod, monkeypatch):
    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse(json_body={"success": True, "id": "crawl-3", "url": json["url"]})

    def fake_get(url, headers=None, timeout=None):
        if url.endswith("/crawl/crawl-3"):
            return FakeResponse(json_body={
                "status": "completed",
                "total": 2,
                "completed": 2,
                "data": [{"markdown": "page one", "metadata": {"sourceURL": "https://docs.example.com/1"}}],
                "next": "https://api.firecrawl.dev/v2/crawl/crawl-3?skip=1",
            })
        assert url == "https://api.firecrawl.dev/v2/crawl/crawl-3?skip=1"
        return FakeResponse(json_body={
            "data": [{"markdown": "page two", "metadata": {"sourceURL": "https://docs.example.com/2"}}],
            "next": None,
        })

    install_fake_requests(monkeypatch, mod, post=fake_post, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, urls=None, crawl_url="https://docs.example.com")
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 2
    assert {r["url"] for r in rows} == {"https://docs.example.com/1", "https://docs.example.com/2"}


def test_crawl_mode_failed_status_raises(mod, monkeypatch):
    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse(json_body={"success": True, "id": "crawl-4", "url": json["url"]})

    def fake_get(url, headers=None, timeout=None):
        return FakeResponse(json_body={"status": "failed", "error": "robots.txt disallowed"})

    install_fake_requests(monkeypatch, mod, post=fake_post, get=fake_get)
    install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, urls=None, crawl_url="https://docs.example.com")
    with pytest.raises(Exception, match="failed"):
        _materialize(component)


def test_crawl_mode_timeout_raises(mod, monkeypatch):
    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse(json_body={"success": True, "id": "crawl-5", "url": json["url"]})

    def fake_get(url, headers=None, timeout=None):
        return FakeResponse(json_body={"status": "scraping", "total": 1, "completed": 0})

    install_fake_requests(monkeypatch, mod, post=fake_post, get=fake_get)
    install_fake_dlt(monkeypatch, mod)
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    # `mod.time` is the same global `time` module every other import sees (including
    # asyncio's own internal event-loop clock), so the fake must never raise
    # StopIteration once exhausted -- it returns 0, 0, then 700 forever after.
    _clock_values = [0, 0]

    def _fake_monotonic():
        return _clock_values.pop(0) if _clock_values else 700

    monkeypatch.setattr(mod.time, "monotonic", _fake_monotonic)
    component = _build_component(
        mod, urls=None, crawl_url="https://docs.example.com",
        poll_timeout_seconds=10, poll_interval_seconds=1,
    )
    with pytest.raises(TimeoutError, match="did not reach status='completed'"):
        _materialize(component)


# --- dlt-advanced-options wiring (real DltResource, faked pipeline) ---------------------

def test_hash_columns_pseudonymizes_the_real_resource(mod, monkeypatch):
    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse(json_body={
            "success": True,
            "data": {"markdown": "body", "metadata": {"title": "Secret Title", "sourceURL": "https://example.com/a"}},
        })

    install_fake_requests(monkeypatch, mod, post=fake_post)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, hash_columns=["title"], hash_salt="pepper")
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    expected = hashlib.sha256(("Secret Title" + "pepper").encode()).hexdigest()
    assert rows[0]["title"] == expected


def test_hash_columns_without_salt_raises(mod, monkeypatch):
    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse(json_body={"success": True, "data": {"markdown": "x", "metadata": {}}})

    install_fake_requests(monkeypatch, mod, post=fake_post)
    install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, hash_columns=["title"])
    with pytest.raises(Exception):
        _materialize(component)


def test_exclude_columns_drops_the_column_from_the_real_resource(mod, monkeypatch):
    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse(json_body={
            "success": True,
            "data": {"markdown": "body", "metadata": {"title": "T", "sourceURL": "https://example.com/a"}},
        })

    install_fake_requests(monkeypatch, mod, post=fake_post)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, exclude_columns=["title"])
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert "title" not in rows[0]


def test_max_rows_per_resource_caps_rows_on_the_real_resource(mod, monkeypatch):
    def fake_post(url, json=None, headers=None, timeout=None):
        requested = json["url"]
        return FakeResponse(json_body={
            "success": True,
            "data": {"markdown": requested, "metadata": {"sourceURL": requested}},
        })

    install_fake_requests(monkeypatch, mod, post=fake_post)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(
        mod, urls="https://example.com/a,https://example.com/b,https://example.com/c",
        max_rows_per_resource=1,
    )
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1


def test_on_schema_change_applies_schema_contract_hint_to_the_real_resource(mod, monkeypatch):
    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse(json_body={"success": True, "data": {"markdown": "a", "metadata": {}}})

    install_fake_requests(monkeypatch, mod, post=fake_post)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, on_schema_change="block_all")
    result = _materialize(component)
    assert result.success
    resource = fake_pipeline.run_calls[-1][0]
    assert resource.compute_table_schema()["schema_contract"] == {
        "tables": "evolve", "columns": "freeze", "data_type": "freeze",
    }


def test_on_schema_change_rejects_invalid_value(mod):
    with pytest.raises(Exception):
        _build_component(mod, on_schema_change="bogus")


def test_resolve_schema_contract_maps_each_value(mod):
    assert mod._resolve_schema_contract("allow_all") == {"tables": "evolve", "columns": "evolve", "data_type": "evolve"}
    assert mod._resolve_schema_contract("allow_new_columns") == {"tables": "evolve", "columns": "evolve", "data_type": "freeze"}
    assert mod._resolve_schema_contract("block_all") == {"tables": "evolve", "columns": "freeze", "data_type": "freeze"}
    with pytest.raises(ValueError, match="on_schema_change"):
        mod._resolve_schema_contract("bogus")


def test_make_exclude_columns_map_drops_listed_columns_and_skips_missing(mod):
    fn = mod._make_exclude_columns_map(["ssn", "missing"])
    assert fn({"id": 1, "ssn": "x", "name": "kept"}) == {"id": 1, "name": "kept"}


# --- destination handling / metadata assembly (via FakeSqlClient readback) --------------

def test_destination_persist_only_emits_materialize_result(mod, monkeypatch):
    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse(json_body={"success": True, "data": {"markdown": "a", "metadata": {}}})

    install_fake_requests(monkeypatch, mod, post=fake_post)
    install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, destination="duckdb", persist_only=True)
    result = _materialize(component)
    assert result.success
    mats = result.asset_materializations_for_node("test_asset")
    assert len(mats) == 1
    assert mats[0].metadata["dataset_name"].value == "test_asset"


def test_filesystem_destination_without_persist_only_warns_and_still_succeeds(mod, monkeypatch, tmp_path):
    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse(json_body={"success": True, "data": {"markdown": "a", "metadata": {}}})

    install_fake_requests(monkeypatch, mod, post=fake_post)
    install_fake_dlt(monkeypatch, mod)
    component = _build_component(
        mod, destination="filesystem", bucket_url=f"file://{tmp_path}",
    )
    result = _materialize(component)
    assert result.success
    mats = result.asset_materializations_for_node("test_asset")
    assert mats[0].metadata["destination"].value == "filesystem"


def test_no_destination_reads_back_from_sql_client_into_a_dataframe(mod, monkeypatch):
    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse(json_body={
            "success": True,
            "data": {"markdown": "hello", "metadata": {"title": "T", "sourceURL": "https://example.com/a"}},
        })

    install_fake_requests(monkeypatch, mod, post=fake_post)
    table_rows = {"pages": pd.DataFrame({
        "url": ["https://example.com/a"], "title": ["T"],
        "scraped_at": ["2024-01-01T00:00:00+00:00"], "markdown": ["hello"],
    })}
    install_fake_dlt(monkeypatch, mod, table_rows=table_rows)
    component = _build_component(mod)
    result = _materialize(component)
    assert result.success
    df = result.output_for_node("test_asset")
    assert {"url", "title", "scraped_at", "markdown"}.issubset(set(df.columns))
    assert len(df) == 1
