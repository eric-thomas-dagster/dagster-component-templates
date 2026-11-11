"""Tests for ConfluenceDocumentsComponent.

`dlt.pipeline(...)` is faked (see conftest.py); the REAL `@dlt.resource` object dlt
builds is still exercised via `fake_pipeline.run_calls[-1]`. The one real "paid call"
this component makes -- `requests.get` against Confluence's real REST endpoints -- is
mocked via `install_fake_requests`.
"""
import base64
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
def _creds_env(monkeypatch):
    monkeypatch.setenv("CONFLUENCE_EMAIL", "person@example.com")
    monkeypatch.setenv("CONFLUENCE_API_TOKEN", "test-token-123")


def _build_component(mod, **overrides):
    defaults = dict(
        asset_name="test_asset",
        base_url="https://yoursite.atlassian.net",
        email_env_var="CONFLUENCE_EMAIL",
        api_token_env_var="CONFLUENCE_API_TOKEN",
        space_key="ENG",
        include_preview_metadata=False,
    )
    defaults.update(overrides)
    return mod.ConfluenceDocumentsComponent(**defaults)


def _materialize(component):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})


def _page(page_id, title, html, webui, when="2024-01-01T00:00:00.000Z", body_format="storage"):
    return {
        "id": page_id,
        "title": title,
        "body": {body_format: {"value": html, "representation": body_format}},
        "version": {"when": when, "number": 1},
        "_links": {"webui": webui, "self": f"https://yoursite.atlassian.net/wiki/rest/api/content/{page_id}"},
    }


# --- validation ----------------------------------------------------------------------

def test_requires_exactly_one_of_space_key_or_ancestor_page_id(mod):
    with pytest.raises(pydantic.ValidationError, match="space_key|ancestor_page_id"):
        mod.ConfluenceDocumentsComponent(
            asset_name="test_asset",
            base_url="https://yoursite.atlassian.net",
            email_env_var="CONFLUENCE_EMAIL",
            api_token_env_var="CONFLUENCE_API_TOKEN",
        )


def test_rejects_both_space_key_and_ancestor_page_id(mod):
    with pytest.raises(pydantic.ValidationError, match="space_key|ancestor_page_id"):
        mod.ConfluenceDocumentsComponent(
            asset_name="test_asset",
            base_url="https://yoursite.atlassian.net",
            email_env_var="CONFLUENCE_EMAIL",
            api_token_env_var="CONFLUENCE_API_TOKEN",
            space_key="ENG",
            ancestor_page_id="123456",
        )


def test_missing_email_env_var_raises(mod, monkeypatch):
    monkeypatch.delenv("CONFLUENCE_EMAIL", raising=False)
    install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod)
    with pytest.raises(Exception, match="CONFLUENCE_EMAIL"):
        _materialize(component)


def test_missing_api_token_env_var_raises(mod, monkeypatch):
    monkeypatch.delenv("CONFLUENCE_API_TOKEN", raising=False)
    install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod)
    with pytest.raises(Exception, match="CONFLUENCE_API_TOKEN"):
        _materialize(component)


# --- space mode: listing + body fetch + HTML-to-text + auth header ---------------------

def test_space_mode_builds_expected_row_and_sends_basic_auth(mod, monkeypatch):
    def fake_get(url, params=None, headers=None, timeout=None):
        assert url.endswith("/wiki/rest/api/content")
        assert params["spaceKey"] == "ENG"
        assert params["type"] == "page"
        assert params["expand"] == "body.storage,version"
        expected_token = base64.b64encode(b"person@example.com:test-token-123").decode("ascii")
        assert headers["Authorization"] == f"Basic {expected_token}"
        return FakeResponse(json_body={
            "results": [_page("111", "Runbook", "<p>Hello <b>world</b></p>", "/wiki/spaces/ENG/pages/111/Runbook")],
        })

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod)
    result = _materialize(component)
    assert result.success

    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1
    row = rows[0]
    assert row["title"] == "Runbook"
    assert "Hello" in row["content"] and "world" in row["content"]
    assert "<b>" not in row["content"]
    assert row["url"] == "https://yoursite.atlassian.net/wiki/spaces/ENG/pages/111/Runbook"


def test_space_mode_emits_the_document_source_contract_columns(mod, monkeypatch):
    def fake_get(url, params=None, headers=None, timeout=None):
        return FakeResponse(json_body={
            "results": [_page("111", "Runbook", "<p>Some content</p>", "/wiki/spaces/ENG/pages/111/Runbook")],
        })

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod)
    result = _materialize(component)
    assert result.success

    row = list(fake_pipeline.run_calls[-1][0])[0]
    assert row["doc_id"] == "111"
    assert "Some content" in row["content"]
    assert row["title"] == "Runbook"
    assert row["url"]
    assert row["source_type"] == "confluence"
    assert row["updated_at"] == "2024-01-01T00:00:00.000Z"
    assert row["content_hash"] == hashlib.sha256(row["content"].encode("utf-8")).hexdigest()


def test_space_mode_paginates_via_start_limit(mod, monkeypatch):
    calls = []

    def fake_get(url, params=None, headers=None, timeout=None):
        calls.append(dict(params))
        start = params["start"]
        if start == 0:
            return FakeResponse(json_body={
                "results": [_page(str(i), f"Page {i}", "<p>x</p>", f"/wiki/spaces/ENG/pages/{i}") for i in range(2)],
            })
        return FakeResponse(json_body={"results": []})

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, page_size=2)
    result = _materialize(component)
    assert result.success

    assert [c["start"] for c in calls] == [0, 2]
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 2


def test_space_mode_respects_max_pages(mod, monkeypatch):
    def fake_get(url, params=None, headers=None, timeout=None):
        return FakeResponse(json_body={
            "results": [_page(str(i), f"Page {i}", "<p>x</p>", f"/wiki/spaces/ENG/pages/{i}") for i in range(10)],
        })

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, page_size=10, max_pages=3)
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 3


def test_body_format_view_reads_body_view(mod, monkeypatch):
    def fake_get(url, params=None, headers=None, timeout=None):
        assert params["expand"] == "body.view,version"
        return FakeResponse(json_body={
            "results": [_page("111", "Runbook", "<p>view text</p>", "/wiki/x", body_format="view")],
        })

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, body_format="view")
    result = _materialize(component)
    assert result.success
    row = list(fake_pipeline.run_calls[-1][0])[0]
    assert "view text" in row["content"]


# --- ancestor mode: CQL search + cursor pagination --------------------------------------

def test_ancestor_mode_uses_cql_search(mod, monkeypatch):
    def fake_get(url, params=None, headers=None, timeout=None):
        assert url.endswith("/wiki/rest/api/content/search")
        assert params["cql"] == "ancestor=123456"
        return FakeResponse(json_body={
            "results": [_page("222", "Child Page", "<p>child content</p>", "/wiki/spaces/ENG/pages/222")],
            "_links": {},
        })

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, space_key=None, ancestor_page_id="123456")
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1
    assert rows[0]["doc_id"] == "222"


def test_ancestor_mode_follows_links_next_cursor(mod, monkeypatch):
    def fake_get(url, params=None, headers=None, timeout=None):
        if url.endswith("/wiki/rest/api/content/search"):
            return FakeResponse(json_body={
                "results": [_page("1", "P1", "<p>one</p>", "/wiki/1")],
                "_links": {"next": "/wiki/rest/api/content/search?cql=ancestor%3D123456&cursor=abc"},
            })
        assert url == "https://yoursite.atlassian.net/wiki/rest/api/content/search?cql=ancestor%3D123456&cursor=abc"
        return FakeResponse(json_body={
            "results": [_page("2", "P2", "<p>two</p>", "/wiki/2")],
            "_links": {},
        })

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, space_key=None, ancestor_page_id="123456")
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 2
    assert {r["doc_id"] for r in rows} == {"1", "2"}


# --- dlt-advanced-options wiring (real DltResource, faked pipeline) ---------------------

def test_hash_columns_pseudonymizes_the_real_resource(mod, monkeypatch):
    def fake_get(url, params=None, headers=None, timeout=None):
        return FakeResponse(json_body={
            "results": [_page("111", "Secret Title", "<p>x</p>", "/wiki/x")],
        })

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, hash_columns=["title"], hash_salt="pepper")
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    expected = hashlib.sha256(("Secret Title" + "pepper").encode()).hexdigest()
    assert rows[0]["title"] == expected


def test_hash_columns_without_salt_raises(mod, monkeypatch):
    def fake_get(url, params=None, headers=None, timeout=None):
        return FakeResponse(json_body={"results": [_page("111", "T", "<p>x</p>", "/wiki/x")]})

    install_fake_requests(monkeypatch, mod, get=fake_get)
    install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, hash_columns=["title"])
    with pytest.raises(Exception):
        _materialize(component)


def test_exclude_columns_drops_the_column_from_the_real_resource(mod, monkeypatch):
    def fake_get(url, params=None, headers=None, timeout=None):
        return FakeResponse(json_body={"results": [_page("111", "T", "<p>x</p>", "/wiki/x")]})

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, exclude_columns=["title"])
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert "title" not in rows[0]


def test_max_rows_per_resource_caps_rows_on_the_real_resource(mod, monkeypatch):
    def fake_get(url, params=None, headers=None, timeout=None):
        return FakeResponse(json_body={
            "results": [_page(str(i), f"P{i}", "<p>x</p>", f"/wiki/{i}") for i in range(3)],
        })

    install_fake_requests(monkeypatch, mod, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, max_rows_per_resource=1)
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1


def test_on_schema_change_applies_schema_contract_hint_to_the_real_resource(mod, monkeypatch):
    def fake_get(url, params=None, headers=None, timeout=None):
        return FakeResponse(json_body={"results": [_page("111", "T", "<p>x</p>", "/wiki/x")]})

    install_fake_requests(monkeypatch, mod, get=fake_get)
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


# --- destination handling / metadata assembly (via FakeSqlClient readback) --------------

def test_destination_persist_only_emits_materialize_result(mod, monkeypatch):
    def fake_get(url, params=None, headers=None, timeout=None):
        return FakeResponse(json_body={"results": [_page("111", "T", "<p>x</p>", "/wiki/x")]})

    install_fake_requests(monkeypatch, mod, get=fake_get)
    install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, destination="duckdb", persist_only=True)
    result = _materialize(component)
    assert result.success
    mats = result.asset_materializations_for_node("test_asset")
    assert len(mats) == 1
    assert mats[0].metadata["dataset_name"].value == "test_asset"
    assert mats[0].metadata["pages_fetched"].value == 1


def test_filesystem_destination_without_persist_only_warns_and_still_succeeds(mod, monkeypatch, tmp_path):
    def fake_get(url, params=None, headers=None, timeout=None):
        return FakeResponse(json_body={"results": [_page("111", "T", "<p>x</p>", "/wiki/x")]})

    install_fake_requests(monkeypatch, mod, get=fake_get)
    install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, destination="filesystem", bucket_url=f"file://{tmp_path}")
    result = _materialize(component)
    assert result.success
    mats = result.asset_materializations_for_node("test_asset")
    assert mats[0].metadata["destination"].value == "filesystem"


def test_no_destination_reads_back_from_sql_client_into_a_dataframe(mod, monkeypatch):
    def fake_get(url, params=None, headers=None, timeout=None):
        return FakeResponse(json_body={
            "results": [_page("111", "T", "<p>hello</p>", "/wiki/x")],
        })

    install_fake_requests(monkeypatch, mod, get=fake_get)
    table_rows = {"pages": pd.DataFrame({
        "doc_id": ["111"], "title": ["T"], "url": ["https://yoursite.atlassian.net/wiki/x"],
        "source_type": ["confluence"], "updated_at": ["2024-01-01T00:00:00.000Z"],
        "content": ["hello"], "content_hash": ["abc"],
    })}
    install_fake_dlt(monkeypatch, mod, table_rows=table_rows)
    component = _build_component(mod)
    result = _materialize(component)
    assert result.success
    df = result.output_for_node("test_asset")
    assert {"doc_id", "title", "url", "source_type", "updated_at", "content", "content_hash"}.issubset(set(df.columns))
    assert len(df) == 1
