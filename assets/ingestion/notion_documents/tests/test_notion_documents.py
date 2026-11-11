"""Tests for NotionDocumentsComponent.

`dlt.pipeline(...)` is faked (see conftest.py); the REAL `@dlt.resource` object dlt
builds is still exercised, so hash_columns/exclude_columns/max_rows_per_resource/
on_schema_change are verified by iterating the real resource the component built
(`fake_pipeline.run_calls[-1]`). The one real "paid call" this component makes --
`requests.post`/`requests.get` against Notion's real endpoints (`POST /v1/databases/
{id}/query`, `GET /v1/blocks/{id}/children`, `GET /v1/pages/{id}`) -- is mocked via
`install_fake_requests`.
"""
import hashlib

import dagster as dg
import pydantic
import pytest

from .conftest import (
    FakeResponse,
    install_fake_dlt,
    install_fake_requests,
    load_component_module,
    notion_block,
    notion_page_obj,
)


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture(autouse=True)
def _api_key_env(monkeypatch):
    monkeypatch.setenv("NOTION_API_KEY", "test-key-123")


def _build_component(mod, **overrides):
    defaults = dict(
        asset_name="test_asset",
        database_id="db-1",
        include_preview_metadata=False,
    )
    defaults.update(overrides)
    return mod.NotionDocumentsComponent(**defaults)


def _materialize(component):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})


def _route_requests(monkeypatch, mod, *, db_pages=None, blocks_children=None, pages_by_id=None,
                     db_has_more_sequence=None, blocks_has_more_sequence=None):
    """Wires fake_post (database query) + fake_get (block children / page retrieve) by
    URL, routing on the real Notion URL shapes this component calls."""
    db_pages = db_pages or {}
    blocks_children = blocks_children or {}
    pages_by_id = pages_by_id or {}

    def fake_post(url, json=None, headers=None, timeout=None):
        assert "/databases/" in url and url.endswith("/query")
        assert headers["Authorization"] == "Bearer test-key-123"
        assert headers["Notion-Version"] == mod.NOTION_VERSION
        db_id = url.split("/databases/")[1].split("/query")[0]
        return FakeResponse(json_body={
            "results": db_pages.get(db_id, []),
            "has_more": False,
            "next_cursor": None,
        })

    def fake_get(url, params=None, headers=None, timeout=None):
        assert headers["Authorization"] == "Bearer test-key-123"
        if "/blocks/" in url and url.endswith("/children"):
            block_id = url.split("/blocks/")[1].split("/children")[0]
            children = blocks_children.get(block_id, [])
            return FakeResponse(json_body={"results": children, "has_more": False, "next_cursor": None})
        if "/pages/" in url:
            page_id = url.split("/pages/")[1]
            return FakeResponse(json_body=pages_by_id[page_id])
        raise AssertionError(f"unexpected GET {url}")

    install_fake_requests(monkeypatch, mod, post=fake_post, get=fake_get)
    return fake_post, fake_get


# --- validation ----------------------------------------------------------------------

def test_requires_exactly_one_of_database_id_or_root_page_ids(mod):
    with pytest.raises(pydantic.ValidationError, match="database_id.*root_page_ids|root_page_ids.*database_id"):
        mod.NotionDocumentsComponent(asset_name="test_asset")


def test_rejects_both_database_id_and_root_page_ids(mod):
    with pytest.raises(pydantic.ValidationError, match="database_id.*root_page_ids|root_page_ids.*database_id"):
        mod.NotionDocumentsComponent(asset_name="test_asset", database_id="db-1", root_page_ids="page-1")


def test_missing_api_key_raises(mod, monkeypatch):
    monkeypatch.delenv("NOTION_API_KEY", raising=False)
    install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod)
    with pytest.raises(Exception, match="NOTION_API_KEY"):
        _materialize(component)


# --- database mode: listing + block-walking + markdown rendering -----------------------

def test_database_mode_renders_multiple_block_types_to_markdown(mod, monkeypatch):
    page = notion_page_obj("page-1", title="My Page")
    blocks = [
        notion_block("heading_1", rich_text=["Section One"]),
        notion_block("paragraph", rich_text=["Some body text."]),
        notion_block("bulleted_list_item", rich_text=["First bullet"]),
        notion_block("to_do", rich_text=["Finish the thing"], checked=True),
        notion_block("quote", rich_text=["A wise quote"]),
        notion_block("code", rich_text=["print('hi')"], language="python"),
        notion_block("divider"),  # no rich_text -- must be skipped, not crash
    ]
    _route_requests(
        monkeypatch, mod,
        db_pages={"db-1": [page]},
        blocks_children={"page-1": blocks},
    )
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, rate_limit_delay=0)
    result = _materialize(component)
    assert result.success

    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1
    content = rows[0]["content"]
    assert "# Section One" in content
    assert "Some body text." in content
    assert "- First bullet" in content
    assert "- [x] Finish the thing" in content
    assert "> A wise quote" in content
    assert "```python\nprint('hi')\n```" in content


def test_database_mode_paginates_block_children(mod, monkeypatch):
    page = notion_page_obj("page-1", title="Paged")

    def fake_post(url, json=None, headers=None, timeout=None):
        return FakeResponse(json_body={"results": [page], "has_more": False, "next_cursor": None})

    calls = {"n": 0}

    def fake_get(url, params=None, headers=None, timeout=None):
        assert "/blocks/page-1/children" in url
        calls["n"] += 1
        if params.get("start_cursor") is None:
            return FakeResponse(json_body={
                "results": [notion_block("paragraph", rich_text=["Page 1 text"])],
                "has_more": True,
                "next_cursor": "cursor-2",
            })
        assert params["start_cursor"] == "cursor-2"
        return FakeResponse(json_body={
            "results": [notion_block("paragraph", rich_text=["Page 2 text"])],
            "has_more": False,
            "next_cursor": None,
        })

    install_fake_requests(monkeypatch, mod, post=fake_post, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, rate_limit_delay=0)
    result = _materialize(component)
    assert result.success
    assert calls["n"] == 2
    rows = list(fake_pipeline.run_calls[-1][0])
    assert "Page 1 text" in rows[0]["content"]
    assert "Page 2 text" in rows[0]["content"]


def test_database_mode_paginates_database_query(mod, monkeypatch):
    page_a = notion_page_obj("page-a", title="A")
    page_b = notion_page_obj("page-b", title="B")

    calls = {"n": 0}

    def fake_post(url, json=None, headers=None, timeout=None):
        calls["n"] += 1
        if json.get("start_cursor") is None:
            return FakeResponse(json_body={"results": [page_a], "has_more": True, "next_cursor": "db-cursor-2"})
        assert json["start_cursor"] == "db-cursor-2"
        return FakeResponse(json_body={"results": [page_b], "has_more": False, "next_cursor": None})

    def fake_get(url, params=None, headers=None, timeout=None):
        return FakeResponse(json_body={"results": [], "has_more": False, "next_cursor": None})

    install_fake_requests(monkeypatch, mod, post=fake_post, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, rate_limit_delay=0)
    result = _materialize(component)
    assert result.success
    assert calls["n"] == 2
    rows = list(fake_pipeline.run_calls[-1][0])
    assert {r["doc_id"] for r in rows} == {"page-a", "page-b"}


def test_database_mode_emits_document_source_contract_columns(mod, monkeypatch):
    page = notion_page_obj("page-1", title="Contract Page", url="https://www.notion.so/page1",
                            last_edited_time="2025-06-01T12:00:00.000Z")
    _route_requests(
        monkeypatch, mod,
        db_pages={"db-1": [page]},
        blocks_children={"page-1": [notion_block("paragraph", rich_text=["hello world"])]},
    )
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, rate_limit_delay=0)
    result = _materialize(component)
    assert result.success

    row = list(fake_pipeline.run_calls[-1][0])[0]
    assert row["doc_id"] == "page-1"
    assert row["content"] == "hello world"
    assert row["title"] == "Contract Page"
    assert row["url"] == "https://www.notion.so/page1"
    assert row["source_type"] == "notion"
    assert row["updated_at"] == "2025-06-01T12:00:00.000Z"
    assert row["content_hash"] == hashlib.sha256("hello world".encode("utf-8")).hexdigest()


def test_nested_blocks_are_walked_recursively(mod, monkeypatch):
    page = notion_page_obj("page-1")
    parent_block = notion_block("bulleted_list_item", rich_text=["Parent item"], has_children=True, block_id="parent-block")
    child_block = notion_block("paragraph", rich_text=["Nested child text"])
    blocks_children = {
        "page-1": [parent_block],
        "parent-block": [child_block],
    }
    _route_requests(monkeypatch, mod, db_pages={"db-1": [page]}, blocks_children=blocks_children)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, rate_limit_delay=0)
    result = _materialize(component)
    assert result.success
    content = list(fake_pipeline.run_calls[-1][0])[0]["content"]
    assert "Parent item" in content
    assert "Nested child text" in content


# --- page-tree mode: recursion through child_page blocks --------------------------------

def test_page_tree_mode_recurses_into_child_pages(mod, monkeypatch):
    root_page = notion_page_obj("root-page", title="Root")
    child_page = notion_page_obj("child-page", title="Child")

    # child_page blocks carry a `title` string, not rich_text -- id must match the
    # real page id so _get_page(page_id, ...) in page-tree mode resolves correctly.
    child_page_block = notion_block("child_page", block_id="child-page", title="Child")

    blocks_children = {
        "root-page": [notion_block("paragraph", rich_text=["Root body"]), child_page_block],
        "child-page": [notion_block("paragraph", rich_text=["Child body"])],
    }
    _route_requests(
        monkeypatch, mod,
        blocks_children=blocks_children,
        pages_by_id={"root-page": root_page, "child-page": child_page},
    )
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, database_id=None, root_page_ids="root-page", rate_limit_delay=0)
    result = _materialize(component)
    assert result.success

    rows = list(fake_pipeline.run_calls[-1][0])
    assert {r["doc_id"] for r in rows} == {"root-page", "child-page"}
    root_row = next(r for r in rows if r["doc_id"] == "root-page")
    child_row = next(r for r in rows if r["doc_id"] == "child-page")
    assert "Root body" in root_row["content"]
    # child_page block itself contributes no markdown line to the parent's content
    assert "Child body" not in root_row["content"]
    assert "Child body" in child_row["content"]


def test_page_tree_mode_multiple_root_ids_deduplicates_shared_child(mod, monkeypatch):
    root_a = notion_page_obj("root-a")
    root_b = notion_page_obj("root-b")
    shared_child = notion_page_obj("shared-child")

    shared_child_block = {"object": "block", "id": "cp-1", "type": "child_page", "has_children": False, "child_page": {"title": "Shared"}}

    blocks_children = {
        "root-a": [shared_child_block],
        "root-b": [shared_child_block],
        "shared-child": [],
    }
    # the same child_page block id appears under both roots' children but points at the
    # same underlying page id via `_get_page` lookup
    shared_child_block["id"] = "shared-child"

    _route_requests(
        monkeypatch, mod,
        blocks_children=blocks_children,
        pages_by_id={"root-a": root_a, "root-b": root_b, "shared-child": shared_child},
    )
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, database_id=None, root_page_ids="root-a,root-b", rate_limit_delay=0)
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    doc_ids = [r["doc_id"] for r in rows]
    assert doc_ids.count("shared-child") == 1
    assert set(doc_ids) == {"root-a", "root-b", "shared-child"}


def test_page_tree_mode_ignores_child_database_by_default(mod, monkeypatch):
    root_page = notion_page_obj("root-page")
    child_db_block = {"object": "block", "id": "child-db-1", "type": "child_database", "has_children": False, "child_database": {"title": "Linked DB"}}
    _route_requests(
        monkeypatch, mod,
        blocks_children={"root-page": [child_db_block]},
        pages_by_id={"root-page": root_page},
    )
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(
        mod, database_id=None, root_page_ids="root-page", include_child_databases=False, rate_limit_delay=0,
    )
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1
    assert rows[0]["doc_id"] == "root-page"


def test_page_tree_mode_recurses_into_child_database_when_enabled(mod, monkeypatch):
    root_page = notion_page_obj("root-page")
    db_page = notion_page_obj("db-page-1", title="From linked DB")
    child_db_block = {"object": "block", "id": "child-db-1", "type": "child_database", "has_children": False, "child_database": {"title": "Linked DB"}}

    def fake_post(url, json=None, headers=None, timeout=None):
        assert "/databases/child-db-1/query" in url
        return FakeResponse(json_body={"results": [db_page], "has_more": False, "next_cursor": None})

    def fake_get(url, params=None, headers=None, timeout=None):
        if "/blocks/root-page/children" in url:
            return FakeResponse(json_body={"results": [child_db_block], "has_more": False, "next_cursor": None})
        if "/blocks/db-page-1/children" in url:
            return FakeResponse(json_body={"results": [notion_block("paragraph", rich_text=["DB page body"])], "has_more": False, "next_cursor": None})
        if url.endswith("/pages/root-page"):
            return FakeResponse(json_body=root_page)
        if url.endswith("/pages/db-page-1"):
            return FakeResponse(json_body=db_page)
        raise AssertionError(f"unexpected GET {url}")

    install_fake_requests(monkeypatch, mod, post=fake_post, get=fake_get)
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(
        mod, database_id=None, root_page_ids="root-page", include_child_databases=True, rate_limit_delay=0,
    )
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert {r["doc_id"] for r in rows} == {"root-page", "db-page-1"}
    db_row = next(r for r in rows if r["doc_id"] == "db-page-1")
    assert "DB page body" in db_row["content"]


# --- dlt-advanced-options wiring (real DltResource, faked pipeline) ---------------------

def test_hash_columns_pseudonymizes_the_real_resource(mod, monkeypatch):
    page = notion_page_obj("page-1", title="Secret Title")
    _route_requests(monkeypatch, mod, db_pages={"db-1": [page]}, blocks_children={"page-1": []})
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, hash_columns=["title"], hash_salt="pepper", rate_limit_delay=0)
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    expected = hashlib.sha256(("Secret Title" + "pepper").encode()).hexdigest()
    assert rows[0]["title"] == expected


def test_exclude_columns_drops_the_column_from_the_real_resource(mod, monkeypatch):
    page = notion_page_obj("page-1", title="T")
    _route_requests(monkeypatch, mod, db_pages={"db-1": [page]}, blocks_children={"page-1": []})
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, exclude_columns=["title"], rate_limit_delay=0)
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert "title" not in rows[0]


def test_max_rows_per_resource_caps_rows_on_the_real_resource(mod, monkeypatch):
    pages = [notion_page_obj(f"page-{i}") for i in range(3)]
    _route_requests(
        monkeypatch, mod,
        db_pages={"db-1": pages},
        blocks_children={p["id"]: [] for p in pages},
    )
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, max_rows_per_resource=1, rate_limit_delay=0)
    result = _materialize(component)
    assert result.success
    rows = list(fake_pipeline.run_calls[-1][0])
    assert len(rows) == 1


def test_on_schema_change_applies_schema_contract_hint_to_the_real_resource(mod, monkeypatch):
    page = notion_page_obj("page-1")
    _route_requests(monkeypatch, mod, db_pages={"db-1": [page]}, blocks_children={"page-1": []})
    fake_pipeline = install_fake_dlt(monkeypatch, mod)
    component = _build_component(mod, on_schema_change="block_all", rate_limit_delay=0)
    result = _materialize(component)
    assert result.success
    resource = fake_pipeline.run_calls[-1][0]
    assert resource.compute_table_schema()["schema_contract"] == {
        "tables": "evolve", "columns": "freeze", "data_type": "freeze",
    }


# --- markdown-rendering helper unit tests ------------------------------------------------

def test_block_to_markdown_renders_each_type(mod):
    assert mod._block_to_markdown(notion_block("heading_1", rich_text=["H1"])) == "# H1"
    assert mod._block_to_markdown(notion_block("heading_2", rich_text=["H2"])) == "## H2"
    assert mod._block_to_markdown(notion_block("heading_3", rich_text=["H3"])) == "### H3"
    assert mod._block_to_markdown(notion_block("bulleted_list_item", rich_text=["B"])) == "- B"
    assert mod._block_to_markdown(notion_block("numbered_list_item", rich_text=["N"])) == "1. N"
    assert mod._block_to_markdown(notion_block("to_do", rich_text=["T"], checked=False)) == "- [ ] T"
    assert mod._block_to_markdown(notion_block("to_do", rich_text=["T"], checked=True)) == "- [x] T"
    assert mod._block_to_markdown(notion_block("quote", rich_text=["Q"])) == "> Q"
    assert mod._block_to_markdown(notion_block("code", rich_text=["c = 1"], language="python")) == "```python\nc = 1\n```"
    assert mod._block_to_markdown(notion_block("paragraph", rich_text=["P"])) == "P"


def test_block_to_markdown_returns_none_for_non_rich_text_types(mod):
    assert mod._block_to_markdown(notion_block("divider")) is None
    child_page_block = {"object": "block", "id": "x", "type": "child_page", "has_children": False, "child_page": {"title": "X"}}
    assert mod._block_to_markdown(child_page_block) is None
    child_db_block = {"object": "block", "id": "y", "type": "child_database", "has_children": False, "child_database": {"title": "Y"}}
    assert mod._block_to_markdown(child_db_block) is None


def test_extract_title_finds_the_title_typed_property(mod):
    props = {
        "Status": {"type": "select", "select": {"name": "Done"}},
        "Name": {"type": "title", "title": [{"plain_text": "Hello "}, {"plain_text": "World"}]},
    }
    assert mod._extract_title(props) == "Hello World"
    assert mod._extract_title(None) == ""
    assert mod._extract_title({}) == ""
