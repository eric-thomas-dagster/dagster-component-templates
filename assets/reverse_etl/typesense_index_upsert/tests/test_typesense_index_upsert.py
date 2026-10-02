"""Committed regression tests for TypesenseIndexUpsertComponent.

The real Typesense cluster is never hit here -- `_call_typesense_import_api`
and `_call_typesense_delete_api` (the two external, network boundaries)
are monkeypatched wholesale, while document building (the id_field -> 'id'
mapping, which -- unlike Algolia/OpenSearch -- lives INSIDE the document
body), dual source resolution, validation, chunking, the per-document
delete loop, and metadata are all exercised for real.
"""
import json

import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeTypesenseResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def recorded_import_calls(mod, monkeypatch):
    calls = []

    def _fake_call(resource, collection_name, ndjson_body, action):
        docs = [json.loads(line) for line in ndjson_body.strip("\n").split("\n") if line]
        calls.append({"collection_name": collection_name, "action": action, "docs": docs})
        return [{"success": True} for _ in docs]

    monkeypatch.setattr(mod, "_call_typesense_import_api", _fake_call)
    return calls


@pytest.fixture()
def recorded_delete_calls(mod, monkeypatch):
    calls = []

    def _fake_delete(resource, collection_name, doc_id):
        calls.append({"collection_name": collection_name, "doc_id": doc_id})
        return {"id": doc_id}

    monkeypatch.setattr(mod, "_call_typesense_delete_api", _fake_delete)
    return calls


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_catalog", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"typesense_resource": resource})


# --- document building (pure) --------------------------------------------

def test_build_document_puts_id_inside_body_as_string(mod):
    row = {"product_id": 42, "name": "Widget", "price": 9.99}
    doc_id, doc = mod._build_document(row, "product_id")
    assert doc_id == 42
    assert doc["id"] == "42"
    assert isinstance(doc["id"], str)
    assert doc["name"] == "Widget"
    assert "product_id" not in doc


def test_build_document_returns_none_for_missing_id(mod):
    doc_id, doc = mod._build_document({"product_id": None, "name": "Widget"}, "product_id")
    assert doc_id is None
    assert doc == {}


def test_build_document_returns_none_for_nan_id(mod):
    doc_id, _ = mod._build_document({"product_id": float("nan")}, "product_id")
    assert doc_id is None


# --- NDJSON building (pure) -------------------------------------------------

def test_build_ndjson_body_one_json_object_per_line(mod):
    body = mod._build_ndjson_body([{"id": "1", "name": "A"}, {"id": "2", "name": "B"}])
    lines = body.strip("\n").split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"id": "1", "name": "A"}
    assert json.loads(lines[1]) == {"id": "2", "name": "B"}
    assert body.endswith("\n")


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.TypesenseIndexUpsertComponent(
            asset_name="x", collection_name="products", id_field="product_id",
        ).build_defs(context=None)


def test_invalid_operation_raises(mod):
    with pytest.raises(ValueError, match="operation must be"):
        mod.TypesenseIndexUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            collection_name="products",
            id_field="product_id",
            operation="add",
        ).build_defs(context=None)


def test_missing_id_field_column_raises_failure(mod):
    df = pd.DataFrame({"name": ["Widget"]})
    resource = FakeTypesenseResource()
    component = mod.TypesenseIndexUpsertComponent(
        asset_name="typesense_out",
        upstream_asset_key="upstream_catalog",
        collection_name="products",
        id_field="product_id",
    )
    with pytest.raises(Exception, match="product_id"):
        _materialize(component, df, resource)


# --- full asset body, upsert path -------------------------------------------

def test_upsert_operation_end_to_end(mod, recorded_import_calls):
    df = pd.DataFrame({"product_id": ["p1", "p2"], "name": ["Widget", "Gadget"]})
    resource = FakeTypesenseResource()
    component = mod.TypesenseIndexUpsertComponent(
        asset_name="typesense_out",
        upstream_asset_key="upstream_catalog",
        collection_name="products",
        id_field="product_id",
        operation="upsert",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_import_calls) == 1
    call = recorded_import_calls[0]
    assert call["collection_name"] == "products"
    assert call["action"] == "upsert"
    assert call["docs"][0] == {"id": "p1", "name": "Widget"}

    out = metadata_for(result, "typesense_out")
    assert out["rows_submitted"] == 2
    assert out["operation"] == "upsert"
    assert out["api_requests"] == 1


def test_rows_with_no_id_are_skipped_and_counted(mod, recorded_import_calls):
    df = pd.DataFrame({"product_id": ["p1", None], "name": ["Widget", "Gadget"]})
    resource = FakeTypesenseResource()
    component = mod.TypesenseIndexUpsertComponent(
        asset_name="typesense_out",
        upstream_asset_key="upstream_catalog",
        collection_name="products",
        id_field="product_id",
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "typesense_out")
    assert out["rows_skipped_no_id"] == 1
    assert out["rows_submitted"] == 1


def test_empty_upstream_makes_no_api_call(mod, recorded_import_calls):
    df = pd.DataFrame({"product_id": [], "name": []})
    resource = FakeTypesenseResource()
    component = mod.TypesenseIndexUpsertComponent(
        asset_name="typesense_out",
        upstream_asset_key="upstream_catalog",
        collection_name="products",
        id_field="product_id",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_import_calls == []


def test_docs_chunked_at_docs_per_import_request(mod, recorded_import_calls, monkeypatch):
    monkeypatch.setattr(mod, "_DOCS_PER_IMPORT_REQUEST", 2)
    df = pd.DataFrame({"product_id": [f"p{i}" for i in range(5)], "name": [f"n{i}" for i in range(5)]})
    resource = FakeTypesenseResource()
    component = mod.TypesenseIndexUpsertComponent(
        asset_name="typesense_out",
        upstream_asset_key="upstream_catalog",
        collection_name="products",
        id_field="product_id",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_import_calls) == 3
    assert [len(c["docs"]) for c in recorded_import_calls] == [2, 2, 1]


def test_batch_size_caps_rows(mod, recorded_import_calls):
    df = pd.DataFrame({"product_id": [f"p{i}" for i in range(10)], "name": [f"n{i}" for i in range(10)]})
    resource = FakeTypesenseResource()
    component = mod.TypesenseIndexUpsertComponent(
        asset_name="typesense_out",
        upstream_asset_key="upstream_catalog",
        collection_name="products",
        id_field="product_id",
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "typesense_out")
    assert out["rows_submitted"] == 3


def test_source_inline_mode(mod, recorded_import_calls):
    resource = FakeTypesenseResource()
    component = mod.TypesenseIndexUpsertComponent(
        asset_name="typesense_out",
        source={"kind": "inline", "rows": [{"product_id": "p1", "name": "A"}, {"product_id": "p2", "name": "B"}]},
        collection_name="products",
        id_field="product_id",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"typesense_resource": resource})
    assert result.success
    out = metadata_for(result, "typesense_out")
    assert out["rows_submitted"] == 2


def test_partial_line_failures_are_counted(mod, monkeypatch):
    def _fake_call(resource, collection_name, ndjson_body, action):
        return [{"success": True}, {"success": False, "error": "Field `name` not found."}]

    monkeypatch.setattr(mod, "_call_typesense_import_api", _fake_call)
    df = pd.DataFrame({"product_id": ["p1", "p2"], "name": ["Widget", "Gadget"]})
    resource = FakeTypesenseResource()
    component = mod.TypesenseIndexUpsertComponent(
        asset_name="typesense_out",
        upstream_asset_key="upstream_catalog",
        collection_name="products",
        id_field="product_id",
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "typesense_out")
    assert out["rows_submitted"] == 1
    assert out["rows_errored"] == 1
    assert "first_errors" in out


# --- delete path: per-document loop, NOT the import endpoint ---------------

def test_delete_operation_loops_one_call_per_row_not_bulk(mod, recorded_delete_calls, recorded_import_calls):
    df = pd.DataFrame({"product_id": ["p1", "p2", "p3"], "name": ["A", "B", "C"]})
    resource = FakeTypesenseResource()
    component = mod.TypesenseIndexUpsertComponent(
        asset_name="typesense_out",
        upstream_asset_key="upstream_catalog",
        collection_name="products",
        id_field="product_id",
        operation="delete",
    )
    result = _materialize(component, df, resource)
    assert result.success
    # Bulk import endpoint must NEVER be used for deletes.
    assert recorded_import_calls == []
    assert len(recorded_delete_calls) == 3
    assert {c["doc_id"] for c in recorded_delete_calls} == {"p1", "p2", "p3"}

    out = metadata_for(result, "typesense_out")
    assert out["rows_submitted"] == 3
    assert out["api_requests"] == 3
    assert out["operation"] == "delete"


def test_delete_operation_partial_failure_is_counted(mod, monkeypatch):
    def _flaky_delete(resource, collection_name, doc_id):
        if doc_id == "p2":
            raise RuntimeError("404 not found")
        return {"id": doc_id}

    monkeypatch.setattr(mod, "_call_typesense_delete_api", _flaky_delete)
    df = pd.DataFrame({"product_id": ["p1", "p2"], "name": ["A", "B"]})
    resource = FakeTypesenseResource()
    component = mod.TypesenseIndexUpsertComponent(
        asset_name="typesense_out",
        upstream_asset_key="upstream_catalog",
        collection_name="products",
        id_field="product_id",
        operation="delete",
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "typesense_out")
    assert out["rows_submitted"] == 1
    assert out["rows_errored"] == 1
