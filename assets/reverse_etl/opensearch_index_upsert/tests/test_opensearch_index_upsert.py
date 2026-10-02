"""Committed regression tests for OpenSearchIndexUpsertComponent.

The real OpenSearch cluster is never hit here -- `_call_opensearch_api`
(the one external, network boundary) is monkeypatched wholesale, while
NDJSON document/body building (the id_field -> _id mapping, delete's
no-source-line rule), dual source resolution, validation, chunking, and
metadata are all exercised for real.
"""
import json

import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeOpenSearchResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def recorded_calls(mod, monkeypatch):
    calls = []

    def _fake_call(resource, ndjson_body):
        calls.append(ndjson_body)
        return {"errors": False, "items": []}

    monkeypatch.setattr(mod, "_call_opensearch_api", _fake_call)
    return calls


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_catalog", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"opensearch_resource": resource})


# --- document building (pure) --------------------------------------------

def test_build_document_excludes_id_field_from_source(mod):
    row = {"product_id": "p1", "name": "Widget", "price": 9.99}
    doc_id, source = mod._build_document(row, "product_id")
    assert doc_id == "p1"
    assert source == {"name": "Widget", "price": 9.99}
    assert "product_id" not in source


def test_build_document_returns_none_for_missing_id(mod):
    doc_id, source = mod._build_document({"product_id": None, "name": "Widget"}, "product_id")
    assert doc_id is None
    assert source == {}


def test_build_document_returns_none_for_nan_id(mod):
    doc_id, _ = mod._build_document({"product_id": float("nan")}, "product_id")
    assert doc_id is None


# --- NDJSON building (pure) -------------------------------------------------

def test_build_ndjson_body_upsert_has_metadata_and_source_lines(mod):
    body = mod._build_ndjson_body("products", [("p1", {"name": "Widget"})], "upsert")
    lines = body.strip("\n").split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"index": {"_index": "products", "_id": "p1"}}
    assert json.loads(lines[1]) == {"name": "Widget"}


def test_build_ndjson_body_delete_has_no_source_line(mod):
    body = mod._build_ndjson_body("products", [("p1", {"name": "Widget"})], "delete")
    lines = body.strip("\n").split("\n")
    assert len(lines) == 1
    assert json.loads(lines[0]) == {"delete": {"_index": "products", "_id": "p1"}}


def test_build_ndjson_body_ends_with_trailing_newline(mod):
    body = mod._build_ndjson_body("products", [("p1", {"name": "Widget"})], "upsert")
    assert body.endswith("\n")


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.OpenSearchIndexUpsertComponent(
            asset_name="x", index_name="products", id_field="product_id",
        ).build_defs(context=None)


def test_invalid_operation_raises(mod):
    with pytest.raises(ValueError, match="operation must be"):
        mod.OpenSearchIndexUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            index_name="products",
            id_field="product_id",
            operation="add",
        ).build_defs(context=None)


def test_missing_id_field_column_raises_failure(mod):
    df = pd.DataFrame({"name": ["Widget"]})
    resource = FakeOpenSearchResource()
    component = mod.OpenSearchIndexUpsertComponent(
        asset_name="opensearch_out",
        upstream_asset_key="upstream_catalog",
        index_name="products",
        id_field="product_id",
    )
    with pytest.raises(Exception, match="product_id"):
        _materialize(component, df, resource)


# --- full asset body, against the monkeypatched API call -------------------

def test_upsert_operation_end_to_end(mod, recorded_calls):
    df = pd.DataFrame({
        "product_id": ["p1", "p2"],
        "name": ["Widget", "Gadget"],
    })
    resource = FakeOpenSearchResource()
    component = mod.OpenSearchIndexUpsertComponent(
        asset_name="opensearch_out",
        upstream_asset_key="upstream_catalog",
        index_name="products",
        id_field="product_id",
        operation="upsert",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_calls) == 1
    lines = recorded_calls[0].strip("\n").split("\n")
    assert json.loads(lines[0]) == {"index": {"_index": "products", "_id": "p1"}}
    assert json.loads(lines[1]) == {"name": "Widget"}

    out = metadata_for(result, "opensearch_out")
    assert out["rows_submitted"] == 2
    assert out["operation"] == "upsert"
    assert out["api_requests"] == 1


def test_rows_with_no_id_are_skipped_and_counted(mod, recorded_calls):
    df = pd.DataFrame({"product_id": ["p1", None], "name": ["Widget", "Gadget"]})
    resource = FakeOpenSearchResource()
    component = mod.OpenSearchIndexUpsertComponent(
        asset_name="opensearch_out",
        upstream_asset_key="upstream_catalog",
        index_name="products",
        id_field="product_id",
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "opensearch_out")
    assert out["rows_skipped_no_id"] == 1
    assert out["rows_submitted"] == 1


def test_empty_upstream_makes_no_api_call(mod, recorded_calls):
    df = pd.DataFrame({"product_id": [], "name": []})
    resource = FakeOpenSearchResource()
    component = mod.OpenSearchIndexUpsertComponent(
        asset_name="opensearch_out",
        upstream_asset_key="upstream_catalog",
        index_name="products",
        id_field="product_id",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls == []


def test_rows_chunked_at_rows_per_request(mod, recorded_calls, monkeypatch):
    monkeypatch.setattr(mod, "_ROWS_PER_REQUEST", 2)
    df = pd.DataFrame({"product_id": [f"p{i}" for i in range(5)], "name": [f"n{i}" for i in range(5)]})
    resource = FakeOpenSearchResource()
    component = mod.OpenSearchIndexUpsertComponent(
        asset_name="opensearch_out",
        upstream_asset_key="upstream_catalog",
        index_name="products",
        id_field="product_id",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_calls) == 3


def test_batch_size_caps_rows(mod, recorded_calls):
    df = pd.DataFrame({"product_id": [f"p{i}" for i in range(10)], "name": [f"n{i}" for i in range(10)]})
    resource = FakeOpenSearchResource()
    component = mod.OpenSearchIndexUpsertComponent(
        asset_name="opensearch_out",
        upstream_asset_key="upstream_catalog",
        index_name="products",
        id_field="product_id",
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "opensearch_out")
    assert out["rows_submitted"] == 3


def test_source_inline_mode(mod, recorded_calls):
    resource = FakeOpenSearchResource()
    component = mod.OpenSearchIndexUpsertComponent(
        asset_name="opensearch_out",
        source={"kind": "inline", "rows": [{"product_id": "p1", "name": "A"}, {"product_id": "p2", "name": "B"}]},
        index_name="products",
        id_field="product_id",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"opensearch_resource": resource})
    assert result.success
    out = metadata_for(result, "opensearch_out")
    assert out["rows_submitted"] == 2


def test_delete_operation_end_to_end(mod, recorded_calls):
    df = pd.DataFrame({"product_id": ["p1"], "name": ["Widget"]})
    resource = FakeOpenSearchResource()
    component = mod.OpenSearchIndexUpsertComponent(
        asset_name="opensearch_out",
        upstream_asset_key="upstream_catalog",
        index_name="products",
        id_field="product_id",
        operation="delete",
    )
    result = _materialize(component, df, resource)
    assert result.success
    lines = recorded_calls[0].strip("\n").split("\n")
    assert len(lines) == 1
    assert json.loads(lines[0]) == {"delete": {"_index": "products", "_id": "p1"}}
    out = metadata_for(result, "opensearch_out")
    assert out["operation"] == "delete"


def test_partial_item_errors_are_counted_from_bulk_response(mod, monkeypatch):
    def _fake_call(resource, ndjson_body):
        return {
            "errors": True,
            "items": [
                {"index": {"_id": "p1", "status": 201}},
                {"index": {"_id": "p2", "status": 409, "error": "version conflict"}},
            ],
        }

    monkeypatch.setattr(mod, "_call_opensearch_api", _fake_call)
    df = pd.DataFrame({"product_id": ["p1", "p2"], "name": ["Widget", "Gadget"]})
    resource = FakeOpenSearchResource()
    component = mod.OpenSearchIndexUpsertComponent(
        asset_name="opensearch_out",
        upstream_asset_key="upstream_catalog",
        index_name="products",
        id_field="product_id",
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "opensearch_out")
    assert out["rows_submitted"] == 1
    assert out["rows_errored"] == 1
    assert "first_errors" in out


def test_chunk_level_exception_is_counted_as_errored_not_fatal(mod, monkeypatch):
    def _failing_call(resource, ndjson_body):
        raise RuntimeError("simulated connection error")

    monkeypatch.setattr(mod, "_call_opensearch_api", _failing_call)
    df = pd.DataFrame({"product_id": ["p1"], "name": ["Widget"]})
    resource = FakeOpenSearchResource()
    component = mod.OpenSearchIndexUpsertComponent(
        asset_name="opensearch_out",
        upstream_asset_key="upstream_catalog",
        index_name="products",
        id_field="product_id",
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "opensearch_out")
    assert out["rows_errored"] == 1
    assert out["rows_submitted"] == 0
