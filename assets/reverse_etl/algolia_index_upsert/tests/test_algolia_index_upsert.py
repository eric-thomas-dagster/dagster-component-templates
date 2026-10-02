"""Committed regression tests for AlgoliaIndexUpsertComponent.

The real Algolia REST endpoint is never hit here -- `_call_algolia_api`
(the one external, network boundary) is monkeypatched wholesale, while
document building (the id_field -> objectID mapping), dual source
resolution, validation, chunking, and metadata are all exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeAlgoliaResource, load_component_module, make_upstream_asset, metadata_for


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def recorded_calls(mod, monkeypatch):
    calls = []

    def _fake_call(resource, index_name, operations):
        calls.append({"index_name": index_name, "operations": [dict(op) for op in operations]})
        return {"taskID": 123, "objectIDs": [op["body"].get("objectID") for op in operations]}

    monkeypatch.setattr(mod, "_call_algolia_api", _fake_call)
    return calls


def _materialize(component, upstream_df, resource):
    upstream_asset = make_upstream_asset("upstream_catalog", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"algolia_resource": resource})


# --- document building (pure) --------------------------------------------

def test_build_document_excludes_id_field_from_body(mod):
    row = {"product_id": "p1", "name": "Widget", "price": 9.99}
    obj_id, body = mod._build_document(row, "product_id")
    assert obj_id == "p1"
    assert body == {"name": "Widget", "price": 9.99}
    assert "product_id" not in body


def test_build_document_returns_none_for_missing_id(mod):
    row = {"product_id": None, "name": "Widget"}
    obj_id, body = mod._build_document(row, "product_id")
    assert obj_id is None
    assert body == {}


def test_build_document_returns_none_for_nan_id(mod):
    row = {"product_id": float("nan"), "name": "Widget"}
    obj_id, body = mod._build_document(row, "product_id")
    assert obj_id is None


def test_build_document_drops_nan_and_none_fields(mod):
    row = {"product_id": "p1", "name": "Widget", "description": None, "rating": float("nan")}
    obj_id, body = mod._build_document(row, "product_id")
    assert obj_id == "p1"
    assert body == {"name": "Widget"}


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.AlgoliaIndexUpsertComponent(
            asset_name="x", index_name="products", id_field="product_id",
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.AlgoliaIndexUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            source={"kind": "inline", "rows": []},
            index_name="products",
            id_field="product_id",
        ).build_defs(context=None)


def test_invalid_operation_raises(mod):
    with pytest.raises(ValueError, match="operation must be"):
        mod.AlgoliaIndexUpsertComponent(
            asset_name="x",
            upstream_asset_key="foo",
            index_name="products",
            id_field="product_id",
            operation="add",
        ).build_defs(context=None)


def test_missing_id_field_column_raises_failure(mod, recorded_calls):
    df = pd.DataFrame({"name": ["Widget"]})
    resource = FakeAlgoliaResource()
    component = mod.AlgoliaIndexUpsertComponent(
        asset_name="algolia_out",
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
        "price": [9.99, 19.99],
    })
    resource = FakeAlgoliaResource()
    component = mod.AlgoliaIndexUpsertComponent(
        asset_name="algolia_out",
        upstream_asset_key="upstream_catalog",
        index_name="products",
        id_field="product_id",
        operation="upsert",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_calls) == 1
    call = recorded_calls[0]
    assert call["index_name"] == "products"
    assert len(call["operations"]) == 2
    assert call["operations"][0]["action"] == "updateObject"
    assert call["operations"][0]["body"]["objectID"] == "p1"
    assert call["operations"][0]["body"]["name"] == "Widget"

    out = metadata_for(result, "algolia_out")
    assert out["rows_submitted"] == 2
    assert out["rows_skipped_no_id"] == 0
    assert out["operation"] == "upsert"
    assert out["api_requests"] == 1


def test_delete_operation_sends_deleteobject_with_only_objectid(mod, recorded_calls):
    df = pd.DataFrame({"product_id": ["p1"], "name": ["Widget"]})
    resource = FakeAlgoliaResource()
    component = mod.AlgoliaIndexUpsertComponent(
        asset_name="algolia_out",
        upstream_asset_key="upstream_catalog",
        index_name="products",
        id_field="product_id",
        operation="delete",
    )
    result = _materialize(component, df, resource)
    assert result.success
    op = recorded_calls[0]["operations"][0]
    assert op["action"] == "deleteObject"
    assert op["body"] == {"objectID": "p1"}


def test_rows_with_no_id_are_skipped_and_counted(mod, recorded_calls):
    df = pd.DataFrame({"product_id": ["p1", None, ""], "name": ["Widget", "Gadget", "Thing"]})
    resource = FakeAlgoliaResource()
    component = mod.AlgoliaIndexUpsertComponent(
        asset_name="algolia_out",
        upstream_asset_key="upstream_catalog",
        index_name="products",
        id_field="product_id",
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "algolia_out")
    # "" is falsy but not None/NaN -- still a usable id in this component.
    assert out["rows_skipped_no_id"] == 1
    assert out["rows_submitted"] == 2


def test_empty_upstream_makes_no_api_call(mod, recorded_calls):
    df = pd.DataFrame({"product_id": [], "name": []})
    resource = FakeAlgoliaResource()
    component = mod.AlgoliaIndexUpsertComponent(
        asset_name="algolia_out",
        upstream_asset_key="upstream_catalog",
        index_name="products",
        id_field="product_id",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert recorded_calls == []
    out = metadata_for(result, "algolia_out")
    assert out["rows_submitted"] == 0


def test_operations_chunked_at_operations_per_request(mod, recorded_calls, monkeypatch):
    monkeypatch.setattr(mod, "_OPERATIONS_PER_REQUEST", 2)
    df = pd.DataFrame({"product_id": [f"p{i}" for i in range(5)], "name": [f"n{i}" for i in range(5)]})
    resource = FakeAlgoliaResource()
    component = mod.AlgoliaIndexUpsertComponent(
        asset_name="algolia_out",
        upstream_asset_key="upstream_catalog",
        index_name="products",
        id_field="product_id",
    )
    result = _materialize(component, df, resource)
    assert result.success
    assert len(recorded_calls) == 3
    assert [len(c["operations"]) for c in recorded_calls] == [2, 2, 1]


def test_batch_size_caps_rows(mod, recorded_calls):
    df = pd.DataFrame({"product_id": [f"p{i}" for i in range(10)], "name": [f"n{i}" for i in range(10)]})
    resource = FakeAlgoliaResource()
    component = mod.AlgoliaIndexUpsertComponent(
        asset_name="algolia_out",
        upstream_asset_key="upstream_catalog",
        index_name="products",
        id_field="product_id",
        batch_size=3,
    )
    result = _materialize(component, df, resource)
    assert result.success
    out = metadata_for(result, "algolia_out")
    assert out["rows_submitted"] == 3


def test_source_inline_mode(mod, recorded_calls):
    resource = FakeAlgoliaResource()
    component = mod.AlgoliaIndexUpsertComponent(
        asset_name="algolia_out",
        source={"kind": "inline", "rows": [{"product_id": "p1", "name": "A"}, {"product_id": "p2", "name": "B"}]},
        index_name="products",
        id_field="product_id",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"algolia_resource": resource})
    assert result.success
    out = metadata_for(result, "algolia_out")
    assert out["rows_submitted"] == 2


def test_chunk_failure_is_counted_as_errored_not_fatal(mod, monkeypatch):
    def _failing_call(resource, index_name, operations):
        raise RuntimeError("simulated 403")

    monkeypatch.setattr(mod, "_call_algolia_api", _failing_call)
    df = pd.DataFrame({"product_id": ["p1"], "name": ["Widget"]})
    resource = FakeAlgoliaResource()
    component = mod.AlgoliaIndexUpsertComponent(
        asset_name="algolia_out",
        upstream_asset_key="upstream_catalog",
        index_name="products",
        id_field="product_id",
    )
    result = _materialize(component, df, resource)
    assert result.success  # component doesn't hard-fail the whole run on API errors
    out = metadata_for(result, "algolia_out")
    assert out["rows_errored"] == 1
    assert out["rows_submitted"] == 0
    assert "first_errors" in out
