"""Committed regression tests for TrelloCardUpsertComponent.

The real Trello API is never called here -- `_call_trello_api` (the one
external, paid-API boundary) is monkeypatched wholesale, while dual source
resolution, key-marker embedding/extraction, create-vs-update routing, and
metadata are all exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeTrelloResource, load_component_module, make_upstream_asset, metadata_for

LIST_ID = "5f8a1b2c3d4e5f6a7b8c9d0e"


def _fake_backend(existing_cards=None):
    existing_cards = existing_cards or []
    calls = {"list": [], "create": [], "update": []}
    _next_id = {"n": 1000}

    def _call(resource, method, path, params=None):
        if method == "GET" and path == f"lists/{LIST_ID}/cards":
            calls["list"].append(params)
            return existing_cards
        if method == "POST" and path == "cards":
            calls["create"].append(params)
            _next_id["n"] += 1
            return {"id": str(_next_id["n"]), **params}
        if method == "PUT" and path.startswith("cards/"):
            card_id = path.split("/", 1)[1]
            calls["update"].append((card_id, params))
            return {"id": card_id, **params}
        raise AssertionError(f"unexpected call: {method} {path}")

    return _call, calls


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, fake_call, monkeypatch, mod_):
    monkeypatch.setattr(mod_, "_call_trello_api", fake_call)
    upstream_asset = make_upstream_asset("upstream_cards", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"trello_resource": FakeTrelloResource()})


# --- marker helpers -------------------------------------------------------

def test_extract_key_roundtrip(mod):
    desc = mod._make_desc("some body", "INC-1")
    assert mod._extract_key(desc) == "INC-1"
    assert "some body" in desc


def test_extract_key_none_when_no_marker(mod):
    assert mod._extract_key("plain text") is None
    assert mod._extract_key(None) is None


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.TrelloCardUpsertComponent(
            asset_name="x", list_id=LIST_ID, key_column="id", name_column="name",
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.TrelloCardUpsertComponent(
            asset_name="x", list_id=LIST_ID, key_column="id", name_column="name",
            upstream_asset_key="foo", source={"kind": "inline", "rows": []},
        ).build_defs(context=None)


def test_missing_upstream_columns_raises_failure(mod, monkeypatch):
    fake_call, _ = _fake_backend()
    df = pd.DataFrame({"id": ["1"]})  # missing name_column
    component = mod.TrelloCardUpsertComponent(
        asset_name="trello_out", upstream_asset_key="upstream_cards",
        list_id=LIST_ID, key_column="id", name_column="name",
    )
    with pytest.raises(Exception):
        _materialize(component, df, fake_call, monkeypatch, mod)


# --- create / update routing ----------------------------------------------

def test_fresh_rows_created(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": ["INC-1", "INC-2"], "name": ["A", "B"]})
    component = mod.TrelloCardUpsertComponent(
        asset_name="trello_out", upstream_asset_key="upstream_cards",
        list_id=LIST_ID, key_column="id", name_column="name",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert len(calls["create"]) == 2
    assert len(calls["update"]) == 0
    for params in calls["create"]:
        assert params["idList"] == LIST_ID
        assert "<!-- dagster-key:" in params["desc"]
    out = metadata_for(result, "trello_out")
    assert out["rows_created"] == 2
    assert out["rows_upserted"] == 2


def test_existing_rows_updated(mod, monkeypatch):
    desc = mod._make_desc("", "INC-1")
    fake_call, calls = _fake_backend(existing_cards=[{"id": "500", "desc": desc}])
    df = pd.DataFrame({"id": ["INC-1", "INC-2"], "name": ["A", "B"]})
    component = mod.TrelloCardUpsertComponent(
        asset_name="trello_out", upstream_asset_key="upstream_cards",
        list_id=LIST_ID, key_column="id", name_column="name",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert len(calls["update"]) == 1
    assert calls["update"][0][0] == "500"
    assert len(calls["create"]) == 1
    out = metadata_for(result, "trello_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 1


# --- field mapping -----------------------------------------------------

def test_due_and_labels_and_closed_passed_through(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({
        "id": ["INC-1"], "name": ["A"],
        "due_date": ["2026-01-01T00:00:00Z"],
        "label_ids": ["lbl1,lbl2"],
        "is_resolved": [True],
    })
    component = mod.TrelloCardUpsertComponent(
        asset_name="trello_out", upstream_asset_key="upstream_cards",
        list_id=LIST_ID, key_column="id", name_column="name",
        due_column="due_date", id_labels_column="label_ids", closed_column="is_resolved",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    body = calls["create"][0]
    assert body["due"] == "2026-01-01T00:00:00Z"
    assert body["idLabels"] == "lbl1,lbl2"
    assert body["closed"] == "true"


# --- edge cases -------------------------------------------------------------

def test_empty_upstream_short_circuits(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": [], "name": []})
    component = mod.TrelloCardUpsertComponent(
        asset_name="trello_out", upstream_asset_key="upstream_cards",
        list_id=LIST_ID, key_column="id", name_column="name",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert calls["create"] == [] and calls["list"] == []
    out = metadata_for(result, "trello_out")
    assert out["rows_upserted"] == 0


def test_rows_missing_key_skipped(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": ["INC-1", None], "name": ["A", "B"]})
    component = mod.TrelloCardUpsertComponent(
        asset_name="trello_out", upstream_asset_key="upstream_cards",
        list_id=LIST_ID, key_column="id", name_column="name",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    out = metadata_for(result, "trello_out")
    assert out["rows_skipped_no_key"] == 1
    assert len(calls["create"]) == 1


def test_batch_size_caps_rows(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": [f"INC-{i}" for i in range(10)], "name": [f"T{i}" for i in range(10)]})
    component = mod.TrelloCardUpsertComponent(
        asset_name="trello_out", upstream_asset_key="upstream_cards",
        list_id=LIST_ID, key_column="id", name_column="name", batch_size=3,
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    out = metadata_for(result, "trello_out")
    assert out["rows_upserted"] == 3


def test_source_inline_mode(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    component = mod.TrelloCardUpsertComponent(
        asset_name="trello_out",
        source={"kind": "inline", "rows": [{"id": "INC-1", "name": "A"}]},
        list_id=LIST_ID, key_column="id", name_column="name",
    )
    monkeypatch.setattr(mod, "_call_trello_api", fake_call)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"trello_resource": FakeTrelloResource()})
    assert result.success
    assert len(calls["create"]) == 1


def test_api_exception_recorded_as_error_not_raised(mod, monkeypatch):
    def _fake_call(resource, method, path, params=None):
        if method == "GET":
            return []
        raise RuntimeError("Trello 500")

    monkeypatch.setattr(mod, "_call_trello_api", _fake_call)
    df = pd.DataFrame({"id": ["INC-1"], "name": ["A"]})
    component = mod.TrelloCardUpsertComponent(
        asset_name="trello_out", upstream_asset_key="upstream_cards",
        list_id=LIST_ID, key_column="id", name_column="name",
    )
    upstream_asset = make_upstream_asset("upstream_cards", df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def, upstream_asset], resources={"trello_resource": FakeTrelloResource()})
    assert result.success
    out = metadata_for(result, "trello_out")
    assert out["rows_errored"] == 1
    assert "Trello 500" in out["first_errors"][0]
