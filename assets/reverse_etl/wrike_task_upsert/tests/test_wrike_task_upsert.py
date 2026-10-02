"""Committed regression tests for WrikeTaskUpsertComponent.

The real Wrike API is never called here -- `_call_wrike_api` (the one
external, paid-API boundary) is monkeypatched wholesale, while dual source
resolution, key-marker embedding/extraction, status/importance
validation, create-vs-update routing, and metadata are all exercised for
real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeWrikeResource, load_component_module, make_upstream_asset, metadata_for

FOLDER_ID = "IEAABBCC123456"


def _fake_backend(existing_tasks=None):
    existing_tasks = existing_tasks or []
    calls = {"list": [], "create": [], "update": []}
    _next_id = {"n": 1000}

    def _call(resource, method, path, params=None, json_body=None):
        if method == "GET" and path == f"folders/{FOLDER_ID}/tasks":
            calls["list"].append(params)
            return {"kind": "tasks", "data": existing_tasks}
        if method == "POST" and path == f"folders/{FOLDER_ID}/tasks":
            calls["create"].append(json_body)
            _next_id["n"] += 1
            task = {"id": str(_next_id["n"]), **json_body}
            return {"kind": "tasks", "data": [task]}
        if method == "PUT" and path.startswith("tasks/"):
            task_id = path.split("/", 1)[1]
            calls["update"].append((task_id, json_body))
            return {"kind": "tasks", "data": [{"id": task_id, **json_body}]}
        raise AssertionError(f"unexpected call: {method} {path}")

    return _call, calls


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, fake_call, monkeypatch, mod_):
    monkeypatch.setattr(mod_, "_call_wrike_api", fake_call)
    upstream_asset = make_upstream_asset("upstream_tasks", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"wrike_resource": FakeWrikeResource()})


# --- marker helpers -------------------------------------------------------

def test_extract_key_roundtrip(mod):
    desc = mod._make_description("body text", "INC-1")
    assert mod._extract_key(desc) == "INC-1"
    assert "body text" in desc


def test_extract_key_none_when_no_marker(mod):
    assert mod._extract_key("plain description") is None
    assert mod._extract_key(None) is None


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.WrikeTaskUpsertComponent(
            asset_name="x", folder_id=FOLDER_ID, key_column="id", title_column="title",
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.WrikeTaskUpsertComponent(
            asset_name="x", folder_id=FOLDER_ID, key_column="id", title_column="title",
            upstream_asset_key="foo", source={"kind": "inline", "rows": []},
        ).build_defs(context=None)


def test_missing_upstream_columns_raises_failure(mod, monkeypatch):
    fake_call, _ = _fake_backend()
    df = pd.DataFrame({"id": ["1"]})  # missing title_column
    component = mod.WrikeTaskUpsertComponent(
        asset_name="wrike_out", upstream_asset_key="upstream_tasks",
        folder_id=FOLDER_ID, key_column="id", title_column="title",
    )
    with pytest.raises(Exception):
        _materialize(component, df, fake_call, monkeypatch, mod)


# --- create / update routing ----------------------------------------------

def test_fresh_rows_created(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": ["INC-1", "INC-2"], "title": ["A", "B"]})
    component = mod.WrikeTaskUpsertComponent(
        asset_name="wrike_out", upstream_asset_key="upstream_tasks",
        folder_id=FOLDER_ID, key_column="id", title_column="title",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert len(calls["create"]) == 2
    assert len(calls["update"]) == 0
    for body in calls["create"]:
        assert "<!-- dagster-key:" in body["description"]
    out = metadata_for(result, "wrike_out")
    assert out["rows_created"] == 2
    assert out["rows_upserted"] == 2


def test_existing_rows_updated(mod, monkeypatch):
    desc = mod._make_description("", "INC-1")
    fake_call, calls = _fake_backend(existing_tasks=[{"id": "500", "description": desc}])
    df = pd.DataFrame({"id": ["INC-1", "INC-2"], "title": ["A", "B"]})
    component = mod.WrikeTaskUpsertComponent(
        asset_name="wrike_out", upstream_asset_key="upstream_tasks",
        folder_id=FOLDER_ID, key_column="id", title_column="title",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert len(calls["update"]) == 1
    assert calls["update"][0][0] == "500"
    assert len(calls["create"]) == 1
    out = metadata_for(result, "wrike_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 1


# --- field validation -----------------------------------------------------

def test_valid_status_and_importance_passed_through(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": ["INC-1"], "title": ["A"], "status": ["active"], "severity": ["high"]})
    component = mod.WrikeTaskUpsertComponent(
        asset_name="wrike_out", upstream_asset_key="upstream_tasks",
        folder_id=FOLDER_ID, key_column="id", title_column="title",
        status_column="status", importance_column="severity",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert calls["create"][0]["status"] == "Active"
    assert calls["create"][0]["importance"] == "High"


def test_invalid_status_skipped_without_crashing(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": ["INC-1"], "title": ["A"], "status": ["bogus"]})
    component = mod.WrikeTaskUpsertComponent(
        asset_name="wrike_out", upstream_asset_key="upstream_tasks",
        folder_id=FOLDER_ID, key_column="id", title_column="title", status_column="status",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert "status" not in calls["create"][0]


def test_due_date_sent_as_dates_due(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": ["INC-1"], "title": ["A"], "due_date": ["2026-01-01"]})
    component = mod.WrikeTaskUpsertComponent(
        asset_name="wrike_out", upstream_asset_key="upstream_tasks",
        folder_id=FOLDER_ID, key_column="id", title_column="title", due_date_column="due_date",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert calls["create"][0]["dates"] == {"due": "2026-01-01"}


# --- edge cases -------------------------------------------------------------

def test_empty_upstream_short_circuits(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": [], "title": []})
    component = mod.WrikeTaskUpsertComponent(
        asset_name="wrike_out", upstream_asset_key="upstream_tasks",
        folder_id=FOLDER_ID, key_column="id", title_column="title",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert calls["create"] == [] and calls["list"] == []
    out = metadata_for(result, "wrike_out")
    assert out["rows_upserted"] == 0


def test_rows_missing_key_skipped(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": ["INC-1", None], "title": ["A", "B"]})
    component = mod.WrikeTaskUpsertComponent(
        asset_name="wrike_out", upstream_asset_key="upstream_tasks",
        folder_id=FOLDER_ID, key_column="id", title_column="title",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    out = metadata_for(result, "wrike_out")
    assert out["rows_skipped_no_key"] == 1
    assert len(calls["create"]) == 1


def test_batch_size_caps_rows(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": [f"INC-{i}" for i in range(10)], "title": [f"T{i}" for i in range(10)]})
    component = mod.WrikeTaskUpsertComponent(
        asset_name="wrike_out", upstream_asset_key="upstream_tasks",
        folder_id=FOLDER_ID, key_column="id", title_column="title", batch_size=3,
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    out = metadata_for(result, "wrike_out")
    assert out["rows_upserted"] == 3


def test_source_inline_mode(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    component = mod.WrikeTaskUpsertComponent(
        asset_name="wrike_out",
        source={"kind": "inline", "rows": [{"id": "INC-1", "title": "A"}]},
        folder_id=FOLDER_ID, key_column="id", title_column="title",
    )
    monkeypatch.setattr(mod, "_call_wrike_api", fake_call)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"wrike_resource": FakeWrikeResource()})
    assert result.success
    assert len(calls["create"]) == 1


def test_api_exception_recorded_as_error_not_raised(mod, monkeypatch):
    def _fake_call(resource, method, path, params=None, json_body=None):
        if method == "GET":
            return {"kind": "tasks", "data": []}
        raise RuntimeError("Wrike 500")

    monkeypatch.setattr(mod, "_call_wrike_api", _fake_call)
    df = pd.DataFrame({"id": ["INC-1"], "title": ["A"]})
    component = mod.WrikeTaskUpsertComponent(
        asset_name="wrike_out", upstream_asset_key="upstream_tasks",
        folder_id=FOLDER_ID, key_column="id", title_column="title",
    )
    upstream_asset = make_upstream_asset("upstream_tasks", df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def, upstream_asset], resources={"wrike_resource": FakeWrikeResource()})
    assert result.success
    out = metadata_for(result, "wrike_out")
    assert out["rows_errored"] == 1
    assert "Wrike 500" in out["first_errors"][0]
