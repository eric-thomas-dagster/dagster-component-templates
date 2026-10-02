"""Committed regression tests for ClickUpTaskUpsertComponent.

The real ClickUp API is never called here -- `_call_clickup_api` (the one
external, paid-API boundary) is monkeypatched wholesale, while dual source
resolution, key-marker embedding/extraction, pagination, create-vs-update
routing, and metadata are all exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeClickUpResource, load_component_module, make_upstream_asset, metadata_for

LIST_ID = "901234567"


def _fake_backend(existing_tasks=None):
    """existing_tasks: list of {"id":..., "description":...} dicts simulating
    tasks already present in the target List."""
    existing_tasks = existing_tasks or []
    calls = {"list_pages": [], "create": [], "update": []}
    _next_id = {"n": 1000}

    def _call(resource, method, path, params=None, json_body=None):
        if method == "GET" and path == f"list/{LIST_ID}/task":
            calls["list_pages"].append(params)
            page = params["page"]
            if page == 0:
                return {"tasks": existing_tasks}
            return {"tasks": []}
        if method == "POST" and path == f"list/{LIST_ID}/task":
            calls["create"].append(json_body)
            _next_id["n"] += 1
            return {"id": str(_next_id["n"]), **json_body}
        if method == "PUT" and path.startswith("task/"):
            task_id = path.split("/", 1)[1]
            calls["update"].append((task_id, json_body))
            return {"id": task_id, **json_body}
        raise AssertionError(f"unexpected call: {method} {path}")

    return _call, calls


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, fake_call, monkeypatch, mod_):
    monkeypatch.setattr(mod_, "_call_clickup_api", fake_call)
    upstream_asset = make_upstream_asset("upstream_tasks", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"clickup_resource": FakeClickUpResource()})


# --- marker helpers -------------------------------------------------------

def test_extract_key_roundtrip(mod):
    desc = mod._make_description("some body text", "INC-1")
    assert mod._extract_key(desc) == "INC-1"
    assert "some body text" in desc


def test_extract_key_none_when_no_marker(mod):
    assert mod._extract_key("just a plain description") is None
    assert mod._extract_key(None) is None


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.ClickUpTaskUpsertComponent(
            asset_name="x", list_id=LIST_ID, key_column="id", name_column="name",
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.ClickUpTaskUpsertComponent(
            asset_name="x", list_id=LIST_ID, key_column="id", name_column="name",
            upstream_asset_key="foo", source={"kind": "inline", "rows": []},
        ).build_defs(context=None)


def test_missing_upstream_columns_raises_failure(mod, monkeypatch):
    fake_call, _ = _fake_backend()
    df = pd.DataFrame({"id": ["1"]})  # missing name_column
    component = mod.ClickUpTaskUpsertComponent(
        asset_name="clickup_out", upstream_asset_key="upstream_tasks",
        list_id=LIST_ID, key_column="id", name_column="name",
    )
    with pytest.raises(Exception):
        _materialize(component, df, fake_call, monkeypatch, mod)


# --- create / update routing ----------------------------------------------

def test_fresh_rows_created(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": ["INC-1", "INC-2"], "name": ["A", "B"]})
    component = mod.ClickUpTaskUpsertComponent(
        asset_name="clickup_out", upstream_asset_key="upstream_tasks",
        list_id=LIST_ID, key_column="id", name_column="name",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert len(calls["create"]) == 2
    assert len(calls["update"]) == 0
    for body in calls["create"]:
        assert "<!-- dagster-key:" in body["description"]
    out = metadata_for(result, "clickup_out")
    assert out["rows_created"] == 2
    assert out["rows_upserted"] == 2


def test_existing_rows_updated(mod, monkeypatch):
    desc = mod._make_description("", "INC-1")
    fake_call, calls = _fake_backend(existing_tasks=[{"id": "500", "description": desc}])
    df = pd.DataFrame({"id": ["INC-1", "INC-2"], "name": ["A", "B"]})
    component = mod.ClickUpTaskUpsertComponent(
        asset_name="clickup_out", upstream_asset_key="upstream_tasks",
        list_id=LIST_ID, key_column="id", name_column="name",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert len(calls["update"]) == 1
    assert calls["update"][0][0] == "500"
    assert len(calls["create"]) == 1
    out = metadata_for(result, "clickup_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 1


# --- field mapping -----------------------------------------------------

def test_priority_name_and_int(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": ["INC-1", "INC-2"], "name": ["A", "B"], "severity": ["Urgent", 4]})
    component = mod.ClickUpTaskUpsertComponent(
        asset_name="clickup_out", upstream_asset_key="upstream_tasks",
        list_id=LIST_ID, key_column="id", name_column="name", priority_column="severity",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert calls["create"][0]["priority"] == 1
    assert calls["create"][1]["priority"] == 4


def test_assignees_and_tags_parsed(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({
        "id": ["INC-1"], "name": ["A"],
        "assignee_ids": ["111,222"], "labels": ["bug,urgent"],
    })
    component = mod.ClickUpTaskUpsertComponent(
        asset_name="clickup_out", upstream_asset_key="upstream_tasks",
        list_id=LIST_ID, key_column="id", name_column="name",
        assignees_column="assignee_ids", tags_column="labels",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert calls["create"][0]["assignees"] == [111, 222]
    assert calls["create"][0]["tags"] == ["bug", "urgent"]


# --- pagination ----------------------------------------------------------

def test_pagination_stops_on_short_page(mod, monkeypatch):
    full_page = [{"id": str(i), "description": mod._make_description("", f"K{i}")} for i in range(100)]
    fake_call, calls = _fake_backend()

    def _paged_call(resource, method, path, params=None, json_body=None):
        if method == "GET":
            calls["list_pages"].append(params)
            if params["page"] == 0:
                return {"tasks": full_page}
            return {"tasks": []}
        return fake_call(resource, method, path, params=params, json_body=json_body)

    df = pd.DataFrame({"id": ["NEWKEY"], "name": ["A"]})
    component = mod.ClickUpTaskUpsertComponent(
        asset_name="clickup_out", upstream_asset_key="upstream_tasks",
        list_id=LIST_ID, key_column="id", name_column="name",
    )
    monkeypatch.setattr(mod, "_call_clickup_api", _paged_call)
    upstream_asset = make_upstream_asset("upstream_tasks", df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def, upstream_asset], resources={"clickup_resource": FakeClickUpResource()})
    assert result.success
    # Page 0 (100 items) then page 1 (0 items) -- stops because len < 100.
    assert len(calls["list_pages"]) == 2


# --- edge cases -------------------------------------------------------------

def test_empty_upstream_short_circuits(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": [], "name": []})
    component = mod.ClickUpTaskUpsertComponent(
        asset_name="clickup_out", upstream_asset_key="upstream_tasks",
        list_id=LIST_ID, key_column="id", name_column="name",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert calls["create"] == [] and calls["list_pages"] == []
    out = metadata_for(result, "clickup_out")
    assert out["rows_upserted"] == 0


def test_rows_missing_key_skipped(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": ["INC-1", None], "name": ["A", "B"]})
    component = mod.ClickUpTaskUpsertComponent(
        asset_name="clickup_out", upstream_asset_key="upstream_tasks",
        list_id=LIST_ID, key_column="id", name_column="name",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    out = metadata_for(result, "clickup_out")
    assert out["rows_skipped_no_key"] == 1
    assert len(calls["create"]) == 1


def test_batch_size_caps_rows(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": [f"INC-{i}" for i in range(10)], "name": [f"T{i}" for i in range(10)]})
    component = mod.ClickUpTaskUpsertComponent(
        asset_name="clickup_out", upstream_asset_key="upstream_tasks",
        list_id=LIST_ID, key_column="id", name_column="name", batch_size=3,
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    out = metadata_for(result, "clickup_out")
    assert out["rows_upserted"] == 3


def test_source_inline_mode(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    component = mod.ClickUpTaskUpsertComponent(
        asset_name="clickup_out",
        source={"kind": "inline", "rows": [{"id": "INC-1", "name": "A"}]},
        list_id=LIST_ID, key_column="id", name_column="name",
    )
    monkeypatch.setattr(mod, "_call_clickup_api", fake_call)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"clickup_resource": FakeClickUpResource()})
    assert result.success
    assert len(calls["create"]) == 1


def test_api_exception_recorded_as_error_not_raised(mod, monkeypatch):
    def _fake_call(resource, method, path, params=None, json_body=None):
        if method == "GET":
            return {"tasks": []}
        raise RuntimeError("ClickUp 500")

    monkeypatch.setattr(mod, "_call_clickup_api", _fake_call)
    df = pd.DataFrame({"id": ["INC-1"], "name": ["A"]})
    component = mod.ClickUpTaskUpsertComponent(
        asset_name="clickup_out", upstream_asset_key="upstream_tasks",
        list_id=LIST_ID, key_column="id", name_column="name",
    )
    upstream_asset = make_upstream_asset("upstream_tasks", df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def, upstream_asset], resources={"clickup_resource": FakeClickUpResource()})
    assert result.success
    out = metadata_for(result, "clickup_out")
    assert out["rows_errored"] == 1
    assert "ClickUp 500" in out["first_errors"][0]
