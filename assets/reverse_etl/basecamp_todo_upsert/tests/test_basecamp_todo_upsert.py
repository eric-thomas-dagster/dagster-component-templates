"""Committed regression tests for BasecampTodoUpsertComponent.

The real Basecamp API is never called here -- `_call_basecamp_api` (the
one external, paid-API boundary) is monkeypatched wholesale, while dual
source resolution, key-marker embedding/extraction, pagination,
create-vs-update routing, completion-toggle routing, and metadata are all
exercised for real.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeBasecampResource, load_component_module, make_upstream_asset, metadata_for

PROJECT_ID = "2085958505"
TODOLIST_ID = "1069480012"
TODOS_PATH = f"buckets/{PROJECT_ID}/todolists/{TODOLIST_ID}/todos.json"


def _fake_backend(existing_active=None, existing_completed=None):
    existing_active = existing_active or []
    existing_completed = existing_completed or []
    calls = {"list_active": [], "list_completed": [], "create": [], "update": [], "completion": []}
    _next_id = {"n": 1000}

    def _call(resource, method, path, params=None, json_body=None):
        if method == "GET" and path == TODOS_PATH:
            params = params or {}
            page = params.get("page", 1)
            if params.get("completed") == "true":
                calls["list_completed"].append(params)
                return existing_completed if page == 1 else []
            calls["list_active"].append(params)
            return existing_active if page == 1 else []
        if method == "POST" and path == TODOS_PATH:
            calls["create"].append(json_body)
            _next_id["n"] += 1
            return {"id": _next_id["n"], **json_body}
        if method == "PUT" and path.startswith(f"buckets/{PROJECT_ID}/todos/") and path.endswith(".json") and "completion" not in path:
            todo_id = path.split("/")[-1].replace(".json", "")
            calls["update"].append((todo_id, json_body))
            return {"id": todo_id, **json_body}
        if path.endswith("/completion.json"):
            todo_id = path.split("/")[-2]
            calls["completion"].append((method, todo_id))
            return {}
        raise AssertionError(f"unexpected call: {method} {path}")

    return _call, calls


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, upstream_df, fake_call, monkeypatch, mod_):
    monkeypatch.setattr(mod_, "_call_basecamp_api", fake_call)
    upstream_asset = make_upstream_asset("upstream_todos", upstream_df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def, upstream_asset], resources={"basecamp_resource": FakeBasecampResource()})


# --- marker helpers -------------------------------------------------------

def test_extract_key_roundtrip(mod):
    desc = mod._make_description("body", "INC-1")
    assert mod._extract_key(desc) == "INC-1"
    assert "body" in desc


def test_extract_key_none_when_no_marker(mod):
    assert mod._extract_key("plain description") is None
    assert mod._extract_key(None) is None


# --- validation ------------------------------------------------------------

def test_neither_upstream_nor_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.BasecampTodoUpsertComponent(
            asset_name="x", project_id=PROJECT_ID, todolist_id=TODOLIST_ID,
            key_column="id", content_column="content",
        ).build_defs(context=None)


def test_both_upstream_and_source_raises(mod):
    with pytest.raises(ValueError, match="exactly one of"):
        mod.BasecampTodoUpsertComponent(
            asset_name="x", project_id=PROJECT_ID, todolist_id=TODOLIST_ID,
            key_column="id", content_column="content",
            upstream_asset_key="foo", source={"kind": "inline", "rows": []},
        ).build_defs(context=None)


def test_missing_upstream_columns_raises_failure(mod, monkeypatch):
    fake_call, _ = _fake_backend()
    df = pd.DataFrame({"id": ["1"]})  # missing content_column
    component = mod.BasecampTodoUpsertComponent(
        asset_name="basecamp_out", upstream_asset_key="upstream_todos",
        project_id=PROJECT_ID, todolist_id=TODOLIST_ID, key_column="id", content_column="content",
    )
    with pytest.raises(Exception):
        _materialize(component, df, fake_call, monkeypatch, mod)


# --- create / update routing ----------------------------------------------

def test_fresh_rows_created(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": ["INC-1", "INC-2"], "content": ["A", "B"]})
    component = mod.BasecampTodoUpsertComponent(
        asset_name="basecamp_out", upstream_asset_key="upstream_todos",
        project_id=PROJECT_ID, todolist_id=TODOLIST_ID, key_column="id", content_column="content",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert len(calls["create"]) == 2
    assert len(calls["update"]) == 0
    for body in calls["create"]:
        assert "<!-- dagster-key:" in body["description"]
    out = metadata_for(result, "basecamp_out")
    assert out["rows_created"] == 2
    assert out["rows_upserted"] == 2
    # Both active + completed listings are checked, even with nothing existing.
    assert len(calls["list_active"]) == 1
    assert len(calls["list_completed"]) == 1


def test_existing_rows_updated(mod, monkeypatch):
    desc = mod._make_description("", "INC-1")
    fake_call, calls = _fake_backend(existing_active=[{"id": 500, "description": desc}])
    df = pd.DataFrame({"id": ["INC-1", "INC-2"], "content": ["A", "B"]})
    component = mod.BasecampTodoUpsertComponent(
        asset_name="basecamp_out", upstream_asset_key="upstream_todos",
        project_id=PROJECT_ID, todolist_id=TODOLIST_ID, key_column="id", content_column="content",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert len(calls["update"]) == 1
    assert calls["update"][0][0] == "500"
    assert len(calls["create"]) == 1
    out = metadata_for(result, "basecamp_out")
    assert out["rows_created"] == 1
    assert out["rows_updated"] == 1


def test_matches_against_completed_todos_too(mod, monkeypatch):
    desc = mod._make_description("", "INC-1")
    fake_call, calls = _fake_backend(existing_completed=[{"id": 777, "description": desc}])
    df = pd.DataFrame({"id": ["INC-1"], "content": ["A"]})
    component = mod.BasecampTodoUpsertComponent(
        asset_name="basecamp_out", upstream_asset_key="upstream_todos",
        project_id=PROJECT_ID, todolist_id=TODOLIST_ID, key_column="id", content_column="content",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert len(calls["update"]) == 1
    assert calls["update"][0][0] == "777"


# --- completion toggling -----------------------------------------------

def test_completed_true_posts_completion(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": ["INC-1"], "content": ["A"], "is_resolved": [True]})
    component = mod.BasecampTodoUpsertComponent(
        asset_name="basecamp_out", upstream_asset_key="upstream_todos",
        project_id=PROJECT_ID, todolist_id=TODOLIST_ID, key_column="id", content_column="content",
        completed_column="is_resolved",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert len(calls["completion"]) == 1
    assert calls["completion"][0][0] == "POST"
    out = metadata_for(result, "basecamp_out")
    assert out["rows_completion_toggled"] == 1


def test_completed_false_deletes_completion(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": ["INC-1"], "content": ["A"], "is_resolved": [False]})
    component = mod.BasecampTodoUpsertComponent(
        asset_name="basecamp_out", upstream_asset_key="upstream_todos",
        project_id=PROJECT_ID, todolist_id=TODOLIST_ID, key_column="id", content_column="content",
        completed_column="is_resolved",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert len(calls["completion"]) == 1
    assert calls["completion"][0][0] == "DELETE"


def test_no_completed_column_never_toggles(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": ["INC-1"], "content": ["A"]})
    component = mod.BasecampTodoUpsertComponent(
        asset_name="basecamp_out", upstream_asset_key="upstream_todos",
        project_id=PROJECT_ID, todolist_id=TODOLIST_ID, key_column="id", content_column="content",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert calls["completion"] == []


# --- field mapping -----------------------------------------------------

def test_assignee_ids_and_due_on_passed_through(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({
        "id": ["INC-1"], "content": ["A"],
        "assignee_person_ids": ["111,222"], "due_date": ["2026-01-01"],
    })
    component = mod.BasecampTodoUpsertComponent(
        asset_name="basecamp_out", upstream_asset_key="upstream_todos",
        project_id=PROJECT_ID, todolist_id=TODOLIST_ID, key_column="id", content_column="content",
        assignee_ids_column="assignee_person_ids", due_on_column="due_date",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert calls["create"][0]["assignee_ids"] == [111, 222]
    assert calls["create"][0]["due_on"] == "2026-01-01"


# --- pagination ----------------------------------------------------------

def test_pagination_stops_on_empty_page(mod, monkeypatch):
    desc = mod._make_description("", "EXISTING")
    fake_call, calls = _fake_backend(existing_active=[{"id": 1, "description": desc}])
    df = pd.DataFrame({"id": ["INC-1"], "content": ["A"]})
    component = mod.BasecampTodoUpsertComponent(
        asset_name="basecamp_out", upstream_asset_key="upstream_todos",
        project_id=PROJECT_ID, todolist_id=TODOLIST_ID, key_column="id", content_column="content",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    # Active: page 1 (data) then page 2 (empty) -- stops.
    assert len(calls["list_active"]) == 2
    # Completed: no existing_completed fixture data -- page 1 is already
    # empty, so it stops after just one call.
    assert len(calls["list_completed"]) == 1


# --- edge cases -------------------------------------------------------------

def test_empty_upstream_short_circuits(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": [], "content": []})
    component = mod.BasecampTodoUpsertComponent(
        asset_name="basecamp_out", upstream_asset_key="upstream_todos",
        project_id=PROJECT_ID, todolist_id=TODOLIST_ID, key_column="id", content_column="content",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    assert calls["create"] == [] and calls["list_active"] == []
    out = metadata_for(result, "basecamp_out")
    assert out["rows_upserted"] == 0


def test_rows_missing_key_skipped(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": ["INC-1", None], "content": ["A", "B"]})
    component = mod.BasecampTodoUpsertComponent(
        asset_name="basecamp_out", upstream_asset_key="upstream_todos",
        project_id=PROJECT_ID, todolist_id=TODOLIST_ID, key_column="id", content_column="content",
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    out = metadata_for(result, "basecamp_out")
    assert out["rows_skipped_no_key"] == 1
    assert len(calls["create"]) == 1


def test_batch_size_caps_rows(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    df = pd.DataFrame({"id": [f"INC-{i}" for i in range(10)], "content": [f"T{i}" for i in range(10)]})
    component = mod.BasecampTodoUpsertComponent(
        asset_name="basecamp_out", upstream_asset_key="upstream_todos",
        project_id=PROJECT_ID, todolist_id=TODOLIST_ID, key_column="id", content_column="content",
        batch_size=3,
    )
    result = _materialize(component, df, fake_call, monkeypatch, mod)
    assert result.success
    out = metadata_for(result, "basecamp_out")
    assert out["rows_upserted"] == 3


def test_source_inline_mode(mod, monkeypatch):
    fake_call, calls = _fake_backend()
    component = mod.BasecampTodoUpsertComponent(
        asset_name="basecamp_out",
        source={"kind": "inline", "rows": [{"id": "INC-1", "content": "A"}]},
        project_id=PROJECT_ID, todolist_id=TODOLIST_ID, key_column="id", content_column="content",
    )
    monkeypatch.setattr(mod, "_call_basecamp_api", fake_call)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], resources={"basecamp_resource": FakeBasecampResource()})
    assert result.success
    assert len(calls["create"]) == 1


def test_api_exception_recorded_as_error_not_raised(mod, monkeypatch):
    def _fake_call(resource, method, path, params=None, json_body=None):
        if method == "GET":
            return []
        raise RuntimeError("Basecamp 500")

    monkeypatch.setattr(mod, "_call_basecamp_api", _fake_call)
    df = pd.DataFrame({"id": ["INC-1"], "content": ["A"]})
    component = mod.BasecampTodoUpsertComponent(
        asset_name="basecamp_out", upstream_asset_key="upstream_todos",
        project_id=PROJECT_ID, todolist_id=TODOLIST_ID, key_column="id", content_column="content",
    )
    upstream_asset = make_upstream_asset("upstream_todos", df)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def, upstream_asset], resources={"basecamp_resource": FakeBasecampResource()})
    assert result.success
    out = metadata_for(result, "basecamp_out")
    assert out["rows_errored"] == 1
    assert "Basecamp 500" in out["first_errors"][0]
