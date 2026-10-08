"""Committed regression tests for ReclaimIngestionComponent.

The one external call this component makes -- the dlt pipeline run against
the real Reclaim.ai REST API, and the follow-up sql_client() query-back --
is monkeypatched wholesale via `install_fake_dlt` (see conftest.py).
Everything the component actually owns is exercised for real: resource-config
construction (`_build_resources_config`), the `/api/events/v2` required
start/end date-window default, the bearer-auth config shape, and the
per-resource DataFrame combination + metadata shape.
"""
import datetime as dt

import pandas as pd
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(mod, component, table_rows=None, monkeypatch=None, raise_on_error=True):
    import dagster as dg

    fake_pipeline, captured = install_fake_dlt(monkeypatch, mod, table_rows=table_rows)
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    result = dg.materialize([asset_def], raise_on_error=raise_on_error)
    return result, fake_pipeline, captured


def _metadata_for(result, asset_name: str) -> dict:
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out


# --- _build_resources_config: pure logic, no mocking needed ------------------------

def test_default_resources_expands_to_four_dlt_resources(mod):
    config_resources = mod._build_resources_config("tasks,events,habits", None, None)
    names = [r["name"] for r in config_resources]
    assert names == ["tasks", "events", "habits_daily", "habit_templates"]


def test_tasks_resource_uses_real_path_with_no_params(mod):
    config_resources = mod._build_resources_config("tasks", None, None)
    assert len(config_resources) == 1
    assert config_resources[0]["endpoint"]["path"] == "api/tasks"
    # Confirms the component does NOT send a client-side `user` query param --
    # see README/docstring: it's a framework-injected auth principal in the
    # real OpenAPI spec, not something callers pass explicitly.
    assert "params" not in config_resources[0]["endpoint"]


def test_events_resource_uses_v2_path(mod):
    config_resources = mod._build_resources_config("events", "2026-01-01", "2026-01-31")
    assert len(config_resources) == 1
    assert config_resources[0]["name"] == "events"
    assert config_resources[0]["endpoint"]["path"] == "api/events/v2"


def test_events_date_window_passes_through_when_set(mod):
    config_resources = mod._build_resources_config("events", "2026-01-01", "2026-01-31")
    params = config_resources[0]["endpoint"]["params"]
    assert params == {"start": "2026-01-01", "end": "2026-01-31"}


def test_events_date_window_defaults_when_unset(mod):
    config_resources = mod._build_resources_config("events", None, None)
    params = config_resources[0]["endpoint"]["params"]
    start = dt.date.fromisoformat(params["start"])
    end = dt.date.fromisoformat(params["end"])
    assert (end - start).days == 30
    # start defaults to "today" in UTC -- sanity-check it's not wildly off
    # from actual today (guards against a timezone/off-by-one regression)
    # without hardcoding a specific date into the test.
    assert abs((start - dt.datetime.now(dt.timezone.utc).date()).days) <= 1


def test_events_date_window_partial_override_fills_only_the_missing_side(mod):
    config_resources = mod._build_resources_config("events", "2020-01-01", None)
    params = config_resources[0]["endpoint"]["params"]
    assert params["start"] == "2020-01-01"
    # end was left unset -> defaulted, not left None
    assert params["end"] is not None
    assert params["end"] != "2020-01-01"


def test_habits_expands_to_daily_and_templates(mod):
    config_resources = mod._build_resources_config("habits", None, None)
    names = [r["name"] for r in config_resources]
    paths = [r["endpoint"]["path"] for r in config_resources]
    assert names == ["habits_daily", "habit_templates"]
    assert paths == ["api/assist/habits/daily", "api/assist/habits/templates"]


def test_unknown_resource_name_is_ignored(mod):
    config_resources = mod._build_resources_config("tasks,not_a_real_resource", None, None)
    assert [r["name"] for r in config_resources] == ["tasks"]


def test_events_not_requested_does_not_trigger_date_defaulting(mod):
    # If 'events' isn't in the requested resources at all, no date-window
    # computation should happen (nothing to compute it for).
    config_resources = mod._build_resources_config("tasks,habits", None, None)
    assert all(r["name"] != "events" for r in config_resources)


# --- end-to-end: bearer auth + base URL + full resource set -----------------------

def test_source_config_uses_bearer_auth_and_real_base_url(mod, monkeypatch):
    component = mod.ReclaimIngestionComponent(
        asset_name="reclaim_out",
        api_token="tok_abc123",
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={"tasks": pd.DataFrame({"id": [1, 2]})},
        monkeypatch=monkeypatch,
    )
    config = captured["config"]
    assert config["client"]["base_url"] == "https://api.app.reclaim.ai"
    assert config["client"]["auth"] == {"type": "bearer", "token": "tok_abc123"}


def test_default_resources_requested_metadata(mod, monkeypatch):
    component = mod.ReclaimIngestionComponent(
        asset_name="reclaim_out",
        api_token="tok_abc123",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "tasks": pd.DataFrame({"id": [1]}),
            "events": pd.DataFrame({"id": [1]}),
            "habits_daily": pd.DataFrame({"id": [1]}),
            "habit_templates": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "reclaim_out")
    assert out["resources_requested"] == ["tasks", "events", "habits_daily", "habit_templates"]
    assert set(out["resources_loaded"]) == {"tasks", "events", "habits_daily", "habit_templates"}
    assert out["row_count"] == 4


# --- DataFrame combination from the (fake) duckdb query-back -----------------------

def test_combines_per_resource_tables_into_one_dataframe(mod, monkeypatch):
    component = mod.ReclaimIngestionComponent(
        asset_name="reclaim_out",
        api_token="tok_abc123",
        resources="tasks,habits",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "tasks": pd.DataFrame({"id": [1, 2], "title": ["a", "b"]}),
            "habits_daily": pd.DataFrame({"id": [1], "title": ["c"]}),
            "habit_templates": pd.DataFrame({"id": [1], "title": ["d"]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "reclaim_out")
    assert out["row_count"] == 4
    assert out["rows_tasks"] == 2
    assert out["rows_habits_daily"] == 1
    assert out["rows_habit_templates"] == 1


def test_empty_tables_return_empty_dataframe_with_warning_not_failure(mod, monkeypatch):
    component = mod.ReclaimIngestionComponent(
        asset_name="reclaim_out",
        api_token="tok_abc123",
        resources="tasks",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={"tasks": pd.DataFrame()},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "reclaim_out")
    assert "row_count" not in out  # falls back to base_metadata only, per component.py


# --- persist_only / non-SQL destination path (shared boilerplate, still worth covering) --

def test_persist_only_emits_materialize_result_without_querying_back(mod, monkeypatch):
    component = mod.ReclaimIngestionComponent(
        asset_name="reclaim_out",
        api_token="tok_abc123",
        resources="tasks",
        persist_only=True,
    )
    result, fake_pipeline, _captured = _materialize(
        mod, component,
        table_rows={"tasks": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    # sql_client() exists on the fake pipeline but should never be reached
    # (no queries recorded) since persist_only short-circuits before it.
    assert fake_pipeline.sql_client_instance.queries == []
