"""Committed regression tests for LatticeIngestionComponent.

The one external call this component makes -- the dlt pipeline run against
the real Lattice REST API, and the follow-up sql_client() query-back -- is
monkeypatched wholesale via `install_fake_dlt` (see conftest.py).
Everything the component actually owns is exercised for real: resource-config
construction (`_build_resources_config`), the bearer-auth config shape, and
the per-resource DataFrame combination + metadata shape.
"""
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

def test_default_resources_expands_to_four_resources(mod):
    config_resources = mod._build_resources_config("users,goals,updates,reviews")
    names = [r["name"] for r in config_resources]
    assert names == ["users", "goals", "updates", "reviews"]


def test_users_resource_uses_real_path_and_data_selector(mod):
    config_resources = mod._build_resources_config("users")
    assert len(config_resources) == 1
    assert config_resources[0]["name"] == "users"
    assert config_resources[0]["endpoint"]["path"] == "v1/users"
    assert config_resources[0]["endpoint"]["data_selector"] == "data"


def test_goals_resource_path(mod):
    config_resources = mod._build_resources_config("goals")
    assert config_resources[0]["endpoint"]["path"] == "v1/goals"


def test_updates_resource_path(mod):
    config_resources = mod._build_resources_config("updates")
    assert config_resources[0]["endpoint"]["path"] == "v1/updates"


def test_reviews_resource_path(mod):
    config_resources = mod._build_resources_config("reviews")
    assert config_resources[0]["endpoint"]["path"] == "v1/reviews"


def test_unknown_resource_name_is_ignored(mod):
    config_resources = mod._build_resources_config("users,not_a_real_resource")
    assert [r["name"] for r in config_resources] == ["users"]


def test_subset_of_resources_preserves_requested_order(mod):
    config_resources = mod._build_resources_config("reviews,users")
    assert [r["name"] for r in config_resources] == ["reviews", "users"]


# --- end-to-end: bearer auth + base URL + full resource set -----------------------

def test_source_config_uses_bearer_auth_and_real_base_url(mod, monkeypatch):
    component = mod.LatticeIngestionComponent(
        asset_name="lattice_out",
        api_key="lattice_key_abc123",
    )
    _result, _pipeline, captured = _materialize(
        mod, component,
        table_rows={"users": pd.DataFrame({"id": [1, 2]})},
        monkeypatch=monkeypatch,
    )
    config = captured["config"]
    assert config["client"]["base_url"] == "https://api.latticehq.com"
    assert config["client"]["auth"] == {"type": "bearer", "token": "lattice_key_abc123"}


def test_default_resources_requested_metadata(mod, monkeypatch):
    component = mod.LatticeIngestionComponent(
        asset_name="lattice_out",
        api_key="lattice_key_abc123",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "users": pd.DataFrame({"id": [1]}),
            "goals": pd.DataFrame({"id": [1]}),
            "updates": pd.DataFrame({"id": [1]}),
            "reviews": pd.DataFrame({"id": [1]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "lattice_out")
    assert out["resources_requested"] == ["users", "goals", "updates", "reviews"]
    assert set(out["resources_loaded"]) == {"users", "goals", "updates", "reviews"}
    assert out["row_count"] == 4


# --- DataFrame combination from the (fake) duckdb query-back -----------------------

def test_combines_per_resource_tables_into_one_dataframe(mod, monkeypatch):
    component = mod.LatticeIngestionComponent(
        asset_name="lattice_out",
        api_key="lattice_key_abc123",
        resources="users,goals",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={
            "users": pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}),
            "goals": pd.DataFrame({"id": [1], "name": ["c"]}),
        },
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "lattice_out")
    assert out["row_count"] == 3
    assert out["rows_users"] == 2
    assert out["rows_goals"] == 1


def test_empty_tables_return_empty_dataframe_with_warning_not_failure(mod, monkeypatch):
    component = mod.LatticeIngestionComponent(
        asset_name="lattice_out",
        api_key="lattice_key_abc123",
        resources="users",
    )
    result, _pipeline, _captured = _materialize(
        mod, component,
        table_rows={"users": pd.DataFrame()},
        monkeypatch=monkeypatch,
    )
    assert result.success
    out = _metadata_for(result, "lattice_out")
    assert "row_count" not in out  # falls back to base_metadata only, per component.py


# --- persist_only / non-SQL destination path (shared boilerplate, still worth covering) --

def test_persist_only_emits_materialize_result_without_querying_back(mod, monkeypatch):
    component = mod.LatticeIngestionComponent(
        asset_name="lattice_out",
        api_key="lattice_key_abc123",
        resources="users",
        persist_only=True,
    )
    result, fake_pipeline, _captured = _materialize(
        mod, component,
        table_rows={"users": pd.DataFrame({"id": [1]})},
        monkeypatch=monkeypatch,
    )
    assert result.success
    # sql_client() exists on the fake pipeline but should never be reached
    # (no queries recorded) since persist_only short-circuits before it.
    assert fake_pipeline.sql_client_instance.queries == []
