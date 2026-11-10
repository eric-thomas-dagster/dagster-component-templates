"""on_schema_change/exclude_columns/max_rows_per_resource for ServiceNowIngestionComponent.

This component has no conftest.py (its own test_servicenow_ingestion.py loads the
module and fakes dlt inline) and that file's own `_patch_dlt` fakes
`rest_api_source(config)` to return a bare `object()` -- fine for config-capture
checks, but it has no `.resources` to exercise exclude_columns/hash_columns/
max_rows_per_resource against. This file defines its own `_Permissive`-style fake
(same duck-typed stand-in used by every other getattr-style component in this repo's
dlt rollout) so those can be exercised end-to-end too, and separately verifies
on_schema_change via the config dict directly, since that's resolved BEFORE
rest_api_source(config) is even called.
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("servicenow_ingestion_component_adv", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

ServiceNowIngestionComponent = _mod.ServiceNowIngestionComponent


def _build_component(**overrides):
    defaults = dict(
        asset_name="test_asset",
        instance_env_var="SNOW_INSTANCE",
        username_env_var="SNOW_USERNAME",
        password_env_var="SNOW_PASSWORD",
        tables="incident",
    )
    defaults.update(overrides)
    return ServiceNowIngestionComponent(**defaults)


class _Permissive:
    def __getattr__(self, name):
        return self

    def __call__(self, *args, **kwargs):
        return self

    def __iter__(self):
        return iter([self])


class _Cursor:
    def df(self):
        import pandas as pd
        return pd.DataFrame({"sys_id": []})


class _Sql:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute_query(self, sql, *a, **k):
        from contextlib import contextmanager

        @contextmanager
        def cm():
            yield _Cursor()
        return cm()


class _Pipeline:
    def run(self, *a, **k):
        return "load-info"

    def sql_client(self):
        return _Sql()


def _patch_dlt_permissive(monkeypatch, captured_configs):
    monkeypatch.setattr(_mod.dlt, "pipeline", lambda **kw: _Pipeline())
    import dlt.sources.rest_api as _rest_api

    def _fake_rest_api_source(config):
        captured_configs.append(config)
        return _Permissive()

    monkeypatch.setattr(_rest_api, "rest_api_source", _fake_rest_api_source)


def _set_env(monkeypatch):
    monkeypatch.setenv("SNOW_INSTANCE", "mycompany")
    monkeypatch.setenv("SNOW_USERNAME", "svc_account")
    monkeypatch.setenv("SNOW_PASSWORD", "hunter2")


def test_on_schema_change_sets_resource_defaults_before_rest_api_source_is_called(monkeypatch):
    _set_env(monkeypatch)
    captured = []
    _patch_dlt_permissive(monkeypatch, captured)
    component = _build_component(on_schema_change="block_all")
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert captured[-1]["resource_defaults"]["schema_contract"] == {
        "tables": "evolve", "columns": "freeze", "data_type": "freeze",
    }


def test_on_schema_change_default_does_not_set_resource_defaults(monkeypatch):
    _set_env(monkeypatch)
    captured = []
    _patch_dlt_permissive(monkeypatch, captured)
    component = _build_component()
    asset_def = component.build_defs(context=None).assets[0]
    dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert "resource_defaults" not in captured[-1]


def test_on_schema_change_rejects_invalid_value():
    with pytest.raises(Exception):
        _build_component(on_schema_change="bogus")


def test_exclude_columns_does_not_crash(monkeypatch):
    _set_env(monkeypatch)
    _patch_dlt_permissive(monkeypatch, [])
    component = _build_component(exclude_columns=["notes"])
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success


def test_exclude_columns_applied_before_hash_columns():
    exclude_map = _mod._make_exclude_columns_map(["email"])
    hash_map = _mod._make_hash_columns_map(["email"], "s")
    assert hash_map(exclude_map({"email": "a@b.com"})) == {}


def test_max_rows_per_resource_does_not_crash(monkeypatch):
    _set_env(monkeypatch)
    _patch_dlt_permissive(monkeypatch, [])
    component = _build_component(max_rows_per_resource=10)
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success


def test_no_advanced_options_set_is_a_no_op(monkeypatch):
    _set_env(monkeypatch)
    _patch_dlt_permissive(monkeypatch, [])
    component = _build_component()
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success


def test_resolve_schema_contract_maps_each_value():
    assert _mod._resolve_schema_contract("allow_all") == {"tables": "evolve", "columns": "evolve", "data_type": "evolve"}
    assert _mod._resolve_schema_contract("allow_new_columns") == {"tables": "evolve", "columns": "evolve", "data_type": "freeze"}
    assert _mod._resolve_schema_contract("block_all") == {"tables": "evolve", "columns": "freeze", "data_type": "freeze"}
    with pytest.raises(ValueError, match="on_schema_change"):
        _mod._resolve_schema_contract("bogus")


def test_make_exclude_columns_map_drops_listed_columns_and_skips_missing():
    fn = _mod._make_exclude_columns_map(["ssn", "missing"])
    assert fn({"id": 1, "ssn": "x", "name": "kept"}) == {"id": 1, "name": "kept"}
