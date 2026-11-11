"""A missing/unreachable database during state refresh must not crash the whole code location.

Regression: `write_state_to_path` called `_introspect_schemas(database_url=db_url, ...)` directly
-- a real DB connection -- with no guard. `StateBackedComponent.build_defs` runs
`write_state_to_path` synchronously on every `dagster dev` reload, so an unset
`database_url_env_var`, or an unreachable database, raised straight out of `build_defs` and
crashed the ENTIRE code location -- not just this component's own assets. The introspection call
is now wrapped so any failure falls back to an empty cached state instead, same as the existing
`talend_cloud_workspace` precedent.
"""
import json

import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


def test_missing_env_var_writes_empty_state_instead_of_raising(mod, monkeypatch, tmp_path):
    monkeypatch.delenv("WAREHOUSE_URL", raising=False)
    comp = mod.WarehouseSchemaAssetsComponent(database_url_env_var="WAREHOUSE_URL")
    state_path = tmp_path / "state"

    comp.write_state_to_path(state_path)  # must not raise

    assert json.loads(state_path.read_text()) == []


def test_unreachable_database_also_writes_empty_state_instead_of_raising(mod, monkeypatch, tmp_path):
    monkeypatch.setenv("WAREHOUSE_URL", "postgresql://user:pwd@nope:5432/db")

    def _raise(*args, **kwargs):
        raise ConnectionError("could not connect to server")

    monkeypatch.setattr(mod, "_introspect_schemas", _raise)
    comp = mod.WarehouseSchemaAssetsComponent(database_url_env_var="WAREHOUSE_URL")
    state_path = tmp_path / "state"

    comp.write_state_to_path(state_path)  # must not raise

    assert json.loads(state_path.read_text()) == []
