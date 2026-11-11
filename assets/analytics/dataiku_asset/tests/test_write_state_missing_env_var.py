"""A missing/unreachable credential during state refresh must not crash the whole code location.

Regression: `write_state_to_path` called `_fetch_scenarios(host, api_key, ...)` directly --
a real HTTP call -- with no guard. `StateBackedComponent.build_defs` runs `write_state_to_path`
synchronously on every `dagster dev` reload (and whenever state hasn't been refreshed yet), so an
unset `host_env_var`/`api_key_env_var`, or an unreachable Dataiku instance, raised straight out of
`build_defs` and crashed the ENTIRE code location -- not just this component's own assets. The
fetch is now wrapped so any failure falls back to an empty cached state instead, same as the
existing `talend_cloud_workspace` precedent.
"""
import json

import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


def test_missing_env_vars_writes_empty_state_instead_of_raising(mod, monkeypatch, tmp_path):
    monkeypatch.delenv("DK_HOST", raising=False)
    monkeypatch.delenv("DK_KEY", raising=False)
    comp = mod.DataikuAssetComponent(host_env_var="DK_HOST", api_key_env_var="DK_KEY")
    state_path = tmp_path / "state"

    comp.write_state_to_path(state_path)  # must not raise

    assert json.loads(state_path.read_text()) == {}


def test_unreachable_api_also_writes_empty_state_instead_of_raising(mod, monkeypatch, tmp_path):
    monkeypatch.setenv("DK_HOST", "https://dataiku.example.com")
    monkeypatch.setenv("DK_KEY", "fake-key")

    def _raise(*args, **kwargs):
        raise ConnectionError("no route to host")

    monkeypatch.setattr(mod, "_fetch_scenarios", _raise)
    comp = mod.DataikuAssetComponent(host_env_var="DK_HOST", api_key_env_var="DK_KEY")
    state_path = tmp_path / "state"

    comp.write_state_to_path(state_path)  # must not raise

    assert json.loads(state_path.read_text()) == {}
