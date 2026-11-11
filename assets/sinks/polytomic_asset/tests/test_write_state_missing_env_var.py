"""A missing/unreachable Polytomic API during state refresh must not crash the code location.

Regression: `write_state_to_path` called `_list_syncs(api_key)` directly -- a real HTTP call --
with no guard. `StateBackedComponent.build_defs` runs `write_state_to_path` synchronously on every
`dagster dev` reload, so an unset `api_key_env_var`, or an unreachable Polytomic API, raised
straight out of `build_defs` and crashed the ENTIRE code location -- not just this component's own
assets. The discovery call is now wrapped so any failure falls back to an empty cached state
instead, same as the existing `talend_cloud_workspace` precedent.
"""
import json

import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


def test_missing_env_var_writes_empty_state_instead_of_raising(mod, monkeypatch, tmp_path):
    monkeypatch.delenv("POLYTOMIC_API_KEY", raising=False)
    comp = mod.PolytomicAssetComponent(api_key_env_var="POLYTOMIC_API_KEY")
    state_path = tmp_path / "state"

    comp.write_state_to_path(state_path)  # must not raise

    assert json.loads(state_path.read_text()) == []


def test_unreachable_api_also_writes_empty_state_instead_of_raising(mod, monkeypatch, tmp_path):
    monkeypatch.setenv("POLYTOMIC_API_KEY", "fake-key")

    def _raise(*args, **kwargs):
        raise ConnectionError("no route to host")

    monkeypatch.setattr(mod, "_list_syncs", _raise)
    comp = mod.PolytomicAssetComponent(api_key_env_var="POLYTOMIC_API_KEY")
    state_path = tmp_path / "state"

    comp.write_state_to_path(state_path)  # must not raise

    assert json.loads(state_path.read_text()) == []
