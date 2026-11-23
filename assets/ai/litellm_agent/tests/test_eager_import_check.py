"""`build_defs` warns (doesn't raise) when litellm isn't installed, so `dg check defs` surfaces a
missing provider SDK up front instead of the first materialization failing with a buried
ImportError -- without breaking the whole code location's load over one missing optional dep."""
import sys

import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_warns_when_litellm_is_not_installed(mod, monkeypatch):
    monkeypatch.setitem(sys.modules, "litellm", None)
    component = mod.LiteLLMAgentComponent(asset_name="agent_out", prompt="hi")
    with pytest.warns(UserWarning, match="litellm is not installed"):
        component.build_defs(None)


def test_no_warning_when_litellm_is_installed(mod, recwarn):
    component = mod.LiteLLMAgentComponent(asset_name="agent_out", prompt="hi")
    component.build_defs(None)
    assert not any("litellm is not installed" in str(w.message) for w in recwarn.list)
