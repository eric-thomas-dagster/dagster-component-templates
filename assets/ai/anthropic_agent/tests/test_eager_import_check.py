"""`build_defs` warns (doesn't raise) when the anthropic SDK isn't installed, so `dg check defs`
surfaces a missing provider SDK up front instead of the first materialization failing with a
buried ImportError -- without breaking the whole code location's load over one missing optional
dep."""
import sys

import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_warns_when_anthropic_is_not_installed(mod, monkeypatch):
    monkeypatch.setitem(sys.modules, "anthropic", None)
    component = mod.AnthropicAgentComponent(asset_name="agent_out", prompt="hi")
    with pytest.warns(UserWarning, match="anthropic SDK is not installed"):
        component.build_defs(None)


def test_no_warning_when_anthropic_is_installed(mod, recwarn):
    pytest.importorskip("anthropic")
    component = mod.AnthropicAgentComponent(asset_name="agent_out", prompt="hi")
    component.build_defs(None)
    assert not any("anthropic SDK is not installed" in str(w.message) for w in recwarn.list)
