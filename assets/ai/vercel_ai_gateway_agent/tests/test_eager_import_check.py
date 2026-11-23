"""`build_defs` warns (doesn't raise) when the openai SDK isn't installed (this component talks
to Vercel's OpenAI-compatible gateway through it), so `dg check defs` surfaces a missing
dependency up front instead of the first materialization failing with a buried ImportError --
without breaking the whole code location's load over one missing optional dep."""
import sys

import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_warns_when_openai_is_not_installed(mod, monkeypatch):
    monkeypatch.setitem(sys.modules, "openai", None)
    component = mod.VercelAIGatewayAgentComponent(asset_name="agent_out", prompt="hi")
    with pytest.warns(UserWarning, match="openai SDK is not installed"):
        component.build_defs(None)


def test_no_warning_when_openai_is_installed(mod, recwarn):
    component = mod.VercelAIGatewayAgentComponent(asset_name="agent_out", prompt="hi")
    component.build_defs(None)
    assert not any("openai SDK is not installed" in str(w.message) for w in recwarn.list)
