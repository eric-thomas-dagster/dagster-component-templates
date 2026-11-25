import importlib.util
import sys
from pathlib import Path

import pytest

_COMPONENT_PATH = Path(__file__).resolve().parent.parent / "component.py"


def load_component_module():
    spec = importlib.util.spec_from_file_location("cloudflare_mcp_server_component", _COMPONENT_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return load_component_module()
