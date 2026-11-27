import importlib.util
from pathlib import Path

import pytest


def load_component_module():
    here = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("pubsub_resource_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def mod():
    return load_component_module()
