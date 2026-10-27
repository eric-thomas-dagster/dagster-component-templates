"""Importlib-based module loader, same convention as every other component's
tests/conftest.py in this repo -- works without the package being pip-installed."""
import importlib.util
from pathlib import Path

import pytest

_MODULE_PATH = (
    Path(__file__).parent.parent / "src" / "prospect_recon" / "defs" / "prospect_recon_pipeline" / "definitions.py"
)


def load_pipeline_module():
    spec = importlib.util.spec_from_file_location("prospect_recon_pipeline_definitions", _MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def mod():
    return load_pipeline_module()
