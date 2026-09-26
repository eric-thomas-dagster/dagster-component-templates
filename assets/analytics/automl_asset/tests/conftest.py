"""Shared test helpers for AutoMLAssetComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed.
"""
import importlib.util
import pathlib
from types import ModuleType

import pytest


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "automl_asset_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _flaml_automl_available() -> bool:
    try:
        from flaml import AutoML  # noqa: F401
        return True
    except ImportError:
        return False


requires_flaml_automl = pytest.mark.skipif(
    not _flaml_automl_available(),
    reason='requires flaml[automl] (pip install "flaml[automl]"; on macOS also `brew install libomp`)',
)
