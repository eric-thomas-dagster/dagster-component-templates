"""Shared test helpers for DataprocResourceComponent (self-contained; loads component.py via
importlib so tests don't require the parent package to be pip-installed).

`google-cloud-dataproc` is the external boundary mocked here (ClusterControllerClient /
JobControllerClient), matching this repo's "mock only the paid/external call" convention.
"""
import importlib.util
import pathlib
from types import ModuleType

import pytest


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("dataproc_resource_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return load_component_module()
