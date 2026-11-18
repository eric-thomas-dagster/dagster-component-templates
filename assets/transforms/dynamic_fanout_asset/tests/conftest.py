"""Loads component.py directly so tests don't need the parent package installed."""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg
import pytest


def load_component_module() -> ModuleType:
    component_py = pathlib.Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("dynamic_fanout_asset_component", component_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return load_component_module()


def materialize(component, assets_extra=None, **kwargs):
    asset_def = component.build_defs(context=None).assets[0]
    assets = ([*assets_extra, asset_def] if assets_extra else [asset_def])
    return dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()}, **kwargs)
