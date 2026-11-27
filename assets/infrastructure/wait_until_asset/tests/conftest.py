"""Loads component.py directly so tests don't need the parent package installed."""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg
import pytest


def load_component_module() -> ModuleType:
    component_py = pathlib.Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("wait_until_asset_component", component_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return load_component_module()


def materialize(component, **kwargs):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()}, **kwargs)


def metadata_for(result, asset_name):
    """Look up a materialization's metadata by asset key rather than by the internal op/node
    name -- the op name is now derived per-instance (see component.py's `_sanitize_identifier`
    use) to avoid collisions between multiple instances, so it no longer equals asset_name."""
    key = dg.AssetKey.from_user_string(asset_name)
    for event in result.get_asset_materialization_events():
        if event.asset_key == key:
            return event.materialization.metadata
    return None
