"""Loads component.py directly so tests don't need the parent package installed."""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg
import pytest


def load_component_module() -> ModuleType:
    component_py = pathlib.Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("s3_copy_object_asset_component", component_py)
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


class FakeS3Client:
    def __init__(self):
        self.copy_calls = []
        self.copy_delay_seconds = 0

    def copy_object(self, **kwargs):
        if self.copy_delay_seconds:
            import time
            time.sleep(self.copy_delay_seconds)
        self.copy_calls.append(kwargs)
