"""Loads component.py directly so tests don't need the parent package installed."""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg
import pytest


def load_component_module() -> ModuleType:
    component_py = pathlib.Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("partition_window_sensor_component", component_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def instance():
    with dg.DagsterInstance.ephemeral() as inst:
        yield inst


def materialize_partitions(instance, asset_key: str, partition_keys):
    key = dg.AssetKey.from_user_string(asset_key)
    for pk in partition_keys:
        instance.report_runless_asset_event(dg.AssetMaterialization(asset_key=key, partition=pk))


def sensor_of(component):
    return list(component.build_defs(None).sensors)[0]


def tick(sensor, instance, cursor=None):
    return sensor(dg.build_sensor_context(instance=instance, cursor=cursor))
