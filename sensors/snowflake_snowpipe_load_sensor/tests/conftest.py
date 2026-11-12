"""Shared test helpers for snowflake_snowpipe_load_sensor (self-contained; loads
component.py via importlib). Only the Snowflake wire boundary is faked.
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("snowflake_snowpipe_load_sensor_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
