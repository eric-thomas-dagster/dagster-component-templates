"""Shared test helpers for snowflake_dynamic_table_refresh_asset (self-contained; loads
component.py via importlib).

Only the external Snowflake wire boundary is faked; the component runs for real under
dg.materialize().
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "snowflake_dynamic_table_refresh_asset_component", here / "component.py"
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
