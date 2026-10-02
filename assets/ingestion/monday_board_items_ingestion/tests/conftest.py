"""Shared test helpers for MondayBoardItemsIngestionComponent.

Loads component.py (and the paired resource's component.py) directly via
importlib so tests don't require the parent package to be pip-installed.
Mirrors the convention in assets/dbt/enriched_dbt_project/tests/conftest.py.
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "monday_board_items_ingestion_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def load_resource_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent.parent.parent.parent
    resource_py = here / "resources" / "monday_resource" / "component.py"
    spec = importlib.util.spec_from_file_location(
        "monday_resource_component", resource_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
