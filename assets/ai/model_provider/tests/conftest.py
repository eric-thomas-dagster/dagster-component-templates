"""Shared test helpers for ModelProviderComponent.

Loads component.py directly via importlib so tests don't require the parent
package to be pip-installed.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location("model_provider_component", component_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def execute_with_checks(component_defs, instance=None, resources=None):
    """dg.materialize() does not execute/report @asset_checks (confirmed in
    rag_grounding_check's own tests) -- build a real Definitions + implicit job instead."""
    full_defs = dg.Definitions(
        assets=list(component_defs.assets),
        asset_checks=list(component_defs.asset_checks),
        resources=resources or {},
    )
    job = full_defs.get_implicit_global_asset_job_def()
    return job.execute_in_process(instance=instance)
