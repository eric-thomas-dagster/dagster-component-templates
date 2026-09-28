"""Shared test helpers for RagGroundingCheckComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg
import pytest


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "rag_grounding_check_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def make_upstream_asset(name: str, df):
    """A fresh @asset closure per call -- needed because materializing the
    same function object twice under different DataFrames in one test file
    would otherwise share the same closed-over `df`."""
    @dg.asset(name=name)
    def _upstream():
        return df
    return _upstream


def execute_with_checks(component_defs, upstream_asset, instance=None, resources=None):
    """Build a full Definitions (asset + its asset_check) and execute it as
    a real job -- dg.materialize() doesn't run/report asset checks, unlike
    the AssetsDefinition + AssetChecksDefinition + get_implicit_global_asset_job_def
    path used here (confirmed live: materialize()'s result.get_asset_check_evaluations()
    is always empty; this path is what actually evaluates checks)."""
    assets = list(component_defs.assets)
    if upstream_asset is not None:
        assets = assets + [upstream_asset]
    full_defs = dg.Definitions(assets=assets, asset_checks=list(component_defs.asset_checks), resources=resources or {})
    job = full_defs.get_implicit_global_asset_job_def()
    return job.execute_in_process(instance=instance)


def _duckdb_available() -> bool:
    try:
        import duckdb  # noqa: F401
        import dagster_duckdb  # noqa: F401
        return True
    except ImportError:
        return False


requires_duckdb = pytest.mark.skipif(
    not _duckdb_available(),
    reason="requires duckdb + dagster-duckdb (pip install duckdb dagster-duckdb)",
)
