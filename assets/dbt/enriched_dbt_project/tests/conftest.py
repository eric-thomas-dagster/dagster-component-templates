"""Shared test helpers for EnrichedDbtProjectComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. No real `dbt` CLI invocation happens in
these tests -- only `_enrich_spec` / `_build_external_package_specs` /
`get_asset_spec` are called directly against hand-built manifest dicts, so a
`DbtProject` pointing at a minimal fixture directory (just a
`dbt_project.yml`, no real models) is enough to construct a real component
instance.
"""
import importlib.util
import pathlib
from types import ModuleType

import pytest


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "enriched_dbt_project_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


FIXTURE_PROJECT_DIR = pathlib.Path(__file__).resolve().parent / "fixture_dbt_project"


def _dagster_dbt_available() -> bool:
    try:
        import dagster_dbt  # noqa: F401
        return True
    except ImportError:
        return False


requires_dagster_dbt = pytest.mark.skipif(
    not _dagster_dbt_available(),
    reason="requires dagster-dbt (pip install dagster-dbt)",
)
