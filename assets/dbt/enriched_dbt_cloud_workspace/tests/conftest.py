"""Shared test helpers for EnrichedDbtCloudWorkspaceComponent.

`DbtCloudWorkspace` construction makes no network calls, so a real
component instance can be built without hitting the dbt Cloud API -- these
tests only call `_enrich_spec` / `_build_external_package_specs` /
`get_asset_spec` directly against hand-built manifest dicts.

Loaded via `importlib.import_module` on the real dotted package path (NOT
the `spec_from_file_location` trick used by most other components' tests)
because component.py itself does relative imports of sibling modules in
this folder (`from ._job_selection import ...`, `from ._run_monitor import
...`) -- those only resolve when the module is imported as part of its
real parent package, not loaded standalone from an arbitrary file path.
"""
import importlib
from types import ModuleType
from typing import Any

import pytest


def load_component_module() -> ModuleType:
    return importlib.import_module("assets.dbt.enriched_dbt_cloud_workspace.component")


def make_component(mod: ModuleType, **overrides: Any):
    """Construct a real EnrichedDbtCloudWorkspaceComponent for tests.

    The class is `@dataclass`-decorated on top of a Pydantic `dg.Model`
    base (`DbtCloudComponent`) -- a pre-existing combination (not introduced
    by these changes). Its dataclass-generated `__init__` doesn't work for
    plain construction: `@dataclass` field collection only sees annotations
    on `@dataclass`-decorated classes in the MRO (missing base-class fields
    like `workspace` entirely), AND calling it bypasses Pydantic's own
    init, leaving required internal state (`__pydantic_fields_set__` etc.)
    never set up, so even attribute assignment inside that broken
    `__init__` crashes. Real usage always goes through the YAML/Resolvable
    component-loading path instead, which doesn't hit any of this.

    `model_construct` is Pydantic's own bypass-validation constructor --
    it sets up that internal state correctly and fills in every field's
    real default for anything not passed, so it works for both the
    dataclass-only fields and the inherited Pydantic ones (`workspace`,
    `select`, ...) uniformly."""
    return mod.EnrichedDbtCloudWorkspaceComponent.model_construct(**overrides)


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
