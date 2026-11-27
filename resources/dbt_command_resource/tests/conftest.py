"""Shared test helpers for DbtCommandResource.

Loads component.py directly via importlib so tests don't require the parent package to be
pip-installed. Tests run REAL `dbt` CLI invocations (via dbt-core + dbt-duckdb, both already
installed in this environment) against a tiny fixture project copied into a fresh tmp_path per
test -- no mocking of dbt itself, since the whole point of this component is to shell out to the
real CLI.
"""
import importlib.util
import pathlib
import shutil
from types import ModuleType

import pytest


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location("dbt_command_resource_component", component_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return load_component_module()


FIXTURE_PROJECT_DIR = pathlib.Path(__file__).resolve().parent / "fixture_dbt_project"


@pytest.fixture
def dbt_project(tmp_path):
    """Copy the fixture dbt project into tmp_path and write a duckdb profiles.yml pointing at a
    fresh db file there, so each test gets an isolated, writable project + warehouse."""
    project_dir = tmp_path / "project"
    shutil.copytree(FIXTURE_PROJECT_DIR, project_dir)
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    db_path = (tmp_path / "warehouse.duckdb").as_posix()
    (profiles_dir / "profiles.yml").write_text(f"""
dbt_command_asset_fixture:
  target: test
  outputs:
    test:
      type: duckdb
      path: {db_path!r}
      threads: 1
""")
    return {
        "project_dir": project_dir.as_posix(),
        "profiles_dir": profiles_dir.as_posix(),
    }


@pytest.fixture(autouse=True)
def _no_dbt_telemetry(monkeypatch):
    monkeypatch.setenv("DBT_SEND_ANONYMOUS_USAGE_STATS", "False")
    monkeypatch.setenv("DO_NOT_TRACK", "1")
    monkeypatch.setenv("DBT_VERSION_CHECK", "False")
