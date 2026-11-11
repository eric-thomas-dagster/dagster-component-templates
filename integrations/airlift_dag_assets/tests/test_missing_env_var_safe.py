"""A missing Airflow credential env var must not crash the whole code location.

Regression: `build_defs` used to resolve `auth_user_env_var`/`auth_pass_env_var` via
`dg.EnvVar(...).get_value() or ""` and feed the (possibly empty) result straight into
`AirflowInstance`/`build_defs_from_airflow_instance`, which calls Airflow's REST API
eagerly as part of discovery -- an unset or wrong credential fails that call and the
raw exception propagated out of `build_defs`, crashing code-location load for every
other component in the same location. `build_defs` now checks both env vars are set
*before* importing `dagster_airlift` or calling out to Airflow, and returns an empty
`Definitions()` (with a warning log) instead.
"""
import dagster as dg
import pytest

from .conftest import load_component_module


class _FakeLog:
    def __init__(self):
        self.warnings = []

    def warning(self, msg):
        self.warnings.append(msg)


class _FakeContext:
    def __init__(self):
        self.log = _FakeLog()


@pytest.fixture
def mod():
    return load_component_module()


def test_missing_both_env_vars_returns_empty_defs_not_a_crash(mod, monkeypatch):
    monkeypatch.delenv("AIRFLOW_USERNAME", raising=False)
    monkeypatch.delenv("AIRFLOW_PASSWORD", raising=False)
    comp = mod.AirliftDagAssetsComponent(airflow_url="https://airflow.example.com", dag_id="my_dag")
    context = _FakeContext()
    defs = comp.build_defs(context)
    assert isinstance(defs, dg.Definitions)
    assert not defs.assets
    assert context.log.warnings


def test_missing_one_env_var_also_returns_empty_defs(mod, monkeypatch):
    monkeypatch.setenv("AIRFLOW_USERNAME", "admin")
    monkeypatch.delenv("AIRFLOW_PASSWORD", raising=False)
    comp = mod.AirliftDagAssetsComponent(airflow_url="https://airflow.example.com", dag_id="my_dag")
    context = _FakeContext()
    defs = comp.build_defs(context)
    assert not defs.assets
    assert context.log.warnings
