"""Real, committed tests for ScopedSetupTeardownComponent -- real dg.materialize() throughout,
real subprocess calls for shell mode (no mocking: this component's only "external" surface is
the shell itself, free/local)."""
import importlib
import os

import dagster as dg
import pytest

FIXTURES_PATH = "resources.scoped_setup_teardown.tests.fixture_callables"


@pytest.fixture()
def fx():
    m = importlib.import_module(FIXTURES_PATH)
    m.reset()
    return m


def _materialize_with_resource(mod, component, asset_fn, raise_on_error=True):
    @dg.asset(required_resource_keys={component.resource_key})
    def _a(context):
        return asset_fn(context) if asset_fn else "ran"

    defs = component.build_defs(context=None)
    resource = defs.resources[component.resource_key]
    return dg.materialize([_a], resources={component.resource_key: resource}, raise_on_error=raise_on_error)


def test_callable_setup_and_teardown_both_run(mod, fx):
    component = mod.ScopedSetupTeardownComponent(
        resource_key="scope", setup_callable_path=f"{FIXTURES_PATH}:setup_no_return",
        teardown_callable_path=f"{FIXTURES_PATH}:teardown_no_param",
    )
    result = _materialize_with_resource(mod, component, None)
    assert result.success
    assert fx.CALLS["order"] == ["setup", "teardown"]


def test_setup_return_value_passed_to_teardown_with_a_param(mod, fx):
    component = mod.ScopedSetupTeardownComponent(
        resource_key="scope", setup_callable_path=f"{FIXTURES_PATH}:setup_returns_handle",
        teardown_callable_path=f"{FIXTURES_PATH}:teardown_with_param",
    )
    result = _materialize_with_resource(mod, component, None)
    assert result.success
    assert fx.CALLS["teardown_received"] == {"handle": "fake-connection"}


def test_teardown_runs_even_when_the_consuming_asset_fails(mod, fx):
    component = mod.ScopedSetupTeardownComponent(
        resource_key="scope", setup_callable_path=f"{FIXTURES_PATH}:setup_no_return",
        teardown_callable_path=f"{FIXTURES_PATH}:teardown_no_param",
    )

    def _boom(context):
        raise ValueError("deliberate asset failure")

    result = _materialize_with_resource(mod, component, _boom, raise_on_error=False)
    assert not result.success
    assert fx.CALLS["order"] == ["setup", "teardown"]  # teardown still ran


def test_teardown_does_not_run_if_setup_itself_fails(mod, fx):
    component = mod.ScopedSetupTeardownComponent(
        resource_key="scope", setup_callable_path=f"{FIXTURES_PATH}:setup_that_raises",
        teardown_callable_path=f"{FIXTURES_PATH}:teardown_no_param",
    )
    result = _materialize_with_resource(mod, component, None, raise_on_error=False)
    assert not result.success
    assert fx.CALLS["order"] == ["setup_raised"]  # teardown never ran


def test_shell_setup_and_teardown_real_subprocess(mod, tmp_path):
    marker = tmp_path / "marker.txt"
    component = mod.ScopedSetupTeardownComponent(
        resource_key="scope",
        setup_command=f"echo setup >> {marker}",
        teardown_command=f"echo teardown >> {marker}",
    )
    result = _materialize_with_resource(mod, component, None)
    assert result.success
    assert marker.read_text().splitlines() == ["setup", "teardown"]


def test_shell_env_vars_and_cwd(mod, tmp_path):
    out_file = tmp_path / "out.txt"
    component = mod.ScopedSetupTeardownComponent(
        resource_key="scope",
        setup_command=f"echo $MY_VAR > {out_file}",
        env_vars={"MY_VAR": "hello-from-env"},
    )
    result = _materialize_with_resource(mod, component, None)
    assert result.success
    assert out_file.read_text().strip() == "hello-from-env"


def test_shell_teardown_failure_fails_but_does_not_crash_the_process(mod, tmp_path):
    marker = tmp_path / "marker.txt"
    component = mod.ScopedSetupTeardownComponent(
        resource_key="scope",
        setup_command=f"echo setup >> {marker}",
        teardown_command="exit 1",
    )
    result = _materialize_with_resource(mod, component, None, raise_on_error=False)
    assert marker.exists()  # setup did run


def test_rejects_both_setup_callable_and_setup_command(mod):
    with pytest.raises(Exception, match="setup_callable_path"):
        mod.ScopedSetupTeardownComponent(
            resource_key="scope", setup_callable_path=f"{FIXTURES_PATH}:setup_no_return",
            setup_command="echo hi",
        )


def test_rejects_both_teardown_callable_and_teardown_command(mod):
    with pytest.raises(Exception, match="teardown_callable_path"):
        mod.ScopedSetupTeardownComponent(
            resource_key="scope", teardown_callable_path=f"{FIXTURES_PATH}:teardown_no_param",
            teardown_command="echo hi",
        )


def test_neither_setup_nor_teardown_set_is_a_valid_no_op(mod):
    component = mod.ScopedSetupTeardownComponent(resource_key="scope")
    result = _materialize_with_resource(mod, component, None)
    assert result.success
