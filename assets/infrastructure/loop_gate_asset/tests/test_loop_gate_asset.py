"""Real, committed tests for LoopGateAssetComponent -- real dg.materialize() throughout."""
import importlib

import dagster as dg
import pytest

from .conftest import load_component_module

FIXTURES_PATH = "assets.infrastructure.loop_gate_asset.tests.fixture_callables"


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def fx():
    m = importlib.import_module(FIXTURES_PATH)
    m.reset()
    return m


def _upstream_asset(key: str, value):
    @dg.asset(key=dg.AssetKey.from_user_string(key))
    def _a():
        return value
    return _a


# --- fixed-count loop (no until_callable) ---------------------------------------------------

def test_no_until_runs_exactly_max_iterations_and_succeeds(mod, fx):
    component = mod.LoopGateAssetComponent(
        asset_key="d/loop", callable_path=f"{FIXTURES_PATH}:increment", max_iterations=5,
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert fx.CALLS["n"] == 5
    assert result.output_for_node("d__loop") == 5
    mat = result.asset_materializations_for_node("d__loop")[0]
    assert mat.metadata["iterations_run"].value == 5
    assert "converged" not in mat.metadata


# --- until_callable stops the loop early ------------------------------------------------------

def test_until_callable_stops_loop_early(mod, fx):
    component = mod.LoopGateAssetComponent(
        asset_key="d/loop", callable_path=f"{FIXTURES_PATH}:increment",
        until_callable_path=f"{FIXTURES_PATH}:stop_at_three", max_iterations=10,
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert fx.CALLS["n"] == 3  # stopped after the 3rd iteration, not all 10
    assert result.output_for_node("d__loop") == 3
    mat = result.asset_materializations_for_node("d__loop")[0]
    assert mat.metadata["iterations_run"].value == 3
    assert mat.metadata["converged"].value is True


def test_until_callable_with_reason_tuple(mod, fx):
    component = mod.LoopGateAssetComponent(
        asset_key="d/loop", callable_path=f"{FIXTURES_PATH}:increment",
        until_callable_path=f"{FIXTURES_PATH}:stop_at_three_with_reason", max_iterations=10,
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert result.output_for_node("d__loop") == 3


# --- until_callable never stops -> exhausts max_iterations -> fails ---------------------------

def test_until_callable_never_stops_exhausts_and_fails(mod, fx):
    component = mod.LoopGateAssetComponent(
        asset_key="d/loop", callable_path=f"{FIXTURES_PATH}:increment",
        until_callable_path=f"{FIXTURES_PATH}:never_stops", max_iterations=4,
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize(
        [asset_def], resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=False,
    )
    assert not result.success
    assert fx.CALLS["n"] == 4  # ran every allotted iteration before failing


# --- until_callable returns something other than bool/(bool, reason) -> fails immediately -----

def test_until_callable_bad_return_value_fails(mod, fx):
    component = mod.LoopGateAssetComponent(
        asset_key="d/loop", callable_path=f"{FIXTURES_PATH}:increment",
        until_callable_path=f"{FIXTURES_PATH}:returns_none", max_iterations=10,
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize(
        [asset_def], resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=False,
    )
    assert not result.success
    assert fx.CALLS["n"] == 1  # failed on the very first iteration's bad until return


# --- LoopState.previous/.index match Airflow 3.4's real contract ---------------------------
# (loop.index is 1-based -- the first iteration is 1, not 0 -- and loop.previous is the single
# immediately-preceding iteration's result, None on iteration 1, NOT a list of every prior
# result. Verified against Airflow's own 3.4.0 docs, not assumed.)

def test_loop_state_previous_and_index_match_airflow_contract(mod, fx):
    component = mod.LoopGateAssetComponent(
        asset_key="d/loop", callable_path=f"{FIXTURES_PATH}:increment",
        until_callable_path=f"{FIXTURES_PATH}:previous_matches_prior_result", max_iterations=10,
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success  # the until_callable's own assertion would have failed the run otherwise
    assert result.output_for_node("d__loop") == 3


# --- inputs (TaskFlow-style upstream binding) -------------------------------------------------

def test_inputs_bound_to_body_callable_every_iteration(mod, fx):
    upstream = _upstream_asset("d/up", "seed")
    component = mod.LoopGateAssetComponent(
        asset_key="d/loop", callable_path=f"{FIXTURES_PATH}:wants_upstream",
        inputs={"upstream_a": "d/up"}, max_iterations=3,
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([upstream, asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert fx.CALLS["upstream_a"] == "seed"
    assert result.output_for_node("d__loop") == ("seed", 3)


# --- validation ---------------------------------------------------------------------------

def test_max_iterations_must_be_positive(mod):
    with pytest.raises(Exception):
        mod.LoopGateAssetComponent(asset_key="d/loop", callable_path=f"{FIXTURES_PATH}:increment", max_iterations=0)


def test_timeout_seconds_bounds_the_whole_loop(mod, fx):
    component = mod.LoopGateAssetComponent(
        asset_key="d/loop", callable_path=f"{FIXTURES_PATH}:slow_increment",
        max_iterations=1000, timeout_seconds=0.1,
    )
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize(
        [asset_def], resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=False,
    )
    assert not result.success


def test_pool_and_retry_policy_are_wired(mod, fx):
    component = mod.LoopGateAssetComponent(
        asset_key="d/loop", callable_path=f"{FIXTURES_PATH}:increment", max_iterations=2,
        pool="my_pool", retry_policy_max_retries=2, retry_policy_delay_seconds=1,
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.op.pool == "my_pool"
    assert asset_def.op.retry_policy.max_retries == 2
