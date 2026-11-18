"""Real, committed tests for ConditionalAssetComponent -- real dg.materialize() throughout,
asserting actual STEP_SKIPPED/STEP_SUCCESS outcomes (the real, verified Dagster mechanism this
component is built on), not just construction.
"""
import importlib

import dagster as dg
import pytest

from .conftest import load_component_module, step_statuses

FIXTURES_PATH = "assets.infrastructure.conditional_asset.tests.fixture_callables"


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


def _downstream_asset(key: str, upstream_key: str):
    @dg.asset(key=dg.AssetKey.from_user_string(key), ins={"u": dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_key))})
    def _d(u):
        return f"ran: {u}"
    return _d


# --- short_circuit --------------------------------------------------------------------------

def test_short_circuit_true_materializes_and_downstream_runs(mod):
    component = mod.ConditionalAssetComponent(
        mode="short_circuit", callable_path=f"{FIXTURES_PATH}:returns_true", asset_key="d/gate",
    )
    gate_def = component.build_defs(context=None).assets[0]
    downstream = _downstream_asset("d/after", "d/gate")
    result = dg.materialize([gate_def, downstream], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    statuses = step_statuses(result)
    assert statuses["d__after"] == "STEP_SUCCESS"


def test_short_circuit_false_skips_downstream(mod):
    component = mod.ConditionalAssetComponent(
        mode="short_circuit", callable_path=f"{FIXTURES_PATH}:returns_false", asset_key="d/gate",
    )
    gate_def = component.build_defs(context=None).assets[0]
    downstream = _downstream_asset("d/after", "d/gate")
    result = dg.materialize([gate_def, downstream], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success  # a skip is not a failure
    statuses = step_statuses(result)
    assert statuses["d__after"] == "STEP_SKIPPED"


def test_short_circuit_skip_cascades_through_a_chain(mod):
    component = mod.ConditionalAssetComponent(
        mode="short_circuit", callable_path=f"{FIXTURES_PATH}:returns_false", asset_key="d/gate",
    )
    gate_def = component.build_defs(context=None).assets[0]
    d1 = _downstream_asset("d/step1", "d/gate")

    @dg.asset(key=dg.AssetKey.from_user_string("d/step2"), ins={"u": dg.AssetIn(key=dg.AssetKey.from_user_string("d/step1"))})
    def step2(u):
        return "ran2"

    result = dg.materialize([gate_def, d1, step2], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    statuses = step_statuses(result)
    assert statuses["d__step1"] == "STEP_SKIPPED"
    assert statuses["d__step2"] == "STEP_SKIPPED"


def test_short_circuit_inputs_binds_taskflow_style(mod, fx):
    upstream = _upstream_asset("up/a", "hello")
    component = mod.ConditionalAssetComponent(
        mode="short_circuit", callable_path=f"{FIXTURES_PATH}:wants_upstream", asset_key="d/gate",
        inputs={"upstream_a": "up/a"},
    )
    gate_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([upstream, gate_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert fx.CALLS["upstream_a"] == "hello"


def test_callable_declaring_nothing_receives_nothing(mod, fx):
    component = mod.ConditionalAssetComponent(
        mode="short_circuit", callable_path=f"{FIXTURES_PATH}:wants_nothing", asset_key="d/gate",
    )
    gate_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([gate_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert fx.CALLS["called"] is True


# --- branch -----------------------------------------------------------------------------

def test_branch_chooses_a_only_a_downstream_runs(mod):
    component = mod.ConditionalAssetComponent(
        mode="branch", callable_path=f"{FIXTURES_PATH}:choose_a",
        branches={"a": "d/branch_a", "b": "d/branch_b"},
    )
    branch_def = component.build_defs(context=None).assets[0]
    after_a = _downstream_asset("d/after_a", "d/branch_a")
    after_b = _downstream_asset("d/after_b", "d/branch_b")
    result = dg.materialize([branch_def, after_a, after_b], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    statuses = step_statuses(result)
    assert statuses["d__after_a"] == "STEP_SUCCESS"
    assert statuses["d__after_b"] == "STEP_SKIPPED"


def test_branch_chooses_b_only_b_downstream_runs(mod):
    component = mod.ConditionalAssetComponent(
        mode="branch", callable_path=f"{FIXTURES_PATH}:choose_b",
        branches={"a": "d/branch_a", "b": "d/branch_b"},
    )
    branch_def = component.build_defs(context=None).assets[0]
    after_a = _downstream_asset("d/after_a", "d/branch_a")
    after_b = _downstream_asset("d/after_b", "d/branch_b")
    result = dg.materialize([branch_def, after_a, after_b], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    statuses = step_statuses(result)
    assert statuses["d__after_a"] == "STEP_SKIPPED"
    assert statuses["d__after_b"] == "STEP_SUCCESS"


def test_branch_with_explicit_payload_tuple(mod):
    component = mod.ConditionalAssetComponent(
        mode="branch", callable_path=f"{FIXTURES_PATH}:choose_with_payload",
        branches={"a": "d/branch_a", "b": "d/branch_b"},
    )
    branch_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([branch_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    mats = result.asset_materializations_for_node("_conditional")
    assert len(mats) == 1  # branch_a materialized (branch_b skipped, no Output for it)


def test_branch_returning_an_undeclared_name_fails_clearly(mod):
    component = mod.ConditionalAssetComponent(
        mode="branch", callable_path=f"{FIXTURES_PATH}:choose_undeclared_branch",
        branches={"a": "d/branch_a", "b": "d/branch_b"},
    )
    branch_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([branch_def], resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=False)
    assert not result.success


# --- validation ------------------------------------------------------------------------

def test_short_circuit_requires_asset_key(mod):
    with pytest.raises(Exception, match="asset_key"):
        mod.ConditionalAssetComponent(mode="short_circuit", callable_path=f"{FIXTURES_PATH}:returns_true")


def test_branch_requires_branches(mod):
    with pytest.raises(Exception, match="branches"):
        mod.ConditionalAssetComponent(mode="branch", callable_path=f"{FIXTURES_PATH}:choose_a")


def test_short_circuit_rejects_branches_field(mod):
    with pytest.raises(Exception, match="branches"):
        mod.ConditionalAssetComponent(
            mode="short_circuit", callable_path=f"{FIXTURES_PATH}:returns_true",
            asset_key="d/gate", branches={"a": "d/a"},
        )
