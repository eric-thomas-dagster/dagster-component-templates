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
    mats = result.asset_materializations_for_node(branch_def.node_def.name)
    assert len(mats) == 2  # decision (always) + branch_a (branch_b skipped, no Output for it)


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


# --- pool / retry_policy / timeout_seconds ------------------------------------------------

def test_no_pool_by_default(mod):
    component = mod.ConditionalAssetComponent(
        mode="short_circuit", callable_path=f"{FIXTURES_PATH}:returns_true", asset_key="d/gate",
    )
    gate_def = component.build_defs(context=None).assets[0]
    assert gate_def.op.pool is None


def test_pool_wires_through(mod):
    component = mod.ConditionalAssetComponent(
        mode="short_circuit", callable_path=f"{FIXTURES_PATH}:returns_true", asset_key="d/gate", pool="decision_slots",
    )
    gate_def = component.build_defs(context=None).assets[0]
    assert gate_def.op.pool == "decision_slots"


def test_retry_policy_wires_through(mod):
    component = mod.ConditionalAssetComponent(
        mode="branch", callable_path=f"{FIXTURES_PATH}:choose_a", branches={"a": "d/a", "b": "d/b"},
        retry_policy_max_retries=3, retry_policy_delay_seconds=2, retry_policy_backoff="linear",
    )
    branch_def = component.build_defs(context=None).assets[0]
    rp = branch_def.op.retry_policy
    assert rp is not None and rp.max_retries == 3 and rp.delay == 2


def test_timeout_seconds_fails_a_slow_short_circuit_callable(mod):
    import time
    component = mod.ConditionalAssetComponent(
        mode="short_circuit", callable_path=f"{FIXTURES_PATH}:slow_true", asset_key="d/gate", timeout_seconds=0.1,
    )
    gate_def = component.build_defs(context=None).assets[0]
    start = time.monotonic()
    result = dg.materialize([gate_def], resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=False)
    assert not result.success
    assert time.monotonic() - start < 1


def test_two_branch_instances_in_one_project_do_not_clash(mod):
    """Regression: both instances used to name their op '_conditional' unconditionally,
    so merging two of them crashed at repository construction with
    DagsterInvalidDefinitionError: conflicting node definitions with the same name."""
    c1 = mod.ConditionalAssetComponent(mode="branch", callable_path=f"{FIXTURES_PATH}:choose_a", branches={"a": "p/a", "b": "p/b"})
    c2 = mod.ConditionalAssetComponent(mode="branch", callable_path=f"{FIXTURES_PATH}:choose_b", branches={"a": "q/a", "b": "q/b"})
    defs1 = c1.build_defs(context=None)
    defs2 = c2.build_defs(context=None)
    merged = dg.Definitions.merge(defs1, defs2)
    merged.get_repository_def().get_all_jobs()  # raises DagsterInvalidDefinitionError if it clashes
    assert defs1.assets[0].node_def.name != defs2.assets[0].node_def.name


def test_branch_mode_decision_asset_always_materializes(mod):
    """Regression: branch mode had no asset representing the decision itself -- only the
    chosen branch's own asset materialized, with nothing a consumer could depend on to know
    a decision was made at all."""
    component = mod.ConditionalAssetComponent(mode="branch", callable_path=f"{FIXTURES_PATH}:choose_a", branches={"a": "r/a", "b": "r/b"})
    branch_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([branch_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    materialized = {str(e.asset_key) for e in result.get_asset_materialization_events()}
    assert str(dg.AssetKey(["r", "decision"])) in materialized
    assert str(dg.AssetKey(["r", "a"])) in materialized
    decision_event = next(e for e in result.get_asset_materialization_events() if e.asset_key == dg.AssetKey(["r", "decision"]))
    assert decision_event.materialization.metadata["chosen_branch"].value == "a"


def test_short_circuit_falsy_still_materializes_a_decision_asset(mod):
    """Regression: a falsy short_circuit result materialized nothing at all, making
    'the gate correctly failed' indistinguishable from an unrelated skip/crash."""
    component = mod.ConditionalAssetComponent(mode="short_circuit", callable_path=f"{FIXTURES_PATH}:returns_false", asset_key="s/gate")
    gate_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([gate_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    materialized = {str(e.asset_key) for e in result.get_asset_materialization_events()}
    assert str(dg.AssetKey(["s", "gate_decision"])) in materialized
    assert str(dg.AssetKey(["s", "gate"])) not in materialized
    decision_event = next(e for e in result.get_asset_materialization_events() if e.asset_key == dg.AssetKey(["s", "gate_decision"]))
    assert decision_event.materialization.metadata["passed"].value is False


def test_branch_mode_without_shared_parent_requires_explicit_decision_key(mod):
    with pytest.raises(ValueError, match="decision_asset_key"):
        mod.ConditionalAssetComponent(
            mode="branch", callable_path=f"{FIXTURES_PATH}:choose_a", branches={"a": "t/a", "b": "u/b"},
        ).build_defs(context=None)
