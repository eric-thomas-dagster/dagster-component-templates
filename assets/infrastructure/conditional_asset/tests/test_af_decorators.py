"""Real, committed tests for af_gate/af_guard/af_finish -- the value-level marker
decorators that make conditional_asset's structural skip propagation rejoin-capable.
No mocking: these are pure Python decorators, and the full-graph tests use real
`dg.job(...).execute_in_process()`.
"""
import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


# --- af_gate: unit-level ------------------------------------------------------------

def test_af_gate_success_wraps_the_return_value(mod):
    @mod.af_gate()
    def step():
        return 42
    outcome = step()
    assert outcome.state == "success"
    assert outcome.value == 42


def test_af_gate_afskip_becomes_skipped_outcome(mod):
    @mod.af_gate()
    def step():
        raise mod.AfSkip("nothing to do")
    outcome = step()
    assert outcome.state == "skipped"
    assert outcome.error == "nothing to do"


def test_af_gate_other_exception_becomes_failed_outcome(mod):
    @mod.af_gate()
    def step():
        raise ValueError("boom")
    outcome = step()
    assert outcome.state == "failed"
    assert "boom" in outcome.error


def test_af_gate_never_calls_fn_when_upstream_failed(mod):
    called = {"yes": False}

    @mod.af_gate()
    def step(x):
        called["yes"] = True
        return x

    upstream = mod.StepOutcome("failed", error="upstream boom")
    outcome = step(upstream)
    assert outcome.state == "upstream_failed"
    assert called["yes"] is False


def test_af_gate_never_calls_fn_when_upstream_skipped(mod):
    @mod.af_gate()
    def step(x):
        return x

    outcome = step(mod.StepOutcome("skipped"))
    assert outcome.state == "upstream_skipped"


def test_af_gate_unwraps_a_successful_upstream_outcome_to_its_real_value(mod):
    @mod.af_gate()
    def step(x):
        return x * 2

    outcome = step(mod.StepOutcome("success", value=21))
    assert outcome.state == "success"
    assert outcome.value == 42


def test_af_gate_treats_a_bare_undecorated_upstream_value_as_success(mod):
    @mod.af_gate()
    def step(x):
        return x + 1

    outcome = step(5)
    assert outcome.state == "success"
    assert outcome.value == 6


# --- af_guard: rule evaluation --------------------------------------------------------

def test_af_guard_unknown_rule_rejected(mod):
    with pytest.raises(ValueError, match="unknown rule"):
        mod.af_guard(rule="not_a_real_rule")


@pytest.mark.parametrize(
    "rule,states,expected",
    [
        ("all_success", ["success", "success"], True),
        ("all_success", ["success", "failed"], False),
        ("one_success", ["failed", "success"], True),
        ("one_success", ["failed", "skipped"], False),
        ("one_failed", ["success", "failed"], True),
        ("one_failed", ["success", "success"], False),
        ("none_failed", ["success", "skipped"], True),
        ("none_failed", ["success", "failed"], False),
        ("none_failed_min_one_success", ["success", "skipped"], True),
        ("none_failed_min_one_success", ["skipped", "skipped"], False),
        ("none_skipped", ["success", "failed"], True),
        ("none_skipped", ["success", "skipped"], False),
        ("all_failed", ["failed", "upstream_failed"], True),
        ("all_failed", ["failed", "success"], False),
        ("always", ["failed", "skipped"], True),
    ],
)
def test_af_guard_trigger_rule_matrix(mod, rule, states, expected):
    called = {"yes": False}

    @mod.af_guard(rule=rule)
    def step(*ins):
        called["yes"] = True
        return "ran"

    outcomes = [mod.StepOutcome(s) for s in states]
    outcome = step(*outcomes)
    assert called["yes"] is expected
    assert (outcome.state == "success") is expected


def test_af_guard_skipped_outcome_explains_the_rule(mod):
    @mod.af_guard(rule="all_success")
    def step(x):
        return x

    outcome = step(mod.StepOutcome("failed"))
    assert outcome.state == "skipped"
    assert "all_success" in outcome.error


# --- af_finish: fails the run for real -------------------------------------------------

def test_af_finish_passes_through_when_nothing_failed(mod):
    @mod.af_finish()
    def finish(a, b):
        return "done"

    result = finish(mod.StepOutcome("success", value=1), mod.StepOutcome("skipped"))
    assert result == "done"


def test_af_finish_raises_dagster_failure_when_an_upstream_failed(mod):
    @mod.af_finish()
    def finish(a):
        return "done"

    with pytest.raises(dg.Failure, match="upstream failure"):
        finish(mod.StepOutcome("failed", error="boom"))


def test_af_finish_raises_dagster_failure_when_the_step_itself_raises(mod):
    @mod.af_finish()
    def finish():
        raise ValueError("kaboom")

    with pytest.raises(dg.Failure, match="kaboom"):
        finish()


# --- Full op-graph integration: the actual rejoin af_guard/af_finish exist for -----------

def test_rejoin_graph_runs_the_join_step_even_though_one_branch_failed(mod):
    """The case conditional_asset's README documents as NOT possible via structural skip
    propagation: a join depending on two branches, one of which failed, still runs (instead
    of being skipped outright) and makes its own decision -- then af_finish still fails the
    run for real, since a branch genuinely failed."""

    @dg.op
    @mod.af_gate()
    def branch_ok():
        return "us-result"

    @dg.op
    @mod.af_gate()
    def branch_fails():
        raise ValueError("region unavailable")

    @dg.op
    @mod.af_guard(rule="one_success")
    def join(us, eu):
        return f"joined:{us}"

    @dg.op
    @mod.af_finish()
    def finish(joined):
        return joined

    @dg.job
    def pipeline():
        finish(join(branch_ok(), branch_fails()))

    result = pipeline.execute_in_process(raise_on_error=False)
    # The join step itself ran to success (rejoined on one_success) -- not STEP_SKIPPED --
    # even though branch_fails failed; af_finish then fails the overall run for real.
    step_events = {ev.step_key: ev.event_type_value for ev in result.all_events if ev.step_key}
    assert step_events["join"] == "STEP_SUCCESS"
    assert step_events["finish"] == "STEP_FAILURE"
    assert not result.success
