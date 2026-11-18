"""Tests for the four Airflow-trigger-rule synthetic presets: all_upstream_done,
any_upstream_failed, none_failed_min_one_success, any_of_updated.

Structural tests match this file's existing convention (test_on_deploy_if_code_changed.py,
test_eager_fill_missing_and_wait_for_deps.py): build the condition, inspect its repr.
`test_any_upstream_failed_fires_once_not_every_tick` goes one step further with a real
`dg.evaluate_automation_conditions` functional check -- the riskiest thing to get wrong here
is a condition that stays "true" forever after one failure and re-requests on every single
tick (a materialization storm), since `execution_failed()` is documented as sticky.
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("aca_trigger_rules", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

apply_rules = _mod.apply_rules
_build_condition_for_rule = _mod._build_condition_for_rule

_STUB_DEFS = dg.Definitions(assets=[dg.AssetSpec(key=dg.AssetKey(["stub"]))])
_STUB_SPEC = dg.AssetSpec(key=dg.AssetKey(["stub"]))


@pytest.mark.parametrize("preset,expected_substrings", [
    ("all_upstream_done", ["in_progress"]),
    ("any_upstream_failed", ["executionfailed", "in_progress"]),
    ("none_failed_min_one_success", ["updated", "executionfailed", "in_progress"]),
    ("any_of_updated", ["updated", "in_progress"]),
])
def test_preset_builds_expected_composite(preset, expected_substrings):
    cond = _build_condition_for_rule({"preset": preset}, _STUB_SPEC, _STUB_DEFS)
    assert cond is not None
    dumped = repr(cond).lower()
    for sub in expected_substrings:
        assert sub in dumped, f"preset={preset!r}: expected {sub!r} in {dumped[:400]}"


def test_apply_rules_targets_a_selection_with_any_upstream_failed():
    @dg.asset
    def upstream():
        return None

    @dg.asset(deps=[upstream])
    def downstream():
        return None

    defs = dg.Definitions(assets=[upstream, downstream])
    rules = [{"name": "retry_on_upstream_failure", "selection": "downstream", "preset": "any_upstream_failed"}]
    new_defs = apply_rules(defs, rules=rules)

    specs_by_key = {s.key: s for s in new_defs.resolve_all_asset_specs()}
    assert specs_by_key[dg.AssetKey(["downstream"])].automation_condition is not None
    assert specs_by_key[dg.AssetKey(["upstream"])].automation_condition is None


def test_any_upstream_failed_fires_once_not_every_tick():
    """Real functional check via dg.evaluate_automation_conditions: a downstream asset with
    preset=any_upstream_failed requests a run on the tick right after upstream fails, but NOT
    on a later tick where nothing new has happened (the .newly_true() guard)."""
    @dg.asset
    def upstream():
        raise RuntimeError("intentional failure")

    cond = _build_condition_for_rule({"preset": "any_upstream_failed"}, _STUB_SPEC, _STUB_DEFS)

    @dg.asset(deps=[upstream], automation_condition=cond)
    def downstream():
        return None

    defs = dg.Definitions(assets=[upstream, downstream])
    instance = dg.DagsterInstance.ephemeral()

    # Fail the upstream for real.
    dg.materialize([upstream], instance=instance, raise_on_error=False)

    result_1 = dg.evaluate_automation_conditions(defs=defs, instance=instance)
    assert result_1.get_num_requested(dg.AssetKey(["downstream"])) == 1, (
        "should request a run the tick right after the failure"
    )

    # Simulate that the requested run happened (materialize downstream for real), then
    # evaluate again with NO new upstream failure -- must NOT re-request.
    dg.materialize([downstream], instance=instance, raise_on_error=False)
    result_2 = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, cursor=result_1.cursor,
    )
    assert result_2.get_num_requested(dg.AssetKey(["downstream"])) == 0, (
        "must not re-request on a later tick with no new failure (execution_failed() is "
        "sticky; .newly_true() is the guard against a materialization storm here)"
    )


def test_unknown_preset_still_errors_clearly():
    rule = {"preset": "nonexistent_preset_name"}
    with pytest.raises(ValueError, match="nonexistent_preset_name"):
        _build_condition_for_rule(rule, _STUB_SPEC, _STUB_DEFS)
