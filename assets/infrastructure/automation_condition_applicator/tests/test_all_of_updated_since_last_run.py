"""Tests for preset='all_of_updated_since_last_run' and strategy='all_deps_updated'
(derive_from_upstreams) -- both build the same real gap: "run when every listed upstream
asset has updated since this asset last ran."

VERIFIED LIVE before writing the fix: a single `.since_last_handled()` wrapped around the
whole AND (`(A.newly_updated() & B.newly_updated()).since_last_handled()`) never fires once
upstreams update on different ticks -- it needs `.since_last_handled()` applied to EACH
per-asset term individually, then ANDed together. `all_deps_match(newly_updated()).since_last_handled()`
doesn't give the same result either (also verified live: never fires). The real functional
tests below are the ones that actually catch a regression to either broken form -- the
structural (repr-substring) tests would pass for the broken forms too.
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("aca_all_of_updated", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

apply_rules = _mod.apply_rules
_build_condition_for_rule = _mod._build_condition_for_rule


def _two_upstream_defs():
    @dg.asset
    def upstream_a():
        return 1

    @dg.asset
    def upstream_b():
        return 1

    @dg.asset(deps=[upstream_a, upstream_b])
    def downstream():
        return 1

    return upstream_a, upstream_b, downstream


# ---- preset='all_of_updated_since_last_run' ----

def test_preset_requires_at_least_one_dependency():
    stub_spec = dg.AssetSpec(key=dg.AssetKey(["stub"]))
    stub_defs = dg.Definitions(assets=[stub_spec])
    with pytest.raises(ValueError, match="dependency"):
        _build_condition_for_rule({"preset": "all_of_updated_since_last_run"}, stub_spec, stub_defs)


def test_preset_builds_a_condition_per_dependency():
    upstream_a, upstream_b, downstream = _two_upstream_defs()
    defs = dg.Definitions(assets=[upstream_a, upstream_b, downstream])
    spec = next(s for s in defs.resolve_all_asset_specs() if s.key == dg.AssetKey(["downstream"]))
    cond = _build_condition_for_rule({"preset": "all_of_updated_since_last_run"}, spec, defs)
    dumped = repr(cond).lower()
    assert dumped.count("sincecondition") == 2  # one per upstream, not one wrapping the AND
    assert "in_progress" in dumped


def test_preset_does_not_fire_until_all_upstreams_have_updated():
    """The user's own stated test requirement: must not fire when only one of two
    upstreams has updated."""
    upstream_a, upstream_b, downstream = _two_upstream_defs()
    defs = dg.Definitions(assets=[upstream_a, upstream_b, downstream])
    rules = [{"selection": "downstream", "preset": "all_of_updated_since_last_run"}]
    new_defs = apply_rules(defs, rules=rules)

    with dg.instance_for_test() as instance:
        dg.materialize([upstream_a, upstream_b], instance=instance)
        r0 = dg.evaluate_automation_conditions(defs=new_defs, instance=instance)
        assert r0.get_num_requested(dg.AssetKey("downstream")) == 0

        dg.materialize([upstream_a], instance=instance)
        r1 = dg.evaluate_automation_conditions(defs=new_defs, instance=instance, cursor=r0.cursor)
        assert r1.get_num_requested(dg.AssetKey("downstream")) == 0, (
            "must not fire when only one of two upstreams has updated"
        )

        dg.materialize([upstream_b], instance=instance)
        r2 = dg.evaluate_automation_conditions(defs=new_defs, instance=instance, cursor=r1.cursor)
        assert r2.get_num_requested(dg.AssetKey("downstream")) == 1, (
            "must fire once BOTH upstreams have updated since downstream last ran"
        )


# ---- strategy='all_deps_updated' (derive_from_upstreams) ----

def test_strategy_requires_at_least_one_dependency():
    stub_spec = dg.AssetSpec(key=dg.AssetKey(["stub"]))
    stub_defs = dg.Definitions(assets=[stub_spec])
    with pytest.raises(ValueError, match="dependency"):
        _build_condition_for_rule(
            {"derive_from_upstreams": True, "strategy": "all_deps_updated"}, stub_spec, stub_defs,
        )


def test_strategy_does_not_fire_until_all_upstreams_have_updated():
    upstream_a, upstream_b, downstream = _two_upstream_defs()
    defs = dg.Definitions(assets=[upstream_a, upstream_b, downstream])
    rules = [{"selection": "downstream", "derive_from_upstreams": True, "strategy": "all_deps_updated"}]
    new_defs = apply_rules(defs, rules=rules)

    with dg.instance_for_test() as instance:
        dg.materialize([upstream_a, upstream_b], instance=instance)
        r0 = dg.evaluate_automation_conditions(defs=new_defs, instance=instance)
        assert r0.get_num_requested(dg.AssetKey("downstream")) == 0

        dg.materialize([upstream_a], instance=instance)
        r1 = dg.evaluate_automation_conditions(defs=new_defs, instance=instance, cursor=r0.cursor)
        assert r1.get_num_requested(dg.AssetKey("downstream")) == 0

        dg.materialize([upstream_b], instance=instance)
        r2 = dg.evaluate_automation_conditions(defs=new_defs, instance=instance, cursor=r1.cursor)
        assert r2.get_num_requested(dg.AssetKey("downstream")) == 1
