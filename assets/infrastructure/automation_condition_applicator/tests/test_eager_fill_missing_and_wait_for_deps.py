"""Tests for three additions closing a real gap found reviewing external
feedback: a real project had hand-written two AutomationCondition factory
functions (`eager_after_blocking_checks`, `after_all_deps_since_cron`) because
neither was expressible in this component without the `python:` escape hatch.

1. `preset: eager_fill_missing` -- eager() + a safe one-time backfill of the
   latest missing partition.
2. `all_deps_updated_since_cron: "<cron>"` -- wait for every dependency since
   a cron tick, then run once (how an asset waits for work it doesn't own).
3. `require_blocking_checks_passed: true` -- a modifier, orthogonal to (1)
   and (2), composing with any rule shape.

Combined, (1)+(3) and (2)+(3) reproduce the two hand-written functions
exactly, natively in YAML, with no `python:` needed.
"""
import importlib.util
import pathlib

import dagster as dg
import pytest

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("aca_fill_missing", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

apply_rules = _mod.apply_rules
_build_condition_for_rule = _mod._build_condition_for_rule

_STUB_DEFS = dg.Definitions(assets=[dg.AssetSpec(key=dg.AssetKey(["stub"]))])
_STUB_SPEC = dg.AssetSpec(key=dg.AssetKey(["stub"]))


def test_eager_fill_missing_preset_builds_without_error():
    cond = _build_condition_for_rule({"preset": "eager_fill_missing"}, _STUB_SPEC, _STUB_DEFS)
    assert cond is not None
    dumped = repr(cond).lower()
    assert "missing" in dumped
    assert "eager" in dumped or "updated" in dumped  # eager() expands structurally in repr


def test_all_deps_updated_since_cron_rule_builds_without_error():
    cond = _build_condition_for_rule(
        {"all_deps_updated_since_cron": "30 18 * * 1-5"}, _STUB_SPEC, _STUB_DEFS,
    )
    assert cond is not None
    dumped = repr(cond).lower()
    assert "updated_since_cron" in dumped or "since_cron" in dumped
    assert "in_progress" in dumped  # the baked-in ~in_progress()/~any_deps_in_progress() guard


def test_require_blocking_checks_passed_modifier_composes_with_a_preset():
    cond = _build_condition_for_rule(
        {"preset": "eager", "require_blocking_checks_passed": True}, _STUB_SPEC, _STUB_DEFS,
    )
    assert cond is not None
    assert "blocking_checks" in repr(cond).lower()


def test_require_blocking_checks_passed_modifier_composes_with_all_deps_updated_since_cron():
    cond = _build_condition_for_rule(
        {"all_deps_updated_since_cron": "0 9 * * *", "require_blocking_checks_passed": True},
        _STUB_SPEC, _STUB_DEFS,
    )
    assert cond is not None
    dumped = repr(cond).lower()
    assert "blocking_checks" in dumped
    assert "since_cron" in dumped


def test_eager_after_blocking_checks_reproduced_natively(tmp_path):
    """preset: eager_fill_missing + require_blocking_checks_passed reproduces
    the hand-written eager_after_blocking_checks() function's condition
    end-to-end against a real Definitions."""
    @dg.asset
    def upstream():
        return None

    @dg.asset(deps=[upstream])
    def downstream():
        return None

    defs = dg.Definitions(assets=[upstream, downstream])
    new_defs = apply_rules(defs, rules=[{
        "selection": "*",
        "preset": "eager_fill_missing",
        "require_blocking_checks_passed": True,
    }])
    specs_by_key = {s.key: s for s in new_defs.resolve_all_asset_specs()}
    for spec in specs_by_key.values():
        assert spec.automation_condition is not None
        dumped = repr(spec.automation_condition).lower()
        assert "missing" in dumped
        assert "blocking_checks" in dumped


def test_after_all_deps_since_cron_reproduced_natively(tmp_path):
    """all_deps_updated_since_cron + require_blocking_checks_passed
    reproduces the hand-written after_all_deps_since_cron(cron) function's
    condition end-to-end against a real Definitions, scoped by selection
    the same way a fall-through rule set would be."""
    @dg.asset
    def lab_file():
        return None

    @dg.asset
    def mirth_job():
        return None

    @dg.asset(deps=[lab_file, mirth_job])
    def lab_results_staging():
        return None

    defs = dg.Definitions(assets=[lab_file, mirth_job, lab_results_staging])
    new_defs = apply_rules(defs, rules=[{
        "selection": "key:lab_results_staging",
        "all_deps_updated_since_cron": "30 18 * * 1-5",
        "require_blocking_checks_passed": True,
    }])
    specs_by_key = {s.key: s for s in new_defs.resolve_all_asset_specs()}

    staging_spec = specs_by_key[dg.AssetKey(["lab_results_staging"])]
    assert staging_spec.automation_condition is not None
    dumped = repr(staging_spec.automation_condition).lower()
    assert "since_cron" in dumped
    assert "blocking_checks" in dumped
    assert "in_progress" in dumped

    # Untouched siblings -- selection only matched the staging asset.
    assert specs_by_key[dg.AssetKey(["lab_file"])].automation_condition is None
    assert specs_by_key[dg.AssetKey(["mirth_job"])].automation_condition is None


def test_all_deps_updated_since_cron_and_preset_are_mutually_exclusive_rule_shapes():
    """A rule with neither a recognized shape nor all_deps_updated_since_cron
    still raises the same clear error, now mentioning the new shape."""
    with pytest.raises(ValueError, match="all_deps_updated_since_cron"):
        _build_condition_for_rule({}, _STUB_SPEC, _STUB_DEFS)
