"""Real, committed test for a discoverability gap a real user hit: declared
rules, called `.build_defs()` (the normal Dagster component-loading path),
got nothing applied, and nothing anywhere explained why -- because this
component is a no-op by itself unless `defs_with_automation_rules(__file__)`
(or `.apply()`) is separately wired into `definitions.py`. Fixed with a
loud, unconditional warning (not an error -- build_defs() has no way to know
whether the wiring exists elsewhere in the project) whenever rules are
configured.
"""
import importlib.util
import logging
import pathlib

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("aca_warns_unwired", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

AutomationConditionApplicatorComponent = _mod.AutomationConditionApplicatorComponent


def test_warns_when_rules_are_configured(caplog):
    comp = AutomationConditionApplicatorComponent(rules=[{"selection": "*", "preset": "eager"}])
    with caplog.at_level(logging.WARNING):
        comp.build_defs(None)
    assert any("does nothing by itself" in r.message for r in caplog.records)
    assert any("defs_with_automation_rules" in r.message for r in caplog.records)


def test_no_warning_when_rules_is_empty(caplog):
    comp = AutomationConditionApplicatorComponent(rules=[])
    with caplog.at_level(logging.WARNING):
        comp.build_defs(None)
    assert caplog.records == []


def test_build_defs_is_still_a_no_op_definitions():
    """The warning is observability only -- behavior is unchanged."""
    comp = AutomationConditionApplicatorComponent(rules=[{"selection": "*", "preset": "eager"}])
    defs = comp.build_defs(None)
    assert list(defs.assets) == []
