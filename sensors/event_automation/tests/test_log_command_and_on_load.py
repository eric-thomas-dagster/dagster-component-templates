"""Tests for EventAutomationComponent's `log`/`run_command` actions and `on_load` trigger (migration
wishlist item 8: "an action that runs a command or just logs" + "a 'run once at load' trigger,
needed to apply queue limits with set_concurrency_limit"). Same conventions as
test_event_automation.py: _execute_action tested directly with a MagicMock logger, cursor-based
sensors tested via make_context()/unwrap(). run_command runs REAL shell commands (true/false/sleep),
never mocked -- same discipline the rest of this repo applies to subprocess-based features.
"""
from unittest.mock import MagicMock

import pytest

from .conftest import load_component_module
from .test_event_automation import make_context, unwrap

comp_mod = load_component_module()


# ── LogAction ──────────────────────────────────────────────────────────────
def test_log_action_logs_at_the_requested_level():
    action = comp_mod.LogAction(message="hello {event_type}", level="warning")
    logger = MagicMock()
    result = comp_mod._execute_action(action, {"event_type": "test"}, logger)
    assert result is None
    logger.warning.assert_called_once_with("hello test")
    logger.info.assert_not_called()


def test_log_action_default_level_is_info():
    action = comp_mod.LogAction(message="x")
    logger = MagicMock()
    comp_mod._execute_action(action, {}, logger)
    logger.info.assert_called_once_with("x")


def test_log_action_rejects_unknown_level():
    with pytest.raises(ValueError, match="level must be one of"):
        comp_mod.LogAction(message="x", level="critical")


def test_log_action_default_message_uses_the_message_token():
    action = comp_mod.LogAction()
    logger = MagicMock()
    comp_mod._execute_action(action, {"message": "something happened"}, logger)
    logger.info.assert_called_once_with("something happened")


# ── CommandAction ────────────────────────────────────────────────────────
def test_command_action_runs_a_real_command_and_logs_stdout():
    action = comp_mod.CommandAction(command="echo hello-{event_type}")
    logger = MagicMock()
    result = comp_mod._execute_action(action, {"event_type": "world"}, logger)
    assert result is None
    logged = " ".join(str(c) for c in logger.info.call_args_list)
    assert "hello-world" in logged


def test_command_action_nonzero_exit_logs_a_warning_by_default():
    action = comp_mod.CommandAction(command="exit 7")
    logger = MagicMock()
    comp_mod._execute_action(action, {}, logger)
    logger.warning.assert_called_once()
    assert "exited 7" in logger.warning.call_args[0][0]


def test_command_action_fail_on_nonzero_exit_raises():
    action = comp_mod.CommandAction(command="exit 3", fail_on_nonzero_exit=True)
    with pytest.raises(RuntimeError, match="exited 3"):
        comp_mod._execute_action(action, {}, MagicMock())


def test_command_action_timeout_logs_a_warning_by_default():
    action = comp_mod.CommandAction(command="sleep 5", timeout_seconds=1)
    logger = MagicMock()
    comp_mod._execute_action(action, {}, logger)
    logger.warning.assert_called_once()
    assert "timed out" in logger.warning.call_args[0][0]


def test_command_action_timeout_with_fail_on_nonzero_exit_raises():
    action = comp_mod.CommandAction(command="sleep 5", timeout_seconds=1, fail_on_nonzero_exit=True)
    with pytest.raises(RuntimeError, match="timed out"):
        comp_mod._execute_action(action, {}, MagicMock())


def test_command_action_env_is_merged_over_the_process_environment(monkeypatch):
    monkeypatch.setenv("EA_TEST_BASE_VAR", "base")
    action = comp_mod.CommandAction(command="echo $EA_TEST_BASE_VAR-$EA_TEST_EXTRA_VAR", env={"EA_TEST_EXTRA_VAR": "extra"})
    logger = MagicMock()
    comp_mod._execute_action(action, {}, logger)
    logged = " ".join(str(c) for c in logger.info.call_args_list)
    assert "base-extra" in logged


# ── OnLoadTrigger ──────────────────────────────────────────────────────────
def test_on_load_fires_exactly_once_then_skips_forever():
    trigger = comp_mod.OnLoadTrigger()
    actions = [comp_mod.EmitEventAction(asset_key="x")]
    sensor = comp_mod._build_on_load_sensor(
        "test_on_load", trigger, actions, comp_mod.dg.DefaultSensorStatus.STOPPED
    )
    ctx = make_context()
    first = unwrap(sensor._evaluation_fn(ctx))
    assert ctx.cursor == "fired"
    assert not hasattr(first, "skip_message")  # first tick: fires (SensorResult or None, not a SkipReason)

    second = unwrap(sensor._evaluation_fn(ctx))  # same cursor state carried over
    assert hasattr(second, "skip_message")
    assert "already fired" in second.skip_message


def test_on_load_builds_a_sensor_not_a_schedule():
    trigger = comp_mod.OnLoadTrigger()
    sensor = comp_mod._build_on_load_sensor(
        "test_on_load2", trigger, [], comp_mod.dg.DefaultSensorStatus.STOPPED
    )
    assert isinstance(sensor, comp_mod.dg.SensorDefinition)
    assert sensor.name == "test_on_load2"


def test_on_load_runs_set_concurrency_limit_once_via_build_defs():
    """The exact motivating use case: apply a concurrency limit once at load, not on every tick."""
    component = comp_mod.EventAutomationComponent(
        name="apply_limits",
        when=[{"type": "on_load"}],
        then=[{"type": "set_concurrency_limit", "concurrency_key": "snowflake", "limit": 10}],
    )
    defs = component.build_defs(None)
    sensors = list(defs.sensors)
    assert len(sensors) == 1
    assert sensors[0].name == "apply_limits__on_load_0"
