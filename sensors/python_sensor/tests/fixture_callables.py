"""Module-level target functions for PythonSensorComponent tests -- referenced via dotted
'module.path:function_name' strings, so they must be real, importable module-level functions.

Referenced from the test file via `importlib.import_module("sensors.python_sensor.tests.fixture_callables")`
(not `from . import fixture_callables`) so the module object -- and the mutable STATE dict it
holds -- is the same one `callable_path` resolves to, mirroring the pattern used in
`assets/infrastructure/python_callable_asset/tests/fixture_callables.py`.
"""
from typing import Any, Dict

STATE: Dict[str, Any] = {"calls": 0}


def reset():
    STATE.clear()
    STATE["calls"] = 0


def raises(context):
    raise RuntimeError("boom")


def always_false(context):
    return False


def always_true(context):
    return True


def true_with_payload(context):
    return True, {"amount": 42}


def dict_payload(context):
    return {"amount": 7}


def second_call_true(context):
    STATE["calls"] += 1
    return STATE["calls"] >= 2


def with_static_args(context, a, b, flag=None):
    STATE["last_args"] = (a, b, flag)
    return True


def list_of_item_keys(context):
    return STATE.get("items", [])


def list_of_item_tuples(context):
    return STATE.get("items", [])
