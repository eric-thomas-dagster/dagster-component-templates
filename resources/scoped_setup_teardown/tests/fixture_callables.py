"""Module-level target functions for ScopedSetupTeardownComponent tests -- referenced via dotted
'module.path:function_name' strings, so they must be real, importable module-level functions.
"""
from typing import Any, Dict

CALLS: Dict[str, Any] = {"order": []}


def reset():
    CALLS["order"] = []
    CALLS.pop("teardown_received", None)


def setup_no_return():
    CALLS["order"].append("setup")


def setup_returns_handle():
    CALLS["order"].append("setup")
    return {"handle": "fake-connection"}


def teardown_no_param():
    CALLS["order"].append("teardown")


def teardown_with_param(setup_value):
    CALLS["order"].append("teardown")
    CALLS["teardown_received"] = setup_value


def setup_that_raises():
    CALLS["order"].append("setup_raised")
    raise RuntimeError("setup failed on purpose")
