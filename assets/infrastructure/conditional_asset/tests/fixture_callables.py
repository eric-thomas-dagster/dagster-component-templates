"""Module-level target functions for ConditionalAssetComponent tests -- referenced via dotted
'module.path:function_name' strings, so they must be real, importable module-level functions.
"""
from typing import Any, Dict

CALLS: Dict[str, Any] = {}


def reset():
    CALLS.clear()


def returns_true():
    return True


def returns_false():
    return False


def wants_upstream(upstream_a):
    CALLS["upstream_a"] = upstream_a
    return bool(upstream_a)


def choose_a():
    return "a"


def choose_b():
    return "b"


def choose_with_payload():
    return ("a", {"detail": "payload-value"})


def choose_undeclared_branch():
    return "not_a_real_branch"


def wants_nothing():
    CALLS["called"] = True
    return True


def slow_true():
    import time
    time.sleep(1)
    return True
