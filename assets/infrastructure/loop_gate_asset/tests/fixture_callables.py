"""Module-level target functions for LoopGateAssetComponent tests -- referenced via dotted
'module.path:function_name' strings, so they must be real, importable module-level functions.
"""
from typing import Any, Dict

CALLS: Dict[str, Any] = {}


def reset():
    CALLS.clear()


def increment():
    CALLS["n"] = CALLS.get("n", 0) + 1
    return CALLS["n"]


def wants_upstream(upstream_a):
    CALLS["upstream_a"] = upstream_a
    CALLS["n"] = CALLS.get("n", 0) + 1
    return (upstream_a, CALLS["n"])


def stop_at_three(loop):
    return loop.result >= 3


def stop_at_three_with_reason(loop):
    return (loop.result >= 3, f"reached {loop.result}")


def never_stops(loop):
    return False


def returns_none(loop):
    pass


def uses_previous(loop):
    return len(loop.previous) == loop.index and loop.index >= 2


def slow_increment():
    import time
    time.sleep(0.05)
    return increment()
