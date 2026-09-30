"""Module-level warmup_fn/tick_fn targets for WarmScheduledJobComponent tests
-- referenced via dotted 'module.path:function_name' strings, so they must be
real, importable module-level functions (not closures/lambdas)."""
from typing import Any, Dict

CALLS: Dict[str, Any] = {"warmup_count": 0, "tick_count": 0, "tick_states": []}


def reset():
    CALLS["warmup_count"] = 0
    CALLS["tick_count"] = 0
    CALLS["tick_states"] = []


def warmup(context):
    CALLS["warmup_count"] += 1
    return {"warmed_at_call": CALLS["warmup_count"]}


def run_tick(context, warm_state, scheduled_time):
    CALLS["tick_count"] += 1
    CALLS["tick_states"].append(warm_state)
    return {"tick_seen_warm_state": warm_state is not None}


def run_tick_raises(context, warm_state, scheduled_time):
    CALLS["tick_count"] += 1
    raise RuntimeError("intentional test failure")


def run_tick_raises_once_then_ok(context, warm_state, scheduled_time):
    CALLS["tick_count"] += 1
    if CALLS["tick_count"] == 1:
        raise RuntimeError("intentional first-tick failure")
    return {}
