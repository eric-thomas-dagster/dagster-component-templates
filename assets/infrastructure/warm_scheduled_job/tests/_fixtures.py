"""Module-level warmup_fn/tick_fn targets for WarmScheduledJobComponent tests
-- referenced via dotted 'module.path:function_name' strings, so they must be
real, importable module-level functions (not closures/lambdas)."""
from typing import Any, Dict

import dagster as dg

from assets.infrastructure.warm_scheduled_job.component import warmjob

CALLS: Dict[str, Any] = {
    "warmup_count": 0, "tick_count": 0, "tick_states": [],
    "job_a_ticks": 0, "job_b_ticks": 0,
    "warmjob_op_calls": 0, "warmjob_seen_resource": None,
}


def reset():
    CALLS["warmup_count"] = 0
    CALLS["tick_count"] = 0
    CALLS["tick_states"] = []
    CALLS["job_a_ticks"] = 0
    CALLS["job_b_ticks"] = 0
    CALLS["warmjob_op_calls"] = 0
    CALLS["warmjob_seen_resource"] = None


def warmup(context):
    CALLS["warmup_count"] += 1
    return {"warmed_at_call": CALLS["warmup_count"]}


def warmup_with_demo_resource(context):
    CALLS["warmup_count"] += 1
    return {"demo_resource": "resource_value_from_warmup"}


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


def run_tick_job_a(context, warm_state, scheduled_time):
    CALLS["job_a_ticks"] += 1
    return {"job": "a", "warm_state": warm_state}


def run_tick_job_b(context, warm_state, scheduled_time):
    CALLS["job_b_ticks"] += 1
    return {"job": "b", "warm_state": warm_state}


# --- @warmjob fixtures: a completely normal Dagster job (real op, real
# resource requirement) wrapped with @warmjob, so it's directly usable as a
# tick_fn. Confirms warm_state bridges into the real job's normal resource
# system, and that the dispatched execution is a real, separate run. ------

@dg.op(required_resource_keys={"demo_resource"})
def _warmjob_increment_op(context):
    CALLS["warmjob_op_calls"] += 1
    CALLS["warmjob_seen_resource"] = context.resources.demo_resource


@warmjob
@dg.job
def warmjob_test_job():
    _warmjob_increment_op()


@dg.op
def _warmjob_failing_op(context):
    CALLS["warmjob_op_calls"] += 1
    raise RuntimeError("intentional warmjob dispatch failure")


@warmjob
@dg.job
def warmjob_failing_test_job():
    _warmjob_failing_op()
