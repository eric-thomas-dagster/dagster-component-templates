"""Module-level target functions for WaitUntilAssetComponent's condition='callable' tests --
referenced via dotted 'module.path:function_name' strings, so they must be real, importable
module-level functions."""
STATE = {"calls": 0, "true_after": 1}


def reset(true_after=1):
    STATE["calls"] = 0
    STATE["true_after"] = true_after


def becomes_true_after_n_calls():
    STATE["calls"] += 1
    return STATE["calls"] >= STATE["true_after"]


def always_raises():
    raise RuntimeError("boom")
