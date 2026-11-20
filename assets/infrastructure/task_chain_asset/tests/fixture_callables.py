"""Module-level target functions for TaskChainAssetComponent's python-step tests -- referenced
via dotted 'module.path:function_name' strings, so they must be real, importable module-level
functions (same convention as python_callable_asset's own fixture_callables.py)."""
from typing import Any, Dict

CALLS: Dict[str, Any] = {}


def reset():
    CALLS.clear()


def record_args_kwargs(*args, **kwargs):
    CALLS["args"] = args
    CALLS["kwargs"] = kwargs
    return {"args": args, "kwargs": kwargs}


def always_raises(*args, **kwargs):
    raise ValueError("boom")
