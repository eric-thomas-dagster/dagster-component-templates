"""Module-level target functions for PythonCallableAssetComponent tests -- referenced via dotted
'module.path:function_name' strings, so they must be real, importable module-level functions.

Referenced from the test file via `importlib.import_module("assets.infrastructure.python_callable_asset.tests.fixture_callables")`
(not `from . import fixture_callables`) so the module object -- and the mutable CALLS dict it holds --
is the same one `callable_path` resolves to, mirroring the pattern used in
`assets/infrastructure/warm_scheduled_job/tests/_fixtures.py`.
"""
from typing import Any, Dict

CALLS: Dict[str, Any] = {}


def reset():
    CALLS.clear()


def record_args_kwargs(*args, **kwargs):
    CALLS["args"] = args
    CALLS["kwargs"] = kwargs
    return {"args": args, "kwargs": kwargs}


def taskflow_style(upstream_a):
    CALLS["upstream_a"] = upstream_a
    return upstream_a


def wants_nothing():
    CALLS["called"] = True
    return "no-context"


def wants_ds_only(ds):
    CALLS["ds"] = ds
    return ds


def wants_full_context(ds, ds_nodash, ts, logical_date, data_interval_start, data_interval_end, run_id, params, dag_run, ti, Variable, S3Hook):
    CALLS.update(
        ds=ds, ds_nodash=ds_nodash, ts=ts, logical_date=logical_date,
        data_interval_start=data_interval_start, data_interval_end=data_interval_end,
        run_id=run_id, params=params, dag_run_conf=dag_run.conf, ti=ti,
        Variable=Variable, S3Hook=S3Hook,
    )
    return "full-context"


def wants_var_kwargs_catchall(**context):
    CALLS["context_keys"] = sorted(context)
    CALLS["context"] = context
    return sorted(context)


def pulls_xcom(ti):
    CALLS["pulled"] = ti.xcom_pull(task_ids="upstream_a")
    return CALLS["pulled"]


def pulls_undeclared_xcom(ti):
    return ti.xcom_pull(task_ids="not_declared")


def reports_try_number(ti):
    CALLS["try_number"] = ti.try_number
    return ti.try_number
