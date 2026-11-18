"""PythonCallableAssetComponent.

Run an arbitrary Python callable as a Dagster asset -- Airflow PythonOperator /
`@task` equivalent, with an optional Airflow-compatible execution context so a
converted callable body can run unmodified.
"""

import importlib
import inspect
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field, model_validator


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


# --- Airflow-compat shims: inlined, not a separate importable package (see component README) -----------------

class _AirflowVariable:
    """Mirrors Airflow's `Variable.get` -- reads `AIRFLOW_VAR_<KEY upper>`, Airflow's own real,
    documented env-var-backing convention for Variables (not invented here)."""

    @staticmethod
    def get(key: str, default_var: Any = None) -> Any:
        import os
        val = os.environ.get(f"AIRFLOW_VAR_{str(key).upper()}")
        return val if val is not None else default_var


class _AirflowS3Hook:
    """A small, explicitly partial shim over `airflow.providers.amazon.aws.hooks.s3.S3Hook`,
    covering only `get_key` / `check_for_key` / `load_string` / `read_key` against boto3 directly.
    Not a verified full parity port -- extend as real converted callables need more methods."""

    def __init__(self, client: Optional[Any] = None, **boto_kwargs):
        if client is not None:
            self._client = client
        else:
            import boto3
            self._client = boto3.client("s3", **boto_kwargs)

    def check_for_key(self, key: str, bucket_name: str) -> bool:
        from botocore.exceptions import ClientError
        try:
            self._client.head_object(Bucket=bucket_name, Key=key)
            return True
        except ClientError:
            return False

    def get_key(self, key: str, bucket_name: str) -> Any:
        return self._client.get_object(Bucket=bucket_name, Key=key)

    def read_key(self, key: str, bucket_name: str) -> str:
        obj = self.get_key(key, bucket_name)
        return obj["Body"].read().decode("utf-8")

    def load_string(self, string_data: str, key: str, bucket_name: str, replace: bool = False) -> None:
        if not replace and self.check_for_key(key, bucket_name):
            raise ValueError(f"Key {key!r} already exists in bucket {bucket_name!r} and replace=False.")
        self._client.put_object(Bucket=bucket_name, Key=key, Body=string_data.encode("utf-8"))


class _AirflowTaskInstance:
    """Mirrors Airflow's `ti` -- `xcom_pull(task_ids=...)` resolves from this run's `inputs` values
    (the only upstreams this component loads a value for); `try_number` mirrors Dagster's 0-indexed
    `context.retry_number` as Airflow's 1-indexed convention."""

    def __init__(self, upstream_values: Dict[str, Any], retry_number: int):
        self._upstream_values = upstream_values
        self.try_number = retry_number + 1

    def xcom_pull(self, task_ids: Optional[str] = None, key: str = "return_value") -> Any:
        if task_ids is None:
            return None
        if task_ids not in self._upstream_values:
            raise ValueError(
                f"ti.xcom_pull(task_ids={task_ids!r}): {task_ids!r} is not declared in `inputs` "
                f"(declared: {sorted(self._upstream_values)}); add `inputs: {{{task_ids}: <asset_key>}}` "
                "to load its value, or use `deps` only if you don't need the value."
            )
        return self._upstream_values[task_ids]


class _AirflowDagRun:
    def __init__(self, conf: Dict[str, Any]):
        self.conf = conf


# Jinja: exactly these root identifiers are supported; anything else raises at load time, not at run time.
_JINJA_WHITELIST = {"ds", "ds_nodash", "logical_date", "params", "var"}
_JINJA_IDENTIFIER_RE = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)")


def _validate_jinja_identifiers(value: Any, *, field_name: str) -> None:
    """Recursively scans string leaves of `value` for `{{ <identifier>` and raises ValueError at
    component-load time if `<identifier>` is not in `_JINJA_WHITELIST` -- matches Airflow's own
    fixed macro set rather than exposing arbitrary Jinja globals."""
    if isinstance(value, str):
        for name in _JINJA_IDENTIFIER_RE.findall(value):
            if name not in _JINJA_WHITELIST:
                raise ValueError(
                    f"{field_name}: unsupported Jinja variable {{{{ {name} }}}} -- only "
                    f"{sorted(_JINJA_WHITELIST)} are supported in context_mode='airflow'."
                )
    elif isinstance(value, dict):
        for v in value.values():
            _validate_jinja_identifiers(v, field_name=field_name)
    elif isinstance(value, (list, tuple)):
        for v in value:
            _validate_jinja_identifiers(v, field_name=field_name)


def _jinja_env():
    import jinja2

    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    env.filters["ds"] = lambda dt: dt.strftime("%Y-%m-%d")
    env.filters["ds_nodash"] = lambda dt: dt.strftime("%Y%m%d")
    return env


class _VarValueProxy:
    """`var.value.FOO` -> `_AirflowVariable.get("FOO")`, matching `{{ var.value.FOO }}` usage."""

    def __getattr__(self, key: str) -> Any:
        return _AirflowVariable.get(key) or ""


class _Var:
    def __init__(self):
        self.value = _VarValueProxy()


def _jinja_render_deep(value: Any, jinja_ctx: Dict[str, Any]) -> Any:
    if isinstance(value, str):
        if "{{" not in value:
            return value
        env = _jinja_env()
        try:
            return env.from_string(value).render(**jinja_ctx)
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"Jinja render failed for {value!r}: {e}") from e
    if isinstance(value, dict):
        return {k: _jinja_render_deep(v, jinja_ctx) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jinja_render_deep(v, jinja_ctx) for v in value]
    return value


def _resolve_callable(callable_path: str):
    if ":" not in callable_path:
        raise ValueError(f"callable_path must be 'module.path:function_name', got {callable_path!r}")
    mod_path, fn_name = callable_path.split(":", 1)
    mod = importlib.import_module(mod_path)
    fn = getattr(mod, fn_name)
    return fn


class PythonCallableAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run an arbitrary Python callable as a Dagster asset -- Airflow PythonOperator / `@task` equivalent.

    `context_mode: airflow` injects an Airflow-compatible execution context (`ds`, `ds_nodash`, `ts`,
    `logical_date`, `data_interval_start`/`data_interval_end`, `run_id`, `params`, `dag_run.conf`,
    `ti.xcom_pull`/`ti.try_number`, `Variable`, `S3Hook`) into the callable's call -- only the names the
    callable's own signature declares (or everything, if it declares `**kwargs`), and supports the same
    set of variables as Jinja templates (`{{ ds }}`, `{{ logical_date | ds }}`, `{{ params.x }}`,
    `{{ var.value.FOO }}`) in `args`/`kwargs` string fields.
    """

    asset_key: str = Field(description="Dagster asset key, slash-delimited (e.g. 'mydag/mytask').")
    callable_path: str = Field(description="'module.path:function_name' (e.g. 'mypkg.tasks:extract').")
    args: Optional[List[Any]] = Field(default=None, description="Static positional arguments passed to the callable.")
    kwargs: Optional[Dict[str, Any]] = Field(default=None, description="Static keyword arguments passed to the callable.")
    inputs: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "{name: upstream_asset_key}. Each entry's value is loaded via the IO manager. `name` doubles as the "
            "Airflow task_id for `ti.xcom_pull(task_ids=name)`, and -- if the callable's signature declares a "
            "parameter called `name` -- is bound directly to it (TaskFlow-style). Keys must be valid Python "
            "identifiers."
        ),
    )
    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys -- no value loaded (use `inputs` to load a value).",
    )
    context_mode: Literal["none", "airflow"] = Field(
        default="none",
        description="'airflow' injects the Airflow-compatible context described above; 'none' calls the callable with just `args`/`kwargs`.",
    )
    params: Optional[Dict[str, Any]] = Field(default=None, description="Static dict exposed as `params` (mirrors Airflow's DAG-level params).")
    dag_run_conf_tag: Optional[str] = Field(
        default="airflow/dag_run_conf",
        description=(
            "Run tag name carrying a JSON-encoded `dag_run.conf` dict -- set `dg launch --tags "
            "'airflow/dag_run_conf={\"key\": \"value\"}'` (or your own tag name). Absent tag -> `{}`. "
            "Set to None/null to disable."
        ),
    )
    airflow_version: Literal[2, 3] = Field(
        default=2,
        description=(
            "Controls only `logical_date`: 2 (default) = partition interval start (Airflow 2's "
            "execution_date semantics -- the overwhelmingly common case for already-written DAGs). "
            "3 = partition interval end (approximates Airflow 3's CronTriggerTimetable aligning "
            "logical_date with run_after; not a full timetable port). `data_interval_start`/"
            "`data_interval_end` are always the true interval regardless of this setting."
        ),
    )

    # --- Common attributes (same shape as ssh_asset / dataframe_join) ---------------------------------
    group_name: str = Field(default="infrastructure", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Human-readable description surfaced in the Dagster UI.")
    timeout_seconds: Optional[int] = Field(default=None, description="Unused by this component today (no built-in call timeout) -- kept for schema consistency with sibling infrastructure components.")

    partition_type: Optional[str] = Field(default=None, description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'cron', 'static', 'dynamic', 'multi', or None for unpartitioned.")
    partition_cron_schedule: Optional[str] = Field(default=None, description="Required when partition_type='cron'.")
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d", description="strftime format for partition keys produced by partition_type='cron'.")
    partition_end_offset: int = Field(default=0, description="0 (default): a time-based partition set only ever holds COMPLETED windows. 1: include the current, still-in-progress window.")
    partition_timezone: Optional[str] = Field(default=None, description="IANA timezone for the partitions definition (defaults to UTC).")
    partition_start: Optional[str] = Field(default=None, description="Partition start date in ISO format, e.g. '2024-01-01'. Required when partition_type is time-based.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition (partition_type='dynamic').")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None, description="Multi-axis partition spec. Overrides flat fields when set.")
    partition_values: Optional[Union[str, List[str]]] = Field(default=None, description="Comma-separated values for static or multi partitioning.")
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(default=None, description="Backfill execution strategy for a partitioned asset.")
    backfill_policy_max_partitions_per_run: Optional[int] = Field(default=None, description="For backfill_policy='multi_run' only.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")

    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog. Auto-inferred from component name when unset.")
    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="Maximum acceptable lag in minutes before the asset is considered stale.")
    freshness_cron: Optional[str] = Field(default=None, description="Cron schedule string for the freshness policy.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("deps", "owners", "kinds"), csv=("partition_values",), tags=("asset_tags",))

    @model_validator(mode="after")
    def _validate_inputs_and_jinja(self):
        for name in (self.inputs or {}):
            if not name.isidentifier():
                raise ValueError(f"PythonCallableAssetComponent: inputs key {name!r} must be a valid Python identifier.")
        if self.context_mode == "airflow":
            _validate_jinja_identifiers(self.args, field_name="args")
            _validate_jinja_identifiers(self.kwargs, field_name="kwargs")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Python Callable Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self

        asset_deps = [dg.AssetKey(d.split("/")) for d in (component.deps or [])]
        ins = {name: dg.AssetIn(key=dg.AssetKey.from_user_string(key)) for name, key in (component.inputs or {}).items()}

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule, cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
            end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        _freshness_policy_obj = (
            _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
            if self.freshness_max_lag_minutes is not None else None
        )

        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            _backfill_policy_obj = dg.BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            _backfill_policy_obj = dg.BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        @dg.asset(
            key=dg.AssetKey(component.asset_key.split("/")),
            ins=ins or None,
            deps=asset_deps,
            group_name=component.group_name,
            description=component.description,
            partitions_def=partitions_def,
            backfill_policy=_backfill_policy_obj,
            retry_policy=_retry_policy,
            freshness_policy=_freshness_policy_obj,
            owners=component.owners or [],
            tags=component.asset_tags or None,
            kinds=set(component.kinds) if component.kinds else None,
        )
        def _the_asset(context: dg.AssetExecutionContext, **upstream_kwargs) -> Any:
            fn = _resolve_callable(component.callable_path)
            sig_params = inspect.signature(fn).parameters
            has_var_kw = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in sig_params.values())

            call_kwargs: Dict[str, Any] = dict(component.kwargs or {})
            call_args: List[Any] = list(component.args or [])

            if component.context_mode == "airflow":
                logical_date = None
                data_interval_start = None
                data_interval_end = None
                if context.has_partition_key or context.has_partition_key_range:
                    window = context.partition_time_window
                    data_interval_start, data_interval_end = window.start, window.end
                    logical_date = data_interval_start if component.airflow_version == 2 else data_interval_end
                else:
                    logical_date = datetime.now(timezone.utc)
                    data_interval_start = data_interval_end = logical_date

                dag_run_conf: Dict[str, Any] = {}
                if component.dag_run_conf_tag:
                    raw = (context.run.tags or {}).get(component.dag_run_conf_tag)
                    if raw:
                        try:
                            dag_run_conf = json.loads(raw)
                        except (TypeError, ValueError):
                            context.log.warning(
                                f"dag_run_conf_tag {component.dag_run_conf_tag!r} is not valid JSON, ignoring: {raw!r}"
                            )

                airflow_ctx: Dict[str, Any] = {
                    "ds": logical_date.strftime("%Y-%m-%d"),
                    "ds_nodash": logical_date.strftime("%Y%m%d"),
                    "ts": logical_date.isoformat(),
                    "logical_date": logical_date,
                    "data_interval_start": data_interval_start,
                    "data_interval_end": data_interval_end,
                    "run_id": context.run.run_id,
                    "params": component.params or {},
                    "dag_run": _AirflowDagRun(dag_run_conf),
                    "ti": _AirflowTaskInstance(upstream_kwargs, context.retry_number),
                    "Variable": _AirflowVariable,
                    "S3Hook": _AirflowS3Hook,
                }
                if has_var_kw:
                    call_kwargs.update(airflow_ctx)
                else:
                    for name, val in airflow_ctx.items():
                        if name in sig_params:
                            call_kwargs[name] = val

                jinja_ctx = {
                    "ds": airflow_ctx["ds"], "ds_nodash": airflow_ctx["ds_nodash"],
                    "logical_date": logical_date, "params": component.params or {}, "var": _Var(),
                }
                call_args = _jinja_render_deep(call_args, jinja_ctx)
                call_kwargs = _jinja_render_deep(call_kwargs, jinja_ctx)

            for name, val in upstream_kwargs.items():  # TaskFlow-style direct binding from `inputs`
                if has_var_kw or name in sig_params:
                    call_kwargs.setdefault(name, val)

            context.log.info(f"calling {component.callable_path}(args={call_args}, kwargs={list(call_kwargs)})")
            result = fn(*call_args, **call_kwargs)
            context.log.info(f"{component.callable_path} returned: {result!r}"[:500])
            return result

        return dg.Definitions(assets=[_the_asset])
