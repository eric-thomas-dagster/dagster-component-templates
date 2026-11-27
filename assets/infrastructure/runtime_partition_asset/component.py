"""RuntimePartitionAssetComponent.

An asset whose partition key is chosen WHILE it runs, not before -- Airflow 3.3's
`PartitionedAtRuntime` (`outlet_events[self].add_partitions(key)`), verified against Airflow's
own official `example_asset_partition` example DAG source before building anything (same
source already used for `partition_window_sensor`).

Why this needs a different shape than every other partitioned asset in this repo: Dagster
requires a partitioned step's partition key to be known BEFORE the run starts (the execution
plan is built knowing which partition is targeted; the `dagster/partition` run tag must match
an already-registered key). There is no way for a normal partitioned `@dg.asset` to decide its
own key mid-compute. Confirmed live, not guessed, the real mechanism that works instead: the
EXECUTING op is unpartitioned (so Dagster imposes no pre-known-key requirement on it), and once
it has decided the key, it (1) registers it via `instance.add_dynamic_partitions(...)` if new,
then (2) reports the materialization for a SEPARATE, catalog-facing `AssetSpec` -- partitioned
with that same `DynamicPartitionsDefinition`, with no compute op of its own -- via
`instance.report_runless_asset_event(AssetMaterialization(partition=key))`. Same "observed, not
computed" shape this repo's own `external_assets/` category already uses, just dynamically
partitioned instead of unpartitioned.
"""
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field, model_validator


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=(), strs=()):
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
    for k in strs:  # str (or Dict[str, str]) fields: a numeric-looking YAML scalar resolves to int/float/bool
        v = out.get(k)
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            out[k] = str(v)
        elif isinstance(v, dict):
            out[k] = {str(a): (("true" if b is True else "false") if isinstance(b, bool) else (str(b) if isinstance(b, (int, float)) else b)) for a, b in v.items()}
    return out
# END field-shape normalization


# BEGIN generic timeout wrapper (inlined per component; keep copies identical — see tests/test_generic_timeout_drift.py)
def _run_with_timeout(fn, timeout_seconds, *args, **kwargs):
    """Bounds how long this step waits for `fn`, raising dg.Failure past timeout_seconds.
    Python threads can't be forcibly killed -- fn keeps running in the background past the
    deadline (same documented trade-off as this repo's own @timeout decorator); this bounds
    the STEP's wait, not the underlying call itself. Deliberately NOT `with ThreadPoolExecutor()`
    -- that form's __exit__ calls shutdown(wait=True), which blocks until the background call
    finishes anyway, silently defeating the timeout for a call that hangs indefinitely."""
    if not timeout_seconds:
        return fn(*args, **kwargs)
    import concurrent.futures
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = executor.submit(fn, *args, **kwargs)
    try:
        result = future.result(timeout=timeout_seconds)
    except concurrent.futures.TimeoutError:
        executor.shutdown(wait=False)
        raise dg.Failure(description=f"Exceeded timeout_seconds={timeout_seconds}.") from None
    executor.shutdown(wait=False)
    return result
# END generic timeout wrapper


def _resolve_callable(callable_path: str):
    import importlib
    if ":" not in callable_path:
        raise ValueError(f"callable_path must be 'module.path:function_name', got {callable_path!r}")
    mod_path, fn_name = callable_path.split(":", 1)
    mod = importlib.import_module(mod_path)
    return getattr(mod, fn_name)


def _bind_call(fn, args, kwargs, upstream_kwargs):
    """Inject only the names `fn`'s own signature declares (or everything, for **kwargs) --
    the same "inject only what's declared" rule as python_callable_asset/conditional_asset."""
    import inspect
    sig_params = inspect.signature(fn).parameters
    has_var_kw = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in sig_params.values())
    call_kwargs = dict(kwargs or {})
    for name, val in upstream_kwargs.items():
        if has_var_kw or name in sig_params:
            call_kwargs.setdefault(name, val)
    return fn(*(args or []), **call_kwargs)


class RuntimePartitionAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """An asset whose partition key is chosen while it runs -- Airflow's `PartitionedAtRuntime`.
    The executing step is itself unpartitioned; it registers the chosen key as a dynamic
    partition (if new) and reports the materialization for a separate, catalog-facing,
    dynamically-partitioned asset that has no compute op of its own."""

    asset_key: str = Field(description="The catalog-facing asset key (slash-delimited) that ends up partitioned by `dynamic_partitions_name`. Has no compute op of its own -- materializations are reported for it, not produced by it directly.")
    executor_asset_key: Optional[str] = Field(
        default=None,
        description="Key (slash-delimited) for the real, unpartitioned executing asset that runs the callable. Defaults to '<asset_key>_runner'. Keep distinct from `asset_key` -- Dagster doesn't allow one AssetsDefinition to be both the unpartitioned runner and the dynamically-partitioned target.",
    )
    dynamic_partitions_name: str = Field(description="Name for the DynamicPartitionsDefinition backing `asset_key`. Register once per logical partition set -- share this name across producers/consumers of the same runtime-chosen keys.")
    callable_path: str = Field(description="'module.path:function_name', the real compute. Return shape depends on `key_source` -- see that field's description.")
    key_source: Literal["run_tag", "run_config", "callable_return"] = Field(
        default="callable_return",
        description=(
            "Where the chosen partition key comes from. 'run_tag': read from a run tag "
            "(`key_run_tag`) set by whoever triggers the run -- Airflow's 'triggered "
            "externally with a region code' case; the callable is called as normal and its "
            "return value is the materialized value. 'run_config': read from an op config "
            "field (`key_config_field`) passed at launch time; same calling convention. "
            "'callable_return' (default): the callable itself decides the key -- it must "
            "return a (key, payload) 2-tuple; Airflow's PartitionedAtRuntime case where the "
            "task computes the key as it runs."
        ),
    )
    key_run_tag: Optional[str] = Field(default=None, description="key_source='run_tag' only (required then): the run tag name holding the chosen key.")
    key_config_field: Optional[str] = Field(default=None, description="key_source='run_config' only (required then): the op config field name holding the chosen key.")
    args: Optional[List[Any]] = Field(default=None, description="Static positional arguments passed to the callable.")
    kwargs: Optional[Dict[str, Any]] = Field(default=None, description="Static keyword arguments passed to the callable.")
    inputs: Optional[Dict[str, str]] = Field(
        default=None,
        description="{name: upstream_asset_key}. Each entry's value is loaded via the IO manager and bound to a parameter of that name if the callable's signature declares one (TaskFlow-style) -- same shape as python_callable_asset's `inputs`.",
    )
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys for the executing asset -- no value loaded (use `inputs` to load a value).")
    report_value_as_metadata: bool = Field(
        default=True,
        description="If true, the callable's returned payload is recorded as `value` metadata on the reported materialization (via dg.MetadataValue.text/json as appropriate) -- best-effort; non-JSON-serializable payloads are stringified. Set false to skip this if the payload is large or sensitive.",
    )

    group_name: str = Field(default="infrastructure", description="Dagster asset group name (applied to both the executor and the catalog-facing asset).")
    description: Optional[str] = Field(default=None, description="Asset description (applied to the catalog-facing asset).")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the catalog-facing asset.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on the executing asset's own step if it fails. Defines a RetryPolicy.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")
    pool: Optional[str] = Field(
        default=None,
        description=(
            "Dagster's native named concurrency pool (e.g. Airflow's `pool` -- a fixed "
            "number of slots shared across every asset/op using the same pool name). The "
            "pool's own slot count is configured at the deployment level (dagster.yaml's "
            "concurrency: pools:, or the Dagster+ UI), not here."
        ),
    )
    timeout_seconds: Optional[float] = Field(
        default=None,
        description="Fail the executing asset's step if the callable hasn't returned after this many seconds. Enforced via a background-thread wrapper -- the callable may continue running past the deadline (Python threads can't be forcibly killed); this bounds the step's wait, not the call itself.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=(), tags=('asset_tags',))

    @model_validator(mode="after")
    def _validate_key_source(self):
        if self.key_source == "run_tag" and not self.key_run_tag:
            raise ValueError("RuntimePartitionAssetComponent: key_source='run_tag' requires `key_run_tag`.")
        if self.key_source == "run_config" and not self.key_config_field:
            raise ValueError("RuntimePartitionAssetComponent: key_source='run_config' requires `key_config_field`.")
        if self.asset_key == (self.executor_asset_key or ""):
            raise ValueError("RuntimePartitionAssetComponent: `asset_key` and `executor_asset_key` must be different.")
        for name in (self.inputs or {}):
            if not name.isidentifier():
                raise ValueError(f"RuntimePartitionAssetComponent: inputs key {name!r} must be a valid Python identifier.")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Runtime Partition Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        target_key = dg.AssetKey(component.asset_key.split("/"))
        executor_key = dg.AssetKey((component.executor_asset_key or f"{component.asset_key}_runner").split("/"))
        dpd = dg.DynamicPartitionsDefinition(name=component.dynamic_partitions_name)

        ins = {name: dg.AssetIn(key=dg.AssetKey.from_user_string(key)) for name, key in (component.inputs or {}).items()}
        deps_keys = [dg.AssetKey.from_user_string(k) for k in (component.deps or [])]
        tags = component.asset_tags or None
        kinds = set(component.kinds) if component.kinds else None

        target_spec = dg.AssetSpec(
            key=target_key,
            description=component.description or f"Dynamically partitioned by {component.dynamic_partitions_name!r} -- keys chosen at runtime by {executor_key.to_user_string()}.",
            partitions_def=dpd,
            group_name=component.group_name,
            owners=component.owners or None,
            tags=tags,
            kinds=kinds,
        )

        _retry_policy = None
        if component.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=component.retry_policy_max_retries,
                delay=component.retry_policy_delay_seconds or 1,
                backoff=Backoff[component.retry_policy_backoff.upper()],
            )

        @dg.asset(
            key=executor_key,
            ins=ins or None,
            deps=deps_keys,
            group_name=component.group_name,
            description=f"Runs the runtime-partition callable for {target_key.to_user_string()}.",
            retry_policy=_retry_policy,
            pool=component.pool,
        )
        def _runner(context: dg.AssetExecutionContext, **upstream_kwargs):
            fn = _resolve_callable(component.callable_path)

            if component.key_source == "callable_return":
                result = _run_with_timeout(lambda: _bind_call(fn, component.args, component.kwargs, upstream_kwargs), component.timeout_seconds)
                if not (isinstance(result, tuple) and len(result) == 2):
                    raise dg.Failure(
                        description=f"key_source='callable_return': callable {component.callable_path!r} must return a (key, payload) 2-tuple, got {type(result).__name__}.",
                    )
                chosen_key, payload = result
            else:
                if component.key_source == "run_tag":
                    chosen_key = (context.run.tags or {}).get(component.key_run_tag)
                    source_desc = f"run tag {component.key_run_tag!r}"
                else:
                    op_config = context.op_execution_context.op_config or {}
                    chosen_key = op_config.get(component.key_config_field)
                    source_desc = f"op config field {component.key_config_field!r}"
                if not chosen_key:
                    raise dg.Failure(description=f"RuntimePartitionAssetComponent: {source_desc} is not set on this run; required for key_source={component.key_source!r}.")
                payload = _run_with_timeout(lambda: _bind_call(fn, component.args, component.kwargs, upstream_kwargs), component.timeout_seconds)

            chosen_key = str(chosen_key)
            if not context.instance.has_dynamic_partition(component.dynamic_partitions_name, chosen_key):
                context.instance.add_dynamic_partitions(component.dynamic_partitions_name, [chosen_key])
                context.log.info(f"Registered new dynamic partition {chosen_key!r} on {component.dynamic_partitions_name!r}.")

            report_metadata: Dict[str, Any] = {"runtime_partition_key": dg.MetadataValue.text(chosen_key)}
            if component.report_value_as_metadata and payload is not None:
                try:
                    report_metadata["value"] = dg.MetadataValue.json(payload)
                except (TypeError, ValueError):
                    report_metadata["value"] = dg.MetadataValue.text(str(payload))

            context.instance.report_runless_asset_event(
                dg.AssetMaterialization(asset_key=target_key, partition=chosen_key, metadata=report_metadata),
            )
            context.log.info(f"Reported materialization of {target_key.to_user_string()} partition={chosen_key!r}.")

            return dg.MaterializeResult(metadata={
                "chosen_partition_key": dg.MetadataValue.text(chosen_key),
                "dynamic_partitions_name": dg.MetadataValue.text(component.dynamic_partitions_name),
                "target_asset_key": dg.MetadataValue.text(target_key.to_user_string()),
            })

        return dg.Definitions(assets=[_runner, target_spec])
