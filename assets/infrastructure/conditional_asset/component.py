"""ConditionalAssetComponent.

Branch / short-circuit as a Dagster asset -- the Airflow `ShortCircuitOperator` /
`BranchPythonOperator` equivalent. Built on a real, verified, native Dagster mechanism: a
`@dg.multi_asset` output declared `AssetOut(is_required=False)` that the compute function
chooses not to `yield` causes every downstream step depending on it to skip (cascading through
arbitrarily long chains), while the run still succeeds -- confirmed with real
`dg.materialize()` runs before building this, not assumed.
"""
import inspect
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field, model_validator


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
    the same "inject only what's declared" rule as python_callable_asset."""
    sig_params = inspect.signature(fn).parameters
    has_var_kw = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in sig_params.values())
    call_kwargs = dict(kwargs or {})
    for name, val in upstream_kwargs.items():
        if has_var_kw or name in sig_params:
            call_kwargs.setdefault(name, val)
    return fn(*(args or []), **call_kwargs)


class ConditionalAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Branch / short-circuit as a Dagster asset -- the Airflow `ShortCircuitOperator` /
    `BranchPythonOperator` equivalent.

    `mode: "short_circuit"`: the callable returns a truthy/falsy value. Truthy -> the single
    `asset_key` materializes and everything downstream runs normally. Falsy -> it doesn't
    materialize this run, and every downstream asset depending on it is skipped (not failed) --
    cascading through the whole downstream chain.

    `mode: "branch"`: the callable returns the name of exactly one of the statically-declared
    `branches` (mirroring Airflow's BranchPythonOperator -- the *set* of possible branches is
    always known at authoring time; only which one runs is dynamic). Only that branch's asset
    materializes; every other branch -- and everything depending on it -- skips.
    """

    mode: Literal["short_circuit", "branch"] = Field(description="'short_circuit': one asset, materializes or not. 'branch': pick exactly one of several statically-declared branches.")
    callable_path: str = Field(description="'module.path:function_name'. short_circuit: returns a truthy/falsy value. branch: returns a branch name (str), or (branch_name, payload) for an explicit payload.")
    args: Optional[List[Any]] = Field(default=None, description="Static positional arguments passed to the callable.")
    kwargs: Optional[Dict[str, Any]] = Field(default=None, description="Static keyword arguments passed to the callable.")
    inputs: Optional[Dict[str, str]] = Field(
        default=None,
        description="{name: upstream_asset_key}. Each entry's value is loaded via the IO manager and bound to a parameter of that name if the callable's signature declares one (TaskFlow-style) -- same shape as python_callable_asset's `inputs`.",
    )
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys -- no value loaded (use `inputs` to load a value).")

    asset_key: Optional[str] = Field(default=None, description="mode='short_circuit' only (required then): the single gate asset key, slash-delimited.")
    branches: Optional[Dict[str, str]] = Field(
        default=None,
        description="mode='branch' only (required then): {branch_name: asset_key}. asset_key is slash-delimited. The callable must return one of these branch_name values.",
    )

    group_name: str = Field(default="infrastructure", description="Dagster asset group name (applied to every branch uniformly in branch mode).")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset(s) in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy. Applies to the whole decision op (both the gate and, in branch mode, every branch), same as any other multi_asset.")
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
        description="Fail this step if the decision callable hasn't returned after this many seconds. Enforced via a background-thread wrapper -- the underlying callable may continue running past the deadline (Python threads can't be forcibly killed); this bounds the step's wait, not the call itself.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=(), tags=('asset_tags',))

    @model_validator(mode="after")
    def _validate_mode_fields(self):
        if self.mode == "short_circuit":
            if not self.asset_key:
                raise ValueError("ConditionalAssetComponent: mode='short_circuit' requires `asset_key`.")
            if self.branches:
                raise ValueError("ConditionalAssetComponent: `branches` is only used with mode='branch'.")
        else:
            if not self.branches:
                raise ValueError("ConditionalAssetComponent: mode='branch' requires `branches` ({branch_name: asset_key}).")
            if self.asset_key:
                raise ValueError("ConditionalAssetComponent: `asset_key` is only used with mode='short_circuit'.")
        for name in (self.inputs or {}):
            if not name.isidentifier():
                raise ValueError(f"ConditionalAssetComponent: inputs key {name!r} must be a valid Python identifier.")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Conditional Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        ins = {name: dg.AssetIn(key=dg.AssetKey.from_user_string(key)) for name, key in (component.inputs or {}).items()}
        deps_keys = [dg.AssetKey(d.split("/")) for d in (component.deps or [])]
        tags = component.asset_tags or None
        kinds = set(component.kinds) if component.kinds else None

        _retry_policy = None
        if component.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=component.retry_policy_max_retries,
                delay=component.retry_policy_delay_seconds or 1,
                backoff=Backoff[component.retry_policy_backoff.upper()],
            )

        if component.mode == "short_circuit":
            outs = {
                "gate": dg.AssetOut(
                    key=dg.AssetKey(component.asset_key.split("/")),
                    description=component.description,
                    is_required=False,
                    owners=component.owners or None,
                    tags=tags,
                    kinds=kinds,
                ),
            }

            @dg.multi_asset(
                outs=outs, ins=ins or None, deps=deps_keys, group_name=component.group_name,
                retry_policy=_retry_policy, pool=component.pool,
            )
            def _conditional(context: dg.AssetExecutionContext, **upstream_kwargs):
                fn = _resolve_callable(component.callable_path)
                result = _run_with_timeout(
                    lambda: _bind_call(fn, component.args, component.kwargs, upstream_kwargs), component.timeout_seconds,
                )
                if result:
                    yield dg.Output(result, output_name="gate")
                else:
                    context.log.info("short_circuit: callable returned falsy -- gate asset not materialized, downstream will skip.")

            return dg.Definitions(assets=[_conditional])

        branches = component.branches
        outs = {
            name: dg.AssetOut(
                key=dg.AssetKey(key.split("/")),
                description=component.description,
                is_required=False,
                owners=component.owners or None,
                tags=tags,
                kinds=kinds,
            )
            for name, key in branches.items()
        }

        @dg.multi_asset(
            outs=outs, ins=ins or None, deps=deps_keys, group_name=component.group_name,
            retry_policy=_retry_policy, pool=component.pool,
        )
        def _conditional(context: dg.AssetExecutionContext, **upstream_kwargs):
            fn = _resolve_callable(component.callable_path)
            result = _run_with_timeout(
                lambda: _bind_call(fn, component.args, component.kwargs, upstream_kwargs), component.timeout_seconds,
            )
            if isinstance(result, tuple) and len(result) == 2:
                chosen, payload = result
            else:
                chosen, payload = result, result
            if chosen not in branches:
                raise dg.Failure(
                    description=f"callable {component.callable_path!r} returned branch {chosen!r}, "
                    f"not one of the declared branches {sorted(branches)}."
                )
            context.log.info(f"branch: chose {chosen!r}; {sorted(set(branches) - {chosen})} will skip.")
            yield dg.Output(payload, output_name=chosen)

        return dg.Definitions(assets=[_conditional])


# --- af_gate / af_guard / af_finish -----------------------------------------------------
#
# `ConditionalAssetComponent` (above) is Dagster's STRUCTURAL skip propagation
# (`AssetOut(is_required=False)`, not yielded -> the engine skips every true downstream
# dependent). That's the right tool for a plain short-circuit/branch, but it has no
# selective rejoin: a step with several upstream branches skips the moment even ONE of
# them skipped, because the step never runs at all (see the component's README).
#
# These three decorators are the other tool, for pipelines that need that rejoin: a
# VALUE-level marker instead of a structural skip. Every decorated step always actually
# runs (nothing is skipped at the Dagster level) and returns a `StepOutcome` describing
# what happened, so the next step can inspect its upstreams' states and decide for itself
# -- exactly what the converter's own ~150-line `af_gate`/`af_guard`/`af_finish` runtime
# helper does today for branch/short-circuit/join/cleanup/alert shapes translated from
# Airflow trigger rules.
#
# Plain Python decorators (not a Component -- there's no per-step YAML config to express
# here beyond a rule name), applied BEFORE `@dg.op`/`@dg.asset`, same convention as
# `@on_hooks`/`@smart_retry`/`@timeout` elsewhere in this package.

import functools  # noqa: E402

_FAILED_STATES = ("failed", "upstream_failed")
_SKIPPED_STATES = ("skipped", "upstream_skipped")

# Airflow's own TriggerRule names/semantics, mapped onto this marker model. `all_done` has
# no distinct meaning here (there's no "still pending" state once a marker exists -- every
# upstream has already resolved to one of success/skipped/failed/upstream_*), so it's kept
# as an alias of `always` for naming fidelity with Airflow, not because it's a different
# check.
_TRIGGER_RULES = {
    "all_success": lambda states: all(s == "success" for s in states),
    "all_failed": lambda states: all(s in _FAILED_STATES for s in states),
    "all_done": lambda states: True,
    "one_failed": lambda states: any(s in _FAILED_STATES for s in states),
    "one_success": lambda states: any(s == "success" for s in states),
    "none_failed": lambda states: not any(s in _FAILED_STATES for s in states),
    "none_failed_min_one_success": lambda states: (
        not any(s in _FAILED_STATES for s in states) and any(s == "success" for s in states)
    ),
    "none_skipped": lambda states: not any(s in _SKIPPED_STATES for s in states),
    "always": lambda states: True,
}


class AfSkip(Exception):
    """Raise inside an `@af_gate`/`@af_guard`-wrapped function to mark this step as
    deliberately skipped, instead of failed -- the gate/guard's own short-circuit decision,
    expressed from inside the step body rather than by a guard rule over upstream states."""


class StepOutcome:
    """The marker `af_gate`/`af_guard` return instead of the raw value, and that
    `af_guard`/`af_finish` read off their inputs. `state` is one of 'success', 'skipped',
    'failed', 'upstream_failed', 'upstream_skipped'. `value` is the real return value when
    `state == 'success'`. `error` is a human-readable reason for any other state. `issues`
    accumulates every failure/skip message from the WHOLE upstream lineage feeding this
    outcome, not just this step's own -- so a join whose trigger rule let it proceed despite
    an upstream failure still carries that failure forward for `af_finish` to catch at the
    end, instead of a satisfied rule silently erasing it."""

    def __init__(self, state: str, value: Any = None, error: Optional[str] = None, issues: Optional[List[str]] = None):
        self.state = state
        self.value = value
        self.error = error
        self.issues = list(issues) if issues else []

    def __repr__(self) -> str:
        return f"StepOutcome(state={self.state!r}, value={self.value!r}, error={self.error!r}, issues={self.issues!r})"


def _is_context(obj) -> bool:
    return hasattr(obj, "log") and hasattr(obj, "run")


def _unwrap_call(args, kwargs):
    """Split a leading Dagster context (if any) off `args`, and reduce every remaining
    positional/keyword value to its real value while collecting each one's state and its
    full upstream `issues` lineage. A bare value not produced by one of these decorators is
    treated as state='success' with no issues, so a chain can mix decorated and undecorated
    steps."""
    context = args[0] if args and _is_context(args[0]) else None
    rest = args[1:] if context is not None else args
    states: List[str] = []
    issues: List[str] = []

    def _unwrap(v):
        if isinstance(v, StepOutcome):
            states.append(v.state)
            issues.extend(v.issues)
            return v.value
        states.append("success")
        return v

    new_args = ((context,) if context is not None else ()) + tuple(_unwrap(v) for v in rest)
    new_kwargs = {k: _unwrap(v) for k, v in kwargs.items()}
    return new_args, new_kwargs, states, issues


def af_gate(_fn=None):
    """A step that returns a `StepOutcome` marker instead of raising/propagating a real
    skip. If any upstream input is itself a non-'success' `StepOutcome`, the wrapped
    function is never called -- this step becomes 'upstream_failed'/'upstream_skipped'
    without running. Otherwise it runs; `AfSkip` -> 'skipped', any other exception ->
    'failed', a normal return -> 'success' (value preserved for downstream af_gate/
    af_guard steps to unwrap).

    ```python
    from dagster_community_components import af_gate

    @dg.op
    @af_gate()
    def check_rows(data):
        if not data:
            raise AfSkip("no rows to process")
        return len(data)
    ```
    """
    def _decorator(fn):
        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            new_args, new_kwargs, states, issues = _unwrap_call(args, kwargs)
            if any(s in _FAILED_STATES for s in states):
                return StepOutcome("upstream_failed", error="an upstream step failed", issues=issues)
            if any(s in _SKIPPED_STATES for s in states):
                return StepOutcome("upstream_skipped", error="an upstream step was skipped", issues=issues)
            try:
                result = fn(*new_args, **new_kwargs)
            except AfSkip as exc:
                msg = str(exc) or "skipped"
                return StepOutcome("skipped", error=msg, issues=issues + [msg])
            except BaseException as exc:  # noqa: BLE001
                msg = f"{type(exc).__name__}: {exc}"
                return StepOutcome("failed", error=msg, issues=issues + [msg])
            if isinstance(result, StepOutcome):
                result.issues = issues + result.issues
                return result
            return StepOutcome("success", value=result, issues=issues)
        return _wrapped
    return _decorator(_fn) if _fn is not None else _decorator


def af_guard(rule: str = "all_success"):
    """A join/rejoin step: evaluates `rule` (an Airflow TriggerRule name -- 'all_success'
    (default), 'all_failed', 'all_done', 'one_failed', 'one_success', 'none_failed',
    'none_failed_min_one_success', 'none_skipped', 'always') over its upstream inputs'
    states. Rule satisfied -> the wrapped function runs with the real (unwrapped) upstream
    values, same outcome handling as `af_gate`. Rule not satisfied -> the function is never
    called and this step becomes 'skipped' -- this is what makes a rejoin possible: the
    step still RUNS (at the Dagster level) regardless of which upstream branches actually
    materialized, so it's never itself skipped by structural propagation.

    ```python
    from dagster_community_components import af_guard

    @dg.op
    @af_guard(rule="one_success")   # cleanup/alert: run if at least one branch succeeded
    def notify(us_result, eu_result):
        ...
    ```
    """
    if rule not in _TRIGGER_RULES:
        raise ValueError(f"af_guard: unknown rule {rule!r}; choose one of {sorted(_TRIGGER_RULES)}")
    check = _TRIGGER_RULES[rule]

    def _decorator(fn):
        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            new_args, new_kwargs, states, issues = _unwrap_call(args, kwargs)
            if not check(states):
                return StepOutcome(
                    "skipped",
                    error=f"af_guard rule {rule!r} not satisfied by upstream states {states}",
                    issues=issues,
                )
            try:
                result = fn(*new_args, **new_kwargs)
            except AfSkip as exc:
                msg = str(exc) or "skipped"
                return StepOutcome("skipped", error=msg, issues=issues + [msg])
            except BaseException as exc:  # noqa: BLE001
                msg = f"{type(exc).__name__}: {exc}"
                return StepOutcome("failed", error=msg, issues=issues + [msg])
            if isinstance(result, StepOutcome):
                result.issues = issues + result.issues
                return result
            # Rule satisfied despite an upstream failure/skip (e.g. one_success,
            # none_failed) -- the step still ran, but `issues` carries that upstream
            # problem forward so a downstream af_finish can still catch it.
            return StepOutcome("success", value=result, issues=issues)
        return _wrapped
    return _decorator


def af_finish(_fn=None):
    """The pipeline's last step: always runs (same rejoin property as `af_guard`), but
    raises `dg.Failure` -- actually failing the Dagster run -- if any upstream input is a
    'failed'/'upstream_failed' `StepOutcome`, or if the wrapped function itself raises.
    Turns the soft, value-level failures `af_gate`/`af_guard` caught along the way back
    into a real run failure at the end, instead of a run that silently "succeeds" despite
    every branch having failed.

    ```python
    from dagster_community_components import af_finish

    @dg.op
    @af_finish()
    def pipeline_done(us_result, eu_result):
        return "done"
    ```
    """
    def _decorator(fn):
        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            new_args, new_kwargs, states, issues = _unwrap_call(args, kwargs)
            failed = [s for s in states if s in _FAILED_STATES]
            try:
                result = fn(*new_args, **new_kwargs)
            except BaseException as exc:  # noqa: BLE001
                raise dg.Failure(
                    description=f"af_finish: step raised {type(exc).__name__}: {exc}",
                ) from exc
            # `issues` (not just `states`) is the real check -- a guard upstream may have
            # proceeded (state='success') despite a failure elsewhere in its own upstream
            # lineage; that failure is still sitting in `issues`.
            if failed or issues:
                raise dg.Failure(
                    description=(
                        f"af_finish: upstream failure(s) detected (direct states {failed}, "
                        f"lineage issues {issues}); failing the run."
                    ),
                )
            return result
        return _wrapped
    return _decorator(_fn) if _fn is not None else _decorator
