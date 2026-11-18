"""LoopGateAssetComponent.

Repeat a step's work until a condition is met -- Airflow 3.4's task-loop feature
(`@task_group.loop(max_iterations=, until=)`), verified against Airflow's own 3.4.0 docs and
Astronomer's task-loops guide, not guessed. `LoopGateOperator` (the name this was requested
under) is NOT a published, importable operator class -- it's the internal name Airflow's UI
shows for the gate task the `.loop()` feature generates after the looped group's terminal task.
The real, user-facing mechanism is: a task group runs its body, then a gate task calls the
`until` callable, which decides (via a `loop` object carrying `.result`/`.previous`/`.index`/
`.max_iterations`) whether to run the body again. `until` must return `bool` or a
`(stop, reason)` pair -- any other return value fails the gate, even `None`. No `until` ->
a fixed-count loop that always succeeds after `max_iterations` runs.

This component models that same shape as ONE Dagster asset and ONE callable, not a whole task
GROUP: the body callable runs in a plain Python loop inside the asset's own compute (not
separate ops per iteration -- Dagster has no primitive for "re-run this op N more times within
one step" the way Airflow's scheduler does across task instances), and the `until` callable is
consulted the same way after each iteration.

What this does NOT do:
- Repeat a whole multi-task GROUP the way Airflow's `.loop()` does -- only a single callable's
  body. If the thing that needs to repeat is itself a multi-step pipeline, call it as one
  function from `callable_path` (it can internally invoke several other functions), or compose
  several `loop_gate_asset`s if the steps need to be independently observable/retryable.
- Preserve a real Airflow gate's per-iteration retry: a real gate's `retries` stay within the
  SAME iteration -- only that one bad `until` call is retried, body state and prior iterations
  untouched. Dagster's own `RetryPolicy` instead retries the WHOLE loop from its first
  iteration, because the whole loop is one step here. If that distinction matters to your
  pipeline, add your own retry/idempotency handling inside the body callable instead of relying
  on `retry_policy_max_retries`.
"""
import inspect
from typing import Any, Dict, List, Optional, Union

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


class LoopState:
    """Passed as `loop=` to the `until_callable` -- mirrors the real Airflow 3.4 gate's own
    `loop` object (verified against Airflow's own 3.4.0 docs, not guessed): `.result` (this
    iteration's body return value), `.previous` (the IMMEDIATELY PRECEDING iteration's result
    -- a single value, not a list of every prior result -- or `None` on iteration 1),
    `.index` (1-based iteration number -- the first iteration is 1, not 0), `.max_iterations`."""

    def __init__(self, result: Any, previous: Any, index: int, max_iterations: int):
        self.result = result
        self.previous = previous
        self.index = index
        self.max_iterations = max_iterations

    def __repr__(self) -> str:
        return f"LoopState(result={self.result!r}, previous={self.previous!r}, index={self.index}, max_iterations={self.max_iterations})"


def _normalize_until_decision(decision):
    """Real Airflow 3.4 semantics: `until` must return bool or LoopDecision(stop=, reason=);
    ANY other return value -- including None -- fails the gate immediately. Mirrored exactly:
    a careless `until_callable` that forgets to return fails loudly instead of looping
    forever or silently stopping."""
    if isinstance(decision, bool):
        return decision, None
    if isinstance(decision, tuple) and len(decision) == 2 and isinstance(decision[0], bool):
        return decision[0], decision[1]
    raise dg.Failure(
        description=(
            f"loop_gate: until_callable must return bool or (stop: bool, reason), "
            f"got {decision!r}."
        )
    )


class LoopGateAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Repeat a step's work until a condition is met -- Airflow 3.4's task-loop
    (`@task_group.loop(max_iterations=, until=)`). `callable_path` is the body, run once per
    iteration. `until_callable_path` (optional) decides whether to stop: it's called with a
    single `loop=` keyword argument (a `LoopState`: `.result`, `.previous`, `.index`,
    `.max_iterations`) after every iteration, and must return `bool` or `(stop, reason)` --
    anything else, including a forgotten `return`, fails the run. Omitting `until_callable_path`
    makes this a fixed-count loop: the body runs `max_iterations` times and the asset always
    succeeds (matches Airflow's own fixed-count behavior with no `until`)."""

    asset_key: str = Field(description="The asset's key, slash-delimited.")
    callable_path: str = Field(description="'module.path:function_name'. The loop body -- called once per iteration with the same TaskFlow-style `inputs`/`args`/`kwargs` binding as python_callable_asset.")
    until_callable_path: Optional[str] = Field(
        default=None,
        description="'module.path:function_name'. Called as until_fn(loop=LoopState(...)) after every iteration. Must return bool or (stop: bool, reason). Omit for a fixed-count loop (runs max_iterations times, always succeeds).",
    )
    max_iterations: int = Field(description="Maximum number of times the body runs. Required, same as Airflow's own `.loop(max_iterations=...)`.")
    args: Optional[List[Any]] = Field(default=None, description="Static positional arguments passed to the body callable on every iteration.")
    kwargs: Optional[Dict[str, Any]] = Field(default=None, description="Static keyword arguments passed to the body callable on every iteration.")
    inputs: Optional[Dict[str, str]] = Field(
        default=None,
        description="{name: upstream_asset_key}. Each entry's value is loaded via the IO manager (once, before the loop starts) and bound to a parameter of that name if the body callable's signature declares one (TaskFlow-style) -- same shape as python_callable_asset's `inputs`.",
    )
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys -- no value loaded (use `inputs` to load a value).")

    group_name: str = Field(default="infrastructure", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Retries the WHOLE loop from iteration 0 -- see this component's module docstring for how this differs from a real Airflow gate's own per-iteration retry.")
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
        description="Fail this step if the WHOLE loop (every iteration, not just one) hasn't finished after this many seconds. Enforced via a background-thread wrapper -- the underlying call may continue running past the deadline (Python threads can't be forcibly killed); this bounds the step's wait, not the call itself.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=(), tags=('asset_tags',))

    @model_validator(mode="after")
    def _validate_fields(self):
        if self.max_iterations < 1:
            raise ValueError("LoopGateAssetComponent: max_iterations must be >= 1.")
        for name in (self.inputs or {}):
            if not name.isidentifier():
                raise ValueError(f"LoopGateAssetComponent: inputs key {name!r} must be a valid Python identifier.")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Loop Gate Asset", editable=True)

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

        @dg.asset(
            key=dg.AssetKey(component.asset_key.split("/")),
            description=component.description,
            ins=ins or None,
            deps=deps_keys,
            group_name=component.group_name,
            owners=component.owners or None,
            tags=tags,
            kinds=kinds,
            retry_policy=_retry_policy,
            pool=component.pool,
        )
        def _loop_gate(context: dg.AssetExecutionContext, **upstream_kwargs):
            body_fn = _resolve_callable(component.callable_path)
            until_fn = _resolve_callable(component.until_callable_path) if component.until_callable_path else None

            def _run_loop():
                previous: Any = None  # Airflow 3.4: the PRECEDING iteration's result, None on iteration 1
                final: Any = None
                for i in range(1, component.max_iterations + 1):  # Airflow 3.4: loop.index starts at one
                    final = _bind_call(body_fn, component.args, component.kwargs, upstream_kwargs)
                    if until_fn is not None:
                        decision = until_fn(loop=LoopState(final, previous, i, component.max_iterations))
                        stop, reason = _normalize_until_decision(decision)
                        context.log.info(
                            f"loop_gate: iteration {i} until_callable -> stop={stop}" + (f" ({reason})" if reason else "")
                        )
                        if stop:
                            return final, i, True
                    previous = final
                if until_fn is not None:
                    raise dg.Failure(
                        description=(
                            f"loop_gate: until_callable {component.until_callable_path!r} did not return "
                            f"stop=True within max_iterations={component.max_iterations}."
                        ),
                    )
                return final, component.max_iterations, None

            final, iterations_run, converged = _run_with_timeout(_run_loop, component.timeout_seconds)
            metadata: Dict[str, Any] = {"iterations_run": dg.MetadataValue.int(iterations_run)}
            if converged is not None:
                metadata["converged"] = dg.MetadataValue.bool(converged)
            return dg.Output(final, metadata=metadata)

        return dg.Definitions(assets=[_loop_gate])
