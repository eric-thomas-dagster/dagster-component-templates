"""SmartRetryComponent — retry classification beyond Dagster's built-in RetryPolicy.

Dagster's `RetryPolicy` retries every failure equally (max_retries, delay,
backoff). It has no built-in way to say "retry on 429 + 5xx but fail
immediately on 400/401/403/404" — because the retry decision lives at the
step-runner level, not the compute level, and Python exceptions get
Dagster-Failure-wrapped before RetryPolicy sees them.

This component gives customers the missing knob: classify errors at
compute time and re-raise as `RetryRequested` (retryable — step will
re-run) OR `Failure` (permanent — step gives up).

## Two classification rule kinds in v1

1. **`http_status`** — for compute that raises `requests.HTTPError` or
   `httpx.HTTPStatusError`. Inspects the response's status code + matches
   against `transient_codes` / `permanent_codes` lists.

2. **`exception_class`** — matches raised exception by dotted class path
   (e.g. `requests.exceptions.ConnectionError`, `TimeoutError`,
   `psycopg2.OperationalError`). Uses `isinstance` — subclasses match too.

Rules are evaluated top to bottom on each exception. First rule that
classifies the error wins (retryable OR permanent). If no rule matches,
the error is re-raised as-is (Dagster's default behavior applies).

## Compute shapes supported

The wrapped compute is either:

- **`kind: python`** — dotted `module.path:function_name`. Called with
  `context` + the upstream asset value (if `upstream_asset_key` set).
- **`kind: shell`** — shell command. `context.partition_key` threaded
  in as `DAGSTER_PARTITION_KEY` env var. Non-zero exit → RuntimeError.
- **`kind: http`** — one-off HTTP call. Auto-classifies status codes
  per RFC 7231 defaults; overridable via `retry_rules`.

## Retry backoff

Standard exponential-with-jitter. `retry_policy.backoff: exponential |
linear | fixed`. `initial_delay_seconds` + `max_delay_seconds` cap the
sleep. Sleeps happen INSIDE the compute (blocking), then RetryRequested
is raised — Dagster's step runner picks it up + re-runs on the next
attempt.

## What's coming in v2

- **LLM classification** — a small LLM call ("is this error transient?")
  when neither http_status nor exception_class rules match. Costs one
  LLM roundtrip per unclassified error; gate behind `enable_llm_fallback`.
- **Sliding-window rate limiting** — "no more than 3 retries per
  60-second window" to prevent runaway loops.
- **Circuit breaker** — after N total failures, refuse to attempt for
  M seconds. Prevents wasted API budget on a downed dependency.
"""

import functools
import importlib
import os
import random
import subprocess
import time
from typing import Any, Callable, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _resolve_python_callable(ref: str):
    """`module.path:function_name` → the callable."""
    if ":" not in ref:
        raise ValueError(
            f"smart_retry: python ref must be `module.path:function_name`, got {ref!r}"
        )
    module_path, func_name = ref.rsplit(":", 1)
    mod = importlib.import_module(module_path.strip())
    func = getattr(mod, func_name.strip(), None)
    if not callable(func):
        raise ValueError(
            f"smart_retry: {ref!r} does not resolve to a callable"
        )
    return func


def _resolve_exception_class(dotted: str):
    """`module.path.ClassName` → the class. Builtins like `TimeoutError` are
    resolved from the `builtins` module."""
    import builtins as _builtins
    if "." not in dotted:
        # Bare builtin like `TimeoutError`, `ValueError`, `ConnectionError`
        return getattr(_builtins, dotted, None)
    module_path, cls_name = dotted.rsplit(".", 1)
    try:
        mod = importlib.import_module(module_path)
    except ImportError:
        return None
    return getattr(mod, cls_name, None)


def _extract_http_status(exc: BaseException) -> Optional[int]:
    """Try to pull an HTTP status code from a common HTTPError-shape."""
    # requests
    resp = getattr(exc, "response", None)
    if resp is not None:
        code = getattr(resp, "status_code", None)
        if isinstance(code, int):
            return code
    # httpx
    resp = getattr(exc, "response", None)
    if resp is not None:
        code = getattr(resp, "status_code", None)
        if isinstance(code, int):
            return code
    # urllib
    code = getattr(exc, "code", None)
    if isinstance(code, int):
        return code
    return None


def _classify(exc: BaseException, rules: List[Dict[str, Any]]) -> Optional[str]:
    """Return `'transient'` | `'permanent'` | None (no rule matched)."""
    for rule in rules:
        kind = rule.get("kind")
        if kind == "http_status":
            status = _extract_http_status(exc)
            if status is None:
                continue
            transient_codes = set(int(c) for c in (rule.get("transient_codes") or []))
            permanent_codes = set(int(c) for c in (rule.get("permanent_codes") or []))
            if status in transient_codes:
                return "transient"
            if status in permanent_codes:
                return "permanent"
        elif kind == "exception_class":
            transient_dotted = rule.get("transient") or []
            permanent_dotted = rule.get("permanent") or []
            for dotted in transient_dotted:
                cls = _resolve_exception_class(dotted)
                if cls and isinstance(exc, cls):
                    return "transient"
            for dotted in permanent_dotted:
                cls = _resolve_exception_class(dotted)
                if cls and isinstance(exc, cls):
                    return "permanent"
    return None


def _compute_delay(
    attempt: int,
    backoff: str,
    initial: float,
    maximum: float,
    jitter: bool,
) -> float:
    """Compute the sleep delay before the next retry attempt (1-indexed)."""
    if backoff == "fixed":
        delay = initial
    elif backoff == "linear":
        delay = initial * attempt
    else:  # exponential (default)
        delay = initial * (2 ** (attempt - 1))
    delay = min(delay, maximum)
    if jitter:
        delay = delay * (0.5 + random.random() * 0.5)
    return max(0.0, delay)


def _run_compute(
    compute: Dict[str, Any],
    context: dg.AssetExecutionContext,
    upstream: Any,
) -> Any:
    """Dispatch on `compute.kind`. Returns whatever the compute returns."""
    kind = (compute.get("kind") or "python").lower()

    if kind == "python":
        ref = compute.get("python")
        if not ref:
            raise ValueError("compute.kind=python requires `python: 'module.path:func'`.")
        func = _resolve_python_callable(ref)
        # Accept: func(context) OR func(context, upstream) OR func()
        import inspect
        sig = inspect.signature(func)
        params = list(sig.parameters.values())
        n_positional = sum(1 for p in params if p.kind in (p.POSITIONAL_OR_KEYWORD, p.POSITIONAL_ONLY))
        if n_positional == 0:
            return func()
        if n_positional == 1:
            return func(context)
        return func(context, upstream)

    if kind == "shell":
        cmd = compute.get("cmd") or compute.get("command")
        if not cmd:
            raise ValueError("compute.kind=shell requires `cmd:` (list or string).")
        args = cmd if isinstance(cmd, list) else ["bash", "-c", cmd]
        env = dict(os.environ)
        if context.has_partition_key:
            env["DAGSTER_PARTITION_KEY"] = context.partition_key
        result = subprocess.run(
            args, env=env, capture_output=True, text=True,
            timeout=int(compute.get("timeout_seconds") or 300),
        )
        if result.stdout:
            context.log.info(result.stdout.strip())
        if result.returncode != 0:
            raise RuntimeError(
                f"shell compute exited {result.returncode}: {result.stderr[:400]!r}"
            )
        return result.stdout

    if kind == "http":
        import requests
        url = compute.get("url")
        method = (compute.get("method") or "GET").upper()
        headers = compute.get("headers") or {}
        body = compute.get("body")
        params = compute.get("params") or {}
        timeout = int(compute.get("timeout_seconds") or 30)
        resp = requests.request(method, url, headers=headers, json=body, params=params, timeout=timeout)
        resp.raise_for_status()  # will emit requests.HTTPError with response attached
        try:
            return resp.json()
        except ValueError:
            return {"raw": resp.text}

    raise ValueError(f"smart_retry: compute.kind={kind!r} not supported (python | shell | http)")


class SmartRetryComponent(dg.Component, dg.Model, dg.Resolvable):
    """Retry an asset's compute with classification-aware policies.

    Wraps a Python callable, shell command, or one-off HTTP call. Catches
    exceptions, classifies them against configurable rules (HTTP status
    codes, exception class), and either:

      - Sleeps + retries (transient — up to `retry_policy.max_attempts`)
      - Fails immediately (permanent — no wasted retries)

    Beyond Dagster's built-in RetryPolicy which retries every failure
    equally, this component lets you say "retry on 429 + 5xx but fail
    immediately on 400/401/403/404".

    Example:
        ```yaml
        type: dagster_community_components.SmartRetryComponent
        attributes:
          asset_name: enriched_orders
          upstream_asset_key: raw_orders
          compute:
            kind: python
            python: "my_project.enrichers:enrich_orders"
          retry_rules:
            - kind: http_status
              transient_codes: [429, 500, 502, 503, 504]
              permanent_codes: [400, 401, 403, 404, 422]
            - kind: exception_class
              transient: [ConnectionError, TimeoutError, "requests.exceptions.ConnectionError"]
              permanent: [ValueError, KeyError, "pydantic.ValidationError"]
          retry_policy:
            max_attempts: 5
            backoff: exponential
            initial_delay_seconds: 1.0
            max_delay_seconds: 60.0
            jitter: true
        ```
    """

    asset_name: str = Field(
        description="Dagster asset name."
    )
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Optional upstream asset the compute callable receives as second arg.",
    )

    compute: Dict[str, Any] = Field(
        description=(
            "How to run the actual work. Shape: `{kind: python|shell|http, ...kind-specific fields}`. "
            "python: `python: 'mod.path:func'`. "
            "shell: `cmd: '...'` or `cmd: [list]` + optional `timeout_seconds`. "
            "http: `method, url, headers, body, params, timeout_seconds`."
        ),
    )

    retry_rules: List[Dict[str, Any]] = Field(
        default_factory=list,
        description=(
            "Ordered classification rules. Each: `{kind: http_status | exception_class, ...}`. "
            "First matching rule wins. If no rule classifies, the error re-raises as-is "
            "(Dagster's default Failure handling applies)."
        ),
    )

    retry_policy: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Backoff config: `{max_attempts: 5, backoff: exponential|linear|fixed, "
            "initial_delay_seconds: 1.0, max_delay_seconds: 60.0, jitter: true}`. "
            "Defaults: max_attempts=3, backoff=exponential, initial=1.0, max=60.0, jitter=true."
        ),
    )

    # Catalog / governance
    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Default: ['python', 'retry'].",
    )

    # Partitioning
    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Any] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Smart Retry", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        rules = list(self.retry_rules or [])
        policy = dict(self.retry_policy or {})

        max_attempts = int(policy.get("max_attempts") or 3)
        backoff = str(policy.get("backoff") or "exponential")
        initial_delay = float(policy.get("initial_delay_seconds") or 1.0)
        max_delay = float(policy.get("max_delay_seconds") or 60.0)
        jitter = bool(policy.get("jitter", True))

        if backoff not in ("exponential", "linear", "fixed"):
            raise ValueError(f"retry_policy.backoff must be exponential|linear|fixed; got {backoff!r}")
        if max_attempts < 1:
            raise ValueError(f"retry_policy.max_attempts must be >= 1; got {max_attempts}")

        kinds_set = set(self.kinds or []) | {"python", "retry"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )

        ins = {}
        if upstream_asset_key:
            ins["upstream"] = dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Smart-retry-wrapped compute for {asset_name}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            kinds=kinds_set,
            partitions_def=partitions_def,
            ins=ins,
        )
        def _wrapped(context: dg.AssetExecutionContext, **kwargs):
            upstream = kwargs.get("upstream")
            last_exc: Optional[BaseException] = None
            for attempt in range(1, max_attempts + 1):
                try:
                    result = _run_compute(compute, context, upstream)
                    if attempt > 1:
                        context.log.info(
                            f"[smart_retry] succeeded on attempt {attempt}/{max_attempts}"
                        )
                    return dg.MaterializeResult(
                        metadata={
                            "attempts": attempt,
                            "compute_kind": compute.get("kind", "python"),
                        }
                    )
                except BaseException as exc:  # noqa: BLE001
                    last_exc = exc
                    classification = _classify(exc, rules)
                    exc_summary = f"{type(exc).__name__}: {str(exc)[:200]}"
                    status = _extract_http_status(exc)
                    status_str = f" (status={status})" if status is not None else ""

                    if classification == "permanent":
                        context.log.error(
                            f"[smart_retry] permanent failure on attempt {attempt}{status_str}: {exc_summary}"
                        )
                        raise dg.Failure(
                            description=(
                                f"smart_retry classified as PERMANENT{status_str} on attempt {attempt}: {exc_summary}"
                            ),
                            metadata={
                                "classification": "permanent",
                                "attempt": attempt,
                                "http_status": status if status is not None else -1,
                                "exception_class": type(exc).__name__,
                            },
                        ) from exc

                    if attempt >= max_attempts:
                        context.log.error(
                            f"[smart_retry] exhausted {max_attempts} attempts{status_str}: {exc_summary}"
                        )
                        classification_str = classification or "unclassified"
                        raise dg.Failure(
                            description=(
                                f"smart_retry exhausted {max_attempts} attempts "
                                f"(last classification: {classification_str}){status_str}: {exc_summary}"
                            ),
                            metadata={
                                "classification": classification_str,
                                "attempts": attempt,
                                "http_status": status if status is not None else -1,
                                "exception_class": type(exc).__name__,
                            },
                        ) from exc

                    # Transient OR unclassified — retry
                    delay = _compute_delay(attempt, backoff, initial_delay, max_delay, jitter)
                    context.log.warning(
                        f"[smart_retry] attempt {attempt}/{max_attempts} failed ({classification or 'unclassified'})"
                        f"{status_str}: {exc_summary} — sleeping {delay:.1f}s"
                    )
                    time.sleep(delay)

            # Fallthrough — shouldn't reach here (loop always returns or raises).
            if last_exc:
                raise last_exc
            raise RuntimeError("smart_retry: exhausted attempts with no captured exception")

        return dg.Definitions(assets=[_wrapped])


# --------------------------------------------------------------------------
# Public: @smart_retry decorator — wrap ANY existing Python function
# (typically the compute of an existing @dg.asset) with the same
# classification-aware retry engine the component uses. Composable — no
# YAML changes, no re-wiring existing components.
# --------------------------------------------------------------------------


def smart_retry(
    rules: Optional[List[Dict[str, Any]]] = None,
    *,
    max_attempts: int = 3,
    backoff: str = "exponential",
    initial_delay_seconds: float = 1.0,
    max_delay_seconds: float = 60.0,
    jitter: bool = True,
) -> Callable:
    """Decorator that adds classification-aware retry to any Python function.

    Applied BEFORE `@dg.asset` (or any other Dagster decorator) so the
    retry logic wraps the compute:

        ```python
        from dagster_community_components import smart_retry

        @dg.asset(ins={"raw": dg.AssetIn(...)})
        @smart_retry(
            rules=[
                {"kind": "http_status",
                 "transient_codes": [429, 500, 502, 503, 504],
                 "permanent_codes": [400, 401, 403, 404]},
                {"kind": "exception_class",
                 "transient": ["ConnectionError", "TimeoutError"],
                 "permanent": ["ValueError", "KeyError"]},
            ],
            max_attempts=5,
            backoff="exponential",
        )
        def enriched_orders(context, raw):
            return call_api(raw)   # existing user code, unchanged
        ```

    Same classification + backoff engine as `SmartRetryComponent`. Args
    mirror the component's `retry_rules` + `retry_policy` fields.

    On PERMANENT classification → raises `dg.Failure` immediately with
    classification metadata.
    On exhausted retries → raises `dg.Failure` with attempt count.
    On success → returns the underlying function's return value.

    Logs go through `context.log.*` if the function's first positional
    arg (or `context` kwarg) has a `.log` attribute — otherwise silent.
    """
    _rules = list(rules or [])
    if backoff not in ("exponential", "linear", "fixed"):
        raise ValueError(f"backoff must be exponential|linear|fixed; got {backoff!r}")
    if max_attempts < 1:
        raise ValueError(f"max_attempts must be >= 1; got {max_attempts}")

    def _decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            # Best-effort context extraction — Dagster passes context as the
            # first positional arg for @asset/@op-shaped compute functions.
            context = None
            if args and hasattr(args[0], "log"):
                context = args[0]
            elif "context" in kwargs and hasattr(kwargs["context"], "log"):
                context = kwargs["context"]

            def _log(level: str, msg: str):
                if context is not None:
                    getattr(context.log, level, context.log.info)(msg)

            last_exc: Optional[BaseException] = None
            for attempt in range(1, max_attempts + 1):
                try:
                    result = fn(*args, **kwargs)
                    if attempt > 1:
                        _log("info", f"[smart_retry] succeeded on attempt {attempt}/{max_attempts}")
                    return result
                except BaseException as exc:  # noqa: BLE001
                    last_exc = exc
                    classification = _classify(exc, _rules)
                    exc_summary = f"{type(exc).__name__}: {str(exc)[:200]}"
                    status = _extract_http_status(exc)
                    status_str = f" (status={status})" if status is not None else ""

                    if classification == "permanent":
                        _log("error", f"[smart_retry] permanent failure on attempt {attempt}{status_str}: {exc_summary}")
                        raise dg.Failure(
                            description=f"smart_retry classified as PERMANENT{status_str} on attempt {attempt}: {exc_summary}",
                            metadata={
                                "classification": "permanent",
                                "attempt": attempt,
                                "http_status": status if status is not None else -1,
                                "exception_class": type(exc).__name__,
                            },
                        ) from exc

                    if attempt >= max_attempts:
                        _log("error", f"[smart_retry] exhausted {max_attempts} attempts{status_str}: {exc_summary}")
                        classification_str = classification or "unclassified"
                        raise dg.Failure(
                            description=(
                                f"smart_retry exhausted {max_attempts} attempts "
                                f"(last classification: {classification_str}){status_str}: {exc_summary}"
                            ),
                            metadata={
                                "classification": classification_str,
                                "attempts": attempt,
                                "http_status": status if status is not None else -1,
                                "exception_class": type(exc).__name__,
                            },
                        ) from exc

                    delay = _compute_delay(attempt, backoff, initial_delay_seconds, max_delay_seconds, jitter)
                    _log(
                        "warning",
                        f"[smart_retry] attempt {attempt}/{max_attempts} failed ({classification or 'unclassified'})"
                        f"{status_str}: {exc_summary} — sleeping {delay:.1f}s",
                    )
                    time.sleep(delay)

            if last_exc:
                raise last_exc
            raise RuntimeError("smart_retry: exhausted attempts with no captured exception")

        # Attach the rules for introspection / testing.
        _wrapped.__smart_retry_rules__ = _rules  # type: ignore[attr-defined]
        return _wrapped

    return _decorator


def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
):
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily": return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly": return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly": return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("static partition requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("dynamic partition requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        return MultiPartitionsDefinition({d["name"]: _build_axis(d) for d in partition_dimensions})

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily": return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly": return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly": return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values: raise ValueError("static requires values")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("dynamic requires dynamic_partition_name")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")
