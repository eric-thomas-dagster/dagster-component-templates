"""SmartRetryComponent — retry classification beyond Dagster's built-in RetryPolicy.

Dagster's `RetryPolicy` retries every failure equally (max_retries, delay,
backoff). It has no built-in way to say "retry on 429 + 5xx but fail
immediately on 400/401/403/404" — because the retry decision lives at the
step-runner level, not the compute level, and Python exceptions get
Dagster-Failure-wrapped before RetryPolicy sees them.

This component gives customers the missing knob: classify errors at
compute time and re-raise as `dg.RetryRequested(seconds_to_wait=...)`
(retryable — step goes to `up_for_retry`, sleeps in the step runner
outside the worker slot, comes back as a NEW step attempt in the run
graph) OR `dg.Failure` (permanent — step gives up immediately with
classification metadata attached).

The key thing: because we use Dagster's native `RetryRequested`
lifecycle instead of a busy `for attempt in range(N): time.sleep()`
loop inside the compute, every retry is a first-class Dagster step
attempt — visible in the run graph, tracked in Insights, `retry_number`
increments naturally, backoff sleep doesn't hold a worker slot. That's
the whole reason this component is worth reaching for over hand-rolled
try/except loops.

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

## Day-1 advanced features (all opt-in)

- **LLM classification** — small LLM call ("is this transient?") for
  errors NEITHER rule matched. Gate behind `llm_fallback: {...}`.
- **Sliding-window rate limiting** — cap retries per `window_seconds`.
- **Circuit breaker** — cross-run breaker via Dagster instance events
  (`AssetObservation` tags). Survives worker restarts + Serverless
  container churn. Falls back to in-process state when no context
  is available (tests).
"""

import functools
import importlib
import os
import random
import subprocess
import threading
import time
from collections import deque
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

import dagster as dg
from pydantic import Field


# --------------------------------------------------------------------------
# Cross-materialization state (module-level, per Python process)
#
# On long-running workers (Dagster+ Hybrid, K8s deployments), retries + circuit
# state persist across materializations in the same process. On Dagster+
# Serverless (ephemeral containers) state resets per run — that's fine for
# rate-limit-within-one-call semantics but the circuit-breaker cross-run
# guarantee only holds when the worker is long-lived. Cross-worker persistence
# (fsspec sidecar) is a follow-up.
# --------------------------------------------------------------------------

_STATE_LOCK = threading.Lock()


class _RateLimiter:
    """Sliding-window rate limiter.

    Cap `max_events` in any `window_seconds`. Records timestamps of retry
    attempts; if a new attempt would push us over the cap, either raises
    (mode="fail") or blocks until the window slides (mode="wait").
    """
    _windows: Dict[str, Deque[float]] = {}

    @classmethod
    def check_and_record(
        cls, key: str, max_events: int, window_seconds: float, mode: str,
    ) -> Optional[float]:
        """Return `None` on allowed. Return the time-to-wait if `mode=wait`
        and we'd exceed the cap. Raise `RuntimeError` if `mode=fail`."""
        with _STATE_LOCK:
            q = cls._windows.setdefault(key, deque())
            now = time.time()
            cutoff = now - window_seconds
            while q and q[0] < cutoff:
                q.popleft()
            if len(q) < max_events:
                q.append(now)
                return None
            # Over the cap.
            oldest = q[0]
            wait_s = max(0.0, window_seconds - (now - oldest) + 0.001)
        if mode == "wait":
            time.sleep(wait_s)
            # Recurse — after the sleep the window has slid.
            return cls.check_and_record(key, max_events, window_seconds, mode)
        raise RuntimeError(
            f"rate_limit exceeded: {len(cls._windows[key])} retries in the last "
            f"{window_seconds}s (cap={max_events})"
        )

    @classmethod
    def reset(cls, key: str) -> None:
        with _STATE_LOCK:
            cls._windows.pop(key, None)


class _CircuitBreaker:
    """Sliding-window circuit breaker with Dagster-instance-backed persistence.

    Two-tier state:
      1. **Dagster instance** (primary — cross-run, cross-worker, survives
         Serverless container churn). Uses `context.instance.get_event_records`
         to count prior asset-materialization FAILED steps for the key +
         `AssetObservation` events to mark the OPEN state's cooldown window.
      2. **In-process module dict** (fallback — for tests + code paths that
         don't have a Dagster context available).

    On Dagster-context calls: state is read/written through `context.instance`
    so multiple worker processes + serverless containers see the same breaker.
    On no-context calls: falls back to the module-level dict.
    """

    # Fallback in-process state.
    _state: Dict[str, Dict[str, Any]] = {}

    # Sentinel tag stored on AssetObservation events to mark "breaker OPEN".
    _OPEN_TAG = "wap_circuit_open_until"
    _FAIL_TAG = "wap_circuit_failure"

    @classmethod
    def check(cls, key: str, context: Any = None) -> Optional[float]:
        """Return `None` if closed. Return remaining seconds if open."""
        # Try instance-backed state first when we have a Dagster context.
        if context is not None and getattr(context, "instance", None) is not None:
            open_until = cls._get_instance_open_until(context, key)
            if open_until is not None:
                now = time.time()
                if open_until > now:
                    return open_until - now
                return None
        with _STATE_LOCK:
            s = cls._state.get(key)
            if not s:
                return None
            open_until = s.get("open_until", 0.0)
            now = time.time()
            if open_until > now:
                return open_until - now
            return None

    @classmethod
    def record_failure(
        cls, key: str, threshold: int, observation_window_seconds: float,
        cooldown_seconds: float, context: Any = None,
    ) -> None:
        # Instance-backed path: query recent tagged observations, count, open if threshold hit.
        if context is not None and getattr(context, "instance", None) is not None:
            now = time.time()
            # Emit the failure as an AssetObservation tagged with the key.
            try:
                from dagster import AssetObservation, AssetKey
                _asset_key = None
                if hasattr(context, "asset_key"):
                    _asset_key = context.asset_key
                elif hasattr(context, "assets_def"):
                    ad = context.assets_def
                    _asset_key = next(iter(ad.keys), None) if ad else None
                if _asset_key is None:
                    _asset_key = AssetKey([f"smart_retry_{key}"])
                # Emit through the runtime context so it lands in the event log
                # (this is what Dagster's own components do for observations).
                if hasattr(context, "log_event"):
                    context.log_event(AssetObservation(
                        asset_key=_asset_key,
                        tags={cls._FAIL_TAG: key, "ts": str(now)},
                    ))
            except Exception:  # noqa: BLE001
                pass
            # Count failures in the observation window.
            n_failures = cls._count_instance_failures(
                context, key, observation_window_seconds
            )
            if n_failures >= threshold:
                # Open the breaker: emit an observation marking cooldown end.
                try:
                    from dagster import AssetObservation, AssetKey
                    _asset_key = None
                    if hasattr(context, "asset_key"):
                        _asset_key = context.asset_key
                    if _asset_key is None:
                        _asset_key = AssetKey([f"smart_retry_{key}"])
                    if hasattr(context, "log_event"):
                        context.log_event(AssetObservation(
                            asset_key=_asset_key,
                            tags={cls._OPEN_TAG: key,
                                  "open_until": str(now + cooldown_seconds)},
                        ))
                except Exception:  # noqa: BLE001
                    pass
            return
        # Fallback: in-process state.
        with _STATE_LOCK:
            s = cls._state.setdefault(key, {"failures": deque(), "open_until": 0.0})
            now = time.time()
            cutoff = now - observation_window_seconds
            fq = s["failures"]
            while fq and fq[0] < cutoff:
                fq.popleft()
            fq.append(now)
            if len(fq) >= threshold:
                s["open_until"] = now + cooldown_seconds
                fq.clear()  # Reset window after opening.

    @classmethod
    def _get_instance_open_until(cls, context: Any, key: str) -> Optional[float]:
        """Query Dagster event log for the most recent `wap_circuit_open_until=<key>`
        observation and return its `open_until` timestamp."""
        try:
            from dagster import EventRecordsFilter, DagsterEventType
            records = context.instance.get_event_records(
                event_records_filter=EventRecordsFilter(
                    event_type=DagsterEventType.ASSET_OBSERVATION,
                ),
                limit=50, ascending=False,
            )
            for r in records:
                tags = (r.asset_observation.tags if r.asset_observation else None) or {}
                if tags.get(cls._OPEN_TAG) == key:
                    try:
                        return float(tags.get("open_until") or 0)
                    except Exception:  # noqa: BLE001
                        continue
        except Exception:  # noqa: BLE001
            return None
        return None

    @classmethod
    def _count_instance_failures(
        cls, context: Any, key: str, window_seconds: float,
    ) -> int:
        """Count `wap_circuit_failure=<key>` observations within the window."""
        try:
            from dagster import EventRecordsFilter, DagsterEventType
            records = context.instance.get_event_records(
                event_records_filter=EventRecordsFilter(
                    event_type=DagsterEventType.ASSET_OBSERVATION,
                ),
                limit=200, ascending=False,
            )
            cutoff = time.time() - window_seconds
            n = 0
            for r in records:
                tags = (r.asset_observation.tags if r.asset_observation else None) or {}
                if tags.get(cls._FAIL_TAG) != key:
                    continue
                try:
                    ts = float(tags.get("ts") or 0)
                except Exception:  # noqa: BLE001
                    continue
                if ts >= cutoff:
                    n += 1
            return n
        except Exception:  # noqa: BLE001
            return 0

    @classmethod
    def record_success(cls, key: str) -> None:
        with _STATE_LOCK:
            cls._state.pop(key, None)


def _llm_classify(
    exc: BaseException, model: str, api_key_env_var: str, timeout_seconds: float,
) -> Optional[str]:
    """LLM fallback classifier — one API call per unclassified error.

    Prompts a small model with the error class + message and asks whether it's
    likely transient (network/rate-limit/temporary) or permanent (bad
    input/misconfig/logic bug). Returns 'transient' | 'permanent' | None on
    parse failure or missing creds.
    """
    api_key = os.environ.get(api_key_env_var)
    if not api_key:
        return None
    try:
        from openai import OpenAI
    except ImportError:
        return None
    exc_class = type(exc).__name__
    exc_msg = str(exc)[:600]
    status = _extract_http_status(exc)
    status_note = f" (HTTP status={status})" if status is not None else ""
    prompt = (
        "You are classifying an error to decide whether to retry an operation.\n\n"
        f"Error class: {exc_class}\n"
        f"Error message: {exc_msg}\n"
        f"Context{status_note}\n\n"
        "Answer with ONE word: 'transient' (likely to succeed on retry — "
        "network glitch, rate limit, temporary service outage, timeout) OR "
        "'permanent' (retry will not help — bad input, missing config, "
        "authorization failure, logic bug).\n\n"
        "Answer:"
    )
    try:
        client = OpenAI(api_key=api_key, timeout=timeout_seconds)
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=8,
            temperature=0.0,
        )
        content = (resp.choices[0].message.content or "").strip().lower()
        if content.startswith("transient"):
            return "transient"
        if content.startswith("permanent"):
            return "permanent"
        return None
    except Exception:  # noqa: BLE001
        return None


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


def _classify(
    exc: BaseException,
    rules: List[Dict[str, Any]],
    *,
    llm_fallback: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Return `'transient'` | `'permanent'` | None (no rule matched).

    If `llm_fallback` is set and no rule matches, calls an LLM classifier
    with `{model, api_key_env_var, timeout_seconds}`. Costs ~1 API call
    per unclassified error.
    """
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
    # LLM fallback for unclassified errors.
    if llm_fallback:
        return _llm_classify(
            exc,
            model=llm_fallback.get("model", "gpt-4o-mini"),
            api_key_env_var=llm_fallback.get("api_key_env_var", "OPENAI_API_KEY"),
            timeout_seconds=float(llm_fallback.get("timeout_seconds", 10)),
        )
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

    llm_fallback: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "OPTIONAL. LLM classification for unclassified errors — one API call "
            "per un-rule-matched exception. Shape: "
            "`{model: 'gpt-4o-mini', api_key_env_var: 'OPENAI_API_KEY', timeout_seconds: 10}`. "
            "Omit to skip LLM fallback (unclassified errors treated as retryable "
            "up to max_attempts)."
        ),
    )

    rate_limit: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "OPTIONAL. Sliding-window rate limit ON RETRIES. Prevents runaway "
            "retry loops. Shape: "
            "`{max_events: 3, window_seconds: 60, mode: fail|wait, key: null}`. "
            "`key` defaults to `asset_name`; set explicitly to share a limit "
            "across multiple assets (e.g. all assets hitting one API)."
        ),
    )

    circuit_breaker: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "OPTIONAL. Cross-materialization circuit breaker. After `threshold` "
            "failures in `observation_window_seconds`, opens the circuit for "
            "`cooldown_seconds` — any new attempt during that window fails "
            "IMMEDIATELY without touching the compute. Prevents burning API "
            "budget on a downed dependency. Shape: "
            "`{threshold: 5, observation_window_seconds: 300, cooldown_seconds: 60, key: null}`. "
            "State is module-level (persists across materializations in the same "
            "Python process — long-lived workers only; ephemeral Serverless "
            "containers reset state per run)."
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

        llm_fallback_cfg = dict(self.llm_fallback) if self.llm_fallback else None
        rate_limit_cfg = dict(self.rate_limit) if self.rate_limit else None
        circuit_cfg = dict(self.circuit_breaker) if self.circuit_breaker else None

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

        # Circuit-breaker + rate-limit keys default to asset_name so multi-instance
        # deployments of the same YAML each have their own state; users can share
        # via an explicit `key` on either config.
        circuit_key = (circuit_cfg.get("key") if circuit_cfg else None) or asset_name
        rate_limit_key = (rate_limit_cfg.get("key") if rate_limit_cfg else None) or asset_name

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

            # `context.retry_number` is 0 for the initial attempt, 1 for
            # first retry, etc. We convert to a 1-based attempt number for
            # log lines + metadata.
            attempt = context.retry_number + 1

            # ── Circuit breaker: fail fast if OPEN ──
            # Check ONLY on the initial attempt — if the breaker opens
            # mid-retry-cycle, the current retry chain still finishes so we
            # don't leave a step stuck in up_for_retry forever.
            if circuit_cfg and context.retry_number == 0:
                open_seconds = _CircuitBreaker.check(circuit_key, context=context)
                if open_seconds is not None:
                    context.log.error(
                        f"[smart_retry] circuit OPEN for {circuit_key!r}; "
                        f"~{open_seconds:.1f}s cooldown remaining."
                    )
                    raise dg.Failure(
                        description=(
                            f"smart_retry circuit OPEN for {circuit_key!r} — "
                            f"~{open_seconds:.1f}s cooldown remaining. Prior "
                            f"failures exceeded threshold; refusing to attempt "
                            f"compute until cooldown elapses."
                        ),
                        metadata={
                            "classification": dg.MetadataValue.text("circuit_open"),
                            "circuit_key": dg.MetadataValue.text(circuit_key),
                            "cooldown_remaining_seconds": dg.MetadataValue.float(round(open_seconds, 2)),
                        },
                    )

            try:
                _run_compute(compute, context, upstream)
                if attempt > 1:
                    context.log.info(
                        f"[smart_retry] succeeded on attempt {attempt}/{max_attempts}"
                    )
                # Success: reset circuit-breaker accounting so intermittent
                # failures don't accumulate toward opening.
                if circuit_cfg:
                    _CircuitBreaker.record_success(circuit_key)
                return dg.MaterializeResult(
                    metadata={
                        "attempts": dg.MetadataValue.int(attempt),
                        "compute_kind": dg.MetadataValue.text(compute.get("kind", "python")),
                    }
                )
            except (dg.Failure, dg.RetryRequested):
                # Compute already spoke Dagster's retry/failure vocabulary
                # (e.g., nested @smart_retry, or explicit dg.Failure from a
                # sub-component). Don't reclassify — propagate as-is.
                raise
            except BaseException as exc:  # noqa: BLE001
                classification = _classify(exc, rules, llm_fallback=llm_fallback_cfg)
                exc_summary = f"{type(exc).__name__}: {str(exc)[:200]}"
                status = _extract_http_status(exc)
                status_str = f" (status={status})" if status is not None else ""

                # Record failure for circuit-breaker accounting BEFORE the
                # classification-branch decides how to fail. Passes `context`
                # so cross-run state via Dagster instance events kicks in on
                # real workers (falls back to in-process state in test contexts).
                if circuit_cfg:
                    _CircuitBreaker.record_failure(
                        circuit_key,
                        int(circuit_cfg.get("threshold") or 5),
                        float(circuit_cfg.get("observation_window_seconds") or 300),
                        float(circuit_cfg.get("cooldown_seconds") or 60),
                        context=context,
                    )

                if classification == "permanent":
                    context.log.error(
                        f"[smart_retry] permanent failure on attempt {attempt}{status_str}: {exc_summary}"
                    )
                    raise dg.Failure(
                        description=(
                            f"smart_retry classified as PERMANENT{status_str} on attempt {attempt}: {exc_summary}"
                        ),
                        metadata={
                            "classification": dg.MetadataValue.text("permanent"),
                            "attempt": dg.MetadataValue.int(attempt),
                            "http_status": dg.MetadataValue.int(status if status is not None else -1),
                            "exception_class": dg.MetadataValue.text(type(exc).__name__),
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
                            "classification": dg.MetadataValue.text(classification_str),
                            "attempts": dg.MetadataValue.int(attempt),
                            "http_status": dg.MetadataValue.int(status if status is not None else -1),
                            "exception_class": dg.MetadataValue.text(type(exc).__name__),
                        },
                    ) from exc

                # Transient OR unclassified — retry, subject to rate limit.
                if rate_limit_cfg:
                    try:
                        _RateLimiter.check_and_record(
                            rate_limit_key,
                            int(rate_limit_cfg.get("max_events") or 3),
                            float(rate_limit_cfg.get("window_seconds") or 60),
                            str(rate_limit_cfg.get("mode") or "fail"),
                        )
                    except RuntimeError as rl_exc:
                        context.log.error(
                            f"[smart_retry] rate limit hit for {rate_limit_key!r}: {rl_exc}"
                        )
                        raise dg.Failure(
                            description=(
                                f"smart_retry rate_limit exceeded for {rate_limit_key!r} "
                                f"— last error: {exc_summary}"
                            ),
                            metadata={
                                "classification": dg.MetadataValue.text("rate_limited"),
                                "rate_limit_key": dg.MetadataValue.text(rate_limit_key),
                                "last_exception_class": dg.MetadataValue.text(type(exc).__name__),
                            },
                        ) from exc

                delay = _compute_delay(attempt, backoff, initial_delay, max_delay, jitter)
                context.log.warning(
                    f"[smart_retry] attempt {attempt}/{max_attempts} failed ({classification or 'unclassified'})"
                    f"{status_str}: {exc_summary} — requesting Dagster retry in {delay:.1f}s"
                )
                # ── Real Dagster retry — the step goes to up_for_retry,
                # sleeps in the step runner (not this worker slot), and comes
                # back as a NEW step attempt. Shows up in the run graph,
                # tracked in Insights, and doesn't block a slot during backoff.
                raise dg.RetryRequested(
                    max_retries=max_attempts - 1,
                    seconds_to_wait=delay,
                ) from exc

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
    llm_fallback: Optional[Dict[str, Any]] = None,
    rate_limit: Optional[Dict[str, Any]] = None,
    circuit_breaker: Optional[Dict[str, Any]] = None,
    key: Optional[str] = None,
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
            # Optional day-1 features (all opt-in):
            llm_fallback={"model": "gpt-4o-mini", "api_key_env_var": "OPENAI_API_KEY"},
            rate_limit={"max_events": 3, "window_seconds": 60, "mode": "fail"},
            circuit_breaker={"threshold": 5, "observation_window_seconds": 300, "cooldown_seconds": 60},
            key="enrich_api",  # defaults to fn.__qualname__ if omitted
        )
        def enriched_orders(context, raw):
            return call_api(raw)   # existing user code, unchanged
        ```

    Same classification + backoff engine as `SmartRetryComponent`. Args
    mirror the component's `retry_rules` + `retry_policy` fields.

    `llm_fallback` — {model, api_key_env_var, timeout_seconds}. LLM
      classifies unrule-matched exceptions as transient/permanent.
    `rate_limit` — {max_events, window_seconds, mode: fail|wait}. Caps
      retries within a sliding window.
    `circuit_breaker` — {threshold, observation_window_seconds,
      cooldown_seconds}. Fails fast when the breaker is OPEN.
    `key` — shared state key for rate-limit + circuit-breaker; defaults to
      `fn.__qualname__` so each decorated function has its own state.

    On PERMANENT classification → raises `dg.Failure` immediately with
    classification metadata.
    On exhausted retries → raises `dg.Failure` with attempt count.
    On success → returns the underlying function's return value.
    """
    _rules = list(rules or [])
    if backoff not in ("exponential", "linear", "fixed"):
        raise ValueError(f"backoff must be exponential|linear|fixed; got {backoff!r}")
    if max_attempts < 1:
        raise ValueError(f"max_attempts must be >= 1; got {max_attempts}")
    _llm_cfg = dict(llm_fallback) if llm_fallback else None
    _rl_cfg = dict(rate_limit) if rate_limit else None
    _cb_cfg = dict(circuit_breaker) if circuit_breaker else None

    def _decorator(fn: Callable) -> Callable:
        _state_key = key or getattr(fn, "__qualname__", None) or getattr(fn, "__name__", "smart_retry_default")

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

            # ── Circuit breaker: fail fast if OPEN ──
            # Passes `context` so cross-run state via Dagster instance events kicks
            # in on real workers (falls back to in-process state when context is None).
            if _cb_cfg:
                open_seconds = _CircuitBreaker.check(_state_key, context=context)
                if open_seconds is not None:
                    _log("error", f"[smart_retry] circuit OPEN for {_state_key!r}; ~{open_seconds:.1f}s cooldown remaining.")
                    raise dg.Failure(
                        description=f"smart_retry circuit OPEN for {_state_key!r} — ~{open_seconds:.1f}s cooldown remaining.",
                        metadata={
                            "classification": dg.MetadataValue.text("circuit_open"),
                            "circuit_key": dg.MetadataValue.text(_state_key),
                            "cooldown_remaining_seconds": dg.MetadataValue.float(round(open_seconds, 2)),
                        },
                    )

            if context is None or not hasattr(context, "retry_number"):
                raise RuntimeError(
                    "@smart_retry requires a Dagster context — apply it to the compute "
                    "of a `@dg.asset` / `@dg.op` so the first arg is an execution context."
                )

            attempt = context.retry_number + 1  # 1-based for log lines + metadata

            try:
                result = fn(*args, **kwargs)
                if attempt > 1:
                    _log("info", f"[smart_retry] succeeded on attempt {attempt}/{max_attempts}")
                if _cb_cfg:
                    _CircuitBreaker.record_success(_state_key)
                return result
            except (dg.Failure, dg.RetryRequested):
                # Compute already spoke Dagster's retry/failure vocabulary
                # (e.g., a nested @smart_retry) — propagate as-is.
                raise
            except BaseException as exc:  # noqa: BLE001
                classification = _classify(exc, _rules, llm_fallback=_llm_cfg)
                exc_summary = f"{type(exc).__name__}: {str(exc)[:200]}"
                status = _extract_http_status(exc)
                status_str = f" (status={status})" if status is not None else ""

                if _cb_cfg:
                    _CircuitBreaker.record_failure(
                        _state_key,
                        int(_cb_cfg.get("threshold") or 5),
                        float(_cb_cfg.get("observation_window_seconds") or 300),
                        float(_cb_cfg.get("cooldown_seconds") or 60),
                        context=context,
                    )

                if classification == "permanent":
                    _log("error", f"[smart_retry] permanent failure on attempt {attempt}{status_str}: {exc_summary}")
                    raise dg.Failure(
                        description=f"smart_retry classified as PERMANENT{status_str} on attempt {attempt}: {exc_summary}",
                        metadata={
                            "classification": dg.MetadataValue.text("permanent"),
                            "attempt": dg.MetadataValue.int(attempt),
                            "http_status": dg.MetadataValue.int(status if status is not None else -1),
                            "exception_class": dg.MetadataValue.text(type(exc).__name__),
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
                            "classification": dg.MetadataValue.text(classification_str),
                            "attempts": dg.MetadataValue.int(attempt),
                            "http_status": dg.MetadataValue.int(status if status is not None else -1),
                            "exception_class": dg.MetadataValue.text(type(exc).__name__),
                        },
                    ) from exc

                # Rate-limit check before scheduling the next attempt.
                if _rl_cfg:
                    try:
                        _RateLimiter.check_and_record(
                            _state_key,
                            int(_rl_cfg.get("max_events") or 3),
                            float(_rl_cfg.get("window_seconds") or 60),
                            str(_rl_cfg.get("mode") or "fail"),
                        )
                    except RuntimeError as rl_exc:
                        _log("error", f"[smart_retry] rate limit hit for {_state_key!r}: {rl_exc}")
                        raise dg.Failure(
                            description=f"smart_retry rate_limit exceeded for {_state_key!r} — last error: {exc_summary}",
                            metadata={
                                "classification": dg.MetadataValue.text("rate_limited"),
                                "rate_limit_key": dg.MetadataValue.text(_state_key),
                                "last_exception_class": dg.MetadataValue.text(type(exc).__name__),
                            },
                        ) from exc

                delay = _compute_delay(attempt, backoff, initial_delay_seconds, max_delay_seconds, jitter)
                _log(
                    "warning",
                    f"[smart_retry] attempt {attempt}/{max_attempts} failed ({classification or 'unclassified'})"
                    f"{status_str}: {exc_summary} — requesting Dagster retry in {delay:.1f}s",
                )
                # Step goes to up_for_retry, waits in the step runner
                # (no worker slot held), returns as a new step attempt
                # visible in the run graph + tracked in Insights.
                raise dg.RetryRequested(
                    max_retries=max_attempts - 1,
                    seconds_to_wait=delay,
                ) from exc

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
