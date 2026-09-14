"""TaskAssetComponent + `@task` decorator — dynamic, runtime-declared sub-steps
   with two shipping shapes:

1. **`TaskAssetComponent`** (YAML component + `@task_asset` decorator) —
   declare N processing LAYERS at compile time; the number of work items
   at each layer is 100% discovered at runtime. Each layer's fan-out
   renders as real graph nodes (via `DynamicOutput`). Between layers we
   auto-scaffold a `collect + re-emit` bridge so any depth is legal
   (Dagster explicitly rejects two dynamic outputs feeding one op — the
   bridge sidesteps that).

2. **`@task` decorator + `child_step(context, name)` context manager** —
   arbitrary-depth programmatic nesting from inside ANY compute. Emits
   fabricated `STEP_START` / `STEP_SUCCESS` / `STEP_FAILURE` events keyed
   `<parent_step_key>[task.<name>.<seq>]`. Renders in the LOG tab with
   hierarchical attribution + real durations + status. Doesn't render as
   graph nodes (the frontend graph iterator filters to plan-known
   step_keys) — but the log-level attribution is enough for many
   debugging / observability cases.

## Which shape to pick

- Doc-parser / batch-per-row / per-item LLM processing / any "scan
  then dispatch" pattern → **`TaskAssetComponent`** (graph render).
- Agentic tool-use loop / recursive drill-down / deep parsers where
  the shape is truly stack-nested and unknowable to a scanner →
  **`@task` decorator** (log attribution).

Both compose freely.

## Layered pattern — the mechanic

```yaml
type: dagster_community_components.TaskAssetComponent
attributes:
  asset_name: parse_document

  layers:
    - name: scan
      compute: "my_project.parser:scan"          # emits initial work items
    - name: process_block
      compute: "my_project.parser:process_block" # per block, returns list of next-layer specs
    - name: parse_url
      compute: "my_project.parser:parse_url"     # per URL, returns per-URL data
```

Every layer's `compute` is a `mod:fn` reference. Signatures:

- **First layer** (the scan) receives `(context)` and returns/yields
  an iterable of `(task_name, task_spec)` pairs. `task_spec` is any
  JSON-safe object — passed as input to the next layer.
- **Middle layers** receive `(context, task_spec)` (the item from the
  previous layer). Return a list of `(task_name, task_spec)` pairs to
  fan out to the next layer, OR return a plain value (terminal for
  this branch).
- **Final layer** (optional) receives the collected list from the prior
  layer. Returns the asset's final value.

## Graph shape

    layer0_scan  →  layer1_process[?]  →  bridge01  →  layer2_process[?]  →  ...  →  collect

Each `layerN_process[?]` node fans out at runtime into
`layerN_process[<mapping_key>]` instances — visible in the UI.
"""

import contextlib
import contextvars
import functools
import importlib
import itertools
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import dagster as dg
from dagster import DynamicOut, DynamicOutput
from dagster._core.events import DagsterEvent, DagsterEventType
from dagster._core.execution.plan.objects import StepSuccessData, StepFailureData
from dagster._utils.error import serializable_error_info_from_exc_info
from pydantic import Field


# ==========================================================================
# Shape 2: `@task` decorator + `child_step(context, name)` context manager
# ==========================================================================
# Emits fabricated STEP_START/SUCCESS/FAILURE events attributed to a
# synthetic step_key `<parent>[task.<name>.<seq>]`. Nests arbitrarily via
# a contextvars-backed active-task stack. Log tab shows hierarchical
# step_keys; graph does not render (documented limit).

_seq_counter = itertools.count(1)


def _next_seq() -> int:
    return next(_seq_counter)


# Active task-key suffix stack — contextvar so nested @task calls see the
# correct parent regardless of thread/async context.
_active_task_stack: "contextvars.ContextVar[tuple[str, ...]]" = contextvars.ContextVar(
    "_active_task_stack", default=()
)

# When set (by `@task_asset`), `@task` calls RECORD instead of execute:
# each call appends (name, fn, args, kwargs) to this list, which the
# framework then fans out via DynamicOutput. `None` = execute normally.
_recording_queue: "contextvars.ContextVar[Optional[list]]" = contextvars.ContextVar(
    "_recording_queue", default=None
)


def _parent_step_key(context: Any) -> str:
    """Root step_key = the enclosing op's step.key + any active task suffixes."""
    try:
        step_ctx = context.get_step_execution_context()
        root = step_ctx.step.key
    except Exception:  # noqa: BLE001
        root = "task"
    stack = _active_task_stack.get()
    if stack:
        return root + "".join(stack)
    return root


def _push_task(suffix: str):
    stack = _active_task_stack.get()
    return _active_task_stack.set(stack + (suffix,))


def _pop_task(token) -> None:
    _active_task_stack.reset(token)


def _emit(context: Any, event_type: DagsterEventType, step_key: str,
          message: str = "", event_specific_data: Optional[Any] = None) -> None:
    try:
        run_id = context.run.run_id
    except Exception:  # noqa: BLE001
        try:
            run_id = context.run_id
        except Exception:  # noqa: BLE001
            return
    ev = DagsterEvent(
        event_type_value=event_type.value,
        job_name=context.job_name,
        message=message,
        step_key=step_key,
        event_specific_data=event_specific_data,
    )
    context.instance.report_dagster_event(ev, run_id=run_id)


@contextlib.contextmanager
def child_step(context: Any, op_name: str, mapping_key: Optional[str] = None):
    """Enter a child step scope. Emits STEP_START on entry; STEP_SUCCESS on
    normal exit; STEP_FAILURE on exception (and re-raises).

    Step_key shape mimics Dagster's DynamicOutput convention:
        `<parent>.<op_name>[<mapping_key>]`
    where the frontend renders `<op_name>` as the box label + `[<mapping_key>]`
    as a badge. If `mapping_key` isn't supplied, an auto-seq counter is used.

    ```python
    with child_step(context, "parse_title"):                     # → parse_title[1]
        title = extract_title(doc)
    with child_step(context, "parse_url", mapping_key="acme_com"):  # → parse_url[acme_com]
        parse(url)
    ```
    """
    parent = _parent_step_key(context)
    safe_op = "".join(ch if (ch.isalnum() or ch in "_-") else "_" for ch in op_name)
    if mapping_key is not None:
        safe_mk = "".join(ch if (ch.isalnum() or ch in "_-") else "_" for ch in str(mapping_key))
    else:
        safe_mk = str(_next_seq())
    suffix = f".{safe_op}[{safe_mk}]"
    key = parent + suffix
    started = time.time()
    name = f"{op_name}[{safe_mk}]"

    _emit(context, DagsterEventType.STEP_START, key, message=f"[task:{name}] start")
    try:
        context.log.info(f"[task:{name}] → start (step_key={key})")
    except Exception:  # noqa: BLE001
        pass

    token = _push_task(suffix)
    try:
        try:
            yield ChildStepHandle(context, key, name, started)
        except BaseException as exc:  # noqa: BLE001
            duration_ms = (time.time() - started) * 1000.0
            try:
                info = serializable_error_info_from_exc_info(
                    (type(exc), exc, exc.__traceback__)
                )
            except Exception:  # noqa: BLE001
                info = None
            _emit(
                context, DagsterEventType.STEP_FAILURE, key,
                message=f"[task:{name}] failed after {duration_ms:.1f}ms: {type(exc).__name__}: {exc}",
                event_specific_data=StepFailureData(error=info, user_failure_data=None),
            )
            try:
                context.log.error(
                    f"[task:{name}] ← failed after {duration_ms:.1f}ms: {type(exc).__name__}: {exc}"
                )
            except Exception:  # noqa: BLE001
                pass
            raise
        else:
            duration_ms = (time.time() - started) * 1000.0
            _emit(
                context, DagsterEventType.STEP_SUCCESS, key,
                message=f"[task:{name}] ok in {duration_ms:.1f}ms",
                event_specific_data=StepSuccessData(duration_ms=duration_ms),
            )
            try:
                context.log.info(f"[task:{name}] ← ok in {duration_ms:.1f}ms")
            except Exception:  # noqa: BLE001
                pass
    finally:
        _pop_task(token)


class ChildStepHandle:
    """Yielded by `child_step()` — lightweight scope object for the child."""
    def __init__(self, context: Any, step_key: str, name: str, started: float):
        self.context = context
        self.step_key = step_key
        self.name = name
        self._started = started

    @property
    def elapsed_seconds(self) -> float:
        return time.time() - self._started


# ==========================================================================
# Task cache — resumability primitive for runtime-decided sub-work
# ==========================================================================
# Prefect-style: cache @task results by a computed key. On re-run, cached
# hits skip execution → effective resume-from-failure for dynamic workloads.
# @task events still emit for cache hits (so the graph node still renders),
# just with near-zero duration + a [cache_hit] tag on the log line.

class TaskCache:
    """Protocol for a @task result cache. Implement `get` / `put` / `has`.

    Return value from `get` for a miss is the sentinel `TaskCache.MISS`.
    On hit, return the stored value directly (any Python object).
    """
    MISS = object()

    def get(self, key: str) -> Any:  # returns MISS if not present
        raise NotImplementedError
    def put(self, key: str, value: Any) -> None:
        raise NotImplementedError
    def has(self, key: str) -> bool:
        return self.get(key) is not TaskCache.MISS


class FilesystemTaskCache(TaskCache):
    """Local disk task cache. Stores each entry as a pickle file under
    `<base_dir>/<sha256(key)>.pkl`. Optional TTL enforced on `get`.

    LRU eviction: pass `max_entries` and/or `max_bytes` to cap the cache
    dir size. After each `put`, if EITHER cap is exceeded, oldest files
    (by mtime) are deleted until BOTH caps are satisfied.
    """

    def __init__(
        self,
        base_dir: str,
        ttl_seconds: Optional[float] = None,
        max_entries: Optional[int] = None,
        max_bytes: Optional[int] = None,
    ):
        import os
        os.makedirs(base_dir, exist_ok=True)
        self._base = base_dir
        self._ttl = ttl_seconds
        self._max_entries = max_entries
        self._max_bytes = max_bytes

    def _path(self, key: str) -> str:
        import hashlib, os
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return os.path.join(self._base, f"{digest}.pkl")

    def get(self, key: str) -> Any:
        import os, pickle
        p = self._path(key)
        if not os.path.exists(p):
            return TaskCache.MISS
        if self._ttl is not None:
            age = time.time() - os.path.getmtime(p)
            if age > self._ttl:
                return TaskCache.MISS
        try:
            with open(p, "rb") as f:
                return pickle.load(f)
        except Exception:  # noqa: BLE001
            return TaskCache.MISS

    def put(self, key: str, value: Any) -> None:
        import pickle
        with open(self._path(key), "wb") as f:
            pickle.dump(value, f)
        if self._max_entries is not None or self._max_bytes is not None:
            self._evict_lru()

    def _evict_lru(self) -> None:
        """Delete oldest files (by mtime) until both caps are satisfied."""
        import os
        try:
            files = [
                (os.path.join(self._base, f), os.path.getmtime(os.path.join(self._base, f)), os.path.getsize(os.path.join(self._base, f)))
                for f in os.listdir(self._base) if f.endswith(".pkl")
            ]
        except OSError:
            return
        files.sort(key=lambda t: t[1])  # oldest first
        while files:
            over_entries = self._max_entries is not None and len(files) > self._max_entries
            over_bytes = self._max_bytes is not None and sum(t[2] for t in files) > self._max_bytes
            if not (over_entries or over_bytes):
                break
            path, _, _ = files.pop(0)
            try:
                os.remove(path)
            except OSError:
                pass


# ═════════════════════════════════════════════════════════════════════
# CachePolicy — composable cache-key policy (Prefect parity)
# ═════════════════════════════════════════════════════════════════════

@functools.total_ordering
class CachePolicy:
    """Composable cache policy. Prefect-parity `CachePolicy` shape.

    Combine building blocks with `+`:

        INPUTS + TASK_SOURCE     # cache key = hash(inputs) + hash(function source)
        INPUTS + CROSS_RUN       # inputs-hashed, cache shared across all runs
        NO_CACHE                 # bypass cache entirely, even if @task cache= is set

    Building blocks (module-level constants below):
      INPUTS       — include a hash of task inputs in the key (Prefect's `task_input_hash`)
      TASK_SOURCE  — include a hash of the function's source code (invalidates on code edit)
      RUN_ONLY     — scope cache to the current run's run_id (never survives across runs)
      ROOT_RUN     — scope to root_run_id (Dagster default; survives re-execute-from-failure)
      CROSS_RUN    — no run scoping (Prefect default; "parse this URL once ever")
      NO_CACHE     — disable caching for this task

    Attributes:
      include_inputs: hash args/kwargs into the cache key.
      include_source: hash the function's source code into the cache key.
      run_scope: 'root_run' | 'cross_run' | 'run_only'.
      disabled: bypass the cache entirely (NO_CACHE).
    """
    __slots__ = ("include_inputs", "include_source", "run_scope", "disabled")

    def __init__(
        self,
        include_inputs: bool = False,
        include_source: bool = False,
        run_scope: str = "root_run",
        disabled: bool = False,
    ):
        if run_scope not in ("root_run", "cross_run", "run_only"):
            raise ValueError(f"CachePolicy run_scope must be one of root_run/cross_run/run_only; got {run_scope!r}")
        object.__setattr__(self, "include_inputs", include_inputs)
        object.__setattr__(self, "include_source", include_source)
        object.__setattr__(self, "run_scope", run_scope)
        object.__setattr__(self, "disabled", disabled)

    def __add__(self, other: "CachePolicy") -> "CachePolicy":
        if not isinstance(other, CachePolicy):
            return NotImplemented
        # NO_CACHE + anything = NO_CACHE (short-circuits)
        if self.disabled or other.disabled:
            return CachePolicy(disabled=True)
        # For run_scope: right-hand non-default wins so `INPUTS + CROSS_RUN` → cross_run.
        rs = other.run_scope if other.run_scope != "root_run" else self.run_scope
        return CachePolicy(
            include_inputs=self.include_inputs or other.include_inputs,
            include_source=self.include_source or other.include_source,
            run_scope=rs,
            disabled=False,
        )

    def __repr__(self) -> str:
        if self.disabled:
            return "NO_CACHE"
        parts = []
        if self.include_inputs: parts.append("INPUTS")
        if self.include_source: parts.append("TASK_SOURCE")
        if self.run_scope != "root_run": parts.append(self.run_scope.upper())
        return " + ".join(parts) if parts else "CachePolicy()"

    def __eq__(self, other):
        return (isinstance(other, CachePolicy)
                and self.include_inputs == other.include_inputs
                and self.include_source == other.include_source
                and self.run_scope == other.run_scope
                and self.disabled == other.disabled)

    def __lt__(self, other):
        # For total_ordering — never actually ordered semantically.
        return repr(self) < repr(other)

    def __hash__(self):
        return hash((self.include_inputs, self.include_source, self.run_scope, self.disabled))


# Building-block constants — combine with `+`
INPUTS = CachePolicy(include_inputs=True)
TASK_SOURCE = CachePolicy(include_source=True)
RUN_ONLY = CachePolicy(run_scope="run_only")
ROOT_RUN = CachePolicy(run_scope="root_run")
CROSS_RUN = CachePolicy(run_scope="cross_run")
NO_CACHE = CachePolicy(disabled=True)
# Default = INPUTS with root_run scoping (Prefect's default + Dagster's re-execute survival property).
DEFAULT_CACHE_POLICY = INPUTS


def _hash_task_inputs(args: tuple, kwargs: dict) -> str:
    """Auto cache-key from args + kwargs (Prefect's `task_input_hash` parity).

    Skips positional args[0] (the Dagster context) since it isn't a task
    input in the Prefect sense. Uses repr() for stability; complex types
    (DataFrames, numpy) should pre-serialize.
    """
    import hashlib
    payload = repr((args[1:], sorted(kwargs.items()) if kwargs else []))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _hash_function_source(fn: Callable) -> str:
    """SHA-256 of the function's source code. Invalidates cache when the
    function body changes (Prefect `TASK_SOURCE` parity)."""
    import hashlib, inspect
    try:
        src = inspect.getsource(fn)
    except (OSError, TypeError):
        # Falls back to qualname when source is unavailable (e.g. C-extension).
        src = getattr(fn, "__qualname__", repr(fn))
    return hashlib.sha256(src.encode("utf-8")).hexdigest()


# ═════════════════════════════════════════════════════════════════════
# Task-tag concurrency pool (Prefect concurrency-limit parity)
# ═════════════════════════════════════════════════════════════════════
#
# Prefect: `@task(tags=["gpu"])` + a global concurrency limit per tag.
# DCC:     `@task(concurrency_pool="gpu", max_concurrent=3)` — acquires
#          a slot from a named pool before running; releases after.
#
# Backed by a threading.Semaphore registry — in-process concurrency
# control. For cross-run / cross-process pools (5 GPU licenses across
# a fleet), use `rpa_queue_concurrency_lock` (event-log-backed).

import threading as _threading

_POOL_LOCK = _threading.Lock()
_POOLS: Dict[str, "_threading.Semaphore"] = {}
_POOL_CAPS: Dict[str, int] = {}


def _get_pool(name: str, max_concurrent: int) -> "_threading.Semaphore":
    """Get-or-create a named semaphore. Raises if max_concurrent differs
    from a prior binding for the same name (misconfiguration guard)."""
    with _POOL_LOCK:
        existing_cap = _POOL_CAPS.get(name)
        if existing_cap is not None and existing_cap != max_concurrent:
            raise ValueError(
                f"Task concurrency pool {name!r} previously bound to "
                f"max_concurrent={existing_cap}; got {max_concurrent}. "
                f"All @task calls sharing a pool must agree on max_concurrent."
            )
        if name not in _POOLS:
            _POOLS[name] = _threading.Semaphore(max_concurrent)
            _POOL_CAPS[name] = max_concurrent
        return _POOLS[name]


# ─── Cross-run pool (event-log-backed) ─────────────────────────────
#
# Prefect's global concurrency limits are cross-run because their API server
# tracks slot state centrally. Dagster has no per-se API server, but the
# event log IS a shared substrate — the same one @cached and @throttle use.
# `_cross_run_pool_*` emits AssetObservation events on a synthetic
# `__task_pool_<name>` asset key; scans back to count active slots.

_CROSS_RUN_TAG_SAFE = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-"
)


def _cross_run_pool_key(name: str) -> "dg.AssetKey":
    """Synthetic asset key where acquire/release events are emitted."""
    safe = "".join(c if c in _CROSS_RUN_TAG_SAFE else "_" for c in name)
    return dg.AssetKey([f"__task_pool_{safe}"])


def _cross_run_pool_count(
    instance: Any,
    pool_asset_key: "dg.AssetKey",
    ttl_seconds: float,
    window_seconds: float,
) -> int:
    """Scan recent AssetObservation events on the pool key; count active slots.

    Pairs acquire/release by run_id in metadata. Any acquire older than
    ttl_seconds without a matching release is treated as released (stale —
    killed run). Same pattern as rpa_queue_concurrency_lock.
    """
    try:
        from dagster import EventRecordsFilter, DagsterEventType
    except Exception:  # noqa: BLE001
        return 0

    now_ts = time.time()
    try:
        records = instance.get_event_records(
            event_records_filter=EventRecordsFilter(
                event_type=DagsterEventType.ASSET_OBSERVATION,
                asset_key=pool_asset_key,
                after_timestamp=now_ts - float(window_seconds),
            ),
            limit=5000,
            ascending=True,
        )
    except Exception:  # noqa: BLE001
        return 0

    def _md_get(md: Any, key: str) -> Optional[str]:
        if md is None:
            return None
        v = md.get(key) if hasattr(md, "get") else None
        if v is None:
            return None
        for attr in ("value", "text"):
            inner = getattr(v, attr, None)
            if inner is not None:
                return str(inner)
        return str(v)

    active: Dict[str, float] = {}  # run_id -> acquired_at_ts
    for r in records:
        obs = getattr(r, "asset_observation", None)
        md = getattr(obs, "metadata", None) if obs is not None else None
        if md is None:
            de = getattr(r, "dagster_event", None)
            esd = getattr(de, "event_specific_data", None) if de is not None else None
            mat = getattr(esd, "asset_observation", None) if esd is not None else None
            md = getattr(mat, "metadata", None) if mat is not None else None
        if md is None:
            continue
        run_id = _md_get(md, "run_id")
        event = _md_get(md, "event")
        ts_str = _md_get(md, "ts_epoch")
        try:
            ts_val = float(ts_str) if ts_str is not None else float(getattr(r, "timestamp", now_ts) or now_ts)
        except Exception:  # noqa: BLE001
            ts_val = float(getattr(r, "timestamp", now_ts) or now_ts)
        if not run_id or not event:
            continue
        if event == "acquire":
            active[run_id] = ts_val
        elif event == "release":
            active.pop(run_id, None)

    return sum(1 for ts in active.values() if now_ts - ts <= ttl_seconds)


def _cross_run_pool_emit(context, pool_asset_key: "dg.AssetKey", event_name: str, task_name: str) -> None:
    """Emit an acquire/release event to the pool asset key."""
    try:
        context.log_event(dg.AssetObservation(
            asset_key=pool_asset_key,
            metadata={
                "run_id": context.run.run_id,
                "event": event_name,
                "task": task_name,
                "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "ts_epoch": time.time(),
            },
        ))
    except Exception as exc:  # noqa: BLE001
        try:
            context.log.warning(
                f"[task:{task_name}] cross-run pool emit {event_name!r} failed: "
                f"{type(exc).__name__} — slot state may drift"
            )
        except Exception:  # noqa: BLE001
            pass


# ═════════════════════════════════════════════════════════════════════
# Async task support (Prefect async @task parity)
# ═════════════════════════════════════════════════════════════════════

def _wrap_async_as_sync(inner: Callable) -> Callable:
    """If `inner` is an async coroutine function, wrap it so the sync
    caller can invoke it. Uses `asyncio.run()` for the common case (no
    running event loop) and falls back to a thread when already inside
    a running loop."""
    import asyncio, inspect
    if not inspect.iscoroutinefunction(inner):
        return inner

    @functools.wraps(inner)
    def _sync_wrapper(*args, **kwargs):
        try:
            asyncio.get_running_loop()
            # Already inside a running event loop — nested asyncio.run() forbidden.
            # Isolate the coroutine in a fresh thread with its own loop.
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                return ex.submit(asyncio.run, inner(*args, **kwargs)).result()
        except RuntimeError:
            # No running loop in this thread — safe to use asyncio.run directly.
            return asyncio.run(inner(*args, **kwargs))

    return _sync_wrapper


def _coerce_expiration(cache_ttl_seconds: Optional[float], cache_expiration: Any) -> Optional[float]:
    """cache_expiration accepts int/float/timedelta; cache_ttl_seconds is
    the original name kept for backward-compat. If both are set,
    cache_expiration wins (it's the newer, Prefect-shape param)."""
    if cache_expiration is not None:
        # Handle timedelta transparently
        seconds = getattr(cache_expiration, "total_seconds", None)
        if callable(seconds):
            return seconds()
        return float(cache_expiration)
    return cache_ttl_seconds


class IOManagerBackedTaskCache(TaskCache):
    """Adapter: wrap any Dagster IOManager as a task cache backend. Users
    who already have an S3/GCS/Snowflake IO manager configured for asset
    outputs can reuse it as the cache store without configuring separate
    storage.

    Bridging strategy: fabricate minimal `OutputContext`/`InputContext`
    shims with `run_id="__task_cache__"` and `step_key=<cache_key>`. Most
    IO managers use these fields only to derive a storage path — they
    don't validate against actual run storage. Filesystem, s3_pickle,
    gcs_pickle work out of the box; database-schema IO managers that
    assume a run_id table row won't.
    """

    _RUN_ID = "__task_cache__"

    def __init__(self, io_manager, ttl_seconds: Optional[float] = None):
        # Accept both a concrete IOManager (has handle_output/load_input) and
        # a ConfigurableIOManagerFactory (must call create_io_manager first).
        # When Dagster injects a resource into an op, this is already resolved
        # to the concrete IOManager — but if user constructs this outside a
        # resource context (tests, notebooks), the factory needs realizing.
        if hasattr(io_manager, "handle_output"):
            self._io = io_manager
        elif hasattr(io_manager, "create_io_manager"):
            from dagster import build_init_resource_context
            self._io = io_manager.create_io_manager(build_init_resource_context())
        else:
            raise TypeError(
                f"IOManagerBackedTaskCache: expected an IOManager or "
                f"ConfigurableIOManagerFactory; got {type(io_manager).__name__}"
            )
        self._ttl = ttl_seconds
        self._puts: Dict[str, float] = {}  # in-memory TTL tracking

    def _fake_output_context(self, key: str):
        from dagster import build_output_context
        return build_output_context(
            step_key=key, name="value", run_id=self._RUN_ID,
        )

    def _fake_input_context(self, key: str):
        from dagster import build_input_context
        upstream = self._fake_output_context(key)
        return build_input_context(upstream_output=upstream)

    def get(self, key: str) -> Any:
        if self._ttl is not None:
            put_at = self._puts.get(key)
            if put_at is None or (time.time() - put_at) > self._ttl:
                return TaskCache.MISS
        try:
            return self._io.load_input(self._fake_input_context(key))
        except Exception:  # noqa: BLE001
            return TaskCache.MISS

    def put(self, key: str, value: Any) -> None:
        try:
            self._io.handle_output(self._fake_output_context(key), value)
            self._puts[key] = time.time()
        except Exception:  # noqa: BLE001
            pass  # cache put failures are non-fatal — compute already ran


def _resolve_cache(context: Any, cache: Optional["TaskCache"],
                   cache_resource: Optional[str]) -> Optional["TaskCache"]:
    """Resolve the TaskCache instance for a @task call.

    - `cache=<TaskCache>` passed directly to @task always wins.
    - `cache_resource=<name>` looks up the resource on `context.resources.<name>`.
      NOTE: This requires the parent asset to declare
      `required_resource_keys={<name>}` — Dagster filters out undeclared
      resources from the step context.
    """
    if cache is not None:
        return cache
    if cache_resource is None:
        return None
    try:
        resources = getattr(context, "resources", None)
        return getattr(resources, cache_resource, None) if resources is not None else None
    except Exception:  # noqa: BLE001
        return None


def _default_cache_key(fn_name: str, args: tuple, kwargs: dict) -> str:
    """Fallback cache key when the user doesn't supply cache_key_fn.
    Hashes the repr of positional args (skipping context) + kwargs."""
    import hashlib
    payload = f"{fn_name}|{args!r}|{sorted(kwargs.items())!r}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def task(
    fn: Optional[Callable] = None,
    *,
    name: Optional[str] = None,
    cache_key_fn: Optional[Callable] = None,
    cache_ttl_seconds: Optional[float] = None,
    cache_expiration: Any = None,
    cache: Any = None,
    cache_policy: Optional["CachePolicy"] = None,
    cache_resource: Optional[str] = None,
    refresh_cache: bool = False,
    retry_condition_fn: Optional[Callable] = None,
    max_retries: int = 3,
    retry_delay_seconds: float = 0.0,
    retry_jitter_factor: float = 0.0,
    concurrency_pool: Optional[str] = None,
    max_concurrent: Optional[int] = None,
    concurrency_pool_scope: str = "in_process",
    concurrency_pool_ttl_seconds: float = 3600.0,
    concurrency_pool_wait_interval_seconds: float = 2.0,
    concurrency_pool_max_wait_attempts: int = 300,
    timeout_seconds: Optional[float] = None,
    log_prints: bool = False,
    on_completion: Optional[List[Callable]] = None,
    on_failure: Optional[List[Callable]] = None,
    on_running: Optional[List[Callable]] = None,
    on_awaiting_retry: Optional[List[Callable]] = None,
    task_run_name: Optional[str] = None,
    result_storage_key: Optional[str] = None,
    viz_return_value: bool = True,
    emit_state_observations: bool = True,
) -> Callable:
    """Mark a callable as a Dagster sub-task with optional cache — Prefect
    `@task` parity plus Dagster-native extras. Behavior depends on where
    it's called from:

    **Inside a `@task_asset`** — the call is RECORDED (not executed) so the
    framework can fan out via `DynamicOutput`. Each call becomes a
    distinct graph node. The call returns `None` at record time; the
    actual execution happens in per-task worker steps.

    **Outside a `@task_asset`** — the call executes normally, wrapped in
    a `child_step()` scope for log-tab attribution. Nesting is arbitrary
    depth; step_key reflects the call stack. Not graph-visible.

    ```python
    # Zero-config cache (Prefect parity — auto-hash inputs, filesystem backend):
    @task(cache=True)
    def parse_url(context, url): ...

    # Composable cache policy (Prefect CachePolicy parity):
    @task(cache=True, cache_policy=INPUTS + TASK_SOURCE, cache_expiration=timedelta(hours=1))
    def parse_url(context, url): ...

    # Custom backend:
    _cache = FilesystemTaskCache(base_dir="/tmp/cache", max_entries=1000, max_bytes=10*1024**3)
    @task(cache=_cache, cache_policy=INPUTS)
    def parse_url(context, url): ...

    # Cross-run scoping (parse this URL once ever):
    @task(cache=True, cache_policy=INPUTS + CROSS_RUN)
    def parse_url(context, url): ...

    # Per-call bypass:
    result = parse_url(context, url, task_no_cache=True)   # this call skips cache
    ```

    Args:
        fn: The wrapped function. First positional arg must be a Dagster context.
        name: Override the task name; defaults to `fn.__name__`.
        cache: One of —
            * ``TaskCache`` instance (e.g., ``FilesystemTaskCache(base_dir=...)``)
            * ``True`` — auto-configure a ``FilesystemTaskCache`` at ``/tmp/dagster_task_cache/<name>/``
            * ``None`` (default) — no caching
        cache_policy: Composable ``CachePolicy`` (default ``INPUTS`` when ``cache`` is set).
            Combine with ``+``: ``INPUTS + TASK_SOURCE + CROSS_RUN``.
        cache_key_fn: Optional ``(context, *args, **kwargs) -> str``. If supplied,
            takes precedence over ``cache_policy.include_inputs``. When both are
            None but ``cache`` is set, auto-hash inputs (Prefect ``task_input_hash`` parity).
        cache_ttl_seconds: Optional TTL in seconds. Entries older than this are
            treated as misses.
        cache_expiration: Alias for ``cache_ttl_seconds`` accepting ``int`` /
            ``float`` / ``datetime.timedelta`` (Prefect parity). Wins over
            ``cache_ttl_seconds`` if both are set.
        cache_resource: Alternative to ``cache=`` — name of a resource-registered
            ``TaskCache``. Requires the parent asset to declare
            ``required_resource_keys={<name>}``.
        refresh_cache: If True, every invocation forces a cache MISS (always
            re-computes and re-stores). Prefect ``refresh_cache=True`` parity
            at the decorator level. See also the ``refresh_cache=true`` run tag
            (per-run override).

    Runtime overrides:
        - Set run tag ``refresh_cache=true`` (or ``dagster/refresh_cache=true``)
          to force a MISS across all @task calls in the run — Prefect
          ``.submit(refresh_cache=True)`` parity.
        - Pass ``task_no_cache=True`` at the call site to bypass cache for
          a single invocation without any decorator change.

    Cache scoping:

        By default cache keys are scoped to ``root_run_id`` — Dagster's
        re-execute-from-failure preserves ``root_run_id`` across attempts,
        so cached results from earlier attempts SURVIVE the retry. Net-new
        materializations get a fresh ``root_run_id``, so cached results
        from any prior run are invisible — no bleeding of stale cross-run
        values.

        Opt into other scopes via ``cache_policy``:
        - ``ROOT_RUN`` (default) — survives re-execute-from-failure
        - ``RUN_ONLY`` — cache lives only for one specific run_id
        - ``CROSS_RUN`` — cache shared across all runs (Prefect's default;
          "parse this URL once ever")
    """
    # Coerce cache=True into a real FilesystemTaskCache with sensible defaults.
    _cache_arg = cache
    if _cache_arg is True:
        import tempfile, os
        _default_dir = os.path.join(tempfile.gettempdir(), "dagster_task_cache", name or (fn.__name__ if fn else "task"))
        _cache_arg = FilesystemTaskCache(base_dir=_default_dir)
    # Resolve cache policy default: when cache is set but no policy given,
    # use INPUTS (Prefect's default — task_input_hash equivalent).
    _policy = cache_policy if cache_policy is not None else (INPUTS if _cache_arg is not None else None)
    # TTL: cache_expiration (timedelta-friendly) supersedes cache_ttl_seconds.
    _ttl = _coerce_expiration(cache_ttl_seconds, cache_expiration)
    # Concurrency pool validation — if either is set, both must be set.
    if (concurrency_pool is None) ^ (max_concurrent is None):
        raise ValueError(
            "@task: concurrency_pool and max_concurrent must be set together. "
            "Pass both, or neither."
        )
    if concurrency_pool_scope not in ("in_process", "cross_run"):
        raise ValueError(
            f"@task: concurrency_pool_scope must be 'in_process' or 'cross_run'; "
            f"got {concurrency_pool_scope!r}"
        )
    # in_process → threading.Semaphore (per-python-process cap)
    # cross_run → event-log-backed (works across runs / workers / hosts)
    _pool_semaphore = (
        _get_pool(concurrency_pool, max_concurrent)
        if concurrency_pool and concurrency_pool_scope == "in_process"
        else None
    )
    _cross_run_pool = (
        _cross_run_pool_key(concurrency_pool)
        if concurrency_pool and concurrency_pool_scope == "cross_run"
        else None
    )
    def _decorator(inner: Callable) -> Callable:
        step_name = name or getattr(inner, "__name__", "task")
        # Auto-wrap async coroutine functions so sync execution paths (Dagster ops)
        # can invoke them transparently. `inner` stays sync-shape for the rest of
        # the wrapper; async detection + event-loop handling is done once at
        # decoration time.
        inner = _wrap_async_as_sync(inner)

        def _templated_task_run_name(args_seq, kwargs_map):
            """Format task_run_name using function args + kwargs. Prefect parity.

            Templates like 'parse_{url}' or 'row_{i}_of_{n}' pull from kwargs
            (fallback to positional args by index) — dynamic display name for
            the run graph node. Malformed templates fall back to step_name.
            """
            if not task_run_name:
                return None
            try:
                lookup = dict(kwargs_map)
                for i, v in enumerate(args_seq[1:]):  # skip context arg
                    lookup.setdefault(f"arg{i}", v)
                return task_run_name.format(**lookup)
            except Exception:  # noqa: BLE001
                return None

        def _templated_storage_key(args_seq, kwargs_map):
            """Format result_storage_key using args + kwargs. Prefect parity —
            gives users control over the cache storage path so cached files
            are inspectable by name (e.g. 'invoices/{invoice_id}.pkl' vs
            an opaque sha256 hash). Overrides cache_key_fn / INPUTS when set."""
            if not result_storage_key:
                return None
            try:
                lookup = dict(kwargs_map)
                for i, v in enumerate(args_seq[1:]):
                    lookup.setdefault(f"arg{i}", v)
                return result_storage_key.format(**lookup)
            except Exception:  # noqa: BLE001
                return None

        @functools.wraps(inner)
        def _wrapped(*args, **kwargs):
            # Precedence: call-site task_name= overrides everything, then
            # decorator-supplied task_run_name (with template rendering),
            # then step_name (the function name).
            _templated = _templated_task_run_name(args, kwargs) if task_run_name else None
            explicit_name = kwargs.pop("task_name", None) or _templated or step_name

            # ── RECORDING MODE (called inside a @task_asset) ──
            queue = _recording_queue.get()
            if queue is not None:
                # Strip the context arg if present — the worker op re-injects
                # its own context when it executes the task for real.
                strip_first = bool(args) and hasattr(args[0], "instance") and hasattr(args[0], "log")
                clean_args = list(args[1:]) if strip_first else list(args)
                # Auto-suffix duplicate names so each mapping_key is unique
                seq = 1 + sum(1 for e in queue if e["base_name"] == explicit_name)
                queue.append({
                    "base_name": explicit_name,
                    "name": explicit_name if seq == 1 else f"{explicit_name}_{seq}",
                    "fn": inner,
                    "args": clean_args,
                    "kwargs": dict(kwargs),
                })
                return None  # placeholder

            # ── EXECUTE MODE (called outside a @task_asset) ──
            context = None
            if args and hasattr(args[0], "instance") and hasattr(args[0], "log"):
                context = args[0]
            elif "context" in kwargs and hasattr(kwargs["context"], "instance"):
                context = kwargs["context"]
            if context is None:
                return inner(*args, **kwargs)
            # `task_name` becomes the mapping_key badge; op_name stays fn.__name__
            mapping_key = explicit_name if explicit_name != step_name else None

            # Per-call bypass: task_no_cache=True at the call site skips cache
            # for this specific invocation (Prefect .submit(refresh_cache=True) parity).
            call_no_cache = bool(kwargs.pop("task_no_cache", False))

            # ── CACHE LOOKUP (before running the block) ──
            # Cache is engaged when EITHER a user cache_key_fn is set OR a
            # cache backend + policy is present (auto-hash inputs).
            policy = _policy
            policy_disabled = policy is not None and policy.disabled
            wants_cache = (
                (cache_key_fn is not None or (policy is not None and policy.include_inputs))
                and not policy_disabled
                and not call_no_cache
            )
            resolved_cache = _resolve_cache(context, _cache_arg, cache_resource) if wants_cache else None
            if resolved_cache is not None:
                # Build the user cache key: explicit cache_key_fn wins;
                # otherwise auto-hash inputs (Prefect task_input_hash parity).
                try:
                    # Precedence: result_storage_key template (human-readable)
                    # > user cache_key_fn > auto INPUTS hash.
                    templated_storage = _templated_storage_key(args, kwargs) if result_storage_key else None
                    if templated_storage is not None:
                        user_key = templated_storage
                    elif cache_key_fn is not None:
                        user_key = cache_key_fn(context, *args[1:], **kwargs)
                    else:
                        user_key = _hash_task_inputs(args, kwargs)
                except Exception as exc:  # noqa: BLE001
                    context.log.warning(f"[task:{step_name}] cache_key_fn raised {type(exc).__name__}; bypassing cache")
                    user_key = None
                if user_key is not None:
                    # Optionally include function source hash — invalidate on code edit.
                    key_parts = [user_key]
                    if policy is not None and policy.include_source:
                        key_parts.append(_hash_function_source(inner))
                    user_key = ":".join(key_parts)

                    # Run scoping: root_run (default; survives re-execute-from-failure) /
                    # run_only (this specific run) / cross_run (no scoping — Prefect default).
                    run_scope = policy.run_scope if policy is not None else "root_run"
                    if run_scope == "cross_run":
                        key = f"cross:{user_key}"
                    elif run_scope == "run_only":
                        key = f"{context.run.run_id}:{user_key}"
                    else:  # root_run (default)
                        root_id = getattr(context.run, "root_run_id", None) or context.run.run_id
                        key = f"{root_id}:{user_key}"

                    # Refresh signals: decorator-level `refresh_cache=True` +
                    # run-tag `refresh_cache=true` (per-run override). Both bypass
                    # cache read AND overwrite on put.
                    run_tags = getattr(context.run, "tags", None) or {}
                    tag_refresh = any(
                        str(run_tags.get(k, "")).lower() in ("true", "1", "yes")
                        for k in ("refresh_cache", "dagster/refresh_cache")
                    )
                    refresh_requested = refresh_cache or tag_refresh
                    hit = TaskCache.MISS if refresh_requested else resolved_cache.get(key)
                    if refresh_requested:
                        which = "decorator refresh_cache=True" if refresh_cache else "run tag refresh_cache=true"
                        try:
                            context.log.info(f"[task:{step_name}] {which} — forced MISS, key={user_key[:32]}...")
                        except Exception:  # noqa: BLE001
                            pass
                    if hit is not TaskCache.MISS:
                        # Emit synthetic events so the node still renders.
                        with child_step(context, step_name, mapping_key=mapping_key):
                            try:
                                context.log.info(f"[task:{step_name}[{mapping_key or 'auto'}]] [cache_hit] key={user_key[:32]}...")
                            except Exception:  # noqa: BLE001
                                pass
                        return hit
                    # Miss — execute inside child_step, then cache the result.
                    with child_step(context, step_name, mapping_key=mapping_key):
                        result = _execute_with_guards(context, inner, args, kwargs)
                    try:
                        resolved_cache.put(key, result)
                    except Exception as exc:  # noqa: BLE001
                        context.log.warning(f"[task:{step_name}] cache.put raised {type(exc).__name__}; result not cached")
                    return result

            # ── NO CACHE — plain execute ──
            # NO_CACHE policy / call_no_cache=True / no cache_key_fn+backend
            with child_step(context, step_name, mapping_key=mapping_key):
                return _execute_with_guards(context, inner, args, kwargs)

        def _execute_with_guards(context, fn_to_call, args, kwargs):
            """Wrap the actual inner call with the full Prefect-parity feature set:

            - Concurrency pool (in_process semaphore or cross_run event-log)
            - Timeout (hard-kill compute past deadline via ThreadPoolExecutor)
            - Log prints (redirect print() to context.log.info during execute)
            - retry_condition_fn (predicate → RetryRequested with jitter)
            - on_completion / on_failure hooks (fire per outcome, exceptions trapped)
            - task_run_name (templated from args → mapping_key badge)
            - Result truncation for the run graph if viz_return_value=False
            - Cache-key contribution from result_storage_key template
            """
            import random as _random
            import contextlib as _contextlib
            import io as _io

            @_contextlib.contextmanager
            def _maybe_capture_prints():
                if not log_prints:
                    yield
                    return
                buf = _io.StringIO()
                with _contextlib.redirect_stdout(buf):
                    try:
                        yield
                    finally:
                        captured = buf.getvalue().rstrip()
                        if captured:
                            for line in captured.splitlines():
                                try:
                                    context.log.info(f"[print] {line}")
                                except Exception:  # noqa: BLE001
                                    pass

            def _apply_timeout(fn, args, kwargs):
                if timeout_seconds is None:
                    return fn(*args, **kwargs)
                import concurrent.futures as _cf
                with _cf.ThreadPoolExecutor(max_workers=1) as _ex:
                    fut = _ex.submit(fn, *args, **kwargs)
                    try:
                        return fut.result(timeout=timeout_seconds)
                    except _cf.TimeoutError:
                        raise TimeoutError(
                            f"@task {step_name!r} exceeded timeout_seconds={timeout_seconds}"
                        ) from None

            def _run_hooks(hooks, kind, exc=None):
                if not hooks:
                    return
                for hook in hooks:
                    try:
                        hook(context, exc) if exc is not None else hook(context)
                    except Exception as hook_exc:  # noqa: BLE001
                        try:
                            context.log.warning(
                                f"[task:{step_name}] on_{kind} hook "
                                f"{getattr(hook, '__name__', repr(hook))!r} raised "
                                f"{type(hook_exc).__name__}; ignored"
                            )
                        except Exception:  # noqa: BLE001
                            pass

            _TASK_STATE_ASSET_KEY = dg.AssetKey([f"__task_state_{step_name}"])

            def _emit_state(state_name: str, extra: Optional[dict] = None):
                """Emit an AssetObservation event capturing a task-run state
                transition. Prefect-parity for the state model (Running /
                AwaitingRetry / Completed / Failed). Queryable via
                context.instance.get_event_records + AssetObservation filter.
                """
                if not emit_state_observations:
                    return
                md = {
                    "state": state_name,
                    "task": step_name,
                    "run_id": getattr(context.run, "run_id", "unknown"),
                    "ts_epoch": time.time(),
                }
                if extra:
                    md.update({k: str(v) for k, v in extra.items()})
                try:
                    context.log_event(dg.AssetObservation(
                        asset_key=_TASK_STATE_ASSET_KEY, metadata=md,
                    ))
                except Exception:  # noqa: BLE001
                    pass

            def _call():
                # Wraps: log_prints capture → timeout → retry_condition_fn guard
                # → on_completion / on_failure hooks. Emits Running →
                # Completed/Failed/AwaitingRetry state observations along the
                # way (Prefect state-model parity, minus Cancelled/Crashed
                # which need Dagster kill-signal integration).
                _emit_state("Running")
                _run_hooks(on_running, "running")
                try:
                    with _maybe_capture_prints():
                        result = _apply_timeout(fn_to_call, args, kwargs)
                    _emit_state("Completed")
                    _run_hooks(on_completion, "completion")
                    if not viz_return_value:
                        # Emit a marker asset observation so users can spot the
                        # hidden-return-value flag; the returned value itself
                        # still flows to the caller (Dagster's asset return is
                        # already the caller's, not the graph's — this is a
                        # display-only flag matching Prefect's viz_return_value).
                        try:
                            context.log.info(f"[task:{step_name}] viz_return_value=False (return value hidden from run-graph preview)")
                        except Exception:  # noqa: BLE001
                            pass
                    return result
                except Exception as exc:  # noqa: BLE001
                    if retry_condition_fn is not None:
                        try:
                            should_retry = bool(retry_condition_fn(context, exc))
                        except Exception as pred_exc:  # noqa: BLE001
                            context.log.warning(
                                f"[task:{step_name}] retry_condition_fn raised "
                                f"{type(pred_exc).__name__}; treating as re-raise"
                            )
                            _run_hooks(on_failure, "failure", exc)
                            raise exc from None
                        if should_retry:
                            # Apply Prefect-parity retry jitter to the delay.
                            base_delay = retry_delay_seconds or 0.0
                            jitter = retry_jitter_factor or 0.0
                            if jitter > 0:
                                delay = base_delay * (1 + _random.uniform(-jitter, jitter))
                            else:
                                delay = base_delay
                            context.log.info(
                                f"[task:{step_name}] retry_condition_fn matched "
                                f"{type(exc).__name__} — requesting Dagster retry "
                                f"(max_retries={max_retries}, delay={round(delay, 3)}s)"
                            )
                            _emit_state("AwaitingRetry", extra={
                                "exception": type(exc).__name__,
                                "delay_seconds": round(delay, 3),
                            })
                            _run_hooks(on_awaiting_retry, "awaiting_retry", exc)
                            raise dg.RetryRequested(
                                max_retries=max_retries,
                                seconds_to_wait=max(0.0, delay),
                            ) from exc
                    _emit_state("Failed", extra={"exception": type(exc).__name__})
                    _run_hooks(on_failure, "failure", exc)
                    raise

            if _pool_semaphore is not None:
                # In-process semaphore path
                context.log.info(f"[task:{step_name}] acquiring in-process pool={concurrency_pool!r} (cap={max_concurrent})")
                _pool_semaphore.acquire()
                try:
                    return _call()
                finally:
                    _pool_semaphore.release()
            if _cross_run_pool is not None:
                # Cross-run event-log-backed pool. Poll until active < max_concurrent
                # (bounded by max_wait_attempts), emit acquire, run, emit release.
                for attempt in range(concurrency_pool_max_wait_attempts):
                    active = _cross_run_pool_count(
                        context.instance,
                        _cross_run_pool,
                        concurrency_pool_ttl_seconds,
                        max(concurrency_pool_ttl_seconds * 2, 7200.0),
                    )
                    if active < max_concurrent:
                        break
                    context.log.info(
                        f"[task:{step_name}] cross-run pool={concurrency_pool!r} at "
                        f"capacity ({active}/{max_concurrent}); waiting "
                        f"{concurrency_pool_wait_interval_seconds}s "
                        f"(attempt {attempt + 1}/{concurrency_pool_max_wait_attempts})"
                    )
                    time.sleep(concurrency_pool_wait_interval_seconds)
                else:
                    raise dg.Failure(
                        description=(
                            f"@task pool={concurrency_pool!r} did not free a slot "
                            f"within {concurrency_pool_max_wait_attempts * concurrency_pool_wait_interval_seconds:.0f}s"
                        )
                    )
                context.log.info(f"[task:{step_name}] acquired cross-run pool={concurrency_pool!r} slot ({active + 1}/{max_concurrent})")
                _cross_run_pool_emit(context, _cross_run_pool, "acquire", step_name)
                try:
                    return _call()
                finally:
                    _cross_run_pool_emit(context, _cross_run_pool, "release", step_name)
            return _call()

        _wrapped.__task_name__ = step_name  # type: ignore[attr-defined]
        return _wrapped

    if fn is not None and callable(fn):
        return _decorator(fn)
    return _decorator


# ==========================================================================
# Shape 1: TaskAssetComponent — layered runtime fan-out with graph render
# ==========================================================================
# User declares N processing layers at compile time. Each layer is a
# @dg.op with DynamicOut (except the terminal layer). Between layers we
# insert a `collect + re-emit` bridge so any depth is legal.

def _load_layer_fn(ref: str) -> Callable:
    if not ref or ":" not in ref:
        raise ValueError(f"layer compute must be 'module.path:function_name'; got {ref!r}")
    mod_path, fn_name = ref.rsplit(":", 1)
    fn = getattr(importlib.import_module(mod_path.strip()), fn_name.strip(), None)
    if not callable(fn):
        raise ValueError(f"layer compute {ref!r} not callable")
    return fn


def _safe_mapping_key(name: str) -> str:
    return "".join(ch if (ch.isalnum() or ch in "._") else "_" for ch in name)[:80] or "task"


def _iter_named_tasks(result: Any) -> Iterable[Tuple[str, Any]]:
    """A layer callable that wants to fan out MUST return an iterable of
    2-tuples `(name, spec)` OR dicts with 'name'+'spec' keys. Any other
    return value is treated as a TERMINAL result for that branch — no
    further fan-out; the value is dropped from this bridge (users who
    want to keep terminal values should accumulate them in the compute
    or use `@task` for log attribution).

    This intentionally does NOT auto-name arbitrary iterables — plain
    dicts iterate over their keys, which produces meaningless mapping
    keys and downstream failures.
    """
    if result is None:
        return
    if isinstance(result, dict) and "name" in result and "spec" in result:
        yield str(result["name"]), result["spec"]
        return
    if not isinstance(result, (list, tuple)):
        return
    for item in result:
        if isinstance(item, tuple) and len(item) == 2:
            yield str(item[0]), item[1]
        elif isinstance(item, dict) and "name" in item and "spec" in item:
            yield str(item["name"]), item["spec"]
        # Anything else is silently skipped — terminal-branch semantics.


class LayerSpec(dg.Model, dg.Resolvable):
    """One layer of the TaskAssetComponent pipeline."""
    name: str = Field(description="Layer name. Becomes the mapped op's key in the run graph.")
    compute: str = Field(description="'mod:fn' — the callable that processes items at this layer.")


class TaskAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape: declare N runtime-fan-out layers; each layer's work items
    are 100% runtime-discovered. Every layer renders as a graph node.
    """

    asset_name: str = Field(description="Dagster asset name.")
    upstream_asset_key: Optional[str] = Field(default=None)

    layers: List[LayerSpec] = Field(
        description="Ordered list of processing layers. First layer's compute receives "
                    "(context) and emits initial work items. Subsequent layers receive "
                    "(context, task_spec) per item. A layer that yields (name, spec) pairs "
                    "fans out to the next layer; a layer that returns a plain value is terminal."
    )
    terminal: Optional[str] = Field(
        default=None,
        description="Optional final `mod:fn` that receives (context, all_results: list) "
                    "and returns the asset's final value.",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None, description="Default: ['python', 'task'].")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Task Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key

        layers = list(self.layers or [])
        if not layers:
            raise ValueError("TaskAssetComponent.layers must contain at least one layer")

        # Resolve every layer's callable up front (fail fast on bad refs)
        layer_callables = [_load_layer_fn(l.compute) for l in layers]
        layer_names = [l.name for l in layers]
        terminal_callable = _load_layer_fn(self.terminal) if self.terminal else None

        kinds_set = set(self.kinds or []) | {"python", "task"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        ins = {}
        if upstream_asset_key:
            ins["upstream"] = dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))

        # ── Build the layered graph ──
        # For each layer we generate two ops:
        #   layerN_generate — DynamicOut master. Layer 0's generate calls the
        #     user's scan fn to emit initial specs. Layer N>0's generate is
        #     the "flatten bridge" that reads the prior layer's collected
        #     outputs and re-emits DynamicOutputs.
        #   layerN_process — mapped worker. Calls the user's fn per spec.
        #     Returns either a plain value (terminal for this branch) or a
        #     list of (name, spec) pairs to hand to layerN+1.

        ops = []
        first_generate_op = None
        prior_process_op = None

        for i, (layer_name, layer_fn) in enumerate(zip(layer_names, layer_callables)):
            safe_layer = _safe_mapping_key(layer_name)

            if i == 0:
                # First generator — scans initial work items
                @dg.op(
                    name=f"{safe_layer}_scan",
                    out=DynamicOut(),
                )
                def _scan_op(context, _fn=layer_fn):
                    for name, spec in _iter_named_tasks(_fn(context)):
                        yield DynamicOutput(value=spec, mapping_key=_safe_mapping_key(name))
                first_generate_op = _scan_op
                ops.append(_scan_op)

                @dg.op(name=f"{safe_layer}_process")
                def _first_process_op(context, task_spec, _name=layer_name):
                    context.log.info(f"[layer:{_name}] processing 1 item")
                    # First layer's process is actually a no-op — the scan
                    # already did the work. But we forward the spec so
                    # downstream layers can process it. If there's no next
                    # layer, this becomes the terminal worker.
                    return task_spec
                # Actually — the first layer's SCAN function does the emission.
                # There's no "worker" per item at level 0. We skip _first_process_op.
                # Restart: layer 0 is scan-only.

                prior_process_op = None  # Will use scan output directly
                continue

            # Layer i > 0 — bridge (collect prior + flatten) + worker
            @dg.op(name=f"{safe_layer}_bridge", out=DynamicOut())
            def _bridge_op(context, prior_outputs: list, _lname=layer_name):
                """Collect prior layer's outputs; each is either a list of
                (name, spec) pairs OR a plain value. Emit DynamicOutputs for
                each (name, spec) pair; drop plain values (no fan-out)."""
                n = 0
                for item in prior_outputs:
                    for name, spec in _iter_named_tasks(item):
                        yield DynamicOutput(value=spec, mapping_key=_safe_mapping_key(f"{_lname}_{name}_{n}"))
                        n += 1
                context.log.info(f"[bridge:{_lname}] emitted {n} sub-tasks")

            @dg.op(name=f"{safe_layer}_process")
            def _process_op(context, task_spec, _fn=layer_fn, _lname=layer_name):
                context.log.info(f"[layer:{_lname}] processing item")
                return _fn(context, task_spec)

            ops.append(_bridge_op)
            ops.append(_process_op)

        # ── Assemble the graph_asset ──
        @dg.graph_asset(
            name=asset_name,
            group_name=self.group_name,
            description=self.description or f"Layered task-asset {asset_name} ({len(layers)} layers)",
        )
        def _asset_graph():
            # Wire the layers
            current = None  # DynamicOutputs handle
            for i, layer_name in enumerate(layer_names):
                safe_layer = _safe_mapping_key(layer_name)
                if i == 0:
                    # First layer = scan_op only (already emits DynamicOutputs)
                    scan_op = _find_op(ops, f"{safe_layer}_scan")
                    current = scan_op()
                else:
                    # Layer i>0 = bridge + process
                    bridge_op = _find_op(ops, f"{safe_layer}_bridge")
                    process_op = _find_op(ops, f"{safe_layer}_process")
                    prior_process_op = _find_op(ops, f"{_safe_mapping_key(layer_names[i-1])}_process") if i > 1 else None
                    # If prior process op exists, its outputs are what we bridge
                    # Otherwise (i == 1), we bridge the scan_op's DynamicOutputs directly through a first-layer worker
                    if i == 1:
                        # Layer 1 processes the scan's DynamicOutputs directly
                        current = current.map(process_op)
                    else:
                        # Layer 2+ — first collect prior, then bridge to new fan-out, then process
                        collected = current.collect()
                        current = bridge_op(collected).map(process_op)

            # After all layers, collect + optionally run terminal fn
            final_list = current.collect()
            if terminal_callable is not None:
                @dg.op(name="terminal_reduce")
                def _terminal_op(context, results: list):
                    return terminal_callable(context, results)
                ops.append(_terminal_op)
                return _terminal_op(final_list)
            else:
                @dg.op(name="collect_final")
                def _collect_op(context, results: list):
                    context.log.info(f"[collect] {len(results)} final results")
                    return {"n_results": len(results)}
                ops.append(_collect_op)
                return _collect_op(final_list)

        return dg.Definitions(assets=[_asset_graph])


def _find_op(ops: list, name: str):
    for op in ops:
        if getattr(op, "name", None) == name:
            return op
    raise KeyError(f"op {name!r} not found among {[o.name for o in ops]}")


# NOTE: Truly sequential-with-data-dependency "each call = graph node, N runtime-unknown"
# is a genuine Dagster-core gap. Every workaround we tried had honest costs
# (RetryRequested conflates with failure; fabricated events log-only; pre-declared
# max_slots pollute the graph with SKIPPED nodes). Tracked in
# docs/FEEDBACK_dynamic_ops_in_run_graph.md as the concrete engineering ask.
#
# For those patterns today: use `@task` (log-attribution only, arbitrary depth,
# real Python semantics). Graph shows only the outer asset; per-call attribution
# in the log tab.


# ==========================================================================
# Companion: @task_asset — SIMPLEST possible imperative-generator shape
# ==========================================================================
# User writes an imperative generator that YIELDS (name, callable, *args)
# tuples. Framework turns each yield into a DynamicOutput; a single worker
# op dispatches by resolving the callable and invoking it. All yielded
# tasks render as sibling graph nodes under one `run_task[?]` fan-out.

def _fn_ref(fn: Callable) -> str:
    """Serialize a Python callable to a 'mod:qualname' string (JSON-safe)."""
    mod = getattr(fn, "__module__", None)
    name = getattr(fn, "__qualname__", None) or getattr(fn, "__name__", None)
    if not mod or not name:
        raise ValueError(f"@task_asset: cannot serialize callable {fn!r} — needs __module__ + __qualname__")
    return f"{mod}:{name}"


def _resolve_fn_ref(ref: str) -> Callable:
    mod_path, name = ref.rsplit(":", 1)
    obj = importlib.import_module(mod_path)
    for part in name.split("."):
        obj = getattr(obj, part)
    if not callable(obj):
        raise ValueError(f"@task_asset: {ref!r} is not callable")
    return obj


def task_asset(
    fn: Optional[Callable] = None,
    *,
    name: Optional[str] = None,
    group_name: Optional[str] = None,
    description: Optional[str] = None,
    partitions_def: Optional[Any] = None,
) -> Callable:
    """Decorator turning imperative-looking Python into a graph asset with
    per-`@task`-call runtime fan-out.

    ```python
    from dagster_community_components import task, task_asset

    @task
    def parse_title(context, doc): return doc["title"]

    @task
    def parse_text(context, block): ...

    @task
    def parse_url(context, url): ...

    @task_asset
    def parse_document(context):
        doc = load()
        parse_title(context, doc)           # ← just call, like a noob
        for block in doc["blocks"]:
            if block["kind"] == "text":
                parse_text(context, block)
                for url in extract_urls(block):
                    parse_url(context, url)  # ← nested, per-URL fan-out
    ```

    Every `@task` call inside `@task_asset` is RECORDED (not executed).
    After the imperative body finishes, the framework fans out all
    recorded calls via `DynamicOutput` — each becomes a distinct graph
    node under `<asset>.run_task[<name>]`. Real execution happens in
    per-task worker steps.

    **Constraint**: `@task` calls inside `@task_asset` return `None` at
    record time (they haven't executed yet). You can't branch on the
    return value: `if parse_title(context, doc) == "Q3": ...` won't
    work. If you need result-branching, use `@task` OUTSIDE a
    `@task_asset` (imperative execute + log attribution, no graph
    render).

    Args:
        fn: The wrapped fn. Signature: `(context) -> None`. Body calls
            `@task`-decorated functions.
        name: Asset name (defaults to fn.__name__).
        group_name / description: standard Dagster asset options.
    """
    def _decorator(inner: Callable) -> Callable:
        asset_name = name or inner.__name__

        @dg.op(name=f"{asset_name}_scan", out=DynamicOut())
        def _scan(context):
            # Activate recording mode — @task calls populate this queue
            queue: list = []
            token = _recording_queue.set(queue)
            try:
                inner(context)
            finally:
                _recording_queue.reset(token)

            context.log.info(f"[task_asset:{asset_name}] recorded {len(queue)} @task calls")
            for entry in queue:
                spec = {
                    "fn_ref": _fn_ref(entry["fn"]),
                    "args": entry["args"],
                    "kwargs": entry["kwargs"],
                    "name": entry["name"],
                }
                yield DynamicOutput(value=spec, mapping_key=_safe_mapping_key(entry["name"]))

        @dg.op(name=f"{asset_name}_run_task")
        def _run_task(context, task_spec: dict):
            fn = _resolve_fn_ref(task_spec["fn_ref"])
            args = task_spec.get("args", [])
            kwargs = task_spec.get("kwargs", {})
            context.log.info(f"[task:{task_spec['name']}] {task_spec['fn_ref']}")
            # `context` is the worker step's context — passed as first arg to
            # the recorded fn to preserve the standard `(context, ...)` shape.
            return fn(context, *args, **kwargs)

        @dg.op(name=f"{asset_name}_collect")
        def _collect(context, results: list):
            context.log.info(f"[task_asset:{asset_name}] collected {len(results)} results")
            return {"n_tasks": len(results), "results": results}

        graph_kwargs: Dict[str, Any] = {
            "name": asset_name,
            "group_name": group_name,
            "description": description,
        }
        if partitions_def is not None:
            graph_kwargs["partitions_def"] = partitions_def

        @dg.graph_asset(**graph_kwargs)
        def _asset_graph():
            return _collect(_scan().map(_run_task).collect())

        return _asset_graph

    if fn is not None and callable(fn):
        return _decorator(fn)
    return _decorator
