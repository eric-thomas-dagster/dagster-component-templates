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
def child_step(context: Any, name: str):
    """Enter a child step scope. Emits STEP_START on entry; STEP_SUCCESS on
    normal exit; STEP_FAILURE on exception (and re-raises).

    ```python
    with child_step(context, "parse_title"):
        title = extract_title(doc)
    ```
    """
    seq = _next_seq()
    parent = _parent_step_key(context)
    safe = "".join(ch if (ch.isalnum() or ch in "._-") else "_" for ch in name)
    suffix = f"[task.{safe}.{seq}]"
    key = parent + suffix
    started = time.time()

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


def task(fn: Optional[Callable] = None, *, name: Optional[str] = None) -> Callable:
    """Mark a callable as a Dagster sub-task. Behavior depends on where
    it's called from:

    **Inside a `@task_asset`** — the call is RECORDED (not executed) so the
    framework can fan out via `DynamicOutput`. Each call becomes a
    distinct graph node. The call returns `None` at record time; the
    actual execution happens in per-task worker steps.

    **Outside a `@task_asset`** — the call executes normally, wrapped in
    a `child_step()` scope for log-tab attribution. Nesting is arbitrary
    depth; step_key reflects the call stack. Not graph-visible.

    ```python
    @task
    def parse_url(context, url): ...

    @task
    def parse_text(context, block):
        for url in extract_urls(block):
            parse_url(context, url)   # nested — graph fans out if inside @task_asset

    @task_asset
    def parse_document(context):
        doc = load()
        parse_title(context, doc)     # RECORDED → fans out as graph node
        for block in doc["blocks"]:
            if block["kind"] == "text":
                parse_text(context, block)   # RECORDED
    ```

    Args:
        fn: The wrapped function. First positional arg must be a Dagster context.
        name: Override the task name; defaults to `fn.__name__`.
    """
    def _decorator(inner: Callable) -> Callable:
        step_name = name or getattr(inner, "__name__", "task")

        @functools.wraps(inner)
        def _wrapped(*args, **kwargs):
            explicit_name = kwargs.pop("task_name", None) or step_name

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
            with child_step(context, explicit_name):
                return inner(*args, **kwargs)

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
