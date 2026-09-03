# Feedback / feature request: runtime-visible dynamic ops in the run graph

**Author:** Eric Thomas (community components maintainer)
**Date:** 2026-09-03
**Audience:** Dagster core engineering

## The minimal change

**~50 lines of TypeScript in the run-graph renderer. Zero Python
changes.** Everything else in the stack already works today:

| Layer | Works today? | Change needed |
|---|---|---|
| `instance.report_dagster_event(STEP_START, ...)` — public event emission | ✅ | none |
| Event log storage accepts any step_key | ✅ | none |
| `get_step_stats_for_run` returns synthetic step_keys with lifecycle timing | ✅ | none |
| GraphQL `Run.stepStats` surfaces them | ✅ | none |
| Frontend graph iterates `executionPlan.steps` (compile-time known) | ❌ | ~50 LOC TS |

The frontend fix + a ~15-line `context.step(op_name, mapping_key)`
context manager in user-land (which anyone can write today) — and
that's the complete feature.

### Why frontend, not backend?

A backend-only fix has to pick: return the full step_key (uniqueness +
correct `stepStats` join for status colors, but ugly labels like
`parse_document.parse_text[block_0].parse_url[reports]`), OR return
the short form (clean labels like `parse_url[reports]`, but stats
lookup breaks and steps display as grey/unknown). It cannot do both
because it returns a single value for `step.key`.

The frontend can do both trivially: use the full key for `stepStats`
lookup, render the short form as the display label. That's the
smoking-gun reason this belongs client-side.

Today the run-graph iterator does roughly:

```js
executionPlan.steps.forEach(planStep => {
  const stat = stepStats.find(s => v(planStep.name, s.stepKey));
  renderNode(planStep, stat);
});
```

Flip it: iterate `stepStats` (the authoritative source of "what
actually happened"), then derive display label + parent from a
step_key naming convention `<parent>.<op_name>[<mapping_key>]` —
the same convention Dagster's own `DynamicOutput` mapped steps already
use:

```js
stepStats.forEach(stat => {
  const suffix = /\.([A-Za-z_][A-Za-z0-9_-]*)\[([A-Za-z0-9_-]+)\]$/.exec(stat.stepKey);
  const label = suffix ? `${suffix[1]}[${suffix[2]}]` : stat.stepKey;   // e.g. "parse_url[reports]"
  const parent = suffix ? stat.stepKey.slice(0, suffix.index) : null;    // e.g. "parse_document.parse_text[block_0]"
  renderNode({label, parent, stat});
});
```

That's the initial render. For **live streaming** (nodes appearing as
their STEP_START events arrive during an executing run — the run graph's
whole point), the frontend's event subscription handler needs to add
unknown step_keys on the fly instead of silently dropping them:

```typescript
onStepStartEvent(event) {
  let node = nodes.find(n => n.stepKey === event.stepKey);
  if (!node) {                                                // NEW
    const suffix = /\.([A-Za-z_][A-Za-z0-9_-]*)\[([A-Za-z0-9_-]+)\]$/.exec(event.stepKey);
    if (suffix) {
      node = createNode({
        stepKey: event.stepKey,
        label: `${suffix[1]}[${suffix[2]}]`,
        parent: event.stepKey.slice(0, suffix.index),
      });
      nodes.push(node);
    }
  }
  if (node) node.state = "running";
}
```

Without this second piece, users only see synthetic steps after a page
refresh — which defeats the "watch your pipeline run" mental model
that makes the run graph valuable in the first place. With this piece,
nodes pop in as their STEP_START events stream through, exactly like
retry attempts do today.

Together: ~50 LOC in the initial-render iterator + ~15 LOC in the
event-stream handler. Every step_key emitted to the event log with
this shape renders as a graph node with meaningful label, proper
parent→child arrows, real duration + status — live, not just on
refresh.

### Everything else already exists

- **Event emission**: `instance.report_dagster_event(DagsterEvent(STEP_START, step_key=...))` — public API today.
- **Storage**: event log accepts synthetic step_keys, indexes them, returns them.
- **`get_step_stats_for_run`** returns them with proper `start_time` / `end_time` / `status`.
- **GraphQL `Run.stepStats`** surfaces them via the same resolver used for real steps.
- **Naming convention**: same shape as `DynamicOutput` mapped step keys (`op[mapping_key]`), just recursively nested.

The only reason it doesn't render today is the frontend graph
iterator uses `executionPlan.steps` (compile-time known) as the
source of truth, not `stepStats` (runtime-authoritative).

### User code

Zero decorators required from Dagster core. A ~15-line context
manager in user-land is all the sugar we need:

```python
@contextmanager
def step(context, op_name, mapping_key):
    parent = _active.get() or context.get_step_execution_context().step.key
    key = f"{parent}.{op_name}[{mapping_key}]"
    t0 = time.time()
    context.instance.report_dagster_event(
        DagsterEvent(event_type_value="STEP_START", job_name=context.job_name,
                     step_key=key, message=f"[{op_name}] start"),
        run_id=context.run.run_id,
    )
    token = _active.set(key)
    try:
        yield
    finally:
        _active.reset(token)
        context.instance.report_dagster_event(
            DagsterEvent(event_type_value="STEP_SUCCESS", job_name=context.job_name,
                         step_key=key, event_specific_data=StepSuccessData(duration_ms=(time.time()-t0)*1000)),
            run_id=context.run.run_id,
        )
```

Which then lets users write plain imperative Python:

```python
@dg.asset
def parse_document(context):
    doc = load()
    for block in doc["blocks"]:
        if block["kind"] == "text":
            with step(context, "parse_text", mapping_key=f"block_{block['index']}"):
                for url in extract_urls(block):
                    with step(context, "parse_url", mapping_key=urlparse(url).path):
                        parse(url)   # real Python, real branching, real returns
```

Or wrap that in a `@task`-style decorator (~20 lines of sugar in
user-land — see `dagster_community_components.task` in this repo for a
reference implementation).

### User code (what a customer writes today)

**No decorator required.** Two ergonomic shapes, both 100% imperative
Python. The only Dagster addition needed is a `context.step(op_name,
mapping_key)` context manager — ~15 lines of core code that just emits
STEP_START on entry + STEP_SUCCESS/FAILURE on exit.

**Shape 1 — inline context manager (zero magic):**

```python
import dagster as dg

@dg.asset
def parse_document(context):
    doc = load()
    for block in doc["blocks"]:
        if block["kind"] == "text":
            with context.step("parse_text", mapping_key=f"block_{block['index']}"):
                for url in extract_urls(block):
                    with context.step("parse_url", mapping_key=urlparse(url).path):
                        parse(url)   # any Python, real return values, nested
```

**Shape 2 — `@task`-style decorator (ergonomic sugar; user can implement themselves):**

```python
@task
def parse_url(context, url):
    ...

@task
def parse_text(context, block):
    for url in extract_urls(block):
        parse_url(context, url)     # nested — hangs off parse_text in graph

@dg.asset
def parse_document(context):
    doc = load()
    for block in doc["blocks"]:
        if block["kind"] == "text":
            parse_text(context, block)   # imperative, real branching, real returns
```

**No `yield`. No special asset decorator. No `DynamicOutput` reshape.**
The run graph shows every call as a distinct node with proper
parent→child arrows in a full hierarchy — every step_key visible with
correct edges.

### Why not just use `@dg.op`?

`@dg.op` is a **graph-authoring construct** — declared at plan-compile
time, executed as a plan step. You cannot call it from inside another
op the way you'd call a Python function; it isn't a callable helper,
it's a plan node. The runtime-visible sub-tasks use case is precisely
the shape `@dg.op` was never designed to serve.

The ask here isn't to change `@dg.op`. It's to add ONE context-manager
helper for emitting sub-step events that show up in the graph.

### What `@task` does under the hood (already works today)

```python
@contextmanager
def child_step(context, name):
    seq = _next_seq()
    parent = _active_task_stack.get() or context.get_step_execution_context().step.key
    step_key = f"{parent}.{name}[{seq}]"    # Dagster-native mapping shape

    context.instance.report_dagster_event(DagsterEvent(
        event_type_value="STEP_START", step_key=step_key,
        job_name=context.job_name, message=f"[task:{name}] start",
    ), run_id=context.run.run_id)

    try:
        yield
    except BaseException as exc:
        # emit STEP_FAILURE with StepFailureData
        raise
    else:
        # emit STEP_SUCCESS with StepSuccessData(duration_ms=...)
```

That's it. Zero new Dagster APIs — we're using `instance.report_dagster_event()`
which is already public. The events land in the event log with proper
lifecycle timestamps.

### The two GraphQL patches (the ONE thing missing)

**Patch 1: `dagster_graphql/schema/pipelines/pipeline.py::resolve_executionPlan`**
Read the event log's step_stats, derive synthetic step_snaps for any
step_key matching the naming convention `<parent>.<name>[<seq>]`, and
attach them to the plan:

```python
def resolve_executionPlan(self, graphene_info):
    execution_plan_snapshot = ...  # unchanged
    remote_plan = RemoteExecutionPlan(execution_plan_snapshot=...)

    # NEW: derive synthetic steps from event log
    real_keys = {s.key for s in remote_plan.get_steps_in_plan()}
    stats = graphene_info.context.instance.event_log_storage.get_step_stats_for_run(self.dagster_run.run_id)
    SUFFIX_RE = re.compile(r"\.([A-Za-z_][A-Za-z0-9_-]*)\[(\d+)\]$")
    synthetic_keys = [s.step_key for s in stats
                      if s.step_key not in real_keys and SUFFIX_RE.search(s.step_key)]

    if not synthetic_keys:
        return GrapheneExecutionPlan(remote_plan)

    def _parent_of(k):
        m = SUFFIX_RE.search(k)
        return k[:m.start()] if m else None

    synthetic_snaps = [
        ExecutionStepSnap(
            key=k, kind=StepKind.COMPUTE, metadata_items=[],
            outputs=[ExecutionStepOutputSnap(name="result", dagster_type_key="Any")],
            inputs=[ExecutionStepInputSnap(
                name="parent", dagster_type_key="Any",
                upstream_output_handles=[StepOutputHandle(step_key=_parent_of(k), output_name="result")],
            )] if _parent_of(k) else [],
            node_handle_id=k, tags={"synthetic": "task"},
        )
        for k in synthetic_keys
    ]

    remote_plan._synthetic_snap_map = {s.key: s for s in synthetic_snaps}
    aug = GrapheneExecutionPlan(remote_plan)
    aug._synthetic_step_snaps = synthetic_snaps
    return aug
```

**Patch 2: `dagster_graphql/schema/execution.py`** — append synthetic
steps to the resolver + honor them when resolving parent deps:

```python
def resolve_steps(self, _graphene_info):
    steps = [GrapheneExecutionStep(self._remote_execution_plan, self._remote_execution_plan.get_step_by_key(step.key))
             for step in self._remote_execution_plan.get_steps_in_plan()]
    for snap in getattr(self, "_synthetic_step_snaps", []) or []:      # NEW
        steps.append(GrapheneExecutionStep(self._remote_execution_plan, snap))
    return steps

def resolve_dependsOn(self, _graphene_info):
    synth = getattr(self._remote_execution_plan, "_synthetic_snap_map", None) or {}
    deps = []
    for key in self._step_input_snap.upstream_step_keys:
        if self._remote_execution_plan.key_in_plan(key):
            deps.append(GrapheneExecutionStep(self._remote_execution_plan, self._remote_execution_plan.get_step_by_key(key)))
        elif key in synth:                                              # NEW
            deps.append(GrapheneExecutionStep(self._remote_execution_plan, synth[key]))
    return deps
```

That's the whole change. **~60 lines, 2 files, GraphQL-only.**

### What it looks like

Live-tested end-to-end against `dagster dev` at `1.12.12`. A run of
`parse_document` (a `@dg.asset` that calls 4 `@task`-decorated helpers
imperatively, with nested `parse_text → parse_url → parse_domain`)
produces a run graph with:

- 1 real op: `parse_document`
- 4 direct children: `parse_title`, `text_block_0`, `text_block_2`,
  `text_block_3`, `table_block_1`
- 2 grandchildren under `text_block_0` (2 URLs): `parse_url`
- 1 grandchild under `text_block_2` (1 URL): `parse_url`
- 3 great-grandchildren under each `parse_url`: `parse_domain`

**Zero DynamicOutput reshape. Zero pre-declaration. Full runtime
discovery. Every step key visible in the graph with correct parent
edges.**

### Why this specific naming convention

`<parent>.<name>[<seq>]` mimics Dagster's existing `<op>[<mapping_key>]`
convention for `DynamicOutput`-fanned steps. The frontend already renders
that shape as "name" boxes with `[seq]` mapping-key badges. No frontend
changes required.

### Prior art / related mechanisms this reuses

- **`RetryRequested` lifecycle** — same `STEP_START` / `STEP_UP_FOR_RETRY`
  / `STEP_RESTARTED` / `STEP_SUCCESS` events, different naming.
- **`DynamicOutput` mapping resolution** — same `<parent>[mapping_key]`
  step_key convention, different origin (post-plan-compile).
- **`context.instance.report_dagster_event()`** — same event emission
  API. Already public.

---

## TL;DR — and the key finding

**This is a UI-only change.** The entire backend data path for dynamic
runtime step events already works — we verified end-to-end that a
compute can emit `STEP_START` / `STEP_SUCCESS` / `STEP_FAILURE` events
with a synthetic step_key via `context.instance.report_dagster_event()`,
and:

- `get_step_stats_for_run` returns them with proper start/end times +
  attempt counts + status (verified via
  `instance.event_log_storage.get_step_stats_for_run(run_id)`).
- The GraphQL `Run.stepStats` resolver surfaces them — the same resolver
  the frontend already uses for real step stats.
- The event log records them like any other step event.

**The only reason they don't render as graph nodes today: the frontend
graph iterator (packed JS chunk) does
`stepStats.find(e => v(i.name, e.stepKey))` where `i.name` comes from
`executionPlan.steps`, so it silently skips any step_key not in the
plan.**

Concretely, this Node.js snippet from the shipped Dagit bundle:

```js
// in the run-graph render loop
stepStats.find(e => v(i.name, e.stepKey))
```

...iterates plan-known steps and looks up stats for each. If the
iteration flipped — iterate stepStats and render each — dynamic steps
would render for free, using the data model that already exists.

The ask is: **let the run-graph renderer trust the event log as the
authoritative source of step_keys, not only `executionPlan.steps`. A
nested-step-key convention (e.g., `<parent>[task.<name>.<seq>]` — the
same shape already used for `DynamicOutput` mapping_keys) can render as
children of `<parent>` with zero backend changes.**

## The workloads that need this

Dagster's job run UI is declarative — every op the graph will render is
known at plan-compile time. This creates a real usability gap for a
growing class of workloads where the "shape" of what happened is only
known at runtime:

1. **Sequential-dependent iterations** (each step needs prior's real
   output) — agentic tool-use loops, API pagination, ML early stopping,
   iterative refinement, recursive descent parsers.
2. **Dynamic per-step routing** (which downstream op fires depends on a
   prior op's result content).

The **one** dynamic slot the UI supports today is **retry attempts** (via
`RetryRequested`), which appear as `STEP_UP_FOR_RETRY` →
`STEP_RESTARTED` cycles under an existing step_key. `DynamicOutput` is a
second mechanism but its semantics are one-shot parallel fan-out (all
mapping_keys emitted from one generator op, then executed in parallel) —
not sequential-with-data-dependency.

Concretely, **none of the workarounds we tried close this gap cleanly:**

| Workaround | Honest cost |
|---|---|
| Fabricated `STEP_START`/`SUCCESS`/`FAILURE` events keyed `<parent>[task.<name>.<seq>]` | Log tab shows the hierarchy + real durations. Graph tab does NOT render them (frontend filters to `executionPlan.steps`). |
| Use `RetryRequested` as "iteration N" | Bastardizes retry semantics — the UI shows N "retry attempts" on a step that actually succeeded N times. Confusing. |
| Pre-declare `max_slots` iteration ops with `is_required=False` skip semantics | Graph pollution: if `max_slots=20` and the loop runs 3 iterations, users see 3 SUCCESS + 17 SKIPPED nodes. |
| Chain sequential `DynamicOutput` layers with a `collect + re-emit` bridge between them | Legal but each layer must be pre-declared. Doesn't help for unbounded runtime-count iterations. |
| Pipes / external compute + `PipesClient` | Adds an external process boundary. Overkill for in-run sub-step visibility. |

**Proposal**: add a first-class `context.spawn_child_step(name)` API. The
runtime plumbing already exists (retry-attempt lifecycle events and
`_new_dynamic_mappings` resolution both mutate `ActiveExecution` and emit
`STEP_*` events after plan compile). This proposal reuses that plumbing
rather than adding new machinery.

## Why this matters — the workload shape

The community components registry now ships a family of "agentic"
components (`AgenticPipelineComponent` with `tool_use_loop` /
`route` / `map` / `debate` / `critique_loop` / etc.). These
compose N LLM calls whose count + branching is **decided at runtime**
based on the LLM's own choices.

Today, the entire agentic loop is **one Dagster step**. From the UI, a
customer sees a step "step_key=summarize_tickets" that took 47 seconds.
They cannot see:

- The 8 tool calls the LLM chose to make.
- Which branch of a `route` step it picked.
- Whether a `critique_loop` bailed early on iteration 2 or ran the full 5.
- Which of 12 fanned-out items in a `map` step failed.

All of that lives in `AssetObservation` metadata + `context.log` output.
Which is auditable, but not **visible in the run graph** — the visual
mental model customers actually use to reason about a run.

The AI-orchestration story is where this bites hardest, but the pattern
generalizes: any decorator or component that wants to make internal
control flow visible has the same limitation.

## What already exists (and its limits)

### Mechanism 1: `RetryRequested` — dynamic step attempts

```python
raise dg.RetryRequested(max_retries=N, seconds_to_wait=delay)
```

**What it does:** Step goes to `STEP_UP_FOR_RETRY`, waits in the step
runner, comes back as a new step attempt (`STEP_RESTARTED` → new
`STEP_START`). `context.retry_number` increments. UI renders the
attempts as a lifecycle under the same step node.

**Code path:** [`ActiveExecution.mark_up_for_retry`](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/execution/plan/active.py#L550)
places the step in `_waiting_to_retry`, then `_update()` moves it
back to `_executable` after backoff.

**Limit:** The step_key doesn't change. This is retry-of-existing-step,
not spawn-new-step. Perfect for reliability primitives (`@smart_retry`,
`@partition_lock` wait mode) — which is why we lit both up on this
mechanism. Wrong tool for "show me the LLM made 8 tool calls."

### Mechanism 2: `DynamicOutput` — fan-out over runtime values

```python
@op(out=DynamicOut())
def generate_subtasks():
    for item in fetch_list_from_api():
        yield DynamicOutput(value=item, mapping_key=f"item_{item.id}")

@op
def process_subtask(subtask):
    ...

generate_subtasks().map(process_subtask)
```

**What it does:** Op declares `DynamicOut`. Compute yields N
`DynamicOutput(mapping_key=...)`. Downstream ops must be `.map()`'ed at
DAG-authoring time. At runtime, `ActiveExecution._resolve_any_dynamic_outputs`
sets `_new_dynamic_mappings=True`, and `ExecutionPlan.resolve(mappings)`
converts each `UnresolvedMappedExecutionStep` into N concrete
`ExecutionStep`s (one per mapping_key).

**Limit:** Requires two ops (generator + mapper) declared at authoring
time and a `DynamicOut` type. You cannot have a single op decide "I'd
like to spawn a sub-step called `fetch_user_123`" mid-compute. The DAG
shape is fixed; only the fan-out width is runtime.

Also: it's shaped for parallel independent sub-tasks. Doesn't fit
sequential, dependent sub-tasks (e.g., "call tool A → decide based on
result → call tool B").

### Mechanism 3: Just emit `AssetObservation`s

The escape hatch we use today. `context.log_event(AssetObservation(...))`
lands each event in the event log with tags + metadata. Sensors query it,
the asset detail page shows it. **The step graph doesn't render it.**

Which brings us to what's missing.

## What's missing: runtime child-step spawning

A single first-class API — usable from any compute — that spawns a
step-graph-visible sub-node without requiring prior DAG declaration.
Something like:

```python
@dg.asset
def summarize_tickets(context):
    tickets = fetch_tickets()

    with context.spawn_child_step("route_by_priority"):
        priority = route_llm(tickets)

    for i, tool_name in enumerate(agent_loop.tool_plan):
        with context.spawn_child_step(f"tool_call_{i}_{tool_name}"):
            agent_loop.execute_tool(tool_name)

    with context.spawn_child_step("final_synthesis"):
        return synthesize(agent_loop.trace)
```

The UI renders this as a parent step `summarize_tickets` with child nodes
`route_by_priority`, `tool_call_0_search`, `tool_call_1_read`, ...,
`final_synthesis` — each with its own STEP_START / STEP_SUCCESS /
STEP_FAILURE lifecycle, its own duration, its own logs, its own metadata.

## Proposed minimal API

```python
class OpExecutionContext:
    @contextmanager
    def spawn_child_step(
        self,
        name: str,
        *,
        tags: Mapping[str, str] | None = None,
    ) -> Iterator["ChildStepContext"]:
        """Emit STEP_START on entry + STEP_SUCCESS/STEP_FAILURE on exit
        for a child step_key derived from the parent (e.g., `<parent>[child.<name>]`).

        The child appears in the run graph as a nested node under the
        parent step. Its logs, metadata, and duration are tracked
        separately. Failure inside the block re-raises after emitting
        STEP_FAILURE for the child (parent behavior unchanged — the
        parent catches or propagates as usual).
        """
```

`ChildStepContext` is a thin wrapper over `OpExecutionContext` scoped to
the child: `child_ctx.log.info(...)` lands under the child's step_key,
`child_ctx.add_output_metadata(...)` attaches to the child.

## Why this is a small ask

The two existing mechanisms already do 90% of the work:

1. **Retry attempts** prove that `ActiveExecution` can accept new step
   lifecycle events for a step_key that wasn't in the initial executable
   set, and the UI already renders them under a parent node.
2. **DynamicOutput** proves that the step-key naming convention supports
   nested children (`<step>[<mapping_key>]`) and the UI already handles
   the parent/child render.

What's needed is a new API surface that reuses these primitives for a
different shape:

- Reuse the child step_key convention (`<parent>[<child_name>]`)
- Reuse the step lifecycle events (`STEP_START`, `STEP_SUCCESS`,
  `STEP_FAILURE`)
- Reuse the log routing (per-step_key log capture already works)
- New: a lightweight `ChildStepContext` that scopes writes to the child

Notably NOT needed:

- No new event types
- No new UI rendering (children of a step are already a rendered shape)
- No changes to `ExecutionPlan` (children don't need to exist in the plan
  — they're a runtime overlay, like retry attempts)

## Where a community decorator could plug in

If this API existed, a companion `@dg.asset` decorator could turn any
compute into a "traced" op that automatically spawns a child step per
top-level function call inside it, per iteration of a bounded loop, per
external HTTP call, etc. The user opts in by wrapping the compute; the
decorator handles the boilerplate.

Concrete near-term uses from the community components registry:

| Component | Would surface in the run graph |
|---|---|
| `AgenticPipelineComponent.tool_use_loop` | Each tool call as its own child step |
| `AgenticPipelineComponent.map` | Each fanned-out item's LLM call (today it's `DynamicOut`-shaped, but that requires 2 ops; child-step API would let it stay 1 op with runtime children) |
| `AgenticPipelineComponent.route` | The router's choice + the picked specialist as separate children |
| `AgenticPipelineComponent.critique_loop` | Each drafter+critic iteration |
| `SmartRetryComponent` | Already uses `RetryRequested`; N/A |
| Any HTTP-fetch component | Each retry attempt as a child (would deprecate a lot of ad-hoc log parsing) |
| Any Pipes-driven external compute | Each external phase as a child (Pipes already emits step-scoped events; children would give it a graph shape) |

## What I've verified locally

Traced the retry lifecycle end-to-end:

- `RetryRequested` raised from compute
- Caught by step runner (`compute.py` → `handle_step_output`)
- `ActiveExecution.mark_up_for_retry(step_key, at_time)` places step in
  `_waiting_to_retry`
- `_update()` moves it back to `_executable` after backoff
- Step re-executes; `previous_attempt_count` (aka `context.retry_number`)
  increments
- `STEP_UP_FOR_RETRY` and `STEP_RESTARTED` events written to event log
- UI renders attempts as retry lifecycle under same step node

The `ActiveExecution` API is what receives these mutations. A child-step
API would use the same class:

```python
class ActiveExecution:
    def register_child_step(self, parent_key: str, child_name: str, ...) -> str:
        """Create a virtual step_key for a runtime child and mark it active."""
```

The event stream + event log write path is unchanged.

## What I've ruled out

- **Emit `STEP_START` events with a fabricated step_key from a decorator.**
  Tested — the event log accepts the write, but the UI doesn't render the
  step_key because it's not in the resolved `ExecutionPlan`. It appears
  as a bare log line.
- **Restructure user compute into a `DynamicOut` generator + mapped
  downstream op.** Possible but requires the decorator to reshape the
  user's DAG (turn one asset into two ops in a graph). Loses the
  ergonomics that make a decorator worth reaching for.
- **Wrap sub-tasks as nested `@dg.asset`s and materialize them
  imperatively via `dg.materialize`.** Doesn't work — nested
  materialization launches a *new run*, not sub-steps within the current
  run. Whole different graph in the UI.

Which leaves: a real API from engineering. Happy to prototype if that's
useful, or provide more of the "what would the AI-orchestration story
look like with this" thread.

## Related community work

If this ships, the AI-orchestration story becomes materially better —
16 components in the [`decorator`
category](https://dagster-component-ui.vercel.app/?category=decorator)
and the whole `AgenticPipelineComponent` op family
would gain first-class run-graph visibility without any user-facing
YAML/API changes.

Happy to chat: eric.thomas@dagsterlabs.com.
