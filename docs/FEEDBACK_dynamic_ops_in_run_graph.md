# Runtime-declared sub-steps in the run graph

**Author:** Eric Thomas (community components maintainer)
**Date:** 2026-09-03
**Audience:** Dagster core / UI engineering
**Verified against:** `dagster@1.13.20`

## Framing

The industry line is that Prefect supports runtime-dynamic tasks while
Dagster requires everything declared at plan-compile time. It's worth
being precise: **that's a UI constraint, not a runtime constraint.**

Dagster's runtime already accepts runtime-declared steps end-to-end:

- `instance.report_dagster_event(STEP_START, step_key=<anything>)` is a
  public API and the event log stores whatever you give it
- `get_step_stats_for_run` returns synthetic step_keys with correct
  start/end times and status
- GraphQL `Run.stepStats` surfaces them (same resolver as real steps)
- `RunMetadataProvider.extractMetadataFromLogs` pushes them into
  `metadata.steps` on the frontend

The "an `@op` must exist at plan-compile time" framing describes what
the *run-graph renderer* draws today — nodes it recognizes from
`executionPlan.steps` — not what the runtime supports. The renderer
silently drops step_keys it doesn't recognize.

The two frontend patches below (~60 LOC, zero Python changes) lift that
constraint. Dagster gets runtime-dynamic tasks natively, on top of the
step_stats + retry-lifecycle + per-step-log-routing + metadata plumbing
we already have keyed by step_key.

## The ask

**~75 lines of TypeScript, two files, no Python changes.** Everything else
in the stack already works today:

| Layer | Works today? | Change needed |
|---|---|---|
| `instance.report_dagster_event(STEP_START, ...)` — public event emission | ✅ | none |
| Event log storage accepts arbitrary step_keys | ✅ | none |
| `get_step_stats_for_run` returns synthetic step_keys with lifecycle timing | ✅ | none |
| GraphQL `Run.stepStats` surfaces them | ✅ | none |
| `RunMetadataProvider.extractMetadataFromLogs` processes them into `metadata.steps` | ✅ | none |
| Run-graph iterator only iterates `executionPlan.steps` (compile-time known) | ❌ | ~55 LOC in `toGraphQueryItems.tsx` (node creation) |
| Sibling nodes render in alphabetical key order, not execution order | ❌ | ~10 LOC in `toGraphQueryItems.tsx` (start-time sort) |
| Gantt box label is the full step_key (unreadable for nested keys) | ❌ | ~10 LOC in `GanttChart.tsx` |

## The two patches

### Patch 1 — `js_modules/ui-core/src/gantt/toGraphQueryItems.tsx`

Add a fourth step to the iterator that walks `runtimeStepMetadata` (the
authoritative source of "what actually happened"), creates graph nodes
for any step_key not already in the plan, and wires parent→child edges
via the naming convention `<parent>.<op_name>[<mapping_key>]` — the
same shape Dagster's own `DynamicOutput` mapped steps already use.

```typescript
const SYNTHETIC_TASK_SUFFIX_RE = /\.([A-Za-z_][A-Za-z0-9_-]*)\[([A-Za-z0-9_-]+)\]$/;

// Step 4: Runtime-declared synthetic steps.
// Sort by start_time so siblings under each parent render top-to-bottom
// in execution order (not alphabetical). Fall back to key length as a
// tiebreaker — because a child cannot start before its parent's stack
// frame is active, this also guarantees parents insert before children
// even while events are still streaming in.
const orderedRuntimeKeys = [...runtimeStepKeys].sort((a, b) => {
  const startA = runtimeStepMetadata[a]?.start ?? Number.POSITIVE_INFINITY;
  const startB = runtimeStepMetadata[b]?.start ?? Number.POSITIVE_INFINITY;
  if (startA !== startB) return startA - startB;
  return a.length - b.length;
});

for (const stepKey of orderedRuntimeKeys) {
  if (nodeTable[stepKey]) continue;
  const m = SYNTHETIC_TASK_SUFFIX_RE.exec(stepKey);
  if (!m) continue;
  const parentKey = stepKey.slice(0, m.index);

  nodeTable[stepKey] = {
    name: stepKey,
    inputs: [{dependsOn: []}],
    outputs: [],
    metadata: runtimeStepMetadata[stepKey],
  };

  if (nodeTable[parentKey]) {
    nodeTable[stepKey]!.inputs[0]!.dependsOn.push({solid: {name: parentKey}});
    let parentOutput = nodeTable[parentKey]!.outputs[0]!;
    if (!parentOutput) {
      parentOutput = {dependedBy: []};
      nodeTable[parentKey]!.outputs.push(parentOutput);
    }
    // dependedBy push-order determines vertical row placement in the
    // Gantt chart — that's why the start_time sort above matters.
    parentOutput.dependedBy.push({solid: {name: stepKey}});
  }
}
```

### Patch 2 — `js_modules/ui-core/src/gantt/GanttChart.tsx`

Full step_key stays the internal identifier (needed for the `stepStats`
metadata join, tooltip, selection, click handlers). But the *rendered
label* on the Gantt box shortens to just the tail `op[mapping_key]` so
deeply nested nodes stay readable:

```typescript
const SYNTHETIC_TASK_TAIL_RE = /\.([A-Za-z_][A-Za-z0-9_-]*\[[A-Za-z0-9_-]+\])$/;

function displayLabelForStepName(name: string): string {
  const m = SYNTHETIC_TASK_TAIL_RE.exec(name);
  return m ? m[1]! : name;
}

function truncatedBoxLabel(box: GanttChartBox) {
  if (box.width <= BOX_SHOW_LABEL_WIDTH_CUTOFF) return undefined;
  const totalPadding = 7 + (box.state === IStepState.RUNNING ? 16 : 0);
  const maxLength = (box.width - totalPadding) / 6.2;
  return withMiddleTruncation(displayLabelForStepName(box.node.name), {maxLength});
}
```

Without patch 2, deeply nested nodes render as
`parse_document.parse_text[block_0].parse_url[reports].parse_domain[acme_com]` —
unreadable in any real workload.

## Why frontend, not backend?

A backend-only fix has to pick one:

- Return **full step_key** → `stepStats` join works (proper status colors)
  but labels are unreadable
- Return **short label** → labels are clean but `stepStats` lookup breaks
  (all steps render grey/unknown)

The backend returns a single value for `step.key` — it can't do both.
The frontend can do both trivially: use the full key for `stepStats`
lookup, render the short form as the display label. That's the
smoking-gun reason this belongs client-side.

## Empirical verification (2026-09-03)

Cloned `dagster-io/dagster` at the `1.13.20` tag, applied both patches,
ran `./build.sh` in `js_modules/app-oss`, overlaid the built
`dagster_webserver/webapp/build/` onto a live venv, ran `dagster dev`.

**Test workload** — a plain `@dg.asset` with nested `@task`-decorated
Python helpers. This is what a Prefect user writes today; with these
patches Dagster gets the same shape natively:

```python
@task
def parse_domain(context, url):
    return urlparse(url).netloc

@task
def parse_url(context, url):
    parse_domain(context, url, task_name=urlparse(url).netloc.replace(".", "_"))
    return {"url": url}

@task
def parse_text(context, block):
    for u in re.findall(r"https?://\S+", block["content"]):
        parse_url(context, u, task_name=urlparse(u).path.strip("/") or "root")

@task
def parse_table(context, block):
    ...

@task
def parse_title(context, doc):
    return doc["title"]

@dg.asset
def parse_document(context):
    doc = load()
    parse_title(context, doc)
    for block in doc["blocks"]:
        if block["kind"] == "text":
            parse_text(context, block, task_name=f"block_{block['index']}")
        elif block["kind"] == "table":
            parse_table(context, block, task_name=f"block_{block['index']}")
```

Plain imperative Python — real `if`/`else`, real `for`, real return
values, arbitrary nesting depth. Every `@task` call renders as a
distinct graph node under its caller, with real durations, status
colors, and parent→child arrows. `@task` is user-space sugar (see
"About `@task`" below); the underlying mechanism is one public Dagster
API and nothing else.

**Result** (see screen recording): the run graph shows `parse_document`
as the root, with three levels of nested children — every node with
correct duration, status colors, and parent→child arrows.

**Live streaming works.** Nodes appear + change color as
`STEP_START`/`STEP_SUCCESS` events stream in during execution — no page
refresh needed. `RunMetadataProvider.extractMetadataFromLogs` was
already processing synthetic step_keys into `metadata.steps`; the
`toGraphQueryItems` patch just needed to render them.

## The workloads this unblocks

Dagster's run UI is declarative — every op the graph will render is
known at plan-compile time. This is a real gap for workloads where the
shape of what happened is only known at runtime:

- **Agentic tool-use loops** — the LLM decides how many tool calls to
  make. Today: one long-running step. With this: each tool call as a
  distinct child node with real duration.
- **Recursive parsers** — doc → text blocks → URLs → domains. Depth +
  fan-out both runtime-discovered.
- **Sequential-dependent iterations** where each step needs prior's
  actual return value (API pagination cursors, ML early stopping,
  iterative refinement). `DynamicOutput` is parallel-only.

`RetryRequested` and `DynamicOutput` are Dagster's two existing dynamic
mechanisms, but both have shapes that don't fit this pattern:

- `RetryRequested` — same step_key, "attempt N of same work." Wrong tool
  for "distinct work items."
- `DynamicOutput` — one generator + one mapped worker, both pre-declared.
  Explicitly rejects a mapped worker being downstream of two dynamic
  outputs, so multi-layer nesting requires `collect + re-emit` bridges
  per layer.

Fabricated `STEP_START`/`SUCCESS`/`FAILURE` events with a
convention-driven naming scheme sidesteps both — and this PR makes them
render in the graph.

## Zero new Dagster APIs

The whole feature depends on **one public Dagster API**:

```python
context.instance.report_dagster_event(
    DagsterEvent(event_type_value="STEP_START",
                 job_name=context.job_name,
                 step_key="parse_document.parse_text[block_0]"),
    run_id=context.run.run_id,
)
```

That's the primitive. Emit `STEP_START` on entry, `STEP_SUCCESS` on
exit, using a step_key shaped `<parent>.<op_name>[<mapping_key>]`.
Nothing else in Dagster core needs to change for these patches to land.

## About `@task`

`@task` in the test workload above is a **~30-line user-space
decorator** — not a proposed Dagster core API. It exists solely to
make the caller code look imperative (call it like a function) instead
of forcing `with child_step(context, ...):` boilerplate on every line.

What it does: wraps the decorated function so that when called, it
(a) emits `STEP_START` with a step_key derived from the caller's step
+ the function name + a `task_name` kwarg, (b) runs the function,
(c) emits `STEP_SUCCESS` or `STEP_FAILURE`. It tracks the nesting stack
via `contextvars` so a `@task` calling another `@task` produces the
correct parent step_key automatically.

Anyone can write this decorator in a few minutes. Or write their own
version with different ergonomics. Or skip the decorator entirely and
call `instance.report_dagster_event()` inline. The mechanism is the
same either way.

If Dagster core wants to ship a first-class helper — either the
context manager (`context.child_step`) or the decorator (`@dg.task`) —
that would be a nice addition, but it is **not required** for these
frontend patches to unlock the capability.

---

Happy to open the upstream PR whenever core signals go/no-go. Small
diff, self-contained, easy to review.

— eric.thomas@dagsterlabs.com
