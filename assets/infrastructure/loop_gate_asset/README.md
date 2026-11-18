# LoopGateAsset

Repeat a step's work until a condition is met — Airflow 3.4's task-loop feature (`@task_group.loop(max_iterations=, until=)`).

`LoopGateOperator` (the name this was requested under) is **not** a published, importable Airflow operator class — it's the internal name Airflow's own UI shows for the gate task the `.loop()` feature generates after a looped task group's terminal task (verified against Airflow 3.4.0's own docs and Astronomer's task-loops guide, not guessed). The real, user-facing mechanism: a task group runs its body, then a gate task calls the `until` callable, which decides — via a `loop` object carrying `.result`/`.previous`/`.index`/`.max_iterations` — whether to run the body again. `until` must return `bool` or a `(stop, reason)` pair; any other return value, including a forgotten `return`, fails the gate. No `until` → a fixed-count loop that always succeeds after `max_iterations` runs.

This component models that same shape as **one Dagster asset**: the body callable runs in a plain Python loop inside the asset's own compute function — Dagster has no primitive for "re-run this op N more times within one step" the way Airflow's scheduler does across task instances — and the `until` callable is consulted the same way after each iteration.

## The body callable

`callable_path`: `"module.path:function_name"`. Called once per iteration with the same TaskFlow-style `inputs`/`args`/`kwargs` binding as `python_callable_asset` — `inputs` values are loaded via the IO manager **once, before the loop starts**, and bound to any matching parameter name the callable's signature declares; `args`/`kwargs` are static and repeated on every iteration.

```python
# myproject/refine.py
def refine_once(raw_text, previous_score=None):
    ...
    return {"embedding": [...], "score": 0.91}
```

## The `until` callable

`until_callable_path` (optional): called as `until_fn(loop=loop)` after every iteration — a single keyword argument, matching Airflow's own gate signature. `loop` carries:

| Attribute | Meaning |
|---|---|
| `loop.result` | This iteration's body return value. |
| `loop.previous` | The **immediately preceding** iteration's return value — a single value, not a list of every prior result — or `None` on iteration 1. |
| `loop.index` | **1-based** iteration number: the first iteration is `1`, not `0`. |
| `loop.max_iterations` | The configured `max_iterations`. |

(Matches Airflow 3.4's own `loop` object exactly, including the 1-based `index` and single-value `previous` — verified against Airflow's own docs, not assumed.)

Must return `bool`, or a `(stop: bool, reason: str)` tuple. Any other return value — **including `None`** — fails the run immediately, matching Airflow's own real behavior for a careless `until` function.

```python
def accurate_enough(loop):
    return loop.result["score"] >= 0.9, f"score={loop.result['score']}"
```

- Returns `stop=True` → the loop stops; the asset materializes with the stopping iteration's result.
- Returns `stop=False` and more iterations remain → the loop runs again.
- Returns `stop=False` and `max_iterations` is exhausted → the run **fails** (matches Airflow: "if the loop has run `max_iterations` times and `until` still returns `False`, the gate is marked failed").
- `until_callable_path` omitted entirely → fixed-count loop: the body runs exactly `max_iterations` times and the run always succeeds.

## What this does NOT do

- **Repeat a whole multi-task group.** Airflow's `.loop()` repeats an entire `@task_group` — potentially many tasks wired together. This component repeats **one callable**. If the thing that needs to repeat is itself a multi-step pipeline, write it as one function that internally calls several others, or compose several `loop_gate_asset`s if the steps need to be independently observable/retryable.
- **Preserve per-iteration retry.** A real Airflow gate's `retries` stay within the **same iteration** — only that one bad `until` call is retried; the body's prior iterations are untouched. This component's `retry_policy_max_retries` instead retries the **whole loop from its first iteration**, because the entire loop is one Dagster step. If per-iteration retry/idempotency matters to your pipeline, handle it inside the body callable itself (e.g. make each iteration safe to re-run from scratch) rather than relying on `retry_policy_max_retries` for that guarantee.

## Concurrency, retries, and timeouts

- `pool: <name>` — Dagster's native named concurrency pool (Airflow's `pool`), sharing a fixed number of slots across every asset/op using the same pool name. The slot count is configured at the deployment level (`dagster.yaml`'s `concurrency: pools:`, or the Dagster+ UI).
- `retry_policy_max_retries`/`retry_policy_delay_seconds`/`retry_policy_backoff` — a `RetryPolicy` for the asset. See "What this does NOT do" above for how this differs from a real Airflow gate's per-iteration retry.
- `timeout_seconds` — bounds how long this step waits for the **whole loop** (every iteration, not just one), via a background-thread wrapper. The underlying call may continue running past the deadline (Python threads can't be forcibly killed); this bounds the step's wait, not the call itself.

## See also

[`conditional_asset`](../conditional_asset/) — a single callable deciding whether/which branch to take, once (Airflow's `ShortCircuitOperator`/`BranchPythonOperator`). Use that instead when the decision doesn't need to repeat.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_key` | `str` | The asset's key, slash-delimited. |
| `callable_path` | `str` | 'module.path:function_name'. The loop body -- called once per iteration with the same TaskFlow-style `inputs`/`args`/`kwargs` binding as python_callable_asset. |
| `max_iterations` | `int` | Maximum number of times the body runs. Required, same as Airflow's own `.loop(max_iterations=...)`. |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `timeout_seconds` | `float` | — | Fail this step if the WHOLE loop (every iteration, not just one) hasn't finished after this many seconds. Enforced via a background-thread wrapper -- the underlying call may continue running past the deadline (Python thr… _(full docs in schema.json + component README)_ |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `deps` | `List[str]` | — | Lineage-only upstream asset keys -- no value loaded (use `inputs` to load a value). |
| `group_name` | `str` | `"infrastructure"` | Dagster asset group name. |
| `description` | `str` | — | Asset description. |
| `owners` | `List[str]` | — | Asset owners -- team names ('team:analytics') or email addresses. |
| `asset_tags` | `Union[Dict[str, Any], List[str]]` | — | Additional key-value tags applied to the asset in the Dagster catalog. |
| `kinds` | `List[str]` | — | Asset kinds for the catalog. |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. Retries the WHOLE loop from iteration 0 -- see this component's module docstring for how this differs from a real Airflow gate's own per-iteration retry. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `until_callable_path` | `str` | — | 'module.path:function_name'. Called as until_fn(loop=LoopState(...)) after every iteration. Must return bool or (stop: bool, reason). Omit for a fixed-count loop (runs max_iterations times, always succeeds). |
| `args` | `List[Any]` | — | Static positional arguments passed to the body callable on every iteration. |
| `kwargs` | `Dict[str, Any]` | — | Static keyword arguments passed to the body callable on every iteration. |
| `inputs` | `Dict[str, str]` | — | {name: upstream_asset_key}. Each entry's value is loaded via the IO manager (once, before the loop starts) and bound to a parameter of that name if the body callable's signature declares one (TaskFlow-style) -- same shap… _(full docs in schema.json + component README)_ |
| `pool` | `str` | — | Dagster's native named concurrency pool (e.g. Airflow's `pool` -- a fixed number of slots shared across every asset/op using the same pool name). The pool's own slot count is configured at the deployment level (dagster.y… _(full docs in schema.json + component README)_ |

[//]: # (FIELDS:END)

## Requirements

```
dagster
```
