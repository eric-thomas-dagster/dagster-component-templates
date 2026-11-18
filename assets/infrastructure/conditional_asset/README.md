# ConditionalAsset

Branch / short-circuit as a Dagster asset — the Airflow `ShortCircuitOperator` / `BranchPythonOperator` equivalent.

Built on a real, native Dagster mechanism, verified with live `dg.materialize()` runs before this component was written, not assumed: a `@dg.multi_asset` output declared `AssetOut(is_required=False)` that the compute function chooses not to `yield` causes **every downstream step depending on it to skip** (`STEP_SKIPPED`, not a failure) — cascading through arbitrarily long chains, while the run as a whole still succeeds.

## `mode: "short_circuit"`

One asset, one callable returning a truthy/falsy value. Truthy → the asset materializes and everything downstream runs normally. Falsy → it doesn't materialize this run, and every downstream asset depending on it skips.

```yaml
type: dagster_community_components.ConditionalAssetComponent
attributes:
  mode: short_circuit
  callable_path: myproject.checks:has_new_data
  asset_key: pipeline/gate
```

```python
# myproject/checks.py
def has_new_data() -> bool:
    return check_for_new_rows()
```

## `mode: "branch"`

The callable returns the name of exactly one of the statically-declared `branches` — mirroring Airflow's `BranchPythonOperator`: the *set* of possible branches is always known at authoring time; only which one runs is dynamic. Only the chosen branch's asset materializes; every other branch (and everything depending on it) skips.

```yaml
type: dagster_community_components.ConditionalAssetComponent
attributes:
  mode: branch
  callable_path: myproject.routing:choose_region
  inputs:
    order_data: raw/orders
  branches:
    us: regions/us_processing
    eu: regions/eu_processing
```

```python
# myproject/routing.py
def choose_region(order_data):
    return "us" if order_data["country"] == "US" else "eu"
    # or return ("us", some_payload) for an explicit payload to the chosen branch
```

A downstream asset for one branch:

```yaml
type: dagster_community_components.PythonCallableAssetComponent
attributes:
  asset_key: regions/us_processing/next_step
  callable_path: myproject.us:process
  inputs:
    us_result: regions/us_processing
```

If `choose_region` picks `"eu"`, `regions/us_processing` never materializes that run, and `regions/us_processing/next_step` (and anything downstream of it) skips automatically.

## `inputs` — feeding the decision callable

Same shape as `python_callable_asset`'s `inputs`: `{name: upstream_asset_key}`. Each entry's value is loaded via the IO manager and bound to a matching parameter name if the callable's signature declares one (TaskFlow-style) — only the names the callable's own signature declares are injected (or everything, if it declares `**kwargs`).

## Downstream propagation (verified, not assumed)

When a branch/gate output isn't yielded, every downstream asset that depends on it is
`STEP_SKIPPED` — not materialized, no asset check runs on it, the run still succeeds overall.
Confirmed live via `dg.materialize()`:

- **Cascades transitively** through arbitrarily long chains.
- **Applies to `deps=` (lineage-only) the same as a bound `AssetIn`** — there's no "soft"
  dependency that escapes skip propagation. A downstream asset wired via `deps=["gate"]` alone
  skips exactly like one that actually consumes the gate's value.
- **No selective rejoin.** If a downstream asset depends on *several* upstream branches and
  even one of them was skipped, the whole downstream step is skipped too — Dagster's skip
  propagation is structural (decided at execution-plan time from the dependency graph), not
  "proceed if at least one upstream succeeded." A Python-side default parameter on the skipped
  input doesn't change this (verified — the step never runs at all, so the default is never
  reached). There is no `none_failed_min_one_success`-style trigger rule for *intra-run* skip
  propagation the way there is for schedule-triggered automation conditions
  (`automation_condition_applicator` has that preset, but it's a different mechanism for a
  different trigger). If your conversion needs a rejoin that proceeds when only one of several
  branches ran, restructure so the rejoin asset depends on the branches' *shared successor*
  instead of on the branches directly, or accept that the rejoin step also skips.

## What this does NOT do

- No partitioning, freshness, retry policy, or backfill policy in this version — partitioning a conditionally-yielded multi-output asset is a materially more complex combination (which partition's key applies to which branch's skip decision?) than a quick field addition; a real follow-up, not silently dropped.
- Branch names must be valid dict keys matching exactly what the callable returns — there's no fuzzy matching or default/fallback branch. An undeclared name raises a clear failure at run time.
- **No selective rejoin** (see above) — for that, use `af_gate`/`af_guard`/`af_finish` below instead of (or alongside) this component.

## `af_gate` / `af_guard` / `af_finish` — the rejoin-capable alternative

Plain Python decorators (not YAML components — apply them directly to `@dg.op`/`@dg.asset`
functions), for exactly the case above this component can't do: a join that needs to run even
when some of its upstream branches were skipped or failed. Instead of Dagster's structural skip
propagation, every decorated step always actually runs and returns a `StepOutcome` marker
describing what happened, so the next step can inspect its upstreams' states and decide for
itself — the same shape as the converter's own `af_gate`/`af_guard`/`af_finish` runtime helper
(~150 generated lines) for branch/short-circuit/join/cleanup/alert translated from Airflow
trigger rules.

```python
import dagster as dg
from dagster_community_components import af_gate, af_guard, af_finish, AfSkip

@dg.op
@af_gate()
def extract_region(region):
    if not has_data(region):
        raise AfSkip(f"no data for {region}")
    return fetch(region)

@dg.op
@af_guard(rule="one_success")   # Airflow TriggerRule name -- runs if at least one upstream succeeded
def merge(us, eu):
    return combine(us, eu)

@dg.op
@af_finish()   # fails the run for real if anything upstream failed, even through a satisfied guard
def done(merged):
    return merged
```

- **`af_gate()`** — wraps one step. `AfSkip` inside the function → `StepOutcome(state='skipped')`; any other exception → `'failed'`; a normal return → `'success'` (value preserved). If an upstream input is itself a non-success `StepOutcome`, the function is never called at all (`'upstream_failed'`/`'upstream_skipped'`).
- **`af_guard(rule=...)`** — a join/rejoin step. `rule` is an Airflow TriggerRule name: `all_success` (default), `all_failed`, `all_done`, `one_failed`, `one_success`, `none_failed`, `none_failed_min_one_success`, `none_skipped`, `always`. Rule satisfied → runs with the real unwrapped upstream values. Not satisfied → the function never runs and this step becomes `'skipped'` — but it still actually ran at the Dagster level, which is what makes the rejoin possible.
- **`af_finish()`** — the pipeline's last step. Always runs; raises `dg.Failure` (a real run failure) if anything anywhere in the upstream lineage failed — even through a guard whose rule let it proceed (e.g. `one_success`). A guard letting a run continue doesn't erase the fact that something failed; `af_finish` is what turns that back into a real failed run at the end.

Each `StepOutcome` carries the FULL upstream `issues` lineage, not just its own direct inputs' states — a join that proceeds under `one_success` still passes the other branch's failure forward for `af_finish` to catch, instead of silently losing it.

Mix freely with plain (undecorated) ops/assets: a bare value flowing into `af_gate`/`af_guard`/`af_finish` is treated as `state='success'`.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `mode` | `Literal['short_circuit', 'branch']` | 'short_circuit': one asset, materializes or not. 'branch': pick exactly one of several statically-declared branches. |
| `callable_path` | `str` | 'module.path:function_name'. short_circuit: returns a truthy/falsy value. branch: returns a branch name (str), or (branch_name, payload) for an explicit payload. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `deps` | `List[str]` | — | Lineage-only upstream asset keys -- no value loaded (use `inputs` to load a value). |
| `asset_key` | `str` | — | mode='short_circuit' only (required then): the single gate asset key, slash-delimited. |
| `group_name` | `str` | `"infrastructure"` | Dagster asset group name (applied to every branch uniformly in branch mode). |
| `description` | `str` | — | Asset description. |
| `owners` | `List[str]` | — | Asset owners -- team names ('team:analytics') or email addresses. |
| `asset_tags` | `Union[Dict[str, Any], List[str]]` | — | Additional key-value tags applied to the asset(s) in the Dagster catalog. |
| `kinds` | `List[str]` | — | Asset kinds for the catalog. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `args` | `List[Any]` | — | Static positional arguments passed to the callable. |
| `kwargs` | `Dict[str, Any]` | — | Static keyword arguments passed to the callable. |
| `inputs` | `Dict[str, str]` | — | {name: upstream_asset_key}. Each entry's value is loaded via the IO manager and bound to a parameter of that name if the callable's signature declares one (TaskFlow-style) -- same shape as python_callable_asset's `inputs`. |
| `branches` | `Dict[str, str]` | — | mode='branch' only (required then): {branch_name: asset_key}. asset_key is slash-delimited. The callable must return one of these branch_name values. |

[//]: # (FIELDS:END)

## Requirements

```
dagster
```
