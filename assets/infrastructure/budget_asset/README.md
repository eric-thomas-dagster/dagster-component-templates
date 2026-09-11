# `BudgetAssetComponent` + `@budget` decorator

Per-asset $ cost tracking with a rolling window cap. Every materialization emits an `AssetObservation` carrying `budget_cost_estimate_usd` — sums, averages, and dashboards fall out of ordinary event log queries.

## What it does

- **Per-run cost estimate** — from `cost_per_second` (wall clock) or `cost_fn` (custom callable — required for LLM/API costs).
- **Rolling window cumulative** — sums `budget_cost_estimate_usd` metadata from `ASSET_OBSERVATION` events tagged `budget_cost_asset=<asset>` within `window_days`.
- **Enforcement modes**:
  - `warn` (default) — always run; emit `budget_breach=true` observation when this run pushes cumulative past budget.
  - `fail` — pre-flight: if cumulative already ≥ budget, raise `dg.Failure` BEFORE the compute runs (saves the run). Also post-flight: this run breaches → `dg.Failure`.
  - `skip` — pre-flight: return `MaterializeResult(budget_skipped=true)` if cumulative ≥ budget.

## Why this belongs in Dagster

- **Cost history in the event log** — no side database. FinOps queries are just observation queries.
- **Pre-flight guard** — with `on_breach=fail`, you save the run instead of noticing the breach after burn.
- **`cost_fn` callback for LLM/API costs** — wall-clock doesn't correlate with $ for token-priced APIs. Supply a function that reads `result.usage.total_tokens` and multiplies by price.

## Full YAML example

```yaml
type: dagster_community_components.BudgetAssetComponent
attributes:
  asset_name: llm_summarizer

  compute:
    kind: python
    python: "my_project.llm:summarize"

  cost_fn: "my_project.pricing:openai_usd_from_result"

  budget_usd: 50.0
  window_days: 30
  on_breach: warn        # warn (default) | fail | skip
```

## `@budget` decorator

**Wall-clock rate** (rough — compute is usually not linear in wall clock):

```python
import dagster as dg
from dagster_community_components import budget

@dg.asset
@budget(cost_per_second=0.02, budget_usd=100.0, window_days=30, on_breach="warn")
def costly_pipeline(context):
    return build()
```

**Custom `cost_fn`** (accurate for LLM/API):

```python
def openai_cost(context, elapsed_s, result):
    # result is what the compute returned
    tokens = result.get("usage_tokens", 0)
    return tokens * 0.000002        # $2.00 per 1M output tokens

@dg.asset
@budget(cost_fn=openai_cost, budget_usd=50.0, on_breach="fail")
def llm_summarizer(context):
    return call_openai(...)
```

## Cross-run FinOps sensor

```python
@dg.sensor
def budget_alert(context):
    from dagster import EventRecordsFilter, DagsterEventType
    records = context.instance.get_event_records(
        event_records_filter=EventRecordsFilter(event_type=DagsterEventType.ASSET_OBSERVATION),
        limit=500, ascending=False,
    )
    per_asset_cost = {}
    for r in records:
        tags = r.asset_observation.tags
        key = tags.get("budget_cost_asset")
        if not key:
            continue
        meta = r.asset_observation.metadata
        cost = meta.get("budget_cost_estimate_usd")
        per_asset_cost[key] = per_asset_cost.get(key, 0.0) + (float(cost.value) if cost else 0.0)
    # Alert on top spenders
    ...
```

## Composes with

- **`@sla`, `@timeout`** — SLA measures duration, `@timeout` kills long runs; `@budget` measures $.
- **`@throttle`** — orthogonal: rate limit + budget together model "no more than N runs per hour AND no more than $M per month."
- **`@dry_run`** — dry runs still cost compute time; the observation still lands.
- **`@smart_retry`** — retries still count toward the budget.

## CLI demos using this template

| Demo | Setup script | What it shows |
|---|---|---|
| [`budget_asset.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/budget_asset.md) | [`setup_budget_asset_demo.sh`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/setup_budget_asset_demo.sh) | 100% offline. Demonstrates BOTH shapes: (1) `@budget` Python decorator over `@dg.asset` with a user `cost_fn` — 3 runs of $0.40 each; cumulative crosses $1.00 cap on RUN 3 → `budget_breach=true`, (2) `BudgetAssetComponent { wraps: SyntheticDataGeneratorComponent }` — the "money shot" composability with `cost_per_second: 100.0` wall-clock rate blowing through the cap on the very first run. Both shapes emit the same cost + cumulative + breach observations — the shape you'd use for a FinOps dashboard. |

```bash
curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-community-components-cli/main/examples/setup_budget_asset_demo.sh | bash
```

## Per-partition budgets

Different partitions of the same asset can have different budget caps. Set
`per_partition_budget` to a `{partition_key: usd}` map; the runtime picks the
matching override at compute time. When set, cumulative cost is scoped
per-partition — the cumulative window query is filtered to observations tagged
with the same `budget_partition_key`, so each partition's budget is isolated
from the others.

```yaml
budget_usd: 100                          # default fallback cap
per_partition_budget:
  hourly: 10                              # hourly partitions capped at $10
  daily: 100                              # daily partitions capped at $100
partition_matcher: exact                  # 'exact' (default) | 'prefix' | 'regex'
window_days: 30
```

Python decorator:

```python
@dg.asset(partitions_def=dg.StaticPartitionsDefinition(["hourly", "daily"]))
@budget(
    cost_per_second=0.02,
    budget_usd=100.0,
    per_partition_budget={"hourly": 10, "daily": 100},
    on_breach="fail",
)
def costly_pipeline(context):
    return build()
```

Observations carry `budget_partition_key=<key>` so per-partition cumulative
queries work end-to-end.

## What's not in v1 (roadmap)

- **Multi-asset shared budget** — cap a group of assets under a single team budget.
- **Auto-throttle on approach** — reduce trigger frequency as budget approaches cap.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | — | Dagster asset name. Required when NOT using `wraps:` (inherited from inner in wraps mode). |
| `group_name` | `str` | — | — |
| `description` | `str` | — | — |
| `owners` | `List[str]` | — | — |
| `tags` | `Dict[str, str]` | — | — |
| `kinds` | `List[str]` | — | Default: ['python', 'budget', 'cost']. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_matcher` | `str` | `"exact"` | How partition_key is matched against per_partition_budget keys: 'exact' \| 'prefix' \| 'regex'. Default exact match. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `str` | — | — |
| `compute` | `Dict[str, Any]` | — | `{kind: python, python: 'mod:fn'}`. Mutually exclusive with `wraps`. |
| `wraps` | `Dict[str, Any]` | — | Wrap another DCC component's assets with cost tracking. Shape: `{type: 'dagster_community_components.<Component>', attributes: {...}}`. Mutually exclusive with `compute`. |
| `cost_per_second` | `float` | — | Wall-clock USD/sec rate. Used when cost_fn is null. |
| `cost_fn` | `str` | — | Optional 'mod:fn' callable returning USD given (context, elapsed_s, result). |
| `budget_usd` | `float` | — | Rolling window cap. Null → observation-only, no breach. |
| `window_days` | `float` | `30.0` | Rolling window (days) for cumulative cost sum. |
| `on_breach` | `str` | `"warn"` | 'warn' (default): always run, emit budget_breach observation; 'fail': dg.Failure pre-flight if cumulative >= budget or post-flight if this run breaches; 'skip': return MaterializeResult(budget_skipped=true) pre-flight. |
| `per_partition_budget` | `Dict[str, float]` | — | Per-partition-key override. e.g. {'hourly': 10, 'daily': 100}. Falls back to budget_usd if no key matches. When set, cumulative cost is tracked per-partition (each partition's budget is isolated from others). |

[//]: # (FIELDS:END)
