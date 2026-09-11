# `ThrottleAssetComponent` + `@throttle` decorator

Cross-run rate limiting for asset compute. State lives in the Dagster event log — no Redis, no in-process dict, no drift between workers.

## What it does

Reads the most recent `ASSET_MATERIALIZATION` event for this asset via `context.instance.get_event_records`. If less than `min_gap_seconds` has elapsed since that event, either:

- `skip` (default) — return `None`, emit `AssetObservation` tagged `throttle_skipped=<key>` with wait metadata.
- `fail` — raise `dg.Failure` — surfaced in Dagit as a step failure.

## Why this belongs in Dagster

- **Restart-safe** — throttle state survives Dagster restarts because it's just event log queries.
- **Worker-safe** — concurrent runs on different workers see the same last-materialization view.
- **Auditable** — every skipped run leaves a `throttle_skipped` observation. Sensors can count them, alerts can escalate.
- **Complementary to schedules** — a schedule fires the run, `@throttle` decides whether it actually materializes. Sensors, eager AutomationConditions, and manual re-runs all get gated the same way.

## Full YAML example

```yaml
type: dagster_community_components.ThrottleAssetComponent
attributes:
  asset_name: hot_search_index

  compute:
    kind: python
    python: "my_project.search:rebuild_index"

  min_gap_seconds: 30
  on_throttle: skip           # skip (default) | fail
```

## Composability — `wraps:` an existing component

Stack the throttle primitive over another DCC component's assets with **zero Python**. Direct YAML analog of `@throttle @dg.asset` decorator stacking:

```yaml
type: dagster_community_components.ThrottleAssetComponent
attributes:
  min_gap_seconds: 60
  on_throttle: skip
  wraps:
    type: dagster_community_components.SyntheticDataGeneratorComponent
    attributes:
      asset_name: customers
      schema_type: customers
      row_count: 1000
```

One asset is registered (`customers`) — no duplication. The outer throttle intercepts the inner's compute + emits `throttle_skipped` observations if the min-gap isn't satisfied. Inner's partitions, deps, kinds, tags, group all pass through unchanged.

Stacks arbitrarily deep — `BudgetAssetComponent { wraps: ThrottleAssetComponent { wraps: <inner> } }`.

Same caveats as `SlaAssetComponent.wraps`: single-key inner AssetsDefinitions only; multi-asset inners pass through unwrapped.

## `@throttle` decorator

```python
import dagster as dg
from dagster_community_components import throttle

@dg.asset
@throttle(min_gap_seconds=30, on_throttle="skip")
def hot_search_index(context):
    return rebuild_index()
```

## Composes with

- **`@smart_retry`** — retries respect the throttle window.
- **`@sla`** — SLA measures compute duration; `@throttle` measures inter-run gap.
- **`@cached`** — cache hits still count as materializations for throttling.
- **`@lifecycle`** — throttle checks BEFORE staging; no wasted Write-Audit-Publish work.

## CLI demos using this template

| Demo | Setup script | What it shows |
|---|---|---|
| [`throttle_asset.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/throttle_asset.md) | [`setup_throttle_asset_demo.sh`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/setup_throttle_asset_demo.sh) | 100% offline. Demonstrates BOTH shapes side by side: (1) `@throttle` Python decorator over a `@dg.asset`, (2) `ThrottleAssetComponent { wraps: SyntheticDataGeneratorComponent }` — zero Python, pure YAML composability. Both share the same event-log-backed rate limit and emit the same `throttle_skipped` observations. |

```bash
curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-community-components-cli/main/examples/setup_throttle_asset_demo.sh | bash
```

## Sensor pattern — alert on excessive throttling

```python
@dg.sensor
def throttle_alert(context):
    from dagster import EventRecordsFilter, DagsterEventType
    records = context.instance.get_event_records(
        event_records_filter=EventRecordsFilter(event_type=DagsterEventType.ASSET_OBSERVATION),
        limit=100, ascending=False,
    )
    skipped = sum(1 for r in records if r.asset_observation.tags.get("throttle_skipped") == "hot_search_index")
    if skipped >= 10:
        # 10+ throttles recently — either bump min_gap or investigate over-triggering
        ...
```

## Per-partition throttling

Different partitions of the same asset can have different throttle windows. Set
`per_partition_min_gap` to a `{partition_key: seconds}` map; the runtime picks
the matching override at compute time. When set, throttling is genuinely
per-partition — the "last materialization" query is filtered to the same
`partition_key`, so hourly and daily partitions throttle independently of each
other.

```yaml
min_gap_seconds: 60                     # default fallback
per_partition_min_gap:
  hourly: 30                             # hourly partitions: 30s min gap
  daily: 3600                            # daily partitions: 1h min gap
partition_matcher: exact                 # 'exact' (default) | 'prefix' | 'regex'
```

Python decorator:

```python
@dg.asset(partitions_def=dg.StaticPartitionsDefinition(["hourly", "daily"]))
@throttle(
    min_gap_seconds=60,
    per_partition_min_gap={"hourly": 30, "daily": 3600},
)
def hot_index(context):
    return refresh_index()
```

## What's not in v1 (roadmap)

- **Token bucket** — allow burst-N up to a cap, then throttle.
- **Sliding-window rate** — max N materializations in a window.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `min_gap_seconds` | `float` | Minimum wall-clock gap between materializations. Materializations closer than this are skipped or failed depending on on_throttle. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | — | Dagster asset name. Required when NOT using `wraps:` (inherited from inner in wraps mode). |
| `group_name` | `str` | — | — |
| `description` | `str` | — | — |
| `owners` | `List[str]` | — | — |
| `tags` | `Dict[str, str]` | — | — |
| `kinds` | `List[str]` | — | Default: ['python', 'throttle']. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_matcher` | `str` | `"exact"` | How partition_key is matched against per_partition_min_gap keys: 'exact' \| 'prefix' \| 'regex'. Default exact match. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `str` | — | — |
| `compute` | `Dict[str, Any]` | — | `{kind: python, python: 'mod:fn'}`. Mutually exclusive with `wraps`. |
| `wraps` | `Dict[str, Any]` | — | Wrap another DCC component's assets with throttle rate-limiting instead of defining new compute. Shape: `{type: 'dagster_community_components.<Component>', attributes: {...}}`. Mutually exclusive with `compute`. |
| `on_throttle` | `str` | `"skip"` | 'skip' (default) returns None + emits AssetObservation; 'fail' raises dg.Failure. |
| `key` | `str` | — | Optional label for the throttle_skipped observation tag. Defaults to asset name. |
| `per_partition_min_gap` | `Dict[str, float]` | — | Per-partition-key override. e.g. {'hourly': 30, 'daily': 300}. Falls back to min_gap_seconds if no key matches. When set, throttling is per-partition — the 'last materialization' check is filtered to the same partition_k… _(full docs in schema.json + component README)_ |

[//]: # (FIELDS:END)
