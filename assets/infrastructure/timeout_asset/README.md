# `TimeoutAssetComponent` + `@timeout` decorator

Hard-kill compute after N seconds. Portable, Dagster+ Serverless-safe. Fills a real gap in Dagster's `RetryPolicy` (which has NO timeout knob).

## What this does that `@sla` doesn't

| | `@sla` | `@timeout` |
|---|---|---|
| Timer wraps compute | ✓ | ✓ |
| Observes overrun | ✓ | ✓ |
| Asset materializes past deadline | ✓ (unless `fail`) | ✗ (killed) |
| Prevents runaway budget burn | ✗ | ✓ |
| Cross-run breach tracking | ✓ | ✓ |

Use both together: `@sla(expected=30)` for observation + `@timeout(60)` for the hard limit.

## How it works

Uses `concurrent.futures.ThreadPoolExecutor` with `future.result(timeout=...)`. On timeout, the future is cancelled and `dg.Failure` is raised. Portable across all Dagster deployment shapes (unlike `signal.SIGALRM` which is Unix-main-thread only).

**Caveat**: Python threads can't be forcibly killed. The cancelled compute keeps running in the background (its result is discarded); on a well-behaved compute this is fine, on a stuck one you leak a thread until process exit. Acceptable trade for portability — the deadline is enforced from Dagster's perspective.

## Full YAML example

```yaml
type: dagster_community_components.TimeoutAssetComponent
attributes:
  asset_name: slow_api_call

  compute:
    kind: python
    python: "my_project.api:call_external"

  timeout_seconds: 60
  on_timeout: fail          # fail (default) | warn
```

## `@timeout` decorator

```python
import dagster as dg
from dagster_community_components import timeout

@dg.asset
@timeout(60, on_timeout="fail")
def slow_api_call(context):
    return call_slow_api()  # if this hangs > 60s → cancelled + dg.Failure
```

## Cross-run timeout counting

Every timeout emits an `AssetObservation` tagged `timeout_hit=<key>`. Sensors can count timeouts:

```python
@dg.sensor
def timeout_alert(context):
    from dagster import EventRecordsFilter, DagsterEventType
    records = context.instance.get_event_records(
        event_records_filter=EventRecordsFilter(event_type=DagsterEventType.ASSET_OBSERVATION),
        limit=100, ascending=False,
    )
    n = sum(1 for r in records if r.asset_observation.tags.get("timeout_hit") == "slow_api_call")
    if n >= 3:
        # 3+ timeouts recently — page oncall
        ...
```

## Composes with

- **`@smart_retry`** — retry on `TimeoutError` (classify it as transient in `rules`).
- **`@sla`** — observe overrun; timeout kills the compute at a higher threshold.
- **`@lifecycle`** — kill compute before it dirties staging.
- **`@cached`** — timeout only applies on cache miss.

## Historical baseline

Skip the guess. `derive_timeout_from_history` computes the timeout from the last N SUCCESSFUL runs:

```python
@dg.asset
@timeout(
    seconds=300,  # fallback if <3 successful runs exist
    derive_timeout_from_history={
        "n_runs": 10,
        "statistic": "p99",     # default; want to allow outlier-successful runs
        "multiplier": 1.5,       # 50% headroom over p99
    },
)
def slow_api_call(context):
    return call_slow_api()
```

Or in YAML:

```yaml
type: dagster_community_components.TimeoutAssetComponent
attributes:
  asset_name: slow_api_call
  timeout_seconds: 300
  derive_timeout_from_history:
    n_runs: 10
    statistic: p99
    multiplier: 1.5
  compute: {kind: python, python: my.module:slow_api_call}
```

Every SUCCESSFUL run emits `timeout_actual_seconds` in observation metadata; the derivation reads those from the event log and ignores runs that hit the timeout (so timeouts can't inflate the baseline). Falls back to `timeout_seconds` if fewer than 3 successful runs are available.

## Per-partition timeouts

Different partitions of the same asset can have different hard-kill thresholds.
Set `per_partition_timeout` to a `{partition_key: seconds}` map; the runtime picks
the matching override at compute time and falls back to `timeout_seconds` when no
key matches.

```yaml
timeout_seconds: 60                     # default fallback
per_partition_timeout:
  hourly: 30                             # hourly partitions killed at 30s
  daily: 600                             # daily partitions get 10 minutes
partition_matcher: exact                 # 'exact' (default) | 'prefix' | 'regex'
```

Python decorator:

```python
@dg.asset(partitions_def=dg.StaticPartitionsDefinition(["hourly", "daily"]))
@timeout(
    seconds=60,
    per_partition_timeout={"hourly": 30, "daily": 600},
)
def slow_api_call(context):
    return call_slow_api()
```

The emitted `timeout_seconds` metadata reflects the effective (per-partition)
threshold.

## What's not in v1 (roadmap)

- **Graceful shutdown** — inject a `cancel_token` into the compute so a well-behaved function can check + clean up before being abandoned.

## CLI demos using this template

| Demo | Setup script | What it shows |
|---|---|---|
| [`timeout_asset.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/timeout_asset.md) | [`setup_timeout_asset_demo.sh`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/setup_timeout_asset_demo.sh) | 100% offline. Demonstrates BOTH shapes side by side: (1) `@timeout` Python decorator hard-kills a `@dg.asset` past 1.0s (`SLEEP_SECONDS=0.3` → OK; `SLEEP_SECONDS=2.0` → `dg.Failure`), (2) `TimeoutAssetComponent { wraps: SyntheticDataGeneratorComponent }` — zero Python, wraps an inner 5000-row generator with a 10ms deadline. Both emit the same `timeout_hit` observations. |

```bash
curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-community-components-cli/main/examples/setup_timeout_asset_demo.sh | bash
```

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `timeout_seconds` | `float` | Kill compute if it exceeds this wall-clock time. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | — | Required when NOT using `wraps:`. |
| `group_name` | `str` | — | — |
| `description` | `str` | — | — |
| `owners` | `List[str]` | — | — |
| `tags` | `Dict[str, str]` | — | — |
| `kinds` | `List[str]` | — | Default: ['python', 'timeout']. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_matcher` | `str` | `"exact"` | How partition_key is matched against per_partition_timeout keys: 'exact' \| 'prefix' \| 'regex'. Default exact match. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `str` | — | — |
| `compute` | `Dict[str, Any]` | — | `{kind: python, python: 'mod:fn'}`. Mutually exclusive with `wraps`. |
| `wraps` | `Dict[str, Any]` | — | Wrap another DCC component's assets with a hard-kill timeout. Shape: `{type: 'dagster_community_components.<Component>', attributes: {...}}`. |
| `on_timeout` | `str` | `"fail"` | 'fail' raises dg.Failure. 'warn' logs + returns None (rare). |
| `timeout_key` | `str` | — | Shared key for cross-run timeout counting via event log. Defaults to asset_name. |
| `per_partition_timeout` | `Dict[str, float]` | — | Per-partition-key override. e.g. {'hourly': 30, 'daily': 300}. Falls back to timeout_seconds if no key matches. Only meaningful on partitioned assets. |
| `derive_timeout_from_history` | `Dict[str, Any]` | — | Auto-derive timeout_seconds from prior SUCCESSFUL materializations. Shape: {n_runs: 10, statistic: 'p99' \| 'p95' \| 'median' \| 'mean', multiplier: 1.5}. If set, this OVERRIDES timeout_seconds. Default statistic is 'p99… _(full docs in schema.json + component README)_ |

[//]: # (FIELDS:END)
