# `SlaAssetComponent` + `@sla` decorator

Enforce **wall-clock SLAs** on asset compute — with cross-run escalation after N breaches in a window.

Different from `@dg.asset(freshness_policy=...)`: that governs SOURCE freshness ("when should the upstream data be refreshed"). `@sla` enforces WALL-CLOCK compute duration ("this compute should finish in <= 60 seconds"). Complementary, not redundant.

## Two shapes

| Shape | Use when |
|---|---|
| **`SlaAssetComponent`** (YAML) | New SLA-wrapped asset |
| **`@sla` decorator** (Python) | Wrap an existing `@dg.asset` |

## What it does

1. Timer starts before the compute call, stops after.
2. If elapsed > `expected_duration_seconds` → **breach**:
   - Emits `AssetObservation` tagged `sla_breach=<key>` with typed metadata (actual, expected, overrun %, escalated).
   - Logs a warning (or raises `dg.Failure` if `on_breach: fail`).
3. If `escalate_after_n_breaches` set → counts breaches in the sliding window (via `context.instance.get_event_records`); tags observation `ESCALATED` if threshold hit. Sensors can watch for this and page.

## Why this belongs in Dagster

Every primitive rides on Dagster events:
- **Breach event** → `AssetObservation` with typed `MetadataValue`
- **Cross-run breach history** → `context.instance.get_event_records` filtered on the breach tag
- **Escalation** → observation tags, sensors watch them
- **UI trend** → the observation panel shows SLA overrun % over time

Doing this outside Dagster requires a metrics store + alerting integration + retention. Doing it inside: 20 lines of config.

## Full YAML example

```yaml
type: dagster_community_components.SlaAssetComponent
attributes:
  asset_name: slow_report
  compute:
    kind: python
    python: "my_project.reports:build_slow_report"

  expected_duration_seconds: 60
  on_breach: warn                  # warn | fail
  escalate_after_n_breaches: 3
  escalate_window_seconds: 3600    # 1 hour
```

## Composability — `wraps:` an existing component

The Python `@sla` decorator stacks naturally on any `@dg.asset`. The YAML equivalent lets you wrap an **existing DCC component** without touching its config — the outer `SlaAssetComponent` uses `wraps:` in place of its own `compute:`:

```yaml
# The outer SLA times the inner component's compute; the inner is unchanged.
type: dagster_community_components.SlaAssetComponent
attributes:
  expected_duration_seconds: 60
  on_breach: warn
  sla_key: report_sla
  wraps:
    type: dagster_community_components.CachedAssetComponent
    attributes:
      asset_name: sales_report
      cache_dir: s3://my-cache/reports/
      code_version: v1
      compute:
        kind: python
        python: "my_project.reports:build_sales_report"
```

Materialization semantics: **one asset** is registered (`sales_report`) — no duplication. Each run runs the inner's compute (cache miss → real work, cache hit → parquet load) inside the outer's SLA timer. If wall-clock > `expected_duration_seconds`, the outer emits `sla_breach=report_sla` observation + breach metadata on the materialization.

Stacks arbitrarily deep — one wrap layer per YAML level:

```yaml
type: dagster_community_components.SlaAssetComponent          # outer SLA
attributes:
  expected_duration_seconds: 60
  wraps:
    type: dagster_community_components.SlaAssetComponent      # inner SLA
    attributes:
      expected_duration_seconds: 30
      wraps:
        type: dagster_community_components.CachedAssetComponent
        attributes:
          asset_name: report
          ...
```

Direct Python analog:

```python
@dg.asset
@sla(expected_duration_seconds=60)   # outer
@sla(expected_duration_seconds=30)   # inner
@cached(cache_dir=..., code_version="v1")
def report(context):
    return build()
```

### Caveats (prototype)

- **Single-key inner assets only.** If the wrapped component produces a `@multi_asset` (multiple keys), the wrap is skipped for that AssetsDefinition and it passes through unchanged. Multi-asset support is a follow-up.
- **`code_version` in nested YAML must be a non-numeric string.** YAML's default loader coerces quoted numeric strings inside `Dict[str, Any]` fields (`code_version: "1.0"` → float 1.0). Use `v1`, `1.0.0`, or any non-numeric string until Dagster's loader treats nested dict values type-strictly.
- **Typed `ins:` (upstream inputs with dagster_type) fall back to `deps:` (ordering-only).** Rebuild uses the AssetSpec's dep list; typed input configuration on the inner asset isn't preserved through the wrap.
- **Metadata collision on stacked wraps.** Both layers write `sla_actual_seconds`, `sla_expected_seconds`, `sla_breach` — outer overwrites inner in the materialization. Use `sla_key` on each layer to disambiguate in observations.

## `@sla` decorator

```python
import dagster as dg
from dagster_community_components import sla

@dg.asset
@sla(
    expected_duration_seconds=60,
    on_breach="warn",
    escalate_after_n_breaches=3,
    escalate_window_seconds=3600,
)
def slow_report(context):
    return build_slow_report()
```

## Metadata reported

- `sla_actual_seconds` (float)
- `sla_expected_seconds` (float)
- `sla_breach` (bool)
- On breach: `sla_overrun_pct` (float), `sla_escalated` (bool)

Plus one `AssetObservation` per breach with the same fields — searchable in the event log.

## Composes with

- **`@smart_retry`** — retries count against the SLA budget (each retry adds to elapsed time).
- **`@lifecycle`** — SLA wraps the full write + audit + publish path.
- **`@data_contract`** — contract violations don't count as SLA breaches (they're their own event).
- **Any sensor** — watch for `sla_breach` observations, fire PagerDuty / Slack alerts.

## Alerting recipe

```python
@dg.sensor(name="sla_escalation_watcher")
def sla_escalation_watcher(context):
    from dagster import EventRecordsFilter, DagsterEventType
    records = context.instance.get_event_records(
        event_records_filter=EventRecordsFilter(
            event_type=DagsterEventType.ASSET_OBSERVATION,
        ),
        limit=50, ascending=False,
    )
    for r in records:
        tags = r.asset_observation.tags or {}
        if tags.get("sla_escalated") == "true":
            # Escalated breach — page oncall
            ...
```

## What's not in v1 (roadmap)

- **Per-partition SLAs** — hourly partition has different SLA than daily.
- **Historical baseline** — auto-derive `expected_duration_seconds` from median of last N runs.
- **Budget tracking** — burn-rate visualization ("you have X SLA-seconds left this month").

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Dagster asset name. |
| `compute` | `Dict[str, Any]` | `{kind: python, python: 'mod:fn'}`. Any return type. |
| `expected_duration_seconds` | `float` | Wall-clock SLA threshold. Compute time > this = breach. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | — |
| `description` | `str` | — | — |
| `owners` | `List[str]` | — | — |
| `tags` | `Dict[str, str]` | — | — |
| `kinds` | `List[str]` | — | Asset kinds. Default: ['python', 'sla', 'observability']. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `str` | — | — |
| `on_breach` | `str` | `"warn"` | 'warn' materializes + emits AssetObservation. 'fail' raises dg.Failure. |
| `escalate_after_n_breaches` | `int` | — | Count breaches in escalate_window_seconds. If >= N, tag observation ESCALATED. |
| `escalate_window_seconds` | `float` | `3600` | Sliding window for breach counting (default 1 hour). |
| `sla_key` | `str` | — | Shared SLA key. Defaults to asset_name. Set explicitly to group multiple assets under one SLA budget. |

[//]: # (FIELDS:END)
