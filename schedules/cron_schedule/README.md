# CronSchedule

Defines a `ScheduleDefinition` + an underlying asset job that materializes the configured asset keys whenever the cron expression fires. Replaces the ad-hoc `dg launch --assets '*'` pattern with a real Dagster schedule that shows up in the UI.

Two modes — same field surface:

1. **Un-partitioned** (default): `ScheduleDefinition(job=..., cron_schedule=<your cron>)`. `cron_expression` is required.
2. **Time-partitioned** (opt-in via `partition_type` / `partition_dimensions`): `build_schedule_from_partitioned_job`. Each tick auto-targets the most-recent finished partition. `cron_expression` is optional — Dagster infers cadence from the partitions_def. When supplied, the cron is decomposed into `minute_of_hour` / `hour_of_day` / `day_of_week` / `day_of_month` args and combined with the partitions_def cadence (Dagster forbids passing a raw `cron_schedule` for time-partitioned jobs).

## Capabilities at a glance

- **Any partition shape** — daily / weekly / monthly / hourly, multi-axis, plus escape hatches for static / dynamic (rejected in partitioned mode with a helpful error).
- **Cron validated against cadence** — a daily cron paired with hourly partitions raises `ValueError` at load time instead of failing silently.
- **Non-boundary fire times in partitioned mode** — `cron_expression: "15 9 * * *"` + `partition_type: daily` produces a daily schedule that fires at 09:15 instead of the default 00:00.
- **Multi-axis partitions** — declare `partition_dimensions: [{name, type, start, ...}]` for time × static / time × dynamic combinations.
- **Calendar conditions** — `calendar_condition: last_day_of_month | last_business_day_of_month | first_business_day_of_month` gates a cron on a calendar boundary cron alone can't express (see below).

## Airflow migration: `@once`, `end_date`, `logical_date`

- **`cadence: once`** — Airflow's `@once` DAG (or a dbt-deploy-triggered single run): builds a cursor-gated sensor instead of a schedule. It fires exactly one `RunRequest` the first time it evaluates while `default_status: RUNNING`, then returns `SkipReason` forever after. Mutually exclusive with `cron_expression`, partitioning, `end_date`, and `add_logical_date_tag` (none of those apply to a one-shot trigger).
- **`end_date`** — Airflow's DAG `end_date`. Un-partitioned: the schedule stops firing once `scheduled_execution_time` reaches this date. Partitioned: also passed straight through to the partitions_def's own `end_date`, so no partition past it is ever offered. In the partitioned path, setting `end_date` (or `add_logical_date_tag`) requires `cron_expression` to be set explicitly too — the auto-inferred cron used when neither is set doesn't apply to this manually-resolved path.
- **`add_logical_date_tag`** — tags each run with `logical_date`, matching Airflow's run-identifying timestamp: the partition window's start (Airflow's own definition — `logical_date` = start of the data interval) in partitioned mode, or the scheduled trigger time in un-partitioned mode.

```yaml
# Run once, right after this code location deploys (if default_status: RUNNING)
attributes:
  schedule_name: run_dbt_models_once
  asset_keys: ["dbt/models"]
  cadence: once
  default_status: RUNNING
```

```yaml
# Daily, tagged with logical_date, stopping after a known cutover date
attributes:
  schedule_name: legacy_ingest
  asset_keys: ["raw/legacy_orders"]
  cron_expression: "0 6 * * *"
  add_logical_date_tag: true
  end_date: "2026-06-01"
```

## `calendar_condition`: last day of month, business-day boundaries

Plain cron can't express "the actual last day of the month" (28-31, varies) or a business-day
boundary at all — only a fixed day-of-month number. `calendar_condition` closes that gap: set
`cron_expression` to fire on every CANDIDATE day near the boundary, and `calendar_condition` gates
every tick down to the one true match, skipping the rest.

```yaml
# Replaces a hand-written schedule that fires on 0 23 28-31 * * and then has to work out,
# inside the job itself, whether tomorrow is actually the 1st of the month.
attributes:
  schedule_name: month_end_close
  asset_keys: ["finance/month_end_journal"]
  cron_expression: "0 23 28-31 * *"
  calendar_condition: last_day_of_month
```

Three conditions: `last_day_of_month`, `last_business_day_of_month`, `first_business_day_of_month`
(pair the last two with a cron day-of-month range like `28-31` / `1-3` to cover every month's real
range of candidate dates). "Business day" means Mon–Fri only — there's no holiday calendar here;
holidays vary by locale and company, so a real holiday list needs a hand-written sensor instead.
Only valid with `cadence: scheduled`; in the partitioned path it requires `cron_expression` to be
set explicitly, same as `end_date`/`add_logical_date_tag` above.

## Airflow `CronPartitionTimetable` / `PartitionedAssetTimetable` → Dagster mapping

Verified against Airflow's real classes (`airflow.sdk`), not guessed — fetched directly
from the official rendered source of Airflow's own `example_asset_partition` example DAG
(not blog posts or PR threads). **Version correction from an earlier pass of this doc:**
`CronPartitionTimetable` is real as of Airflow 3.2.0. The richer mapper/window vocabulary
below (`RollupMapper`, `FanOutMapper`, `FixedKeyMapper`, the window classes, `WaitForAll`,
`MinimumCount`) is Airflow **3.3.0**, not 3.2 — an earlier pass of this section
misattributed it.

**Producer side — `CronPartitionTimetable(cron, timezone=..., run_offset=None,
key_format='%Y-%m-%dT%H:%M:%S')`**: a DAG/asset's partition key is the cron-fired run's
timestamp. Maps to `partition_type: cron` (`TimeWindowPartitionsDefinition`) on this
repo's own schedule and asset components. One real gap closed from the earlier pass:
`key_format` (an strftime/strptime string) must actually match Dagster's own `fmt=` for
the partition keys to line up across the two systems — it's not just "turn on cron mode,"
check that the two format strings agree.

**Consumer side — `PartitionedAssetTimetable(assets=..., default_partition_mapper=... |
partition_mapper_config={asset: mapper})`**: triggers when upstream partitioned asset(s)
update, translating the upstream partition key into this DAG's own key space via a
*mapper*. Confirmed directly from the example DAG's real constructor calls:

| Airflow mapper/window | Dagster equivalent |
|---|---|
| `IdentityMapper` (default passthrough) | No mapping needed — same partitions_def on both sides. |
| `StartOfHourMapper` / `StartOfDayMapper` / `StartOfWeekMapper` / `StartOfMonthMapper` / `StartOfYearMapper` (coarsen time granularity) | Nothing to build — `TimeWindowPartitionMapping` already infers this automatically from the two sides' own partitions_defs. |
| `AllowedKeyMapper([...])` (static allow-list, not time-based) | `StaticPartitionsDefinition` membership — equivalent to this repo's `partition_type: static`. |
| `ProductMapper(a, b)` (splits a composite `"region\|timestamp"` string key, maps each half separately) | **No native Dagster primitive parses a delimited string key.** The converter needs to split the key itself and build a `partition_dimensions` multi-axis config from the parts. |
| `FixedKeyMapper("x")` (collapses every upstream key onto one fixed downstream key) | No direct `PartitionMapping` equivalent, but combined with `RollupMapper` below this is the categorical-rollup case `partition_window_sensor` covers (`window_type: segment`). |
| `RollupMapper(upstream_mapper, window, wait_policy)` + `DayWindow`/`WeekWindow`/`MonthWindow`/`SegmentWindow` + `WaitForAll`/`MinimumCount(n)` (N upstream partitions → 1 downstream run, waiting for the window to fill) | **No native Dagster primitive** — confirmed live: `AutomationCondition.all_deps_match(...)` is documented ("at least one partition of... the target's dependencies") and confirmed to require only ONE matching partition per dependency, not ALL partitions mapped by it. Use [`partition_window_sensor`](https://dagster-component-ui.vercel.app/c/partition_window_sensor) (`mode: rollup`), built for exactly this. |
| `FanOutMapper(upstream_mapper, window, max_downstream_keys \| downstream_mapper)` (1 upstream partition → N downstream runs, the inverse of Rollup) | Same gap, inverse direction — `partition_window_sensor` (`mode: fan_out`). |

Scope note: this now includes a component (`partition_window_sensor`) for the Rollup/
FanOut/window/wait-policy family — the one part of the mapping that genuinely had no
Dagster-side primitive to fall back on. Everything else above either needs zero new code
or needs converter-side string parsing (`ProductMapper`) rather than a Dagster primitive.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `schedule_name` | `str` | Unique schedule name shown in the UI. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `tags` | `Union[Dict[str, Any], List[str]]` | — | Tags applied to runs created by this schedule. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned. Note: only time-based types ('daily'/'weekly'/'monthly'/'hourly') work with this schedule's partitioned-job… _(full docs in schema.json + component README)_ |
| `partition_cron_schedule` | `str` | — | Required when partition_type='cron'. A cron expression restricting the date axis to a custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on Dagster's TimeWindowPartitionsDefinition(cron_schedule=...). |
| `partition_cron_fmt` | `str` | `"%Y-%m-%d"` | strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule. |
| `partition_end_offset` | `int` | `0` | Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition. 0 (the Dagster default) means a time-based partition set only ever holds COMPLETED windows -- e.g. a daily set never includes today… _(full docs in schema.json + component README)_ |
| `partition_timezone` | `str` | — | IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default). |
| `partition_start` | `str` | — | Partition start in ISO format (e.g. '2024-01-01' or '2024-01-01-00:00' for hourly). Required for time-based types. |
| `partition_values` | `Union[str, List[str]]` | — | Comma-separated values for static or multi partitioning. Only used if you also set partition_type='multi' (and even then the schedule rejects pure static — see partition_type). |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set. |

### Sensor configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `job_name` | `str` | — | Name of the underlying job (defaults to '<schedule_name>_job'). |
| `default_status` | `str` | `"STOPPED"` | 'RUNNING' (live) or 'STOPPED' (dormant; user must enable). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `cron_expression` | `str` | — | Cron expression (5 fields: m h dom mon dow). E.g. '0 9 * * *' = daily 09:00. REQUIRED when no partition fields are set; OPTIONAL in the partitioned-job path (cadence is inferred from the partitions_def). If supplied in t… _(full docs in schema.json + component README)_ |
| `asset_keys` | `List[str]` | — | Slash-separated asset keys to materialize on each tick. Set this, asset_check_keys, or both. |
| `asset_check_keys` | `List[str]` | — | Asset check keys to run on each tick, as '<asset_key>:<check_name>' (asset key segments slash-separated, e.g. 'marts/orders:row_count_check'). Runs just the check -- via Dagster's real AssetSelection.checks(...), confirm… _(full docs in schema.json + component README)_ |
| `execution_timezone` | `str` | — | IANA timezone for cron evaluation, e.g. 'America/Los_Angeles'. Applied in the un-partitioned path (Dagster falls back to UTC when None). Silently IGNORED in the partitioned path — Dagster's build_schedule_from_partitione… _(full docs in schema.json + component README)_ |
| `cadence` | `str` | `"scheduled"` | 'scheduled' (default): normal cron/partitioned schedule, as below. 'once': mirrors Airflow's @once -- fires a single run the first time this component's sensor evaluates while RUNNING, then never fires again. Builds a se… _(full docs in schema.json + component README)_ |
| `end_date` | `str` | — | ISO date or datetime (e.g. '2026-06-01'; a value with no timezone is assumed UTC) after which this schedule stops firing -- mirrors Airflow's DAG end_date. Un-partitioned: each tick is skipped once scheduled_execution_ti… _(full docs in schema.json + component README)_ |
| `add_logical_date_tag` | `bool` | `false` | If true, tags each run with 'logical_date' -- Airflow's run-identifying timestamp. Partitioned: the partition window's start (Airflow's own definition: logical_date = start of the data interval). Un-partitioned: the sche… _(full docs in schema.json + component README)_ |
| `add_interval_tags` | `bool` | `false` | If true, also tags each run with 'data_interval_start' / 'data_interval_end' (ISO8601) -- the window of data this run covers, for pipelines that process 'the interval that just ended' rather than a single point in time… _(full docs in schema.json + component README)_ |
| `skip_first_tick` | `bool` | `false` | If true, the first tick this schedule ever fires for is skipped instead of launching a run -- avoids a long/expensive run firing immediately right after a fresh deploy, for a schedule whose cadence (e.g. weekly) means th… _(full docs in schema.json + component README)_ |
| `calendar_condition` | `str` | — | 'last_day_of_month' \| 'last_business_day_of_month' \| 'first_business_day_of_month'. When set, each tick is ADDITIONALLY gated by this calendar check (on the tick's own scheduled_execution_time date, in execution_timezo… _(full docs in schema.json + component README)_ |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (when partition_type='dynamic'). NOTE: the schedule rejects dynamic-only partitions. |

[//]: # (FIELDS:END)

## Example YAML
```yaml
type: dagster_community_components.CronScheduleComponent
attributes:
  schedule_name: nightly_etl
  cron_expression: "0 2 * * *"
  asset_keys: [orders_clean, customer_segments, daily_revenue]
  execution_timezone: America/Los_Angeles
  default_status: STOPPED
  tags:
    team: data-platform
```

### Partitioned-job mode

When any time-based partition field (`partition_type`, `partition_dimensions`) is set, the component switches from `ScheduleDefinition` to `build_schedule_from_partitioned_job`. Each tick auto-targets the most-recent finished partition — without this switch, `ScheduleDefinition` against a partitioned asset job errors with "no partition key provided" on every tick.

`cron_expression` is OPTIONAL in this mode. When omitted, Dagster fires at the natural period boundary (midnight for daily, top-of-hour for hourly). When supplied:

- It's **validated against the cadence** (`0 * * * *` fine for hourly; `0 9 * * *` for daily; `15 * * * *` for hourly-at-:15 fine; but `0 * * * *` + `daily` raises).
- The cron is **decomposed** into `minute_of_hour` / `hour_of_day` / `day_of_week` / `day_of_month` args and combined with the partitions_def cadence. Dagster forbids passing a raw `cron_schedule` for time-partitioned jobs — this component handles the decomposition transparently.
- Only **single integers** are supported in the fixed positions (e.g. `15 9 * * *` OK; `*/15 9 * * *` not — the offset args accept one value per field, not lists / ranges / steps).

```yaml
# Hourly partitions, firing at :15 past each hour instead of :00
type: dagster_community_components.CronScheduleComponent
attributes:
  schedule_name: hourly_orders_at_15
  cron_expression: "15 * * * *"
  asset_keys: [python_hourly_orders_for_pipe, orders_to_s3]
  partition_type: hourly
  partition_start: "2026-05-26-00:00"
  default_status: RUNNING
```

```yaml
# Daily partitions, firing at 09:15 every day
type: dagster_community_components.CronScheduleComponent
attributes:
  schedule_name: daily_reports_at_915
  cron_expression: "15 9 * * *"
  asset_keys: [daily_ledger, daily_metrics]
  partition_type: daily
  partition_start: "2024-01-01"
```

`partition_type: static` and `partition_type: dynamic` raise `ValueError` in the partitioned path — `build_schedule_from_partitioned_job` needs a time-based partitions_def. For non-time partitions, use the un-partitioned schedule and select keys downstream.

### Scheduling asset checks, not just assets

`asset_keys` only ever selected assets -- there was no way to put an asset
check on its own schedule (e.g. "re-validate this data quality check every
night without re-materializing the asset"). `asset_check_keys` fixes that:
set it (as `'<asset_key>:<check_name>'` strings, asset key segments
slash-separated), `asset_keys`, or both. Confirmed directly against
Dagster's own `AssetSelection.checks(...)`: a check-only selection really
does run with zero re-materialization of the parent asset.

```yaml
# Re-run a data quality check nightly without re-materializing the (expensive) asset
type: dagster_community_components.CronScheduleComponent
attributes:
  schedule_name: nightly_orders_check
  cron_expression: "0 3 * * *"
  asset_check_keys: ["marts/orders:row_count_check"]
```

```yaml
# Materialize an asset AND run an unrelated asset's check in the same tick
type: dagster_community_components.CronScheduleComponent
attributes:
  schedule_name: nightly_both
  cron_expression: "0 2 * * *"
  asset_keys: [daily_revenue]
  asset_check_keys: ["marts/orders:freshness_check"]
```

### Timezone caveat

`execution_timezone` is honored in the un-partitioned path but **silently ignored** in the partitioned path — Dagster uses the partitions_def's own timezone (it forbids passing `execution_timezone` when a time-partitioned partitions_def is present). Set the timezone on the partitions_def if needed.

## See also

- Sister component: [`interval_schedule`](../interval_schedule/README.md) — same shape but with `every: 15m` / `every: 1h` instead of full cron expressions. Same partition surface. Use when you don't need cron's expressiveness.
