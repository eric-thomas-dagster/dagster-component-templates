# Fivetran Usage Observation Sensor

Attaches Fivetran usage (MAR — Monthly Active Rows) to your asset graph as a numeric metric you can chart in Dagster+ Insights.

## Why this exists — and why it isn't a Fivetran API call

Fivetran's REST API has no endpoint for usage, MAR, or billing data at all — checked against the official API reference directly (22 resource categories: Connection, Destination, Group, Transformation, Webhooks, etc. — none of them usage-related). The only way to get this data is the **Fivetran Platform Connector**: a regular connection Fivetran auto-adds to every destination, syncing on a schedule like any other connector, which writes usage (among other things) into a destination table, `INCREMENTAL_MAR`. There's no API to read the numbers — only SQL against that table, once the Platform Connector has synced.

**Prerequisite**: you need the Fivetran Platform Connector already set up and syncing into a warehouse this sensor can query. It's not something this component creates for you.

## The asset-key match isn't a guess

`INCREMENTAL_MAR` is keyed by `(schema_name, table_name)` — not just the connector. That happens to be *exactly* how `dagster_fivetran`'s default translator builds a Dagster asset key for a synced table: `AssetKey([schema_name, table_name])`. So under default translation settings, this sensor's asset-key reconstruction from a query row is exact, not fuzzy. If your `FivetranAccountComponent` uses a custom `translation:` that changes the key shape, set `asset_key_prefix` to match, or this sensor's observations will land on asset keys that don't exist in your graph.

## Month-to-date, not a rolling window

This sensor recomputes the full **cumulative month-to-date MAR** per table every tick — summed from the start of the current calendar month through today, not a short trailing window. That's deliberate: a rolling "last N days" window would permanently miss a Fivetran restatement to any day older than the window, with no way to ever catch up. Recomputing the whole month every tick means a restatement to *any* day this month gets picked up on the very next tick.

## Reports a delta, not the cumulative level — this matters

Dagster+ Insights custom metrics have **no configurable aggregation**. You register a metadata key, not a sum/average/last-value choice, and — matching every built-in Insights metric (credits, compute duration, etc., all inherently additive) — whatever numeric values get reported under that key are **summed** to build each period's chart point.

That means reporting the cumulative total itself on every tick would be wrong: Insights would sum together every reading this sensor ever emitted, so a real total of 1,000 would chart as many times that, and get worse the more often the sensor ticks. So this sensor reports the **delta** — the net change in the cumulative total since the last tick that saw a change for that asset — under `fivetran/mar`. Summing deltas over any period telescopes back to the real net change for that period, regardless of how many ticks happened. The cumulative level is still attached as `fivetran/mar_month_to_date`, but as *text*, specifically so it can't be mistakenly registered as a second, double-counting metric.

One structural limit this can't remove: Insights attaches a value to the *observation's own timestamp*, not any date in its metadata, so a late restatement's delta lands on the day it was detected, not the Fivetran day it technically belongs to. The running total still converges to correct — day-level attribution of a late correction just won't be perfectly placed.

The per-asset running total resets at a calendar-month boundary (tracked in the sensor's own cursor), so the first tick of a new month reports that month's fresh total as its delta, not a large negative blip from diffing against last month's final number. The matching scope limit: a restatement to a *prior* month's data, arriving after this sensor has already rolled over, falls outside that tick's month-to-date query and won't be caught — scoped to Fivetran's own billing period rather than an arbitrary window.

## Only emits on a real change

A Platform Connector typically syncs once a day; this sensor polls more often than that by default (`check_interval_seconds: 3600`). Most ticks should produce **zero** events — it only emits when an asset's delta is non-zero.

```yaml
type: dagster_community_components.FivetranUsageObservationSensorComponent
attributes:
  sensor_name: fivetran_usage_observer
  resource_key: snowflake
  mar_table: "ANALYTICS.FIVETRAN_LOG.INCREMENTAL_MAR"
  check_interval_seconds: 3600
```

`resource_key` is any Dagster resource exposing `.get_connection()` (a context manager yielding a DB-API-ish connection — `.cursor()` or `.execute()`), the same convention this registry's other warehouse-querying components use.

## Charting it in Dagster+ Insights

Register `fivetran/mar` (not `fivetran/mar_month_to_date`) once under **Deployment → Custom metrics** in the Dagster+ UI, and every asset this sensor touches starts contributing to it automatically — summed daily, weekly, or monthly by Insights, it reconstructs the real MAR for that period.

`emit_materialization: false` (default) means these are `AssetObservation`s — free of Dagster+ credit charges, and a MAR tick won't accidentally trigger downstream `AutomationCondition.eager()` runs (usually not what you want just because a usage number changed). Set `true` only if you specifically want usage changes to drive downstream automation.

## Demo mode

`demo_mode: true` returns synthetic, deterministic cumulative MAR rows that grow across ticks (one table that's always the same, two that vary) — no `resource_key` or real warehouse needed. Useful for confirming the delta logic end to end before pointing this at a real `INCREMENTAL_MAR` table.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `sensor_name` | `str` | Unique sensor name. |
| `resource_key` | `str` | Dagster resource exposing `.get_connection()` (a context manager yielding a DB-API-ish connection -- `.cursor()` or `.execute()`) against the warehouse the Fivetran Platform Connector writes to. Not used when demo_mode=True. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `mar_table` | `str` | `"incremental_mar"` | Fully-qualified INCREMENTAL_MAR table name, exactly as your warehouse expects it, e.g. 'MYDB.FIVETRAN_LOG.INCREMENTAL_MAR' (Snowflake) or 'my_project.fivetran_log.incremental_mar' (BigQuery). |
| `free_type_filter` | `str` | `"PAID"` | Filter on INCREMENTAL_MAR's free_type column -- 'PAID' (default) matches Fivetran's own billing-focused sample queries. Set to null/empty to include free usage too. |
| `asset_key_prefix` | `List[str]` | — | Prefix segments to prepend to the reconstructed asset key (schema_name, table_name). Leave unset to match dagster_fivetran's DEFAULT translator exactly (AssetKey([schema_name, table_name])) -- only set this if your Fivet… _(full docs in schema.json + component README)_ |
| `check_interval_seconds` | `int` | `3600` | Seconds between checks. The Platform Connector itself typically syncs once a day, so hourly is already generous. |
| `emit_materialization` | `bool` | `false` | When False (default), emits AssetObservation -- free of Dagster+ credit charges, and a MAR tick won't trigger downstream AutomationCondition.eager() runs (usually not what you want from a usage number changing). Set True… _(full docs in schema.json + component README)_ |
| `demo_mode` | `bool` | `false` | Skip the real warehouse query entirely -- returns synthetic, deterministic MAR rows that change on alternating ticks. No resource_key or real warehouse required. |

[//]: # (FIELDS:END)
