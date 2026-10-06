# Synthetic Commerce Quickstart

A self-contained Dagster project built entirely from [`dagster-community-components`](https://github.com/eric-thomas-dagster/dagster-component-templates) — no external accounts, API keys, or databases required. Everything runs against synthetic, generated-on-the-fly data, stored in a local DuckDB file.

It demonstrates every core Dagster primitive in one small, partitioned project:

| Primitive | Component | File |
|---|---|---|
| **IO manager** | `DuckDBIOManagerComponent` | `duckdb_io_manager.yaml` |
| **Ingestion (assets)** | `SyntheticDataGeneratorComponent` ×2 | `raw_customers.yaml`, `raw_orders.yaml` |
| **Asset checks** | `EnhancedDataQualityChecks` ×2 (+ 3 free schema-drift checks, one per asset, shipped automatically) | `raw_customers_check.yaml`, `raw_orders_check.yaml` |
| **Analytics (asset)** | `RFMSegmentationComponent` | `customer_rfm_segments.yaml` |
| **dbt project** | `EnrichedDbtProjectComponent` | `dbt_commerce.yaml` + `dbt/synthetic_commerce_dbt/` |
| **Schedules** | `CronScheduleComponent` ×2 (one partitioned, one not) | `daily_commerce_schedule.yaml`, `customer_master_schedule.yaml` |
| **Sensor** | `FilesystemMonitorSensorComponent` | `incoming_file_sensor.yaml` |

## Pipeline shape

```
raw_customers (unpartitioned, 1,000 rows, generated once)
       │
       ▼ (customer_id FK, not a Dagster asset dep)
raw_orders (daily-partitioned, 200 new rows/day) ──► customer_rfm_segments (daily-partitioned)
       │
       ▼ (dbt source: raw.raw_orders / raw.raw_customers)
dbt: stg_customers, stg_orders ──► customers, orders  (full-history marts, not partition-mapped)
```

- **`raw_customers`** — 1,000 synthetic customer profiles (`CUST000001`–`CUST001000`), an **unpartitioned master snapshot**. The generator always emits `customer_id`s starting from `CUST000001` for *any* call, so partitioning this the same way as orders would regenerate the same IDs every day instead of growing the customer base — unpartitioned is the correct choice here, not a limitation.
- **`raw_orders`** — **daily-partitioned**, 200 new orders generated per partition, referencing `raw_customers`' fixed ID range (the generator draws `customer_id` from `CUST000001`-`CUST001000` regardless of partition, so the join always has real signal).
- **`customer_rfm_segments`** — **daily-partitioned to match `raw_orders`**, via `RFMSegmentationComponent`'s `partition_date_column: order_date`. Each partition scores just that day's order batch. (A production RFM would more likely want a rolling lookback window rather than one score per day — this quickstart partitions it anyway to demonstrate a fully partition-aware chain end to end; see "Extending it" below.)
- **dbt (`customers`, `orders` marts)** — reads the **full accumulated history** of `raw.raw_orders`/`raw.raw_customers` (all partitions combined) via `dbt build` each run. `DbtProjectComponent` doesn't map individual dbt models to Dagster partitions, so this layer is intentionally whole-table, same as a typical dbt incremental/full-refresh mart.

## Why two schedules

`raw_customers` is unpartitioned; `raw_orders` + `customer_rfm_segments` are daily-partitioned. A single `CronScheduleComponent` job can't mix the two (its underlying job needs one consistent `partitions_def`), so there are two:

- **`daily_commerce_refresh`** — daily, partitioned (`daily`, start `2026-09-01`), drives **`daily_commerce_job`** = `[raw_orders, customer_rfm_segments]`.
- **`customer_master_refresh`** — weekly (Sundays), unpartitioned, drives **`customer_master_job`** = `[raw_customers]`.

## Schedule + sensor, same partitioned job

`incoming_file_sensor.yaml` watches `data/incoming/` for new `.csv` files and triggers **`daily_commerce_job`** — the *same* job the daily schedule drives — targeting a **specific partition** via `partition_mode: static_partition` + `partition_key_template: "{file_stem}"`. Name the dropped file after the date you want reprocessed (`YYYY-MM-DD.csv`, exactly Dagster's daily partition-key format) to simulate a delayed order-export arriving and backfilling that day:

```bash
echo "order_id,customer_id,total" > data/incoming/2026-09-22.csv
```

The sensor polls every 30 seconds by default (`minimum_interval_seconds`).

## DuckDB + partition_expr

`duckdb_io_manager.yaml` registers `DuckDBIOManagerComponent` as the project's default `io_manager`, writing every asset into `data/synthetic_commerce.duckdb` under the `raw` schema. A DB-table IO manager needs to know which column identifies a partition's rows (to delete/replace just that slice on rematerialize) — set via `metadata: {partition_expr: <column>}`:

- `raw_orders.yaml` sets `partition_expr: order_date` (a real column on that table).
- `customer_rfm_segments.yaml` sets `partition_expr: score_date` — a column `RFMSegmentationComponent` stamps onto its output specifically so a partitioned RFM snapshot has somewhere valid to point a DB IO manager's `partition_expr` at (added as part of this quickstart; see the component's CHANGELOG-equivalent commit).

`raw_customers` is unpartitioned, so it doesn't need one.

## The dbt layer

`dbt/synthetic_commerce_dbt/` is a small, hand-written dbt project (not vendored from `jaffle_shop` — built fresh against this project's own synthetic schema so it reads directly from `raw.raw_customers`/`raw.raw_orders`, no seed CSVs):

- `models/staging/stg_customers.sql`, `stg_orders.sql` — thin passthrough views over the two sources.
- `models/marts/customers.sql` — jaffle-shop-style customer summary (first/most-recent order date, lifetime order count + value).
- `models/marts/orders.sql` — enriched orders with an `is_first_order` flag per customer.

`profiles.yml` points at the **same** `data/synthetic_commerce.duckdb` file the Dagster IO manager writes to (`path: ../../data/synthetic_commerce.duckdb`, resolved relative to the dbt project directory — dagster-dbt invokes the dbt CLI with that directory as its cwd).

## Running it

```bash
cd quickstarts/synthetic_commerce
pip install -e ".[dev]"
dg dev
```

Materialize `raw_customers` once, then a few days of `raw_orders` (which pulls `customer_rfm_segments` along with it), then materialize the dbt assets — or just let `daily_commerce_refresh` / `customer_master_refresh` do it on schedule.

## Extending it

- **Hourly instead of daily**: swap `partition_type: daily` → `hourly` (and adjust `partition_start` to an hour-precision ISO string) on `raw_orders.yaml` and `customer_rfm_segments.yaml` — everything downstream (the schedule's partitioned job, the IO manager's `partition_expr` slicing) keeps working unchanged.
- **Rolling-window RFM instead of per-day**: unpartition `customer_rfm_segments` (drop its `partition_type`/`partition_start`/`partition_date_column`) so it consumes *all* of `raw_orders`' accumulated partitions at once with `lookback_days` doing the real recency filtering — more realistic for RFM specifically, at the cost of not being partition-mapped to `raw_orders` 1:1.
- **Swap the ingestion source**: replace `SyntheticDataGeneratorComponent` with `ParametricDataGeneratorComponent` (`assets/source/parametric_data_generator`) to define your own columns instead of the built-in `customers`/`orders` schemas.
- **More checks**: `asset_checks/` has 17 components (Great Expectations, Soda, Pandera, Monte Carlo, etc.) if you want something heavier than `EnhancedDataQualityChecks`' built-in check types.
- **Reverse ETL / sinks**: pair `customer_rfm_segments` or the dbt `customers` mart with a `sink` component (69 available) to push segments to a warehouse, CRM, or messaging platform.

## Notes

- All `type:` references here use the `dagster_community_components.` prefix — that's the real, installable package (`pyproject.toml`'s `[project] name`). Many older examples elsewhere in this repo still reference a stale `dagster_component_templates.` prefix left over from a prior rename; that prefix does not resolve to an installed package.
- `data/synthetic_commerce.duckdb` and `dbt/synthetic_commerce_dbt/target|dbt_packages|logs/` are gitignored — generated at run time, not source.
