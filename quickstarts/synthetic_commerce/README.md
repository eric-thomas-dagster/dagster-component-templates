# Synthetic Commerce Quickstart

A self-contained Dagster project built entirely from [`dagster-community-components`](https://github.com/eric-thomas-dagster/dagster-component-templates) — no external accounts, API keys, or databases required. Everything runs against synthetic, generated-on-the-fly data.

It demonstrates the five core Dagster primitives in one small project:

| Primitive | Component | File |
|---|---|---|
| **Ingestion (assets)** | `SyntheticDataGeneratorComponent` ×2 | `raw_customers.yaml`, `raw_orders.yaml` |
| **Asset checks** | `PandasDataframeCheckComponent` ×2 (+ 3 free schema-drift checks, one per asset, shipped automatically) | `raw_customers_check.yaml`, `raw_orders_check.yaml` |
| **Analytics (asset)** | `RFMSegmentationComponent` | `customer_rfm_segments.yaml` |
| **Schedule** | `CronScheduleComponent` | `daily_commerce_schedule.yaml` |
| **Sensor** | `FilesystemMonitorSensorComponent` | `incoming_file_sensor.yaml` |

## Pipeline shape

```
raw_customers ──┐
                 ├─> (independent)
raw_orders ─────┴──> customer_rfm_segments
```

- `raw_customers`: 1,000 synthetic customer profiles (`CUST000001`–`CUST001000`).
- `raw_orders`: 5,000 synthetic orders referencing that same customer-id range, so the RFM join has real signal.
- `customer_rfm_segments`: Recency/Frequency/Monetary scoring + segment labels (Champions, At Risk, Lost, etc.) computed from `raw_orders`.

Both ingestion assets carry a blocking/non-blocking `PandasDataframeCheckComponent` validating required columns and dtypes, on top of the free column-schema-drift check every component in this library ships automatically.

## Schedule + sensor, same job

`daily_commerce_schedule.yaml` defines **`daily_commerce_job`** (all three assets) on a daily cron (`0 6 * * *`). `incoming_file_sensor.yaml` watches `data/incoming/` for new `.csv` files and triggers *that same job* by name — no duplicate job definition, just two different triggers (time-based and event-based) pointed at one job. Drop a file into `data/incoming/` while the project is running (`dg dev`) to see the sensor fire:

```bash
echo "order_id,customer_id,total" > data/incoming/export_test.csv
```

The sensor polls every 30 seconds by default (`minimum_interval_seconds`).

## Running it

```bash
cd quickstarts/synthetic_commerce
pip install -e ".[dev]"
dg dev
```

Then materialize `raw_customers` and `raw_orders` from the UI (or let the schedule/sensor do it), and `customer_rfm_segments` will pick up `raw_orders`' output automatically.

## Extending it

- **Partitioning**: both ingestion components accept `partition_type: daily` + `partition_start` — try partitioning `raw_orders` for an incremental daily-load pattern instead of regenerating the full dataset each run.
- **Swap the ingestion source**: replace `SyntheticDataGeneratorComponent` with `ParametricDataGeneratorComponent` (`assets/source/parametric_data_generator`) to define your own columns instead of the built-in `customers`/`orders` schemas.
- **More checks**: `asset_checks/` has 17 components (Great Expectations, Soda, Pandera, Monte Carlo, etc.) if you want something heavier than the pandas-only check used here.
- **Reverse ETL / sinks**: pair `customer_rfm_segments` with a `sink` component (69 available) to push segments to a warehouse, CRM, or messaging platform.

## Notes

- All `type:` references here use the `dagster_community_components.` prefix — that's the real, installable package (`pyproject.toml`'s `[project] name`). Many older examples elsewhere in this repo still reference a stale `dagster_component_templates.` prefix left over from a prior rename; that prefix does not resolve to an installed package.
