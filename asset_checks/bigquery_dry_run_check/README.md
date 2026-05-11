# BigQuery Dry-Run Cost Check

Pre-flight cost guardrail for a BigQuery query. Runs a [dry-run](https://cloud.google.com/bigquery/docs/dry-run-queries) (server-side query plan, no data read, **no cost charged**) and fails the asset check if estimated bytes exceed your budget.

```yaml
type: dagster_component_templates.BigqueryDryRunCheckComponent
attributes:
  asset_key: nightly_revenue_rollup
  sql: |
    SELECT customer_id, SUM(amount_cents) AS revenue
    FROM `my-project.warehouse.orders`
    WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    GROUP BY customer_id
  max_bytes: 100000000000   # 100 GB
  blocking: true
```

## Why dry-run

A dry-run returns the BigQuery query plan + estimated bytes scanned without actually running the query. It's **free** and **fast** (typically < 1s). Perfect for asset checks that need to gate on cost before a real run.

## ⚠️ Bytes is the ground truth, not cost

BigQuery's dry-run API only returns `total_bytes_processed`. There is **no `cost_usd` field** — Google can't compute cost server-side because pricing is region-, edition-, and contract-specific.

This component supports three caps; pick the one that matches your billing model:

| You're on... | Use | Why |
|---|---|---|
| **On-demand pricing** | `max_bytes` (primary) and optionally `max_cost_usd` for readability | Bytes is exact; cost is convenience-converted at `on_demand_price_per_tb_usd` |
| **Flat-rate / capacity reservations** | `max_slot_ms` | You pay for slots whether used or not — bytes/cost are irrelevant |
| **Mixed / unsure** | `max_bytes` only | Always safe; doesn't depend on pricing assumptions |

### About `max_cost_usd`

It's computed locally as `bytes_scanned × on_demand_price_per_tb_usd / 1e12`. The rate defaults to **$6.25/TB (US on-demand, 2025)** but is YOUR responsibility to keep current — Google has changed it before (and EU is $6.25, asia-northeast1 is higher). If you want pinpoint accuracy, set `max_bytes` and ignore `max_cost_usd`.

## Use cases

| Pattern | Setup |
|---|---|
| Daily cost guardrail | `max_bytes` ~ 2× your normal scan size; alerts on anomalous bloat |
| Hard ceiling on ad-hoc queries | `max_bytes` ~ your daily slot budget; `blocking: true` |
| CI gate for new queries | Run in CI on every PR; new queries that exceed budget fail review |
| Cost drift detection | `severity: WARN`, `blocking: false` — log + alert without blocking |

## Metadata produced

| Field | Meaning |
|---|---|
| `bytes_scanned` / `bytes_scanned_human` | Estimated bytes scanned (from BQ, ground truth) |
| `estimated_cost_usd` | Computed locally: `bytes_scanned × on_demand_price_per_tb_usd / 1e12`. Not from BQ. |
| `on_demand_price_per_tb_usd` | Rate used for the cost computation; surfaced so reviewers can spot stale assumptions |
| `slot_ms` | Estimated slot-ms (when available — useful for capacity reservations) |
| `max_bytes` / `max_bytes_human` | Your configured byte threshold (if set) |
| `max_cost_usd` | Your configured USD threshold (if set) |
| `max_slot_ms` | Your configured slot-ms threshold (if set) |
| `failures` | List of which limits were exceeded |

## Pairing with the query asset

The natural pair is a `bigquery_query_asset` whose query you put both here (in the check) and in the asset itself. To avoid the SQL drift problem, store the SQL in a single file and YAML-reference it from both. Or use a `bigquery_create_table_from_query_asset` which makes the SQL canonical in the asset config.

## Auth

Service account needs `roles/bigquery.jobUser` (project-level) — dry-runs are jobs.

## Sister checks

- `bigquery_table_freshness_check` — SLO on max age of a table's last update
- `pandas_dataframe_check` — column existence + dtype (post-materialize)
- `great_expectations_check` — full expectation-suite checks
