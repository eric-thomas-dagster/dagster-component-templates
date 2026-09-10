# `pull_credit_usage.py`

Pull Dagster+ credit usage sliced across **deployment × code location × asset × day**,
and dump to CSV / JSON.

The Dagster+ web UI shows credit usage under Insights but doesn't expose a
cross-deployment / per-code-location / per-asset download as one report. This
CLI hits the same GraphQL endpoints the UI does and merges the results into
one flat table.

- **Script:** [`../pull_credit_usage.py`](../pull_credit_usage.py)
- **Requires:** Python 3.8+ (stdlib only — no external deps) + a Dagster+ user API token
- **Verified end-to-end** against a live Dagster+ deployment on 2026-09-10

## Install + run

```bash
export DAGSTER_CLOUD_API_TOKEN=user:xxxxxxxx

curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/cli/pull_credit_usage.py \
    -o pull_credit_usage.py
chmod +x pull_credit_usage.py
```

## Three subcommands

### `deployments` — list deployments in the org

Also serves as a token sanity-check.

```bash
./pull_credit_usage.py --org acme deployments
#   dev                   id=401310  PRODUCTION  status=ACTIVE
#   prod                  id=143545  PRODUCTION  status=ACTIVE
```

### `metric-types` — list metric names visible to your Dagster+

Includes built-ins (`__dagster_dagster_credits`, `__dagster_execution_time_ms`, …) and any
custom Insights metrics you've synced via `sync_custom_metrics.py`.

```bash
./pull_credit_usage.py --org acme metric-types
```

### `credits` — the actual usage rollup

```bash
./pull_credit_usage.py --org acme \
    credits --start 2026-01-01 --end 2026-09-30 \
    --group-by deployment,code_location,asset,day \
    --output-csv usage.csv
```

## Group-by axes (composable, comma-separated)

| Axis | Meaning |
|---|---|
| `deployment` | One row per deployment |
| `code_location` | One row per (deployment, code_location) |
| `asset` | One row per (deployment, code_location, asset_key) |
| `day` | Appended to any of the above — one row per (…, day) |

Every axis you name becomes a column in the output; `credits` +
`compute_seconds` are the aggregated numeric columns.

The script picks the right GraphQL path based on the axes you request:

| Axes | Path | Endpoints hit |
|---|---|---|
| `deployment` or `deployment,day` | `reportingMetricsByAsset` per deployment | 1 per (deployment × store) |
| includes `code_location` or `asset` | same, with client-side aggregation | 1 per (deployment × store) |

Day bucketing uses `granularity: DAILY` under the hood — no manual
window-splitting. The Dagster+ API returns
`timestamps: [epoch, epoch, …]` + `values: [n, n, …]` per entity, and the
script explodes those into one row per (…, day).

## Output

Three destinations, pick any:

- **stdout** — default when neither `--output-csv` nor `--output-json` is set. Prints CSV
  with headers, ready to pipe into `column -t -s,` or a spreadsheet.
- **`--output-csv <path>`** — CSV file with headers.
- **`--output-json <path>`** — JSON array of row objects.

## Cross-deployment example

```bash
export DAGSTER_CLOUD_API_TOKEN=user:xxxxxxxx

./pull_credit_usage.py --org acme \
    --deployments prod,staging \
    credits --start 2026-01-01 --end 2026-09-30 \
    --group-by deployment,code_location,asset,day \
    --output-csv acme_9mo_full.csv

./pull_credit_usage.py --org acme \
    --deployments prod,staging \
    credits --start 2026-01-01 --end 2026-09-30 \
    --group-by deployment,day \
    --output-csv acme_9mo_daily_trend.csv
```

## Dagster+ Insights internals (learned the hard way)

Behaviors baked into the script — worth knowing when reading the output or
debugging edge cases:

| Constraint | What the CLI does about it |
|---|---|
| Single-query window capped at **120 days** | Auto-chunked. `--start` / `--end` can span any range; the script fans out. |
| **Two metric stores**: `VICTORIA_METRICS` (recent, ≈6 months) + `POSTGRES` (long-tail history) | Default `--store BOTH` queries both and unions. `--store VICTORIA_METRICS` alone will silently drop everything > ~6 months old. |
| `codeLocations` filter supported by POSTGRES but returns 500 on VM | Script never uses the filter. Code-location is joined client-side via `assetNodes { assetKey repository { location { name } } }` on the same deployment endpoint. |
| `reportingMetricsByDeployment` returns `ReportingInputError: Branch deployment metrics are not yet supported in VictoriaMetrics` on VM tenants | Script uses `reportingMetricsByAsset` throughout and rolls up deployment totals client-side. |
| Default `metricsFilter.limit` is 10 | `--limit 5000` default. Raise for very large orgs or split the window. |
| VM 500s with `PythonError: Internal Server Error (Trace ID: …)` are the **"no data" signal**, not transient errors | Not retried. Falls through to POSTGRES cleanly. |

## Cross-boundary behavior

Because VM and POSTGRES track different subsets of the org's history, you'll
sometimes see the same `asset_key` appear twice in `--group-by code_location`
output — once with a populated code_location (from POSTGRES) and once with
`""` (from VM). Those aren't duplicates:

- **POSTGRES rows** carry the historical code_location the asset ran under when
  the credits were consumed.
- **VM rows** come from `reportingMetricsByAsset` which doesn't index by code
  location on VM. The script client-side-joins via `assetNodes`, which only
  knows about currently-deployed assets. Anything renamed or decommissioned
  since VM's retention started (~6 months ago) shows up as `""`.

For a customer-facing report where this split is confusing, aggregate by
`asset` alone (skip `code_location`) — the two paths collapse into one row.

## Common failure modes

| Symptom | Cause | Fix |
|---|---|---|
| `HTTP 401` on `deployments` | Bad token or wrong org name | Verify `DAGSTER_CLOUD_API_TOKEN` and `--org` |
| `No data rows.` | Really no runs in window, OR VM/POSTGRES both empty for the range | Try `metric-types` to confirm `__dagster_dagster_credits` is visible; widen the date range |
| `hit --limit=5000` warning | Org has more assets in this window than the flat query returns | Raise `--limit 20000`, or split by `--deployments`, or narrow `--start`/`--end` |
| Repeated 500s in stderr but final result looks correct | Normal — VM returns 500 for date ranges past its ~6-month retention; POSTGRES picks up the slack | No action needed |

## Sharing with customers

The script is self-contained — stdlib only, no `pip install` step. Safe to
`curl` and hand a customer directly. Rename the `argparse` `prog=` string in
`main()` if you want it to identify differently in `--help` output.
