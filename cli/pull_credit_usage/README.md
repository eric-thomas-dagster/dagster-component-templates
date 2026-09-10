# `pull_credit_usage.py`

Pull Dagster+ credit usage sliced across **deployment × code location × asset × day**,
and dump to CSV / JSON.

The Dagster+ web UI shows credit usage under Insights but doesn't expose a
cross-deployment / per-code-location / per-asset download as one report.
This CLI hits the same GraphQL endpoints the UI does and merges the results
into one flat table.

- **Script:** [`./pull_credit_usage.py`](./pull_credit_usage.py)
- **Requires:** Python 3.8+ (stdlib only — no external deps) + a Dagster+ user API token

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/cli/pull_credit_usage/pull_credit_usage.py \
    -o pull_credit_usage.py
chmod +x pull_credit_usage.py

export DAGSTER_CLOUD_API_TOKEN=user:xxxxxxxx
```

## Four subcommands

### `deployments` — list deployments in the org

Also serves as a token sanity-check.

```bash
./pull_credit_usage.py --org acme deployments
#   dev                   id=401310  PRODUCTION  status=ACTIVE
#   prod                  id=143545  PRODUCTION  status=ACTIVE
```

### `metric-types` — list metric names visible to your Dagster+

Built-in metrics (`__dagster_dagster_credits`, `__dagster_execution_time_ms`,
`__dagster_step_duration_ms`, `__dagster_asset_check_*`, …) plus any custom
Insights metrics you've synced via `sync_custom_metrics.py`. Use this to
pick metric names for the `metrics` subcommand.

```bash
./pull_credit_usage.py --org acme metric-types
```

### `credits` — the ergonomic usage rollup

Pulls `__dagster_dagster_credits` + `__dagster_execution_time_ms` and emits
`credits` + `compute_seconds` columns (ms → s conversion applied for compute).

```bash
./pull_credit_usage.py --org acme \
    credits --start 2026-01-01 --end 2026-09-30 \
    --group-by deployment,code_location,asset,day \
    --output-csv usage.csv
```

### `metrics` — pull any metric(s)

Generic form: takes one or more metric names, produces one column per metric
with the raw aggregate value (no unit conversion). Same axes, same store
handling, same time-chunking as `credits`.

```bash
# One custom metric:
./pull_credit_usage.py --org acme \
    metrics --start 2026-01-01 --end 2026-09-30 \
    --metrics rows_ingested \
    --group-by deployment,asset,day \
    --output-csv rows.csv

# Multiple metrics, one column each:
./pull_credit_usage.py --org acme \
    metrics --start 2026-01-01 --end 2026-09-30 \
    --metrics __dagster_dagster_credits,__dagster_step_duration_ms,cost_usd \
    --group-by deployment,day \
    --output-csv custom.csv
```

Column names strip the `__dagster_` prefix on built-ins:
`__dagster_step_duration_ms` → `step_duration_ms`. Custom metric names
pass through unchanged.

## Options

### Top-level (all subcommands)

| Flag | Required | Default | Description |
|---|---|---|---|
| `--org` | yes | — | Dagster+ org name (e.g. `acme` for `acme.dagster.cloud`) |
| `--token-env` | | `DAGSTER_CLOUD_API_TOKEN` | Env var name holding the user API token |
| `--deployments` | | (all) | Comma-separated deployment names to include, e.g. `prod,staging`. Default: every deployment in the org. |
| `--include-branch-deployments` | | off | Include branch deployments (default: only full deployments) |

### `credits` + `metrics` subcommands (shared flags)

| Flag | Required | Default | Description |
|---|---|---|---|
| `--start` | yes | — | Start date (`YYYY-MM-DD`, inclusive) |
| `--end` | yes | — | End date (`YYYY-MM-DD`, inclusive) |
| `--group-by` | | `deployment,code_location,asset` | Aggregation axes, comma-separated. Any of: `deployment`, `code_location`, `asset`, `day`. Every axis you name becomes a column in the output. |
| `--output-csv <path>` | | (stdout) | Write CSV to this path instead of stdout |
| `--output-json <path>` | | | Write JSON to this path (JSON array of row objects) |
| `--dry-run` | | off | Print what would be queried without executing |
| `--store` | | `BOTH` | Which Insights backend to query: `BOTH` (default), `VICTORIA_METRICS`, or `POSTGRES` |
| `--limit` | | `5000` | Max assets returned per (deployment × store × time chunk). Raise for very large orgs. |

### `metrics` subcommand — additional flag

| Flag | Required | Default | Description |
|---|---|---|---|
| `--metrics` | yes | — | Comma-separated metric names. Use `metric-types` to see what's available on your Dagster+. Built-in names start with `__dagster_`; custom Insights metrics use their `metadata_key`. |

## Group-by axes

| Axis | Meaning |
|---|---|
| `deployment` | One row per deployment |
| `code_location` | One row per (deployment, code_location) |
| `asset` | One row per (deployment, code_location, asset_key) |
| `day` | Appended to any of the above — one row per (…, day) |

For `credits` the numeric columns are always `credits` + `compute_seconds`.
For `metrics` the numeric columns are one per `--metrics` name.

## Output

Three destinations, pick any:

- **stdout** (default) — CSV with headers, ready to pipe into `column -t -s,` or a spreadsheet.
- **`--output-csv <path>`** — CSV file with headers.
- **`--output-json <path>`** — JSON array of row objects.

## Cross-deployment example

```bash
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

## Common failure modes

| Symptom | Cause | Fix |
|---|---|---|
| `HTTP 401` on `deployments` | Bad token or wrong org name | Verify `DAGSTER_CLOUD_API_TOKEN` and `--org` |
| `No data rows.` | Really no runs in window, OR metric name not visible | Run `metric-types` to confirm `Dagster credits` is available; widen the date range |
| `hit --limit=5000` warning | Org has more assets in this window than the flat query returns | Raise `--limit 20000`, or split by `--deployments`, or narrow `--start`/`--end` |
