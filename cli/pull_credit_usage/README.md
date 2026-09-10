# `pull_credit_usage.py`

Pull Dagster+ credit usage sliced across **deployment × code location × asset × day**,
and dump to CSV / JSON.

The Dagster+ web UI shows credit usage under Insights but doesn't expose a
cross-deployment / per-code-location / per-asset download as one report.
This CLI hits the same GraphQL endpoints the UI does and merges the results
into one flat table.

- **Script:** [`../pull_credit_usage.py`](../pull_credit_usage.py)
- **Requires:** Python 3.8+ (stdlib only — no external deps) + a Dagster+ user API token

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/cli/pull_credit_usage.py \
    -o pull_credit_usage.py
chmod +x pull_credit_usage.py

export DAGSTER_CLOUD_API_TOKEN=user:xxxxxxxx
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

Built-in metrics (`Dagster credits`, `Compute duration`, …) plus any custom
Insights metrics you've synced via `sync_custom_metrics.py`.

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

## Options

### Top-level (all subcommands)

| Flag | Required | Default | Description |
|---|---|---|---|
| `--org` | yes | — | Dagster+ org name (e.g. `acme` for `acme.dagster.cloud`) |
| `--token-env` | | `DAGSTER_CLOUD_API_TOKEN` | Env var name holding the user API token |
| `--deployments` | | (all) | Comma-separated deployment names to include, e.g. `prod,staging`. Default: every deployment in the org. |
| `--include-branch-deployments` | | off | Include branch deployments (default: only full deployments) |

### `credits` subcommand

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

## Group-by axes

| Axis | Meaning |
|---|---|
| `deployment` | One row per deployment |
| `code_location` | One row per (deployment, code_location) |
| `asset` | One row per (deployment, code_location, asset_key) |
| `day` | Appended to any of the above — one row per (…, day) |

`credits` + `compute_seconds` are the aggregated numeric columns.

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
