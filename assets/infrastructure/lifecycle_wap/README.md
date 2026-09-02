# `LifecycleWapComponent` + `@lifecycle` decorator

**Write → Audit → Publish** (WAP) lifecycle for assets. Popularized by Netflix + the Iceberg community — write to staging, run quality checks, promote to prod on pass, quarantine on fail. Never publish bad data.

Two shapes, one engine:

| Shape | Use when |
|---|---|
| **`LifecycleWapComponent`** (YAML) | Define a new asset from YAML |
| **`@lifecycle` decorator** (Python) | Wrap an EXISTING `@dg.asset` in Python, no YAML wrangling |

## How it works

1. **Write** — compute function returns a `pandas.DataFrame`; wrapper writes it to a STAGING location (staging file / staging table / staging branch).
2. **Audit** — each check runs against the DataFrame + emits an `AssetCheckResult` (visible in Dagster's asset-check panel).
3. **Publish**:
   - **All checks pass** → apply `on_pass` policy: `publish` (atomic promote) OR `discard` (dry-run).
   - **Any check fails** → apply `on_fail` policy: `quarantine` (move staging aside for triage) OR `discard` (delete) OR `tag_and_keep` (leave staging in place with metadata).

The result: bad data never reaches production. Failed staging is preserved (default) for manual inspection.

## Write backends

### `kind: filesystem` — parquet / csv / json

```yaml
write:
  kind: filesystem
  prod_path: "data/orders/daily.parquet"
  format: parquet            # parquet | csv | json (auto-inferred from suffix)
  staging_path: null         # auto-derived to `data/orders/.staging/daily.parquet`
```

Supports local paths AND fsspec cloud URIs (`s3://`, `gs://`, `abfs://`). Publish = atomic `os.replace` on local, `fs.mv` on cloud (single-fs) or copy+delete on cross-fs.

### `kind: sql` — SQLAlchemy warehouses (Postgres / Snowflake / BigQuery / MySQL / DuckDB)

```yaml
write:
  kind: sql
  resource_key: warehouse       # OR database_url_env_var: DB_URL
  prod_table: orders
  staging_table: null           # defaults to `orders_staging_wap`
  schema: analytics
```

Publish = transactional `DROP TABLE prod; ALTER TABLE staging RENAME TO prod` in a single transaction.

### `kind: iceberg` — via pyiceberg

```yaml
write:
  kind: iceberg
  catalog: default
  table: lakehouse.analytics.orders
  staging_branch: null           # auto-generated `wap_staging_<ts>` if omitted
```

Publish = `fast_forward('main', staging_branch)`. Requires `pip install 'pyiceberg[pyarrow]'`.

## Audit check kinds

| Kind | Shape | What it checks |
|---|---|---|
| `row_count_min` | `{kind, min, name?}` | `len(df) >= min` |
| `row_count_max` | `{kind, max, name?}` | `len(df) <= max` |
| `col_null_ratio_max` | `{kind, col, max, name?}` | fraction of nulls in `col` ≤ `max` |
| `col_unique` | `{kind, col, name?}` | `col` has no duplicates |
| `col_range_min` | `{kind, col, min, name?}` | `df[col].min() >= min` |
| `col_range_max` | `{kind, col, max, name?}` | `df[col].max() <= max` |
| `python` | `{kind, python: 'mod:fn', name}` | User function receives `df`, returns `{passed, description, metadata}` OR bool |

Each check emits an `AssetCheckResult`. Failed checks get `severity=ERROR`; passed checks get `WARN`-only (informational, not shown as errors).

## `@lifecycle` decorator — wrap an existing @dg.asset

```python
import dagster as dg
import pandas as pd
from dagster_community_components import lifecycle

@dg.asset(
    check_specs=[
        dg.AssetCheckSpec(name="rows_gte_1000", asset="daily_orders"),
        dg.AssetCheckSpec(name="user_id_no_nulls", asset="daily_orders"),
        dg.AssetCheckSpec(name="order_id_unique", asset="daily_orders"),
    ],
)
@lifecycle(
    write={"kind": "filesystem",
           "prod_path": "data/orders/daily.parquet",
           "format": "parquet"},
    audit=[
        {"kind": "row_count_min", "min": 1000, "name": "rows_gte_1000"},
        {"kind": "col_null_ratio_max", "col": "user_id", "max": 0.0,
         "name": "user_id_no_nulls"},
        {"kind": "col_unique", "col": "order_id",
         "name": "order_id_unique"},
    ],
    on_pass="publish",
    on_fail="quarantine",
    raise_on_fail=True,
)
def daily_orders(context) -> pd.DataFrame:
    return build_dataset()   # existing user code, unchanged
```

Applied BEFORE `@dg.asset`. The wrapped function must return a `pandas.DataFrame`. `AssetCheckSpec`s on the `@dg.asset` decorator should match the audit check names so the Dagster UI shows them on the asset.

`raise_on_fail=True` (default) → raises `dg.Failure` on any audit fail; downstream blocks. `raise_on_fail=False` → always materializes; downstream can block via `AutomationCondition.any_downstream_conditions()` on the asset checks.

## Full YAML example

```yaml
type: dagster_community_components.LifecycleWapComponent
attributes:
  asset_name: daily_orders

  compute:
    kind: python
    python: "my_project.orders:build_daily"

  write:
    kind: filesystem
    prod_path: "data/orders/daily.parquet"
    format: parquet

  audit:
    - kind: row_count_min
      min: 1000
      name: rows_gte_1000
    - kind: col_null_ratio_max
      col: user_id
      max: 0.0
      name: user_id_no_nulls
    - kind: col_unique
      col: order_id
      name: order_id_unique

  on_pass: publish
  on_fail: quarantine
  quarantine:
    quarantine_path: "data/orders/.quarantine/daily.parquet"
```

Compute function (elsewhere in the codebase):

```python
# my_project/orders.py
import pandas as pd

def build_daily(context):
    # ...build the DataFrame however you want...
    return df
```

## Metadata on the materialization

Every run reports:
- `wap_write_kind` — filesystem | sql | iceberg
- `wap_all_passed` — bool (all audits passed?)
- `wap_publish_outcome` — published | quarantined | discarded | kept
- `wap_row_count` — rows in the DataFrame
- `wap_check_summary` — `N/M passed`
- Backend-specific: `wap_prod_path`, `wap_quarantine_table`, `wap_iceberg_branch`, etc.

Plus one `AssetCheckResult` per audit check with pass/fail + description + metadata.

## Composes with

- `smart_retry` / `@smart_retry` — wrap `daily_orders` with retry classification for transient write failures. The two decorators stack cleanly:
  ```python
  @dg.asset
  @smart_retry(rules=[...], max_attempts=3)
  @lifecycle(write={...}, audit=[...])
  def daily_orders(context):
      return build_dataset()
  ```
- `filesystem_monitor` sensor — trigger a downstream review when data lands in the quarantine directory.

## What's not in v1 (roadmap)

- **Delta Lake backend** — via `delta-rs` or Spark. Same shape as Iceberg (staging branch → publish).
- **BigQuery native (non-sqlalchemy)** — for orgs using BQ without sqlalchemy-bigquery.
- **Great Expectations delegation** — `{kind: great_expectations, suite: '...'}` runs a GE suite as one check.
- **dbt test delegation** — `{kind: dbt_test, select: 'tag:critical'}` runs dbt tests as WAP audits.
- **Cross-partition audit windows** — audit "last 7 days including this partition" instead of only the current DataFrame.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Dagster asset name emitted by this component. |
| `compute` | `Dict[str, Any]` | Compute config. Shape: `{kind: python, python: 'mod:fn'}`. Function returns a pandas DataFrame; the component handles write/audit/publish. |
| `write` | `Dict[str, Any]` | Write backend. Shape (filesystem): `{kind: filesystem, prod_path, staging_path?, format?: parquet\|csv\|json}`. Shape (sql): `{kind: sql, resource_key\|database_url_env_var, prod_table, staging_table?, schema?}`. Shape (… _(full docs in schema.json + component README)_ |
| `audit` | `List[Dict[str, Any]]` | Ordered audit checks. Each: `{kind: row_count_min\|row_count_max\|col_null_ratio_max\|col_unique\|col_range_min\|col_range_max\|python, name?, ...kind-specific fields}`. Every check becomes an AssetCheckSpec. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | — |
| `description` | `str` | — | — |
| `owners` | `List[str]` | — | — |
| `tags` | `Dict[str, str]` | — | — |
| `kinds` | `List[str]` | — | Asset kinds. Default: ['python', 'wap', 'lifecycle']. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `str` | — | Optional upstream asset passed as second arg to compute.python. |
| `on_pass` | `str` | `"publish"` | Publish policy on ALL checks pass: `publish` (default) or `discard`. |
| `on_fail` | `str` | `"quarantine"` | Publish policy on ANY check fail: `quarantine` (default), `discard`, `tag_and_keep`. |
| `quarantine` | `Dict[str, Any]` | — | Quarantine target. Filesystem: `{quarantine_path}`. SQL: `{quarantine_table}`. Iceberg: `{quarantine_tag}`. Omit to auto-derive. |
| `raise_on_fail` | `bool` | `true` | If true (default), raise `dg.Failure` when any audit check fails. Set false to always materialize the asset + rely on AssetCheckResult signals downstream via AutomationCondition. |

[//]: # (FIELDS:END)
