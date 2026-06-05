# Dataframe from .yxdb

Read an Alteryx native binary file (`.yxdb`) and materialize it as a pandas DataFrame asset.

**Why this exists:** `.yxdb` is Alteryx Designer's intermediate / cache format — faster than CSV, preserves dtypes, used heavily in real Alteryx workflows. Until now, customers migrating off Alteryx had to manually export every `.yxdb` to CSV before they could read it from anything else. This component reads it natively.

Pairs with the `alteryx-to-dagster` migration tool — when an Alteryx **Input Data** tool points at a `.yxdb`, the migrator auto-maps it to this component.

---

## Install

The component pulls in [`import-yxdb`](https://pypi.org/project/import-yxdb/) (preferred — one-shot `to_dataframe()`) with a fallback to the lower-level [`yxdb`](https://pypi.org/project/yxdb/) iterator if `import-yxdb` isn't available.

```bash
dagster-component add dataframe_from_yxdb --auto-install
# (installs import-yxdb + yxdb + pandas via the component's requirements.txt)
```

---

## Example

```yaml
type: dagster_component_templates.DataframeFromYxdbComponent
attributes:
  asset_name: customer_master_from_alteryx
  file_path: ${ALTERYX_DATA_DIR}/customers.yxdb
  group_name: ingestion
```

## Standard Dagster knobs supported

| | |
|---|---|
| Freshness | `freshness_max_lag_minutes` + `freshness_cron` |
| Retry | `retry_policy_max_retries` + delay + backoff |
| Kinds | defaults to `['alteryx', 'yxdb']` |
| Owners / Tags / Description / Deps | standard |

## When NOT to use this

| Use case | Right component |
|---|---|
| Read CSV / TSV | `dataframe_from_csv` |
| Read Parquet | `dataframe_from_parquet` |
| Read Excel | `dataframe_from_excel` |
| Read directly from Snowflake / Postgres / BigQuery | `sql_transform` + the matching `*_resource` |
