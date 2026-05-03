# LocalCsvIOManager

Stores pandas DataFrames as CSV files in a local directory — the most human-inspectable option. Slower and lossier than parquet (no dtypes, no indexes preserved) but every asset materialization is a file you can `cat` or open in Excel. Good for demos and reviewing intermediate state.

## Example

```yaml
type: dagster_component_templates.LocalCsvIOManagerComponent
attributes:
  resource_key: io_manager
  base_dir: /tmp/dagster_storage
  create_dir: true
```

## What gets written

Each asset materialization produces a `csv` file at:

```
<base_dir>/<asset_key_part_1>/<asset_key_part_2>/.../<part_n>.csv
```

For partitioned assets, the partition key is appended:
```
<base_dir>/<asset_key>/<partition_key>.csv
```

## Why this exists

Dev / no-warehouse / quick-iteration use cases. Pairs well with
`csv_file_ingestion` upstream and any DataFrame-producing transform.

## Requirements

```
pandas
```
