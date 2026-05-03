# LocalParquetIOManager

A no-frills IO manager that writes pandas DataFrames as Parquet files in a local directory. Each asset gets its own file; partitioned assets get one file per partition. No database, no cloud — perfect for dev iteration and notebook-style work where you want persistence between runs but not the overhead of a warehouse.

## Example

```yaml
type: dagster_component_templates.LocalParquetIOManagerComponent
attributes:
  resource_key: io_manager
  base_dir: /tmp/dagster_storage
  create_dir: true
```

## What gets written

Each asset materialization produces a `parquet` file at:

```
<base_dir>/<asset_key_part_1>/<asset_key_part_2>/.../<part_n>.parquet
```

For partitioned assets, the partition key is appended:
```
<base_dir>/<asset_key>/<partition_key>.parquet
```

## Why this exists

Dev / no-warehouse / quick-iteration use cases. Pairs well with
`csv_file_ingestion` upstream and any DataFrame-producing transform.

## Requirements

```
pandas
pyarrow
```
