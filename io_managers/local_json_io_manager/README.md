# LocalJsonIOManager

Stores pandas DataFrames as newline-delimited JSON (`.jsonl`), one record per line. Friendlier than CSV for nested / dict-typed columns — round-trips Python objects through json. Good for asset shapes that include arbitrary metadata.

## Example

```yaml
type: dagster_component_templates.LocalJsonIOManagerComponent
attributes:
  resource_key: io_manager
  base_dir: /tmp/dagster_storage
  create_dir: true
```

## What gets written

Each asset materialization produces a `jsonl` file at:

```
<base_dir>/<asset_key_part_1>/<asset_key_part_2>/.../<part_n>.jsonl
```

For partitioned assets, the partition key is appended:
```
<base_dir>/<asset_key>/<partition_key>.jsonl
```

## Why this exists

Dev / no-warehouse / quick-iteration use cases. Pairs well with
`csv_file_ingestion` upstream and any DataFrame-producing transform.

## Requirements

```
pandas
```
