# Partition patterns for sinks + transforms + AI components

## Why this doc exists

Partition-awareness isn't one thing. Different component families need different patterns, and picking the wrong one produces silent bugs or ugly downstream ergonomics. This doc lays out the three legit patterns, which family each fits, and how to declare each in YAML.

## The three patterns

### Pattern A — Per-partition-table (or per-partition-path)

**Shape**: `orders_2025-01-15`, `orders_2025-01-16`, ... — one destination per partition.

**When it fits**:
- Object stores where writes are immutable (`bigquery_export_to_gcs`, `s3_writer`, `gcs_writer`).
- Warehouses **when** the user wants partition-level isolation (drop the partition table, re-run — clean).

**How to declare in YAML** (v0.10.51+ on supported sinks):

```yaml
type: dagster_community_components.BigQueryExportToGcsAssetComponent
attributes:
  destination_uri: "gs://exports/{partition_key}/data_*.parquet"
  partition_type: daily
  partition_start: "2025-01-01"
```

On the 2025-01-15 partition, writes to `gs://exports/2025-01-15/data_*.parquet`. Templates without `{partition_key}` pass through unchanged (unpartitioned or full-refresh assets).

**Trade-off**: proliferates destinations. Analytics queries have to `UNION` across all partition tables/paths — annoying at scale.

### Pattern B — Single table, partition_key as column

**Shape**: one table `orders`, with a `partition_date` column that stores the partition key for each row. Analytics queries just filter `WHERE partition_date = '2025-01-15'`.

**When it fits**:
- Warehouses (snowflake, clickhouse, starrocks, doris, bigquery tables, databricks) — the standard analytics pattern.
- Anywhere downstream consumers already know how to filter by a column.

**How to declare in YAML** (v0.10.53+ on supported warehouse sinks):

```yaml
type: dagster_community_components.DataframeToSnowflakeBulkComponent
attributes:
  table: orders                          # single table
  partition_column: partition_date       # each row gets this column with the partition key
  partition_type: daily
  partition_start: "2025-01-01"
```

Materializing partition `2025-01-15` appends rows into `orders` with `partition_date='2025-01-15'`. Downstream `WHERE partition_date = ...` filters cleanly.

**Trade-off**: re-running a partition duplicates rows unless the sink also supports delete-then-insert (see [Idempotent per-partition writes](#idempotent-per-partition-writes) below).

### Pattern C — Straight append (no partition-scoping in the sink)

**Shape**: one table `orders`, no partition awareness on the sink side. Every partition materialization appends rows.

**When it fits**:
- Streaming / event sinks where partition_key is *just a Dagster tracking concept*, not a data-model concept: `dataframe_to_eventhub`, `dataframe_to_kusto` (per-event insert), `dataframe_to_prometheus`.
- Sinks whose downstream consumers dedupe by their own semantics (Kafka log compaction, Redis TTL, etc.).

**How to declare in YAML**:

```yaml
type: dagster_community_components.DataframeToEventHubComponent
attributes:
  # Neither {partition_key} in the destination nor partition_column set.
  # Sink just appends what it's given. Dagster still tracks which partitions
  # materialized when in the run history.
  event_hub_name: orders-events
  partition_type: daily
  partition_start: "2025-01-01"
```

Dagster tracks materialization history per partition (backfills, retries) even though the sink itself does nothing partition-specific with the data. That's fine — the partition is metadata on the RUN, not on the ROW.

## Per-family default: which pattern to reach for

| Component family | Default pattern | Notes |
|---|---|---|
| **Object stores** (`s3_*`, `gcs_*`, `azure_blob_*`, `bigquery_export_to_gcs`) | **Pattern A** (per-partition-path) | Immutable writes → per-partition path is the safe default |
| **Warehouse tables** (`snowflake_bulk`, `clickhouse`, `starrocks`, `doris`, `bigquery_table_insert`, `databricks`) | **Pattern B** (partition_column) | The standard analytics pattern. Analytics queries filter on the column |
| **Warehouse tables, batch-loader semantic** (rare, e.g. dimensional-fact rebuild) | Pattern A opt-in | Set `{partition_key}` in `table:` when you WANT per-partition table isolation |
| **Streaming / event sinks** (`event_hub`, `kusto` streaming, `kafka_produce`, `pubsub_publish`, telemetry: `prometheus`, `otlp_*`, `sentry`, `newrelic`) | **Pattern C** (straight append) | Per-event insert. Partition_key stays in Dagster's tracking, not in the data. |
| **Cache sinks** (`redis`, `memcached`, `dynamodb` KV writes) | Pattern C or B depending on key strategy | If the cache key includes partition, Pattern B; if TTL-based expiry, Pattern C |
| **Notification sinks** (`smtp_send`, `pagerduty_notify`, `slack_message`) | Pattern C, `partition_type: null` recommended | Notifications are events, not data; partitioning is misleading. Consider removing the field |

## Idempotent per-partition writes (Pattern B, next step)

**Not yet shipped as of v0.10.53** — this is a follow-up. Sinks with `partition_column` set will get a `partition_mode: append | delete_then_insert` field. `delete_then_insert` means:

1. Before writing, `DELETE FROM {table} WHERE {partition_column} = '{partition_key}'`.
2. Insert the new rows.

That makes partition re-runs idempotent for Pattern B. Per-vendor SQL differs (Snowflake `DELETE`, ClickHouse `ALTER TABLE ... DELETE`, etc.) — sink-by-sink implementation.

## Transforms + AI components

**Most don't need any partition-aware code in their compute.** If the upstream is partitioned, Dagster's IO manager provides the correct slice on load. The transform's compute function operates on that slice unchanged. Just declare `partitions_def` correctly (via YAML `partition_type` or `post_processing`) and it works.

The exception: if the transform's compute *depends* on knowing the partition key (e.g., writing partition-suffixed side outputs, sending partition-specific alerts), thread `context.partition_key` in explicitly:

```python
def _asset(context, upstream):
    if context.has_partition_key:
        context.log.info(f"processing partition {context.partition_key}")
    return transform(upstream)
```

## dbt models

**Handled by the official `dagster_dbt.DbtProjectComponent`** (which our `DbtDocsEnrichedProjectComponent` inherits from). When a dbt model has `partitions_def` set (via `post_processing` OR via v0.10.49's `meta.dagster.partitions_def`), the parent component threads `partition_key_start` / `partition_key_end` into `dbt run` as `--vars`. The dbt model's SQL then has to use them:

```sql
{% if is_incremental() %}
  WHERE event_date >= '{{ var("partition_key_start") }}'
    AND event_date <  '{{ var("partition_key_end") }}'
{% endif %}
```

That contract is on your dbt model file, not on our component. Nothing extra for us to add.
