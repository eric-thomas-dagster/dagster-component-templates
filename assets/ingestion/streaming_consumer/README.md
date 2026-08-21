# Streaming Consumer

> **🔑 Queue credentials required.** Kafka `bootstrap_servers` + `group_id`; SASL / SSL as needed. Plus whatever sink resource you use (`duckdb`, `postgres`, `snowflake`, ...).

**Always-running queue → transform → sink asset.** The asset's compute is a consumer loop, not a sensor firing per-batch. One long run drains messages, applies a Polars transform per batch, and writes to a sink. Per-batch `AssetMaterialization` heartbeats show up in the catalog so you can see live throughput.

Reach for this when:
- Your source is a queue (Kafka, RabbitMQ later, SQS later, Redis Streams later, NATS later) and you want a **single continuous consumer** rather than a stream of small sensor-triggered runs.
- You need **materialization-per-batch heartbeat** metadata (batch_size, elapsed_seconds, last_offset, total_messages).
- You're on Dagster+ Serverless with **non-isolated runs + `in_process_executor`** — no cold start.

Pair with **`StreamingRunHealthSensorComponent`** for auto-restart: after `max_seconds` (or a crash / Serverless timeout), the sensor detects no active run and fires a new `RunRequest`. Result: 24/7 uptime, sub-second gap between runs.

## Quick example

```yaml
type: dagster_community_components.StreamingConsumerComponent
attributes:
  asset_name: order_events
  group_name: streaming

  queue:
    kind: kafka                                    # v1 supports kafka
    topic: orders
    bootstrap_servers_env_var: KAFKA_BROKERS
    group_id: order_ingest_v1
    auto_offset_reset: latest                       # or 'earliest'
    value_deserializer: json                        # 'json' | 'utf-8' | 'bytes'

  batch_size: 500                                   # cap per-batch pull
  batch_timeout_ms: 1000                            # flush partial batches every 1s
  max_seconds: 3600                                 # 1h bounded — sensor restarts
  # max_seconds: null                               # OR unbounded — runs until killed

  transform:
    - {op: filter, predicate: "amount > 0"}
    - op: with_columns
      expressions:
        ingest_ts: "NOW()"

  sink:
    kind: duckdb                                    # fast path — register-and-INSERT
    resource_key: duckdb
    table: order_events
    if_exists: append
```

## Behavior

- On start: connects to the queue, subscribes to the topic.
- Every batch: polls up to `batch_size` messages (or `batch_timeout_ms` — whichever comes first), wraps in a Polars DataFrame, applies `transform`, writes to `sink`, commits the offset, emits an `AssetMaterialization` event.
- On exit (`max_seconds`, `max_messages`, or SIGTERM): commits final offsets, closes the consumer cleanly, adds summary metadata, returns a `{stop_reason, total_messages, total_batches, elapsed_seconds}` dict.

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Output asset name (also used by the health sensor). |
| `queue` | `dict` | See below. |
| `sink` | `dict` | See below. |

### Loop control

| Field | Default | Description |
|---|---|---|
| `batch_size` | `100` | Max messages per batch. |
| `batch_timeout_ms` | `500` | Max wait per batch before flushing partial. |
| `max_seconds` | `3600` | Bounded run duration. Set `null` for unbounded. |
| `max_messages` | — | Optional cap on total messages consumed. |
| `transform` | — | Polars ops (same shape as `polars_pipeline.operations`). Omit for passthrough. |

## `queue:` — Kafka (v1)

```yaml
queue:
  kind: kafka
  topic: orders                                    # required
  bootstrap_servers_env_var: KAFKA_BROKERS         # OR bootstrap_servers: "b1:9092,b2:9092"
  group_id: order_ingest_v1                        # required
  auto_offset_reset: latest                        # 'latest' | 'earliest'
  value_deserializer: json                         # 'json' (default) | 'utf-8' | 'bytes'

  # Optional SASL / SSL (all forwarded to librdkafka verbatim)
  security_protocol: SASL_SSL
  sasl_mechanism: PLAIN
  sasl_username_env_var: KAFKA_USER
  sasl_password_env_var: KAFKA_PASS

  # Escape hatch — any extra librdkafka config
  extra_config:
    session.timeout.ms: 30000
    max.poll.interval.ms: 300000
```

Every consumed message becomes a row with metadata columns prepended: `_topic`, `_partition`, `_offset`, `_timestamp_ms`. If the value deserializes as a dict, its keys are merged into the row. Otherwise it lands in a `value` column.

## `sink:` — where each batch lands

### `kind: duckdb` (fast path)

```yaml
sink:
  kind: duckdb
  resource_key: duckdb          # a Dagster resource that exposes .get_connection()
  table: order_events
  if_exists: append             # 'append' | 'replace'
  dedup_on: [_topic, _partition, _offset]   # exactly-once on replay — see below
```

Uses `duckdb.register("_batch", df)` + `INSERT INTO ... SELECT * FROM _batch`. 10–100× faster than the SQLAlchemy `table` path on large batches. Writes are wrapped in a transaction — a crash mid-batch rolls back cleanly. Requires a resource whose `.get_connection()` returns a DuckDB connection (e.g. `duckdb_resource`).

### `kind: table` (any SQLAlchemy-compatible resource)

```yaml
sink:
  kind: table
  resource_key: postgres        # or snowflake / bigquery / duckdb / mysql / mssql / oracle / db2
  table: order_events
  schema: public
  if_exists: append
  dedup_on: [_topic, _partition, _offset]   # optional
```

Uses `df.to_pandas().to_sql(table, engine, ...)`. Slower than `kind: duckdb` but works with any resource exposing `.get_engine()` or `.get_connection()`.

### `dedup_on:` — idempotent replays

Kafka's commit-after-write pattern is at-least-once: if the run crashes after sink write but before offset commit, the restarted consumer replays those messages → duplicates in the sink.

`dedup_on: [col1, col2, ...]` fixes this: before insert, run a NOT-EXISTS anti-join against the target table and skip rows whose key tuple already exists.

Since the consumer already prepends `_topic`, `_partition`, `_offset` to every row, `dedup_on: [_topic, _partition, _offset]` gives you a **globally unique key at zero user cost** — effective exactly-once semantics. Use your own columns (e.g. `order_id`) if you want dedup on business-level identity instead.

Heartbeat metadata surfaces both `batch_size` (messages consumed) and `rows_written` (after dedup) per batch — the delta tells you how many rows the dedup filter rejected.

## Metadata

**Per-batch** (`AssetMaterialization` events — visible as a stream in the catalog):
- `batch_size` — messages consumed from the queue.
- `rows_written` — rows actually inserted (differs from `batch_size` when `dedup_on:` skipped some).
- `batch_index`, `total_messages`, `elapsed_seconds`, `last_offset`.
- `checkpoint` — `{partition: last_offset}` map. This IS the "where did we get to" marker. Kafka's broker-side commit stores the same thing (same `group_id` resumes automatically), but having it in asset metadata means you can inspect progress from the Dagster catalog without SSH'ing to a broker.

**Final** (on the terminating `MaterializeResult`):
- `stop_reason` (`max_seconds` / `max_messages` / `external_signal`)
- `total_messages`, `total_batches`, `elapsed_seconds`, `msgs_per_second`
- `queue_kind`, `queue_topic`
- `final_checkpoint` — high-water offset per partition at exit.
- `sink_dedup_on` — echoes the dedup key columns for auditability.

Promote `msgs_per_second` to a Dagster+ Insights custom metric to track ingest throughput; alert if throughput drops below a threshold.

## Failure semantics

- Sink writes are wrapped in a **transaction** — a crash during write rolls back atomically.
- Kafka offsets are committed **after** the sink write — at-least-once semantics.
- With `dedup_on: [_topic, _partition, _offset]` set, replays after a crash are **idempotent** — effective exactly-once at the sink.
- The `checkpoint` metadata per batch means "what did we successfully ingest as of this batch" — durable in the Dagster catalog, independent of broker state.

## Serverless deployment notes

1. **Non-isolated runs + `in_process_executor`** — set both on the code location. Eliminates cold-start on restart between runs.
2. **`max_seconds`** should be less than your Dagster+ Serverless per-run cap (typically 24h). Rec: 3600s (1h) — trades one restart per hour for a graceful commit boundary.
3. **`StreamingRunHealthSensorComponent`** — set `minimum_interval_seconds: 60` (or lower) and target this asset. Restart gap will be < 1 minute.

## Coming (Drop C-2 follow-ups)

- `queue.kind`: `rabbitmq`, `sqs`, `redis_stream`, `nats`.
- Multiple sinks (fan-out per batch).
- Dead-letter queue on transform errors.
