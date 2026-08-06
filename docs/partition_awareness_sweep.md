# Partition-awareness sweep — plan + progress

## Motivation

As of v0.10.50 the audit at [`partition_aware_components.md`](partition_aware_components.md) found that of 573 asset-emitting components:

- 300 are fully partition-aware (compute reads `context.partition_key*`)
- 32 are external assets (no compute — safe by design)
- **241 are "not yet"** — of which **143 declare a `partition_type` field but their compute ignores partition context**. Setting `partition_type: daily` on those creates a *silent correctness bug* — every partition materializes the same data.

**Closing this gap is the highest-value cleanup we have. The community-components pattern says every asset-emitting component should be either genuinely-partitioned or unpartitioned-by-design — not silently partial.**

## Inline pattern (no shared helper)

Community components live independently — no cross-component imports. Each fixed component gets a small self-contained partition-aware block at the top of its `@asset` compute function:

```python
def _asset(context: AssetExecutionContext):
    # Partition-aware output:
    # `destination_uri: "gs://bucket/exports/{partition_key}/data_*.parquet"`
    # renders to "gs://bucket/exports/2025-01-15/..." on the 2025-01-15
    # partition. Templates without {partition_key} pass through unchanged.
    partition_key = context.partition_key if context.has_partition_key else None
    if partition_key and "{partition_key}" in destination_uri:
        resolved_destination = destination_uri.replace("{partition_key}", str(partition_key))
        context.log.info(
            f"partition-aware export: partition_key={partition_key!r} → {resolved_destination}"
        )
    else:
        resolved_destination = destination_uri

    # ... rest of compute uses `resolved_destination` instead of `destination_uri`
```

Every reference to the destination inside the compute becomes `resolved_destination`. Field declarations, `partitions_def=...` on the `@asset` decorator, and metadata emission are unchanged.

## Design update (v0.10.53) — three patterns, per-family default

After shipping Waves 1a + 1b it became clear the `{partition_key}`-in-table-name pattern is only ONE of three legitimate shapes. See [`partition_patterns.md`](partition_patterns.md) for the full design:

| Pattern | Default for | How enabled |
|---|---|---|
| **A — Per-partition table/path** | Object stores | `{partition_key}` in destination field |
| **B — Single table, partition_column** | **Warehouses (recommended default)** | New `partition_column: str` field on the sink |
| **C — Straight append** | Streaming / event sinks | Neither A nor B set — pre-sweep behavior preserved |

Sinks that already support Pattern A (`{partition_key}` templating) as of v0.10.51-52:
`bigquery_export_to_gcs_asset`, `dataframe_to_snowflake_bulk`, `dataframe_to_clickhouse`, `dataframe_to_starrocks`, `dataframe_to_doris`.

Sinks that also support Pattern B as of v0.10.53:
`dataframe_to_snowflake_bulk`, `dataframe_to_clickhouse`, `dataframe_to_starrocks`, `dataframe_to_doris`. (Warehouse sinks — the analytics customer's default.)

## Waves

- **Wave 1a (v0.10.51)** — Pilot on `bigquery_export_to_gcs_asset` establishes Pattern A.
- **Wave 1b (v0.10.52)** — 4 warehouse sinks (snowflake / clickhouse / starrocks / doris) support Pattern A.
- **Wave 1c (v0.10.53)** — Same 4 warehouse sinks now support Pattern B (`partition_column`). See [`partition_patterns.md`](partition_patterns.md) for the design rationale.
- **Wave 2** — ~50 transforms + AI components in the 143-declared-but-not-used group. For most, compute is already correct via upstream IO-manager slicing; the fix is threading `partition_key` into output metadata + logging + any per-run cache keys.
- **Wave 3** — ~75 misc analytics/ingestion/sources in the 143 group. Semantics vary; per-component design.
- **Wave 4** — The 98 "no field at all" components. Add `partition_type` field + wiring where partitioning makes semantic sense; skip infrastructure / catalog agents / notification sinks that are inherently one-shot.
- **Wave 5 (parallel)** — Sinks that shouldn't have `partition_type` field at all (event-per-row telemetry: `dataframe_to_prometheus`, `dataframe_to_otlp_*`, `smtp_send_asset`, `cloud_tasks_enqueue_asset`, `pubsub_publish_asset`): remove the misleading field OR emit a warning when it's set.

## Which sinks in the 143 group can be partitioned?

**Real DataFrame → destination sinks (Wave 1)**:
- `bigquery_export_to_gcs_asset` ✅ (v0.10.51)
- `dataframe_to_azure_table`
- `dataframe_to_eventhub` (per-event, not partitioned — Wave 5 remove field)
- `dataframe_to_fabric_lakehouse`
- `dataframe_to_kusto`
- `dataframe_to_odata`
- `bigtable_writer_asset`
- `firestore_writer_asset`
- `azure_search_indexer`

**Telemetry / observability sinks (Wave 5 — remove misleading field)**:
- `dataframe_to_dynatrace_events`
- `dataframe_to_newrelic_logs`
- `dataframe_to_otlp_logs`
- `dataframe_to_otlp_metrics`
- `dataframe_to_otlp_traces`
- `dataframe_to_prometheus`
- `dataframe_to_sentry`
- `dataframe_to_servicebus`

**Notification / one-shot sinks (Wave 5)**:
- `smtp_send_asset`
- `cloud_tasks_enqueue_asset`
- `pubsub_publish_asset`
- `lineage_to_openmetadata`

## Verification

Every sweep touches `docs/partition_aware_components.md` to move the fixed component from "❌ Not yet" to "✅ Safe". Any component consumed via `dagster-component add <name>` on any release stays fully self-contained — no cross-component imports.
