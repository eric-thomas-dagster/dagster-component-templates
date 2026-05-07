# DataFrame → Azure Data Explorer

Push a DataFrame to a Kusto table on an ADX cluster. Queued ingestion (durable) by default; streaming ingestion (low latency) optional.

## Ingestion modes

- **Queued** (default): Durable. ADX coalesces incoming files for batch ingestion (~5min latency, optimized throughput).
- **Streaming**: Low latency (<1s). Cluster must have streaming ingestion enabled.

## Companion

- `dataframe_from_kusto` — query ADX → DataFrame
