# DataFrame → Azure Service Bus

Send each DataFrame row to an Azure Service Bus queue or topic as a JSON message. Mirrors dataframe_to_eventhub for SB workloads — completes the registry's existing servicebus_monitor + servicebus_to_database_asset (consumer) pair.

## Service Bus vs Event Hubs — when to use which

| Pattern | Use SB (`dataframe_to_servicebus`) | Use EH (`dataframe_to_eventhub`) |
|---|---|---|
| Ordered queue, FIFO | ✓ | partial (per-partition) |
| Topic + multiple subscribers | ✓ | ✓ (consumer groups) |
| Dead-letter queue | ✓ | ✗ |
| Sessions / per-key ordering | ✓ | partial |
| Transactions | ✓ | ✗ |
| Throughput | medium | high |
| Per-msg cost | $$ | $ |
| Retention | up to 14 days (Std) / unlimited (Prem) | up to 7 days (Std) / 90 days (Prem) |

## Companion components

- `servicebus_monitor` (sensor) — watch for new messages, trigger ingestion
- `servicebus_to_database_asset` (ingestion) — consume messages → DB table

## Config patterns

Plain queue (default):
```yaml
attributes:
  destination_name: my-queue
  destination_type: queue
```

Topic with session-based ordering:
```yaml
attributes:
  destination_name: orders-topic
  destination_type: topic
  session_id_column: customer_id   # required if topic has sessions enabled
```
