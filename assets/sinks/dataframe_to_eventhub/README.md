# DataFrame → Azure Event Hubs

Send each row of an upstream DataFrame to an Azure Event Hub as a JSON
event. Use case: inject synthetic / replay events into a queue from any
DataFrame-producing asset (e.g. `synthetic_data_generator`,
`csv_source`, `dataframe_from_table`) without writing a custom producer.

## Composes with

- `synthetic_data_generator` upstream → seed an EH with N test events
- `eventhubs_to_database_asset` downstream → consume + land in DB
- `eventhubs_monitor` (sensor) → drive ingest reactively

## Required env

| Var | Value |
|---|---|
| (your `connection_string_env_var`) | EH namespace or hub-scoped connection string from `az eventhubs namespace authorization-rule keys list` or hub-scoped rule |

## Behavior

- Each row → one `EventData` payload, body = `json.dumps(row, default=str)`
- Rows are added to an `EventDataBatch`; if it overflows EH's 1MB byte cap
  the batch is flushed and a new one started
- `partition_key_column` (optional): pins same-key rows to one partition,
  preserving ordering inside that partition
- No partitioning of the asset itself (use a partitioned upstream if you
  want one EH send per partition_key)

## Pattern: this is one of many DataFrame-to-queue components

This component is dedicated to Azure Event Hubs because each message-queue
SDK has distinct auth + config (connection string, topic, partition key
semantics). The same shape exists for other queues:

- `redis_writer` (already in registry) → Redis hashes / streams
- `dataframe_to_kafka` (planned) → Kafka topic
- `dataframe_to_pubsub` (planned) → GCP Pub/Sub
- `dataframe_to_kinesis` (planned) → AWS Kinesis

Generic configurations for all-of-the-above don't compose well because
each backend is structurally different — keeping per-backend components
focused beats a single fat component with conditional auth/config.
