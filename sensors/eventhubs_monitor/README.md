# Azure Event Hubs Monitor Sensor

Monitor an Azure Event Hub for new events and trigger jobs automatically.

## Overview

This sensor reads events from all partitions of an Event Hub on each evaluation. Per-partition sequence numbers are stored in the sensor cursor. One RunRequest is created per event.

Azure Event Hubs is Kafka-compatible — if you prefer to use the `kafka_monitor` sensor with Event Hubs, set `bootstrap_servers` to your Event Hubs Kafka endpoint and use SASL/PLAIN auth. This sensor uses the native `azure-eventhub` SDK which provides tighter Azure integration.

## Authentication

Uses `DefaultAzureCredential` by default. Alternatively, provide a connection string via environment variable.

### Required RBAC Role

**Azure Event Hubs Data Receiver** on the Event Hub or namespace.

## Configuration

### Required
- **sensor_name** - Unique sensor name
- **eventhub_name** - Event Hub name
- **job_name** - Job to trigger per event
- **fully_qualified_namespace** OR **connection_string_env_var**

### Optional
- **consumer_group** (default `$Default`)
- **max_events_per_partition** (default `100`)
- **minimum_interval_seconds** (default `30`)
- **default_status** (`running` | `stopped`)

## Usage

### With DefaultAzureCredential
```yaml
type: dagster_component_templates.EventHubsMonitorSensorComponent
attributes:
  sensor_name: eventhubs_sensor
  eventhub_name: telemetry-hub
  fully_qualified_namespace: mynamespace.servicebus.windows.net
  consumer_group: dagster-consumer
  job_name: process_telemetry_event
```

### With Connection String
```yaml
type: dagster_component_templates.EventHubsMonitorSensorComponent
attributes:
  sensor_name: eventhubs_sensor
  eventhub_name: telemetry-hub
  connection_string_env_var: EVENTHUBS_CONNECTION_STRING
  job_name: process_telemetry_event
```

## Run Config Schema

```python
{
  "ops": {
    "config": {
      "eventhub_name": str,      # Event Hub name
      "partition_id": str,       # Partition ID
      "sequence_number": int,    # Event sequence number
      "offset": str,             # Event offset
      "enqueued_time": str,      # ISO 8601 enqueue timestamp
      "body": str,               # Event body decoded as UTF-8
      "consumer_group": str      # Consumer group name
    }
  }
}
```

## Requirements

- Python 3.8+, Dagster 1.5.0+, azure-eventhub>=5.11.0, azure-identity>=1.12.0

## License

MIT License
