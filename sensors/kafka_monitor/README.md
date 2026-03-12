# Kafka Monitor Sensor

Monitor a Kafka topic for new messages and trigger jobs automatically.

## Overview

This sensor polls a Kafka topic for new messages on each evaluation. When messages are detected, it triggers a job and passes message content and metadata via run_config to downstream assets. Partition offsets are tracked in the sensor cursor to ensure exactly-once processing across evaluations.

Perfect for:
- Event-driven pipelines triggered by streaming data
- Processing Kafka messages in micro-batches with Dagster
- Bridging streaming sources into batch Dagster jobs
- Reacting to domain events from microservices

## Features

- **Offset Tracking**: Per-partition offsets are stored in the sensor cursor to avoid reprocessing
- **Batch Consumption**: Configurable max messages per evaluation to bound job fan-out
- **SASL Auth Support**: Supports PLAIN, SCRAM-SHA-256, and other SASL mechanisms
- **SSL/TLS Support**: Works with encrypted Kafka clusters
- **Run Config Support**: Passes full message metadata to downstream assets
- **Error Handling**: Gracefully handles broker connectivity and topic errors

## Configuration

### Required Parameters

- **sensor_name** (string) - Unique name for this sensor
- **bootstrap_servers** (string) - Comma-separated Kafka broker addresses
- **topic** (string) - Kafka topic to monitor
- **group_id** (string) - Consumer group ID
- **job_name** (string) - Name of the job to trigger

### Optional Parameters

- **minimum_interval_seconds** (integer) - Time between polling cycles (default: `30`)
- **max_messages_per_poll** (integer) - Cap on RunRequests created per evaluation (default: `100`)
- **poll_timeout_seconds** (float) - Seconds to wait for messages before giving up (default: `5.0`)
- **security_protocol** (string) - `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL` (default: `"PLAINTEXT"`)
- **sasl_mechanism** (string) - SASL mechanism (e.g., `PLAIN`, `SCRAM-SHA-256`)
- **sasl_username_env_var** (string) - Env var name holding SASL username
- **sasl_password_env_var** (string) - Env var name holding SASL password
- **default_status** (string) - Initial sensor status: `running` or `stopped` (default: `"running"`)

## Authentication

### No Authentication (Development)

```yaml
type: dagster_component_templates.KafkaMonitorSensorComponent
attributes:
  sensor_name: kafka_sensor
  bootstrap_servers: localhost:9092
  topic: my-topic
  group_id: dagster-group
  job_name: process_messages
  security_protocol: PLAINTEXT
```

### SASL/PLAIN (Confluent Cloud, MSK with IAM, etc.)

```yaml
type: dagster_component_templates.KafkaMonitorSensorComponent
attributes:
  sensor_name: kafka_sensor
  bootstrap_servers: pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
  topic: my-topic
  group_id: dagster-group
  job_name: process_messages
  security_protocol: SASL_SSL
  sasl_mechanism: PLAIN
  sasl_username_env_var: KAFKA_API_KEY
  sasl_password_env_var: KAFKA_API_SECRET
```

```bash
export KAFKA_API_KEY="your-api-key"
export KAFKA_API_SECRET="your-api-secret"
```

### SCRAM-SHA-256

```yaml
attributes:
  security_protocol: SASL_SSL
  sasl_mechanism: SCRAM-SHA-256
  sasl_username_env_var: KAFKA_USERNAME
  sasl_password_env_var: KAFKA_PASSWORD
```

## Usage

### Basic Example

```yaml
# File: defs/components/kafka_sensor.yaml
type: dagster_component_templates.KafkaMonitorSensorComponent
attributes:
  sensor_name: user_events_sensor
  bootstrap_servers: broker1:9092,broker2:9092
  topic: user-events
  group_id: dagster-user-events-group
  job_name: process_user_events
  minimum_interval_seconds: 30
  max_messages_per_poll: 50
```

### High-Throughput Topic

For topics with high message volume, increase `max_messages_per_poll` and reduce `minimum_interval_seconds`:

```yaml
type: dagster_component_templates.KafkaMonitorSensorComponent
attributes:
  sensor_name: clickstream_sensor
  bootstrap_servers: broker1:9092
  topic: clickstream
  group_id: dagster-clickstream-group
  job_name: process_clickstream_batch
  minimum_interval_seconds: 10
  max_messages_per_poll: 500
  poll_timeout_seconds: 2.0
```

### Low-Latency Alert Topic

For topics where each message must be processed individually:

```yaml
type: dagster_component_templates.KafkaMonitorSensorComponent
attributes:
  sensor_name: alerts_sensor
  bootstrap_servers: broker1:9092
  topic: critical-alerts
  group_id: dagster-alerts-group
  job_name: handle_alert
  minimum_interval_seconds: 5
  max_messages_per_poll: 1
  poll_timeout_seconds: 1.0
```

## Run Config Schema

This sensor passes Kafka message information to downstream assets via run_config:

```python
{
  "ops": {
    "config": {
      "topic": str,              # Kafka topic name
      "partition": int,          # Partition number the message came from
      "offset": int,             # Message offset within the partition
      "key": str,                # Message key (empty string if null)
      "value": str,              # Message value decoded as UTF-8
      "timestamp_ms": int,       # Message timestamp in milliseconds
      "bootstrap_servers": str   # Broker addresses used
    }
  }
}
```

## Usage with Assets

### Create Asset That Accepts Kafka Message Info

```python
from dagster import asset, Config, AssetExecutionContext
import json

class KafkaMessageConfig(Config):
    topic: str
    partition: int
    offset: int
    key: str
    value: str
    timestamp_ms: int
    bootstrap_servers: str

@asset
def process_kafka_message(context: AssetExecutionContext, config: KafkaMessageConfig):
    # Parse JSON payload
    payload = json.loads(config.value)

    context.log.info(
        f"Processing message from {config.topic}[{config.partition}]@{config.offset}"
    )
    context.log.info(f"Payload: {payload}")

    # Process the message...
    return payload
```

### Route Messages by Type

```python
@asset
def route_event(context: AssetExecutionContext, config: KafkaMessageConfig):
    import json

    event = json.loads(config.value)
    event_type = event.get("type")

    if event_type == "user_signup":
        # Handle signup
        pass
    elif event_type == "purchase":
        # Handle purchase
        pass
    else:
        context.log.warning(f"Unknown event type: {event_type}")
```

## Complete Example Pipeline

### 1. Kafka Sensor

```yaml
# defs/components/order_events_sensor.yaml
type: dagster_component_templates.KafkaMonitorSensorComponent
attributes:
  sensor_name: order_events_sensor
  bootstrap_servers: broker1:9092,broker2:9092
  topic: order-events
  group_id: dagster-order-processing
  job_name: process_order_event
  minimum_interval_seconds: 15
  max_messages_per_poll: 25
```

### 2. Processing Asset

```python
# defs/assets/process_order.py
from dagster import asset, Config, AssetExecutionContext
import json

class KafkaMessageConfig(Config):
    topic: str
    partition: int
    offset: int
    key: str
    value: str
    timestamp_ms: int
    bootstrap_servers: str

@asset
def process_order_event(context: AssetExecutionContext, config: KafkaMessageConfig):
    order = json.loads(config.value)

    order_id = order["order_id"]
    customer_id = order["customer_id"]
    total = order["total"]

    context.log.info(f"Processing order {order_id} for customer {customer_id}: ${total}")

    # Write to database, send confirmation, etc.
    return {"order_id": order_id, "status": "processed"}
```

### How It Works

1. **Message Arrives**: Producer writes `{"order_id": "123", ...}` to `order-events`
2. **Sensor Polls**: Sensor consumes up to 25 messages per evaluation
3. **Job Triggered**: One RunRequest per message, each with full message metadata
4. **Asset Processes**: Asset parses and handles the order event
5. **Cursor Updated**: Sensor stores `{partition: next_offset}` to avoid reprocessing

## Cursor Design

The sensor cursor is a JSON object mapping partition IDs to the next offset to consume:

```json
{"0": 1042, "1": 987, "2": 1103}
```

This allows the sensor to resume exactly where it left off after a restart or pause, independent of Kafka consumer group committed offsets.

## Troubleshooting

### Issue: "Topic not found or no partitions available"

**Solutions**:
1. Verify the topic exists: `kafka-topics.sh --list --bootstrap-server localhost:9092`
2. Check topic name spelling (Kafka topics are case-sensitive)
3. Ensure the broker address is reachable from the Dagster host

### Issue: "Error consuming from Kafka"

**Solutions**:
1. Verify `bootstrap_servers` is correct and reachable
2. Check security protocol and SASL credentials
3. Confirm the consumer group has permission to read the topic

### Issue: Messages consumed but jobs not appearing in Dagster

**Solution**: Check the `run_key` uniqueness — each `{topic}-{partition}-{offset}` combination is globally unique, so duplicate detection should not be an issue.

### Issue: Sensor falls behind on high-throughput topics

**Solutions**:
1. Increase `max_messages_per_poll` to process more messages per evaluation
2. Decrease `minimum_interval_seconds` to evaluate more frequently
3. Consider whether each message warrants a full job run, or batch messages into a single run

## Performance Considerations

1. **Job Fan-out**: Each message creates one RunRequest and therefore one job run. For very high-throughput topics, batch multiple messages per run using `max_messages_per_poll` and a job that processes a list
2. **Poll Timeout**: A low `poll_timeout_seconds` reduces sensor evaluation time but may miss messages on slow brokers
3. **Consumer Startup**: Each sensor evaluation creates a new Kafka consumer — this is intentional to keep sensors stateless but adds ~100–300ms overhead per evaluation

## Requirements

- Python 3.8+
- Dagster 1.5.0+
- kafka-python>=2.0.2
- Network access to Kafka brokers

## Contributing

Found a bug or have a feature request?
- GitHub Issues: https://github.com/eric-thomas-dagster/dagster-component-templates/issues

## License

MIT License
