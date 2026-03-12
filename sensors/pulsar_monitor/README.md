# Apache Pulsar Monitor Sensor

Monitor an Apache Pulsar topic for new messages and trigger jobs automatically.

## Overview

This sensor creates a durable subscription on a Pulsar topic and receives messages up to a configured limit per evaluation. Messages are acknowledged after RunRequests are created. Pulsar tracks consumer position server-side per subscription name.

## Authentication

Supports JWT token authentication via environment variable. Unauthenticated clusters work without any auth config.

## Configuration

### Required
- **sensor_name** - Unique sensor name
- **service_url** - Pulsar broker URL (e.g., `pulsar://localhost:6650`)
- **topic** - Fully qualified topic name
- **subscription_name** - Durable subscription name
- **job_name** - Job to trigger per message

### Optional
- **subscription_type** (`Exclusive` | `Shared` | `Failover` | `Key_Shared`, default `Shared`)
- **max_messages_per_poll** (default `100`)
- **receive_timeout_ms** (default `5000`)
- **jwt_token_env_var** — Env var name for JWT auth token
- **minimum_interval_seconds** (default `30`)
- **default_status** (`running` | `stopped`)

## Usage

```yaml
type: dagster_component_templates.PulsarMonitorSensorComponent
attributes:
  sensor_name: pulsar_orders_sensor
  service_url: pulsar+ssl://pulsar.example.com:6651
  topic: persistent://ecommerce/prod/orders
  subscription_name: dagster-orders-sub
  job_name: process_order_message
  jwt_token_env_var: PULSAR_JWT_TOKEN
```

## Run Config Schema

```python
{
  "ops": {
    "config": {
      "topic": str,                  # Pulsar topic name
      "message_id": str,             # Pulsar message ID
      "data": str,                   # Message data decoded as UTF-8
      "publish_timestamp_ms": int,   # Publish timestamp in milliseconds
      "event_timestamp_ms": int,     # Event timestamp in milliseconds
      "partition_key": str,          # Message key
      "properties": str,             # Message properties dict as string
      "subscription": str            # Subscription name
    }
  }
}
```

## Subscription Types

| Type | Use Case |
|------|----------|
| `Exclusive` | Single Dagster process consuming the topic |
| `Shared` | Multiple Dagster deployments sharing the load |
| `Failover` | Active/standby — primary consumer takes all messages |
| `Key_Shared` | Messages with same key always go to same consumer |

## Requirements

- Python 3.8+, Dagster 1.5.0+, pulsar-client>=3.0.0

## License

MIT License
