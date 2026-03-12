# NATS JetStream Monitor Sensor

Monitor a NATS JetStream consumer for new messages and trigger jobs automatically.

## Overview

This sensor fetches messages from a durable JetStream consumer on each evaluation. Messages are acknowledged after RunRequests are created. JetStream tracks consumer position server-side — no cursor management needed in Dagster.

**Note**: The JetStream stream and durable consumer must be pre-created on the NATS server before using this sensor.

## Authentication

Supports `.creds` file authentication, NKey+JWT credentials, and unauthenticated clusters.

## Configuration

### Required
- **sensor_name** - Unique sensor name
- **servers** - Comma-separated NATS server URLs
- **stream_name** - JetStream stream name
- **consumer_name** - Durable consumer name (must exist on server)
- **job_name** - Job to trigger per message

### Optional
- **subject_filter** — Only process messages matching this subject
- **max_messages_per_poll** (default `100`)
- **fetch_timeout_seconds** (default `5.0`)
- **credentials_env_var** — Path to `.creds` file env var
- **jwt_env_var**, **nkey_env_var** — JWT/NKey auth env vars
- **minimum_interval_seconds** (default `30`)
- **default_status** (`running` | `stopped`)

## Pre-create Stream and Consumer

```bash
# Create stream
nats stream add EVENTS --subjects "events.>" --storage file --replicas 1

# Create durable consumer
nats consumer add EVENTS dagster-sensor --filter "events.>" --ack explicit --deliver all
```

## Usage

```yaml
type: dagster_component_templates.NATSMonitorSensorComponent
attributes:
  sensor_name: nats_orders_sensor
  servers: nats://nats.internal:4222
  stream_name: ORDERS
  consumer_name: dagster-sensor
  job_name: process_order_event
  subject_filter: orders.created
  credentials_env_var: NATS_CREDENTIALS_PATH
```

## Run Config Schema

```python
{
  "ops": {
    "config": {
      "stream": str,         # JetStream stream name
      "consumer": str,       # Consumer name
      "subject": str,        # Message subject
      "sequence": int,       # Stream sequence number
      "data": str,           # Message data decoded as UTF-8
      "headers": str,        # Message headers dict as string
      "timestamp_ns": int    # Publish timestamp in nanoseconds
    }
  }
}
```

## Requirements

- Python 3.8+, Dagster 1.5.0+, nats-py>=2.3.0

## License

MIT License
