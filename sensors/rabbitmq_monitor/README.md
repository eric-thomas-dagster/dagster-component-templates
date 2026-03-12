# RabbitMQ Monitor Sensor

Poll a RabbitMQ queue for new messages and trigger jobs automatically.

## Overview

This sensor uses `basic_get` to poll a RabbitMQ queue on each evaluation without holding a persistent consumer connection. Messages are acknowledged after RunRequests are created. One RunRequest is created per message.

## Configuration

### Required
- **sensor_name** - Unique sensor name
- **host** - RabbitMQ broker hostname
- **queue_name** - Queue to monitor
- **job_name** - Job to trigger per message

### Optional
- **port** (default `5672`, TLS: `5671`)
- **virtual_host** (default `/`)
- **username_env_var** — Env var name for username (defaults to `guest`)
- **password_env_var** — Env var name for password (defaults to `guest`)
- **use_tls** (default `false`)
- **max_messages_per_poll** (default `100`)
- **minimum_interval_seconds** (default `30`)
- **default_status** (`running` | `stopped`)

## Usage

```yaml
type: dagster_component_templates.RabbitMQMonitorSensorComponent
attributes:
  sensor_name: rabbitmq_orders_sensor
  host: rabbitmq.internal
  queue_name: order-events
  job_name: process_order_event
  username_env_var: RABBITMQ_USER
  password_env_var: RABBITMQ_PASS
  port: 5672
```

## Run Config Schema

```python
{
  "ops": {
    "config": {
      "queue": str,          # Queue name
      "routing_key": str,    # Message routing key
      "exchange": str,       # Exchange name
      "message_id": str,     # AMQP message ID
      "body": str,           # Message body decoded as UTF-8
      "content_type": str    # MIME content type
    }
  }
}
```

## Usage with Assets

```python
from dagster import asset, Config, AssetExecutionContext
import json

class RabbitMQMessageConfig(Config):
    queue: str
    routing_key: str
    exchange: str
    message_id: str
    body: str
    content_type: str

@asset
def process_rabbitmq_message(context: AssetExecutionContext, config: RabbitMQMessageConfig):
    payload = json.loads(config.body)
    context.log.info(f"Processing {config.routing_key}: {payload}")
    return payload
```

## Requirements

- Python 3.8+, Dagster 1.5.0+, pika>=1.3.0

## License

MIT License
