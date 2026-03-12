# MQTT Monitor Sensor

Subscribe to an MQTT broker topic and trigger jobs when messages are received — ideal for IoT, telemetry, and device event pipelines.

## Overview

This sensor connects to an MQTT broker, subscribes to a topic, collects messages for a configurable window, then disconnects. One RunRequest is created per message. Supports wildcard topics, QoS levels, TLS, and username/password authentication.

## Configuration

### Required
- **sensor_name** - Unique sensor name
- **broker_host** - MQTT broker hostname
- **topic** - Topic to subscribe to (wildcards supported)
- **job_name** - Job to trigger per message

### Optional
- **broker_port** (default `1883`, TLS: `8883`)
- **qos** (0, 1, or 2 — default `1`)
- **collect_window_seconds** (default `5.0`) — How long to listen per evaluation
- **max_messages_per_poll** (default `100`)
- **client_id** — Custom MQTT client ID (auto-generated if unset)
- **use_tls** (default `false`)
- **username_env_var**, **password_env_var** — Credential env vars
- **minimum_interval_seconds** (default `60`)
- **default_status** (`running` | `stopped`)

## MQTT Topic Wildcards

| Pattern | Matches |
|---------|---------|
| `sensors/+/temperature` | `sensors/room1/temperature`, `sensors/room2/temperature` |
| `sensors/#` | All topics under `sensors/` |
| `factory/line1/+/status` | Any sensor status on line 1 |

## Usage

```yaml
type: dagster_component_templates.MQTTMonitorSensorComponent
attributes:
  sensor_name: factory_telemetry_sensor
  broker_host: mqtt.factory.internal
  topic: sensors/+/temperature
  job_name: process_temperature_reading
  qos: 1
  collect_window_seconds: 5.0
  use_tls: true
  broker_port: 8883
  username_env_var: MQTT_USERNAME
  password_env_var: MQTT_PASSWORD
```

## Run Config Schema

```python
{
  "ops": {
    "config": {
      "topic": str,          # Exact topic the message was received on
      "payload": str,        # Message payload decoded as UTF-8
      "qos": int,            # QoS level of the message
      "retain": bool,        # Whether message was a retained message
      "timestamp_s": float,  # Unix timestamp when message was collected
      "broker_host": str     # Broker hostname
    }
  }
}
```

## Usage with Assets

```python
from dagster import asset, Config, AssetExecutionContext
import json

class MQTTMessageConfig(Config):
    topic: str
    payload: str
    qos: int
    retain: bool
    timestamp_s: float
    broker_host: str

@asset
def process_temperature_reading(context: AssetExecutionContext, config: MQTTMessageConfig):
    data = json.loads(config.payload)
    sensor_id = config.topic.split("/")[1]
    temperature = data["value"]
    context.log.info(f"Sensor {sensor_id}: {temperature}°C at {config.timestamp_s}")
    return {"sensor_id": sensor_id, "temperature": temperature}
```

## Notes

- **Retained messages**: The first connection per evaluation will receive any retained message on the subscribed topic. If you only want live messages, filter on `config.retain == False` in your asset.
- **QoS 2**: Not recommended for sensor use — the sensor does not maintain a persistent session between evaluations.
- **High-frequency topics**: For topics publishing faster than `collect_window_seconds`, `max_messages_per_poll` caps the RunRequest fan-out.

## Requirements

- Python 3.8+, Dagster 1.5.0+, paho-mqtt>=1.6.1

## License

MIT License
